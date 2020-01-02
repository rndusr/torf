# This file is part of torf.
#
# torf is free software: you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# torf is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with torf.  If not, see <https://www.gnu.org/licenses/>.

import flatbencode as bencode
import base64
import hashlib
from datetime import datetime
import os
import math
from collections import abc, namedtuple
import errno
import inspect
import io

from . import _utils as utils
from . import _errors as error
from . import _generate as generate

from . import __version__
_PACKAGE_NAME = __name__.split('.')[0]
NCORES = len(os.sched_getaffinity(0))

class Torrent():
    """
    Torrent metainfo representation

    Create a new Torrent object:

    >>> from torf import Torrent
    >>> torrent = Torrent('path/to/My Torrent',
    ...                   trackers=['https://localhost:123/announce'],
    ...                   comment='This is my first torrent. Be gentle.')

    Convenient access to metainfo via properties:

    >>> torrent.comment
    'This is my first torrent. Be gentle.'
    >>> torrent.comment = "This is my first torrent. Let's rock!"
    >>> torrent.private = True

    Full control over unencoded metainfo:

    >>> torrent.metainfo['info']['private']
    True
    >>> torrent.metainfo['more stuff'] = {'foo': 12,
    ...                                   'bar': ('x', 'y', 'z')}

    Hash pieces and update progress once per second:

    >>> def callback(torrent, filepath, pieces_done, pieces_total):
    ...     print(f'{pieces_done/pieces_total*100:3.0f} % done')
    >>> success = torrent.generate(callback, interval=1)
      1 % done
      2 % done
      [...]
    100 % done

    Write torrent file:

    >>> torrent.write('my_torrent.torrent')

    Create magnet link:

    >>> torrent.magnet()
    'magnet:?xt=urn:btih:e167b1fbb42ea72f051f4f50432703308efb8fd1&dn=My+Torrent&xl=142631&tr=https%3A%2F%2Flocalhost%3A123%2Fannounce'

    Read torrent from file:

    >>> t = Torrent.read('my_torrent.torrent')
    >>> t.comment
    "This is my first torrent. Let's rock!"
    """

    def __init__(self, path=None, name=None,
                 exclude=(), trackers=None, webseeds=None, httpseeds=None,
                 private=None, comment=None, source=None,
                 creation_date=None, created_by='%s/%s' % (_PACKAGE_NAME, __version__),
                 piece_size=None, randomize_infohash=False):
        self._metainfo = {}
        self._trackers = utils.Trackers(callback=self._trackers_changed)
        self._webseeds = utils.URLs((), callback=self._webseeds_changed)
        self._httpseeds = utils.URLs((), callback=self._httpseeds_changed)
        self.trackers = trackers
        self.webseeds = webseeds
        self.httpseeds = httpseeds
        self.private = private
        self.comment = comment
        self.creation_date = creation_date
        self.created_by = created_by
        self.source = source
        self.randomize_infohash = randomize_infohash
        self.exclude = exclude
        self.path = path
        # Values that are implicitly changed by setting self.path
        if piece_size is not None:
            self.piece_size = piece_size
        if name is not None:
            self.name = name

    @property
    def metainfo(self):
        """
        Unencoded torrent metainfo as mutable mapping

        You can put anything in here as long as keys are convertable to
        :class:`bytes` and values are convertable to :class:`bytes`,
        :class:`int`, :class:`list` or :class:`dict`.

        See also :meth:`convert` and :meth:`validate`.

        The ``info`` key is guaranteed to exist.
        """
        if 'info' not in self._metainfo:
            self._metainfo['info'] = {}
        return self._metainfo

    @property
    def path(self):
        """
        File system path to torrent content

        The properties :attr:`name` and :attr:`piece_size` are changed
        implicitly when this property is set.

        Setting this property sets ``name`` and ``piece length`` in
        :attr:`metainfo`\ ``['info']`` as well as ``length`` if path is a file
        or ``files`` if path is a directory.

        If set to ``None``, the following keys are removed (if present) from
        :attr:`metainfo`\ ``['info']``: ``piece length``, ``pieces``, ``name``,
        ``length``, ``files``

        :raises PathEmptyError: if :attr:`path` contains no data (i.e. empty
            file, empty directory or directory containing only empty files)
        :raises ReadError: if :attr:`path` is a directory and not readable
        :raises PathNotFoundError: if :attr:`path` doesn't exist
        """
        return getattr(self, '_path', None)
    @path.setter
    def path(self, value):
        info = self.metainfo['info']

        # Unset path and remove related metainfo
        if hasattr(self, '_path'):
            delattr(self, '_path')
        info.pop('pieces', None)

        if value is not None:
            # Set new path and update related metainfo
            path = os.path.normpath(str(value))
            if not os.path.exists(path):
                raise error.PathNotFoundError(value)
            elif os.path.isdir(path):
                files = []
                basepath = path.split(os.sep)
                for filepath in utils.filepaths(path, exclude=self.exclude,
                                                hidden=False, empty=False):
                    files.append({'length': os.path.getsize(filepath),
                                  'path'  : filepath.split(os.sep)[len(basepath):]})
                info['files'] = files
                # If this was previously a singlefile torrent, we must remove
                # the relevant keys from metainfo
                info.pop('length', None)
                info.pop('md5sum', None)
            else:
                info['length'] = os.path.getsize(path)
                # If this was previously a multifile torrent, we must remove the
                # relevant keys from metainfo
                info.pop('files', None)

            if self.size < 1:
                raise error.PathEmptyError(path)
            else:
                self._path = path
                if path == '.':
                    self.name = os.path.basename(os.path.abspath('.'))
                elif path == '..':
                    self.name = os.path.basename(os.path.abspath('..'))
                else:
                    # Set default name
                    info.pop('name', None)
                    self.name
                self.piece_size = None

    @property
    def mode(self):
        """
        "singlefile" if this torrent contains one file that is not in a directory,
        "multifile" if it contains one or more files in a directory, or ``None``
        if no content is specified (i.e. :attr:`path` is None).
        """
        if 'length' in self.metainfo['info']:
            return 'singlefile'
        elif 'files' in self.metainfo['info']:
            return 'multifile'

    @property
    def files(self):
        """
        Yield relative file paths specified in :attr:`metainfo`

        Each path starts with :attr:`name`.

        Note that the paths may not exist. See :attr:`filepaths` for existing
        files.
        """
        info = self.metainfo['info']
        if self.mode == 'singlefile':
            yield info['name']
        elif self.mode == 'multifile':
            rootdir = self.name
            if rootdir is None:
                raise RuntimeError('Torrent has no name')
            for fileinfo in info['files']:
                yield os.path.join(rootdir, os.path.join(*fileinfo['path']))

    @property
    def filepaths(self):
        """
        Yield absolute paths to existing files in :attr:`path`

        Any files that match patterns in :attr:`exclude` as well as hidden and
        empty files are not included.
        """
        if self.path is not None:
            yield from utils.filepaths(self.path, exclude=self.exclude,
                                       hidden=False, empty=False)

    File = namedtuple('File', ('name', 'path', 'dir', 'size'))

    @property
    def filetree(self):
        """
        :attr:`files` as a dictionary tree

        Parent nodes are dictionaries and leaf nodes are :attr:`File` instances.
        The top node is always a dictionary with the single key :attr:`name`.

        If :attr:`path` is ``None``, this is an empty ``dict``.

        Example:

        .. code:: python

            {'Torrent': {'foo.txt': File(name='foo.txt',
                                         path='Torrent/foo.txt',
                                         dir='Torrent',
                                         size=123456),
                         'bar': {'baz.pdf': File(name='baz.pdf',
                                                 path='Torrent/bar/baz.pdf',
                                                 dir='Torrent/bar',
                                                 size=123456),
                                 'baz.mp3': File(name='baz.mp3',
                                                 path='Torrent/bar/baz.mp3',
                                                 dir='Torrent/bar',
                                                 size=123456)}}}
        """
        tree = {}   # Complete directory tree
        prefix = []
        paths = (f.split(os.sep) for f in self.files)
        for path in paths:
            dirpath = path[:-1]  # Path without filename
            filename = path[-1]
            subtree = tree
            for item in dirpath:
                if item not in subtree:
                    subtree[item] = {}
                subtree = subtree[item]
            subtree[filename] = self.File(filename,
                                          os.path.join(*path),
                                          os.path.join(*dirpath) if dirpath else '',
                                          self.partial_size(path))
        return tree

    def remove(self, *paths):
        """
        Remove files from :attr:`metainfo`\ ``['info']``\ ``['files']``

        :param str paths: Iterable of relative paths to remove; each path must
            start with :attr:`name`
        :type path: str or iterable

        Non-existing paths are silently ignored.

        If files are removed after :meth:`generate` was called, it must be
        called again.

        :raises PathNotFoundError: if path is not specified in :attr:`metainfo`
        :raises RuntimeError: if :attr:`mode` is not "multifile"
        """
        if self.mode == 'singlefile':
            raise RuntimeError('Cannot remove files from single-file torrent')
        elif self.mode is None:
            raise RuntimeError('No files specified in torrent')

        # Ensure we can edit file list in place
        if not isinstance(self.metainfo['info']['files'], abc.MutableSequence):
            self.metainfo['info']['files'] = list(self.metainfo['info']['files'])

        files_removed = False
        for path in paths:
            if isinstance(path, str):
                path = tuple(path.split(os.sep))
            else:
                path = tuple(path)

            # Edit file list; to prevent KeyErrors when removing items in a
            # loop, we use a generator expression instead
            def keep(info):
                path_ = (self.name,) + tuple(info['path'])
                keep = not utils.iterable_startswith(path_, path)
                if not keep:
                    nonlocal files_removed
                    files_removed = True
                return keep
            self.metainfo['info']['files'][:] = (
                info
                for i,info in enumerate(self.metainfo['info']['files'])
                if keep(info)
            )

        if files_removed and 'pieces' in self.metainfo['info']:
            del self.metainfo['info']['pieces']

    def partial_size(self, path):
        """
        Return size of one or more files as specified in :attr:`metainfo`

        :param path: Relative path within torrent, starting with :attr:`name`;
                     may point to file or directory
        :type path: str or iterable

        :raises PathNotFoundError: if path is not specified in :attr:`metainfo`
        """
        if isinstance(path, str):
            path = tuple(path.split(os.sep))
        else:
            path = tuple(path)
        if self.mode == 'singlefile' and path == (self.name,):
            return self.metainfo['info']['length']
        elif self.mode == 'multifile':
            file_sizes = []
            for info in self.metainfo['info']['files']:
                this_path = (self.name,) + tuple(info['path'])
                if this_path == path:
                    # path points to file
                    return info['length']
                elif utils.iterable_startswith(this_path, path):
                    # path points to directory
                    file_sizes.append(info['length'])
            if file_sizes:
                return sum(file_sizes)
        raise error.PathNotFoundError(os.path.join(*path))

    @property
    def size(self):
        """
        Total size of content in bytes or ``None`` if :attr:`path` is ``None``
        """
        if self.mode == 'singlefile':
            return self.metainfo['info']['length']
        elif self.mode == 'multifile':
            return sum(fileinfo['length']
                       for fileinfo in self.metainfo['info']['files'])

    @property
    def piece_size(self):
        """
        Piece size/length or ``None``

        If set to ``None`` and :attr:`size` is not ``None``, use the return
        value of :attr:`calculate_piece_size`.

        Setting this property sets ``piece length`` in :attr:`metainfo`\
        ``['info']``.
        """
        if 'piece length' not in self.metainfo['info']:
            if self.size is None:
                return None
            else:
                self.piece_size = None  # Calculate piece size
        return self.metainfo['info']['piece length']
    @piece_size.setter
    def piece_size(self, value):
        if value is None:
            size = self.size
            if not size:
                raise RuntimeError(f'Cannot calculate piece size with no "path" specified')
            else:
                value = self.calculate_piece_size(size)
        try:
            piece_length = int(value)
        except (TypeError, ValueError):
            raise ValueError(f'piece_size must be int, not {value!r}')
        else:
            if not utils.is_power_of_2(piece_length):
                raise error.PieceSizeError(piece_length)
            elif not self.piece_size_min <= piece_length <= self.piece_size_max:
                raise error.PieceSizeError(piece_length,
                                           min=self.piece_size_min,
                                           max=self.piece_size_max)
            self.metainfo['info']['piece length'] = piece_length

    def calculate_piece_size(self, size):
        """
        Return the piece size for a total torrent size of ``size`` bytes

        For torrents up to 1 GiB, the maximum number of pieces is 1000 which
        means the maximum piece size is 1 MiB.  With increasing torrent size,
        both the number of pieces and the maximum piece size are increased.  For
        torrents between 32 and 80 GiB a maximum piece size of 8 MiB is
        maintained by increasing the number of pieces up to 10,000.  For
        torrents larger than 80 GiB the piece size is 16 MiB, using as many
        pieces as necessary.

        You may override this method if you need a different algorithm.

        :raises RuntimeError: if :attr:`size` returns ``None``
        :return: calculated piece size
        """
        if size <= 2**30:          #  1 GiB /  1024 pieces = 1 MiB max
            pieces = size / 1024
        elif size <= 4 * 2**30:    #  4 GiB /  2048 pieces = 2 MiB max
            pieces = size / 2048
        elif size <= 6 * 2**30:    #  6 GiB /  3072 pieces = 2 MiB max
            pieces = size / 3072
        elif size <= 8 * 2**30:    #  8 GiB /  2048 pieces = 4 MiB max
            pieces = size / 2048
        elif size <= 16 * 2**30:   # 16 GiB /  2048 pieces = 8 MiB max
            pieces = size / 2048
        elif size <= 32 * 2**30:   # 32 GiB /  4096 pieces = 8 MiB max
            pieces = size / 4096
        elif size <= 64 * 2**30:   # 64 GiB /  8192 pieces = 8 MiB max
            pieces = size / 8192
        elif size <= 80 * 2**30:   # 80 GiB / 10000 pieces = 8 MiB max
            pieces = size / 10000
        else:
            return 16 * 2**20      # 16 MiB (absolute maximum)
        # Math is magic!
        return max(1 << max(0, math.ceil(math.log(pieces, 2))),
                   self.piece_size_min)

    piece_size_min = 16 * 1024  # 16 KiB
    """
    Smallest allowed piece size

    Setting :attr:`piece_size` to a smaller value raises
    :class:`PieceSizeError`.
    """

    piece_size_max = 16 * 1024*1024  # 16 MiB
    """
    Greatest allowed piece size

    Setting :attr:`piece_size` to a greater value raises
    :class:`PieceSizeError`.
    """

    @property
    def pieces(self):
        """
        Number of pieces the content is split into or ``None`` if :attr:`piece_size`
        returns ``None``
        """
        if self.piece_size is None:
            return None
        else:
            return math.ceil(self.size / self.piece_size)

    @property
    def hashes(self):
        """
        Tuple of SHA1 piece hashes as :class:`bytes` or ``None`` if
        :attr:`metainfo`\ ``['info']``\ ``['pieces']`` isn't a :class:`bytes` or
        :class:`bytearray`.
        """
        hashes = self.metainfo['info'].get('pieces')
        if isinstance(hashes, (bytes, bytearray)):
            # Each hash is 20 bytes long
            return tuple(bytes(hashes[pos:pos+20])
                         for pos in range(0, len(hashes), 20))

    @property
    def name(self):
        """
        Name of the torrent

        Default to last item in :attr:`path` or ``None`` if :attr:`path` is
        ``None``.

        If this property is set to ``None`` and :attr:`path` is not ``None``, it
        is set to the default name, i.e. the last item in :attr:`path`.

        Setting this property sets or removes ``name`` in :attr:`metainfo`\
        ``['info']``.
        """
        if 'name' not in self.metainfo['info'] and self.path is not None:
            self.metainfo['info']['name'] = os.path.basename(self.path)
        return self.metainfo['info'].get('name', None)
    @name.setter
    def name(self, value):
        if value is None:
            self.metainfo['info'].pop('name', None)
            self.name  # Set default name
        else:
            self.metainfo['info']['name'] = str(value)

    @property
    def trackers(self):
        """
        List of tiers (lists) of announce URLs

        This is a smart list that ensures the proper list-of-lists structure,
        validation and deduplication.  You can set this property to a URL, an
        iterable of URLs or an iterable of iterables of URLs (or "tiers").

        This property also automatically sets :attr:`metainfo`\ ``['announce']``
        and :attr:`metainfo`\ ``['announce-list']`` every time it set or
        manipulated according to these rules:

        - If it contains a single URL, :attr:`metainfo`\ ``['announce']`` is set
          and :attr:`metainfo`\ ``['announce-list']`` is removed if it exists.

        - If it contains an iterable of URLs, :attr:`metainfo`\ ``['announce']``
          is set to the first URL and :attr:`metainfo`\ ``['announce-list']`` is
          set to a list of tiers, one tier for each URL.

        - If it contains an iterable of iterables of URLs, :attr:`metainfo`\
          ``['announce']`` is set to the first URL of the first iterable and
          :attr:`metainfo`\ ``['announce-list']`` is set to a list of tiers, one
          tier for each iterable of URLs.

        You can manage :attr:`metainfo`\ ``['announce']`` and :attr:`metainfo`\
        ``['announce-list']`` manually as long as you never touch this property.

        :raises URLError: if any of the announce URLs is invalid
        :raises ValueError: if set to anything that isn't an Iterable and not a
            string
        """
        self._sync_trackers_from_metainfo()
        return self._trackers

    @trackers.setter
    def trackers(self, value):
        # We store the list of tiers in self._trackers because we automatically
        # add/remove "announce" and "announce-list" to/from metainfo whenever it
        # changes (see _trackers_changed callback.)
        if isinstance(value, str):
            self._trackers.replace((value,))
        elif isinstance(value, abc.Iterable):
            # We need to change `value`'s identity, otherwise this clears all
            # trackers: t.trackers += ['http://foo']
            self._trackers.replace(tuple(value))
        elif value is None:
            self._trackers.clear()
        else:
            raise ValueError(f'Must be Iterable, str or None, not {type(value).__name__}: {value}')

    def _sync_trackers_from_metainfo(self):
        # If URLs in metainfo differ from self._trackers, update self._trackers
        self._trackers._callback = None
        announce = self.metainfo.get('announce', None)
        announce_list = self.metainfo.get('announce-list', ())
        trackers = utils.Trackers(*announce_list)
        if announce and announce not in trackers.flat:
            trackers.insert(0, [announce])
        if self._trackers != trackers:
            self._trackers.replace(trackers)
        self._trackers._callback = self._trackers_changed

    def _trackers_changed(self, announce_list):
        # Automatically use first tracker of first tier as "announce"
        try:
            self.metainfo['announce'] = announce_list[0][0]
        except IndexError:
            self.metainfo.pop('announce', None)

        if len(announce_list.flat) <= 1:
            self.metainfo.pop('announce-list', None)
        elif announce_list and announce_list != self.metainfo.get('announce-list'):
            self.metainfo['announce-list'] = announce_list

    @property
    def webseeds(self):
        """
        List of webseed URLs or ``None`` for no webseeds

        http://bittorrent.org/beps/bep_0019.html

        Setting or manipulating this property automatically sets or removes
        ``url-list`` in :attr:`metainfo`. You can manage :attr:`metainfo`\
        ``['url-list']`` yourself if you never touch this property.

        :raises URLError: if any URL is invalid
        :raises ValueError: if set to any non-iterable
        """
        webseeds = self.metainfo.get('url-list', ())
        if not isinstance(webseeds, utils.URLs):
            webseeds = utils.URLs(webseeds, callback=self._webseeds_changed)
        if self._webseeds != webseeds:
            self._webseeds.replace(webseeds)
        return self._webseeds

    @webseeds.setter
    def webseeds(self, value):
        if isinstance(value, str):
            self._webseeds.replace((value,))
        elif isinstance(value, abc.Iterable):
            self._webseeds.replace(tuple(value))
        elif value is None:
            self._webseeds.clear()
        else:
            raise ValueError(f'Must be Iterable, str or None, not {type(value).__name__}: {value}')

    def _webseeds_changed(self, webseeds):
        if webseeds:
            self.metainfo['url-list'] = webseeds
        else:
            self.metainfo.pop('url-list', None)



    @property
    def httpseeds(self):
        """
        List of httpseed URLs or ``None`` for no httpseeds

        http://bittorrent.org/beps/bep_0017.html

        Setting or manipulating this property automatically sets or removes
        ``httpseeds`` in :attr:`metainfo`. You can manage :attr:`metainfo`\
        ``['httpseeds']`` yourself if you never touch this property.

        :raises URLError: if any URL is invalid
        :raises ValueError: if set to any non-iterable
        """
        httpseeds = self.metainfo.get('httpseeds', ())
        if not isinstance(httpseeds, utils.URLs):
            httpseeds = utils.URLs(httpseeds, callback=self._httpseeds_changed)
        if self._httpseeds != httpseeds:
            self._httpseeds.replace(httpseeds)
        return self._httpseeds

    @httpseeds.setter
    def httpseeds(self, value):
        if isinstance(value, str):
            self._httpseeds.replace((value,))
        elif isinstance(value, abc.Iterable):
            self._httpseeds.replace(tuple(value))
        elif value is None:
            self._httpseeds.clear()
        else:
            raise ValueError(f'Must be Iterable, str or None, not {type(value).__name__}: {value}')

    def _httpseeds_changed(self, httpseeds):
        if httpseeds:
            self.metainfo['httpseeds'] = httpseeds
        else:
            self.metainfo.pop('httpseeds', None)

    @property
    def private(self):
        """
        Whether torrent should use trackers exclusively for peer discovery

        Setting this property sets or removes ``private`` in :attr:`metainfo`\
        ``['info']``.  Setting it to ``None`` removes ``private`` from
        :attr:`metainfo`\ ``['info']``.
        """
        return bool(self.metainfo['info'].get('private', False))
    @private.setter
    def private(self, value):
        if value is None:
            self.metainfo['info'].pop('private', None)
        else:
            self.metainfo['info']['private'] = bool(value)

    @property
    def comment(self):
        """
        Comment string or ``None`` for no comment

        Setting this property sets or removes ``comment`` in :attr:`metainfo`.
        """
        return self.metainfo.get('comment', None)
    @comment.setter
    def comment(self, value):
        if value is not None:
            self.metainfo['comment'] = str(value)
        else:
            self.metainfo.pop('comment', None)

    @property
    def creation_date(self):
        """
        :class:`datetime.datetime` instance or ``None`` for no creation date

        :class:`int` and :class:`float` are also allowed and converted with
        :meth:`datetime.datetime.fromtimestamp`.

        Setting this property sets or removes ``creation date`` in
        :attr:`metainfo`.
        """
        return self.metainfo.get('creation date', None)
    @creation_date.setter
    def creation_date(self, value):
        if isinstance(value, (float, int)):
            self.metainfo['creation date'] = datetime.fromtimestamp(value)
        elif isinstance(value, datetime):
            self.metainfo['creation date'] = value
        elif value is None:
            self.metainfo.pop('creation date', None)
        else:
            raise ValueError(f'Must be None, int or datetime object, not {value!r}')

    @property
    def created_by(self):
        """
        Application name or ``None`` for no creator

        Setting this property sets or removes ``created by`` in
        :attr:`metainfo`.
        """
        return self.metainfo.get('created by', None)
    @created_by.setter
    def created_by(self, value):
        if value is not None:
            self.metainfo['created by'] = str(value)
        else:
            self.metainfo.pop('created by', None)

    @property
    def source(self):
        """
        Source string or ``None`` for no source

        Setting this property sets or removes ``source`` in :attr:`metainfo`\ ``['info']``.
        """
        return self.metainfo['info'].get('source', None)
    @source.setter
    def source(self, value):
        if value is not None:
            self.metainfo['info']['source'] = str(value)
        else:
            self.metainfo['info'].pop('source', None)

    @property
    def exclude(self):
        """
        List of file/directory name patterns to exclude

        Every file path is split at the directory separator and each part, from
        base directory to file, is matched against each pattern.

        Matching is done with :func:`fnmatch.fnmatch`, which uses these special
        characters:

        \*
          matches everything

        ?
          matches any single character

        [seq]
          matches any character in seq

        [!seq]
          matches any char not in seq

        :raises PathEmptyError: if all files are excluded
        """
        return self._exclude
    @exclude.setter
    def exclude(self, value):
        if isinstance(value, str):
            value = [value]
        else:
            value = list(value)
        if value != getattr(self, '_exclude', None):
            self._exclude = value
            self.path = self.path  # Re-filter file paths

    @property
    def infohash(self):
        """SHA1 info hash"""
        try:
            # Try to calculate infohash
            self.validate()
            try:
                info = utils.encode_dict(self.metainfo['info'])
            except ValueError as e:
                raise error.MetainfoError(str(e))
            else:
                return hashlib.sha1(bencode.encode(info)).hexdigest()
        except error.MetainfoError as e:
            # If we can't calculate infohash, see if it was explicitly specifed.
            # This is necessary to create a Torrent from a Magnet URI.
            try:
                return self._infohash
            except AttributeError:
                raise e

    @property
    def infohash_base32(self):
        """Base 32 encoded SHA1 info hash"""
        return base64.b32encode(base64.b16decode(self.infohash.upper()))

    @property
    def randomize_infohash(self):
        """
        Whether to ensure that :attr:`infohash` is always different

        This allows cross-seeding without changing :attr:`piece_size` manually.

        Setting this property to ``True`` sets ``entropy`` in
        :attr:`metainfo`\ ``['info']`` to a random integer. Setting it to
        ``False`` removes that value.
        """
        return bool(self.metainfo['info'].get('entropy', False))
    @randomize_infohash.setter
    def randomize_infohash(self, value):
        if value:
            # According to BEP0003 "Integers have no size limitation", but some
            # parsers seem to have problems with large numbers.
            import random
            self.metainfo['info']['entropy'] = random.randint(-2e9, 2e9)
        else:
            self.metainfo['info'].pop('entropy', None)

    @property
    def is_ready(self):
        """Whether this torrent is ready to be exported to a file or magnet link"""
        try:
            self.validate()
        except error.MetainfoError:
            return False
        else:
            return True

    def generate(self, threads=None, callback=None, interval=0):
        """
        Hash pieces and report progress to `callback`

        This method sets ``pieces`` in :attr:`metainfo`\ ``['info']`` if all
        pieces are hashed successfully.

        :param int threads: How many threads to use for hashing pieces or
            ``None`` to use one thread per available CPU core
        :param callable callback: Callable to report progress and/or abort

            `callback` must accept 4 positional arguments:

                1. Torrent instance (:class:`Torrent`)
                2. Path of the currently hashed file (:class:`str`)
                3. Number of hashed pieces (:class:`int`)
                4. Total number of pieces (:class:`int`)

            If `callback` returns anything that is not ``None``, hashing is
            stopped.
        :param float interval: Minimum number of seconds between calls to
            `callback`; if 0, `callback` is called once per hashed piece

        :raises PathEmptyError: if :attr:`path` contains only empty
            files/directories
        :raises PathNotFoundError: if :attr:`path` does not exist
        :raises ReadError: if :attr:`path` or any file beneath it is not
            readable
        :raises RuntimeError: if :attr:`path` is None

        :return: ``True`` if all pieces were successfully hashed, ``False``
            otherwise
        """
        self.metainfo['info']['pieces'] = bytes()

        if self.path is None:
            raise RuntimeError('generate() called with no path specified')
        elif not os.path.exists(self.path):
            raise error.PathNotFoundError(self.path)
        elif utils.real_size(self.path) < 1:
            raise error.PathEmptyError(self.path)

        if callback is not None:
            maybe_cancel = generate.CancelCallback(callback, interval)
        else:
            maybe_cancel = None
        threads = threads or NCORES

        # Read piece_size'd chunks from disk and push them to queue for hashing
        reader = generate.Reader(filepaths=self.filepaths,
                                 piece_size=self.piece_size,
                                 queue_size=threads*3)

        # Pool of workers that pull from reader's piece queue, calculate the
        # hashes, and quickly offload the results to a hash queue
        hasher_threadpool = generate.HashWorkerPool(threads, reader.piece_queue)

        # Pull from the hash queue; also call callback and maybe stop everything
        def collector_callback(filepath, pieces_done, piece_index, piece_hash,
                               maybe_cancel=maybe_cancel, torrent=self, pieces_total=self.pieces):
            if maybe_cancel is not None:
                maybe_cancel(cb_args=(torrent, filepath, pieces_done, pieces_total),
                             # Always call callback after the last piece was hashed
                             force_call=pieces_done >= pieces_total)
        collector_thread = generate.CollectorWorker(hasher_threadpool.hash_queue,
                                                    callback=collector_callback)

        if maybe_cancel:
           maybe_cancel.on_cancel(reader.stop,
                                  hasher_threadpool.stop,
                                  collector_thread.stop)

        try:
            reader.read()
        except BaseException as e:
            hasher_threadpool.stop()
            collector_thread.stop()
            raise
        finally:
            hasher_threadpool.join()
            collector_thread.join()

        # Store generated hashes in metainfo
        hashes_count = len(collector_thread.hashes) / 20
        if hashes_count == self.pieces:
            self.metainfo['info']['pieces'] = collector_thread.hashes
            return True
        elif hashes_count < self.pieces:
            # Hashing was aborted by callback
            return False
        else:
            raise RuntimeError('Unexpected number of hashes generated: '
                               f'{hashes_count} instead of {self.pieces}')

    def _verify_prepare(self, path, callback, interval):
        """Common tasks of :meth:`verify` and :meth:`verify_filesize`"""
        self.validate()

        if callback is not None:
            callback = generate.CancelCallback(callback, interval=interval)
        else:
            # Stop the verification process if there was an exception
            callback = generate.CancelCallback(lambda *cb_args: cb_args[-1])

        # Generate an ordered list of file system paths and their corresponding
        # paths inside the torrent
        paths = []
        for torrent_filepath in self.files:
            torrent_subpath = torrent_filepath.split(os.sep)[1:]
            fs_filepath = os.path.normpath(os.path.join(path, *torrent_subpath))
            paths.append((fs_filepath, torrent_filepath))

        return paths, callback

    def verify_filesize(self, path, callback=None):
        """
        Check if `path` looks like it should contain all the data of this torrent

        Walk through :attr:`files` and check if each file exists in `path`, is
        readable and has the correct size.  Excess files in `path` are ignored.

        This is fast and should find most manipulations, but :meth:`verify` is
        necessary to detect corruptions (e.g. due to bit rot).

        :param str path: Directory or file to check
        :param callable callback: Callable to report progress and/or abort

            `callback` must accept 6 positional arguments:

                1. Torrent instance (:class:`Torrent`)
                2. File path in file system (:class:`str`)
                3. File path in torrent (:class:`str`)
                4. Number of checked files (:class:`int`)
                5. Total number of files (:class:`int`)
                6. Exception (:class:`TorfError`) or ``None``

            If `callback` returns anything that is not ``None``, verification is
            stopped.

        If a callback is specified, exceptions are not raised but passed to
        `callback` instead.

        :raises VerifyFileSizeError: if a file has an unexpected size
        :raises VerifyNotDirectoryError: if `path` is a directory and this
            torrent contains a single file
        :raises PathNotFoundError: if a file doesn't exist
        :raises MetainfoError: if :meth:`validate` fails

        :return: ``True`` if `path` is verified successfully, ``False``
            otherwise
        """
        raise_exceptions = not callback
        filepaths, cancel = self._verify_prepare(path, callback, interval=0)
        files_total = len(filepaths)
        exception = None

        for files_done,(fs_filepath,torrent_filepath) in enumerate(filepaths, start=1):
            # Check if path exists
            if not os.path.exists(fs_filepath):
                exception = error.PathNotFoundError(fs_filepath)
                if cancel(cb_args=(self, fs_filepath, torrent_filepath,
                                   files_done, files_total, exception),
                          force_call=True):
                    break
                else:
                    continue

            # If we expect a file, check if path is a file.  We don't need to
            # check for a directory if we expect one because we are iterating
            # over files (filepaths), so the path "foo/bar/baz" will result in a
            # PathNotFoundError if "foo" or "foo/bar" is a file.
            if self.mode == 'singlefile' and os.path.isdir(path):
                exception = error.VerifyNotDirectoryError(fs_filepath)
                if cancel(cb_args=(self, fs_filepath, torrent_filepath,
                                   files_done, files_total, exception),
                          force_call=True):
                    break
                else:
                    continue

            # Check file size
            fs_filepath_size = os.path.getsize(os.path.realpath(fs_filepath))
            expected_size = self.partial_size(torrent_filepath)
            if fs_filepath_size != expected_size:
                exception = error.VerifyFileSizeError(fs_filepath, fs_filepath_size, expected_size)
                if cancel(cb_args=(self, fs_filepath, torrent_filepath,
                                   files_done, files_total, exception),
                          force_call=True):
                    break
                else:
                    continue

            # Report no error for current file
            if cancel(cb_args=(self, fs_filepath, torrent_filepath, files_done, files_total, None),
                      force_call=True):
                break

        if exception:
            if raise_exceptions:
                raise exception
            else:
                return False
        else:
            return True

    def verify(self, path, skip_file_on_first_error=True, threads=None,
               callback=None, interval=0):
        """
        Check if `path` contains all the data of this torrent

        Generate hashes from the contents of :attr:`files` and compare each
        generated hash to the ones stored in :attr:`metainfo`\ ``['info']``\
        ``['pieces']``.

        :param str path: Directory or file to check
        :param bool skip_file_on_first_error: Whether to stop hashing pieces
            from file if a piece from it is corrupt
        :param int threads: How many threads to use for hashing pieces or
            ``None`` to use one thread per available CPU core
        :param callable callback: Callable to report progress and/or abort

            `callback` must accept 7 positional arguments:

                1. Torrent instance (:class:`Torrent`)
                2. File path in file system (:class:`str`)
                3. Number of checked pieces (:class:`int`)
                4. Total number of pieces (:class:`int`)
                5. Index of the current piece (:class:`int`)
                6. SHA1 hash of the current piece (:class:`bytes`)
                7. Exception (:class:`TorfError`) or ``None``

            If `callback` returns anything that is not ``None``, verification is
            stopped.

        :param float interval: Minimum number of seconds between calls to
            `callback` (if 0, `callback` is called once per piece); this is
            ignored in case of error

        If a callback is specified, exceptions are not raised but passed to
        `callback` instead.

        :raises VerifyContentError: if a file contains unexpected data
        :raises ReadError: if a file is not readable
        :raises MetainfoError: if :meth:`validate` fails

        :return: ``True`` if `path` is verified successfully, ``False``
            otherwise
        """
        raise_exceptions = not callback
        filepaths, maybe_cancel = self._verify_prepare(path, callback, interval=interval)
        fs_filepaths = tuple(x[0] for x in filepaths)

        if self.mode == 'singlefile' and os.path.isdir(os.path.realpath(path)):
            exception = error.VerifyNotDirectoryError(path)
            maybe_cancel(cb_args=(self, fs_filepaths[0], 0, self.pieces, 0, None, exception),
                         force_call=True)
        elif self.mode == 'multifile' and not os.path.isdir(os.path.realpath(path)):
            exception = error.VerifyIsDirectoryError(path)
            maybe_cancel(cb_args=(self, fs_filepaths[0], 0, self.pieces, 0, None, exception),
                         force_call=True)
        else:
            exception = None
            threads = threads or NCORES

            # Read piece_size'd chunks from disk and push them to queue for hashing
            def reader_error_callback(exc, filepath, piece_index, torrent=self,
                                      pieces_done=0, pieces_total=self.pieces,
                                      piece_hash=None):
                nonlocal exception
                exception = exc
                maybe_cancel(cb_args=(torrent, filepath, pieces_done, pieces_total,
                                      piece_index, piece_hash, exception),
                             force_call=True)
            reader = generate.Reader(filepaths=fs_filepaths,
                                     file_sizes={fs_path:self.partial_size(t_path)
                                                 for fs_path,t_path in filepaths},
                                     piece_size=self.piece_size,
                                     queue_size=threads*3,
                                     error_callback=reader_error_callback)
            file_was_skipped=lambda f: f in reader.skipped_files

            # Pool of workers that pull from reader_thread's piece queue, calculate
            # the hashes, and quickly offload the results to a hash queue
            hasher_threadpool = generate.HashWorkerPool(threads, reader.piece_queue,
                                                        file_was_skipped=file_was_skipped)

            # Pull from the hash queue; also call `callback` and maybe stop everything
            def collector_callback(filepath, pieces_done, piece_index, piece_hash,
                                   torrent=self, pieces_total=self.pieces, piece_size=self.piece_size,
                                   exp_hashes=self.hashes,
                                   exp_filesizes={fs_path : self.partial_size(t_path)
                                                  for fs_path,t_path in filepaths},
                                   files_size_checked=[]):
                nonlocal exception

                # Verify piece hash first
                if piece_hash != exp_hashes[piece_index]:
                    if skip_file_on_first_error:
                        reader.skip_file(filepath)
                    files = tuple((fs_path, self.partial_size(t_path))
                                  for fs_path,t_path in filepaths)
                    exception = error.VerifyContentError(piece_index, piece_size, files)
                    maybe_cancel(cb_args=(torrent, filepath, pieces_done, pieces_total,
                                          piece_index, piece_hash, exception),
                                 force_call=True)
                    return

                # `filepath` could be OK but have surplus bytes at the end.  The
                # reader won't read those extra bytes because it maintains piece
                # offsets as defined in the metainfo.
                if filepath not in files_size_checked:
                    files_size_checked.append(filepath)
                    if os.path.getsize(filepath) > exp_filesizes[filepath]:
                        exception = error.VerifyFileSizeError(filepath,
                                                              os.path.getsize(filepath),
                                                              exp_filesizes[filepath])
                        maybe_cancel(cb_args=(torrent, filepath, pieces_done, pieces_total,
                                              piece_index, piece_hash, exception),
                                     force_call=True)
                        if skip_file_on_first_error:
                            reader.skip_file(filepath)
                        return

                # No error, but report progress
                maybe_cancel(cb_args=(torrent, filepath, pieces_done, pieces_total,
                                      piece_index, piece_hash, None),
                             # Always call callback after the last piece was hashed
                             force_call=pieces_done >= pieces_total)
            collector_thread = generate.CollectorWorker(hasher_threadpool.hash_queue,
                                                        callback=collector_callback,
                                                        file_was_skipped=file_was_skipped)
            maybe_cancel.on_cancel(reader.stop,
                                   hasher_threadpool.stop,
                                   collector_thread.stop)
            try:
                reader.read()
            except BaseException as e:
                hasher_threadpool.stop()
                collector_thread.stop()
                raise
            finally:
                hasher_threadpool.join()
                collector_thread.join()
                if collector_thread.exception is not None:
                    exception = collector_thread.exception

        # Raise exception unless the callback function already handled it
        if exception:
            if raise_exceptions:
                raise exception
            else:
                return False
        else:
            return True

    def validate(self):
        """
        Check if all mandatory keys exist in :attr:`metainfo` and all standard keys
        have correct types

        The necessary values are documented here:
            | http://bittorrent.org/beps/bep_0003.html
            | https://wiki.theory.org/index.php/BitTorrentSpecification#Metainfo_File_Structure

        :raises MetainfoError: if :attr:`metainfo` would not generate a valid
            torrent file or magnet link
        """
        md = self.metainfo
        info = md['info']

        # Check values shared by singlefile and multifile torrents
        utils.assert_type(md, ('info', 'name'), (str,), must_exist=True)
        utils.assert_type(md, ('info', 'piece length'), (int,), must_exist=True)
        utils.assert_type(md, ('info', 'pieces'), (abc.ByteString,), must_exist=True)
        utils.assert_type(md, ('announce',), (str,), must_exist=False, check=utils.is_url)
        utils.assert_type(md, ('announce-list',), (utils.Iterable,), must_exist=False)
        for i,_ in enumerate(md.get('announce-list', ())):
            utils.assert_type(md, ('announce-list', i), (utils.Iterable,))
            for j,_ in enumerate(md['announce-list'][i]):
                utils.assert_type(md, ('announce-list', i, j), (str,), check=utils.is_url)

        if len(info['pieces']) == 0:
            raise error.MetainfoError("['info']['pieces'] is empty")

        elif len(info['pieces']) % 20 != 0:
            raise error.MetainfoError("length of ['info']['pieces'] is not divisible by 20")

        elif info.get('private') and not md.get('announce') and not md.get('announce-list'):
            raise error.MetainfoError("['info']['private'] is True but no announce URLs are specified")

        elif 'length' in info and 'files' in info:
            raise error.MetainfoError("['info'] includes both 'length' and 'files'")

        elif 'length' in info:
            # Validate info as singlefile torrent
            utils.assert_type(md, ('info', 'length'), (int, float), must_exist=True)
            utils.assert_type(md, ('info', 'md5sum'), (str,), must_exist=False, check=utils.is_md5sum)

            # Validate expected number of pieces
            piece_count = int(len(info['pieces']) / 20)
            exp_piece_count = math.ceil(info['length'] / info['piece length'])
            if piece_count != exp_piece_count:
                raise error.MetainfoError(f'Expected {exp_piece_count} pieces but there are {piece_count}')

            if self.path is not None:
                # Check if filepath actually points to a file
                if not os.path.isfile(self.path):
                    raise error.MetainfoError(f"Metainfo includes {self.path} as file, but it is not a file")

                # Check if size matches
                if os.path.getsize(self.path) != info['length']:
                    raise error.MetainfoError(f"Mismatching file sizes in metainfo ({info['length']})"
                                              f" and file system ({os.path.getsize(self.path)}): "
                                              f"{self.path!r}")

        elif 'files' in info:
            # Validate info as multifile torrent
            utils.assert_type(md, ('info', 'files'), (utils.Iterable,), must_exist=True)
            for i,fileinfo in enumerate(info['files']):
                utils.assert_type(md, ('info', 'files', i), (abc.Mapping,), must_exist=True)
                utils.assert_type(md, ('info', 'files', i, 'length'), (int, float), must_exist=True)
                utils.assert_type(md, ('info', 'files', i, 'path'), (utils.Iterable,), must_exist=True)
                utils.assert_type(md, ('info', 'files', i, 'md5sum'), (str,), must_exist=False, check=utils.is_md5sum)
                for j,item in enumerate(fileinfo['path']):
                    utils.assert_type(md, ('info', 'files', i, 'path', j), (str,))

            # - validate() should ensure that ['info']['pieces'] is math.ceil(self.size /
            #   self.piece_size) bytes long.
            piece_count = int(len(info['pieces']) / 20)
            exp_piece_count = math.ceil(sum(fileinfo['length'] for fileinfo in info['files'])
                                        / info['piece length'])
            if piece_count != exp_piece_count:
                raise error.MetainfoError(f'Expected {exp_piece_count} pieces but there are {piece_count}')

            if self.path is not None:
                # Check if filepath actually points to a directory
                if not os.path.isdir(self.path):
                    raise error.MetainfoError(f"Metainfo includes {self.path} as directory, but it is not a directory")

                for i,fileinfo in enumerate(info['files']):
                    filepath = os.path.join(self.path, os.path.join(*fileinfo['path']))

                    # Check if filepath exists and is a file
                    if not os.path.exists(filepath):
                        raise error.MetainfoError(f"Metainfo includes file that doesn't exist: {filepath!r}")
                    if not os.path.isfile(filepath):
                        raise error.MetainfoError(f"Metainfo includes file that isn't a file: {filepath!r}")

                    # Check if sizes match
                    if os.path.getsize(filepath) != fileinfo['length']:
                        raise error.MetainfoError(f"Mismatching file sizes in metainfo ({fileinfo['length']})"
                                                  f" and file system ({os.path.getsize(filepath)}): "
                                                  f"{filepath!r}")

        else:
            raise error.MetainfoError("Missing 'length' or 'files' in metainfo")

    def convert(self):
        """
        Return :attr:`metainfo` with all keys encoded to :class:`bytes` and all
        values encoded to :class:`bytes`, :class:`int`, :class:`list` or
        :class:`OrderedDict`

        :raises MetainfoError: on values that cannot be converted properly
        """
        try:
            return utils.encode_dict(self.metainfo)
        except ValueError as e:
            raise error.MetainfoError(str(e))

    def dump(self, validate=True):
        """
        Create bencoded :attr:`metainfo` (i.e. the content of a torrent file)

        :param bool validate: Whether to run :meth:`validate` first

        :return: :attr:`metainfo` as bencoded :class:`bytes`
        """
        if validate:
            self.validate()
        return bencode.encode(self.convert())

    def write_stream(self, stream, validate=True):
        """
        Write :attr:`metainfo` to a file-like object

        Before any data is written, `stream` is truncated if possible.

        :param stream: Writable file-like object (e.g. :class:`io.BytesIO`)
        :param bool validate: Whether to run :meth:`validate` first

        :raises WriteError: if writing to `stream` fails
        :raises MetainfoError: if `validate` is `True` and :attr:`metainfo`
            contains invalid data
        """
        content = self.dump(validate=validate)
        try:
            # Remove existing data from stream *after* dump() didn't raise
            # anything so we don't destroy it prematurely.
            if stream.seekable():
                stream.seek(0)
                stream.truncate(0)
            stream.write(content)
        except OSError as e:
            raise error.WriteError(e.errno)

    def write(self, filepath, validate=True, overwrite=False, mode=0o666):
        """
        Write :attr:`metainfo` to torrent file

        This method is essentially equivalent to:

        >>> with open('my.torrent', 'wb') as f:
        ...     f.write_stream(torrent.dump())

        :param filepath: Path of the torrent file
        :param bool validate: Whether to run :meth:`validate` first
        :param bool overwrite: Whether to silently overwrite `filepath` (only
            if all pieces were hashed successfully)
        :param mode: File permissions of `filepath`

        :raises WriteError: if writing to `filepath` fails
        :raises MetainfoError: if `validate` is `True` and :attr:`metainfo`
            contains invalid data
        """
        if not overwrite and os.path.exists(filepath):
            raise error.WriteError(errno.EEXIST, filepath)

        # Get file content before opening the file in case there are errors like
        # incomplete metainfo
        content = io.BytesIO()
        self.write_stream(content, validate=validate)
        content.seek(0)
        try:
            with open(filepath, 'wb') as f:
                f.write(content.read())
        except OSError as e:
            raise error.WriteError(e.errno, filepath)

    def magnet(self, name=True, size=True, trackers=True, tracker=False, validate=True):
        """
        BTIH magnet URI

        :param bool name: Whether to include the name
        :param bool size: Whether to include the size
        :param bool trackers: Whether to include all trackers
        :param bool tracker: Whether to include only the first tracker of the
            first tier (overrides `trackers`)
        :param bool validate: Whether to run :meth:`validate` first

        :raises MetainfoError: if `validate` is `True` and :attr:`metainfo`
            contains invalid data
        """
        if validate:
            self.validate()

        kwargs = {'xt': 'urn:btih:' + self.infohash}
        if name:
            kwargs['dn'] = self.name
        if size:
            kwargs['xl'] = self.size

        if tracker:
            if 'announce' in self.metainfo:
                kwargs['tr'] = (self.metainfo['announce'],)
        elif trackers:
            if 'announce-list' in self.metainfo:
                kwargs['tr'] = (url
                                for tier in self.metainfo['announce-list']
                                for url in tier)
        if self.webseeds is not None:
            kwargs['ws'] = self.webseeds

        # Prevent circular import issues
        from ._magnet import Magnet
        return Magnet(**kwargs)

    # Maximum number of bytes that read() reads from torrent files.  This limit
    # exists because we don't want to read gigabytes before raising an error.
    MAX_TORRENT_FILE_SIZE = int(10e6)  # 10MB

    @classmethod
    def read_stream(cls, stream, validate=True):
        """
        Read torrent metainfo from file-like object

        :param stream: Readable file-like object (e.g. :class:`io.BytesIO`)
        :param bool validate: Whether to run :meth:`validate` on the new Torrent
            object

        :raises ReadError: if reading from `stream` fails
        :raises BdecodeError: if `stream` does not produce a valid bencoded byte
            sequence
        :raises MetainfoError: if `validate` is `True` and the read metainfo is
            invalid

        :return: New Torrent object
        """
        try:
            content = stream.read(cls.MAX_TORRENT_FILE_SIZE)
        except OSError as e:
            raise error.ReadError(e.errno)
        else:
            try:
                metainfo_enc = bencode.decode(content)
            except bencode.DecodingError as e:
                raise error.BdecodeError()

            if validate:
                if b'info' not in metainfo_enc:
                    raise error.MetainfoError("Missing 'info'")
                elif not isinstance(metainfo_enc[b'info'], abc.Mapping):
                    raise error.MetainfoError("'info' is not a dictionary")
                elif b'pieces' not in metainfo_enc[b'info']:
                    raise error.MetainfoError("Missing 'pieces' in ['info']")

            # Extract 'pieces' from metainfo because it's the only byte sequence
            # that isn't supposed to be decoded to unicode.
            if b'info' in metainfo_enc and b'pieces' in metainfo_enc[b'info']:
                pieces = metainfo_enc[b'info'].pop(b'pieces')
                metainfo = utils.decode_dict(metainfo_enc)
                metainfo['info']['pieces'] = pieces
            else:
                metainfo = utils.decode_dict(metainfo_enc)

            torrent = cls()
            torrent._metainfo = metainfo

            # Convert some values from official types to something nicer
            # (e.g. int -> datetime)
            for attr in ('creation_date', 'private'):
                setattr(torrent, attr, getattr(torrent, attr))

            if validate:
                torrent.validate()

            return torrent

    @classmethod
    def read(cls, filepath, validate=True):
        """
        Read torrent metainfo from file

        This method is essentially equivalent to:

        >>> with open('my.torrent', 'rb') as f:
        ...     torrent = Torrent.read_stream(f)

        :param filepath: Path of the torrent file
        :param bool validate: Whether to run :meth:`validate` on the new Torrent
            object

        :raises ReadError: if reading from `filepath` fails
        :raises BdecodeError: if `filepath` does not contain a valid bencoded byte
            sequence
        :raises MetainfoError: if `validate` is `True` and the read metainfo is
            invalid

        :return: New Torrent object
        """
        try:
            with open(filepath, 'rb') as f:
                return cls.read_stream(f)
        except (OSError, error.ReadError) as e:
            raise error.ReadError(e.errno, filepath)
        except error.BdecodeError:
            raise error.BdecodeError(filepath)

    def copy(self):
        """
        Return a new object with the same metainfo

        Internally, this simply copies the internal metainfo dictionary with
        :func:`copy.deepcopy` and gives it to the new instance.
        """
        from copy import deepcopy
        cp = type(self)()
        cp._metainfo = deepcopy(self._metainfo)
        return cp

    def __repr__(self):
        sig = inspect.signature(self.__init__)
        args = []
        for param in sig.parameters.values():
            value = getattr(self, param.name)
            default = param.default
            if default is param.empty:
                args.append(repr(value))
            elif value and default != value:
                args.append(f'{param.name}={value!r}')
        return type(self).__name__ + '(' + ', '.join(args) + ')'

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self._metainfo == other._metainfo
        else:
            return NotImplemented

    def __hash__(self, other):
        return hash(tuple(sorted(self._metainfo.items())))
