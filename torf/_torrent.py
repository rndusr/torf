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

import base64
import errno
import hashlib
import inspect
import io
import itertools
import math
import os
import pathlib
import re
from collections import abc
from datetime import datetime

import flatbencode as bencode

from . import __version__
from . import _errors as error
from . import _generate as generate
from . import _utils as utils

_PACKAGE_NAME = __name__.split('.')[0]

# os.sched_getaffinity() is only available on some Unix platforms.
try:
    NCORES = len(os.sched_getaffinity(0))
except AttributeError:
    import multiprocessing
    NCORES = multiprocessing.cpu_count()

DEFAULT_TORRENT_NAME = 'UNNAMED TORRENT'

class Torrent():
    """
    Torrent metainfo representation

    Create a new Torrent instance:

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

    >>> def cb(torrent, filepath, pieces_done, pieces_total):
    ...     print(f'{pieces_done/pieces_total*100:3.0f} % done')
    >>> success = torrent.generate(callback=cb, interval=1)
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
    >>> t.metainfo['info']['more stuff']
    {'bar': ['x', 'y', 'z'], 'foo': 12}
    """

    def __init__(self, path=None, name=None,
                 exclude_globs=(), exclude_regexs=(),
                 include_globs=(), include_regexs=(),
                 trackers=None, webseeds=None, httpseeds=None,
                 private=None, comment=None, source=None, creation_date=None,
                 created_by='%s %s' % (_PACKAGE_NAME, __version__),
                 piece_size=None, randomize_infohash=False):
        self._path = None
        self._metainfo = {}
        self._exclude = {'globs'  : utils.MonitoredList(callback=self._filters_changed, type=str),
                         'regexs' : utils.MonitoredList(callback=self._filters_changed, type=re.compile)}
        self._include = {'globs'  : utils.MonitoredList(callback=self._filters_changed, type=str),
                         'regexs' : utils.MonitoredList(callback=self._filters_changed, type=re.compile)}
        self.trackers = trackers
        self.webseeds = webseeds
        self.httpseeds = httpseeds
        self.private = private
        self.comment = comment
        self.creation_date = creation_date
        self.created_by = created_by
        self.source = source
        self.randomize_infohash = randomize_infohash
        self.exclude_globs = exclude_globs
        self.exclude_regexs = exclude_regexs
        self.include_globs = include_globs
        self.include_regexs = include_regexs
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

        Files are filtered according to :attr:`exclude_globs`,
        :attr:`exclude_regexs`, :attr:`include_globs` and
        :attr:`include_regexs`.

        Setting or manipulating this property updates
        :attr:`metainfo`\\ ``['info']``:

        - ``name``, ``piece length`` and ``files`` or ``length`` are set.
        - ``pieces`` and ``md5sum`` are removed if they exist.

        :raises ReadError: if :attr:`path` or any path underneath it is not
            readable
        """
        return self._path

    @path.setter
    def path(self, value):
        if value is None:
            # Keep info about name and files, but forget where they are stored
            self._path = None
            self.metainfo['info'].pop('pieces', None)
        else:
            basepath = pathlib.Path(str(value))
            filepaths = tuple(utils.File(fp, size=utils.real_size(fp))
                              for fp in utils.list_files(basepath))
            self._set_files(filepaths, basepath)

    @property
    def files(self):
        """
        List of relative paths in this torrent

        Paths are :class:`File` objects and items are automatically
        deduplicated.  Every path starts with :attr:`name`.

        Setting or manipulating this property updates
        :attr:`metainfo`\\ ``['info']``:

        - ``name``, ``piece length`` and ``files`` or ``length`` are set.
        - ``pieces`` and ``md5sum`` are removed if they exist.

        See :attr:`filepaths` for a list of file system paths.

        :raises PathError: if any path is absolute
        :raises CommonPathError: if not all files share a common parent
            directory
        :raises ValueError: if any file is not a :class:`File` object
        """
        info = self.metainfo['info']
        if self.mode == 'singlefile':
            files = (utils.File(info.get('name', DEFAULT_TORRENT_NAME), size=self.size),)
        elif self.mode == 'multifile':
            basedir = info.get('name', DEFAULT_TORRENT_NAME)
            files = (utils.File(os.path.join(basedir, *fileinfo['path']),
                                size=fileinfo['length'])
                     for fileinfo in info['files'])
        else:
            files = ()
        return utils.Files(files, callback=self._files_changed)

    def _files_changed(self, files):
        self.files = files

    @files.setter
    def files(self, files):
        if not isinstance(files, utils.Iterable):
            raise ValueError(f'Not an Iterable: {files}')
        for f in files:
            if not isinstance(f, utils.File):
                raise ValueError(f'Not a File object: {f}')
            elif f.is_absolute():
                raise error.PathError(f, msg='Not a relative path')

        if not files:
            self._set_files(files=())
        else:
            # os.path.commonpath() returns '' if there is no common path and
            # raises ValueError if there are absolute and relative paths.
            try:
                basepath = os.path.commonpath(files)
            except ValueError:
                basepath = ''
            if basepath == '':
                raise error.CommonPathError(files)
            self._set_files(files, pathlib.Path(basepath))

    @property
    def filepaths(self):
        """
        List of paths of existing files in :attr:`path` included in the torrent

        Paths are :class:`Filepath` objects and items are automatically
        deduplicated.  Directories are resolved into a list of files.

        Setting or manipulating this property updates
        :attr:`metainfo`\\ ``['info']``:

        - ``name``, ``piece length`` and ``files`` or ``length`` are set.
        - ``pieces`` and ``md5sum`` are removed if they exist.

        :raises ReadError: if any file path is not readable
        """
        filepaths = ()
        if self.path is not None:
            if self.mode == 'singlefile':
                filepaths = (self.path,)
            elif self.mode == 'multifile':
                dirpath = self.path
                filepaths = (os.path.join(dirpath, *fileinfo['path'])
                             for fileinfo in self.metainfo['info']['files'])
        return utils.Filepaths(filepaths, callback=self._filepaths_changed)

    def _filepaths_changed(self, filepaths):
        self.filepaths = filepaths

    @filepaths.setter
    def filepaths(self, filepaths):
        if not isinstance(filepaths, utils.Iterable):
            raise ValueError(f'Not an Iterable: {filepaths}')

        filepaths = utils.Filepaths(filepaths)  # Resolve directories
        if not filepaths:
            self._set_files(files=())
        else:
            # Make all paths absolute so we can find the common path.  Do not
            # resolve symlinks so the user isn't confronted with unexpected
            # paths in case of an error.
            cwd = pathlib.Path.cwd()
            filepaths_abs = tuple(fp if fp.is_absolute() else cwd / fp
                                  for fp in filepaths)
            try:
                basepath = pathlib.Path(os.path.commonpath(filepaths_abs))
            except ValueError:
                raise error.CommonPathError(filepaths)
            filepaths = tuple(utils.File(fp, size=utils.real_size(fp))
                              for fp in filepaths)
            self._set_files(filepaths, basepath)

    def _set_files(self, files, basepath=None):
        """
        Update ``name`` and ``files`` or ``length``, remove ``pieces`` and
        ``md5sum`` in :attr:`metainfo`\\ ``['info']``

        :param files: Sequence of :class:`File`
        :param basepath: path-like that all paths in `files` start with; may be
            ``None`` if ``files`` is empty
        """
        def abspath(p):
            # Absolute path without resolved symlinks
            if p.is_absolute():
                return pathlib.Path(os.path.normpath(p))
            else:
                return pathlib.Path.cwd() / os.path.normpath(p)

        def relpath_without_parent(p):
            # Relative path without common parent directory
            return pathlib.Path(abspath(p)).relative_to(abspath(basepath))

        def relpath_with_parent(p):
            # Relative path with common parent directory
            return pathlib.Path(abspath(p)).relative_to(abspath(basepath).parent)

        # Apply filters to relative paths with torrent name as first segment
        exclude_globs = tuple(str(g) for g in self._exclude['globs'])
        exclude_regexs = tuple(re.compile(r) for r in self._exclude['regexs'])
        exclude = tuple(itertools.chain(exclude_globs, exclude_regexs))
        include_globs = tuple(str(g) for g in self._include['globs'])
        include_regexs = tuple(re.compile(r) for r in self._include['regexs'])
        include = tuple(itertools.chain(include_globs, include_regexs))
        files = utils.filter_files(files, getter=relpath_with_parent,
                                   exclude=exclude, include=include,
                                   hidden=False, empty=False)

        info = self.metainfo['info']
        if not files or all(f.size <= 0 for f in files):
            info.pop('files', None)
            info.pop('length', None)
            info.pop('pieces', None)
            info.pop('md5sum', None)
        elif len(files) == 1 and files[0] == basepath:
            # There is only one file and it is not in a directory.
            # NOTE: A directory with a single file in it is a multifile torrent.
            info['length'] = files[0].size
            info['name'] = files[0].name
            info.pop('files', None)
            info.pop('pieces', None)
            info.pop('md5sum', None)
        else:
            if str(basepath) == os.curdir:
                # Name of current working directory
                name = pathlib.Path.cwd().name
            elif str(basepath) == os.pardir:
                # Name of logical parent directory
                # NOTE: Path.resolve() returns the physical parent directory; if
                # the parent directory is a symlink, we get an unexpected name
                name = os.path.basename(os.path.dirname(os.getcwd()))
            elif str(basepath).endswith(os.curdir) or str(basepath).endswith(os.pardir):
                # Name of current/parent directory (logical parent, see NOTE above)
                name = pathlib.Path(os.path.normpath(basepath)).name
            else:
                name = basepath.name

            files_info = []
            for f in sorted(files):
                files_info.append({'length': f.size,
                                   'path'  : list(relpath_without_parent(f).parts)})
            info['name'] = name
            info['files'] = files_info
            info.pop('length', None)
            info.pop('pieces', None)
            info.pop('md5sum', None)

        # Set new path attribute if basepath exists
        if basepath is not None and os.path.exists(basepath):
            self._path = basepath
        else:
            self._path = None

        # Calculate new piece size
        self.piece_size = None

    @property
    def exclude_globs(self):
        """
        List of case-insensitive wildcard patterns to exclude

        Include patterns take precedence over exclude patterns to allow
        including files that match an exclude pattern.

        Patterns are matched against paths in :attr:`files`.

        ======== ============================
        Wildcard Description
        ======== ============================
        \\*      matches everything
        ?        matches any single character
        [SEQ]    matches any character in SEQ
        [!SEQ]   matches any char not in SEQ
        ======== ============================
        """
        return self._exclude['globs']

    @exclude_globs.setter
    def exclude_globs(self, value):
        if not isinstance(value, utils.Iterable):
            raise ValueError(f'Must be Iterable, not {type(value).__name__}: {value}')
        self._exclude['globs'][:] = value

    @property
    def include_globs(self):
        """
        List of case-insensitive wildcard patterns to include

        See :attr:`exclude_globs`.
        """
        return self._include['globs']

    @include_globs.setter
    def include_globs(self, value):
        if not isinstance(value, utils.Iterable):
            raise ValueError(f'Must be Iterable, not {type(value).__name__}: {value}')
        self._include['globs'][:] = value

    @property
    def exclude_regexs(self):
        """
        List of regular expression patterns to exclude

        Include patterns take precedence over exclude patterns to allow
        including files that match an exclude pattern.

        Patterns are matched against paths in :attr:`files`.

        :raises re.error: if any regular expression is invalid
        """
        return self._exclude['regexs']

    @exclude_regexs.setter
    def exclude_regexs(self, value):
        if not isinstance(value, utils.Iterable):
            raise ValueError(f'Must be Iterable, not {type(value).__name__}: {value}')
        self._exclude['regexs'][:] = value

    @property
    def include_regexs(self):
        """
        List of regular expression patterns to include

        See :attr:`exclude_regexs`.
        """
        return self._include['regexs']

    @include_regexs.setter
    def include_regexs(self, value):
        if not isinstance(value, utils.Iterable):
            raise ValueError(f'Must be Iterable, not {type(value).__name__}: {value}')
        self._include['regexs'][:] = value

    def _filters_changed(self, _):
        """Callback for MonitoredLists in Torrent._exclude"""
        # Apply filters
        if self.path is not None:
            # Read file list from disk again
            self.path = self.path
        else:
            # There are no existing files specified so we can just remove files
            self.files = self.files

    @property
    def filetree(self):
        """
        :attr:`files` as a dictionary tree

        Parent nodes are dictionaries and leaf nodes are :class:`File` objects.
        The top node is always a dictionary with the single key :attr:`name`.

        Example:

        .. code:: python

            {'Torrent': {'bar': {'baz.mp3': File('Torrent/bar/baz.mp3',
                                                 size=543210),
                                 'baz.pdf': File('Torrent/bar/baz.pdf',
                                                 size=999)},
                         'foo.txt': File('Torrent/foo.txt',
                                         size=123456)}}
        """
        tree = {}   # Complete directory tree
        paths = (tuple(f.parts) for f in self.files)
        for path in paths:
            dirpath = path[:-1]  # Path without filename
            filename = path[-1]
            subtree = tree
            for item in dirpath:
                if item not in subtree:
                    subtree[item] = {}
                subtree = subtree[item]
            subtree[filename] = utils.File(path, size=self.partial_size(path))
        return tree

    @property
    def name(self):
        """
        Name of the torrent

        Default to last item in :attr:`path` or ``None`` if :attr:`path` is
        ``None``.

        If this property is set to ``None`` and :attr:`path` is not ``None``, it
        is set to the default.

        Setting this property sets or removes ``name`` in
        :attr:`metainfo`\\ ``['info']``.
        """
        if 'name' not in self.metainfo['info'] and self.path is not None:
            self.metainfo['info']['name'] = self.path.name
        return self.metainfo['info'].get('name', None)

    @name.setter
    def name(self, value):
        if value is None:
            self.metainfo['info'].pop('name', None)
            self.name  # Set default name
        else:
            self.metainfo['info']['name'] = str(value)

    @property
    def mode(self):
        """
        ``singlefile`` if this torrent contains one file that is not in a directory,
        ``multifile`` if it contains one or more files in a directory, or
        ``None`` if no content is specified (i.e. :attr:`files` is empty).
        """
        if 'length' in self.metainfo['info']:
            return 'singlefile'
        elif 'files' in self.metainfo['info']:
            return 'multifile'

    @property
    def size(self):
        """Total size of content in bytes"""
        if self.mode == 'singlefile':
            return self.metainfo['info']['length']
        elif self.mode == 'multifile':
            return sum(fileinfo['length']
                       for fileinfo in self.metainfo['info']['files'])
        else:
            return 0

    def partial_size(self, path):
        """
        Return size of one or more files as specified in :attr:`metainfo`

        :param path: Relative path within torrent, starting with :attr:`name`;
                     may point to file or directory
        :type path: str, path-like or iterable

        :raises PathError: if `path` is not known
        """
        if isinstance(path, str):
            path = tuple(path.split(os.sep))
        elif isinstance(path, os.PathLike):
            path = tuple(path.parts)
        elif isinstance(path, abc.Iterable):
            path = tuple(str(part) for part in path)
        else:
            raise ValueError(f'Must be str, Path or Iterable, not {type(path).__name__}: {path}')
        if self.mode == 'singlefile' and path == (self.name,):
            return self.metainfo['info']['length']
        elif self.mode == 'multifile':
            file_sizes = []
            for info in self.metainfo['info']['files']:
                this_path = (self.name,) + tuple(c for c in info['path'] if c)
                if this_path == path:
                    # path points to file
                    return info['length']
                elif utils.iterable_startswith(this_path, path):
                    # path points to directory
                    file_sizes.append(info['length'])
            if file_sizes:
                return sum(file_sizes)
        raise error.PathError(os.path.join(*path), msg='Unknown path')

    @property
    def piece_size(self):
        """
        Length of each piece in bytes

        If set to ``None`` and :attr:`size` is larger than 0, use the return
        value of :attr:`calculate_piece_size`.

        Setting this property sets or removes ``piece length`` in
        :attr:`metainfo`\\ ``['info']``.
        """
        if 'piece length' not in self.metainfo['info']:
            self.piece_size = None  # Calculate piece size
        return self.metainfo['info'].get('piece length', 0)

    @piece_size.setter
    def piece_size(self, value):
        if value is None:
            if self.size <= 0:
                self.metainfo['info'].pop('piece length', None)
                return
            else:
                value = self.calculate_piece_size(self.size)
        try:
            piece_length = int(value)
        except (TypeError, ValueError):
            raise ValueError(f'piece_size must be int, not {type(value).__name__}: {value!r}')
        else:
            if not utils.is_power_of_2(piece_length):
                raise error.PieceSizeError(piece_length)
            elif not self.piece_size_min <= piece_length <= self.piece_size_max:
                raise error.PieceSizeError(piece_length,
                                           min=self.piece_size_min,
                                           max=self.piece_size_max)
            self.metainfo['info']['piece length'] = piece_length

    @classmethod
    def calculate_piece_size(cls, size):
        """
        Return the piece size for a total torrent size of ``size`` bytes

        For torrents up to 1 GiB, the maximum number of pieces is 1024 which
        means the maximum piece size is 1 MiB.  With increasing torrent size
        both the number of pieces and the maximum piece size are gradually
        increased up to 10,240 pieces of 8 MiB.  For torrents larger than 80 GiB
        the piece size is :attr:`piece_size_max` with as many pieces as
        necessary.

        It is safe to override this method to implement a custom algorithm.

        :return: calculated piece size
        """
        if size <= 2**30:          # 1 GiB / 1024 pieces = 1 MiB max
            pieces = size / 1024
        elif size <= 4 * 2**30:    # 4 GiB / 2048 pieces = 2 MiB max
            pieces = size / 2048
        elif size <= 6 * 2**30:    # 6 GiB / 3072 pieces = 2 MiB max
            pieces = size / 3072
        elif size <= 8 * 2**30:    # 8 GiB / 2048 pieces = 4 MiB max
            pieces = size / 2048
        elif size <= 16 * 2**30:   # 16 GiB / 2048 pieces = 8 MiB max
            pieces = size / 2048
        elif size <= 32 * 2**30:   # 32 GiB / 4096 pieces = 8 MiB max
            pieces = size / 4096
        elif size <= 64 * 2**30:   # 64 GiB / 8192 pieces = 8 MiB max
            pieces = size / 8192
        else:
            pieces = size / 10240
        # Math is magic!
        return int(min(max(1 << max(0, math.ceil(math.log(pieces, 2))),
                           cls.piece_size_min),
                       cls.piece_size_max))

    piece_size_min = 16 * 1024  # 16 KiB
    """
    Smallest allowed piece size

    Setting :attr:`piece_size` to a smaller value raises
    :class:`PieceSizeError`.
    """

    piece_size_max = 16 * 1024 * 1024  # 16 MiB
    """
    Greatest allowed piece size

    Setting :attr:`piece_size` to a greater value raises
    :class:`PieceSizeError`.
    """

    @property
    def pieces(self):
        """
        Number of pieces the content is split into
        """
        if self.size > 0:
            return math.ceil(self.size / self.piece_size)
        else:
            return 0

    @property
    def hashes(self):
        """Tuple of SHA1 piece hashes as :class:`bytes`"""
        hashes = self.metainfo['info'].get('pieces')
        if isinstance(hashes, (bytes, bytearray)):
            # Each hash is 20 bytes long
            return tuple(bytes(hashes[pos : pos + 20])
                         for pos in range(0, len(hashes), 20))
        else:
            return ()

    @property
    def trackers(self):
        """
        List of tiers (lists) of announce URLs

        http://bittorrent.org/beps/bep_0012.html

        This is a smart list that ensures the proper list-of-lists structure,
        validation and deduplication.  You can set this property to a URL, an
        iterable of URLs or an iterable of iterables of URLs (i.e. "tiers").

        This property automatically sets :attr:`metainfo`\\ ``['announce']`` and
        :attr:`metainfo`\\ ``['announce-list']`` when it is manipulated or set
        according to these rules:

        - If it contains a single URL, :attr:`metainfo`\\ ``['announce']`` is set
          and :attr:`metainfo`\\ ``['announce-list']`` is removed if it exists.

        - If it contains an iterable of URLs, :attr:`metainfo`\\ ``['announce']``
          is set to the first URL and :attr:`metainfo`\\ ``['announce-list']`` is
          set to a list of tiers, one tier for each URL.

        - If it contains an iterable of iterables of URLs,
          :attr:`metainfo`\\ ``['announce']`` is set to the first URL of the first
          iterable and :attr:`metainfo`\\ ``['announce-list']`` is set to a list
          of tiers, one tier for each iterable of URLs.

        :raises URLError: if any of the announce URLs is invalid
        :raises ValueError: if set to anything that isn't an iterable or a
            string
        """
        tiers = list(self.metainfo.get('announce-list', ()))
        announce = self.metainfo.get('announce', None)
        flat_urls = tuple(url for tier in tiers for url in tier)
        if announce is not None and announce not in flat_urls:
            tiers.insert(0, [announce])
        return utils.Trackers(tiers, callback=self._trackers_changed)

    @trackers.setter
    def trackers(self, value):
        if value is None:
            value = ()
        if isinstance(value, abc.Iterable):
            self._trackers_changed(utils.Trackers(value))
        else:
            raise ValueError(f'Must be Iterable, str or None, not {type(value).__name__}: {value}')

    def _trackers_changed(self, trackers):
        # Set "announce" to first tracker of first tier
        try:
            self.metainfo['announce'] = str(trackers[0][0])
        except IndexError:
            self.metainfo.pop('announce', None)

        # Remove "announce-list" if there's only one tracker
        if len(trackers.flat) <= 1:
            self.metainfo.pop('announce-list', None)
        else:
            if 'announce-list' not in self.metainfo:
                self.metainfo['announce-list'] = []
            # Set announce-list without changing its identity
            self.metainfo['announce-list'][:] = ([str(url) for url in tier]
                                                 for tier in trackers)

    @property
    def webseeds(self):
        """
        List of webseed URLs

        http://bittorrent.org/beps/bep_0019.html

        The list returned by this property automatically updates
        :attr:`metainfo`\\ ``['url-list']`` when manipulated.  Setting this
        property sets :attr:`metainfo`\\ ``['url-list']``.

        :raises URLError: if any URL is invalid
        :raises ValueError: if set to anything that isn't an iterable or a
            string
        """
        return utils.URLs(self.metainfo.get('url-list', ()),
                          callback=self._webseeds_changed)

    @webseeds.setter
    def webseeds(self, value):
        if isinstance(value, str):
            urls = utils.URLs((value,))
        elif isinstance(value, abc.Iterable):
            urls = utils.URLs(value)
        elif value is None:
            urls = utils.URLs(())
        else:
            raise ValueError(f'Must be Iterable, str or None, not {type(value).__name__}: {value}')
        self._webseeds_changed(urls)

    def _webseeds_changed(self, webseeds):
        if webseeds:
            self.metainfo['url-list'] = [str(url) for url in webseeds]
        else:
            self.metainfo.pop('url-list', None)

    @property
    def httpseeds(self):
        """
        List of webseed URLs

        http://bittorrent.org/beps/bep_0017.html

        The list returned by this property automatically updates
        :attr:`metainfo`\\ ``['httpseeds']`` when manipulated.  Setting this
        property sets :attr:`metainfo`\\ ``['httpseeds']``.

        :raises URLError: if any URL is invalid
        :raises ValueError: if set to anything that isn't an iterable or a
            string
        """
        return utils.URLs(self.metainfo.get('httpseeds', ()),
                          callback=self._httpseeds_changed)

    @httpseeds.setter
    def httpseeds(self, value):
        if isinstance(value, str):
            urls = utils.URLs((value,))
        elif isinstance(value, abc.Iterable):
            urls = utils.URLs(value)
        elif value is None:
            urls = utils.URLs(())
        else:
            raise ValueError(f'Must be Iterable, str or None, not {type(value).__name__}: {value}')
        self._httpseeds_changed(urls)

    def _httpseeds_changed(self, httpseeds):
        if httpseeds:
            self.metainfo['httpseeds'] = [str(url) for url in httpseeds]
        else:
            self.metainfo.pop('httpseeds', None)

    @property
    def private(self):
        """
        Whether torrent should use trackers exclusively for peer discovery

        ``True`` or ``False`` if
        :attr:`metainfo`\\ ``['info']``\\ ``['private']`` exists, ``None``
        otherwise.
        """
        if 'private' in self.metainfo['info']:
            return bool(self.metainfo['info']['private'])
        else:
            return None

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

        Setting this property sets or removes :attr:`metainfo`\\ ``['comment']``.
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

        Setting this property sets or removes
        :attr:`metainfo`\\ ``['creation date']``.
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

        Setting this property sets or removes
        :attr:`metainfo`\\ ``['created by']``.
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

        Setting this property sets or removes
        :attr:`metainfo`\\ ``['info']``\\ ``['created by']``.
        """
        return self.metainfo['info'].get('source', None)

    @source.setter
    def source(self, value):
        if value is not None:
            self.metainfo['info']['source'] = str(value)
        else:
            self.metainfo['info'].pop('source', None)

    @property
    def infohash(self):
        """
        SHA1 info hash

        :raises MetainfoError: if :attr:`validate` fails or :attr:`metainfo` is
            not bencodable
        """
        try:
            # Try to calculate infohash
            self.validate()
            try:
                info = utils.encode_dict(self.metainfo['info'])
            except ValueError as e:
                raise error.MetainfoError(e)
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
        Whether to ensure that :attr:`infohash` is always unique

        This allows for cross-seeding without changing :attr:`piece_size`.

        Setting this property to ``True`` sets
        :attr:`metainfo`\\ ``['info']``\\ ``['entropy']`` to a random integer.
        Setting it to ``False`` removes that field.
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

        This method sets :attr:`metainfo`\\ ``['info']``\\ ``['pieces']`` after
        all pieces are hashed successfully.

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

        :raises PathError: if :attr:`path` contains only empty files/directories
        :raises ReadError: if :attr:`path` or any file beneath it is not
            readable
        :raises RuntimeError: if :attr:`path` is None

        :return: ``True`` if all pieces were successfully hashed, ``False``
            otherwise
        """
        self.metainfo['info']['pieces'] = bytes()
        filepaths = self.filepaths

        if self.path is None:
            raise RuntimeError('generate() called with no path specified')
        elif sum(utils.real_size(fp) for fp in filepaths) < 1:
            raise error.PathError(self.path, msg='Empty or all files excluded')

        if callback is not None:
            maybe_cancel = generate.CancelCallback(callback, interval)
        else:
            maybe_cancel = None
        threads = threads or NCORES

        # Read piece_size'd chunks from disk and push them to queue for hashing
        reader = generate.Reader(filepaths=filepaths,
                                 piece_size=self.piece_size,
                                 queue_size=threads * 3)

        # Pool of workers that pull from reader's piece queue, calculate the
        # hashes, and quickly offload the results to a hash queue
        hasher_threadpool = generate.HasherPool(threads, reader.piece_queue)

        # Pull from the hash queue and call status/cancel callback
        def collector_callback(filepath, pieces_done, piece_index, piece_hash, exc,
                               maybe_cancel=maybe_cancel, torrent=self, pieces_total=self.pieces):
            if exc is not None:
                raise exc
            elif maybe_cancel is not None:
                maybe_cancel(cb_args=(torrent, filepath, pieces_done, pieces_total),
                             # Always call callback after the last piece was hashed
                             force_call=pieces_done >= pieces_total)
        collector_thread = generate.Collector(hasher_threadpool.hash_queue,
                                              callback=collector_callback)

        if maybe_cancel:
            maybe_cancel.on_cancel(reader.stop,
                                   hasher_threadpool.stop,
                                   collector_thread.stop)

        try:
            reader.read()
        except BaseException:
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
            torrent_subpath = torrent_filepath.parts[1:]
            fs_filepath = pathlib.Path(path, *torrent_subpath)
            paths.append((fs_filepath, torrent_filepath))

        return paths, callback

    def verify_filesize(self, path, callback=None):
        """
        Check if `path` looks like it should contain all the data of this torrent

        Walk through :attr:`files` and check if each file exists relative to
        `path`, is readable and has the correct size.  Excess files in `path`
        are ignored.

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
        :raises ReadError: if any file's size can't be determined
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
                exception = error.ReadError(errno.ENOENT, fs_filepath)
                if cancel(cb_args=(self, fs_filepath, torrent_filepath,
                                   files_done, files_total, exception),
                          force_call=True):
                    break
                else:
                    continue

            # If we expect a file, check if path is a file.  We don't need to
            # check for a directory if we expect one because we are iterating
            # over files (filepaths), so the path "foo/bar/baz" will result in a
            # ReadError if "foo" or "foo/bar" is a file.
            if self.mode == 'singlefile' and os.path.isdir(path):
                exception = error.VerifyNotDirectoryError(fs_filepath)
                if cancel(cb_args=(self, fs_filepath, torrent_filepath,
                                   files_done, files_total, exception),
                          force_call=True):
                    break
                else:
                    continue

            # Check file size
            fs_filepath_size = utils.real_size(fs_filepath)
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

    def verify(self, path, skip_on_error=True, threads=None, callback=None, interval=0):
        """
        Check if `path` contains all the data specified in this torrent

        Generate hashes from the contents of :attr:`files` and compare them to
        the ones stored in :attr:`metainfo`\\ ``['info']``\\ ``['pieces']``.

        :param str path: Directory or file to check
        :param bool skip_on_error: Whether to stop hashing pieces from file if a
            piece from it is corrupt
        :param int threads: How many threads to use for hashing pieces or
            ``None`` to use one thread per available CPU core
        :param callable callback: Callable to report progress and/or abort

            `callback` must accept 7 positional arguments:

                1. Torrent instance (:class:`Torrent`)
                2. File path in file system (:class:`str`)
                3. Number of checked pieces (:class:`int`)
                4. Total number of pieces (:class:`int`)
                5. Index of the current piece (:class:`int`)
                6. SHA1 hash of the current piece or ``None`` (:class:`bytes`)
                7. Exception (:class:`TorfError`) or ``None``

            If `callback` returns anything that is not ``None``, verification is
            stopped.

        :param float interval: Minimum number of seconds between calls to
            `callback` (if 0, `callback` is called once per piece); this is
            ignored if an error is found

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

        if self.mode == 'singlefile' and os.path.isdir(path):
            exception = error.VerifyNotDirectoryError(path)
            maybe_cancel(cb_args=(self, fs_filepaths[0], 0, self.pieces, 0, None, exception),
                         force_call=True)
        elif self.mode == 'multifile' and not os.path.isdir(path):
            exception = error.VerifyIsDirectoryError(path)
            maybe_cancel(cb_args=(self, fs_filepaths[0], 0, self.pieces, 0, None, exception),
                         force_call=True)
        else:
            exception = None
            threads = threads or NCORES
            reader = generate.Reader(filepaths=fs_filepaths,
                                     file_sizes={fs_path:self.partial_size(t_path)
                                                 for fs_path,t_path in filepaths},
                                     piece_size=self.piece_size,
                                     queue_size=threads * 3,
                                     skip_on_error=skip_on_error)

            # Pool of workers that pull from reader_thread's piece queue, calculate
            # the hashes, and quickly offload the results to a hash queue
            hasher_threadpool = generate.HasherPool(threads, reader.piece_queue,
                                                    file_was_skipped=reader.file_was_skipped)

            # Pull from the hash queue; also call `callback` and maybe stop everything
            def collector_callback(filepath, pieces_done, piece_index, piece_hash, exc,
                                   torrent=self, pieces_total=self.pieces, piece_size=self.piece_size,
                                   exp_hashes=self.hashes,
                                   exp_filesizes={fs_path : self.partial_size(t_path)
                                                  for fs_path,t_path in filepaths}):
                nonlocal exception
                if exc is not None:
                    # Reader raised exception
                    exception = exc
                    maybe_cancel(cb_args=(torrent, filepath, pieces_done, pieces_total,
                                          piece_index, piece_hash, exception),
                                 force_call=True)
                elif piece_hash is not None and piece_hash != exp_hashes[piece_index]:
                    # Piece hash doesn't match
                    if skip_on_error:
                        reader.skip_file(filepath, piece_index)
                    file_sizes = tuple((fs_path, self.partial_size(t_path))
                                       for fs_path,t_path in filepaths)
                    exception = error.VerifyContentError(piece_index, piece_size, file_sizes)
                    maybe_cancel(cb_args=(torrent, filepath, pieces_done, pieces_total,
                                          piece_index, piece_hash, exception),
                                 force_call=True)
                else:
                    # No error; report progress
                    maybe_cancel(cb_args=(torrent, filepath, pieces_done, pieces_total,
                                          piece_index, piece_hash, None),
                                 # Always call callback after the last piece was
                                 # hashed to report completion
                                 force_call=pieces_done >= pieces_total)
            collector_thread = generate.Collector(hasher_threadpool.hash_queue,
                                                  callback=collector_callback,
                                                  file_was_skipped=reader.file_was_skipped)
            maybe_cancel.on_cancel(reader.stop,
                                   hasher_threadpool.stop,
                                   collector_thread.stop)
            try:
                reader.read()
            except BaseException:
                hasher_threadpool.stop()
                collector_thread.stop()
                raise
            finally:
                hasher_threadpool.join()
                collector_thread.join()
                if collector_thread.exception is not None:
                    exception = collector_thread.exception

        # Raise exception unless it was passed to callback
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

        References:
          | http://bittorrent.org/beps/bep_0003.html
          | https://wiki.theory.org/index.php/BitTorrentSpecification#Metainfo_File_Structure

        :raises MetainfoError: if :attr:`metainfo` would not generate a valid
            torrent file or magnet link
        """
        md = self.metainfo
        info = md['info']

        # Check values shared by singlefile and multifile torrents
        utils.assert_type(md, ('info',), (dict,), must_exist=True)
        utils.assert_type(md, ('info', 'name'), (str,), must_exist=True)
        utils.assert_type(md, ('info', 'piece length'), (int,), must_exist=True,
                          check=utils.is_power_of_2)
        utils.assert_type(md, ('info', 'pieces'), (abc.ByteString,), must_exist=True)
        utils.assert_type(md, ('info', 'private'), (bool, int), must_exist=False)
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
                path_size = utils.real_size(self.path)
                if path_size != info['length']:
                    raise error.MetainfoError(f"Mismatching file sizes in metainfo ({info['length']})"
                                              f" and file system ({path_size}): {self.path}")

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
                        raise error.MetainfoError(f"Metainfo includes file that doesn't exist: {filepath}")
                    if not os.path.isfile(filepath):
                        raise error.MetainfoError(f"Metainfo includes file that isn't a file: {filepath}")

                    # Check if sizes match
                    filesize = utils.real_size(filepath)
                    if filesize != fileinfo['length']:
                        raise error.MetainfoError(f"Mismatching file sizes in metainfo ({fileinfo['length']})"
                                                  f" and file system ({filesize}): {filepath}")

        else:
            raise error.MetainfoError("Missing 'length' or 'files' in 'info'")

    def convert(self):
        """
        Return :attr:`metainfo` with all keys encoded to :class:`bytes` and all
        values encoded to :class:`bytes`, :class:`int`, :class:`list` or
        :class:`OrderedDict`

        :raises MetainfoError: if a value cannot be converted properly
        """
        try:
            return utils.encode_dict(self.metainfo)
        except ValueError as e:
            raise error.MetainfoError(e)

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

        :param stream: Writable file-like object (e.g. :class:`io.BytesIO`)
        :param bool validate: Whether to run :meth:`validate` first

        :raises WriteError: if writing to `stream` fails
        :raises MetainfoError: if :attr:`metainfo` is invalid
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

    def write(self, filepath, validate=True, overwrite=False):
        """
        Write :attr:`metainfo` to torrent file

        :param filepath: Path of the torrent file
        :param bool validate: Whether to run :meth:`validate` first
        :param bool overwrite: Whether to silently overwrite `filepath` (only
            after all pieces were hashed successfully)

        :raises WriteError: if writing to `filepath` fails
        :raises MetainfoError: if :attr:`metainfo` is invalid
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

    def magnet(self, name=True, size=True, trackers=True, tracker=False):
        """
        :class:`Magnet` instance

        :param bool name: Whether to include the name
        :param bool size: Whether to include the size
        :param bool trackers: Whether to include all trackers
        :param bool tracker: Whether to include only the first tracker of the
            first tier (overrides `trackers`)

        :raises MetainfoError: if :attr:`metainfo` is invalid
        """
        kwargs = {'xt': 'urn:btih:' + self.infohash}
        if name:
            kwargs['dn'] = self.name
        if size:
            kwargs['xl'] = self.size

        if tracker:
            kwargs['tr'] = (self.trackers[0][0],)
        elif trackers:
            kwargs['tr'] = (url
                            for tier in self.trackers
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
            instance

            NOTE: If the "info" field is not a dictionary,
                  :class:`MetainfoError` is raised even if `validate` is set to
                  False

        :raises ReadError: if reading from `stream` fails
        :raises BdecodeError: if `stream` does not produce a valid bencoded byte
            sequence
        :raises MetainfoError: if `validate` is `True` and the read metainfo is
            invalid

        :return: New :class:`Torrent` instance
        """
        try:
            content = stream.read(cls.MAX_TORRENT_FILE_SIZE)
        except OSError as e:
            raise error.ReadError(e.errno)
        else:
            try:
                metainfo_enc = bencode.decode(content)
            except (bencode.DecodingError, ValueError):
                raise error.BdecodeError()
            else:
                if not isinstance(metainfo_enc, abc.Mapping):
                    raise error.BdecodeError()

            # Extract 'pieces' from metainfo before decoding because it's the
            # only byte sequence that isn't supposed to be decoded to a string.
            if (b'info' in metainfo_enc and
                isinstance(metainfo_enc[b'info'], dict) and
                b'pieces' in metainfo_enc[b'info']):
                pieces = metainfo_enc[b'info'].pop(b'pieces')
                metainfo = utils.decode_dict(metainfo_enc)
                metainfo['info']['pieces'] = pieces
            else:
                metainfo = utils.decode_dict(metainfo_enc)

            # "info" must be a dictionary.  If validation is not wanted, it's OK
            # if it doesn't exist because the "metainfo" property will add
            # automatically.
            utils.assert_type(metainfo, ('info',), (dict,), must_exist=validate)

            torrent = cls()
            torrent._metainfo = metainfo

            # Convert "creation date" to datetime.datetime and "private" to
            # bool, but only if they exist
            if b'creation date' in metainfo_enc:
                torrent.creation_date = metainfo_enc[b'creation date']
            if b'private' in metainfo_enc.get(b'info', {}):
                torrent.private = metainfo_enc[b'info'][b'private']

            if validate:
                torrent.validate()

            return torrent

    @classmethod
    def read(cls, filepath, validate=True):
        """
        Read torrent metainfo from file

        :param filepath: Path of the torrent file
        :param bool validate: Whether to run :meth:`validate` on the new Torrent
            instance

        :raises ReadError: if reading from `filepath` fails
        :raises BdecodeError: if `filepath` does not contain a valid bencoded byte
            sequence
        :raises MetainfoError: if `validate` is `True` and the read metainfo is
            invalid

        :return: New :class:`Torrent` instance
        """
        try:
            with open(filepath, 'rb') as f:
                return cls.read_stream(f, validate=validate)
        except (OSError, error.ReadError) as e:
            raise error.ReadError(e.errno, filepath)
        except error.BdecodeError:
            raise error.BdecodeError(filepath)

    def copy(self):
        """Create a new :class:`Torrent` instance with the same metainfo"""
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
