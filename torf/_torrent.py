# MIT License

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from bencoder import bencode, bdecode, BTFailure
from base64 import b32encode
from hashlib import sha1, md5
from datetime import datetime
import os
import math
import time
from collections import abc
import errno
import inspect
import io

from . import _utils as utils
from . import _errors as error
from ._version import __version__
_PACKAGE_NAME = __name__.split('.')[0]


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

    MAX_PIECES     = 1500
    MIN_PIECE_SIZE = 2 ** 14  # 16 KiB
    MAX_PIECE_SIZE = 2 ** 26  # 64 MiB

    def __init__(self, path=None, name=None,
                 exclude=(), trackers=(), webseeds=(), httpseeds=(),
                 private=False, comment=None, source=None,
                 creation_date=None, created_by='%s/%s' % (_PACKAGE_NAME, __version__),
                 piece_size=None, include_md5=False, randomize_infohash=False):
        self._metainfo = {}
        self.trackers = trackers
        self.webseeds = webseeds
        self.httpseeds = httpseeds
        self.private = private
        self.comment = comment
        self.creation_date = creation_date
        self.created_by = created_by
        self.source = source
        self.include_md5 = include_md5
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
        :class:`int`, :class:`list` or :class:`dict`. See also :meth:`convert`
        and :meth:`validate`.

        The ``info`` key is guaranteed to exist.
        """
        if 'info' not in self._metainfo:
            self._metainfo['info'] = {}
        return self._metainfo

    @property
    def path(self):
        """
        Path to torrent content

        The properties :attr:`name` and :attr:`piece_size` are changed
        implicitly when this property is set.

        Setting this property sets ``name`` and ``piece length`` in
        :attr:`metainfo`\ ``['info']`` as well as ``length`` if path is a file
        or ``files`` if path is a directory.

        If set to ``None``, the following keys are removed (if present) from
        :attr:`metainfo`\ ``['info']``: ``piece length``, ``pieces``, ``name``,
        ``length``, ``md5sum``, ``files``

        :raises PathEmptyError: if :attr:`path` contains no data (i.e. empty
            file, empty directory or directory containing only empty files)
        """
        return getattr(self, '_path', None)
    @path.setter
    def path(self, value):
        info = self.metainfo['info']

        # Unset path and remove related metainfo
        if hasattr(self, '_path'):
            delattr(self, '_path')
        for key in ('piece length', 'pieces', 'name', 'length', 'md5sum', 'files'):
            info.pop(key, None)

        if value is not None:
            # Set new path and update related metainfo
            path = os.path.normpath(str(value))
            if os.path.isfile(path):
                info['length'] = os.path.getsize(path)
            elif os.path.isdir(path):
                files = []
                basepath = path.split(os.sep)
                for filepath in utils.filepaths(path, exclude=self.exclude,
                                                hidden=False, empty=False):
                    files.append({'length': os.path.getsize(filepath),
                                  'path'  : filepath.split(os.sep)[len(basepath):]})
                info['files'] = files
            else:
                raise error.PathNotFoundError(value)

            if self.size < 1:
                raise error.PathEmptyError(path)
            else:
                self._path = path
                if path == '.':
                    self.name = os.path.basename(os.path.abspath('.'))
                elif path == '..':
                    self.name = os.path.basename(os.path.abspath('..'))
                else:
                    self.name  # Set default name in metainfo dict
                self.calculate_piece_size()

    @property
    def files(self):
        """
        Yield relative file paths specified in :attr:`metainfo`

        Each paths starts with :attr:`name`.

        Note that the paths may not exist. See :attr:`filepaths` for existing
        files.
        """
        info = self.metainfo['info']
        if 'length' in info:    # Singlefile
            yield info['name']
        elif 'files' in info:   # Multifile torrent
            rootdir = self.name
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

    @property
    def filetree(self):
        """
        :attr:`files` as a dictionary tree

        Each node is a ``dict`` that maps directory/file names to child nodes.
        Each child node is a ``dict`` for directories and ``None`` for files.

        If :attr:`path` is ``None``, this is an empty ``dict``.
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
            subtree[filename] = None
        return tree

    @property
    def size(self):
        """
        Total size of content in bytes or ``None`` if :attr:`path` is ``None``
        """
        if 'length' in self.metainfo['info']:   # Singlefile
            return self.metainfo['info']['length']
        elif 'files' in self.metainfo['info']:  # Multifile torrent
            return sum(fileinfo['length']
                       for fileinfo in self.metainfo['info']['files'])

    @property
    def piece_size(self):
        """
        Piece size/length or ``None``

        If set to ``None``, :attr:`calculate_piece_size` is called.

        If :attr:`size` returns ``None``, this also returns ``None``.

        Setting this property sets ``piece length`` in :attr:`metainfo`\
        ``['info']``.
        """
        if 'piece length' not in self.metainfo['info']:
            if self.size is None:
                return None
            else:
                self.calculate_piece_size()
        return self.metainfo['info']['piece length']
    @piece_size.setter
    def piece_size(self, value):
        if value is None:
            self.calculate_piece_size()
        else:
            try:
                piece_length = int(value)
            except (TypeError, ValueError):
                raise ValueError(f'piece_size must be int, not {value!r}')
            else:
                if self.MIN_PIECE_SIZE <= value <= self.MAX_PIECE_SIZE:
                    if not utils.is_power_of_2(piece_length):
                        raise error.PieceSizeError(size=piece_length)
                    self.metainfo['info']['piece length'] = piece_length
                else:
                    raise error.PieceSizeError(min=self.MIN_PIECE_SIZE,
                                               max=self.MAX_PIECE_SIZE)

    def calculate_piece_size(self):
        """
        Calculate and add ``piece length`` to ``info`` in :attr:`metainfo`

        The piece size is calculated so that there are no more than
        :attr:`MAX_PIECES` pieces unless it is larger than
        :attr:`MAX_PIECE_SIZE`, in which case there is no limit on the number of
        pieces.

        :raises RuntimeError: if :attr:`size` returns ``None``
        """
        size = self.size
        if not size:
            raise RuntimeError(f'Cannot calculate piece size with no "path" specified')
        else:
            self.metainfo['info']['piece length'] = utils.calc_piece_size(
                size, self.MAX_PIECES, self.MIN_PIECE_SIZE, self.MAX_PIECE_SIZE)

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
    def name(self):
        """
        Name of the torrent

        Default to last item in :attr:`path` or ``None`` if :attr:`path` is
        ``None``.

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
        List of tiers of announce URLs or ``None`` for no trackers

        A tier is either a single announce URL (:class:`str`) or an
        :class:`~collections.abc.Iterable` (e.g. :class:`list`) of announce
        URLs.

        Setting this property sets or removes ``announce`` and ``announce-list``
        in :attr:`metainfo`. ``announce`` is set to the first tracker of the
        first tier.

        :raises URLError: if any of the announce URLs is invalid
        """
        announce_list = self.metainfo.get('announce-list', None)
        if not announce_list:
            announce = self.metainfo.get('announce', None)
            if announce:
                return [[announce]]
        else:
            return announce_list
    @trackers.setter
    def trackers(self, value):
        if not value:
            self.metainfo.pop('announce-list', None)
            self.metainfo.pop('announce', None)
        else:
            self.metainfo['announce-list'] = []
            for item in value:
                if isinstance(item, str):
                    tier = [utils.validated_url(str(item))]
                else:
                    tier = []
                    for url in item:
                        tier.append(utils.validated_url(str(url)))
                self.metainfo['announce-list'].append(tier)

            # First tracker is also available via 'announce'
            if self.metainfo['announce-list']:
                self.metainfo['announce'] = self.metainfo['announce-list'][0][0]

    @property
    def webseeds(self):
        """List of webseed URLs or ``None`` for no webseeds

        http://bittorrent.org/beps/bep_0019.html
        """
        return self.metainfo.get('url-list', None)
    @webseeds.setter
    def webseeds(self, value):
        if not value:
            self.metainfo.pop('url-list', None)
        else:
            self.metainfo['url-list'] = [utils.validated_url(url) for url in value]

    @property
    def httpseeds(self):
        """
        List of httpseed URLs or ``None`` for no httpseeds

        http://bittorrent.org/beps/bep_0017.html
        """
        return self.metainfo.get('httpseeds', None)
    @httpseeds.setter
    def httpseeds(self, value):
        if not value:
            self.metainfo.pop('httpseeds', None)
        else:
            self.metainfo['httpseeds'] = [utils.validated_url(url) for url in value]

    @property
    def private(self):
        """
        Whether torrent should use trackers exclusively for peer discovery

        Setting this property sets or removes ``private`` in :attr:`metainfo`\ ``['info']``.
        """
        return bool(self.metainfo['info'].get('private', False))
    @private.setter
    def private(self, value):
        if value:
            self.metainfo['info']['private'] = True
        else:
            self.metainfo['info'].pop('private', None)

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
    def include_md5(self):
        """
        Whether to include MD5 sums for each file

        This takes only effect when :meth:`generate` is called.
        """
        return getattr(self, '_include_md5', False)
    @include_md5.setter
    def include_md5(self, value):
        self._include_md5 = bool(value)

    @property
    def infohash(self):
        """SHA1 info hash"""
        self.validate()
        info = self.convert()[b'info']
        return sha1(bencode(info)).hexdigest()

    @property
    def infohash_base32(self):
        """Base32 encoded SHA1 info hash"""
        self.validate()
        info = self.convert()[b'info']
        return b32encode(sha1(bencode(info)).digest())

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

    def generate(self, callback=None, interval=0):
        """
        Hash pieces and report progress to `callback`

        This method sets ``pieces`` in :attr:`metainfo`\ ``['info']`` when all
        pieces are hashed successfully.

        :param callable callback: Callable with signature ``(torrent, filepath,
            pieces_done, pieces_total)``; if `callback` returns anything else
            than None, hashing is canceled

        :param float interval: Minimum number of seconds between calls to
            `callback` (if 0, `callback` is called once per piece)
        :raises PathEmptyError: if :attr:`path` contains only empty
            files/directories
        :raises PathNotFoundError: if :attr:`path` does not exist
        :raises ReadError: if :attr:`path` or any file beneath it is not
            readable

        :return: ``True`` if all pieces were successfully hashed, ``False``
            otherwise
        """
        if self.path is None:
            raise RuntimeError('generate() called with no path specified')
        elif self.size <= 0:
            raise error.PathEmptyError(self.path)
        elif not os.path.exists(self.path):
            raise error.PathNotFoundError(self.path)

        if callback is not None:
            cancel = lambda *status: callback(*status) is not None
        else:
            cancel = lambda *status: False

        if os.path.isfile(self.path):
            pieces = self._set_pieces_singlefile()
        elif os.path.isdir(self.path):
            pieces = self._set_pieces_multifile()

        # Iterate over hashed pieces and send status information
        last_cb_call = 0
        for filepath,pieces_done,pieces_total in pieces:
            now = time.time()
            if now - last_cb_call >= interval or \
               pieces_done >= pieces_total:
                last_cb_call = now
                if cancel(self, filepath, pieces_done, pieces_total):
                    return False
        return True

    def _set_pieces_singlefile(self):
        filepath = self.path
        piece_size = self.piece_size
        pieces_total = self.pieces
        pieces_done = 0
        pieces = bytearray()
        md5_hasher = md5() if self.include_md5 else None

        for piece in utils.read_chunks(filepath, piece_size):
            pieces.extend(sha1(piece).digest())
            if md5_hasher:
                md5_hasher.update(piece)
            pieces_done += 1
            yield (filepath, pieces_done, pieces_total)

        self.metainfo['info']['pieces'] = pieces
        if md5_hasher:
            self.metainfo['info']['md5sum'] = md5_hasher.hexdigest()

        # Report completion
        yield (filepath, pieces_total, pieces_total)

    def _set_pieces_multifile(self):
        piece_size = self.piece_size
        pieces_total = math.ceil(self.size / piece_size)
        pieces_done = 0
        piece_buffer = bytearray()
        pieces = bytearray()
        md5sums = []

        for filepath in self.filepaths:
            md5_hasher = md5() if self.include_md5 else None

            # Read piece_sized chunks from filepath until piece_buffer is big
            # enough for a new piece
            for chunk in utils.read_chunks(filepath, piece_size):
                piece_buffer.extend(chunk)
                if len(piece_buffer) >= piece_size:
                    piece = piece_buffer[:piece_size]
                    pieces.extend(sha1(piece).digest())
                    del piece_buffer[:piece_size]
                    pieces_done += 1
                    yield (filepath, pieces_done, pieces_total)

                if md5_hasher:
                    md5_hasher.update(chunk)

            if md5_hasher:
                md5sums.append(md5_hasher.hexdigest())

        # Unless self.size is dividable by self.piece_size, there is some data
        # left in piece_buffer
        if len(piece_buffer) > 0:
            pieces.extend(sha1(piece_buffer).digest())

        self.metainfo['info']['pieces'] = pieces
        if md5_hasher:
            for md5sum,fileinfo in zip(md5sums, self.metainfo['info']['files']):
                fileinfo['md5sum'] = md5sum

        # Report completion
        yield (filepath, pieces_total, pieces_total)

    utils.ENCODE_CONVERTERS[datetime] = lambda dt: int(dt.timestamp())
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

    def validate(self):
        """
        Check if all mandatory keys exist in :attr:`metainfo` and are of expected
        types

        The necessary values are documented here:
            | http://bittorrent.org/beps/bep_0003.html
            | https://wiki.theory.org/index.php/BitTorrentSpecification#Metainfo_File_Structure

        Note that ``announce`` is not considered mandatory because clients can
        find peers via DHT.

        :raises MetainfoError: if :attr:`metainfo` would not generate a valid
            torrent file or magnet link
        """
        md = self.metainfo
        info = md['info']

        # Check values shared by singlefile and multifile torrents
        utils.assert_type(md, ('info', 'name'), (str,), must_exist=True)
        utils.assert_type(md, ('info', 'piece length'), (int,), must_exist=True)
        utils.assert_type(md, ('info', 'pieces'), (bytes, bytearray), must_exist=True)

        if 'length' in info and 'files' in info:
            raise error.MetainfoError("['info'] includes both 'length' and 'files'")

        elif 'length' in info:
            # Validate info as singlefile torrent
            utils.assert_type(md, ('info', 'length'), (int, float), must_exist=True)
            utils.assert_type(md, ('info', 'md5sum'), (str,), must_exist=False, check=utils.is_md5sum)

            if self.path is not None:
                # Check if filepath actually points to a file
                if not os.path.isfile(self.path):
                    raise error.MetainfoError(f"Metainfo includes {self.path} as file, but it is not a file")

                # Check if size matches
                if os.path.getsize(self.path) != info['length']:
                    raise error.MetainfoError(f"Mismatching file sizes in metainfo ({info['length']})"
                                              f" and local file system ({os.path.getsize(self.path)}): "
                                              f"{self.path!r}")

        elif 'files' in info:
            # Validate info as multifile torrent
            utils.assert_type(md, ('info', 'files'), (list,), must_exist=True)

            for i,fileinfo in enumerate(info['files']):
                utils.assert_type(md, ('info', 'files', i, 'length'), (int, float), must_exist=True)
                utils.assert_type(md, ('info', 'files', i, 'path'), (list,), must_exist=True)
                utils.assert_type(md, ('info', 'files', i, 'md5sum'), (str,), must_exist=False,
                            check=utils.is_md5sum)

            if self.path is not None:
                # Check if filepath actually points to a directory
                if not os.path.isdir(self.path):
                    raise error.MetainfoError(f"Metainfo includes {self.path} as directory, but it is not a directory")

                for i,fileinfo in enumerate(info['files']):
                    for j,item in enumerate(fileinfo['path']):
                        utils.assert_type(md, ('info', 'files', i, 'path', j), (str,))

                    filepath = os.path.join(self.path, os.path.join(*fileinfo['path']))

                    # Check if filepath exists and is a file
                    if not os.path.exists(filepath):
                        raise error.MetainfoError(f"Metainfo inclues file that doesn't exist: {filepath!r}")
                    if not os.path.isfile(filepath):
                        raise error.MetainfoError(f"Metainfo inclues non-file: {filepath!r}")

                    # Check if sizes match
                    if os.path.getsize(filepath) != fileinfo['length']:
                        raise error.MetainfoError(f"Mismatching file sizes in metainfo ({fileinfo['length']})"
                                                  f" and local file system ({os.path.getsize(filepath)}): "
                                                  f"{filepath!r}")

        else:
            raise error.MetainfoError("Missing 'length' or 'files' in metainfo")

    def dump(self, validate=True):
        """
        Create bencoded :attr:`metainfo` (i.e. the content of a torrent file)

        :param bool validate: Whether to run :meth:`validate` first

        :return: :attr:`metainfo` as bencoded :class:`bytes`
        """
        if validate:
            self.validate()
        return bencode(self.convert())

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
        ...     f.write(torrent.dump())

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
        BTIH Magnet URI

        :param bool name: Whether to include the name
        :param bool size: Whether to include the size
        :param bool trackers: Whether to include all trackers
        :param bool tracker: Whether to include only the first tracker of the
            first tier (overrides `trackers`)
        :param bool validate: Whether to run :meth:`validate` first
        """
        if validate:
            self.validate()

        parts = [f'xt=urn:btih:{self.infohash}']
        if name:
            parts.append(f'dn={utils.urlquote(self.name)}')
        if size:
            parts.append(f'xl={self.size}')

        if self.trackers is not None:
            if tracker:
                parts.append(f'tr={utils.urlquote(self.trackers[0][0])}')
            elif trackers:
                for tier in self.trackers:
                    for url in tier:
                        parts.append(f'tr={utils.urlquote(url)}')

        return 'magnet:?' + '&'.join(parts)

    # Maximum number of bytes that read() reads from torrent files.  This limit
    # exists in case we're accidentally reading from a huge, non-torrent file
    # that could even fill up RAM and crash the whole application.
    MAX_TORRENT_FILE_SIZE = int(10e6)  # 10MB

    @classmethod
    def read_stream(cls, stream, validate=True):
        """
        Read torrent metainfo from file-like object

        :param stream: Readable file-like object (e.g. :class:`io.BytesIO`)
        :param bool validate: Whether to run :meth:`validate` on the new Torrent
            object

        :raises ReadError: if reading from `stream` fails
        :raises ParseError: if `stream` does not produce a valid bencoded byte
            string
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
                metainfo_enc = bdecode(content)
            except BTFailure as e:
                raise error.ParseError()

            if validate:
                if b'info' not in metainfo_enc:
                    raise error.MetainfoError("Missing 'info'")
                elif not isinstance(metainfo_enc[b'info'], abc.Mapping):
                    raise error.MetainfoError("'info' is not a dictionary")
                elif b'pieces' not in metainfo_enc[b'info']:
                    raise error.MetainfoError("Missing 'pieces' in ['info']")

            # Extract 'pieces' from metainfo because it's the only byte string
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

            # Auto-set 'include_md5'
            info = torrent.metainfo['info']
            torrent.include_md5 = ('length' in info and 'md5sum' in info) or \
                                  ('files' in info and all('md5sum' in fileinfo
                                                           for fileinfo in info['files']))

            if validate:
                torrent.validate()

            return torrent

    @classmethod
    def read(cls, filepath, validate=True):
        """
        Read torrent metainfo from file

        :param filepath: Path of the torrent file
        :param bool validate: Whether to run :meth:`validate` on the new Torrent
            object

        :raises ReadError: if reading from `filepath` fails
        :raises ParseError: if `filepath` does not contain a valid bencoded byte
            string
        :raises MetainfoError: if `validate` is `True` and the read metainfo is
            invalid

        :return: New Torrent object
        """
        try:
            with open(filepath, 'rb') as fh:
                return cls.read_stream(fh)
        except (OSError, error.ReadError) as e:
            raise error.ReadError(e.errno, filepath)
        except error.ParseError:
            raise error.ParseError(filepath)

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

    def __deepcopy__(self, _):
        return self.copy()

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
