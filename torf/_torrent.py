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
import inspect
from collections import abc, OrderedDict
import io
import random

from . import _utils as utils
from . import _errors as error
from ._version import __version__

class Torrent():
    """
    Torrent metainfo representation

    Create a new Torrent object:

    >>> from torf import Torrent
    >>> torrent = Torrent('path/to/My Torrent',
    ...                   trackers=['https://localhost:123/announce'],
    ...                   comment='This is my first torrent')

    Convenient access to metainfo via properties:

    >>> torrent.comment
    'This is my first torrent. Be gentle.'
    >>> torrent.private = True

    Full control over unencoded metainfo:

    >>> torrent.metainfo['info']['private']
    True
    >>> torrent.metainfo['more stuff'] = {'foo': 12,
    ...                                   'bar': ('x', 'y', 'z')}

    Start hashing and update progress once per second:

    >>> def callback(filepath, pieces_done, pieces_total):
    ...     print(f'{pieces_done/pieces_total*100:3.0f} % done')
    >>> success = torrent.generate(callback, interval=1)
      1 % done
      2 % done
      [...]
    100 % done

    Write torrent file:

    >>> with open('my_torrent.torrent', 'wb') as f:
    ...    torrent.write(f)

    Generate magnet link:

    >>> torrent.magnet()
    'magnet:?xt=urn:btih:e167b1fbb42ea72f051f4f50432703308efb8fd1&dn=My+Torrent&xl=142631&tr=https%3A%2F%2Flocalhost%3A123%2Fannounce'

    Read torrent:

    >>> with open('my_torrent.torrent', 'rb') as f:
    ...    t = Torrent.read(f)
    >>> t.comment
    'This is my first torrent. Be gentle.'
    """

    MIN_PIECE_SIZE = 2 ** 14  # 16 KiB
    MAX_PIECE_SIZE = 2 ** 26  # 64 MiB

    def __init__(self, path=None, name=None, exclude=(), trackers=(), webseeds=(),
                 httpseeds=(), private=False, comment=None, creation_date=None,
                 created_by=None, source=None, piece_size=None,
                 include_md5=False, randomize_infohash=False):
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
        self.piece_size = piece_size
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

        Setting this property to a file path sets ``name`` and ``length`` in the
        ``info`` dictionary in :attr:`metainfo`.

        Setting this property to a directory path sets ``name`` and ``files`` in
        the ``info`` dictionary in :attr:`metainfo`.

        Setting this property to ``None`` removes the following keys from the
        ``info`` dictionary in :attr:`metainfo`: ``piece length``, ``pieces``,
        ``name``, ``length``, ``md5sum``, ``files``

        :raises PathEmptyError: if :attr:`path` contains no data
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
                self.name  # Set default name in metainfo dict

    @property
    def files(self):
        """
        Yield relative paths to files specified in :attr:`metainfo`

        Paths are prefixed with :attr:`name`.

        Note that the paths may not exist. See :attr:`filepaths` for existing
        files.
        """
        info = self.metainfo['info']
        if 'length' in info:    # Singlefile
            yield info['name']
        elif 'files' in info:   # Multifile torrent
            for fileinfo in info['files']:
                yield os.path.join(self.name, os.path.join(*fileinfo['path']))

    @property
    def filepaths(self):
        """Yield absolute paths to local files in :attr:`path`"""
        if self.path is None:
            return
        for filepath_rel in self.files:
            yield os.path.join(os.path.dirname(self.path), filepath_rel)

    @property
    def size(self):
        """
        Total size of content in bytes or ``None`` if the ``info`` dictionary in
        :attr:`metainfo` doesn't have ``length`` or ``files`` set
        """
        if 'length' in self.metainfo['info']:   # Singlefile
            return self.metainfo['info']['length']
        elif 'files' in self.metainfo['info']:  # Multifile torrent
            return sum(fileinfo['length']
                       for fileinfo in self.metainfo['info']['files'])
        else:
            return None

    @property
    def piece_size(self):
        """
        Piece size/length or ``None`` to pick one automatically

        Setting this property sets or removes ``piece length`` in the ``info``
        dictionary in :attr:`metainfo`.

        Getting this property if it hasn't been set calculates ``piece length``
        so that there are approximately 1500 pieces in total. The result is
        stored as the ``piece length`` value in the ``info`` dictionary in
        :attr:`metainfo`.
        """
        if 'piece length' not in self.metainfo['info']:
            if self.size is None:
                return None
            else:
                self.metainfo['info']['piece length'] = utils.calc_piece_size(
                    self.size, self.MIN_PIECE_SIZE, self.MAX_PIECE_SIZE)
        return self.metainfo['info']['piece length']
    @piece_size.setter
    def piece_size(self, value):
        if value is None:
            self.metainfo['info'].pop('piece length', None)
        else:
            try:
                piece_length = int(value)
            except TypeError:
                raise RuntimeError(f'piece_size must be int, not {value!r}')
            else:
                if self.MIN_PIECE_SIZE <= value <= self.MAX_PIECE_SIZE:
                    self.metainfo['info']['piece length'] = piece_length
                else:
                    raise error.PieceSizeError(min=self.MIN_PIECE_SIZE,
                                               max=self.MAX_PIECE_SIZE)

    @property
    def name(self):
        """
        Name of the torrent

        Default to last item in :attr:`path` or ``None`` if :attr:`path` is
        ``None``.

        Setting this property sets or removes ``name`` in the ``info``
        dictionary of :attr:`metainfo`.
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
        :class:`~collections.abc.Iterable` (e.g. a :class:`list`) of announce
        URLs.

        Setting this property sets or removes ``announce`` and ``announce-list``
        in :attr:`metainfo`. ``announce`` is set to the first tracker of the
        first tier.

        :raise URLError: if any of the announce URLs is invalid
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
                self.metainfo['announce'] = self.metainfo['announce-list'][0]

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

        Setting this property sets or removes ``private`` in the ``info``
        dictionary of :attr:`metainfo`.
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

        Setting this property sets or removes ``source`` in :attr:`metainfo`.
        """
        return self.metainfo.get('source', None)
    @source.setter
    def source(self, value):
        if value is not None:
            self.metainfo['source'] = str(value)
        else:
            self.metainfo.pop('source', None)

    @property
    def exclude(self):
        """
        List of filename patterns to exclude

        \*
          matches everything

        ?
          matches any single character

        [seq]
          matches any character in seq

        [!seq]
          matches any char not in seq
        """
        return self._exclude
    @exclude.setter
    def exclude(self, value):
        if isinstance(value, str):
            self._exclude = [value]
        else:
            self._exclude = list(value)

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
        """SHA1 info hash (run :meth:`generate` first)"""
        self.validate()
        info = self.convert()[b'info']
        return sha1(bencode(info)).hexdigest()

    @property
    def infohash_base32(self):
        """Base32 encoded SHA1 info hash (run :meth:`generate` first)"""
        self.validate()
        info = self.convert()[b'info']
        return b32encode(sha1(bencode(info)).digest())

    @property
    def randomize_infohash(self):
        """
        Whether to ensure that :attr:`infohash` is always different

        This allows cross-seeding without changing :attr:`piece_size` manually.

        Setting this property to ``True`` sets ``entropy`` in the ``info``
        dictionary of :attr:`metainfo` to a random integer. Setting it to
        ``False`` removes that value.
        """
        return bool(self.metainfo['info'].get('entropy', False))
    @randomize_infohash.setter
    def randomize_infohash(self, value):
        if value:
            # According to BEP0003 "Integers have no size limitation", but some
            # parsers seem to have problems with large numbers.
            self.metainfo['info']['entropy'] = random.randint(-2e9, 2e9)
        else:
            self.metainfo['info'].pop('entropy', None)

    def generate(self, callback=None, interval=0):
        """
        Set ``pieces`` in ``info`` dictionary of :attr:`metainfo`

        :param callable callback: Callable with signature ``(filepath,
            pieces_completed, pieces_total)``; if *callback* returns anything
            that is not None, hashing is canceled
        :param int interval: Number of seconds between calls to *callback*

        :raises PathEmptyError: if :attr:`path` contains no data

        :return: True if ``pieces`` was successfully added to :attr:`metainfo`,
                 ``False`` otherwise
        """
        if self.path is None:
            raise RuntimeError('generate() called with no path specified')
        elif self.size <= 0:
            raise error.PathEmptyError(self.path)

        if callback is not None:
            cancel = lambda *status: callback(*status) is not None
        else:
            cancel = lambda *status: False

        if os.path.isfile(self.path):
            set_pieces = self._set_pieces_singlefile()
        elif os.path.isdir(self.path):
            set_pieces = self._set_pieces_multifile()

        # Iterate over hashed pieces and send status information
        last_cb_call = 0
        for filepath,pieces_completed,pieces_total in set_pieces:
            now = time.time()
            if now - last_cb_call >= interval or \
               pieces_completed >= pieces_total:
                last_cb_call = now
                if cancel(filepath, pieces_completed, pieces_total):
                    return False
        return True

    def _set_pieces_singlefile(self):
        filepath = self.path
        piece_size = self.piece_size
        pieces_total = math.ceil(self.size / piece_size)
        pieces_completed = 0
        pieces = bytearray()
        md5_hasher = md5() if self.include_md5 else None

        for piece in utils.read_chunks(filepath, piece_size):
            pieces.extend(sha1(piece).digest())
            if md5_hasher:
                md5_hasher.update(piece)
            pieces_completed += 1
            yield (filepath, pieces_completed, pieces_total)

        self.metainfo['info']['pieces'] = pieces
        if md5_hasher:
            self.metainfo['info']['md5sum'] = md5_hasher.hexdigest()

        # Report completion
        yield (filepath, pieces_total, pieces_total)

    def _set_pieces_multifile(self):
        piece_size = self.piece_size
        pieces_total = math.ceil(self.size / piece_size)
        pieces_completed = 0
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
                    pieces_completed += 1
                    yield (filepath, pieces_completed, pieces_total)

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
        Create bencoded :attr:`metainfo`

        :param bool validate: Whether to run :meth:`validate` first

        :return: :attr:`metainfo` as validated, bencoded :class:`bytes`
        """
        if validate:
            self.validate()
        return bencode(self.convert())

    def write(self, stream, validate=True):
        """
        Write :attr:`metainfo` to file object (run :meth:`generate` first)

        :param stream: A stream or file object (must be opened in ``'wb'`` mode)
        :param bool validate: Whether to run :meth:`validate` first
        """
        if stream.closed:
            raise RuntimeError(f'{stream!r} is closed')
        elif not stream.writable():
            raise RuntimeError(f'{stream!r} is opened in read-only mode')
        elif not isinstance(stream, (io.RawIOBase, io.BufferedIOBase)):
            raise RuntimeError(f'{stream!r} is not opened in binary mode')
        else:
            stream.write(self.dump(validate=validate))

    def magnet(self, name=True, size=True, trackers=True, tracker=False, validate=True):
        """
        BTIH Magnet URI (run :meth:`generate` first)

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

    @classmethod
    def read(cls, stream, validate=True):
        """
        Read torrent metainfo from file object

        :param stream: Stream or file object (must be opened in ``'rb'`` mode)
        :param bool validate: Whether to run :meth:`validate` on the new Torrent
            object

        :raises MetainfoParseError: if `stream` does not contain a valid
            bencoded byte string

        :return: New Torrent object
        """
        if stream.closed:
            raise RuntimeError(f'{stream!r} is closed')
        elif not stream.readable():
            raise RuntimeError(f'{stream!r} is opened in write-only mode')
        elif not isinstance(stream, (io.RawIOBase, io.BufferedIOBase)):
            raise RuntimeError(f'{stream!r} is not opened in binary mode')
        else:
            try:
                md_enc = bdecode(stream.read())
            except BTFailure as e:
                raise error.MetainfoParseError()

            # Extract 'pieces' from metainfo because it's the only byte string
            # that isn't supposed to be decoded to unicode.
            pieces = md_enc[b'info'].pop(b'pieces')
            new_md = utils.decode_dict(md_enc)
            new_md['info']['pieces'] = pieces

            torrent = cls()
            torrent._metainfo = new_md

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
