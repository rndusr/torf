import base64
import collections
import copy
import datetime
import functools
import hashlib
import inspect
import math
import os
import re

from .. import __version__, _bencode, _errors, _metainfo, _utils

NO_DEFAULT = object()
PACKAGE_NAME = __name__.split('.')[0]


class KeywordArguments:
    def __init__(self, provided, defaults):
        for key in provided:
            assert key in defaults
        self.provided = provided
        self.defaults = defaults
        self.all = {**defaults, **provided}


def _get_kwargs():
    """
    Return keyword arguments of the caller method as :class:`dict`

    Defaults are ommitted.
    """
    caller_frame = inspect.currentframe().f_back
    argvalues = inspect.getargvalues(caller_frame)

    # self = argvalues.locals['self']
    # cls = type(self)
    cls = Torrent

    caller_name = caller_frame.f_code.co_name
    caller = getattr(cls, caller_name)
    signature = inspect.signature(caller)

    defaults = {
        name: parameter.default
        for name, parameter in signature.parameters.items()
        if name != 'self'
    }
    # print('defaults:', defaults)

    # For mutable values, if we store the original and it is modified later, our internally stored
    # value suddenly has a different value. If that modified value is used in __repr__() or copy(),
    # terrible things can happen. So here we create a copy of potentially mutable arguments.
    def get_storable_value(value):
        return copy.deepcopy(value)

    kwargs = {
        name: get_storable_value(argvalues.locals[name])
        for name in argvalues.args
        if (
                name != 'self'
                and argvalues.locals[name] != defaults[name]
        )
    }
    return KeywordArguments(kwargs, defaults)


class Torrent:
    """
    ...

    :param str encoding: Metainfo encoding for :class:`str` values

        .. important:: Only UTF-8 creates compliant torrents. This option only exists to read
            non-compliant torrents.

        A list of valid `encoding` values can be found here:
        https://docs.python.org/3/library/codecs.html#standard-encodings

    :param bool raise_on_decoding_error: Whether decoding badly encoded strings should raise
        :class:`CodecError` or the relevant characters should be replaced with "�" (Unicode
        "REPLACEMENT CHARACTER")

    :param bool validate: Whether the provided metadata should be checked to make sure it represents
        a valid torrent

    :param str infohash: Base16 or Base32 SHA1 hash of :attr:`metainfo`\\ ``['info']``

        .. note:: This is usually calculated and is only needed when converting :class:`~.Magnet` to
            :class:`~.Torrent`.

    :param dict metainfo: Custom :attr:`metainfo`

        Keyword arguments that affect :attr:`metainfo` take precedence (e.g. ``comment="foo",
        metainfo={"comment": "bar"}`` will result in ``{"comment": "foo"}``.

    :raise: :class:`TypeError` or :class:`ValueError` if any argument is invalid
    """

    def __init__(
            self,
            *,
            encoding='UTF-8',
            raise_on_decoding_error=False,
            validate=True,
            # Metainfo arguments
            announce=None,
            comment=None,
            created_by=f'{PACKAGE_NAME} {__version__}',
            creation_date=None,
            files=None,
            httpseeds=None,
            infohash=None,
            name=None,
            pieces=None,
            private=None,
            source=None,
            webseeds=None,
            metainfo=None,
    ):
        self._kwargs = _get_kwargs()
        self._handle_kwargs(self._kwargs.provided)

    @functools.cached_property
    def _metainfo_raw(self):
        """
        Raw metainfo as :class:`dict`

        Keys must be :class:`bytes` and values must be :class:`bytes`, :class:`int`, :class:`dict`
        (or dict-like) or :class:`list` (any non-string sequence is also supported).

        .. warning:: Keep in mind that manipulating this property can break things horribly,
            unexpectedly and non-obviously.
        """
        return {}

    @functools.cached_property
    def _metainfo_pure(self):
        """
        Decoded :attr:`_metainfo_raw`

        This is a proxy wrapper around :attr:`_metainfo_raw` that decodes/encodes on demand. This
        object always contains the same data as :attr:`_metainfo_raw`. Setting values on this object
        encodes them properly in :attr:`_metainfo_raw` or raises :class:`~.CodecError`.

        Consider using :meth:`copy` instead if you want to modify the metainfo.

        .. warning:: Keep in mind that manipulating this property can break things horribly,
            unexpectedly and non-obviously.
        """
        return _metainfo.CodecMapping(
            self._metainfo_raw,
            # Even in weird torrents, keys should always be ASCII, which is a subset of UTF-8.
            # We could make this can be configurable if required.
            keys_encoding='UTF-8',
            no_encoding_keypaths=(
                ('info', 'pieces'),
            ),
        )

    @functools.cached_property
    def metainfo(self):
        """
        Decoded metainfo as :class:`~.ImmutableDict`

        Values are :class:`str`, :class:`int`, :class:`~.ImmutableDict` or :class:`tuple`. Keys in
        :class:`ImmutableDict` values are :class:`str`.

        :attr:`metainfo`\\ ``['info']``\\ ``['pieces']`` is not decoded and provided as
        :class:`bytes`.
        """
        # Before we can return the immutable metainfo, we must know if we are fully initialized. For
        # example, if __init__() calls self.metainfo (e.g. for debugging) before all arguments are
        # translated into _metainfo_pure, we create and cache ImmutableDict(_metainfo_pure), which
        # can never be updated, and then _metainfo_pure is updated. We fix this with a flag that is
        # set after all arguments are processed.
        if not hasattr(self, '_metainfo_initialized'):
            raise RuntimeError('metainfo is not fully initialized yet')

        def make_immutable(obj):
            if isinstance(obj, collections.abc.Mapping):
                return _utils.ImmutableDict(
                    (k, make_immutable(v))
                    for k, v in obj.items()
                )

            elif _utils.is_sequence(obj):
                return tuple(
                    make_immutable(v)
                    for v in obj
                )

            elif isinstance(obj, bytearray):
                return bytes(obj)

            else:
                return obj

        return make_immutable(self._metainfo_pure)

    def __repr__(self):
        text = f'<{type(self).__name__} {id(self)}'
        if infohash := self.infohash:
            text += f' infohash={infohash!r}'
        if name := self.name:
            text += f' name={name!r}'
        text += '>'
        return text

    def copy(self, **kwargs):
        """
        Create copy of an instance

        This method takes the same arguments as the class. They overload the original arguments that
        were provided when the instance was created.

        The `metainfo` argument, if provided, is merged with the original :attr:`metainfo`. Any
        other arguments that affect :attr:`metainfo` take precedence over values in `metainfo`.

        :return: New :class:`Torrent` instance
        """
        # Create empty instance. No need to validate.
        cp = type(self)(validate=False)

        # We are actually just starting to initialize our copy.
        self._metainfo_initialized = True

        # Copy raw metainfo and its encoding.
        cp._metainfo_raw = copy.deepcopy(self._metainfo_raw)
        cp._metainfo_pure.values_encoding = self._metainfo_pure.values_encoding
        cp._metainfo_pure.raise_on_decoding_error = self._metainfo_pure.raise_on_decoding_error

        # Copy original "validate" argument unless we overload it.
        if 'validate' not in kwargs and 'validate' in self._kwargs.provided:
            kwargs['validate'] = self._kwargs.provided['validate']

        # Handle our own arguments just like __init__() does. This also sets
        # ``self._metainfo_initialized = True`` again.
        cp._handle_kwargs(kwargs)
        return cp

    def _handle_kwargs(self, kwargs):
        # Because other arguments takes precedence over "metainfo", we must apply "metainfo" first.
        metainfo = kwargs.get('metainfo', None)
        if metainfo is not None:
            self._handle_kwarg_metainfo(metainfo)

        # Validation must be done after all other arguments are handled.
        validate = kwargs.get('validate', True)

        for name, value in kwargs.items():
            if name not in ('validate', 'metainfo'):
                method = getattr(self, f'_handle_kwarg_{name}')
                method(value)

        # Indicate that all arguments have been processed and self.metainfo can now return an
        # immutable version of freeze self._metainfo_pure.
        self._metainfo_initialized = True

        if validate:
            self.validate()

    def _handle_kwarg_encoding(self, encoding):
        self._metainfo_pure.values_encoding = encoding

    def _handle_kwarg_raise_on_decoding_error(self, raise_on_decoding_error):
        self._metainfo_pure.raise_on_decoding_error = raise_on_decoding_error

    def _handle_kwarg_metainfo(self, metainfo):
        merged = _utils.merge_dicts(self._metainfo_pure, metainfo)
        self._metainfo_pure.update(merged)

    def _get_metainfo(self, *keypath, type=None, default=NO_DEFAULT):
        obj = self.metainfo
        for i, key in enumerate(keypath):
            try:
                obj = obj[key]
            except (KeyError, IndexError):
                if default is NO_DEFAULT:
                    path_so_far = '.'.join(str(k) for k in keypath[:i + 1])
                    raise ValueError(f'{path_so_far}: Not found')
                else:
                    return default
        if type:
            try:
                return type(obj)
            except (ValueError, TypeError):
                if default is NO_DEFAULT:
                    keypath_str = '.'.join(str(k) for k in keypath)
                    raise ValueError(f'{keypath_str}: Invalid value for {type.__name__}: {obj!r}')
                else:
                    return default
        else:
            return obj

    def _set_metainfo(self, keypath, value):
        obj = self._metainfo_pure
        if keypath[0] == 'info' and 'info' not in obj:
            obj['info'] = {}
        for key in keypath[:-1]:
            obj = obj[key]
        if value is None:
            try:
                del obj[keypath[-1]]
            except (KeyError, IndexError):
                pass
        else:
            obj[keypath[-1]] = value

    @property
    def announce(self):
        """
        :class:`tuple` of tiers (i.e. :class:`tuple`\\ s) of announce URLs

        https://bittorrent.org/beps/bep_0003.html
        https://bittorrent.org/beps/bep_0012.html
        """
        if 'announce-list' in self.metainfo:
            return tuple(
                tuple(url for url in tier)
                for tier in self.metainfo['announce-list']
            )
        elif 'announce' in self.metainfo:
            # Wrap only tracker in single tier.
            return (
                (self.metainfo['announce'],),
            )
        else:
            return ()

    def _handle_kwarg_announce(self, announce):
        if announce is None:
            self._metainfo_pure.pop('announce', None)
            self._metainfo_pure.pop('announce-list', None)

        elif isinstance(announce, str):
            if announce:
                self._metainfo_pure['announce'] = announce
                # Remove "announce-list" if it exists.
                self._metainfo_pure.pop('announce-list', None)
            else:
                raise ValueError('announce is empty string')

        elif isinstance(announce, collections.abc.Iterable):
            announce_list = []
            for announce_or_tier in announce:
                if isinstance(announce_or_tier, str):
                    announce_list.append((announce_or_tier,))
                elif isinstance(announce_or_tier, collections.abc.Iterable):
                    tier = []
                    for announce in announce_or_tier:
                        if isinstance(announce, str):
                            tier.append(announce)
                        else:
                            raise TypeError(f'Unexpected announce type: {type(announce).__name__}: {announce!r}')
                    announce_list.append(tier)
                else:
                    raise TypeError(
                        f'Unexpected announce type: {type(announce_or_tier).__name__}: '
                        f'{announce_or_tier!r}'
                    )

            if announce_list:
                self._metainfo_pure['announce-list'] = announce_list
            else:
                # Remove empty "announce-list" if it exists.
                self._metainfo_pure.pop('announce-list', None)
            # Remove "announce" if it exists.
            self._metainfo_pure.pop('announce', None)

        else:
            raise TypeError(f'Unexpected announce type: {type(announce).__name__}: {announce!r}')

    @property
    def webseeds(self):
        """
        :class:`tuple` of WebSeed URLs

        http://bittorrent.org/beps/bep_0019.html
        """
        return self._get_metainfo('url-list', type=tuple, default=())

    def _handle_kwarg_webseeds(self, webseeds):
        if webseeds is None:
            self._metainfo_pure.pop('url-list', None)

        elif isinstance(webseeds, str):
            if webseeds:
                self._metainfo_pure['url-list'] = (webseeds,)
            else:
                raise ValueError('webseed is empty string')

        elif isinstance(webseeds, collections.abc.Iterable):
            url_list = []
            for url in webseeds:
                if isinstance(url, str):
                    url_list.append(url)
                else:
                    raise TypeError(f'Unexpected webseed type: {type(url).__name__}: {url!r}')
            if url_list:
                self._metainfo_pure['url-list'] = url_list
            else:
                self._metainfo_pure.pop('url-list', None)

        else:
            raise TypeError(f'Unexpected webseeds type: {type(webseeds).__name__}: {webseeds!r}')

    @property
    def httpseeds(self):
        """
        :class:`tuple` of HTTP Seeding URLs

        http://bittorrent.org/beps/bep_0017.html
        """
        return self._get_metainfo('httpseeds', type=tuple, default=())

    def _handle_kwarg_httpseeds(self, httpseeds):
        if isinstance(httpseeds, str):
            if httpseeds:
                self._metainfo_pure['httpseeds'] = (httpseeds,)
            else:
                raise ValueError('httpseed is empty string')

        elif isinstance(httpseeds, collections.abc.Iterable):
            urls = []
            for url in httpseeds:
                if isinstance(url, str):
                    urls.append(url)
                else:
                    raise TypeError(f'Unexpected httpseed type: {type(url).__name__}: {url!r}')
            if urls:
                self._metainfo_pure['httpseeds'] = urls
            else:
                self._metainfo_pure.pop('httpseeds', None)

        else:
            raise TypeError(f'Unexpected httpseeds type: {type(httpseeds).__name__}: {httpseeds!r}')

    @property
    def comment(self):
        """:attr:`metainfo`\\ ``['comment']`` as :class:`str` or ``None`` if not specified"""
        return self._get_metainfo('comment', type=str, default=None)

    def _handle_kwarg_comment(self, comment):
        if comment is None:
            self._set_metainfo(('comment',), None)
        elif isinstance(comment, str):
            self._set_metainfo(('comment',), comment or None)
        else:
            raise TypeError(f'Unexpected comment type: {type(comment).__name__}: {comment!r}')

    @property
    def created_by(self):
        """:attr:`metainfo`\\ ``['created by']`` as :class:`str` or ``None`` if not specified"""
        return self._get_metainfo('created by', type=str, default=None)

    def _handle_kwarg_created_by(self, created_by):
        if created_by is None:
            self._set_metainfo(('created by',), None)
        elif isinstance(created_by, str):
            self._set_metainfo(('created by',), created_by or None)
        else:
            raise TypeError(f'Unexpected created_by type: {type(created_by).__name__}: {created_by!r}')

    @property
    def creation_date(self):
        """
        :attr:`metainfo`\\ ``['creation date']`` as :class:`datetime.datetime` or ``None`` if
        not specified
        """
        return self._get_metainfo('creation date', type=datetime.datetime.fromtimestamp, default=None)

    def _handle_kwarg_creation_date(self, creation_date):
        if creation_date is None:
            self._set_metainfo(('creation date',), None)
        elif isinstance(creation_date, (int, float)):
            self._set_metainfo(('creation date',), int(creation_date))
        elif isinstance(creation_date, datetime.datetime):
            self._set_metainfo(('creation date',), int(creation_date.timestamp()))
        else:
            raise TypeError(f'Unexpected creation_date type: {type(creation_date).__name__}: {creation_date!r}')

    @property
    def filelist(self):
        """
        :class:`tuple` of relative :class:`~.File` paths in this torrent

        Every path starts with :attr:`name`.
        """
        if info := self._get_metainfo('info', type=dict, default=None):
            # Multi-file torrent
            name = info.get('name', '')
            files = info.get('files', ())
            if name and files:
                return tuple(
                    _utils.File(*(name, *path), size=length)
                    for file in files
                    if (
                            (path := file.get('path', ()))
                            and (length := file.get('length', 0))
                    )
                )

            # Single-file torrent
            length = info.get('length', 0)
            if name and length:
                return (_utils.File(name, size=length),)

        return ()

    def _handle_kwarg_files(self, files):
        if files is None:
            metainfo_files = None
        elif isinstance(files, collections.abc.Iterable):
            metainfo_files = []
            for file in files:
                if isinstance(file, collections.abc.Mapping):
                    metainfo_files.append(file)
                elif isinstance(file, _utils.File):
                    metainfo_files.append({'length': file.size, 'path': file.path})
                elif isinstance(file, collections.abc.Sequence):
                    if len(file) >= 2:
                        path = file[0]
                        if not _utils.is_sequence(path):
                            raise TypeError(f'Expected sequence for path, not {type(path).__name__}: {path!r}')
                        for part in path:
                            if not isinstance(part, str):
                                raise TypeError(f'Expected str in path, not {type(part).__name__}: {part!r}')
                        length = file[1]
                        if not isinstance(length, int):
                            raise TypeError(f'Expected int for size, not {type(length).__name__}: {length!r}')
                        metainfo_files.append({'length': length, 'path': path})
                    else:
                        raise ValueError(f'Expected (<path>, <size>): {file!r}')
                else:
                    raise TypeError(f'Unexpected file type: {type(file).__name__}: {file!r}')
        else:
            raise TypeError(f'Unexpected files type: {type(files).__name__}: {files!r}')
        self._set_metainfo(('info', 'files'), metainfo_files)

    @property
    def filetree(self):
        """
        Nested :class:`dict` :class:`~.File` instances as specified in :attr:`metainfo`

        Keys are :class:`~.File` instances and values are either :class:`dict` or :class:`~.File`
        instances.

        Every path starts with :attr:`name`.

        For example, here is an ``info`` section and the resulting ``filetree``:

        .. code:: python

            {
                'name': 'mytorrent',
                'files': [
                    {'length': 3, 'path': ['foo']},
                    {'length': 6, 'path': ['bar', 'baz']},
                ],
            }

        .. code:: python

            {
                _utils.File('mytorrent', size=9): {
                    _utils.File('mytorrent', 'foo', size=3): _utils.File('mytorrent', 'foo', size=3),
                    _utils.File('mytorrent', 'bar', size=6): {
                        _utils.File('mytorrent', 'bar', 'baz', size=6): _utils.File('mytorrent', 'bar', 'baz', size=6),
                    },
                },
            },
        """
        tree = {}
        for file in self.filelist:
            # Path without filename.
            dirpath = file.path[:-1]

            # Add any missing parent directories.
            subtree = tree
            for i in range(len(dirpath)):
                keypath = dirpath[:i + 1]
                key = _utils.File(*keypath, size=self.size_partial(keypath))
                if key not in subtree:
                    subtree[key] = {}
                # Set current directory to immediate parent directory.
                subtree = subtree[key]

            # Add file to current subtree.
            value = _utils.File(*file.path, size=self.size_partial(file.path))
            subtree[value] = value

        return tree

    @property
    def infohash(self):
        """
        SHA1 hash of the data in :attr:`metainfo`\\ ``['info']`` or ``None``

        This is the `infohash` argument if one was passed. Otherwise, it is calculated from
        :attr:`metainfo` if possible.
        """
        # The infohash is set explicitly when creating a Torrent from a Magnet URI.
        infohash = getattr(self, '_infohash', None)
        if infohash is not None:
            return infohash
        else:
            # If we don't have proper metainfo, the infohash will be useless.
            try:
                self.validate()
            except _errors.ValidationError:
                pass
            else:
                info = self._metainfo_raw.get(b'info', None)
                if info:
                    return hashlib.sha1(_bencode.encode(info)).hexdigest()

    def _handle_kwarg_infohash(self, infohash):
        if infohash is None:
            if hasattr(self, '_infohash'):
                delattr(self, '_infohash')
        elif isinstance(infohash, str):
            if re.search(r'^[0-9a-f]{40}$', infohash, flags=re.IGNORECASE):
                # Base16
                self._infohash = infohash.lower()
            elif re.search(r'^[a-z2-7]{32}$', infohash, flags=re.IGNORECASE):
                # Base32 (re-encode as Base16)
                self._infohash = base64.b16encode(base64.b32decode(infohash)).decode('ascii').lower()
            else:
                raise ValueError(f'Unexpected infohash format: {infohash!r}')
        else:
            raise TypeError(f'Unexpected infohash type: {type(infohash).__name__}: {infohash!r}')

    @property
    def infohash_base32(self):
        """Base32 encoded :attr:`infohash` or ``None``"""
        if infohash := self.infohash:
            return base64.b32encode(base64.b16decode(infohash.upper()))

    @property
    def name(self):
        """
        :attr:`metainfo`\\ ``['info']``\\ ``['name']`` as :class:`str` or ``None`` if not
        specified
        """
        return self._get_metainfo('info', 'name', type=str, default=None)

    def _handle_kwarg_name(self, name):
        if name is None:
            self._set_metainfo(('info', 'name'), None)
        elif isinstance(name, str):
            self._set_metainfo(('info', 'name'), name or None)
        else:
            raise TypeError(f'Unexpected name type: {type(name).__name__}: {name!r}')

    @property
    def pieces(self):
        """
        :class:`tuple` of SHA1 piece hashes as :class:`bytes` or empty :class:`tuple` (``()``)

        Pieces are stored in :attr:`metainfo`\\ ``['info']``\\ ``['pieces']``.
        """
        pieces = self._get_metainfo('info', 'pieces', type=bytes, default=None)
        if pieces:
            # Each piece is 20 bytes long.
            return tuple(
                bytes(pieces[pos : pos + 20])
                for pos in range(0, len(pieces), 20)
            )
        else:
            return ()

    def _handle_kwarg_pieces(self, pieces):
        if pieces is None:
            self._set_metainfo(('info', 'pieces'), None)
        elif isinstance(pieces, (bytes, bytearray)):
            self._set_metainfo(('info', 'pieces'), pieces or None)
        elif isinstance(pieces, collections.abc.Iterable) and not isinstance(pieces, str):
            pieces = tuple(pieces)
            for piece in pieces:
                if not isinstance(piece, (bytes, bytearray)):
                    raise TypeError(f'Unexpected piece type: {type(piece).__name__}: {piece!r}')
            self._set_metainfo(('info', 'pieces'), b''.join(pieces))
        else:
            raise TypeError(f'Unexpected pieces type: {type(pieces).__name__}: {pieces!r}')

    @property
    def pieces_count(self):
        """
        Number of :attr:`pieces` the content should split into for hashing or ``0`` if unknown

        The number of pieces is calculated from :attr:`size` and :attr:`piece_length` and does not
        rely on :attr:`pieces`. We want to know how many pieces we will have without hashing them.
        """
        size, piece_length = self.size, self.piece_length
        if size and size > 0 and piece_length and piece_length > 0:
            return math.ceil(size / piece_length)
        else:
            return 0

    @property
    def piece_length(self):
        """
        :attr:`metainfo`\\ ``['info']``\\ ``['piece length']`` as :class:`int` or ``None`` if
        not specified
        """
        return self._get_metainfo('info', 'piece length', type=int, default=None)

    @property
    def private(self):
        """
        :attr:`metainfo`\\ ``['info']``\\ ``['private']`` as :class:`bool`

        ``True`` if the field exists and is truthy, ``False`` otherwise

        Private torrents must only use trackers and not DHT or PEX for finding peers.
        """
        return self._get_metainfo('info', 'private', type=bool, default=False)

    def _handle_kwarg_private(self, private):
        # Set private flag to 1 (True) or remove it (False).
        self._set_metainfo(('info', 'private'), 1 if private else None)

    @property
    def size(self):
        """Total size of content in bytes as :class:`int` or ``None`` if not specified"""
        # Single-file torrent.
        length = self._get_metainfo('info', 'length', type=int, default=None)
        if length is not None:
            return length

        # Multi-file torrent.
        files = self._get_metainfo('info', 'files', type=tuple, default=None)
        if files is not None:
            return sum(
                fileinfo.get('length', 0)
                for fileinfo in files
                if isinstance(fileinfo, collections.abc.Mapping)
            )

        # Size is unknown.
        return None

    def size_partial(self, path):
        """
        Return combined size of one or more files as specified in :attr:`metainfo`

        :param path: Relative path within torrent, starting with :attr:`name`
        :type path: str, path-like or iterable

        If `path` points to a directory (i.e. an incomplete file path), the sizes of all file that
        start with that path are combined.

        :raises ValueError: if `path` is empty or does not exist
        :raises TypeError: if `path` is of unsupported type
        """
        if isinstance(path, str):
            parts = tuple(part for part in path.split(os.sep) if part)
            print('path from str:', repr(path), '->', repr(parts))
        elif isinstance(path, os.PathLike):
            parts = tuple(os.fspath(path).split(os.sep))
        elif isinstance(path, collections.abc.Iterable):
            parts = tuple(str(part) for part in path)
        else:
            raise TypeError(f'Unexpected path type: {type(path).__name__}: {path}')
        if not parts:
            raise ValueError(f'path must not be empty: {path}')

        if info := self._get_metainfo('info', type=dict, default=None):
            name = info.get('name', None)

            # If this is a single-file torrent, `parts` can only have one value.
            if parts == (name,) and (length := info.get('length', None)):
                return length

            elif fileinfos := info.get('files', ()):
                file_sizes = []
                for fileinfo in fileinfos:
                    this_path = (name, *(part for part in fileinfo.get('path', ()) if part))
                    if this_path == parts:
                        # `path` points to file.
                        return fileinfo.get('length', 0)
                    elif _utils.iterable_startswith(this_path, parts):
                        # path points to directory
                        file_sizes.append(fileinfo.get('length', 0))
                if file_sizes:
                    return sum(file_sizes)

        raise ValueError(f'No such path: {path}')

    @property
    def source(self):
        """
        :attr:`metainfo`\\ ``['info']``\\ ``['source']`` as :class:`str` or ``None`` if not
        specified
        """
        return self._get_metainfo('info', 'source', type=str, default=None)

    def _handle_kwarg_source(self, source):
        if source is None:
            self._set_metainfo(('info', 'source'), None)
        elif isinstance(source, str):
            self._set_metainfo(('info', 'source'), source or None)
        else:
            raise TypeError(f'Unexpected source type: {type(source).__name__}: {source!r}')

    @classmethod
    def from_torrentfile(cls, torrentfile, **kwargs):
        """
        Create instance from ``.torrent`` file

        :param torrentfile: Path to ``.torrent`` file

        Any keyword arguments are forwarded to :meth:`from_stream`.

        :raises ReadError: if reading `torrentfile` fails
        :raises BdecodeError: if `torrentfile` does not contain a valid bencoded byte sequence
        :raises MetainfoError: if `validate` is ``True`` and the metainfo is invalid

        :return: :class:`Torrent` instance
        """
        try:
            with open(torrentfile, 'rb') as f:
                return cls.from_stream(f, **kwargs)
        except (OSError, _errors.ReadError) as e:
            raise _errors.ReadError(e, path=torrentfile)
        except _errors.BdecodeError:
            raise _errors.BdecodeError(torrentfile)

    @classmethod
    def from_stream(cls, stream, *, encoding='UTF-8', raise_on_decoding_error=False, validate=True):
        """
        Create instance from file-like object

        :param stream: :class:`bytes` or :class:`bytearray` or readable file-like object
            (e.g. :class:`io.BytesIO`)

        See :class:`Torrent` for the keyword arguments.

        :raises ReadError: if reading from `stream` fails
        :raises BdecodeError: if `stream` does not provide a valid bencoded byte sequence
        :raises MetainfoError: if `validate` is ``True`` and the read metainfo is invalid

        :return: :class:`Torrent` instance
        """
        data = cls._read_stream(stream)
        metainfo = _bencode.decode(data)
        return cls(
            metainfo=metainfo,
            encoding=encoding,
            raise_on_decoding_error=raise_on_decoding_error,
            validate=validate,
        )

    @classmethod
    def _read_stream(cls, stream):
        if isinstance(stream, (bytes, bytearray)):
            data = stream
        elif hasattr(stream, 'read'):
            # Read from file-like object. We try to read more than MAX_TORRENT_SIZE so ReadError is
            # raised below if we get too many bytes. (`read(n)` reads `n` bytes or less.)
            data = stream.read(cls.MAX_TORRENT_SIZE + 1)
        else:
            raise TypeError(
                'Expected bytes, bytearray or a readable file-like object, '
                f'got {type(stream).__name__}: {stream!r}'
            )

        if len(data) > cls.MAX_TORRENT_SIZE:
            raise _errors.ReadError(
                f'Metainfo exceeds maximum size: {len(data)} > {Torrent.MAX_TORRENT_SIZE}'
            )
        else:
            return data

    MAX_TORRENT_SIZE = int(30e6)  # 30MB
    """
    Maximum length of bencoded metainfo

    Reading anything larger will raise an exception. This prevents reading of gigabytes into memory
    if a wrong file is passed accidentally.
    """

    @classmethod
    def from_path(
            cls,
            path,
            *,
            exclude_globs=(),
            exclude_regexs=(),
            include_globs=(),
            include_regexs=(),
            validate=True,
    ):
        """
        Create instance from file or directory tree

        :param path: Path to file or directory that will be hashed to create a torrent

        :raises ReadError: if `path` or one of its subpaths is not readable

        :return: :class:`Torrent` instance
        """
        self = cls(...)
        self._path = path
        return self

    def validate(self):
        # TODO: Check if '.' or '..' in any path.
        # TODO: Check if '/' or '\\' in any path.

        pass

        # # Only a dictionary can be valid torrent metainfo, not a list or anything else.
        # if not isinstance(metainfo, collections.abc.Mapping):
        #     raise _errors.BdecodeError()
        # else:
        #     # Convert all `bytes` in `metainfo` to `str`.
        #     ...

    def as_magnet(self, name=True, size=True, trackers=True, webseeds=True):
        """
        :class:`Magnet` instance

        :param bool name: Whether to include the name
        :param bool size: Whether to include the size
        :param trackers: ``True`` to include all trackers, :class:`int` to include only that many
            trackers, ``False`` or ``None`` to not include any trackers
        :param webseeds: ``True`` to include all webseeds, :class:`int` to include only that many
            webseeds, ``False`` or ``None`` to not include any webseeds

        :raises MetainfoError: if :attr:`metainfo` is invalid
        """
        kwargs = {'xt': 'urn:btih:' + self.infohash}
        if name:
            kwargs['dn'] = self.name
        if size:
            kwargs['xl'] = self.size

        if trackers is True:
            kwargs['tr'] = _utils.flatten(self.announce)
        elif isinstance(trackers, int) and trackers >= 1:
            kwargs['tr'] = _utils.flatten(self.announce)[:trackers]

        if webseeds is True:
            kwargs['ws'] = self.webseeds
        elif isinstance(webseeds, int) and webseeds >= 1:
            kwargs['ws'] = self.webseeds[:webseeds]

        # Prevent circular import issues.
        from .._magnet import Magnet
        return Magnet(**kwargs)

    # @property
    # def path(self):
    #     """Local file system path of to the files in this torrent or ``None``"""
    #     return self._path
