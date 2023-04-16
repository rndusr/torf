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

import abc
import collections
import contextlib
import errno
import fnmatch
import functools
import http.client
import itertools
import math
import os
import pathlib
import re
import socket
import typing
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from urllib.parse import quote_plus as urlquote  # noqa: F401

from . import _errors as error


def is_divisible_by_16_kib(num):
    """Return whether `num` is divisible by 16384 and positive"""
    if num <= 0:
        return False
    return num % 16384 == 0

def iterable_startswith(a, b):
    a_len = len(a)
    for i, b_item in enumerate(b):
        if i >= a_len:
            # a can't start with b if b is longer than a
            return False
        if a[i] != b_item:
            return False
    return True

def flatten(items):
    for item in items:
        if isinstance(item, Iterable):
            yield from flatten(item)
        else:
            yield item

_md5sum_regex = re.compile(r'^[0-9a-fA-F]{32}$')
def is_md5sum(value):
    return bool(_md5sum_regex.match(value))


def real_size(path):
    """
    Return size for `path`, which is a (link to a) file or directory

    Raise ReadError on failure
    """
    if os.path.isdir(os.path.realpath(path)):
        def onerror(exc):
            raise error.ReadError(getattr(exc, 'errno', None),
                                  getattr(exc, 'filename', None))

        size = 0
        walker = os.walk(path, followlinks=True, onerror=onerror)
        for dirpath,dirnames,filenames in walker:
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                size += os.path.getsize(filepath)
        return size
    else:
        try:
            return os.path.getsize(path)
        except OSError as exc:
            raise error.ReadError(getattr(exc, 'errno', None),
                                  getattr(exc, 'filename', None))

def list_files(path):
    """
    Return list of sorted file paths in `path`

    Raise ReadError if `path` or any file or directory underneath it is not
    readable.
    """
    def assert_readable(path):
        os_supports_effective_ids = os.access in os.supports_effective_ids
        if not os.access(path, os.R_OK, effective_ids=os_supports_effective_ids):
            raise error.ReadError(errno.EACCES, path)

    if os.path.isfile(path):
        assert_readable(path)
        return [path]
    else:
        def onerror(exc):
            raise error.ReadError(getattr(exc, 'errno', None),
                                  getattr(exc, 'filename', None))
        filepaths = []
        for dirpath, dirnames, filenames in os.walk(path, onerror=onerror):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                assert_readable(filepath)
                filepaths.append(filepath)
        return list(sorted(filepaths, key=lambda fp: str(fp).casefold()))


def filter_files(items, getter=lambda f: f, hidden=True, empty=True,
                 exclude=(), include=()):
    """
    Return reduced copy of `items`

    items: Iterable of file paths or abritrary objects that `getter` can turn
        into a a file path
    getter: Callable that takes an item of `filepaths` and returns a file path
    exclude: Sequence of regular expressions or strings with wildcard characters
        (see `fnmatch`) that are matched against full paths
    include: Same as `exclude`, but instead of removing files, matching patterns
        keep files even if they match a pattern in `excluude
    hidden: Whether to include hidden files
    empty: Whether to include empty files
    """
    def is_hidden(path):
        for name in str(path).split(os.sep):
            if name != '.' and name != '..' and name and name[0] == '.':
                return True
        return False

    def is_excluded(path,
                    ex_regexs=tuple(x for x in exclude if isinstance(x, typing.Pattern)),
                    ex_globs=tuple(x for x in exclude if isinstance(x, str)),
                    in_regexs=tuple(i for i in include if isinstance(i, typing.Pattern)),
                    in_globs=tuple(i for i in include if isinstance(i, str))):
        # Include patterns take precedence over exclude pattersn
        if any(r.search(str(path)) for r in in_regexs):
            return False
        elif any(fnmatch.fnmatch(str(path).casefold(), g.casefold()) for g in in_globs):
            return False
        elif any(r.search(str(path)) for r in ex_regexs):
            return True
        elif any(fnmatch.fnmatch(str(path).casefold(), g.casefold()) for g in ex_globs):
            return True
        return False

    items = tuple(items)
    filepaths = tuple(getter(i) for i in items)
    try:
        basepath = pathlib.Path(os.path.commonpath(filepaths))
    except ValueError:
        basepath = pathlib.Path().cwd()

    items_filtered = []
    for item in items:
        filepath = getter(item)
        relpath_without_base = pathlib.Path(os.path.relpath(filepath, basepath))
        relpath_with_base = pathlib.Path(basepath.parent, filepath)
        # Exclude hidden files and directories, but not hidden directories in
        # `basepath`
        if not hidden and is_hidden(relpath_without_base):
            continue
        # Exclude empty file
        elif not empty and os.path.exists(filepath) and real_size(filepath) <= 0:
            continue
        # Exclude file matching regex
        elif is_excluded(relpath_with_base):
            continue
        else:
            items_filtered.append(item)
    return items_filtered


class MonitoredList(collections.abc.MutableSequence):
    """List with change callback"""
    def __init__(self, items=(), callback=None, type=None):
        self._items = []
        self._type = type
        self._callback = callback
        with self._callback_disabled():
            self.replace(items)

    @contextlib.contextmanager
    def _callback_disabled(self):
        cb = self._callback
        self._callback = None
        yield
        self._callback = cb

    def __getitem__(self, index):
        return self._items[index]

    def __delitem__(self, index):
        del self._items[index]
        if self._callback is not None:
            self._callback(self)

    def _coerce(self, value):
        if self._type is not None:
            return self._type(value)
        else:
            return value

    def _filter_func(self, item):
        if item not in self._items:
            return item

    def __setitem__(self, index, value):
        if isinstance(value, Iterable):
            value = map(self._filter_func, map(self._coerce, value))
        else:
            value = self._filter_func(self._coerce(value))
        self._items[index] = value
        if self._callback is not None:
            self._callback(self)

    def insert(self, index, value):
        value = self._filter_func(self._coerce(value))
        if value is not None:
            self._items.insert(index, value)
        if self._callback is not None:
            self._callback(self)

    def replace(self, items):
        if not isinstance(items, Iterable):
            raise ValueError(f'Not an iterable: {items!r}')
        # Don't clear list before we know all new values are valid
        items = tuple(map(self._coerce, items))
        self._items.clear()
        with self._callback_disabled():
            self.extend(items)
        if self._callback is not None:
            self._callback(self)

    def clear(self):
        self._items.clear()
        if self._callback is not None:
            self._callback(self)

    def __len__(self):
        return len(self._items)

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return frozenset(other._items) == frozenset(self._items)
        elif isinstance(other, collections.abc.Iterable):
            return (len(other) == len(self._items) and
                    all(item in self._items for item in other))
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __add__(self, other):
        if isinstance(other, type(self)):
            items = self._items + other._items
        elif isinstance(other, Iterable):
            items = self._items + list(other)
        else:
            items = self._items + [other]
        return type(self)(items, callback=self._callback)

    def __repr__(self):
        return repr(self._items)


class File(os.PathLike):
    """Path-like that also stores the file size"""

    def __fspath__(self):
        return str(self._path)

    def __reduce__(self):
        # __reduce__() is needed to properly pickle File objects
        state = (
            # Preserve positional and keyword arguments
            functools.partial(
                self.__class__,
                os.path.join(*self._path.parts),
                size=self._size,
            ),
            # Mandatory positional args (already preserved by partial())
            (),
        )
        return state

    def __init__(self, path, size):
        if isinstance(path, str):
            self._path = pathlib.Path(path)
        elif isinstance(path, os.PathLike):
            self._path = path
        elif isinstance(path, collections.abc.Iterable):
            self._path = pathlib.Path(*path)
        else:
            raise ValueError(f'Path must be str, PathLike or Iterable, not {type(path).__name__}: {path}')

        try:
            self._size = int(size)
        except (ValueError, TypeError):
            raise ValueError(f'Size must be int, not {type(size).__name__}: {size}')

    @property
    def size(self):
        return self._size

    def __getattr__(self, name):
        return getattr(self._path, name)

    def __str__(self):
        return str(self._path)

    def __eq__(self, other):
        if type(other) is type(self):
            return self._path == other._path and self._size == other._size
        elif isinstance(other, os.PathLike):
            return self._path == other
        else:
            return NotImplemented

    def __hash__(self):
        return hash((self._path, self._size))

    def __gt__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._path > other._path

    def __lt__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._path < other._path

    def __ge__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._path >= other._path

    def __le__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._path <= other._path

    def __repr__(self):
        return f'{type(self).__name__}({repr(str(self._path))}, size={self._size})'


class Files(MonitoredList):
    """Deduplicated list of :class:`Files` objects"""
    def __init__(self, files, callback=None):
        if isinstance(files, str):
            files = (files,)
        else:
            files = flatten(files)
        super().__init__(files, callback=callback, type=File)

    def _coerce(self, value):
        if not isinstance(value, self._type):
            raise ValueError(f'Not a File object: {value} ({type(value).__name__})')
        else:
            return value


class Filepath(type(pathlib.Path())):
    """Path-like that makes relative paths equal to their absolute versions"""
    @classmethod
    def _realpath(cls, path):
        if os.path.islink(path):
            return os.path.realpath(str(path))
        elif os.path.isabs(path):
            return str(path)
        else:
            return os.path.join(os.getcwd(), str(path))

    def __eq__(self, other):
        # Use fast cached path if possible
        if isinstance(other, Filepath):
            return hash(self) == hash(other)
        else:
            return self._realpath(self) == self._realpath(other)

    def __hash__(self):
        try:
            return self.__hash
        except AttributeError:
            self.__hash = hash(self._realpath(self))
            return self.__hash


class Filepaths(MonitoredList):
    """Deduplicated list of :class:`Filepath` objects with change callback"""
    def __init__(self, filepaths, callback=None):
        if isinstance(filepaths, str):
            filepaths = (filepaths,)
        else:
            filepaths = list(flatten(filepaths))
        super().__init__(filepaths, callback=callback, type=Filepath)

    def __setitem__(self, index, path):
        path = self._coerce(path)
        # Remove files that are equal to or start with `path`.  This removes
        # directories recursively.  If `path` exists as a file, it is removed
        # and then added again.
        path_removed = False
        for f in tuple(self._items):
            if path == f or path in f.parents:
                self._items.remove(f)
                path_removed = True
        if path.is_dir():
            self.insert(index, path)
        else:
            if path_removed:
                super().insert(index, path)
            else:
                super().__setitem__(index, path)

    def insert(self, index, path):
        path = self._coerce(path)
        try:
            path_is_dir = path.is_dir()
        except OSError as exc:
            raise error.ReadError(getattr(exc, 'errno', None),
                                  getattr(exc, 'filename', None))
        if path_is_dir:
            # Add files in directory recursively
            with self._callback_disabled():
                for i,child in enumerate(sorted(path.iterdir())):
                    self.insert(index + i, child)
            if self._callback is not None:
                self._callback(self)
        else:
            super().insert(index, path)


def is_url(url):
    """Return whether `url` is a valid URL"""
    try:
        u = urllib.parse.urlparse(url)
        u.port  # Trigger 'invalid port' exception
    except Exception:
        return False
    else:
        if not u.scheme or not u.netloc:
            return False
        return True

class URL(str):
    def __new__(cls, s):
        return super().__new__(cls, str(s).replace(' ', '+'))

    def __init__(self, url):
        if not is_url(url):
            raise error.URLError(url)
        else:
            self._parsed = urllib.parse.urlparse(url)

    @property
    def scheme(self): return self._parsed.scheme
    @property
    def netloc(self): return self._parsed.netloc
    @property
    def hostname(self): return self._parsed.hostname
    @property
    def port(self): return self._parsed.port
    @property
    def path(self): return self._parsed.path
    @property
    def params(self): return self._parsed.params
    @property
    def query(self): return self._parsed.query
    @property
    def fragment(self): return self._parsed.fragment

class URLs(MonitoredList):
    """Auto-flattening list of `:class:URL` objects with change callback"""
    def __init__(self, urls, callback=None, _get_known_urls=lambda: ()):
        self._get_known_urls = _get_known_urls
        if isinstance(urls, str):
            if not urls.strip():
                urls = ()
            else:
                urls = (urls,)
        else:
            urls = flatten(urls)
        super().__init__(urls, callback=callback, type=URL)

    def _filter_func(self, url):
        # _get_known_urls is a hack for the Trackers class to deduplicate across
        # multiple tiers.
        if url not in self._items and url not in self._get_known_urls():
            return url


class Trackers(collections.abc.MutableSequence):
    """List of :class:`URLs` instances with change callback"""
    def __init__(self, tiers, callback=None):
        self._callback = None
        self._tiers = []
        if isinstance(tiers, str):
            self.append((tiers,))
        elif isinstance(tiers, collections.abc.Iterable):
            for urls in tiers:
                self.append(urls)
        else:
            raise ValueError(f'Must be str or Iterable, not {type(tiers).__name__}: {repr(tiers)}')
        self._callback = callback

    @property
    def flat(self):
        """Tuple of all URLs of all tiers"""
        return tuple(flatten(self._tiers))

    @contextlib.contextmanager
    def _callback_disabled(self):
        cb = self._callback
        self._callback = None
        yield
        self._callback = cb

    def _tier_changed(self, tier):
        # Auto-remove empty tiers
        if len(tier) == 0:
            self._tiers.remove(tier)
        if self._callback is not None:
            self._callback(self)

    def __getitem__(self, index):
        return self._tiers[index]

    def __setitem__(self, index, value):
        tier = URLs(value, callback=self._tier_changed,
                    _get_known_urls=lambda self=self: self.flat)
        if len(tier) > 0 and tier not in self._tiers:
            self._tiers[index] = tier
        if self._callback is not None:
            self._callback(self)

    def __delitem__(self, index):
        del self._tiers[index]
        if self._callback is not None:
            self._callback(self)

    def insert(self, index, value):
        tier = URLs(value, callback=self._tier_changed,
                    _get_known_urls=lambda self=self: self.flat)
        if len(tier) > 0 and tier not in self._tiers:
            self._tiers.insert(index, tier)
        if self._callback is not None:
            self._callback(self)

    def replace(self, tiers):
        if not isinstance(tiers, Iterable):
            raise ValueError(f'Not an iterable: {tiers!r}')
        with self._callback_disabled():
            self._tiers.clear()
            for urls in tiers:
                self.append(urls)
        if self._callback is not None:
            self._callback(self)

    def clear(self):
        self._tiers.clear()
        if self._callback is not None:
            self._callback(self)

    def __len__(self):
        return len(self._tiers)

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return other._tiers == self._tiers
        elif isinstance(other, collections.abc.Iterable):
            return list(other) == self._tiers
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __add__(self, other):
        if isinstance(other, type(self)):
            other_tiers = other._tiers
        elif isinstance(other, collections.abc.Iterable):
            other_tiers = other
        new_tiers = []
        for tier1,x in itertools.zip_longest(self._tiers, other_tiers):
            if tier1 is None:
                tier1 = []
            if isinstance(x, str) and len(x) > 1:
                new_tier = tier1 + [x]
            elif isinstance(x, collections.abc.Iterable):
                new_tier = tier1 + list(x)
            elif x is not None:
                return NotImplemented
            else:
                new_tier = tier1
            new_tiers.append(new_tier)
        return type(self)(new_tiers, callback=self._callback)

    def __repr__(self):
        return repr(self._tiers)


def download(url, timeout=60):
    """
    Download data from URL

    :raises ConnectionError: if the download fails or the protocol is not
        supported

    :return: the downloaded data
    """
    if timeout <= 0:
        raise error.ConnectionError(url, 'Timed out')
    elif url.startswith('http://') or url.startswith('https://'):
        return download_http(url, timeout=timeout)
    else:
        raise error.ConnectionError(url, 'Unsupported protocol')

def download_http(url, timeout=60):
    try:
        response = urllib.request.urlopen(URL(url), timeout=timeout).read()
    except urllib.error.URLError as e:
        try:
            msg = e.args[0].strerror
        except (AttributeError, IndexError):
            msg = (getattr(e, 'msg', None) or
                   getattr(e, 'strerror', None) or
                   'Failed')
        raise error.ConnectionError(url, msg)
    except socket.timeout:
        raise error.ConnectionError(url, 'Timed out')
    except http.client.HTTPException:
        raise error.ConnectionError(url, 'No HTTP response')
    except (OSError, IOError):
        raise error.ConnectionError(url, 'Unknown error')
    else:
        return response


class Iterable(abc.ABC):
    """
    Iterable that is not a :class:`str`

    This allows you to write

        isinstance(x, Iterable)

    instead of

        isinstance(x, collections.abc.Iterable) and not isinstance(x, str)
    """
    @classmethod
    def __subclasshook__(cls, C):
        if cls is Iterable:
            if issubclass(C, collections.abc.Iterable) and not issubclass(C, str):
                return True
        return False


def key_exists_in_list_or_dict(key, lst_or_dct):
    """True if `lst_or_dct[key]` does not raise an Exception"""
    if isinstance(lst_or_dct, collections.abc.Mapping) and key in lst_or_dct:
        return True
    elif isinstance(lst_or_dct, collections.abc.Sequence):
        min_i, max_i = 0, len(lst_or_dct)
        if min_i <= key < max_i:
            return True
    return False

def assert_type(obj, keys, exp_types, must_exist=True, check=None):
    """
    Raise MetainfoError if value is not of a particular type

    :param obj: The object to check
    :type obj: sequence or mapping
    :param keys: Sequence of keys so that ``obj[key[0]][key[1]]...`` resolves to
        a value
    :type obj: sequence
    :param exp_types: Sequence of allowed types that the value specified by
        `keys` must be an instance of
    :type obj: sequence
    :param bool must_exist: Whether to raise MetainfoError if `keys` does not
         resolve to a value
    :param callable check: Callable that gets the value specified by `keys` and
        returns True if it is OK, False otherwise
    """
    keys = list(keys)
    keychain = []
    while len(keys[:-1]) > 0:
        key = keys.pop(0)
        try:
            obj = obj[key]
        except (KeyError, IndexError):
            break
        keychain.append(key)

    keychain_str = ''.join(f'[{key!r}]' for key in keychain)
    key = keys.pop(0)

    if not key_exists_in_list_or_dict(key, obj):
        if must_exist:
            if keychain_str:
                raise error.MetainfoError(f'Missing {key!r} in {keychain_str}')
            else:
                raise error.MetainfoError(f'Missing {key!r}')

    elif not isinstance(obj[key], exp_types):
        if len(exp_types) > 2:
            exp_types_str = ', '.join(t.__name__ for t in exp_types[:-1])
            exp_types_str += ' or ' + exp_types[-1].__name__
        else:
            exp_types_str = ' or '.join(t.__name__ for t in exp_types)
        type_str = type(obj[key]).__name__
        raise error.MetainfoError(f'{keychain_str}[{key!r}] must be {exp_types_str}, '
                                  f'not {type_str}: {obj[key]!r}')

    elif check is not None and not check(obj[key]):
        raise error.MetainfoError(f"{keychain_str}[{key!r}] is invalid: {obj[key]!r}")


def decode_value(value):
    if isinstance(value, collections.abc.ByteString):
        # WARNING: Torrents can contain binary data (e.g. "pieces" field). You
        #          should handle and remove those separately beforehand.
        return bytes.decode(value, encoding='utf-8', errors='replace')
    elif isinstance(value, collections.abc.Sequence):
        return decode_list(value)
    elif isinstance(value, collections.abc.Mapping):
        return decode_dict(value)
    else:
        return value

def decode_list(lst):
    lst_dec = []
    for value in lst:
        lst_dec.append(decode_value(value))
    return lst_dec

def decode_dict(dct):
    dct_dec = {}
    for key,value in dct.items():
        value_dec = decode_value(value)
        key_dec = decode_value(key)
        dct_dec[key_dec] = value_dec
    return dct_dec


def encode_value(value):
    if type(value) in ENCODE_ALLOWED_TYPES:
        return value
    else:
        for cls,converter in ENCODE_CONVERTERS.items():
            if isinstance(value, cls):
                return converter(value)
        raise ValueError(f'Invalid value: {value!r}')

def encode_list(lst):
    lst_enc = []
    for i,value in enumerate(lst):
        lst_enc.append(encode_value(value))
    return lst_enc

def encode_dict(dct):
    dct_enc = collections.OrderedDict()
    for key,value in sorted(dct.items()):
        if not isinstance(key, str):
            raise ValueError(f'Invalid key: {key!r}')
        key_enc = str(key).encode('utf8')
        value_enc = encode_value(value)
        dct_enc[key_enc] = value_enc
    return dct_enc

ENCODE_ALLOWED_TYPES = (bytes, int)
ENCODE_CONVERTERS = {
    str: lambda val: str(val).encode(encoding='utf-8', errors='replace'),
    float: int,
    bool: int,
    collections.abc.ByteString: bytes,
    collections.abc.Mapping: encode_dict,
    collections.abc.Sequence: encode_list,
    collections.abc.Collection: encode_list,
    datetime: lambda dt: int(dt.timestamp()),
}
