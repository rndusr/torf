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

import os
import math
from fnmatch import fnmatch
from urllib.parse import urlparse
from urllib.parse import quote_plus as urlquote
import collections
import abc
import re
import errno
import io
from datetime import datetime
import itertools

from . import _errors as error


def flatten(items):
    for item in items:
        if isinstance(item, Iterable):
            yield from flatten(item)
        else:
            yield item


_md5sum_regex = re.compile(r'^[0-9a-fA-F]{32}$')
def is_md5sum(value):
    return bool(_md5sum_regex.match(value))


def is_url(url):
    """Return whether `url` is a valid URL"""
    try:
        u = urlparse(url)
        u.port  # Trigger 'invalid port' exception
    except Exception:
        return False
    else:
        if not u.scheme or not u.netloc:
            return False
        return True


def validated_url(url):
    """Return url if valid, raise URLError otherwise"""
    url = str(url)
    if is_url(url):
        return url
    else:
        raise error.URLError(url)


class _FixedSizeFile():
    """
    File-like object with a guaranteed size

    If `filepath` is smaller than `size`, it is padded with null bytes.  If it
    is larger, EOF is reported after reading `size` bytes.

    If reading from `filepath` raises an `OSError`, a `ReadError` is raised.
    """
    def __init__(self, filepath, size):
        try:
            self._stream = io.FileIO(filepath, mode='r')
        except OSError as e:
            raise error.ReadError(e.errno, filepath)
        self.name = str(filepath)
        self._spec_size = size
        self._real_size = os.path.getsize(self.name)
        self._pos = 0

    def __enter__(self):
        return self

    def __exit__(self, _, __, ___):
        if self._stream is not None:
            self._stream.close()

    def read(self, length):
        oldpos = self._pos
        spec_size = self._spec_size
        real_size = self._real_size
        try:
            chunk = self._stream.read(length)
        except OSError as e:
            raise error.ReadError(e.errno, self.name)

        exp_chunk_length = max(0, min(length, spec_size - oldpos))
        chunk_length = len(chunk)
        newpos = self._pos + chunk_length

        if newpos > spec_size:
            # We've read more bytes than we expected.  Shorten `chunk` so that
            # we returned exactly `spec_size` bytes in total.
            chunk = chunk[:exp_chunk_length]

        elif chunk_length < exp_chunk_length:
            # File is shorter than `spec_size`.  Pad chunk with null bytes.
            nulls = exp_chunk_length - chunk_length
            chunk += b'\x00' * nulls
            newpos += nulls

        self._pos = newpos
        return chunk

def read_chunks(filepath, chunksize, filesize=None, prepend=bytes()):
    """
    Generator that yields chunks from file

    If `filesize` is not None, it is the expected size of `filepath` in bytes.
    If `filepath` has a different size, it is transparently cropped or padded
    with null bytes to match the expected size.

    `prepend` is prepended to the content of `filepath`.
    """
    if filesize is not None:
        cm = _FixedSizeFile(filepath, size=filesize)
    else:
        try:
            cm = open(filepath, 'rb')
        except OSError as e:
            raise error.ReadError(e.errno, filepath)
    try:
        chunk = b''
        for pos in range(0, len(prepend), chunksize):
            chunk = prepend[pos:pos + chunksize]
            if len(chunk) == chunksize:
                yield chunk
                chunk = b''

        with cm as f:
            # Fill last chunk from prepended bytes with first bytes from file
            if chunk:
                chunk += f.read(chunksize - len(chunk))
                yield chunk
            while True:
                chunk = f.read(chunksize)
                if chunk:
                    yield chunk
                else:
                    break  # EOF
    except OSError as e:
        raise error.ReadError(e.errno, filepath)


def real_size(path):
    """
    Return size for `path`, which is a (link to a) file or directory

    Raise ReadError on failure
    """
    if os.path.isdir(os.path.realpath(path)):
        size = 0
        def onerror(exc):
            raise error.ReadError(getattr(exc, 'errno', None),
                                  getattr(exc, 'filename', None))
        walker = os.walk(path, followlinks=True, onerror=onerror)
        for dirpath,dirnames,filenames in walker:
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                size += os.path.getsize(os.path.realpath(filepath))
        return size
    else:
        return os.path.getsize(os.path.realpath(path))


def is_power_of_2(num):
    """Return whether `num` is a power of two"""
    if num == 0:
        return False
    log = math.log2(abs(num))
    return int(log) == float(log)


def iterable_startswith(a, b):
    a_len = len(a)
    for i, b_item in enumerate(b):
        if i >= a_len:
            # a can't start with b if b is longer than a
            return False
        if a[i] != b_item:
            return False
    return True


def is_hidden(path):
    """Whether file or directory is hidden"""
    for name in path.split(os.sep):
        if name != '.' and name != '..' and name and name[0] == '.':
            return True
    return False


def is_match(path, pattern):
    for name in path.split(os.sep):
        if fnmatch(name, pattern):
            return True
    return False


def filepaths(path, exclude=(), hidden=True, empty=True):
    """
    Return list of absolute, sorted file paths

    path: Path to file or directory
    exclude: List of file name patterns to exclude
    hidden: Whether to include hidden files
    empty: Whether to include empty files

    Raise PathNotFoundError if path doesn't exist.
    """
    if not os.path.exists(path):
        raise error.PathNotFoundError(path)
    elif not os.access(path, os.R_OK,
                       effective_ids=os.access in os.supports_effective_ids):
        raise error.ReadError(errno.EACCES, path)

    if os.path.isfile(path):
        return [path]
    else:
        filepaths = []
        for dirpath, dirnames, filenames in os.walk(path):
            # Ignore hidden directory
            if not hidden and is_hidden(dirpath):
                continue

            for filename in filenames:
                # Ignore hidden file
                if not hidden and is_hidden(filename):
                    continue

                filepath = os.path.join(dirpath, filename)
                # Ignore excluded file
                if any(is_match(filepath, pattern) for pattern in exclude):
                    continue
                else:
                    # Ignore empty file
                    if empty or os.path.getsize(os.path.realpath(filepath)) > 0:
                        filepaths.append(filepath)

        return sorted(filepaths, key=lambda fp: fp.casefold())

class URLs(collections.abc.MutableSequence):
    """Auto-flattening list of announce URLs with change callback"""
    def __init__(self, urls, callback=None, _get_known_urls=lambda: ()):
        self._callback = None
        self._get_known_urls = _get_known_urls
        if isinstance(urls, str):
            urls = [urls]
        self._urls = []
        for url in flatten(urls):
            self.append(url)
        self._callback = callback

    def _filtered_url(self, url):
        if url not in self._urls and url not in self._get_known_urls():
            return url

    def __getitem__(self, item):
        return self._urls[item]

    def __setitem__(self, item, value):
        url = self._filtered_url(validated_url(value))
        if url is not None:
            self._urls[item] = url
        if self._callback is not None:
            self._callback(self)

    def __delitem__(self, item):
        del self._urls[item]
        if self._callback is not None:
            self._callback(self)

    def insert(self, index, value):
        url = self._filtered_url(validated_url(value))
        if url is not None:
            self._urls.insert(index, url)
        if self._callback is not None:
            self._callback(self)

    def replace(self, urls):
        if not isinstance(urls, Iterable):
            raise ValueError(f'Not an Iterable: {urls!r}')
        self._urls.clear()
        for url in urls:
            self._urls.append(validated_url(url))
        if self._callback is not None:
            self._callback(self)

    def clear(self):
        self._urls.clear()
        if self._callback is not None:
            self._callback(self)

    def __len__(self):
        return len(self._urls)

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return frozenset(other._urls) == frozenset(self._urls)
        elif isinstance(other, collections.abc.Iterable):
            return (len(other) == len(self._urls) and
                    all(url in self._urls for url in other))
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __add__(self, other):
        if isinstance(other, type(self)):
            urls = self._urls + other._urls
        elif isinstance(other, str):
            urls = self._urls + [other]
        elif isinstance(other, collections.abc.Iterable):
            urls = self._urls + list(other)
        else:
            return NotImplemented
        # Do not provide known URLs right away so false duplicates are not detected
        x = type(self)(urls, callback=self._callback)
        x._get_known_urls = self._get_known_urls
        return x

    def __repr__(self):
        return repr(self._urls)


class Trackers(collections.abc.MutableSequence):
    """List of deduplicated :class:`URLs` instances with change callback"""
    def __init__(self, *tiers, callback=None):
        self._callback = None
        self._tiers = []
        for urls in tiers:
            self.append(urls)
        self._callback = callback
        if self._callback is not None:
            self._callback(self)

    @property
    def flat(self):
        """Tuple of all URLs of all tiers"""
        return tuple(flatten(self._tiers))

    def _tier_changed(self, tier):
        # Auto-remove empty tiers
        if len(tier) == 0:
            self._tiers.remove(tier)
        if self._callback is not None:
            self._callback(self)

    def __getitem__(self, item):
        return self._tiers[item]

    def __setitem__(self, item, value):
        tier = URLs(value, callback=self._tier_changed,
                    _get_known_urls=lambda self=self: self.flat)
        if len(tier) > 0 and tier not in self._tiers:
            self._tiers[item] = tier
        if self._callback is not None:
            self._callback(self)

    def __delitem__(self, item):
        del self._tiers[item]
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
        cb = self._callback
        self._callback = None
        self._tiers.clear()
        for urls in tiers:
            self.append(urls)
        self._callback = cb
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
        return type(self)(*new_tiers, callback=self._callback)

    def __repr__(self):
        return repr(self._tiers)


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
    Raise MetainfoError is not of a particular type

    :param obj: The object to check
    :type obj: sequence or mapping
    :param keys: Sequence of keys so that ``obj[key[0]][key[1]]...`` resolves to
        a value
    :type obj: sequence
    :param exp_types: Sequence of types that the value specified by `keys` must
        be an instance of
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
            raise error.MetainfoError(f"Missing {key!r} in {keychain_str}")

    elif not isinstance(obj[key], exp_types):
        if len(exp_types) > 2:
            exp_types_str = ', '.join(t.__name__ for t in exp_types[:-1])
            exp_types_str += ' or ' + exp_types[-1].__name__
        else:
            exp_types_str = ' or '.join(t.__name__ for t in exp_types)
        type_str = type(obj[key]).__name__
        raise error.MetainfoError(f"{keychain_str}[{key!r}] must be {exp_types_str}, "
                                  f"not {type_str}: {obj[key]!r}")

    elif check is not None and not check(obj[key]):
        raise error.MetainfoError(f"{keychain_str}[{key!r}] is invalid: {obj[key]!r}")


def decode_value(value):
    if isinstance(value, collections.abc.ByteString):
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
