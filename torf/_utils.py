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


import os
import math
from fnmatch import fnmatch
from urllib.parse import urlparse
from urllib.parse import quote_plus as urlquote
from collections import abc, OrderedDict
import re
import errno

from . import _errors as error

_md5sum_regex = re.compile(r'^[0-9a-fA-F]{32}$')
def is_md5sum(value):
    return bool(_md5sum_regex.match(value))


def validated_url(url):
    """Return url if valid, raise URLError otherwise"""
    try:
        u = urlparse(url)
        u.port  # Trigger 'invalid port' exception
    except Exception:
        raise error.URLError(url)
    else:
        if not u.scheme or not u.netloc:
            raise error.URLError(url)
        return url


def read_chunks(filepath, chunk_size):
    """Generator that yields chunks from file"""
    try:
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if chunk:
                    yield chunk
                else:
                    break  # EOF
    except OSError as e:
        raise error.ReadError(e.errno, filepath)


def calc_piece_size(total_size, max_pieces, min_piece_size, max_piece_size):
    """Calculate piece size"""
    ps = 1 << max(0, math.ceil(math.log(total_size / max_pieces, 2)))
    if ps < min_piece_size:
        ps = min_piece_size
    if ps > max_piece_size:
        ps = max_piece_size
    return ps


def is_power_of_2(num):
    """Return whether `num` is a power of two"""
    log = math.log2(num)
    return int(log) == float(log)


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


def key_exists_in_list_or_dict(key, lst_or_dct):
    """True if `lst_or_dct[key]` does not raise an Exception"""
    if isinstance(lst_or_dct, dict) and key in lst_or_dct:
        return True
    elif isinstance(lst_or_dct, list):
        min_i, max_i = 0, len(lst_or_dct)
        if min_i <= key < max_i:
            return True
    return False

def assert_type(lst_or_dct, keys, exp_types, must_exist=True, check=None):
    """
    Raise MetainfoError is not of a particular type

    lst_or_dct: list or dict instance
    keys: Sequence of keys so that `lst_or_dct[key[0]][key[1]]...` resolves to a
          value
    exp_types: Sequence of types that the value specified by `keys` must be an
               instance of
    must_exist: Whether to raise MetainfoError if `keys` does not resolve to a
                value
    check: Callable that gets the value specified by `keys` and returns True if
           it OK, False otherwise
    """
    keys = list(keys)
    keychain = []
    while len(keys[:-1]) > 0:
        key = keys.pop(0)
        try:
            lst_or_dct = lst_or_dct[key]
        except (KeyError, IndexError):
            break
        keychain.append(key)

    keychain_str = ''.join(f'[{key!r}]' for key in keychain)
    key = keys.pop(0)

    if not key_exists_in_list_or_dict(key, lst_or_dct):
        if not must_exist:
            return
        raise error.MetainfoError(f"Missing {key!r} in {keychain_str}")

    elif not isinstance(lst_or_dct[key], exp_types):
        exp_types_str = ' or '.join(t.__name__ for t in exp_types)
        type_str = type(lst_or_dct[key]).__name__
        raise error.MetainfoError(f"{keychain_str}[{key!r}] must be {exp_types_str}, "
                                  f"not {type_str}: {lst_or_dct[key]!r}")

    elif check is not None and not check(lst_or_dct[key]):
        raise error.MetainfoError(f"{keychain_str}[{key!r}] is invalid: {lst_or_dct[key]!r}")


def decode_value(value):
    if isinstance(value, list):
        return decode_list(value)
    elif isinstance(value, abc.Mapping):
        return decode_dict(value)
    else:
        if isinstance(value, bytes):
            return bytes.decode(value, encoding='utf-8', errors='replace')
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
    dct_enc = OrderedDict()
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
    bytearray: bytes,
    abc.Mapping: encode_dict,
    abc.Iterable: encode_list,
}
