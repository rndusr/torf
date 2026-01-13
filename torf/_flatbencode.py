# This is a copy of the dead flatbencode module: https://github.com/acatton/flatbencode
#
# This allows us to fix any potential issues without forking and makes packaging easier.
#
########################################################################################
#
# The MIT License (MIT)
#
# Copyright (c) 2016, Antoine Catton
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import collections
import io
import itertools

ONE_CHAR = 1
INTEGER_START = b'i'
LIST_START = b'l'
DICT_START = b'd'
END = b'e'
NEGATIVE_SIGN = b'-'
STRING_LENGTH_SEPARATOR = b':'

__all__ = ['decode', 'DecodingError', 'encode']


class DecodingError(ValueError):
    pass


def byte_is_integer(b):
    return b'0' <= b <= b'9'


def group_by(it, n):
    """
    >>> list(group_by([1, 2, 3, 4], 2))
    [(1, 2), (3, 4)]
    """
    return zip(*[itertools.islice(it2, i, None, n) for i, it2 in enumerate(itertools.tee(it))])


def list_to_dict(l):
    if not all(isinstance(k, bytes) for k, v in group_by(reversed(l), 2)):
        raise DecodingError
    return collections.OrderedDict(group_by(reversed(l), 2))


def _read_integer(buf):
    c = buf.read(ONE_CHAR)
    if c == NEGATIVE_SIGN:
        negative = True
        c = buf.read(ONE_CHAR)
    else:
        negative = False

    acc = io.BytesIO()
    while c != END:
        if len(c) == 0:
            raise DecodingError
        if not byte_is_integer(c):
            raise DecodingError
        acc.write(c)
        c = buf.read(ONE_CHAR)

    n = acc.getvalue()
    if n.startswith(b'0') and len(n) > 1:  # '03' is illegal
        raise DecodingError
    n = int(n)
    if n == 0 and negative:  # '-0' is illegal
        raise DecodingError
    if negative:
        n = -n
    return n


def _read_length(c, buf):
    acc = io.BytesIO()
    while c != STRING_LENGTH_SEPARATOR:
        if not byte_is_integer(c):
            raise DecodingError
        acc.write(c)
        c = buf.read(ONE_CHAR)
    return int(acc.getvalue())


def _read_string(firstchar, buf):
    length = _read_length(firstchar, buf)
    string = buf.read(length)
    if len(string) != length:
        raise DecodingError
    return string


list_starter = object()
dict_starter = object()


def decode(s):
    buf = io.BufferedReader(io.BytesIO(s))
    buf.seek(0)

    stack = []

    while True:
        c = buf.read(ONE_CHAR)
        if not c:
            raise DecodingError
        if c == END:
            acc = []
            while True:
                if not stack:
                    raise DecodingError
                x = stack.pop()
                if x == list_starter:
                    elem = list(reversed(acc))
                    break
                elif x == dict_starter:
                    elem = list_to_dict(acc)
                    break
                else:
                    acc.append(x)
        elif c == INTEGER_START:
            elem = _read_integer(buf)
        elif c == DICT_START:
            stack.append(dict_starter)
            continue
        elif c == LIST_START:
            stack.append(list_starter)
            continue
        else:
            elem = _read_string(c, buf)

        if not stack:
            end_of_string = not buf.read(ONE_CHAR)
            if not end_of_string:
                raise DecodingError
            return elem
        else:
            stack.append(elem)


def encode(obj):
    def generator(obj):
        if isinstance(obj, dict):
            if not all(isinstance(k, bytes) for k in obj.keys()):
                raise ValueError("Dictionary keys must be strings")
            yield DICT_START
            # Dictionary keys should be sorted according to the BEP-0003:
            #    "Keys must be strings and appear in sorted order (sorted as
            #    raw strings, not alphanumerics)."
            for k in sorted(obj.keys()):
                yield from generator(k)
                yield from generator(obj[k])
            yield END
        elif isinstance(obj, list):
            yield LIST_START
            for elem in obj:
                yield from generator(elem)
            yield END
        elif isinstance(obj, bytes):
            yield str(len(obj)).encode('ascii')
            yield STRING_LENGTH_SEPARATOR
            yield obj
        elif isinstance(obj, int):
            yield INTEGER_START
            yield str(obj).encode('ascii')
            yield END
        else:
            raise ValueError("type {} not supported".format(type(obj)))

    return b''.join(generator(obj))
