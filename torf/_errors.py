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
import errno

class TorfError(Exception):
    """Base exception for all exceptions raised by torf"""

    @property
    def errno(self):
        """Error code (see :mod:`errno` module)"""
        return self._errno


class URLError(TorfError):
    """Invalid URL"""
    def __init__(self, url):
        self._errno = errno.EINVAL
        super().__init__(f'{url}: Invalid URL')


class PieceSizeError(TorfError):
    """Invalid piece size"""
    def __init__(self, min=None, max=None, size=None):
        self._errno = errno.EINVAL
        if min is not None and max is not None:
            super().__init__(f'Piece size must be between {min} and {max}')
        elif size is not None:
            super().__init__(f'Piece size must be a power of two, {size} is not')


class MetainfoError(TorfError):
    """Invalid torrent metainfo"""
    def __init__(self, msg):
        self._errno = errno.EINVAL
        super().__init__(f'Invalid metainfo: {msg}')


class ParseError(TorfError):
    """Invalid bencoded metainfo"""
    def __init__(self, filepath=None):
        self._errno = errno.EINVAL
        if filepath is None:
            super().__init__('Invalid metainfo format')
        else:
            super().__init__(f'{filepath}: Invalid torrent file format')


class PathNotFoundError(TorfError):
    """Path does not exist"""
    def __init__(self, path):
        self._errno = errno.ENOENT
        super().__init__(f'{path}: {os.strerror(self._errno)}')


class PathEmptyError(TorfError):
    """Empty file or directory or directory that contains only empty files"""
    def __init__(self, path):
        self._errno = errno.ENODATA
        if os.path.isfile(path):
            super().__init__(f'{path}: Empty file')
        else:
            super().__init__(f'{path}: Empty directory')

class ReadError(TorfError):
    """Unreadable file or stream"""
    def __init__(self, error_code, path=None):
        self._errno = error_code
        self._path = path
        msg = os.strerror(error_code) if error_code else 'Unable to read'
        if path is None:
            super().__init__(f'{msg}')
        else:
            super().__init__(f'{path}: {msg}')

    @property
    def path(self):
        """File path that caused the error"""
        return self._path

class WriteError(TorfError):
    """Unwritable file or stream"""
    def __init__(self, error_code, path=None):
        self._errno = error_code
        self._path = path
        msg = os.strerror(error_code) if error_code else 'Unable to write'
        if path is None:
            super().__init__(f'{msg}')
        else:
            super().__init__(f'{path}: {msg}')

    @property
    def path(self):
        """File path that caused the error"""
        return self._path
