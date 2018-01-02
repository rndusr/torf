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
    pass

class URLError(TorfError):
    """Invalid URL"""
    def __init__(self, url):
        super().__init__(f'URL is not valid: {url!r}')

class PieceSizeError(TorfError):
    """Invalid piece size"""
    def __init__(self, min, max):
        super().__init__(f'Piece size must be between {min} and {max}')

class MetainfoError(TorfError):
    """Invalid torrent metainfo"""
    def __init__(self, msg):
        super().__init__(f'Invalid metainfo: {msg}')

class ParseError(TorfError):
    """Invalid bencoded metainfo"""
    def __init__(self, filepath=None):
        if filepath is None:
            super().__init__('Invalid metainfo format')
        else:
            super().__init__(f'Invalid file format: {filepath!r}')

class PathNotFoundError(TorfError):
    """Path does not exist"""
    def __init__(self, path):
        super().__init__(f'{os.strerror(errno.ENOENT)}: {path!r}')

class PathEmptyError(TorfError):
    """Empty file or directory or directory that contains only empty files"""
    def __init__(self, path):
        if os.path.isfile(path):
            super().__init__(f'Empty file: {path!r}')
        else:
            super().__init__(f'Empty directory: {path!r}')

class ReadError(TorfError):
    """Unreadable file"""
    def __init__(self, path, error_code):
        self._errno = error_code
        self._path = path
        super().__init__(f'{os.strerror(error_code)}: {path!r}')

    @property
    def errno(self):
        """Error code (see :mod:`errno`)"""
        return self._errno

    @property
    def path(self):
        """File path that caused the error"""
        return self._path

class WriteError(TorfError):
    """Unwritable file"""
    def __init__(self, path, error_code):
        self._errno = error_code
        self._path = path
        super().__init__(f'{os.strerror(error_code)}: {path!r}')

    @property
    def errno(self):
        """Error code (see :mod:`errno`)"""
        return self._errno

    @property
    def path(self):
        """File path that caused the error"""
        return self._path
