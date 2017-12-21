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


class TorfError(Exception):
    """Base exception for all exceptions raised by torf"""
    pass

class URLError(TorfError):
    def __init__(self, url):
        super().__init__(f'URL is not valid: {url!r}')

class PieceSizeError(TorfError):
    def __init__(self, min, max):
        super().__init__(f'Piece size must be between {min} and {max}')

class MetainfoError(TorfError):
    def __init__(self, msg):
        super().__init__(f'Invalid metainfo: {msg}')

class MetainfoParseError(TorfError):
    def __init__(self):
        super().__init__('Invalid metainfo: Invalid torrent')


class PathError(TorfError):
    pass

class PathNotFoundError(PathError):
    def __init__(self, path):
        super().__init__(f'No such file or directory: {path!r}')

class PathEmptyError(PathError):
    def __init__(self, path):
        super().__init__(f'Empty file or directory: {path!r}')

class PathReadError(PathError):
    def __init__(self, path):
        super().__init__(f'Unable to read from {path!r}')
