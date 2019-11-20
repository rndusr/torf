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


class TorfError(Exception):
    """Base exception for all exceptions raised by torf"""
    pass

class URLError(TorfError):
    """Invalid URL"""
    def __init__(self, url):
        self._url = url
        super().__init__(f'{url}: Invalid URL')

    @property
    def url(self):
        """The invalid URL"""
        return self._url


class PieceSizeError(TorfError):
    """Invalid piece size"""
    def __init__(self, size, min=None, max=None):
        self._size = size
        self._min = min
        self._max = max
        if min is not None and max is not None:
            super().__init__(f'Piece size must be between {min} and {max}: {size}')
        else:
            super().__init__(f'Piece size must be a power of 2: {size}')

    @property
    def size(self):
        """The invalid piece size"""
        return self._size

    @property
    def min(self):
        """Smallest allowed piece size or ``None``"""
        return self._min

    @property
    def max(self):
        """Largest allowed piece size or ``None``"""
        return self._max


class MetainfoError(TorfError):
    """Invalid torrent metainfo"""
    def __init__(self, msg):
        super().__init__(f'Invalid metainfo: {msg}')


class ParseError(TorfError):
    """Invalid bencoded metainfo"""
    def __init__(self, filepath=None):
        self._filepath = filepath
        if filepath is None:
            super().__init__('Invalid metainfo format')
        else:
            super().__init__(f'{filepath}: Invalid torrent file format')

    @property
    def filepath(self):
        """Path of the offending torrent file or ``None``"""
        return self._filepath


class PathNotFoundError(TorfError):
    """Path does not exist"""
    def __init__(self, path):
        self._path = path
        super().__init__(f'{path}: No such file or directory')

    @property
    def path(self):
        """Path of the non-existing file or directory"""
        return self._path


class PathEmptyError(TorfError):
    """Empty file or directory or directory that contains only empty files"""
    def __init__(self, path):
        self._path = path
        if os.path.isfile(path):
            super().__init__(f'{path}: Empty file')
        else:
            super().__init__(f'{path}: Empty directory')

    @property
    def path(self):
        """Path of the offending file or directory"""
        return self._path


class IsDirectoryError(TorfError):
    """Expected file/link/etc, but found directory"""
    def __init__(self, path):
        self._path = path
        super().__init__(f'{path}: Is a directory')

    @property
    def path(self):
        """Path of the offending directory"""
        return self._path


class NotDirectoryError(TorfError):
    """Expected (link to) directory, but found something else"""
    def __init__(self, path):
        self._path = path
        super().__init__(f'{path}: Not a directory')

    @property
    def path(self):
        """Path of the offending non-directory"""
        return self._path


class FileSizeError(TorfError):
    """Unexpected file size"""
    def __init__(self, filepath, actual_size, expected_size):
        self._filepath = filepath
        self._actual_size = actual_size
        self._expected_size = expected_size
        super().__init__(f'{filepath}: Unexpected file size: '
                         f'{actual_size} instead of {expected_size} bytes')

    @property
    def filepath(self):
        """Path of the offending file"""
        return self._filepath

    @property
    def actual_size(self):
        """Size as reported by the file system"""
        return self._actual_size

    @property
    def expected_size(self):
        """Size as specified in the metainfo"""
        return self._expected_size


class ContentError(TorfError):
    """On-disk data does not match hashes in metainfo"""
    def __init__(self, piece_index, piece_size, files):
        self._piece_index = piece_index
        self._piece_size = piece_size
        self._files = files
        msg = f'Corruption in piece {piece_index+1}'

        if len(files) > 1:
            # Find the slice in the whole stream of files that contains the
            # corruption (piece_index=0 is the first piece)
            err_i_beg = piece_index * piece_size
            err_i_end = err_i_beg + piece_size

            # Find the files that are covered by the corrupt piece
            corrupt_files = []
            cur_pos = 0
            for filepath,filesize in files:
                # `file` is possibly corrupt if:
                # 1. The corrupt piece STARTS between the beginning and the end
                #    of the file in the stream.
                # 2. The corrupt piece ENDS between the beginning and the end
                #    of the file in the stream.
                # 3. Both beginning and end of the file are between beginning
                #    and end of the corrupt piece (i.e. file fits in one piece).
                file_i_beg = cur_pos
                file_i_end = file_i_beg + filesize
                if (file_i_beg <= err_i_beg < file_i_end or
                    file_i_beg < err_i_end <= file_i_end or
                    (file_i_beg >= err_i_beg and file_i_end < err_i_end)):
                    corrupt_files.append(filepath)
                cur_pos += filesize

            if len(corrupt_files) == 1:
                msg += f' in {corrupt_files[0]}'
            else:
                msg += (', at least one of these files is corrupt: ' +
                        ', '.join(corrupt_files))

        super().__init__(msg)

    @property
    def piece_index(self):
        """Index of the corrupt piece in the stream of concatenated files"""
        return self._piece_index

    @property
    def piece_size(self):
        """Potentially corrupt files"""
        return self._piece_size

    @property
    def files(self):
        """Size of the corrupt piece in bytes"""
        return self._files


class ReadError(TorfError):
    """Unreadable file or stream"""
    def __init__(self, error_code, path=None):
        self._path = path
        msg = os.strerror(error_code) if error_code else 'Unable to read'
        if path is None:
            super().__init__(f'{msg}')
        else:
            super().__init__(f'{path}: {msg}')

    @property
    def path(self):
        """Path of the offending file or ``None``"""
        return self._path


class WriteError(TorfError):
    """Unwritable file or stream"""
    def __init__(self, error_code, path=None):
        self._path = path
        msg = os.strerror(error_code) if error_code else 'Unable to write'
        if path is None:
            super().__init__(f'{msg}')
        else:
            super().__init__(f'{path}: {msg}')

    @property
    def path(self):
        """Path of the offending file or ``None``"""
        return self._path
