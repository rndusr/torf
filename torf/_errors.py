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
        """Error code (see :mod:`errno` module and `errno -l`)"""
        return self._errno


class URLError(TorfError):
    """Invalid URL"""
    def __init__(self, url):
        self._errno = errno.EINVAL
        super().__init__(f'{url}: Invalid URL')


class PieceSizeError(TorfError):
    """Invalid piece size"""
    def __init__(self, size, min=None, max=None):
        self._errno = errno.EINVAL
        if min is not None and max is not None:
            super().__init__(f'Piece size must be between {min} and {max}: {size}')
        elif size is not None:
            super().__init__(f'Piece size must be a power of 2: {size}')


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


class IsDirectoryError(TorfError):
    """Expected file/link/etc, but found directory"""
    def __init__(self, path):
        self._errno = errno.EISDIR
        super().__init__(f'{path}: {os.strerror(self._errno)}')


class FileSizeError(TorfError):
    """Unexpected file size"""
    def __init__(self, file_path, actual_size, expected_size):
        self._errno = errno.EFBIG
        super().__init__(f'{file_path}: Unexpected file size: '
                         f'{actual_size} instead of {expected_size} bytes')


class ContentError(TorfError):
    """On-disk data does not match hashes in metainfo"""
    def __init__(self, piece_index, piece_size, files):
        self._errno = errno.EIO
        msg = f'Corruption in piece {piece_index+1}'

        if len(files) > 1:
            # Find the slice in the whole stream of files that contains the
            # corruption (piece_index=0 is the first piece)
            err_i_beg = piece_index * piece_size
            err_i_end = err_i_beg + piece_size

            # Find the files that are covered by the corrupt piece
            corrupt_files = []
            cur_pos = 0
            for i,file in enumerate(files):
                filesize = os.path.getsize(file)
                file_i_beg = cur_pos
                file_i_end = file_i_beg + filesize

                # `file` is possibly corrupt if:
                # 1. The corrupt piece STARTS between the beginning and the end
                #    of the file in the stream.
                # 2. The corrupt piece ENDS between the beginning and the end
                #    of the file in the stream.
                # 3. Both beginning and end of the file are between beginning
                #    and end of the corrupt piece (i.e. file fits in one piece).
                if (file_i_beg <= err_i_beg < file_i_end or
                    file_i_beg < err_i_end <= file_i_end or
                    (file_i_beg >= err_i_beg and file_i_end < err_i_end)):
                    corrupt_files.append(files[i])
                cur_pos += filesize

            if len(corrupt_files) == 1:
                msg += f' in {corrupt_files[0]}'
            else:
                msg += (', at least one of these files is corrupt: ' +
                        ', '.join(corrupt_files))

        super().__init__(msg)


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
