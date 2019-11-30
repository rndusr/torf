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


class BdecodeError(TorfError):
    """Failed to decode bencoded byte sequence"""
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


class MagnetError(TorfError):
    """Failed to parse magnet URI"""
    def __init__(self, uri, reason=None):
        self._uri = uri
        if reason is not None:
            super().__init__(f'{uri}: {reason}')
        else:
            super().__init__(f'{uri}: Invalid magnet URI')

    @property
    def uri(self):
        """Invalid URI"""
        return self._uri


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


class VerifyNotDirectoryError(TorfError):
    """Expected file/link/etc, but found directory"""
    def __init__(self, path):
        self._path = path
        super().__init__(f'{path}: Is a directory')

    @property
    def path(self):
        """Path of the offending directory"""
        return self._path


class VerifyIsDirectoryError(TorfError):
    """Expected (link to) directory, but found something else"""
    def __init__(self, path):
        self._path = path
        super().__init__(f'{path}: Not a directory')

    @property
    def path(self):
        """Path of the offending non-directory"""
        return self._path


class VerifyFileSizeError(TorfError):
    """Unexpected file size"""
    def __init__(self, filepath, actual_size, expected_size):
        self._filepath = filepath
        self._actual_size = actual_size
        self._expected_size = expected_size
        if actual_size > expected_size:
            super().__init__(f'{filepath}: Too big: {actual_size} instead of {expected_size} bytes')
        elif actual_size < expected_size:
            super().__init__(f'{filepath}: Too small: {actual_size} instead of {expected_size} bytes')
        else:
            raise RuntimeError(f'Unjustified: actual_size={actual_size} ==  expected_size={expected_size}')

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


class VerifyContentError(TorfError):
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
    def __init__(self, errno, path=None):
        self._errno = errno
        self._path = path
        msg = os.strerror(errno) if errno else 'Unable to read'
        if path is None:
            super().__init__(f'{msg}')
        else:
            super().__init__(f'{path}: {msg}')

    @property
    def path(self):
        """Path of the offending file or ``None``"""
        return self._path

    @property
    def errno(self):
        """POSIX error number from errno.h"""
        return self._errno


class WriteError(TorfError):
    """Unwritable file or stream"""
    def __init__(self, errno, path=None):
        self._errno = errno
        self._path = path
        msg = os.strerror(errno) if errno else 'Unable to write'
        if path is None:
            super().__init__(f'{msg}')
        else:
            super().__init__(f'{path}: {msg}')

    @property
    def path(self):
        """Path of the offending file or ``None``"""
        return self._path

    @property
    def errno(self):
        """POSIX error number from errno.h"""
        return self._errno

