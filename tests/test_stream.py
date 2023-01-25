import errno
import math
import os
import re
from unittest.mock import Mock, PropertyMock, call

import pytest

from torf import MemoryError, ReadError, TorrentFileStream, VerifyFileSizeError

from . import ComparableException


class File(str):
    byte_counter = 0

    def __new__(cls, path, content=None):
        self = super().__new__(cls, path)

        if isinstance(content, int):
            self.size = content
            self.content = bytearray()
            for _ in range(0, self.size):
                self.content += type(self).byte_counter.to_bytes(1, byteorder='big')
                if type(self).byte_counter >= 255:
                    type(self).byte_counter = 0
                else:
                    type(self).byte_counter += 1

        else:
            self.size = len(content)
            self.content = bytes(content)

        return self

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return super().__eq__(other) and self.size == other.size
        else:
            return NotImplemented

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((str(self), self.size))

    def __repr__(self):
        return f'{type(self).__name__}({str(self)}, {len(self.content)})'

    def write_at(self, directory, content=None):
        (directory / self).parent.mkdir(parents=True, exist_ok=True)
        if content is not None:
            (directory / self).write_bytes(content)
        else:
            (directory / self).write_bytes(self.content)

    @property
    def parts(self):
        return self.split(os.path.sep)


class Torrent:
    def __init__(self, files, piece_size, path=None):
        self.files = files
        self.path = path
        self.piece_size = piece_size
        self.size = sum(f.size for f in files)
        self.pieces = int(self.size / piece_size) + 1

    @property
    def mode(self):
        if len(self.files) == 1 and os.path.sep not in self.files[0]:
            return 'singlefile'
        else:
            return 'multifile'


@pytest.mark.parametrize('file', (None, File('MyTorrent/foo.txt', 123)))
@pytest.mark.parametrize('none_ok', (True, False))
@pytest.mark.parametrize(
    argnames='torrent_content_path, stream_content_path, custom_content_path, exp_content_path',
    argvalues=(
        ('torrent/path', 'stream/path', 'custom/path', 'custom/path'),
        ('torrent/path', 'stream/path', None, 'stream/path'),
        ('torrent/path', 'stream/path', '', ''),
        ('torrent/path', None, None, 'torrent/path'),
        ('torrent/path', None, '', ''),
        ('torrent/path', '', None, ''),
        (None, None, None, None),
        (None, None, '', ''),
        (None, '', None, ''),
        ('', None, None, ''),
    ),
)
def test_get_content_path_from_multifile_torrent(
        torrent_content_path, stream_content_path, custom_content_path, exp_content_path, none_ok, file):
    torrent = Torrent(piece_size=123, files=(File('MyTorrent/a', 1),), path=torrent_content_path)
    tfs = TorrentFileStream(torrent, content_path=stream_content_path)
    if exp_content_path is None and not none_ok:
        with pytest.raises(ValueError, match=r'^Missing content_path argument and torrent has no path specified$'):
            tfs._get_content_path(custom_content_path, none_ok=none_ok, file=file)
    else:
        content_path = tfs._get_content_path(custom_content_path, none_ok=none_ok, file=file)
        if file is not None:
            file_parts = file.split(os.path.sep)
            if not exp_content_path:
                exp_content_path = file
            else:
                exp_content_path = os.path.join(exp_content_path, *file_parts[1:])
        assert content_path == exp_content_path

@pytest.mark.parametrize('file', (None, File('foo.txt', 123)))
@pytest.mark.parametrize('none_ok', (True, False))
@pytest.mark.parametrize(
    argnames='torrent_content_path, stream_content_path, custom_content_path, exp_content_path',
    argvalues=(
        ('torrent/path', 'stream/path', 'custom/path', 'custom/path'),
        ('torrent/path', 'stream/path', None, 'stream/path'),
        ('torrent/path', 'stream/path', '', ''),
        ('torrent/path', None, None, 'torrent/path'),
        ('torrent/path', None, '', ''),
        ('torrent/path', '', None, ''),
        (None, None, None, None),
        (None, None, '', ''),
        (None, '', None, ''),
        ('', None, None, ''),
    ),
)
def test_get_content_path_from_singlefile_torrent(
        torrent_content_path, stream_content_path, custom_content_path, exp_content_path, none_ok, file):
    torrent = Torrent(piece_size=123, files=(File('a', 1),), path=torrent_content_path)
    tfs = TorrentFileStream(torrent, content_path=stream_content_path)
    if exp_content_path is None and not none_ok:
        with pytest.raises(ValueError, match=r'^Missing content_path argument and torrent has no path specified$'):
            tfs._get_content_path(custom_content_path, none_ok=none_ok, file=file)
    else:
        content_path = tfs._get_content_path(custom_content_path, none_ok=none_ok, file=file)
        if exp_content_path:
            assert content_path == exp_content_path
        elif file:
            assert content_path == file
        else:
            assert content_path is None


def test_behaviour_as_context_manager(mocker):
    torrent = Torrent(piece_size=123, files=(File('a', 1),))
    tfs = TorrentFileStream(torrent)
    mocker.patch.object(tfs, 'close')
    assert tfs.close.call_args_list == []
    with tfs as x:
        assert x is tfs
        assert tfs.close.call_args_list == []
    assert tfs.close.call_args_list == [call()]


def test_close():
    torrent = Torrent(piece_size=123, files=(File('a', 1), File('b', 2), File('c', 3)))
    tfs = TorrentFileStream(torrent)
    mocked_open_files = [Mock() for _ in torrent.files]
    tfs._open_files = {
        f'path/to/{i}': mof
        for i,mof in enumerate(mocked_open_files)
    }
    tfs.close()
    for mof in mocked_open_files:
        assert mof.close.call_args_list == [call()]
    assert tfs._open_files == {}


@pytest.mark.parametrize(
    argnames='chunk_size, files, exp_max_piece_index',
    argvalues=(
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaabbbbbbbbbbbb
        (6, [File('a', 6), File('b', 12)], 2),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaabbbbbbbbbbbb
        (6, [File('a', 7), File('b', 12)], 3),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaabbbbbbbbbbbb
        (6, [File('a', 8), File('b', 12)], 3),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaabbbbbbbbbbbbbbbb
        (6, [File('a', 8), File('b', 16)], 3),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaabbbbbbbbbbbbbbbbb
        (6, [File('a', 8), File('b', 17)], 4),
    ),
    ids=lambda v: str(v),
)
def test_max_piece_index(chunk_size, files, exp_max_piece_index):
    torrent = Torrent(piece_size=chunk_size, files=files)
    tfs = TorrentFileStream(torrent)
    assert tfs.max_piece_index == exp_max_piece_index


@pytest.mark.parametrize(
    argnames='chunk_size, files, file, exp_result',
    argvalues=(
        (4, [File('a', 1)], 'x', ValueError('File not specified: x')),
        (3, [File('a', 1)], 'a', 0),
        (3, [File('a', 3)], 'a', 0),
        (3, [File('a', 4)], 'a', 0),
        (3, [File('a', 5)], 'a', 0),

        # 0   1   2   3   4   5   6   7   8   9
        # aaabbbbbccccccccccdddddd
        (4, [File('a', 3), File('b', 5), File('c', 10), File('d', 6)], 'a', 0),
        (4, [File('a', 3), File('b', 5), File('c', 10), File('d', 6)], 'b', 3),
        (4, [File('a', 3), File('b', 5), File('c', 10), File('d', 6)], 'c', 8),
        (4, [File('a', 3), File('b', 5), File('c', 10), File('d', 6)], 'd', 18),

        # 0   1   2   3   4   5   6   7   8   9
        # abcdddddd
        (4, [File('a', 1), File('b', 1), File('c', 1), File('d', 6)], 'a', 0),
        (4, [File('a', 1), File('b', 1), File('c', 1), File('d', 6)], 'b', 1),
        (4, [File('a', 1), File('b', 1), File('c', 1), File('d', 6)], 'c', 2),
        (4, [File('a', 1), File('b', 1), File('c', 1), File('d', 6)], 'd', 3),

        # 0   1   2   3   4   5   6   7   8   9
        # aaaaaabcd
        (4, [File('a', 6), File('b', 1), File('c', 1), File('d', 1)], 'a', 0),
        (4, [File('a', 6), File('b', 1), File('c', 1), File('d', 1)], 'b', 6),
        (4, [File('a', 6), File('b', 1), File('c', 1), File('d', 1)], 'c', 7),
        (4, [File('a', 6), File('b', 1), File('c', 1), File('d', 1)], 'd', 8),
    ),
    ids=lambda v: str(v),
)
def test_get_file_position(chunk_size, files, file, exp_result):
    torrent = Torrent(piece_size=chunk_size, files=files)
    tfs = TorrentFileStream(torrent)
    if isinstance(exp_result, BaseException):
        with pytest.raises(type(exp_result), match=rf'^{re.escape(str(exp_result))}$'):
            tfs.get_file_position(file)
    else:
        assert tfs.get_file_position(file) == exp_result


@pytest.mark.parametrize(
    argnames='chunk_size, files, position, exp_result',
    argvalues=(
        # 0   1   2   3   4   5
        # abc
        (4, [File('a', 1), File('b', 1), File('c', 1)],
         -1, ValueError('position is out of bounds (0 - 2): -1')),
        (4, [File('a', 1), File('b', 1), File('c', 1)], 0, 'a'),
        (4, [File('a', 1), File('b', 1), File('c', 1)], 1, 'b'),
        (4, [File('a', 1), File('b', 1), File('c', 1)], 2, 'c'),
        (4, [File('a', 1), File('b', 1), File('c', 1)],
         3, ValueError('position is out of bounds (0 - 2): 3')),

        # 0   1   2   3   4   5
        # aaabbbbbcccccccdddddd
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)],
         -1, ValueError('position is out of bounds (0 - 20): -1')),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 0, 'a'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 1, 'a'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 2, 'a'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 3, 'b'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 4, 'b'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 5, 'b'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 6, 'b'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 7, 'b'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 8, 'c'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 9, 'c'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 10, 'c'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 11, 'c'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 12, 'c'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 13, 'c'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 14, 'c'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 15, 'd'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 16, 'd'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 17, 'd'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 18, 'd'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 19, 'd'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)], 20, 'd'),
        (4, [File('a', 3), File('b', 5), File('c', 7), File('d', 6)],
         21, ValueError('position is out of bounds (0 - 20): 21')),
    ),
)
def test_get_file_at_position(chunk_size, files, position, exp_result, mocker):
    torrent = Torrent(piece_size=chunk_size, files=files)
    tfs = TorrentFileStream(torrent)

    def mock_content_path(content_path, none_ok, file):
        return f'{content_path} / {none_ok} / {file}'

    mocker.patch.object(tfs, '_get_content_path', side_effect=mock_content_path)

    if isinstance(exp_result, BaseException):
        with pytest.raises(type(exp_result), match=rf'^{re.escape(str(exp_result))}$'):
            tfs.get_file_at_position(position, content_path='my/custom/path')
    else:
        exp_file = [f for f in files if f == exp_result][0]
        exp_filepath = f'my/custom/path / True / {exp_file}'
        filepath = tfs.get_file_at_position(position, content_path='my/custom/path')
        assert filepath == exp_filepath


@pytest.mark.parametrize(
    argnames='chunk_size, files, exp_piece_indexes',
    argvalues=(
        (3, [File('a', 1)], {'a': [0]}),
        (3, [File('a', 2)], {'a': [0]}),
        (3, [File('a', 3)], {'a': [0]}),
        (3, [File('a', 4)], {'a': [0, 1]}),
        (3, [File('a', 5)], {'a': [0, 1]}),
        (3, [File('a', 6)], {'a': [0, 1]}),
        (3, [File('a', 7)], {'a': [0, 1, 2]}),

        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaabbbbbbbbbbbbbbbbbbbbbbbc
        (6, [File('a', 5), File('b', 23), File('c', 1)], {'a': [0], 'b': [0, 1, 2, 3, 4], 'c': [4]}),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaabbbbbbbbbbbbbbbbbbbbbbbc
        (6, [File('a', 6), File('b', 23), File('c', 1)], {'a': [0], 'b': [1, 2, 3, 4], 'c': [4]}),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaabbbbbbbbbbbbbbbbbbbbbbbc
        (6, [File('a', 7), File('b', 23), File('c', 1)], {'a': [0, 1], 'b': [1, 2, 3, 4], 'c': [5]}),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaabbbbbbbbbbbbbbbbbbbbbbbc
        (6, [File('a', 8), File('b', 23), File('c', 1)], {'a': [0, 1], 'b': [1, 2, 3, 4, 5], 'c': [5]}),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaaaaabbbbbbbbbbbbbbbbbbbc
        (6, [File('a', 11), File('b', 19), File('c', 1)], {'a': [0, 1], 'b': [1, 2, 3, 4], 'c': [5]}),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaaaaaabbbbbbbbbbbbbbbbbbbc
        (6, [File('a', 12), File('b', 19), File('c', 1)], {'a': [0, 1], 'b': [2, 3, 4, 5], 'c': [5]}),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbc
        (6, [File('a', 13), File('b', 19), File('c', 1)], {'a': [0, 1, 2], 'b': [2, 3, 4, 5], 'c': [5]}),
    ),
    ids=lambda v: repr(v),
)
def test_get_piece_indexes_of_file_nonexclusive(chunk_size, files, exp_piece_indexes):
    torrent = Torrent(piece_size=chunk_size, files=files)
    tfs = TorrentFileStream(torrent)
    for filename, exp_indexes in exp_piece_indexes.items():
        file = [f for f in torrent.files if f == filename][0]
        assert tfs.get_piece_indexes_of_file(file) == exp_indexes


@pytest.mark.parametrize(
    argnames='chunk_size, files, exp_piece_indexes',
    argvalues=(
        (3, [File('a', 1)], {'a': [0]}),
        (3, [File('a', 2)], {'a': [0]}),
        (3, [File('a', 3)], {'a': [0]}),
        (3, [File('a', 4)], {'a': [0, 1]}),
        (3, [File('a', 5)], {'a': [0, 1]}),
        (3, [File('a', 6)], {'a': [0, 1]}),
        (3, [File('a', 7)], {'a': [0, 1, 2]}),

        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaabbbbbbbbbbbbbbbbbbbbbbbc
        (6, [File('a', 5), File('b', 23), File('c', 1)], {'a': [], 'b': [1, 2, 3], 'c': []}),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaabbbbbbbbbbbbbbbbbbbbbbbc
        (6, [File('a', 6), File('b', 23), File('c', 1)], {'a': [0], 'b': [1, 2, 3], 'c': []}),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaabbbbbbbbbbbbbbbbbbbbbbbc
        (6, [File('a', 7), File('b', 23), File('c', 1)], {'a': [0], 'b': [2, 3, 4], 'c': [5]}),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaabbbbbbbbbbbbbbbbbbbbbbbc
        (6, [File('a', 8), File('b', 23), File('c', 1)], {'a': [0], 'b': [2, 3, 4], 'c': []}),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaaaaabbbbbbbbbbbbbbbbbbbc
        (6, [File('a', 11), File('b', 19), File('c', 1)], {'a': [0], 'b': [2, 3, 4], 'c': [5]}),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaaaaaabbbbbbbbbbbbbbbbbbbc
        (6, [File('a', 12), File('b', 19), File('c', 1)], {'a': [0, 1], 'b': [2, 3, 4], 'c': []}),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbc
        (6, [File('a', 13), File('b', 19), File('c', 1)], {'a': [0, 1], 'b': [3, 4], 'c': []}),
    ),
    ids=lambda v: repr(v),
)
def test_get_piece_indexes_of_file_exclusive(chunk_size, files, exp_piece_indexes):
    torrent = Torrent(piece_size=chunk_size, files=files)
    tfs = TorrentFileStream(torrent)
    for filename, exp_indexes in exp_piece_indexes.items():
        file = [f for f in torrent.files if f == filename][0]
        assert tfs.get_piece_indexes_of_file(file, exclusive=True) == exp_indexes


@pytest.mark.parametrize(
    argnames='chunk_size, files, first_byte_indexes, last_byte_indexes, exp_files',
    argvalues=(
        # Files smaller than piece size
        # 0     1     2     3     4     5     6     7     8     9     0
        # abbbccccccccccccccd
        (6, [File('a', 1), File('b', 3), File('c', 14), File('d', 1)], range(0, 1), range(0, 1), ['a']),
        (6, [File('a', 1), File('b', 3), File('c', 14), File('d', 1)], range(1, 4), range(1, 4), ['b']),
        (6, [File('a', 1), File('b', 3), File('c', 14), File('d', 1)], range(4, 18), range(4, 18), ['c']),
        (6, [File('a', 1), File('b', 3), File('c', 14), File('d', 1)], range(18, 19), range(18, 19), ['d']),

        (6, [File('a', 1), File('b', 3), File('c', 14), File('d', 1)], range(1, 4), range(18, 19), ['b', 'c', 'd']),
        (6, [File('a', 1), File('b', 3), File('c', 14), File('d', 1)], range(4, 18), range(18, 19), ['c', 'd']),
        (6, [File('a', 1), File('b', 3), File('c', 14), File('d', 1)], range(18, 19), range(18, 19), ['d']),

        (6, [File('a', 1), File('b', 3), File('c', 14), File('d', 1)], range(0, 1), range(0, 1), ['a']),
        (6, [File('a', 1), File('b', 3), File('c', 14), File('d', 1)], range(0, 1), range(1, 4), ['a', 'b']),
        (6, [File('a', 1), File('b', 3), File('c', 14), File('d', 1)], range(0, 1), range(4, 18), ['a', 'b', 'c']),
        (6, [File('a', 1), File('b', 3), File('c', 14), File('d', 1)], range(0, 1), range(18, 19), ['a', 'b', 'c', 'd']),

        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaaaaabccddd
        (6, [File('a', 11), File('b', 1), File('c', 2), File('d', 3)], range(0, 11), range(16, 17), ['a', 'b', 'c', 'd']),
        (6, [File('a', 11), File('b', 1), File('c', 2), File('d', 3)], range(11, 12), range(16, 17), ['b', 'c', 'd']),
        (6, [File('a', 11), File('b', 1), File('c', 2), File('d', 3)], range(12, 14), range(16, 17), ['c', 'd']),
        (6, [File('a', 11), File('b', 1), File('c', 2), File('d', 3)], range(14, 17), range(16, 17), ['d']),
        (6, [File('a', 11), File('b', 1), File('c', 2), File('d', 3)], range(17, 20), range(16, 17), []),

        (6, [File('a', 11), File('b', 1), File('c', 2), File('d', 3)], range(0, 1), range(0, 11), ['a']),
        (6, [File('a', 11), File('b', 1), File('c', 2), File('d', 3)], range(0, 1), range(11, 12), ['a', 'b']),
        (6, [File('a', 11), File('b', 1), File('c', 2), File('d', 3)], range(0, 1), range(12, 14), ['a', 'b', 'c']),
        (6, [File('a', 11), File('b', 1), File('c', 2), File('d', 3)], range(0, 1), range(14, 17), ['a', 'b', 'c', 'd']),
        (6, [File('a', 11), File('b', 1), File('c', 2), File('d', 3)], range(0, 1), range(17, 20), ['a', 'b', 'c', 'd']),

        (6, [File('a', 11), File('b', 1), File('c', 2), File('d', 3)], range(0, 11), range(0, 11), ['a']),
        (6, [File('a', 11), File('b', 1), File('c', 2), File('d', 3)], range(11, 12), range(11, 12), ['b']),
        (6, [File('a', 11), File('b', 1), File('c', 2), File('d', 3)], range(12, 14), range(12, 14), ['c']),
        (6, [File('a', 11), File('b', 1), File('c', 2), File('d', 3)], range(14, 17), range(14, 17), ['d']),
        (6, [File('a', 11), File('b', 1), File('c', 2), File('d', 3)], range(17, 20), range(17, 20), []),

        # All files are bigger than piece size
        # 0           1           2           3           3           4           5           6           7           8
        # aaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccccccccccccccccccccccccccccc
        (12, [File('a', 11), File('b', 49), File('c', 50)], range(0, 11), range(109, 110), ['a', 'b', 'c']),
        (12, [File('a', 11), File('b', 49), File('c', 50)], range(11, 60), range(109, 110), ['b', 'c']),
        (12, [File('a', 11), File('b', 49), File('c', 50)], range(60, 110), range(109, 110), ['c']),
        (12, [File('a', 11), File('b', 49), File('c', 50)], range(110, 112), range(110, 112), []),

        (12, [File('a', 11), File('b', 49), File('c', 50)], range(0, 1), range(0, 11), ['a']),
        (12, [File('a', 11), File('b', 49), File('c', 50)], range(0, 1), range(11, 60), ['a', 'b']),
        (12, [File('a', 11), File('b', 49), File('c', 50)], range(0, 1), range(60, 110), ['a', 'b', 'c']),
        (12, [File('a', 11), File('b', 49), File('c', 50)], range(0, 1), range(110, 112), ['a', 'b', 'c']),

        (12, [File('a', 11), File('b', 49), File('c', 50)], range(0, 11), range(0, 11), ['a']),
        (12, [File('a', 11), File('b', 49), File('c', 50)], range(11, 60), range(11, 60), ['b']),
        (12, [File('a', 11), File('b', 49), File('c', 50)], range(60, 110), range(60, 110), ['c']),
        (12, [File('a', 11), File('b', 49), File('c', 50)], range(110, 112), range(110, 112), []),
    ),
    ids=lambda v: str(v),
)
def test_get_files_at_byte_range(chunk_size, first_byte_indexes, last_byte_indexes, files, exp_files, mocker):
    torrent = Torrent(piece_size=chunk_size, files=files)
    tfs = TorrentFileStream(torrent)

    first_byte_indexes = tuple(first_byte_indexes)
    last_byte_indexes = tuple(last_byte_indexes)
    assert first_byte_indexes, first_byte_indexes
    assert last_byte_indexes, last_byte_indexes

    def mock_content_path(content_path, none_ok, file):
        return f'{content_path} / {none_ok} / {file}'

    mocker.patch.object(tfs, '_get_content_path', side_effect=mock_content_path)

    for first_byte_index in first_byte_indexes:
        for last_byte_index in last_byte_indexes:
            if first_byte_index <= last_byte_index:
                files = tfs.get_files_at_byte_range(
                    first_byte_index,
                    last_byte_index,
                    content_path='my/custom_path',
                )
                assert files == [f'my/custom_path / True / {file}'
                                 for file in exp_files]


@pytest.mark.parametrize(
    argnames='chunk_size, files, file, exp_byte_range',
    argvalues=(
        # All files in one piece
        # 0     1     2     3     4     5     6     7     8     9     0
        # abc
        (6, [File('a', 1), File('b', 1), File('c', 1)], 'a', (0, 0)),
        (6, [File('a', 1), File('b', 1), File('c', 1)], 'b', (1, 1)),
        (6, [File('a', 1), File('b', 1), File('c', 1)], 'c', (2, 2)),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aabccc
        (6, [File('a', 2), File('b', 1), File('c', 3)], 'a', (0, 1)),
        (6, [File('a', 2), File('b', 1), File('c', 3)], 'b', (2, 2)),
        (6, [File('a', 2), File('b', 1), File('c', 3)], 'c', (3, 5)),

        # First piece contains multiple files
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaabbbcccccccccccccccccccc
        (6, [File('a', 3), File('b', 3), File('c', 20)], 'a', (0, 2)),
        (6, [File('a', 3), File('b', 3), File('c', 20)], 'b', (3, 5)),
        (6, [File('a', 3), File('b', 3), File('c', 20)], 'c', (6, 25)),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaabbbbbbbbbbcccccccccccccccccccc
        (6, [File('a', 5), File('b', 10), File('c', 20)], 'a', (0, 4)),
        (6, [File('a', 5), File('b', 10), File('c', 20)], 'b', (5, 14)),
        (6, [File('a', 5), File('b', 10), File('c', 20)], 'c', (15, 34)),

        # Middle piece contains multiple files
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaaaaaaaaaaaaaabbcccccccccccccccccccccccccccccc
        (6, [File('a', 20), File('b', 2), File('c', 30)], 'a', (0, 19)),
        (6, [File('a', 20), File('b', 2), File('c', 30)], 'b', (20, 21)),
        (6, [File('a', 20), File('b', 2), File('c', 30)], 'c', (22, 51)),

        # Last piece contains multiple files
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaaaabbbbbbbbbbbbbbbc
        (6, [File('a', 10), File('b', 15), File('c', 1)], 'a', (0, 9)),
        (6, [File('a', 10), File('b', 15), File('c', 1)], 'b', (10, 24)),
        (6, [File('a', 10), File('b', 15), File('c', 1)], 'c', (25, 25)),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaaaaaaaaaaaaaabbbc
        (6, [File('a', 20), File('b', 3), File('c', 1)], 'a', (0, 19)),
        (6, [File('a', 20), File('b', 3), File('c', 1)], 'b', (20, 22)),
        (6, [File('a', 20), File('b', 3), File('c', 1)], 'c', (23, 23)),
    ),
    ids=lambda v: str(v),
)
def test_get_byte_range_of_file(chunk_size, files, file, exp_byte_range):
    torrent = Torrent(piece_size=chunk_size, files=files)
    tfs = TorrentFileStream(torrent)
    file = {str(f): f for f in torrent.files}[file]
    byte_range = tfs.get_byte_range_of_file(file)
    assert byte_range == exp_byte_range


@pytest.mark.parametrize(
    argnames='chunk_size, files, piece_index, exp_return_value',
    argvalues=(
        # First piece contains multiple files
        # 0     1     2     3     4     5     6     7     8     9     0
        # aabbbccccccccccccccccccccccccccccccccccccccccccccccccc
        (6, [File('a', 2), File('b', 3), File('c', 49)], -1,
         ValueError('piece_index is out of bounds (0 - 8): -1')),
        (6, [File('a', 2), File('b', 3), File('c', 49)], 0, ['a', 'b', 'c']),
        (6, [File('a', 2), File('b', 3), File('c', 49)], 1, ['c']),
        (6, [File('a', 2), File('b', 3), File('c', 49)], 2, ['c']),
        (6, [File('a', 2), File('b', 3), File('c', 49)], 8, ['c']),
        (6, [File('a', 2), File('b', 3), File('c', 49)], 9,
         ValueError('piece_index is out of bounds (0 - 8): 9')),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aabbbbccccccccccccccccccccccccccccccccccccccccccccccccc
        (6, [File('a', 2), File('b', 4), File('c', 49)], -1,
         ValueError('piece_index is out of bounds (0 - 9): -1')),
        (6, [File('a', 2), File('b', 4), File('c', 49)], 0, ['a', 'b']),
        (6, [File('a', 2), File('b', 4), File('c', 49)], 1, ['c']),
        (6, [File('a', 2), File('b', 4), File('c', 49)], 2, ['c']),
        (6, [File('a', 2), File('b', 4), File('c', 49)], 8, ['c']),
        (6, [File('a', 2), File('b', 4), File('c', 49)], 9, ['c']),
        (6, [File('a', 2), File('b', 4), File('c', 49)], 10,
         ValueError('piece_index is out of bounds (0 - 9): 10')),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaabbbbccccccccccccccccccccccccccccccccccccccccccccccccc
        (6, [File('a', 3), File('b', 4), File('c', 49)], -1,
         ValueError('piece_index is out of bounds (0 - 9): -1')),
        (6, [File('a', 3), File('b', 4), File('c', 49)], 0, ['a', 'b']),
        (6, [File('a', 3), File('b', 4), File('c', 49)], 1, ['b', 'c']),
        (6, [File('a', 3), File('b', 4), File('c', 49)], 2, ['c']),
        (6, [File('a', 3), File('b', 4), File('c', 49)], 8, ['c']),
        (6, [File('a', 3), File('b', 4), File('c', 49)], 9, ['c']),
        (6, [File('a', 3), File('b', 4), File('c', 49)], 10,
         ValueError('piece_index is out of bounds (0 - 9): 10')),

        # Middle piece contains multiple files
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaaaaaaabcccdddddddddddddddddddddddddddddddddddd
        (6, [File('a', 13), File('b', 1), File('c', 3), File('d', 36)], -1,
         ValueError('piece_index is out of bounds (0 - 8): -1')),
        (6, [File('a', 13), File('b', 1), File('c', 3), File('d', 36)], 0, ['a']),
        (6, [File('a', 13), File('b', 1), File('c', 3), File('d', 36)], 1, ['a']),
        (6, [File('a', 13), File('b', 1), File('c', 3), File('d', 36)], 2, ['a', 'b', 'c', 'd']),
        (6, [File('a', 13), File('b', 1), File('c', 3), File('d', 36)], 3, ['d']),
        (6, [File('a', 13), File('b', 1), File('c', 3), File('d', 36)], 8, ['d']),
        (6, [File('a', 13), File('b', 1), File('c', 3), File('d', 36)], 9,
         ValueError('piece_index is out of bounds (0 - 8): 9')),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaaaaaaabbcccdddddddddddddddddddddddddddddddddddd
        (6, [File('a', 13), File('b', 2), File('c', 3), File('d', 36)], -1,
         ValueError('piece_index is out of bounds (0 - 8): -1')),
        (6, [File('a', 13), File('b', 2), File('c', 3), File('d', 36)], 0, ['a']),
        (6, [File('a', 13), File('b', 2), File('c', 3), File('d', 36)], 1, ['a']),
        (6, [File('a', 13), File('b', 2), File('c', 3), File('d', 36)], 2, ['a', 'b', 'c']),
        (6, [File('a', 13), File('b', 2), File('c', 3), File('d', 36)], 3, ['d']),
        (6, [File('a', 13), File('b', 2), File('c', 3), File('d', 36)], 8, ['d']),
        (6, [File('a', 13), File('b', 2), File('c', 3), File('d', 36)], 9,
         ValueError('piece_index is out of bounds (0 - 8): 9')),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaaaaaaabbccccdddddddddddddddddddddddddddddddddddd
        (6, [File('a', 13), File('b', 2), File('c', 4), File('d', 36)], -1,
         ValueError('piece_index is out of bounds (0 - 9): -1')),
        (6, [File('a', 13), File('b', 2), File('c', 4), File('d', 36)], 0, ['a']),
        (6, [File('a', 13), File('b', 2), File('c', 4), File('d', 36)], 1, ['a']),
        (6, [File('a', 13), File('b', 2), File('c', 4), File('d', 36)], 2, ['a', 'b', 'c']),
        (6, [File('a', 13), File('b', 2), File('c', 4), File('d', 36)], 3, ['c', 'd']),
        (6, [File('a', 13), File('b', 2), File('c', 4), File('d', 36)], 8, ['d']),
        (6, [File('a', 13), File('b', 2), File('c', 4), File('d', 36)], 9, ['d']),
        (6, [File('a', 13), File('b', 2), File('c', 4), File('d', 36)], 10,
         ValueError('piece_index is out of bounds (0 - 9): 10')),

        # Last piece contains multiple files
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaaaaaaaaaaaaaaaaaaaaaaabccddd
        (6, [File('a', 29), File('b', 1), File('c', 2), File('d', 3)], 3, ['a']),
        (6, [File('a', 29), File('b', 1), File('c', 2), File('d', 3)], 4, ['a', 'b']),
        (6, [File('a', 29), File('b', 1), File('c', 2), File('d', 3)], 5, ['c', 'd']),
        (6, [File('a', 29), File('b', 1), File('c', 2), File('d', 3)], 6,
         ValueError('piece_index is out of bounds (0 - 5): 6')),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaaaaaaaaaaaaaaaaaaaaaaabbccddd
        (6, [File('a', 29), File('b', 2), File('c', 2), File('d', 3)], 3, ['a']),
        (6, [File('a', 29), File('b', 2), File('c', 2), File('d', 3)], 4, ['a', 'b']),
        (6, [File('a', 29), File('b', 2), File('c', 2), File('d', 3)], 5, ['b', 'c', 'd']),
        (6, [File('a', 29), File('b', 2), File('c', 2), File('d', 3)], 6,
         ValueError('piece_index is out of bounds (0 - 5): 6')),
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaaaaaaaaaaaaaaaaaaaaaaabbbccddd
        (6, [File('a', 29), File('b', 3), File('c', 2), File('d', 3)], 3, ['a']),
        (6, [File('a', 29), File('b', 3), File('c', 2), File('d', 3)], 4, ['a', 'b']),
        (6, [File('a', 29), File('b', 3), File('c', 2), File('d', 3)], 5, ['b', 'c', 'd']),
        (6, [File('a', 29), File('b', 3), File('c', 2), File('d', 3)], 6, ['d']),
        (6, [File('a', 29), File('b', 3), File('c', 2), File('d', 3)], 7,
         ValueError('piece_index is out of bounds (0 - 6): 7')),
    ),
    ids=lambda v: str(v),
)
def test_get_files_at_piece_index(chunk_size, files, piece_index, exp_return_value, mocker):
    torrent = Torrent(piece_size=chunk_size, files=files)
    tfs = TorrentFileStream(torrent)

    def mock_content_path(content_path, none_ok, file):
        return f'{content_path} / {none_ok} / {file}'

    mocker.patch.object(tfs, '_get_content_path', side_effect=mock_content_path)

    if isinstance(exp_return_value, BaseException):
        with pytest.raises(type(exp_return_value), match=rf'^{re.escape(str(exp_return_value))}$'):
            tfs.get_files_at_piece_index(piece_index, content_path='my/custom/path')
    else:
        files = tfs.get_files_at_piece_index(piece_index, content_path='my/custom/path')
        assert files == [f'my/custom/path / True / {file}'
                         for file in exp_return_value]


@pytest.mark.parametrize(
    argnames='chunk_size, files, file, relative_piece_indexes, exp_absolute_indexes',
    argvalues=(
        # Multiple files in one piece
        # 0     1     2     3     4     5     6     7     8     9     0
        # abc
        (6, [File('a', 1), File('b', 1), File('c', 1)], 'a', (0, 1, 1000, -1, -2, -1000), [0]),
        (6, [File('a', 1), File('b', 1), File('c', 1)], 'b', (0, 1, 1000, -1, -2, -1000), [0]),
        (6, [File('a', 1), File('b', 1), File('c', 1)], 'c', (0, 1, 1000, -1, -2, -1000), [0]),

        # First piece contains multiple files
        # 0     1     2     3     4     5     6     7     8     9     0
        # aabbbcccccccccccccccccccccccccccccccccccccccccccccccccc
        (6, [File('a', 2), File('b', 3), File('c', 50)], 'a', (0, 1, 1000, -1, -2, -1000), [0]),
        (6, [File('a', 2), File('b', 3), File('c', 50)], 'b', (0, 1, 1000, -1, -2, -1000), [0]),
        (6, [File('a', 2), File('b', 3), File('c', 50)], 'c', (0, 1, 1000, -1, -2, -1000), [0, 1, 8, 9]),

        # Middle piece contains multiple files
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaaaaaaabbcccdddddddddddddddddddddddddddddddddddd
        (6, [File('a', 13), File('b', 2), File('c', 3), File('d', 36)], 'a', (0, 1, 1000, -1, -2, -1000), [0, 1, 2]),
        (6, [File('a', 13), File('b', 2), File('c', 3), File('d', 36)], 'b', (0, 1, 1000, -1, -2, -1000), [2]),
        (6, [File('a', 13), File('b', 2), File('c', 3), File('d', 36)], 'c', (0, 1, 1000, -1, -2, -1000), [2]),
        (6, [File('a', 13), File('b', 2), File('c', 3), File('d', 36)], 'd', (0, 1, 1000, -1, -2, -1000), [3, 4, 7, 8]),

        # Last piece contains multiple files
        # 0     1     2     3     4     5     6     7     8     9     0
        # aaaaaaaaaaaaaaaaaaaaaaaaaaaaaabbcddd
        (6, [File('a', 30), File('b', 2), File('c', 1), File('d', 3)], 'a', (0, 1, 1000, -1, -2, -1000), [0, 1, 3, 4]),
        (6, [File('a', 30), File('b', 2), File('c', 1), File('d', 3)], 'b', (0, 1, 1000, -1, -2, -1000), [5]),
        (6, [File('a', 30), File('b', 2), File('c', 1), File('d', 3)], 'c', (0, 1, 1000, -1, -2, -1000), [5]),
        (6, [File('a', 30), File('b', 2), File('c', 1), File('d', 3)], 'd', (0, 1, 1000, -1, -2, -1000), [5]),
    ),
    ids=lambda v: str(v),
)
def test_get_absolute_piece_indexes(chunk_size, files, file, relative_piece_indexes, exp_absolute_indexes):
    torrent = Torrent(piece_size=chunk_size, files=files)
    tfs = TorrentFileStream(torrent)
    file = [f for f in files if f == file][0]
    assert tfs.get_absolute_piece_indexes(file, relative_piece_indexes) == exp_absolute_indexes


@pytest.mark.parametrize('ignore_empty_files', (True, False), ids=('ignore_empty_files', 'include_empty_files'))
@pytest.mark.parametrize(
    argnames='prefile_size, postfile_size',
    argvalues=(
        (0, 0),
        (11, 0), (12, 0), (13, 0),
        (0, 11), (0, 12), (0, 13),
        (11, 11), (12, 12), (13, 13),
        (11, 12), (11, 12),
        (11, 13), (13, 11),
        (12, 13), (13, 12),
    ),
)
@pytest.mark.parametrize(
    argnames='file, relative_piece_indexes, exp_indexes',
    argvalues=(
        (File('foo', 11), (0, 1, -1, -2), [0]),
        (File('foo', 12), (0, 1, -1, -2), [0]),
        (File('foo', 13), (0, 1, -1, -2), [0, 1]),

        (File('foo', 239), (0, 1, -1, -2), [0, 1, 18, 19]),
        (File('foo', 240), (0, 1, -1, -2), [0, 1, 18, 19]),
        (File('foo', 241), (0, 1, -1, -2), [0, 1, 19, 20]),
    ),
    ids=lambda v: str(v),
)
def test_get_relative_piece_indexes(file, relative_piece_indexes, exp_indexes,
                                    prefile_size, postfile_size,
                                    ignore_empty_files):
    files = []
    if prefile_size or not ignore_empty_files:
        files.append(File('before', prefile_size))
    files.append(file)
    if postfile_size or not ignore_empty_files:
        files.append(File('after', postfile_size))

    torrent = Torrent(piece_size=12, files=files)
    tfs = TorrentFileStream(torrent)
    assert tfs.get_relative_piece_indexes(file, relative_piece_indexes) == exp_indexes


@pytest.mark.parametrize(
    argnames='chunk_size, files, piece_index',
    argvalues=(
        # 0     1     2     3     4     5     6     7
        # abcd
        (6, [File('t/a', 1), File('t/b', 1), File('t/c', 1), File('t/d', 1)], 0),

        # 0     1     2     3     4     5     6     7
        # aaaaaaaaaaabbbbbbbbbbbbbcccccccddddddddddd
        (6, [File('t/a', 11), File('t/b', 13), File('t/c', 7), File('t/d', 11)], 0),
        (6, [File('t/a', 11), File('t/b', 13), File('t/c', 7), File('t/d', 11)], 1),
        (6, [File('t/a', 11), File('t/b', 13), File('t/c', 7), File('t/d', 11)], 2),
        (6, [File('t/a', 11), File('t/b', 13), File('t/c', 7), File('t/d', 11)], 3),
        (6, [File('t/a', 11), File('t/b', 13), File('t/c', 7), File('t/d', 11)], 4),
        (6, [File('t/a', 11), File('t/b', 13), File('t/c', 7), File('t/d', 11)], 5),
        (6, [File('t/a', 11), File('t/b', 13), File('t/c', 7), File('t/d', 11)], 6),

        # First piece contains multiple complete files
        # 0           1           2           3           4           5
        # aaaabbbbbccccccccccccccccccccccccccccccccccccccccccccccccccccc
        (12, [File('t/a', 4), File('t/b', 5), File('t/c', 53)], 0),
        (12, [File('t/a', 4), File('t/b', 5), File('t/c', 53)], 1),
        (12, [File('t/a', 4), File('t/b', 5), File('t/c', 53)], 2),
        (12, [File('t/a', 4), File('t/b', 5), File('t/c', 53)], 3),
        (12, [File('t/a', 4), File('t/b', 5), File('t/c', 53)], 4),
        (12, [File('t/a', 4), File('t/b', 5), File('t/c', 53)], 5),

        # Middle piece contains multiple complete files
        # 0              1              2              3
        # aaaaaaaaaaaaaaaaaaaaabbbbbcccdddddddddddddddddddd
        (15, [File('t/a', 21), File('t/b', 5), File('t/c', 3), File('t/d', 20)], 0),
        (15, [File('t/a', 21), File('t/b', 5), File('t/c', 3), File('t/d', 20)], 1),
        (15, [File('t/a', 21), File('t/b', 5), File('t/c', 3), File('t/d', 20)], 2),
        (15, [File('t/a', 21), File('t/b', 5), File('t/c', 3), File('t/d', 20)], 3),

        # Last piece contains multiple complete files
        # 0           1           2           3
        # aaaaaaaaaaaaaaaaaaaaaaaaaabbbbccccc
        (12, [File('t/a', 26), File('t/b', 4), File('t/c', 5)], 0),
        (12, [File('t/a', 26), File('t/b', 4), File('t/c', 5)], 1),
        (12, [File('t/a', 26), File('t/b', 4), File('t/c', 5)], 2),
    ),
    ids=lambda v: str(v),
)
@pytest.mark.parametrize(
    argnames='torrent_content_path, stream_content_path, custom_content_path, exp_content_path',
    argvalues=(
        ('torrent/path', 'stream/path', 'custom/path', 'custom/path'),
        ('torrent/path', 'stream/path', None, 'stream/path'),
        ('torrent/path', None, None, 'torrent/path'),
        (None, None, None, None),
    ),
)
def test_get_piece_returns_piece_from_files(
        torrent_content_path, stream_content_path, custom_content_path, exp_content_path,
        chunk_size, files, piece_index,
        tmp_path, mocker,
):
    torrent_name = 'my torrent'
    if torrent_content_path:
        torrent_content_path = tmp_path / torrent_content_path / torrent_name
    if stream_content_path:
        stream_content_path = tmp_path / stream_content_path / torrent_name
    if custom_content_path:
        custom_content_path = tmp_path / custom_content_path / torrent_name

    print('torrent_content_path:', torrent_content_path)
    print('stream_content_path:', stream_content_path)
    print('custom_content_path:', custom_content_path)

    if exp_content_path:
        exp_content_path = tmp_path / exp_content_path / torrent_name
        exp_content_path.mkdir(parents=True, exist_ok=True)
        for file in files:
            filepath = exp_content_path.joinpath(*file.parts[1:])
            print(f'{filepath}: {file.size} bytes: {file.content}')
            filepath.write_bytes(file.content)

    stream = b''.join(f.content for f in files)
    print('concatenated stream:', stream)
    start = piece_index * chunk_size
    stop = min(start + chunk_size, len(stream))
    exp_piece = stream[start:stop]
    print('exp_piece:', f'[{start}:{stop}]:', exp_piece)
    exp_piece_length = stop - start
    assert len(exp_piece) == exp_piece_length

    torrent = Torrent(piece_size=chunk_size, files=files, path=torrent_content_path)
    with TorrentFileStream(torrent, content_path=stream_content_path) as tfs:
        if exp_content_path is None:
            with pytest.raises(ValueError, match=r'^Missing content_path argument and torrent has no path specified$'):
                tfs.get_piece(piece_index, content_path=custom_content_path)
        else:
            piece = tfs.get_piece(piece_index, content_path=custom_content_path)
            assert piece == exp_piece


@pytest.mark.parametrize('chunk_size', range(1, 40))
def test_get_piece_resets_seek_position_when_reusing_file_handle(chunk_size, tmp_path):
    files = (
        File('MyTorrent/a', 12),
        File('MyTorrent/b', 13),
        File('MyTorrent/c', 7),
        File('MyTorrent/d', 16),
    )
    for f in files:
        print(f'{f}: {f.size} bytes: {f.content}')
        (tmp_path / 'MyTorrent').mkdir(parents=True, exist_ok=True)
        f.write_at(tmp_path)
    stream = b''.join(f.content for f in files)
    print('concatenated stream:', stream)

    total_size = sum(f.size for f in files)
    max_piece_index = math.floor((total_size - 1) // chunk_size)
    for piece_index in range(max_piece_index + 1):
        print('testing piece:', piece_index)
        start = piece_index * chunk_size
        stop = min(start + chunk_size, len(stream))
        exp_piece = stream[start:stop]
        print('exp_piece:', f'[{start}:{stop}]:', exp_piece)
        exp_piece_length = stop - start
        assert len(exp_piece) == exp_piece_length

        torrent = Torrent(piece_size=chunk_size, files=files)
        with TorrentFileStream(torrent) as tfs:
            for i in range(3):
                piece = tfs.get_piece(piece_index, content_path=tmp_path / 'MyTorrent')
                assert piece == exp_piece


@pytest.mark.parametrize(
    argnames='chunk_size, files, piece_index, exp_max_piece_index',
    argvalues=(
        # First file is smaller than one piece
        # 0           1           2           3           4           5           6           7
        # aaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccccccccc
        (12, [File('t/a', 11), File('t/b', 49), File('t/c', 30)], -1, 7),
        (12, [File('t/a', 11), File('t/b', 49), File('t/c', 30)], 8, 7),

        # Last file is smaller than one piece
        # 0   1   2   3   4   5   6   7
        # aaaaaaaabbbbbbbbbbbbbbbbbccc
        (4, [File('t/a', 8), File('t/b', 17), File('t/c', 3)], -1, 6),
        (4, [File('t/a', 8), File('t/b', 17), File('t/c', 3)], 7, 6),

        # First piece contains multiple complete files
        # 0           1           2           3           4           5
        # aaaabbbbbccccccccccccccccccccccccccccccccccccccccccccccccccccc
        (12, [File('t/a', 4), File('t/b', 5), File('t/c', 53)], -1, 5),
        (12, [File('t/a', 4), File('t/b', 5), File('t/c', 53)], 6, 5),

        # Middle piece contains multiple complete files
        # 0              1              2              3
        # aaaaaaaaaaaaaaaaaaaaabbbbbcccdddddddddddddddddddd
        (15, [File('t/a', 21), File('t/b', 5), File('t/c', 3), File('t/d', 20)], -1, 3),
        (15, [File('t/a', 21), File('t/b', 5), File('t/c', 3), File('t/d', 20)], 4, 3),

        # Last piece contains multiple complete files
        # 0           1           2           3
        # aaaaaaaaaaaaaaaaaaaaaaaaaabbbbccccc
        (12, [File('t/a', 26), File('t/b', 4), File('t/c', 5)], -1, 2),
        (12, [File('t/a', 26), File('t/b', 4), File('t/c', 5)], 3, 2),
    ),
    ids=lambda v: str(v),
)
def test_get_piece_with_piece_index_out_of_bounds(chunk_size, files, piece_index, exp_max_piece_index, tmp_path):
    torrent = Torrent(piece_size=chunk_size, files=files)
    tfs = TorrentFileStream(torrent)
    with pytest.raises(ValueError, match=rf'^piece_index must be in range 0 - {exp_max_piece_index}: {piece_index}$'):
        tfs.get_piece(piece_index)


@pytest.mark.parametrize(
    argnames='chunk_size, files, missing_files, piece_index, exp_missing_file',
    argvalues=(
        # 0     1     2     3     4     5     6     7
        # abcd
        (6, [File('t/a', 1), File('t/b', 1), File('t/c', 1), File('t/d', 1)], ['t/a'], 0, 't/a'),
        (6, [File('t/a', 1), File('t/b', 1), File('t/c', 1), File('t/d', 1)], ['t/b'], 0, 't/b'),
        (6, [File('t/a', 1), File('t/b', 1), File('t/c', 1), File('t/d', 1)], ['t/c'], 0, 't/c'),
        (6, [File('t/a', 1), File('t/b', 1), File('t/c', 1), File('t/d', 1)], ['t/d'], 0, 't/d'),
        (6, [File('t/a', 1), File('t/b', 1), File('t/c', 1), File('t/d', 1)], ['t/b', 't/c'], 0, 't/b'),

        # 0     1     2     3     4     5     6     7
        # aaaaaaaaaaabbbcccccccccccccccccdddddd
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a'], 0, 't/a'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a'], 1, 't/a'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a'], 2, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a'], 3, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a'], 4, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a'], 5, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a'], 6, None),

        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/b'], 0, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/b'], 1, 't/b'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/b'], 2, 't/b'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/b'], 3, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/b'], 4, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/b'], 5, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/b'], 6, None),

        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/c'], 0, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/c'], 1, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/c'], 2, 't/c'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/c'], 3, 't/c'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/c'], 4, 't/c'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/c'], 5, 't/c'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/c'], 6, None),

        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/d'], 0, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/d'], 1, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/d'], 2, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/d'], 3, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/d'], 4, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/d'], 5, 't/d'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/d'], 6, 't/d'),

        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/b'], 0, 't/a'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/b'], 1, 't/a'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/b'], 2, 't/b'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/b'], 3, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/b'], 4, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/b'], 5, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/b'], 6, None),

        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/c'], 0, 't/a'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/c'], 1, 't/a'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/c'], 2, 't/c'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/c'], 3, 't/c'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/c'], 4, 't/c'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/c'], 5, 't/c'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/c'], 6, None),

        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/d'], 0, 't/a'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/d'], 1, 't/a'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/d'], 2, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/d'], 3, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/d'], 4, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/d'], 5, 't/d'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/a', 't/d'], 6, 't/d'),

        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/c', 't/d'], 0, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/c', 't/d'], 1, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/c', 't/d'], 2, 't/c'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/c', 't/d'], 3, 't/c'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/c', 't/d'], 4, 't/c'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/c', 't/d'], 5, 't/c'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/c', 't/d'], 6, 't/d'),

        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/b', 't/d'], 0, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/b', 't/d'], 1, 't/b'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/b', 't/d'], 2, 't/b'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/b', 't/d'], 3, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/b', 't/d'], 4, None),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/b', 't/d'], 5, 't/d'),
        (6, [File('t/a', 11), File('t/b', 3), File('t/c', 17), File('t/d', 6)], ['t/b', 't/d'], 6, 't/d'),
    ),
    ids=lambda v: str(v),
)
def test_get_piece_with_missing_file(chunk_size, files, missing_files, piece_index, exp_missing_file, tmp_path):
    torrent_name = files[0].parts[0]
    for file in files:
        if file not in missing_files:
            filepath = tmp_path / file
            print(f'writing {filepath}: {file.size} bytes: {file.content}')
            filepath.parent.mkdir(parents=True, exist_ok=True)
            filepath.write_bytes(file.content)
        else:
            print(f'not writing {file}: {file.size} bytes: {file.content}')

    torrent = Torrent(piece_size=chunk_size, files=files)
    tfs = TorrentFileStream(torrent)

    if exp_missing_file:
        exp_exception = ReadError(errno.ENOENT, tmp_path / exp_missing_file)
        with pytest.raises(type(exp_exception), match=rf'^{re.escape(str(exp_exception))}$'):
            tfs.get_piece(piece_index, content_path=tmp_path / torrent_name)
    else:
        piece = tfs.get_piece(piece_index, content_path=tmp_path / torrent_name)
        assert isinstance(piece, bytes)


@pytest.mark.parametrize(
    argnames='chunk_size, files, contents, piece_index, exp_result',
    argvalues=(
        # 0     1     2     3     4     5
        # aaaaaaaaaaabbbbbbbbbbbbcccccc
        (6, [File('t/a', 11), File('t/b', 12), File('t/c', 6)], {'t/a': b'x' * 1}, 0, Exception('t/a')),
        (6, [File('t/a', 11), File('t/b', 12), File('t/c', 6)], {'t/a': b'x' * 2}, 1, Exception('t/a')),
        (6, [File('t/a', 11), File('t/b', 12), File('t/c', 6)], {'t/a': b'x' * 3}, 2, bytes),
        (6, [File('t/a', 11), File('t/b', 12), File('t/c', 6)], {'t/a': b'x' * 4}, 3, bytes),
        (6, [File('t/a', 11), File('t/b', 12), File('t/c', 6)], {'t/a': b'x' * 5}, 4, bytes),
        (6, [File('t/a', 11), File('t/b', 12), File('t/c', 6)], {'t/b': b'x' * 6}, 0, bytes),
        (6, [File('t/a', 11), File('t/b', 12), File('t/c', 6)], {'t/b': b'x' * 7}, 1, Exception('t/b')),
        (6, [File('t/a', 11), File('t/b', 12), File('t/c', 6)], {'t/b': b'x' * 8}, 2, Exception('t/b')),
        (6, [File('t/a', 11), File('t/b', 12), File('t/c', 6)], {'t/b': b'x' * 9}, 3, Exception('t/b')),
        (6, [File('t/a', 11), File('t/b', 12), File('t/c', 6)], {'t/b': b'x' * 10}, 4, bytes),
        (6, [File('t/a', 11), File('t/b', 12), File('t/c', 6)], {'t/c': b'x' * 11}, 0, bytes),
        (6, [File('t/a', 11), File('t/b', 12), File('t/c', 6)], {'t/c': b'x' * 12}, 1, bytes),
        (6, [File('t/a', 11), File('t/b', 12), File('t/c', 6)], {'t/c': b'x' * 13}, 2, bytes),
        (6, [File('t/a', 11), File('t/b', 12), File('t/c', 6)], {'t/c': b'x' * 14}, 3, Exception('t/c')),
        (6, [File('t/a', 11), File('t/b', 12), File('t/c', 6)], {'t/c': b'x' * 15}, 4, Exception('t/c')),
    ),
    ids=lambda v: str(v),
)
def test_get_piece_with_wrong_file_size(chunk_size, files, contents, piece_index, exp_result, tmp_path):
    for file in files:
        filepath = tmp_path / file
        filepath.parent.mkdir(parents=True, exist_ok=True)
        content = contents.get(str(file), file.content)
        print(f'{filepath}: {bytes(file.content)}, {len(file.content)} bytes')
        if content != file.content:
            print(f'  wrong file size: {bytes(content)}, {len(content)} bytes')
        filepath.write_bytes(content)

    torrent = Torrent(piece_size=chunk_size, files=files)

    if isinstance(exp_result, BaseException):
        exp_filepath_rel = str(exp_result)
        exp_filepath = str(tmp_path / exp_filepath_rel)
        exp_filesize = {str(f): f.size for f in files}[exp_filepath_rel]
        actual_file_size = os.path.getsize(tmp_path / exp_filepath_rel)
        exp_exception = VerifyFileSizeError(exp_filepath, actual_file_size, exp_filesize)

        with TorrentFileStream(torrent) as tfs:
            with pytest.raises(type(exp_exception), match=rf'^{re.escape(str(exp_exception))}$'):
                tfs.get_piece(piece_index, content_path=tmp_path / 't')

    else:
        stream = b''.join(f.content for f in files)
        print('concatenated stream:', stream)
        start = piece_index * chunk_size
        stop = min(start + chunk_size, len(stream))
        exp_piece = stream[start:stop]
        print('exp_piece:', f'[{start}:{stop}]:', exp_piece)
        exp_piece_length = stop - start
        assert len(exp_piece) == exp_piece_length

        with TorrentFileStream(torrent) as tfs:
            assert tfs.get_piece(piece_index, content_path=tmp_path / 't') == exp_piece


def test_get_file_size_from_fs_returns_file_size(mocker):
    torrent = Torrent(piece_size=123, files=(File('a', 1), File('b', 2), File('c', 3)))
    tfs = TorrentFileStream(torrent)
    exists_mock = mocker.patch('os.path.exists', return_value=True)
    getsize_mock = mocker.patch('os.path.getsize', return_value=123456)
    assert tfs._get_file_size_from_fs('path/to/b') == 123456
    assert exists_mock.call_args_list == [call('path/to/b')]
    assert getsize_mock.call_args_list == [call('path/to/b')]

def test_get_file_size_from_fs_gets_nonexisting_file(mocker):
    torrent = Torrent(piece_size=123, files=(File('a', 1), File('b', 2), File('c', 3)))
    tfs = TorrentFileStream(torrent)
    exists_mock = mocker.patch('os.path.exists', return_value=False)
    getsize_mock = mocker.patch('os.path.getsize', return_value=123456)
    assert tfs._get_file_size_from_fs('path/to/b') is None
    assert exists_mock.call_args_list == [call('path/to/b')]
    assert getsize_mock.call_args_list == []

def test_get_file_size_from_fs_gets_private_file(mocker):
    torrent = Torrent(piece_size=123, files=(File('a', 1), File('b', 2), File('c', 3)))
    tfs = TorrentFileStream(torrent)
    exists_mock = mocker.patch('os.path.exists', return_value=True)
    getsize_mock = mocker.patch('os.path.getsize', side_effect=PermissionError('Size is secret'))
    assert tfs._get_file_size_from_fs('path/to/b') is None
    assert exists_mock.call_args_list == [call('path/to/b')]
    assert getsize_mock.call_args_list == [call('path/to/b')]


def test_get_open_file_gets_nonexisting_file(mocker):
    open_mock = mocker.patch('__main__.open')
    torrent = Torrent(piece_size=123, files=(File('a', 1), File('b', 2), File('c', 3)))
    tfs = TorrentFileStream(torrent)
    path = 'foo/path'
    exp_exception = ReadError(errno.ENOENT, path)
    with pytest.raises(type(exp_exception), match=rf'^{re.escape(str(exp_exception))}$'):
        tfs._get_open_file(path)
    assert open_mock.call_args_list == []

def test_get_open_file_fails_to_open_file(mocker):
    open_mock = mocker.patch('builtins.open', side_effect=OSError(2, 'nope'))
    torrent = Torrent(piece_size=123, files=(File('a', 1), File('b', 2), File('c', 3)))
    tfs = TorrentFileStream(torrent)
    with pytest.raises(ReadError, match=r'^foo/path/b: No such file or directory$'):
        tfs._get_open_file('foo/path/b')
    assert open_mock.call_args_list == [call('foo/path/b', 'rb')]

def test_get_open_file_opens_file_only_once(mocker):
    fh1, fh2 = (Mock(), Mock())
    open_mock = mocker.patch('builtins.open', side_effect=(fh1, fh2))
    torrent = Torrent(piece_size=123, files=(File('a', 1), File('b', 2), File('c', 3)))
    tfs = TorrentFileStream(torrent)
    for _ in range(5):
        assert tfs._get_open_file('foo/path/b') == fh1
    assert open_mock.call_args_list == [call('foo/path/b', 'rb')]

def test_get_open_file_respects_max_open_files(mocker):
    max_open_files = 3
    open_files = {
        f'path/to/file{i}': Mock(name=f'mock file object {i}')
        for i in range(max_open_files + 1)
    }


    torrent = Torrent(piece_size=123, files=(File('a', 1), File('b', 2), File('c', 3)))
    tfs = TorrentFileStream(torrent)

    mocker.patch.object(tfs, 'max_open_files', max_open_files)
    tfs._open_files = open_files.copy()
    open_mock = mocker.patch('builtins.open', return_value=Mock(name='freshly opened file'))

    fh = tfs._get_open_file('another/path')
    assert fh is open_mock.return_value
    assert open_mock.call_args_list == [call('another/path', 'rb')]
    print(open_files)
    print(tfs._open_files)

    assert open_files['path/to/file0'].close.call_args_list == [call()]
    for path, fh in tuple(open_files.items())[1:]:
        assert fh.close.call_args_list == []

    exp_open_files = {
        'path/to/file1': open_files['path/to/file1'],
        'path/to/file2': open_files['path/to/file2'],
        'path/to/file3': open_files['path/to/file3'],
        'another/path': open_mock.return_value,
    }
    assert tfs._open_files == exp_open_files


@pytest.mark.parametrize(
    argnames='chunk_size, files, exp_chunks',
    argvalues=(
        # 0       1       2       3       4       5       6       7       8       8       9
        # ABC
        (8, [File('t/A', b'a'), File('t/B', b'b'), File('t/C', b'c')], [
            (b'abc', ('C', 1), ()),
        ]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAAAAABC
        (8, [File('t/A', b'abcdef'), File('t/B', b'g'), File('t/C', b'h')], [
            (b'abcdefgh', ('C', 1), ()),
        ]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAAAAABCC
        (8, [File('t/A', b'abcdef'), File('t/B', b'g'), File('t/C', b'hi')], [
            (b'abcdefgh', ('C', 2), ()),
            (b'i', ('C', 2), ()),
        ]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAAAAABBC
        (8, [File('t/A', b'abcdef'), File('t/B', b'gh'), File('t/C', b'i')], [
            (b'abcdefgh', ('B', 2), ()),
            (b'i', ('C', 1), ()),
        ]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAAAAABBCC
        (8, [File('t/A', b'abcdef'), File('t/B', b'gh'), File('t/C', b'ij')], [
            (b'abcdefgh', ('B', 2), ()),
            (b'ij', ('C', 2), ()),
        ]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAAAAAAABBCC
        (8, [File('t/A', b'abcdefgh'), File('t/B', b'ij'), File('t/C', b'kl')], [
            (b'abcdefgh', ('A', 8), ()),
            (b'ijkl', ('C', 2), ()),
        ]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAAAAAAAABBCC
        (8, [File('t/A', b'abcdefghi'), File('t/B', b'jk'), File('t/C', b'lm')], [
            (b'abcdefgh', ('A', 9), ()),
            (b'ijklm', ('C', 2), ()),
        ]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAAAAAAAABBCCCCC
        (8, [File('t/A', b'abcdefghi'), File('t/B', b'jk'), File('t/C', b'lmnop')], [
            (b'abcdefgh', ('A', 9), ()),
            (b'ijklmnop', ('C', 5), ()),
        ]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAAAAAAAABBBCCCCC
        (8, [File('t/A', b'abcdefghi'), File('t/B', b'jkl'), File('t/C', b'mnopq')], [
            (b'abcdefgh', ('A', 9), ()),
            (b'ijklmnop', ('C', 5), ()),
            (b'q', ('C', 5), ()),
        ]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAAAAAAABBBBCCCCC
        (8, [File('t/A', b'abcdefgh'), File('t/B', b'ijkl'), File('t/C', b'mnopq')], [
            (b'abcdefgh', ('A', 8), ()),
            (b'ijklmnop', ('C', 5), ()),
            (b'q', ('C', 5), ()),
        ]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAAAAAABBBBCCCCCC
        (8, [File('t/A', b'abcdefg'), File('t/B', b'hijk'), File('t/C', b'lmnopq')], [
            (b'abcdefgh', ('B', 4), ()),
            (b'ijklmnop', ('C', 6), ()),
            (b'q', ('C', 6), ()),
        ]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # ABBBBCCCCC
        (8, [File('t/A', b'a'), File('t/B', b'bcde'), File('t/C', b'fghij')], [
            (b'abcdefgh', ('C', 5), ()),
            (b'ij', ('C', 5), ()),
        ]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # ABBBBCCCC
        (8, [File('t/A', b'a'), File('t/B', b'bcde'), File('t/C', b'fghi')], [
            (b'abcdefgh', ('C', 4), ()),
            (b'i', ('C', 4), ()),
        ]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # ABBBBC
        (8, [File('t/A', b'a'), File('t/B', b'bcde'), File('t/C', b'f')], [
            (b'abcdef', ('C', 1), ()),
        ]),
    ),
    ids=lambda v: str(v),
)
@pytest.mark.parametrize(
    argnames='torrent_content_path, stream_content_path, custom_content_path, exp_content_path',
    argvalues=(
        ('torrent/path', 'stream/path', 'custom/path', 'custom/path'),
        ('torrent/path', 'stream/path', None, 'stream/path'),
        ('torrent/path', None, None, 'torrent/path'),
        (None, None, None, None),
    ),
)
def test_iter_pieces_without_missing_files(
    torrent_content_path, stream_content_path, custom_content_path, exp_content_path,
    chunk_size, files, exp_chunks,
    tmp_path, mocker,
):
    torrent_name = 'my_torrent'
    if torrent_content_path:
        torrent_content_path = (tmp_path / torrent_content_path).parent / torrent_name
    if stream_content_path:
        stream_content_path = (tmp_path / stream_content_path).parent / torrent_name
    if custom_content_path:
        custom_content_path = (tmp_path / custom_content_path).parent / torrent_name

    print('torrent_content_path:', torrent_content_path)
    print('stream_content_path:', stream_content_path)
    print('custom_content_path:', custom_content_path)

    if exp_content_path:
        exp_content_path = (tmp_path / exp_content_path).parent / torrent_name
        exp_content_path.mkdir(parents=True, exist_ok=True)
        for file in files:
            filepath = exp_content_path.joinpath(os.sep.join(file.parts[1:]))
            print(f'{filepath}: {file.size} bytes: {file.content}')
            filepath.write_bytes(file.content)

    exp_chunks_fixed = []
    for chunk, (filepath_rel, filesize), exceptions in exp_chunks:
        if exp_content_path:
            filepath = File(exp_content_path / filepath_rel, filesize)
        else:
            filepath = File(filepath_rel, filesize)
        exp_chunks_fixed.append((chunk, filepath, exceptions))

    torrent = Torrent(piece_size=chunk_size, files=files, path=torrent_content_path)
    with TorrentFileStream(torrent, content_path=stream_content_path) as tfs:
        if exp_content_path is None:
            with pytest.raises(ValueError, match=r'^Missing content_path argument and torrent has no path specified$'):
                list(tfs.iter_pieces(content_path=custom_content_path))
        else:
            assert list(tfs.iter_pieces(content_path=custom_content_path)) == exp_chunks_fixed


@pytest.mark.parametrize(
    argnames='chunk_size, files, missing_files, exp_chunks',
    argvalues=(
        # 0       1       2       3       4       5       6       7       8       8       9
        # ABC
        (8, [File('t/A', b'a'), File('t/B', b'b'), File('t/C', b'c')], ['t/A'], [
            (None, ('t/A', 1), ('t/A',)),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'b'), File('t/C', b'c')], ['t/B'], [
            (None, ('t/B', 1), ('t/B',)),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'b'), File('t/C', b'c')], ['t/C'], [
            (None, ('t/C', 1), ('t/C',)),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'b'), File('t/C', b'c')], ['t/A', 't/B'], [
            (None, ('t/A', 1), ('t/A', 't/B')),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'b'), File('t/C', b'c')], ['t/B', 't/C'], [
            (None, ('t/B', 1), ('t/B', 't/C')),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'b'), File('t/C', b'c')], ['t/A', 't/C'], [
            (None, ('t/A', 1), ('t/A', 't/C')),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'b'), File('t/C', b'c')], ['t/A', 't/B', 't/C'], [
            (None, ('t/A', 1), ('t/A', 't/B', 't/C')),
        ]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAABBBCC
        (8, [File('t/A', b'abc'), File('t/B', b'def'), File('t/C', b'gh')], ['t/A'], [
            (None, ('t/A', 3), ('t/A',)),
        ]),
        (8, [File('t/A', b'abc'), File('t/B', b'def'), File('t/C', b'gh')], ['t/B'], [
            (None, ('t/B', 3), ('t/B',)),
        ]),
        (8, [File('t/A', b'abc'), File('t/B', b'def'), File('t/C', b'gh')], ['t/C'], [
            (None, ('t/C', 2), ('t/C',)),
        ]),
        (8, [File('t/A', b'abc'), File('t/B', b'def'), File('t/C', b'gh')], ['t/A', 't/B'], [
            (None, ('t/A', 3), ('t/A', 't/B')),
        ]),
        (8, [File('t/A', b'abc'), File('t/B', b'def'), File('t/C', b'gh')], ['t/B', 't/C'], [
            (None, ('t/B', 3), ('t/B', 't/C')),
        ]),
        (8, [File('t/A', b'abc'), File('t/B', b'def'), File('t/C', b'gh')], ['t/A', 't/C'], [
            (None, ('t/A', 3), ('t/A', 't/C')),
        ]),
        (8, [File('t/A', b'abc'), File('t/B', b'def'), File('t/C', b'gh')], ['t/A', 't/B', 't/C'], [
            (None, ('t/A', 3), ('t/A', 't/B', 't/C')),
        ]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # ABCCCCCCC
        (8, [File('t/A', b'a'), File('t/B', b'b'), File('t/C', b'cdefghi')], ['t/A'], [
            (None, ('t/A', 1), ('t/A',)),
            (b'i', ('t/C', 7), ()),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'b'), File('t/C', b'cdefghi')], ['t/B'], [
            (None, ('t/B', 1), ('t/B',)),
            (b'i', ('t/C', 7), ()),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'b'), File('t/C', b'cdefghi')], ['t/C'], [
            (None, ('t/C', 7), ('t/C',)),
            (None, ('t/C', 7), ()),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'b'), File('t/C', b'cdefghi')], ['t/A', 't/B'], [
            (None, ('t/A', 1), ('t/A', 't/B')),
            (b'i', ('t/C', 7), ()),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'b'), File('t/C', b'cdefghi')], ['t/B', 't/C'], [
            (None, ('t/B', 1), ('t/B',)),
            (None, ('t/C', 7), ('t/C',)),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'b'), File('t/C', b'cdefghi')], ['t/A', 't/C'], [
            (None, ('t/A', 1), ('t/A',)),
            (None, ('t/C', 7), ('t/C',)),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'b'), File('t/C', b'cdefghi')], ['t/A', 't/B', 't/C'], [
            (None, ('t/A', 1), ('t/A', 't/B')),
            (None, ('t/C', 7), ('t/C',)),
        ]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # ABBBBBBCC
        (8, [File('t/A', b'a'), File('t/B', b'bcdefg'), File('t/C', b'hi')], ['t/A'], [
            (None, ('t/A', 1), ('t/A',)),
            (b'i', ('t/C', 2), ()),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'bcdefg'), File('t/C', b'hi')], ['t/B'], [
            (None, ('t/B', 6), ('t/B',)),
            (b'i', ('t/C', 2), ()),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'bcdefg'), File('t/C', b'hi')], ['t/C'], [
            (None, ('t/C', 2), ('t/C',)),
            (None, ('t/C', 2), ()),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'bcdefg'), File('t/C', b'hi')], ['t/A', 't/B'], [
            (None, ('t/A', 1), ('t/A', 't/B')),
            (b'i', ('t/C', 2), ()),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'bcdefg'), File('t/C', b'hi')], ['t/B', 't/C'], [
            (None, ('t/B', 6), ('t/B',)),
            (None, ('t/C', 2), ('t/C',)),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'bcdefg'), File('t/C', b'hi')], ['t/A', 't/C'], [
            (None, ('t/A', 1), ('t/A',)),
            (None, ('t/C', 2), ('t/C',)),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'bcdefg'), File('t/C', b'hi')], ['t/A', 't/B', 't/C'], [
            (None, ('t/A', 1), ('t/A', 't/B')),
            (None, ('t/C', 2), ('t/C',)),
        ]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # ABBBBBBCCCCCCCCCC
        (8, [File('t/A', b'a'), File('t/B', b'bcdefg'), File('t/C', b'hijklmnopq')], ['t/A'], [
            (None, ('t/A', 1), ('t/A',)),
            (b'ijklmnop', ('t/C', 10), ()),
            (b'q', ('t/C', 10), ()),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'bcdefg'), File('t/C', b'hijklmnopq')], ['t/B'], [
            (None, ('t/B', 6), ('t/B',)),
            (b'ijklmnop', ('t/C', 10), ()),
            (b'q', ('t/C', 10), ()),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'bcdefg'), File('t/C', b'hijklmnopq')], ['t/C'], [
            (None, ('t/C', 10), ('t/C',)),
            (None, ('t/C', 10), ()),
            (None, ('t/C', 10), ()),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'bcdefg'), File('t/C', b'hijklmnopq')], ['t/A', 't/B'], [
            (None, ('t/A', 1), ('t/A', 't/B')),
            (b'ijklmnop', ('t/C', 10), ()),
            (b'q', ('t/C', 10), ()),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'bcdefg'), File('t/C', b'hijklmnopq')], ['t/B', 't/C'], [
            (None, ('t/B', 6), ('t/B',)),
            (None, ('t/C', 10), ('t/C',)),
            (None, ('t/C', 10), ()),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'bcdefg'), File('t/C', b'hijklmnopq')], ['t/A', 't/C'], [
            (None, ('t/A', 1), ('t/A',)),
            (None, ('t/C', 10), ('t/C',)),
            (None, ('t/C', 10), ()),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'bcdefg'), File('t/C', b'hijklmnopq')], ['t/A', 't/B', 't/C'], [
            (None, ('t/A', 1), ('t/A', 't/B')),
            (None, ('t/C', 10), ('t/C',)),
            (None, ('t/C', 10), ()),
        ]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # ABBBBBBBCCCCCCCCCC
        (8, [File('t/A', b'a'), File('t/B', b'bcdefgh'), File('t/C', b'ijklmnopqr')], ['t/A'], [
            (None, ('t/A', 1), ('t/A',)),
            (b'ijklmnop', ('t/C', 10), ()),
            (b'qr', ('t/C', 10), ()),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'bcdefgh'), File('t/C', b'ijklmnopqr')], ['t/B'], [
            (None, ('t/B', 7), ('t/B',)),
            (b'ijklmnop', ('t/C', 10), ()),
            (b'qr', ('t/C', 10), ()),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'bcdefgh'), File('t/C', b'ijklmnopqr')], ['t/C'], [
            (b'abcdefgh', ('t/B', 7), ()),
            (None, ('t/C', 10), ('t/C',)),
            (None, ('t/C', 10), ()),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'bcdefgh'), File('t/C', b'ijklmnopqr')], ['t/A', 't/B'], [
            (None, ('t/A', 1), ('t/A', 't/B')),
            (b'ijklmnop', ('t/C', 10), ()),
            (b'qr', ('t/C', 10), ()),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'bcdefgh'), File('t/C', b'ijklmnopqr')], ['t/B', 't/C'], [
            (None, ('t/B', 7), ('t/B',)),
            (None, ('t/C', 10), ('t/C',)),
            (None, ('t/C', 10), ()),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'bcdefgh'), File('t/C', b'ijklmnopqr')], ['t/A', 't/C'], [
            (None, ('t/A', 1), ('t/A',)),
            (None, ('t/C', 10), ('t/C',)),
            (None, ('t/C', 10), ()),
        ]),
        (8, [File('t/A', b'a'), File('t/B', b'bcdefgh'), File('t/C', b'ijklmnopqr')], ['t/A', 't/B', 't/C'], [
            (None, ('t/A', 1), ('t/A', 't/B')),
            (None, ('t/C', 10), ('t/C',)),
            (None, ('t/C', 10), ()),
        ]),
        # 0   1   2   3   4   5   6   7   8   8   9
        # AAAAABBBCCCCCC
        (4, [File('t/A', b'abcde'), File('t/B', b'fgh'), File('t/C', b'ijklmn')], ['t/A'], [
            (None, ('t/A', 5), ('t/A',)),
            (None, ('t/A', 5), ()),
            (b'ijkl', ('t/C', 6), ()),
            (b'mn', ('t/C', 6), ()),
        ]),
        (4, [File('t/A', b'abcde'), File('t/B', b'fgh'), File('t/C', b'ijklmn')], ['t/B'], [
            (b'abcd', ('t/A', 5), ()),
            (None, ('t/B', 3), ('t/B',)),
            (b'ijkl', ('t/C', 6), ()),
            (b'mn', ('t/C', 6), ()),
        ]),
        (4, [File('t/A', b'abcde'), File('t/B', b'fgh'), File('t/C', b'ijklmn')], ['t/C'], [
            (b'abcd', ('t/A', 5), ()),
            (b'efgh', ('t/B', 3), ()),
            (None, ('t/C', 6), ('t/C',)),
            (None, ('t/C', 6), ()),
        ]),
        (4, [File('t/A', b'abcde'), File('t/B', b'fgh'), File('t/C', b'ijklmn')], ['t/A', 't/B'], [
            (None, ('t/A', 5), ('t/A',)),
            (None, ('t/A', 5), ('t/B',)),
            (b'ijkl', ('t/C', 6), ()),
            (b'mn', ('t/C', 6), ()),
        ]),
        (4, [File('t/A', b'abcde'), File('t/B', b'fgh'), File('t/C', b'ijklmn')], ['t/B', 't/C'], [
            (b'abcd', ('t/A', 5), ()),
            (None, ('t/B', 3), ('t/B',)),
            (None, ('t/C', 6), ('t/C',)),
            (None, ('t/C', 6), ()),
        ]),
        (4, [File('t/A', b'abcde'), File('t/B', b'fgh'), File('t/C', b'ijklmn')], ['t/A', 't/C'], [
            (None, ('t/A', 5), ('t/A',)),
            (None, ('t/A', 5), ()),
            (None, ('t/C', 6), ('t/C',)),
            (None, ('t/C', 6), ()),
        ]),
        (4, [File('t/A', b'abcde'), File('t/B', b'fgh'), File('t/C', b'ijklmn')], ['t/A', 't/B', 't/C'], [
            (None, ('t/A', 5), ('t/A',)),
            (None, ('t/A', 5), ('t/B',)),
            (None, ('t/C', 6), ('t/C',)),
            (None, ('t/C', 6), ()),
        ]),
        # 0   1   2   3   4   5   6   7   8   8   9
        # AAAAABBBBCCCCC
        (4, [File('t/A', b'abcde'), File('t/B', b'fghi'), File('t/C', b'jklmn')], ['t/A'], [
            (None, ('t/A', 5), ('t/A',)),
            (None, ('t/A', 5), ()),
            (b'ijkl', ('t/C', 5), ()),
            (b'mn', ('t/C', 5), ()),
        ]),
        (4, [File('t/A', b'abcde'), File('t/B', b'fghi'), File('t/C', b'jklmn')], ['t/B'], [
            (b'abcd', ('t/A', 5), ()),
            (None, ('t/B', 4), ('t/B',)),
            (None, ('t/B', 4), ()),
            (b'mn', ('t/C', 5), ()),
        ]),
        (4, [File('t/A', b'abcde'), File('t/B', b'fghi'), File('t/C', b'jklmn')], ['t/C'], [
            (b'abcd', ('t/A', 5), ()),
            (b'efgh', ('t/B', 4), ()),
            (None, ('t/C', 5), ('t/C',)),
            (None, ('t/C', 5), ()),
        ]),
        (4, [File('t/A', b'abcde'), File('t/B', b'fghi'), File('t/C', b'jklmn')], ['t/A', 't/B'], [
            (None, ('t/A', 5), ('t/A',)),
            (None, ('t/A', 5), ()),
            (None, ('t/B', 4), ('t/B',)),
            (b'mn', ('t/C', 5), ()),
        ]),
        (4, [File('t/A', b'abcde'), File('t/B', b'fghi'), File('t/C', b'jklmn')], ['t/B', 't/C'], [
            (b'abcd', ('t/A', 5), ()),
            (None, ('t/B', 4), ('t/B',)),
            (None, ('t/B', 4), ()),
            (None, ('t/C', 5), ('t/C',)),
        ]),
        (4, [File('t/A', b'abcde'), File('t/B', b'fghi'), File('t/C', b'jklmn')], ['t/A', 't/C'], [
            (None, ('t/A', 5), ('t/A',)),
            (None, ('t/A', 5), ()),
            (None, ('t/C', 5), ('t/C',)),
            (None, ('t/C', 5), ()),
        ]),
        (4, [File('t/A', b'abcde'), File('t/B', b'fghi'), File('t/C', b'jklmn')], ['t/A', 't/B', 't/C'], [
            (None, ('t/A', 5), ('t/A',)),
            (None, ('t/A', 5), ()),
            (None, ('t/B', 4), ('t/B',)),
            (None, ('t/C', 5), ('t/C',)),
        ]),
        # 0   1   2   3   4   5   6   7   8   8   9
        # AAABBBBBCCCCCC
        (4, [File('t/A', b'abc'), File('t/B', b'defgh'), File('t/C', b'ijklmn')], ['t/A'], [
            (None, ('t/A', 3), ('t/A',)),
            (b'efgh', ('t/B', 5), ()),
            (b'ijkl', ('t/C', 6), ()),
            (b'mn', ('t/C', 6), ()),
        ]),
        (4, [File('t/A', b'abc'), File('t/B', b'defgh'), File('t/C', b'ijklmn')], ['t/B'], [
            (None, ('t/B', 5), ('t/B',)),
            (None, ('t/B', 5), ()),
            (b'ijkl', ('t/C', 6), ()),
            (b'mn', ('t/C', 6), ()),
        ]),
        (4, [File('t/A', b'abc'), File('t/B', b'defgh'), File('t/C', b'ijklmn')], ['t/C'], [
            (b'abcd', ('t/B', 5), ()),
            (b'efgh', ('t/B', 5), ()),
            (None, ('t/C', 6), ('t/C',)),
            (None, ('t/C', 6), ()),
        ]),
        (4, [File('t/A', b'abc'), File('t/B', b'defgh'), File('t/C', b'ijklmn')], ['t/A', 't/B'], [
            (None, ('t/A', 3), ('t/A',)),
            (None, ('t/B', 5), ('t/B',)),
            (b'ijkl', ('t/C', 6), ()),
            (b'mn', ('t/C', 6), ()),
        ]),
        (4, [File('t/A', b'abc'), File('t/B', b'defgh'), File('t/C', b'ijklmn')], ['t/B', 't/C'], [
            (None, ('t/B', 5), ('t/B',)),
            (None, ('t/B', 5), ()),
            (None, ('t/C', 6), ('t/C',)),
            (None, ('t/C', 6), ()),
        ]),
        (4, [File('t/A', b'abc'), File('t/B', b'defgh'), File('t/C', b'ijklmn')], ['t/A', 't/C'], [
            (None, ('t/A', 3), ('t/A',)),
            (b'efgh', ('t/B', 5), ()),
            (None, ('t/C', 6), ('t/C',)),
            (None, ('t/C', 6), ()),
        ]),
        (4, [File('t/A', b'abc'), File('t/B', b'defgh'), File('t/C', b'ijklmn')], ['t/A', 't/B', 't/C'], [
            (None, ('t/A', 3), ('t/A',)),
            (None, ('t/B', 5), ('t/B',)),
            (None, ('t/C', 6), ('t/C',)),
            (None, ('t/C', 6), ()),
        ]),
    ),
    ids=lambda v: str(v),
)
def test_iter_pieces_with_missing_files(chunk_size, files, missing_files, exp_chunks, tmp_path):
    torrent_name = files[0].parts[0]
    content_path = tmp_path / torrent_name
    content_path.mkdir(parents=True, exist_ok=True)
    for f in files:
        if str(f) not in missing_files:
            filepath = tmp_path / f
            print(f'writing {filepath}: {f.size} bytes: {f.content}')
            filepath.write_bytes(f.content)
        else:
            print(f'not writing {f}: {f.size} bytes: {f.content}')

    exp_chunks_fixed = []
    for chunk, (filepath_rel, filesize), exceptions in exp_chunks:
        filepath = File(tmp_path / filepath_rel, filesize)
        exceptions = tuple(ComparableException(ReadError(errno.ENOENT, str(tmp_path / f)))
                           for f in exceptions)
        exp_chunks_fixed.append((chunk, filepath, exceptions))

    torrent = Torrent(piece_size=chunk_size, files=files)
    tfs = TorrentFileStream(torrent)
    chunks = list(tfs.iter_pieces(content_path=content_path))

    def compare(x, y):
        if chunks[x][y] != exp_chunks_fixed[x][y]:
            print(f'{i}: {chunks[x][y]!r}\n   {exp_chunks_fixed[x][y]!r}')

    for i in range(len(chunks)):
        compare(i, 0)
        compare(i, 1)
        compare(i, 2)

    assert chunks == exp_chunks_fixed


class OOMCallback:
    def __init__(self, attempts):
        self._attempts = int(attempts)

    def __call__(self, exception):
        try:
            if self._attempts <= 0:
                print('Raising', repr(exception))
                raise exception
            else:
                print('Ignoring', repr(exception))
        finally:
            self._attempts -= 1

@pytest.mark.parametrize(
    argnames='oom_callback_kwargs, read_results, exp_result, exp_oom_callback_calls',
    argvalues=(
        (
            None,
            [b'abc', b'def', b'ghi'],
            b'abc',
            [],
        ),
        (
            None,
            [MemoryError('one'), MemoryError('two'), b'ghi'],
            MemoryError('Out of memory while reading from path/to/file at position 1'),
            [],
        ),
        (
            {'attempts': 0},
            [MemoryError('one'), MemoryError('two'), b'ghi'],
            MemoryError('Out of memory while reading from path/to/file at position 1'),
            [call(MemoryError('Out of memory while reading from path/to/file at position 1')),],
        ),
        (
            {'attempts': 1},
            [MemoryError('one'), MemoryError('two'), b'ghi'],
            MemoryError('Out of memory while reading from path/to/file at position 2'),
            [
                call(MemoryError('Out of memory while reading from path/to/file at position 1')),
                call(MemoryError('Out of memory while reading from path/to/file at position 2')),
            ],
        ),
        (
            {'attempts': 3},
            [MemoryError('one'), MemoryError('two'), b'ghi'],
            b'ghi',
            [
                call(MemoryError('Out of memory while reading from path/to/file at position 1')),
                call(MemoryError('Out of memory while reading from path/to/file at position 2')),
            ],
        ),
    ),
    ids=lambda v: repr(v),
)
def test_read_from_fh(oom_callback_kwargs, read_results, exp_result, exp_oom_callback_calls, mocker):
    files = [
        File('A', b'abc'),
        File('A', b'def'),
        File('A', b'ghi'),
    ]
    size = 123
    fh = Mock(read=Mock(side_effect=read_results))
    fh.tell.side_effect = [int(n) for n in '1234567890']
    fh.configure_mock(name='path/to/file')

    torrent = Torrent(piece_size=size, files=files)
    tfs = TorrentFileStream(torrent)
    if oom_callback_kwargs is None:
        oom_callback = None
    else:
        oom_callback = OOMCallback(**oom_callback_kwargs)

    if isinstance(exp_result, Exception):
        with pytest.raises(type(exp_result), match=rf'^{re.escape(str(exp_result))}$'):
            print(tfs._read_from_fh(fh, size, oom_callback))
    else:
        return_value = tfs._read_from_fh(fh, size, oom_callback)
        assert return_value is exp_result


def test_get_piece_hash_from_readable_piece(mocker):
    torrent = Torrent(piece_size=123, files=(File('a', 1), File('b', 2), File('c', 3)))
    tfs = TorrentFileStream(torrent)
    get_piece_mock = mocker.patch.object(tfs, 'get_piece', return_value=b'mock piece')
    sha1_mock = mocker.patch('hashlib.sha1', return_value=Mock(digest=Mock(return_value=b'mock hash')))
    assert tfs.get_piece_hash(123, content_path='foo/path') == b'mock hash'
    assert get_piece_mock.call_args_list == [call(123, content_path='foo/path')]
    assert sha1_mock.call_args_list == [call(b'mock piece')]

def test_get_piece_hash_from_piece_from_missing_file(mocker):
    torrent = Torrent(piece_size=123, files=(File('a', 1), File('b', 2), File('c', 3)))
    tfs = TorrentFileStream(torrent)
    get_piece_mock = mocker.patch.object(tfs, 'get_piece', side_effect=ReadError(errno.ENOENT, 'foo/path'))
    sha1_mock = mocker.patch('hashlib.sha1', return_value=Mock(digest=Mock(return_value=b'mock hash')))
    assert tfs.get_piece_hash(123, content_path='foo/path') is None
    assert get_piece_mock.call_args_list == [call(123, content_path='foo/path')]
    assert sha1_mock.call_args_list == []

def test_get_piece_hash_from_piece_from_existing_unreadable_file(mocker):
    torrent = Torrent(piece_size=123, files=(File('a', 1), File('b', 2), File('c', 3)))
    tfs = TorrentFileStream(torrent)
    exception = ReadError(errno.EACCES, 'foo/path')
    get_piece_mock = mocker.patch.object(tfs, 'get_piece', side_effect=exception)
    sha1_mock = mocker.patch('hashlib.sha1', return_value=Mock(digest=Mock(return_value=b'mock hash')))
    with pytest.raises(type(exception), match=rf'^{re.escape(str(exception))}$'):
        tfs.get_piece_hash(123, content_path='foo/path')
    assert get_piece_mock.call_args_list == [call(123, content_path='foo/path')]
    assert sha1_mock.call_args_list == []


def test_verify_piece_verifies_piece_hash(mocker):
    torrent = Torrent(piece_size=123, files=(File('a', 1), File('b', 2), File('c', 3)))
    torrent.hashes = (b'd34d', b'b33f', b'b00b5')
    tfs = TorrentFileStream(torrent)
    mocker.patch.object(tfs, 'get_piece_hash', return_value=b'b33f')
    mocker.patch.object(type(tfs), 'max_piece_index', PropertyMock(return_value=2))
    assert tfs.verify_piece(0, content_path='foo/path') is False
    assert tfs.verify_piece(1, content_path='foo/path') is True
    assert tfs.verify_piece(2, content_path='foo/path') is False
    with pytest.raises(ValueError, match=r'^piece_index must be in range 0 - 2: 3$'):
        tfs.verify_piece(3, content_path='foo/path')

def test_verify_piece_gets_handles_no_piece_hash(mocker):
    torrent = Torrent(piece_size=123, files=(File('a', 1), File('b', 2), File('c', 3)))
    torrent.hashes = (b'd34d', b'b33f', b'b00b5')
    tfs = TorrentFileStream(torrent)
    mocker.patch.object(tfs, 'get_piece_hash', return_value=None)
    mocker.patch.object(type(tfs), 'max_piece_index', PropertyMock(return_value=2))
    assert tfs.verify_piece(0, content_path='foo/path') is None
    assert tfs.verify_piece(1, content_path='foo/path') is None
    assert tfs.verify_piece(2, content_path='foo/path') is None
    with pytest.raises(ValueError, match=r'^piece_index must be in range 0 - 2: 3$'):
        tfs.verify_piece(3, content_path='foo/path')
