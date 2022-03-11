import math
import os
import re
from unittest.mock import Mock, PropertyMock, call

import pytest

import torf
from torf import TorrentFileStream


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

    def __repr__(self):
        return f'{type(self).__name__}({str(self)}, {len(self.content)})'

    def write_at(self, directory, content=None):
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
    argnames='piece_size, files, exp_max_piece_index',
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
def test_max_piece_index(piece_size, files, exp_max_piece_index):
    torrent = Torrent(piece_size=piece_size, files=files)
    tfs = TorrentFileStream(torrent)
    assert tfs.max_piece_index == exp_max_piece_index


@pytest.mark.parametrize(
    argnames='piece_size, files, file, exp_result',
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
def test_get_file_position(piece_size, files, file, exp_result):
    torrent = Torrent(piece_size=piece_size, files=files)
    tfs = TorrentFileStream(torrent)
    if isinstance(exp_result, BaseException):
        with pytest.raises(type(exp_result), match=rf'^{re.escape(str(exp_result))}$'):
            tfs.get_file_position(file)
    else:
        assert tfs.get_file_position(file) == exp_result


@pytest.mark.parametrize(
    argnames='piece_size, files, position, exp_result',
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
def test_get_file_at_position(piece_size, files, position, exp_result, mocker):
    torrent = Torrent(piece_size=piece_size, files=files)
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
    argnames='piece_size, files, exp_piece_indexes',
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
def test_get_piece_indexes_of_file_nonexclusive(piece_size, files, exp_piece_indexes):
    torrent = Torrent(piece_size=piece_size, files=files)
    tfs = TorrentFileStream(torrent)
    for filename, exp_indexes in exp_piece_indexes.items():
        file = [f for f in torrent.files if f == filename][0]
        assert tfs.get_piece_indexes_of_file(file) == exp_indexes


@pytest.mark.parametrize(
    argnames='piece_size, files, exp_piece_indexes',
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
def test_get_piece_indexes_of_file_exclusive(piece_size, files, exp_piece_indexes):
    torrent = Torrent(piece_size=piece_size, files=files)
    tfs = TorrentFileStream(torrent)
    for filename, exp_indexes in exp_piece_indexes.items():
        file = [f for f in torrent.files if f == filename][0]
        assert tfs.get_piece_indexes_of_file(file, exclusive=True) == exp_indexes


@pytest.mark.parametrize(
    argnames='piece_size, files, first_byte_indexes, last_byte_indexes, exp_files',
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
def test_get_files_at_byte_range(piece_size, first_byte_indexes, last_byte_indexes, files, exp_files, mocker):
    torrent = Torrent(piece_size=piece_size, files=files)
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
    argnames='piece_size, files, file, exp_byte_range',
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
def test_get_byte_range_of_file(piece_size, files, file, exp_byte_range):
    torrent = Torrent(piece_size=piece_size, files=files)
    tfs = TorrentFileStream(torrent)
    file = {str(f): f for f in torrent.files}[file]
    byte_range = tfs.get_byte_range_of_file(file)
    assert byte_range == exp_byte_range


@pytest.mark.parametrize(
    argnames='piece_size, files, piece_index, exp_return_value',
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
def test_get_files_at_piece_index(piece_size, files, piece_index, exp_return_value, mocker):
    torrent = Torrent(piece_size=piece_size, files=files)
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
    argnames='piece_size, files, file, relative_piece_indexes, exp_absolute_indexes',
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
def test_get_absolute_piece_indexes(piece_size, files, file, relative_piece_indexes, exp_absolute_indexes):
    torrent = Torrent(piece_size=piece_size, files=files)
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
    argnames='piece_size, files, piece_index',
    argvalues=(
        # 0     1     2     3     4     5     6     7
        # aaaaaaaaaaabbbbbbbbbbbbbcccccccddddddddddd
        (6, [File('a', 11), File('b', 13), File('c', 7), File('d', 11)], 0),
        (6, [File('a', 11), File('b', 13), File('c', 7), File('d', 11)], 1),
        (6, [File('a', 11), File('b', 13), File('c', 7), File('d', 11)], 2),
        (6, [File('a', 11), File('b', 13), File('c', 7), File('d', 11)], 3),
        (6, [File('a', 11), File('b', 13), File('c', 7), File('d', 11)], 4),
        (6, [File('a', 11), File('b', 13), File('c', 7), File('d', 11)], 5),
        (6, [File('a', 11), File('b', 13), File('c', 7), File('d', 11)], 6),

        # First piece contains multiple complete files
        # 0           1           2           3           4           5
        # aaaabbbbbccccccccccccccccccccccccccccccccccccccccccccccccccccc
        (12, [File('a', 4), File('b', 5), File('c', 53)], 0),
        (12, [File('a', 4), File('b', 5), File('c', 53)], 1),
        (12, [File('a', 4), File('b', 5), File('c', 53)], 2),
        (12, [File('a', 4), File('b', 5), File('c', 53)], 3),
        (12, [File('a', 4), File('b', 5), File('c', 53)], 4),
        (12, [File('a', 4), File('b', 5), File('c', 53)], 5),

        # Middle piece contains multiple complete files
        # 0              1              2              3
        # aaaaaaaaaaaaaaaaaaaaabbbbbcccdddddddddddddddddddd
        (15, [File('a', 21), File('b', 5), File('c', 3), File('d', 20)], 0),
        (15, [File('a', 21), File('b', 5), File('c', 3), File('d', 20)], 1),
        (15, [File('a', 21), File('b', 5), File('c', 3), File('d', 20)], 2),
        (15, [File('a', 21), File('b', 5), File('c', 3), File('d', 20)], 3),

        # Last piece contains multiple complete files
        # 0           1           2           3
        # aaaaaaaaaaaaaaaaaaaaaaaaaabbbbccccc
        (12, [File('a', 26), File('b', 4), File('c', 5)], 0),
        (12, [File('a', 26), File('b', 4), File('c', 5)], 1),
        (12, [File('a', 26), File('b', 4), File('c', 5)], 2),
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
        piece_size, files, piece_index,
        tmp_path, mocker,
):
    torrent_name = 'my torrent'
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
            filepath = exp_content_path.joinpath(file)
            print(f'{filepath}: {file.size} bytes: {file.content}')
            filepath.write_bytes(file.content)

    stream = b''.join(f.content for f in files)
    print('concatenated stream:', stream)
    start = piece_index * piece_size
    stop = min(start + piece_size, len(stream))
    exp_piece = stream[start:stop]
    print('exp_piece:', f'[{start}:{stop}]:', exp_piece)
    exp_piece_length = stop - start
    assert len(exp_piece) == exp_piece_length

    torrent = Torrent(piece_size=piece_size, files=files, path=torrent_content_path)
    with TorrentFileStream(torrent, content_path=stream_content_path) as tfs:
        if exp_content_path is None:
            with pytest.raises(ValueError, match=r'^Missing content_path argument and torrent has no path specified$'):
                print(tfs.get_piece(piece_index, content_path=custom_content_path))
        else:
            piece = tfs.get_piece(piece_index, content_path=custom_content_path)
            assert piece == exp_piece


@pytest.mark.parametrize('piece_size', (4, 6, 8, 9, 12))
def test_get_piece_resets_seek_position_when_reusing_file_handle(piece_size, tmp_path):
    files = (
        File('a', 12),
        File('b', 13),
        File('c', 7),
        File('d', 16),
    )
    for f in files:
        print(f'{f}: {f.size} bytes: {f.content}')
        f.write_at(tmp_path)
    stream = b''.join(f.content for f in files)
    print('concatenated stream:', stream)

    total_size = sum(f.size for f in files)
    max_piece_index = math.floor((total_size - 1) // piece_size)
    for piece_index in range(max_piece_index + 1):
        print('testing piece:', piece_index)
        start = piece_index * piece_size
        stop = min(start + piece_size, len(stream))
        exp_piece = stream[start:stop]
        print('exp_piece:', f'[{start}:{stop}]:', exp_piece)
        exp_piece_length = stop - start
        assert len(exp_piece) == exp_piece_length

        torrent = Torrent(piece_size=piece_size, files=files)
        with TorrentFileStream(torrent) as tfs:
            for i in range(3):
                piece = tfs.get_piece(piece_index, content_path=tmp_path)
                assert piece == exp_piece


@pytest.mark.parametrize(
    argnames='piece_size, files, piece_index, exp_max_piece_index',
    argvalues=(
        # First file is smaller than one piece
        # 0           1           2           3           4           5           6           7
        # aaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccccccccc
        (12, [File('a', 11), File('b', 49), File('c', 30)], -1, 7),
        (12, [File('a', 11), File('b', 49), File('c', 30)], 8, 7),

        # Last file is smaller than one piece
        # 0   1   2   3   4   5   6   7
        # aaaaaaaabbbbbbbbbbbbbbbbbccc
        (4, [File('a', 8), File('b', 17), File('c', 3)], -1, 6),
        (4, [File('a', 8), File('b', 17), File('c', 3)], 7, 6),

        # First piece contains multiple complete files
        # 0           1           2           3           4           5           6
        # aaaabbbbbccccccccccccccccccccccccccccccccccccccccccccccccccccc
        (12, [File('a', 4), File('b', 5), File('c', 53)], -1, 5),
        (12, [File('a', 4), File('b', 5), File('c', 53)], 6, 5),

        # Middle piece contains multiple complete files
        # 0              1              2              3
        # aaaaaaaaaaaaaaaaaaaaabbbbbcccdddddddddddddddddddd
        (15, [File('a', 21), File('b', 5), File('c', 3), File('d', 20)], -1, 3),
        (15, [File('a', 21), File('b', 5), File('c', 3), File('d', 20)], 4, 3),

        # Last piece contains multiple complete files
        # 0           1           2           3
        # aaaaaaaaaaaaaaaaaaaaaaaaaabbbbccccc
        (12, [File('a', 26), File('b', 4), File('c', 5)], -1, 2),
        (12, [File('a', 26), File('b', 4), File('c', 5)], 3, 2),
    ),
    ids=lambda v: str(v),
)
def test_get_piece_with_piece_index_out_of_bounds(piece_size, files, piece_index, exp_max_piece_index, tmp_path):
    torrent = Torrent(piece_size=piece_size, files=files)
    tfs = TorrentFileStream(torrent)
    with pytest.raises(ValueError, match=rf'^piece_index must be in range 0 - {exp_max_piece_index}: {piece_index}$'):
        tfs.get_piece(piece_index)


@pytest.mark.parametrize(
    argnames='piece_size, files, missing_files, piece_index, exp_piece',
    argvalues=(
        # 0     1     2     3     4     5     6     7
        # abcd
        (6, [File('a', 1), File('b', 1), File('c', 1), File('d', 1)], ['a'], 0, None),
        (6, [File('a', 1), File('b', 1), File('c', 1), File('d', 1)], ['b'], 0, None),
        (6, [File('a', 1), File('b', 1), File('c', 1), File('d', 1)], ['c'], 0, None),
        (6, [File('a', 1), File('b', 1), File('c', 1), File('d', 1)], ['d'], 0, None),

        # 0     1     2     3     4     5     6     7
        # aaaaaaaaaaabbbcccccccccccccccccdddddd
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a'], 0, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a'], 1, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a'], 2, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a'], 3, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a'], 4, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a'], 5, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a'], 6, bytes),

        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['b'], 0, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['b'], 1, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['b'], 2, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['b'], 3, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['b'], 4, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['b'], 5, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['b'], 6, bytes),

        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['c'], 0, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['c'], 1, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['c'], 2, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['c'], 3, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['c'], 4, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['c'], 5, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['c'], 6, bytes),

        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['d'], 0, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['d'], 1, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['d'], 2, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['d'], 3, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['d'], 4, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['d'], 5, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['d'], 6, None),

        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'b'], 0, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'b'], 1, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'b'], 2, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'b'], 3, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'b'], 4, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'b'], 5, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'b'], 6, bytes),

        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'c'], 0, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'c'], 1, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'c'], 2, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'c'], 3, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'c'], 4, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'c'], 5, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'c'], 6, bytes),

        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'd'], 0, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'd'], 1, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'd'], 2, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'd'], 3, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'd'], 4, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'd'], 5, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['a', 'd'], 6, None),

        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['c', 'd'], 0, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['c', 'd'], 1, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['c', 'd'], 2, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['c', 'd'], 3, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['c', 'd'], 4, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['c', 'd'], 5, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['c', 'd'], 6, None),

        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['b', 'd'], 0, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['b', 'd'], 1, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['b', 'd'], 2, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['b', 'd'], 3, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['b', 'd'], 4, bytes),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['b', 'd'], 5, None),
        (6, [File('a', 11), File('b', 3), File('c', 17), File('d', 6)], ['b', 'd'], 6, None),
    ),
    ids=lambda v: str(v),
)
def test_get_piece_with_missing_file(piece_size, files, missing_files, piece_index, exp_piece, tmp_path):
    for f in files:
        if f not in missing_files:
            print(f'writing {f}: {f.size} bytes: {f.content}')
            f.write_at(tmp_path)
        else:
            print(f'not writing {f}: {f.size} bytes: {f.content}')

    torrent = Torrent(piece_size=piece_size, files=files)
    tfs = TorrentFileStream(torrent)
    piece = tfs.get_piece(piece_index, content_path=tmp_path)
    if exp_piece is None:
        assert piece is None
    else:
        assert isinstance(piece, bytes)


@pytest.mark.parametrize(
    argnames='piece_size, files, contents, piece_index, exp_result',
    argvalues=(
        # 0     1     2     3     4     5
        # aaaaaaaaaaabbbbbbbbbbbbcccccc
        (6, [File('a', 11), File('b', 12), File('c', 6)], {'a': b'x' * 1}, 0, Exception('a')),
        (6, [File('a', 11), File('b', 12), File('c', 6)], {'a': b'x' * 2}, 1, Exception('a')),
        (6, [File('a', 11), File('b', 12), File('c', 6)], {'a': b'x' * 3}, 2, bytes),
        (6, [File('a', 11), File('b', 12), File('c', 6)], {'a': b'x' * 4}, 3, bytes),
        (6, [File('a', 11), File('b', 12), File('c', 6)], {'a': b'x' * 5}, 4, bytes),
        (6, [File('a', 11), File('b', 12), File('c', 6)], {'b': b'x' * 6}, 0, bytes),
        (6, [File('a', 11), File('b', 12), File('c', 6)], {'b': b'x' * 7}, 1, Exception('b')),
        (6, [File('a', 11), File('b', 12), File('c', 6)], {'b': b'x' * 8}, 2, Exception('b')),
        (6, [File('a', 11), File('b', 12), File('c', 6)], {'b': b'x' * 9}, 3, Exception('b')),
        (6, [File('a', 11), File('b', 12), File('c', 6)], {'b': b'x' * 10}, 4, bytes),
        (6, [File('a', 11), File('b', 12), File('c', 6)], {'c': b'x' * 11}, 0, bytes),
        (6, [File('a', 11), File('b', 12), File('c', 6)], {'c': b'x' * 12}, 1, bytes),
        (6, [File('a', 11), File('b', 12), File('c', 6)], {'c': b'x' * 13}, 2, bytes),
        (6, [File('a', 11), File('b', 12), File('c', 6)], {'c': b'x' * 14}, 3, Exception('c')),
        (6, [File('a', 11), File('b', 12), File('c', 6)], {'c': b'x' * 15}, 4, Exception('c')),
    ),
    ids=lambda v: str(v),
)
def test_get_piece_with_wrong_file_size(piece_size, files, contents, piece_index, exp_result, tmp_path):
    for f in files:
        content = contents.get(f, f.content)
        print(f'{f}: {f.size} bytes: {content}, real size: {len(content)} bytes')
        f.write_at(tmp_path, content)

    torrent = Torrent(piece_size=piece_size, files=files)

    if isinstance(exp_result, BaseException):
        exp_filename = str(exp_result)
        exp_filepath = str(tmp_path / exp_filename)
        exp_filesize = {f: f.size for f in files}[exp_filename]
        actual_file_size = os.path.getsize(tmp_path / exp_filename)
        exp_exception = torf.VerifyFileSizeError(exp_filepath, actual_file_size, exp_filesize)
        print('exp_exception:', repr(exp_exception))

        with TorrentFileStream(torrent) as tfs:
            with pytest.raises(type(exp_exception), match=rf'^{re.escape(str(exp_exception))}$'):
                tfs.get_piece(piece_index, content_path=tmp_path)

    else:
        stream = b''.join(f.content for f in files)
        print('concatenated stream:', stream)
        start = piece_index * piece_size
        stop = min(start + piece_size, len(stream))
        exp_piece = stream[start:stop]
        print('exp_piece:', f'[{start}:{stop}]:', exp_piece)
        exp_piece_length = stop - start
        assert len(exp_piece) == exp_piece_length

        with TorrentFileStream(torrent) as tfs:
            assert tfs.get_piece(piece_index, content_path=tmp_path) == exp_piece


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
    exists_mock = mocker.patch('os.path.exists', return_value=False)
    open_mock = mocker.patch('__main__.open')
    torrent = Torrent(piece_size=123, files=(File('a', 1), File('b', 2), File('c', 3)))
    tfs = TorrentFileStream(torrent)
    assert tfs._get_open_file('foo/path/b') is None
    assert exists_mock.call_args_list == [call('foo/path/b')]
    assert open_mock.call_args_list == []

def test_get_open_file_fails_to_open_file(mocker):
    exists_mock = mocker.patch('os.path.exists', return_value=True)
    open_mock = mocker.patch('builtins.open', side_effect=OSError(2, 'nope'))
    torrent = Torrent(piece_size=123, files=(File('a', 1), File('b', 2), File('c', 3)))
    tfs = TorrentFileStream(torrent)
    with pytest.raises(torf.ReadError, match=r'^foo/path/b: No such file or directory$'):
        tfs._get_open_file('foo/path/b')
    assert exists_mock.call_args_list == [call('foo/path/b')]
    assert open_mock.call_args_list == [call('foo/path/b', 'rb')]

def test_get_open_file_opens_file_only_once(mocker):
    exists_mock = mocker.patch('os.path.exists', return_value=True)
    fh1, fh2 = (Mock(), Mock())
    open_mock = mocker.patch('builtins.open', side_effect=(fh1, fh2))
    torrent = Torrent(piece_size=123, files=(File('a', 1), File('b', 2), File('c', 3)))
    tfs = TorrentFileStream(torrent)
    for _ in range(5):
        assert tfs._get_open_file('foo/path/b') == fh1
    assert exists_mock.call_args_list == [call('foo/path/b')]
    assert open_mock.call_args_list == [call('foo/path/b', 'rb')]


@pytest.mark.parametrize(
    argnames='piece_size, files, exp_chunks',
    argvalues=(
        # 0       1       2       3       4       5       6       7       8       8       9
        # ABC
        (8, [File('A', b'a'), File('B', b'b'), File('C', b'c')], [b'abc']),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAAAAABC
        (8, [File('A', b'abcdef'), File('B', b'g'), File('C', b'h')], [b'abcdefgh']),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAAAAABCC
        (8, [File('A', b'abcdef'), File('B', b'g'), File('C', b'hi')], [b'abcdefgh', b'i']),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAAAAABBC
        (8, [File('A', b'abcdef'), File('B', b'gh'), File('C', b'i')], [b'abcdefgh', b'i']),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAAAAABBCC
        (8, [File('A', b'abcdef'), File('B', b'gh'), File('C', b'ij')], [b'abcdefgh', b'ij']),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAAAAAAABBCC
        (8, [File('A', b'abcdefgh'), File('B', b'ij'), File('C', b'kl')], [b'abcdefgh', b'ijkl']),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAAAAAAAABBCC
        (8, [File('A', b'abcdefghi'), File('B', b'jk'), File('C', b'lm')], [b'abcdefgh', b'ijklm']),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAAAAAAAABBCCCCC
        (8, [File('A', b'abcdefghi'), File('B', b'jk'), File('C', b'lmnop')], [b'abcdefgh', b'ijklmnop']),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAAAAAAAABBBCCCCC
        (8, [File('A', b'abcdefghi'), File('B', b'jkl'), File('C', b'mnopq')], [b'abcdefgh', b'ijklmnop', b'q']),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAAAAAAABBBBCCCCC
        (8, [File('A', b'abcdefgh'), File('B', b'ijkl'), File('C', b'mnopq')], [b'abcdefgh', b'ijklmnop', b'q']),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAAAAAABBBBCCCCCC
        (8, [File('A', b'abcdefg'), File('B', b'hijk'), File('C', b'lmnopq')], [b'abcdefgh', b'ijklmnop', b'q']),
        # 0       1       2       3       4       5       6       7       8       8       9
        # ABBBBCCCCC
        (8, [File('A', b'a'), File('B', b'bcde'), File('C', b'fghij')], [b'abcdefgh', b'ij']),
        # 0       1       2       3       4       5       6       7       8       8       9
        # ABBBBCCCC
        (8, [File('A', b'a'), File('B', b'bcde'), File('C', b'fghi')], [b'abcdefgh', b'i']),
        # 0       1       2       3       4       5       6       7       8       8       9
        # ABBBBC
        (8, [File('A', b'a'), File('B', b'bcde'), File('C', b'f')], [b'abcdef']),
    ),
    ids=lambda v: str(v),
)
def test_iter_chunks_without_missing_files(piece_size, files, exp_chunks, tmp_path):
    for f in files:
        # content = os.path.basename(f).encode('utf8') * f.size
        print(f'writing {f}: {f.size} bytes: {f.content}')
        f.write_at(tmp_path)

    torrent = Torrent(piece_size=piece_size, files=files)
    tfs = TorrentFileStream(torrent)
    assert list(tfs.iter_chunks(location=tmp_path)) == exp_chunks


@pytest.mark.parametrize(
    argnames='piece_size, files, missing_files, exp_chunks',
    argvalues=(
        # 0       1       2       3       4       5       6       7       8       8       9
        # ABC
        (8, [File('A', b'a'), File('B', b'b'), File('C', b'c')], ['A'], [None]),
        (8, [File('A', b'a'), File('B', b'b'), File('C', b'c')], ['B'], [None]),
        (8, [File('A', b'a'), File('B', b'b'), File('C', b'c')], ['C'], [None]),
        (8, [File('A', b'a'), File('B', b'b'), File('C', b'c')], ['A', 'B'], [None]),
        (8, [File('A', b'a'), File('B', b'b'), File('C', b'c')], ['B', 'C'], [None]),
        (8, [File('A', b'a'), File('B', b'b'), File('C', b'c')], ['A', 'C'], [None]),
        (8, [File('A', b'a'), File('B', b'b'), File('C', b'c')], ['A', 'B', 'C'], [None]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # AAABBBCC
        (8, [File('A', b'abc'), File('B', b'def'), File('C', b'gh')], ['A'], [None]),
        (8, [File('A', b'abc'), File('B', b'def'), File('C', b'gh')], ['B'], [None]),
        (8, [File('A', b'abc'), File('B', b'def'), File('C', b'gh')], ['C'], [None]),
        (8, [File('A', b'abc'), File('B', b'def'), File('C', b'gh')], ['A', 'B'], [None]),
        (8, [File('A', b'abc'), File('B', b'def'), File('C', b'gh')], ['B', 'C'], [None]),
        (8, [File('A', b'abc'), File('B', b'def'), File('C', b'gh')], ['A', 'C'], [None]),
        (8, [File('A', b'abc'), File('B', b'def'), File('C', b'gh')], ['A', 'B', 'C'], [None]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # ABCCCCCCC
        (8, [File('A', b'a'), File('B', b'b'), File('C', b'cdefghi')], ['A'], [None, b'i']),
        (8, [File('A', b'a'), File('B', b'b'), File('C', b'cdefghi')], ['B'], [None, b'i']),
        (8, [File('A', b'a'), File('B', b'b'), File('C', b'cdefghi')], ['C'], [None, None]),
        (8, [File('A', b'a'), File('B', b'b'), File('C', b'cdefghi')], ['A', 'B'], [None, b'i']),
        (8, [File('A', b'a'), File('B', b'b'), File('C', b'cdefghi')], ['B', 'C'], [None, None]),
        (8, [File('A', b'a'), File('B', b'b'), File('C', b'cdefghi')], ['A', 'C'], [None, None]),
        (8, [File('A', b'a'), File('B', b'b'), File('C', b'cdefghi')], ['A', 'B', 'C'], [None, None]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # ABBBBBBCC
        (8, [File('A', b'a'), File('B', b'bcdefg'), File('C', b'hi')], ['A'], [None, b'i']),
        (8, [File('A', b'a'), File('B', b'bcdefg'), File('C', b'hi')], ['B'], [None, b'i']),
        (8, [File('A', b'a'), File('B', b'bcdefg'), File('C', b'hi')], ['C'], [None, None]),
        (8, [File('A', b'a'), File('B', b'bcdefg'), File('C', b'hi')], ['A', 'B'], [None, b'i']),
        (8, [File('A', b'a'), File('B', b'bcdefg'), File('C', b'hi')], ['B', 'C'], [None, None]),
        (8, [File('A', b'a'), File('B', b'bcdefg'), File('C', b'hi')], ['A', 'C'], [None, None]),
        (8, [File('A', b'a'), File('B', b'bcdefg'), File('C', b'hi')], ['A', 'B', 'C'], [None, None]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # ABBBBBBCCCCCCCCCC
        (8, [File('A', b'a'), File('B', b'bcdefg'), File('C', b'hijklmnopq')], ['A'], [None, b'ijklmnop', b'q']),
        (8, [File('A', b'a'), File('B', b'bcdefg'), File('C', b'hijklmnopq')], ['B'], [None, b'ijklmnop', b'q']),
        (8, [File('A', b'a'), File('B', b'bcdefg'), File('C', b'hijklmnopq')], ['C'], [None, None, None]),
        (8, [File('A', b'a'), File('B', b'bcdefg'), File('C', b'hijklmnopq')], ['A', 'B'], [None, b'ijklmnop', b'q']),
        (8, [File('A', b'a'), File('B', b'bcdefg'), File('C', b'hijklmnopq')], ['B', 'C'], [None, None, None]),
        (8, [File('A', b'a'), File('B', b'bcdefg'), File('C', b'hijklmnopq')], ['A', 'C'], [None, None, None]),
        (8, [File('A', b'a'), File('B', b'bcdefg'), File('C', b'hijklmnopq')], ['A', 'B', 'C'], [None, None, None]),
        # 0       1       2       3       4       5       6       7       8       8       9
        # ABBBBBBBCCCCCCCCCC
        (8, [File('A', b'a'), File('B', b'bcdefgh'), File('C', b'ijklmnopqr')], ['A'], [None, b'ijklmnop', b'qr']),
        (8, [File('A', b'a'), File('B', b'bcdefgh'), File('C', b'ijklmnopqr')], ['B'], [None, b'ijklmnop', b'qr']),
        (8, [File('A', b'a'), File('B', b'bcdefgh'), File('C', b'ijklmnopqr')], ['C'], [b'abcdefgh', None, None]),
        (8, [File('A', b'a'), File('B', b'bcdefgh'), File('C', b'ijklmnopqr')], ['A', 'B'], [None, b'ijklmnop', b'qr']),
        (8, [File('A', b'a'), File('B', b'bcdefgh'), File('C', b'ijklmnopqr')], ['B', 'C'], [None, None, None]),
        (8, [File('A', b'a'), File('B', b'bcdefgh'), File('C', b'ijklmnopqr')], ['A', 'C'], [None, None, None]),
        (8, [File('A', b'a'), File('B', b'bcdefgh'), File('C', b'ijklmnopqr')], ['A', 'B', 'C'], [None, None, None]),
        # 0   1   2   3   4   5   6   7   8   8   9
        # AAAAABBBCCCCCC
        (4, [File('A', b'abcde'), File('B', b'fgh'), File('C', b'ijklmn')], ['A'], [None, None, b'ijkl', b'mn']),
        (4, [File('A', b'abcde'), File('B', b'fgh'), File('C', b'ijklmn')], ['B'], [b'abcd', None, b'ijkl', b'mn']),
        (4, [File('A', b'abcde'), File('B', b'fgh'), File('C', b'ijklmn')], ['C'], [b'abcd', b'efgh', None, None]),
        (4, [File('A', b'abcde'), File('B', b'fgh'), File('C', b'ijklmn')], ['A', 'B'], [None, None, b'ijkl', b'mn']),
        (4, [File('A', b'abcde'), File('B', b'fgh'), File('C', b'ijklmn')], ['B', 'C'], [b'abcd', None, None, None]),
        (4, [File('A', b'abcde'), File('B', b'fgh'), File('C', b'ijklmn')], ['A', 'C'], [None, None, None, None]),
        (4, [File('A', b'abcde'), File('B', b'fgh'), File('C', b'ijklmn')], ['A', 'B', 'C'], [None, None, None, None]),
        # 0   1   2   3   4   5   6   7   8   8   9
        # AAAAABBBBCCCCC
        (4, [File('A', b'abcde'), File('B', b'fghi'), File('C', b'jklmn')], ['A'], [None, None, b'ijkl', b'mn']),
        (4, [File('A', b'abcde'), File('B', b'fghi'), File('C', b'jklmn')], ['B'], [b'abcd', None, None, b'mn']),
        (4, [File('A', b'abcde'), File('B', b'fghi'), File('C', b'jklmn')], ['C'], [b'abcd', b'efgh', None, None]),
        (4, [File('A', b'abcde'), File('B', b'fghi'), File('C', b'jklmn')], ['A', 'B'], [None, None, None, b'mn']),
        (4, [File('A', b'abcde'), File('B', b'fghi'), File('C', b'jklmn')], ['B', 'C'], [b'abcd', None, None, None]),
        (4, [File('A', b'abcde'), File('B', b'fghi'), File('C', b'jklmn')], ['A', 'C'], [None, None, None, None]),
        (4, [File('A', b'abcde'), File('B', b'fghi'), File('C', b'jklmn')], ['A', 'B', 'C'], [None, None, None, None]),
        # 0   1   2   3   4   5   6   7   8   8   9
        # AAABBBBBCCCCC
        (4, [File('A', b'abc'), File('B', b'defgh'), File('C', b'ijklmn')], ['A'], [None, b'efgh', b'ijkl', b'mn']),
        (4, [File('A', b'abc'), File('B', b'defgh'), File('C', b'ijklmn')], ['B'], [None, None, b'ijkl', b'mn']),
        (4, [File('A', b'abc'), File('B', b'defgh'), File('C', b'ijklmn')], ['C'], [b'abcd', b'efgh', None, None]),
        (4, [File('A', b'abc'), File('B', b'defgh'), File('C', b'ijklmn')], ['A', 'B'], [None, None, b'ijkl', b'mn']),
        (4, [File('A', b'abc'), File('B', b'defgh'), File('C', b'ijklmn')], ['B', 'C'], [None, None, None, None]),
        (4, [File('A', b'abc'), File('B', b'defgh'), File('C', b'ijklmn')], ['A', 'C'], [None, b'efgh', None, None]),
        (4, [File('A', b'abc'), File('B', b'defgh'), File('C', b'ijklmn')], ['A', 'B', 'C'], [None, None, None, None]),
    ),
    ids=lambda v: str(v),
)
def test_iter_chunks_with_missing_files(piece_size, files, missing_files, exp_chunks, tmp_path):
    for f in files:
        if f not in missing_files:
            print(f'writing {f}: {f.size} bytes: {f.content}')
            f.write_at(tmp_path)
        else:
            print(f'not writing {f}: {f.size} bytes: {f.content}')

    torrent = Torrent(piece_size=piece_size, files=files)
    tfs = TorrentFileStream(torrent)
    assert list(tfs.iter_chunks(location=tmp_path)) == exp_chunks


def test_get_piece_hash_from_readable_piece(mocker):
    torrent = Torrent(piece_size=123, files=(File('a', 1), File('b', 2), File('c', 3)))
    tfs = TorrentFileStream(torrent)
    get_piece_mock = mocker.patch.object(tfs, 'get_piece', return_value=b'mock piece')
    sha1_mock = mocker.patch('hashlib.sha1', return_value=Mock(digest=Mock(return_value=b'mock hash')))
    assert tfs.get_piece_hash(123, location='foo/path') == b'mock hash'
    assert get_piece_mock.call_args_list == [call(123, location='foo/path')]
    assert sha1_mock.call_args_list == [call(b'mock piece')]

def test_get_piece_hash_from_unreadable_piece(mocker):
    torrent = Torrent(piece_size=123, files=(File('a', 1), File('b', 2), File('c', 3)))
    tfs = TorrentFileStream(torrent)
    get_piece_mock = mocker.patch.object(tfs, 'get_piece', return_value=None)
    sha1_mock = mocker.patch('hashlib.sha1', return_value=Mock(digest=Mock(return_value=b'mock hash')))
    assert tfs.get_piece_hash(123, location='foo/path') is None
    assert get_piece_mock.call_args_list == [call(123, location='foo/path')]
    assert sha1_mock.call_args_list == []


def test_verify_piece_gets_valid_piece_index(mocker):
    torrent = Torrent(piece_size=123, files=(File('a', 1), File('b', 2), File('c', 3)))
    tfs = TorrentFileStream(torrent)
    torrent.hashes = (b'd34d', b'b33f', b'b00b5')
    mocker.patch.object(tfs, 'get_piece_hash', return_value=b'b33f')
    mocker.patch.object(type(tfs), 'max_piece_index', PropertyMock(return_value=2))
    assert tfs.verify_piece(0, location='foo/path') is False
    assert tfs.verify_piece(1, location='foo/path') is True
    assert tfs.verify_piece(2, location='foo/path') is False
    with pytest.raises(ValueError, match=r'^piece_index must be in range 0 - 2: 3$'):
        tfs.verify_piece(3, location='foo/path')

def test_verify_piece_returns_None_for_nonexisting_files(mocker):
    torrent = Torrent(piece_size=123, files=(File('a', 1), File('b', 2), File('c', 3)))
    torrent.hashes = (b'd34d', b'b33f', b'b00b5')
    tfs = TorrentFileStream(torrent)
    mocker.patch.object(tfs, 'get_piece_hash', return_value=None)
    mocker.patch.object(type(tfs), 'max_piece_index', PropertyMock(return_value=2))
    assert tfs.verify_piece(0, location='foo/path') is None
    assert tfs.verify_piece(1, location='foo/path') is None
    assert tfs.verify_piece(2, location='foo/path') is None
    with pytest.raises(ValueError, match=r'^piece_index must be in range 0 - 2: 3$'):
        tfs.verify_piece(3, location='foo/path')
