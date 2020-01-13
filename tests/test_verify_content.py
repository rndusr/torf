import torf

import pytest
from unittest import mock
import os
import shutil
import random
from collections import defaultdict
import logging
log = logging.getLogger('test')


class CollectingCallback():
    def __init__(self, torrent):
        super().__init__()
        self.torrent = torrent
        self.good_pieces = defaultdict(lambda: [])
        self.corrupt_pieces = defaultdict(lambda: [])
        self.skipped_pieces = defaultdict(lambda: [])

    def __call__(self, t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
        assert t is self.torrent
        assert pieces_total == t.pieces
        assert 1 <= pieces_done <= pieces_total
        if exc is not None:
            self.corrupt_pieces[os.path.basename(path)].append((piece_index, str(exc)))
        elif piece_hash is None:
            assert exc is None
            self.skipped_pieces[os.path.basename(path)].append(piece_index)
        else:
            assert exc is None
            assert type(piece_hash) is bytes and len(piece_hash) == 20
            self.good_pieces[os.path.basename(path)].append(piece_index)


def calc_exp_piece_indexes(*files, piece_size):
    combined_size = sum(size for name,size in files)
    current_piece_index = 0
    exp_piece_indexes = {}
    pos = 0
    for i,(filename,filesize) in enumerate(files):
        exp_piece_indexes[filename] = list(range(
            pos // piece_size,
            (pos + filesize) // piece_size))
        pos += filesize
        # If combined sizes are not divisible by piece size, we must add another
        # piece_index to the last file
        indexes = exp_piece_indexes[filename]
        is_last_file = i == len(files)-1
        if indexes:
            # Keep track of the last added piece_index
            current_piece_index = indexes[-1]
        if is_last_file and combined_size % piece_size != 0:
            exp_piece_indexes[filename].append(current_piece_index+1)
    return exp_piece_indexes


def test_validate_is_called_first(monkeypatch):
    torrent = torf.Torrent()
    mock_validate = mock.Mock(side_effect=torf.MetainfoError('Mock error'))
    monkeypatch.setattr(torrent, 'validate', mock_validate)
    with pytest.raises(torf.MetainfoError) as excinfo:
        torrent.verify('some/path')
    assert str(excinfo.value) == f'Invalid metainfo: Mock error'
    mock_validate.assert_called_once_with()


def test_verify_content_successfully_with_singlefile_torrent(file_size, piece_size,
                                                             create_file, create_torrent_file, forced_piece_size):
    with forced_piece_size(piece_size):
        content_path = create_file('some file', file_size)
        with create_torrent_file(path=content_path) as torrent_file:
            torrent = torf.Torrent.read(torrent_file)

            log.debug('################ TEST WITHOUT CALLBACK ##################')
            assert torrent.verify(content_path, skip_file_on_first_error=False) == True

            log.debug('################ TEST WITH CALLBACK ##################')
            exp_piece_indexes = list(range(torrent.pieces))
            exp_call_count = len(exp_piece_indexes)
            def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                assert t == torrent
                assert str(path) == str(content_path)
                assert 1 <= pieces_done <= pieces_total
                assert pieces_total == torrent.pieces
                assert 0 <= piece_index <= pieces_total - 1
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert exc is None
                exp_piece_indexes.remove(piece_index)
            cb = mock.Mock(side_effect=assert_call)
            assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == True
            assert len(exp_piece_indexes) == 0, exp_piece_indexes
            assert cb.call_count == exp_call_count


def test_verify_content__random_corruption_in_singlefile_torrent(file_size, piece_size, forced_piece_size,
                                                                 create_file, create_torrent_file):
    file_size = int(file_size * (random.random() * 4 + 2))
    with forced_piece_size(piece_size):
        print('file_size:', file_size, 'piece_size:', piece_size)
        data = create_file.random_bytes(file_size)
        content_path = create_file('content.jpg', data)
        with create_torrent_file(path=content_path) as torrent_file:
            torrent = torf.Torrent.read(torrent_file)

            for offset in (1, torrent.pieces//2*piece_size, file_size-2):
                print('offset:', offset)

                for errpos in (offset-1, offset, offset+1):
                    print('error position:', errpos)
                    data_corrupt = bytearray(data)
                    data_corrupt[errpos] = (data[errpos] + 1) % 256
                    assert len(data_corrupt) == len(data)
                    assert data_corrupt != data
                    content_path.write_bytes(data_corrupt)

                    corrupt_piece_index = errpos // piece_size
                    print('corrupt_piece_index:', corrupt_piece_index)

                    log.debug('################ TEST WITHOUT CALLBACK ##################')
                    with pytest.raises(torf.VerifyContentError) as excinfo:
                        torrent.verify(content_path, skip_file_on_first_error=False)
                    assert str(excinfo.value) == f'Corruption in piece {corrupt_piece_index+1}'

                    log.debug('################ TEST WITH CALLBACK ##################')
                    def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                        assert t == torrent
                        assert pieces_total == torrent.pieces
                        assert str(path) == str(content_path)
                        assert 1 <= pieces_done <= pieces_total
                        assert 0 <= piece_index <= pieces_total - 1
                        assert type(piece_hash) is bytes and len(piece_hash) == 20
                        if piece_index == corrupt_piece_index:
                            assert str(exc) == f'Corruption in piece {piece_index+1}'
                        else:
                            assert exc is None
                    cb = mock.Mock(side_effect=assert_call)
                    assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False
                    assert cb.call_count == torrent.pieces


def test_verify_content_successfully_with_multifile_torrent(file_size_a, file_size_b, file_size_c, piece_size,
                                                            create_dir, create_torrent_file, forced_piece_size):
    with forced_piece_size(piece_size):
        content_path = create_dir('content',
                                  ('a', file_size_a),
                                  ('b', file_size_b),
                                  ('c', file_size_c))
        exp_piece_indexes = calc_exp_piece_indexes(('a', file_size_a),
                                                   ('b', file_size_b),
                                                   ('c', file_size_c),
                                                   piece_size=piece_size)
        with create_torrent_file(path=content_path) as torrent_file:
            torrent = torf.Torrent.read(torrent_file)

            log.debug('################ TEST WITHOUT CALLBACK ##################')
            assert torrent.verify(content_path, skip_file_on_first_error=False) == True

            log.debug('################ TEST WITH CALLBACK ##################')
            log.debug(exp_piece_indexes)
            all_exp_piece_indexes = list(range(torrent.pieces))
            exp_call_count = len(all_exp_piece_indexes)
            def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                assert t == torrent
                assert 1 <= pieces_done <= pieces_total
                assert pieces_total == torrent.pieces
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert 0 <= piece_index <= pieces_total - 1
                if piece_index in exp_piece_indexes['a']:
                    assert str(path) == str(content_path / 'a')
                    all_exp_piece_indexes.remove(piece_index)
                elif piece_index in exp_piece_indexes['b']:
                    assert str(path) == str(content_path / 'b')
                    all_exp_piece_indexes.remove(piece_index)
                elif piece_index in exp_piece_indexes['c']:
                    assert str(path) == str(content_path / 'c')
                    all_exp_piece_indexes.remove(piece_index)
                assert exc is None
            cb = mock.Mock(side_effect=assert_call)
            assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == True
            assert len(all_exp_piece_indexes) == 0, all_exp_piece_indexes
            assert cb.call_count == exp_call_count

# TODO:
# def test_verify_content__random_corruption_in_multifile_torrent(file_size, piece_size, forced_piece_size,
#                                                                 create_file, create_torrent_file):

def test_verify_content__file_is_missing(create_dir, create_torrent_file, forced_piece_size):
    with forced_piece_size(8) as piece_size:
        content_path = create_dir('content',
                                  ('a', 1*piece_size+3),
                                  ('b', 2*piece_size+4),
                                  ('c', 2*piece_size+5))
        with create_torrent_file(path=content_path) as torrent_file:
            torrent = torf.Torrent.read(torrent_file)

            os.rename(content_path / 'b', content_path / 'b.deleted')
            assert not os.path.exists(content_path / 'b')

            log.debug('################ TEST WITHOUT CALLBACK ##################')
            with pytest.raises(torf.ReadError) as excinfo:
                torrent.verify(content_path, skip_file_on_first_error=False)
            assert str(excinfo.value) == f'{content_path / "b"}: No such file or directory'

            log.debug('################ TEST WITH CALLBACK ##################')
            # (8+3) + (2*8+4) + (2*8+5) = 7 pieces (max_piece_index=6)
            exp_piece_indexes = [
                0, # stream slice  0 -  8: a[ 0: 8]         - ok
                1, # stream slice  8 - 16: a[-3:  ] + b[:5] - ReadError
                1, # stream slice  8 - 16: a[-3:  ] + b[:5] - fake piece
                2, # stream slice 16 - 24: b[ 5:13]         - fake piece
                3, # stream slice 24 - 32: b[13:20] + c[:1] - ReadError
                4, # stream slice 32 - 40: c[ 1: 9]         - ok
                5, # stream slice 40 - 48: c[ 9:17]         - ok
                6, # stream slice 48 - 52: c[17:21]         - ok
            ]
            exp_call_count = len(exp_piece_indexes)

            def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                assert t == torrent
                assert pieces_total == torrent.pieces
                assert 1 <= pieces_done <= pieces_total
                if piece_index in (0,):
                    assert str(path) == str(content_path / 'a')
                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert exc is None
                    exp_piece_indexes.remove(piece_index)
                elif piece_index in (1,):
                    assert str(path) == str(content_path / 'b')
                    assert piece_hash is None
                    # We get piece_index=1 once for the ReadError and once for
                    # the fake piece
                    if exp_piece_indexes.count(1) == 2:
                        assert str(exc) == f'{content_path / "b"}: No such file or directory'
                    else:
                        assert exc is None
                    exp_piece_indexes.remove(piece_index)
                elif piece_index in (2,):
                    assert str(path) == str(content_path / 'b')
                    assert piece_hash is None
                    assert exc is None
                    exp_piece_indexes.remove(piece_index)
                elif piece_index in (3,):
                    assert str(path) == str(content_path / 'c')
                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert str(exc) == (f'Corruption in piece {piece_index+1}, '
                                        'at least one of these files is corrupt: '
                                        f'{content_path / "b"}, {content_path / "c"}')
                    exp_piece_indexes.remove(piece_index)
                elif piece_index in (4, 5, 6):
                    assert str(path) == str(content_path / "c")
                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert exc is None
                    exp_piece_indexes.remove(piece_index)
            cb = mock.Mock(side_effect=assert_call)
            assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False
            assert len(exp_piece_indexes) == 0, exp_piece_indexes
            assert cb.call_count == exp_call_count


def test_verify_content__file_is_smaller(create_dir, create_torrent_file, forced_piece_size):
    with forced_piece_size(8) as piece_size:
        b_data = create_dir.random_bytes(2*piece_size+4)
        content_path = create_dir('content',
                                  ('a', 1*piece_size+3),
                                  ('b', b_data),
                                  ('c', 1*piece_size+5))
        with create_torrent_file(path=content_path) as torrent_file:
            torrent = torf.Torrent.read(torrent_file)

            corruption_offset = piece_size + 2
            b_data_corrupt = b_data[:corruption_offset] + b_data[corruption_offset+1:]
            assert len(b_data_corrupt) == len(b_data) - 1
            (content_path / 'b').write_bytes(b_data_corrupt)

            log.debug('################ TEST WITHOUT CALLBACK ##################')
            with pytest.raises(torf.VerifyFileSizeError) as excinfo:
                torrent.verify(content_path, skip_file_on_first_error=False)
            assert str(excinfo.value) == f'{content_path / "b"}: Too small: 19 instead of 20 bytes'

            log.debug('################ TEST WITH CALLBACK ##################')
            # (8+3) + (2*8+4) + (8+5) = 6 pieces (max_piece_index=5)
            exp_piece_indexes = [
                0,    # stream slice  0 -  8: a[ 0: 8]         - ok
                1,    # stream slice  8 - 16: a[-3:  ] + b[:5] - VerifyFileSizeError for b
                1,    # stream slice  8 - 16: a[-3:  ] + b[:5] - ok
                2,    # stream slice 16 - 24: b[ 5:13]         - missing byte at size(a) + corruption_offset
                3,    # stream slice 24 - 32: b[13:20] + c[:1] - VerifyContentError for b
                4,    # stream slice 32 - 40: c[ 1: 9]         - ok
                5,    # stream slice 40 - 44: c[ 9:13]         - ok
            ]
            exp_call_count = len(exp_piece_indexes)
            exp_piece_1_exc = [torf.VerifyFileSizeError, type(None)]
            def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                assert t == torrent
                assert pieces_total == torrent.pieces
                assert 1 <= pieces_done <= pieces_total
                if piece_index == 0:
                    assert str(path) == str(content_path / 'a')
                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert exc is None
                    exp_piece_indexes.remove(piece_index)
                elif piece_index == 1:
                    exp_piece_1_exc.remove(type(exc))
                    if isinstance(exc, torf.VerifyFileSizeError):
                        assert str(path) == str(content_path / 'b')
                        assert piece_hash is None
                        assert str(exc) == f'{content_path / "b"}: Too small: 19 instead of 20 bytes'
                    else:
                        assert str(path) == str(content_path / 'b')
                        assert type(piece_hash) is bytes and len(piece_hash) == 20
                        assert exc is None
                    exp_piece_indexes.remove(piece_index)
                elif piece_index == 2:
                    assert str(path) == str(content_path / 'b')
                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert str(exc) == f'Corruption in piece 3 in {content_path / "b"}'
                    exp_piece_indexes.remove(piece_index)
                elif piece_index == 3:
                    assert isinstance(exc, torf.VerifyContentError)
                    assert str(path) == str(content_path / 'c')
                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert str(exc) == (f'Corruption in piece 4, at least one of these files is corrupt: '
                                        f'{content_path / "b"}, {content_path / "c"}')
                    exp_piece_indexes.remove(piece_index)
                elif piece_index in (4, 5):
                    assert str(path) == str(content_path / 'c')
                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert exc is None
                    exp_piece_indexes.remove(piece_index)
            cb = mock.Mock(side_effect=assert_call)
            assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False
            assert len(exp_piece_indexes) == 0, exp_piece_indexes
            assert len(exp_piece_1_exc) == 0, exp_piece_1_exc
            assert cb.call_count == exp_call_count


def test_verify_content__file_contains_extra_bytes_in_the_middle(create_dir, create_torrent_file, forced_piece_size):
    with forced_piece_size(8) as piece_size:
        b_data = create_dir.random_bytes(2*piece_size+4)
        content_path = create_dir('content',
                                  ('a', 1*piece_size+3),
                                  ('b', b_data),
                                  ('c', 1*piece_size+5))
        with create_torrent_file(path=content_path) as torrent_file:
            torrent = torf.Torrent.read(torrent_file)

            corruption_offset = 2*piece_size + 1
            b_data_corrupt = b_data[:corruption_offset] + b'\x12' + b_data[corruption_offset:]
            assert len(b_data_corrupt) == len(b_data) + 1
            (content_path / 'b').write_bytes(b_data_corrupt)

            log.debug('################ TEST WITHOUT CALLBACK ##################')
            with pytest.raises(torf.TorfError) as excinfo:
                torrent.verify(content_path, skip_file_on_first_error=False)
            assert str(excinfo.value) == f'{content_path / "b"}: Too big: 21 instead of 20 bytes'

            log.debug('################ TEST WITH CALLBACK ##################')
            # (8+3) + (2*8+4) + (8+5) = 6 pieces (max_piece_index=5)
            exp_piece_indexes = [
                0,    # stream slice  0 -  8: a[ 0: 8]         - ok
                1,    # stream slice  8 - 16: a[-3:  ] + b[:5] - VerifyFileSizeError for b
                1,    # stream slice  8 - 16: a[-3:  ] + b[:5] - ok
                2,    # stream slice 16 - 24: b[ 5:13]         - corrupt, byte 28 in stream has corrupt byte inserted
                3,    # stream slice 24 - 32: b[13:20] + c[:1] - VerifyContentError for b
                4,    # stream slice 32 - 40: c[ 1: 9]         - ok
                5,    # stream slice 40 - 44: c[ 9:13]         - ok
            ]
            exp_call_count = len(exp_piece_indexes)
            exp_piece_1_exc = [torf.VerifyFileSizeError, type(None)]
            def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                assert t == torrent
                assert pieces_total == torrent.pieces
                assert 1 <= pieces_done <= pieces_total
                if piece_index == 0:
                    assert str(path) == str(content_path / 'a')
                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert exc is None
                    exp_piece_indexes.remove(piece_index)
                elif piece_index == 1:
                    exp_piece_1_exc.remove(type(exc))
                    if isinstance(exc, torf.VerifyFileSizeError):
                        assert str(path) == str(content_path / 'b')
                        assert piece_hash is None
                        assert str(exc) == f'{content_path / "b"}: Too big: 21 instead of 20 bytes'
                    else:
                        assert str(path) == str(content_path / 'b')
                        assert type(piece_hash) is bytes and len(piece_hash) == 20
                        assert exc is None
                    exp_piece_indexes.remove(piece_index)
                elif piece_index == 2:
                    assert str(path) == str(content_path / 'b')
                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert exc is None
                    exp_piece_indexes.remove(piece_index)
                elif piece_index == 3:
                    assert str(path) == str(content_path / 'c')
                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert str(exc) == (f'Corruption in piece {piece_index+1}, '
                                        'at least one of these files is corrupt: '
                                        f'{content_path / "b"}, {content_path / "c"}')
                    exp_piece_indexes.remove(piece_index)
                elif piece_index in (4, 5):
                    assert str(path) == str(content_path / 'c')
                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert exc is None
                    exp_piece_indexes.remove(piece_index)
            cb = mock.Mock(side_effect=assert_call)
            assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False
            assert len(exp_piece_indexes) == 0, exp_piece_indexes
            assert len(exp_piece_1_exc) == 0, exp_piece_1_exc
            assert cb.call_count == exp_call_count


def test_verify_content__file_contains_extra_bytes_at_the_end(create_dir, create_torrent_file, forced_piece_size):
    with forced_piece_size(8) as piece_size:
        b_data = create_dir.random_bytes(2*piece_size+4)
        content_path = create_dir('content',
                                  ('a', 1*piece_size+3),
                                  ('b', b_data),
                                  ('c', 1*piece_size+5))
        with create_torrent_file(path=content_path) as torrent_file:
            torrent = torf.Torrent.read(torrent_file)

            corruption_offset = piece_size
            b_data_corrupt = b_data + b'\xff'
            assert len(b_data_corrupt) == len(b_data) + 1
            (content_path / 'b').write_bytes(b_data_corrupt)

            log.debug('################ TEST WITHOUT CALLBACK ##################')
            with pytest.raises(torf.VerifyFileSizeError) as excinfo:
                torrent.verify(content_path, skip_file_on_first_error=False)
            assert str(excinfo.value) == f'{content_path / "b"}: Too big: 21 instead of 20 bytes'

            log.debug('################ TEST WITH CALLBACK ##################')
            # (8+3) + (2*8+4) + (8+5) = 6 pieces (max_piece_index=5)
            exp_piece_indexes = [
                0,    # stream slice  0 -  8: a[ 0: 8]         - ok
                1,    # stream slice  8 - 16: a[-3:  ] + b[:5] - VerifyFileSizeError for b
                1,    # stream slice  8 - 16: a[-3:  ] + b[:5] - ok
                2,    # stream slice 16 - 24: b[ 5:13]         - ok
                3,    # stream slice 24 - 32: b[13:20] + c[:1] - ok
                4,    # stream slice 32 - 40: c[ 1: 9]         - ok
                5,    # stream slice 40 - 44: c[ 9:13]         - ok
            ]

            exp_call_count = len(exp_piece_indexes)
            exp_piece_1_exc = [torf.VerifyFileSizeError, type(None)]
            def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                assert t == torrent
                assert pieces_total == torrent.pieces
                assert 1 <= pieces_done <= pieces_total
                if piece_index == 0:
                    assert str(path) == str(content_path / 'a')
                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert exc is None
                    exp_piece_indexes.remove(piece_index)
                elif piece_index == 1:
                    exp_piece_1_exc.remove(type(exc))
                    if isinstance(exc, torf.VerifyFileSizeError):
                        assert str(path) == str(content_path / 'b')
                        assert piece_hash is None
                        assert str(exc) == f'{content_path / "b"}: Too big: 21 instead of 20 bytes'
                    else:
                        assert str(path) == str(content_path / 'b')
                        assert type(piece_hash) is bytes and len(piece_hash) == 20
                        assert exc is None
                    exp_piece_indexes.remove(piece_index)
                elif piece_index == 2:
                    assert str(path) == str(content_path / 'b')
                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert exc is None
                    exp_piece_indexes.remove(piece_index)
                elif piece_index == 3:
                    assert str(path) == str(content_path / 'c')
                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert exc is None
                    exp_piece_indexes.remove(piece_index)
                elif piece_index in (4, 5):
                    assert str(path) == str(content_path / 'c')
                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert exc is None
                    exp_piece_indexes.remove(piece_index)
            cb = mock.Mock(side_effect=assert_call)
            assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False
            assert len(exp_piece_indexes) == 0, exp_piece_indexes
            assert len(exp_piece_1_exc) == 0, exp_piece_1_exc
            assert cb.call_count == exp_call_count


def test_verify_content__file_is_same_size_but_corrupt(create_dir, create_torrent_file, forced_piece_size):
    with forced_piece_size(8) as piece_size:
        b_data = create_dir.random_bytes(2*piece_size+4)
        content_path = create_dir('content',
                                  ('a', 1*piece_size+3),
                                  ('b', b_data),
                                  ('c', 1*piece_size+5))
        with create_torrent_file(path=content_path) as torrent_file:
            torrent = torf.Torrent.read(torrent_file)

            corruption_offset = 2*piece_size+4 - 1
            b_data_corrupt = b_data[:corruption_offset] + b'\x12' + b_data[corruption_offset+1:]
            assert len(b_data_corrupt) == len(b_data)
            (content_path / 'b').write_bytes(b_data_corrupt)

            log.debug('################ TEST WITHOUT CALLBACK ##################')
            with pytest.raises(torf.VerifyContentError) as excinfo:
                torrent.verify(content_path, skip_file_on_first_error=False)
            assert str(excinfo.value) == (f'Corruption in piece 4, at least one of these files is corrupt: '
                                          f'{content_path / "b"}, {content_path / "c"}')

            log.debug('################ TEST WITH CALLBACK ##################')
            # (8+3) + (2*8+4) + (8+5) = 6 pieces (max_piece_index=5)
            exp_piece_indexes = [
                0,    # stream slice  0 -  8: a[ 0: 8]         - ok
                1,    # stream slice  8 - 16: a[-3:  ] + b[:5] - ok
                2,    # stream slice 16 - 24: b[ 5:13]         - ok
                3,    # stream slice 24 - 32: b[13:20] + c[:1] - VerifyContentError
                4,    # stream slice 32 - 40: c[ 1: 9]         - ok
                5,    # stream slice 40 - 44: c[ 9:13]         - ok
            ]

            exp_call_count = len(exp_piece_indexes)
            def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                assert t == torrent
                assert pieces_total == torrent.pieces
                assert 1 <= pieces_done <= pieces_total
                if piece_index == 0:
                    assert str(path) == str(content_path / 'a')
                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert exc is None
                    exp_piece_indexes.remove(piece_index)
                elif piece_index in (1, 2):
                    assert str(path) == str(content_path / 'b')
                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert exc is None
                    exp_piece_indexes.remove(piece_index)
                elif piece_index == 3:
                    assert str(path) == str(content_path / 'c')
                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert str(exc) == (f'Corruption in piece 4, at least one of these files is corrupt: '
                                        f'{content_path / "b"}, {content_path / "c"}')
                    exp_piece_indexes.remove(piece_index)
                elif piece_index in (4, 5):
                    assert str(path) == str(content_path / 'c')
                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert exc is None
                    exp_piece_indexes.remove(piece_index)
            cb = mock.Mock(side_effect=assert_call)
            assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False
            assert len(exp_piece_indexes) == 0, exp_piece_indexes
            assert cb.call_count == exp_call_count


def test_verify_content__skip_file_on_first_read_error(create_dir, create_torrent_file, forced_piece_size):
    with forced_piece_size(1024) as piece_size:
        b_data = create_dir.random_bytes(30*piece_size+4)
        content_path = create_dir('content',
                                  ('a', 1*piece_size+3),
                                  ('b', b_data),
                                  ('c', 20*piece_size+5))
        with create_torrent_file(path=content_path) as torrent_file:
            torrent = torf.Torrent.read(torrent_file)

            # Create one corruption at the beginning to trigger the skipping and
            # another corruption in the last piece so that the first piece of
            # "c" is also corrupt.
            os.rename(content_path / 'b', content_path / 'b.orig')
            assert not os.path.exists(content_path / 'b')

            log.debug('################ TEST WITHOUT CALLBACK ##################')
            with pytest.raises(torf.ReadError) as excinfo:
                torrent.verify(content_path, skip_file_on_first_error=True)
            assert str(excinfo.value) == f'{content_path / "b"}: No such file or directory'

            log.debug('################ TEST WITH CALLBACK ##################')
            cb = CollectingCallback(torrent)
            assert torrent.verify(content_path, skip_file_on_first_error=True, callback=cb, interval=0) == False
            log.debug(f'good pieces: {dict(cb.good_pieces)}')
            log.debug(f'corrupt pieces: {dict(cb.corrupt_pieces)}')
            log.debug(f'skipped pieces: {dict(cb.skipped_pieces)}')

            assert cb.good_pieces['a'] == [0]
            assert cb.good_pieces['b'] == []
            assert cb.good_pieces['c'] == list(range(32, 52))
            assert cb.corrupt_pieces['a'] == []
            assert cb.corrupt_pieces['b'] == [(1, f'{content_path / "b"}: No such file or directory')]
            assert cb.corrupt_pieces['c'] == [(31, (f'Corruption in piece 32, at least one of these files is corrupt: '
                                                    f'{content_path / "b"}, {content_path / "c"}'))]
            assert cb.skipped_pieces['a'] == []
            assert cb.skipped_pieces['b'] == list(range(1, 31))
            assert cb.skipped_pieces['c'] == []


def test_verify_content__skip_file_on_first_file_size_error(create_dir, create_torrent_file, forced_piece_size):
    with forced_piece_size(1024) as piece_size:
        b_data = create_dir.random_bytes(30*piece_size+4)
        content_path = create_dir('content',
                                  ('a', 1*piece_size+3),
                                  ('b', b_data),
                                  ('c', 20*piece_size+5))
        with create_torrent_file(path=content_path) as torrent_file:
            torrent = torf.Torrent.read(torrent_file)

            # Provoke VerifyFileSizeError
            (content_path / 'b').write_bytes(b'nah')
            assert os.path.getsize(content_path / 'b') != len(b_data)

            log.debug('################ TEST WITHOUT CALLBACK ##################')
            with pytest.raises(torf.VerifyFileSizeError) as excinfo:
                torrent.verify(content_path, skip_file_on_first_error=True)
            assert str(excinfo.value) == f'{content_path / "b"}: Too small: 3 instead of 30724 bytes'

            log.debug('################ TEST WITH CALLBACK ##################')
            cb = CollectingCallback(torrent)
            assert torrent.verify(content_path, skip_file_on_first_error=True, callback=cb, interval=0) == False
            log.debug(f'good pieces: {dict(cb.good_pieces)}')
            log.debug(f'corrupt pieces: {dict(cb.corrupt_pieces)}')
            log.debug(f'skipped pieces: {dict(cb.skipped_pieces)}')

            assert cb.good_pieces['a'] == [0]
            assert cb.good_pieces['b'] == []
            assert cb.good_pieces['c'] == list(range(32, 52))
            assert cb.corrupt_pieces['a'] == []
            assert cb.corrupt_pieces['b'] == [(1, f'{content_path / "b"}: Too small: 3 instead of 30724 bytes')]
            assert cb.corrupt_pieces['c'] == [(31, (f'Corruption in piece 32, at least one of these files is corrupt: '
                                                    f'{content_path / "b"}, {content_path / "c"}'))]
            assert cb.skipped_pieces['a'] == []
            assert cb.skipped_pieces['b'] == list(range(1, 31))
            assert cb.skipped_pieces['c'] == []


def test_verify_content__skip_file_on_first_hash_mismatch(create_dir, create_torrent_file, forced_piece_size):
    with forced_piece_size(1024) as piece_size:
        b_data = create_dir.random_bytes(30*piece_size+4)
        content_path = create_dir('content',
                                  ('a', 1*piece_size+3),
                                  ('b', b_data),
                                  ('c', 20*piece_size+5))
        with create_torrent_file(path=content_path) as torrent_file:
            torrent = torf.Torrent.read(torrent_file)

            # Corrupt multiple pieces
            b_data_corrupt = bytearray(b_data)
            b_data_len = len(b_data)
            for pos in (3*piece_size, 10*piece_size, b_data_len-2):
                b_data_corrupt[pos] = (b_data_corrupt[pos] + 1) % 256
            assert b_data_corrupt != b_data
            (content_path / 'b').write_bytes(b_data_corrupt)

            log.debug('################ TEST WITHOUT CALLBACK ##################')
            with pytest.raises(torf.VerifyContentError) as excinfo:
                torrent.verify(content_path, skip_file_on_first_error=True)
            assert str(excinfo.value) == f'Corruption in piece 5 in {content_path / "b"}'

            log.debug('################ TEST WITH CALLBACK ##################')
            cb = CollectingCallback(torrent)
            assert torrent.verify(content_path, skip_file_on_first_error=True,
                                  callback=cb, interval=0) == False
            log.debug(f'good pieces: {dict(cb.good_pieces)}')
            log.debug(f'corrupt pieces: {dict(cb.corrupt_pieces)}')
            log.debug(f'skipped pieces: {dict(cb.skipped_pieces)}')

            assert cb.good_pieces['a'] == [0]
            assert cb.good_pieces['b'] == [1, 2, 3]
            assert cb.good_pieces['c'] == list(range(32, 52))
            assert cb.corrupt_pieces['a'] == []
            assert cb.corrupt_pieces['b'] == [(4, f'Corruption in piece 5 in {content_path / "b"}')]
            assert cb.corrupt_pieces['c'] == [(31, (f'Corruption in piece 32, at least one of these files is corrupt: '
                                                    f'{content_path / "b"}, {content_path / "c"}'))]
            assert cb.skipped_pieces['a'] == []
            assert cb.skipped_pieces['b'] == list(range(5, 31))
            assert cb.skipped_pieces['c'] == []


def test_verify_content__torrent_contains_file_and_path_is_dir(forced_piece_size,
                                                               create_file, create_dir, create_torrent_file):
    with forced_piece_size(8) as piece_size:
        content_path = create_file('content', create_file.random_size(piece_size))
        with create_torrent_file(path=content_path) as torrent_file:
            torrent = torf.Torrent.read(torrent_file)

            os.remove(content_path)
            new_content_path = create_dir('content',
                                          ('a', create_dir.random_size(piece_size)),
                                          ('b', create_dir.random_size(piece_size)))
            assert os.path.isdir(content_path)

            log.debug('################ TEST WITHOUT CALLBACK ##################')
            with pytest.raises(torf.VerifyNotDirectoryError) as excinfo:
                torrent.verify(content_path)
            assert str(excinfo.value) == f'{content_path}: Is a directory'

            log.debug('################ TEST WITH CALLBACK ##################')
            def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                assert t == torrent
                assert pieces_done == 0
                assert pieces_total == torrent.pieces
                assert piece_index == 0
                assert piece_hash is None
                assert str(path) == str(content_path)
                assert str(exc) == f'{content_path}: Is a directory'
            cb = mock.Mock(side_effect=assert_call)
            assert torrent.verify(content_path, callback=cb, interval=0) == False
            assert cb.call_count == 1


def test_verify_content__torrent_contains_dir_and_path_is_file(forced_piece_size,
                                                               create_file, create_dir, create_torrent_file):
    with forced_piece_size(8) as piece_size:
        content_path = create_dir('content',
                                  ('a', create_dir.random_size()),
                                  ('b', create_dir.random_size()))
        with create_torrent_file(path=content_path) as torrent_file:
            torrent = torf.Torrent.read(torrent_file)

            shutil.rmtree(content_path)
            new_content_path = create_file('content', create_file.random_size())
            assert os.path.isfile(content_path)

            log.debug('################ TEST WITHOUT CALLBACK ##################')
            with pytest.raises(torf.VerifyIsDirectoryError) as excinfo:
                torrent.verify(content_path)
            assert str(excinfo.value) == f'{content_path}: Not a directory'

            log.debug('################ TEST WITH CALLBACK ##################')
            def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                assert t == torrent
                assert pieces_done == 0
                assert pieces_total == torrent.pieces
                assert piece_index == 0
                assert piece_hash is None
                assert str(path) == str(content_path / 'a')
                assert str(exc) == f'{content_path}: Not a directory'
            cb = mock.Mock(side_effect=assert_call)
            assert torrent.verify(content_path, callback=cb, interval=0) == False
            assert cb.call_count == 1


def test_verify_content__parent_path_is_unreadable(file_size_a, file_size_b, piece_size,
                                                   create_dir, forced_piece_size, create_torrent_file):
    with forced_piece_size(piece_size):
        content_path = create_dir('content',
                                  ('readable/x/a', file_size_a),
                                  ('unreadable/x/b', file_size_b))
        with create_torrent_file(path=content_path) as torrent_file:
            torrent = torf.Torrent.read(torrent_file)
            unreadable_path_mode = os.stat(content_path / 'unreadable').st_mode
            try:
                os.chmod(content_path / 'unreadable', mode=0o222)

                log.debug('################ TEST WITHOUT CALLBACK ##################')
                with pytest.raises(torf.ReadError) as excinfo:
                    torrent.verify(content_path)
                assert str(excinfo.value) == f'{content_path / "unreadable/x/b"}: Permission denied'

                log.debug('################ TEST WITH CALLBACK ##################')
                def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                    assert str(path) in (str(content_path / 'readable/x/a'),
                                         str(content_path / 'unreadable/x/b'))
                    if str(path) == str(content_path / 'readable/x/a'):
                        assert type(piece_hash) is bytes and len(piece_hash) == 20
                        assert exc is None
                    elif str(path) == str(content_path / 'readable/x/a'):
                        assert str(exc) == f'{content_path / "unreadable/x/b"}: Permission denied'
                        assert piece_hash is None
                cb = mock.Mock(side_effect=assert_call)
                assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False

                # One call for each piece + 1 extra call for the ReadError
                exp_cb_calls = torrent.pieces + 1
                assert cb.call_count == exp_cb_calls
            finally:
                os.chmod(content_path / 'unreadable', mode=unreadable_path_mode)


def test_verify_content__torrent_contains_dir_and_path_is_file(forced_piece_size,
                                                               create_file, create_dir, create_torrent_file):
    with forced_piece_size(8) as piece_size:
        content_path = create_dir('content',
                                  ('a', create_dir.random_size()),
                                  ('b', create_dir.random_size()))
        with create_torrent_file(path=content_path) as torrent_file:
            torrent = torf.Torrent.read(torrent_file)

            shutil.rmtree(content_path)
            new_content_path = create_file('content', create_file.random_size())
            assert os.path.isfile(content_path)

            log.debug('################ TEST WITHOUT CALLBACK ##################')
            with pytest.raises(torf.VerifyIsDirectoryError) as excinfo:
                torrent.verify(content_path)
            assert str(excinfo.value) == f'{content_path}: Not a directory'

            log.debug('################ TEST WITH CALLBACK ##################')
            def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                assert t == torrent
                assert pieces_done == 0
                assert pieces_total == torrent.pieces
                assert piece_index == 0
                assert piece_hash is None
                assert str(path) == str(content_path / 'a')
                assert str(exc) == f'{content_path}: Not a directory'
            cb = mock.Mock(side_effect=assert_call)
            assert torrent.verify(content_path, callback=cb, interval=0) == False
            assert cb.call_count == 1

def test_verify_content__callback_is_called_at_intervals(forced_piece_size, monkeypatch,
                                                         create_file, create_torrent_file):
    with forced_piece_size(8) as piece_size:
        content_path = create_file('content',
                                   create_file.random_size(min_pieces=10, max_pieces=20))
        with create_torrent_file(path=content_path) as torrent_file:
            torrent = torf.Torrent.read(torrent_file)
            monkeypatch.setattr(torf._generate, 'time_monotonic',
                                mock.Mock(side_effect=range(int(1e9))))
            pieces_seen = []
            def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                pieces_seen.append(piece_index)
            cb = mock.Mock(side_effect=assert_call)
            assert torrent.verify(content_path, callback=cb, interval=2) == True
            assert cb.call_count == torrent.pieces // 2 + 1


def test_verify_content__last_callback_call_is_never_skipped_when_succeeding(forced_piece_size, monkeypatch,
                                                                             create_dir, create_torrent_file):
    with forced_piece_size(8) as piece_size:
        b_data = create_dir.random_bytes(create_dir.random_size(min_pieces=5))
        content_path = create_dir('content',
                                  ('a', create_dir.random_size()),
                                  ('b', b_data),
                                  ('c', create_dir.random_size()))
        with create_torrent_file(path=content_path) as torrent_file:
            torrent = torf.Torrent.read(torrent_file)

            monkeypatch.setattr(torf._generate, 'time_monotonic',
                                mock.Mock(side_effect=range(int(1e9))))

            progresses = []
            def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                progresses.append((pieces_done, pieces_total))
            cb = mock.Mock(side_effect=assert_call)
            assert torrent.verify(content_path, callback=cb, interval=2, skip_file_on_first_error=True) == True
            print(progresses)
            assert progresses[-1] == (torrent.pieces, torrent.pieces)


def test_verify_content__last_callback_call_is_never_skipped_when_failing(forced_piece_size, monkeypatch,
                                                                          create_dir, create_torrent_file):
    with forced_piece_size(8) as piece_size:
        b_data = create_dir.random_bytes(create_dir.random_size(min_pieces=5))
        content_path = create_dir('content',
                                  ('a', create_dir.random_size()),
                                  ('b', b_data),
                                  ('c', create_dir.random_size()))
        with create_torrent_file(path=content_path) as torrent_file:
            torrent = torf.Torrent.read(torrent_file)

            b_data_corrupt = bytearray(b_data)
            b_data_corrupt[piece_size:piece_size] = b'foo'
            assert b_data_corrupt != b_data
            (content_path / 'b').write_bytes(b_data_corrupt)

            monkeypatch.setattr(torf._generate, 'time_monotonic',
                                mock.Mock(side_effect=range(int(1e9))))

            progresses = []
            def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                print(path, pieces_done, pieces_total, piece_index, piece_hash, exc)
                progresses.append((pieces_done, pieces_total))
            cb = mock.Mock(side_effect=assert_call)
            assert torrent.verify(content_path, callback=cb, interval=2, skip_file_on_first_error=True) == False
            print(progresses)
            assert progresses[-1] == (torrent.pieces, torrent.pieces)


def test_verify_content__callback_interval_is_ignored_when_error_occurs(forced_piece_size, monkeypatch,
                                                                        create_file, create_torrent_file):
    with forced_piece_size(8) as piece_size:
        data = create_file.random_bytes(9*piece_size)
        content_path = create_file('content', data)
        with create_torrent_file(path=content_path) as torrent_file:
            torrent = torf.Torrent.read(torrent_file)

            # Corrupt consecutive pieces
            errpos = (4, 5, 6)
            data_corrupt = bytearray(data)
            data_corrupt[piece_size*errpos[0]] = (data[piece_size*errpos[0]] + 1) % 256
            data_corrupt[piece_size*errpos[1]] = (data[piece_size*errpos[0]] + 1) % 256
            data_corrupt[piece_size*errpos[2]] = (data[piece_size*errpos[2]] + 1) % 256
            assert len(data_corrupt) == len(data)
            assert data_corrupt != data
            content_path.write_bytes(data_corrupt)

            monkeypatch.setattr(torf._generate, 'time_monotonic',
                                mock.Mock(side_effect=range(int(1e9))))

            progresses = []
            def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                print(path, pieces_done, pieces_total, piece_index, piece_hash, exc)
                progresses.append((pieces_done, pieces_total))
            cb = mock.Mock(side_effect=assert_call)
            assert torrent.verify(content_path, callback=cb, interval=3, skip_file_on_first_error=False) == False
            assert progresses == [(1, 9), (4, 9), (5, 9), (6, 9), (7, 9), (9, 9)]


# def test_callback_raises_exception(forced_piece_size, monkeypatch,
#                                    create_file, create_torrent_file)
#     content = tmpdir.join('file.jpg')
#     content.write_binary(os.urandom(5*torf.Torrent.piece_size_min))

#     with create_torrent_file(path=content) as torrent_file:
#         torrent = torf.Torrent.read(torrent_file)

#         cb = mock.MagicMock()
#         def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
#             if cb.call_count == 3:
#                 raise RuntimeError("I'm off")
#         cb.side_effect = assert_call
#         with pytest.raises(RuntimeError) as excinfo:
#             torrent.verify(content, skip_file_on_first_error=False, callback=cb)
#         assert excinfo.match(f"^I'm off$")
#         assert cb.call_count == 3
