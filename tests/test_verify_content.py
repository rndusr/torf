import torf

import pytest
from unittest import mock
import os
import shutil


def test_validate_is_called_first(monkeypatch):
    torrent = torf.Torrent()
    mock_validate = mock.MagicMock(side_effect=torf.MetainfoError('Mock error'))
    monkeypatch.setattr(torrent, 'validate', mock_validate)
    with pytest.raises(torf.MetainfoError) as excinfo:
        torrent.verify('some/path')
    assert excinfo.match(f'^Invalid metainfo: Mock error$')
    mock_validate.assert_called_once_with()


def test_verify_content_successfully_with_singlefile_torrent(tmpdir, create_torrent):
    piece_size = torf.Torrent.piece_size_min
    content_path = tmpdir.join('some file.jpg')
    content_path.write_binary(os.urandom(3*piece_size+1000))

    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        # Without callback
        assert torrent.verify(content_path, skip_file_on_first_error=False) == True

        # With callback
        exp_piece_indexes = list(range(4))
        exp_call_count = len(exp_piece_indexes)
        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
            assert t == torrent
            assert str(path) == str(content_path)
            assert 0 <= pieces_done <= pieces_total
            assert pieces_total == torrent.pieces
            assert 0 <= piece_index <= pieces_total - 1
            assert type(piece_hash) is bytes and len(piece_hash) == 20
            assert exc is None
            exp_piece_indexes.remove(piece_index)
            return None
        cb.side_effect = assert_call
        assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == True
        assert cb.call_count == exp_call_count
        assert len(exp_piece_indexes) == 0, exp_piece_indexes


def test_verify_content_successfully_with_multifile_torrent(tmpdir, create_torrent):
    piece_size = torf.Torrent.piece_size_min
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file2 = content_path.join('file2.jpg')
    content_file3 = content_path.join('file3.jpg')
    content_file1.write_binary(os.urandom(2*piece_size+1000))
    content_file2.write_binary(os.urandom(3*piece_size+1000))
    content_file3.write_binary(os.urandom(4*piece_size+3000))

    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        # Without callback
        assert torrent.verify(content_path, skip_file_on_first_error=False) == True

        # With callback
        exp_piece_indexes = list(range(10))
        exp_call_count = len(exp_piece_indexes)
        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
            assert t == torrent
            assert 0 <= pieces_done <= pieces_total
            assert pieces_total == torrent.pieces
            assert type(piece_hash) is bytes and len(piece_hash) == 20
            if piece_index in (0, 1):
                assert str(path) == str(content_file1)
            elif piece_index in (2, 3, 4):
                assert str(path) == str(content_file2)
            elif piece_index in (5, 6, 7, 8, 9):
                assert str(path) == str(content_file3)
            assert exc is None
            exp_piece_indexes.remove(piece_index)
            return None
        cb.side_effect = assert_call
        assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == True
        assert cb.call_count == exp_call_count
        assert len(exp_piece_indexes) == 0, exp_piece_indexes


def test_verify_content__file_is_missing(tmpdir, create_torrent):
    piece_size = torf.Torrent.piece_size_min
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file2 = content_path.join('file2.jpg')
    content_file3 = content_path.join('file3.jpg')
    content_file1.write_binary(os.urandom(piece_size+100))
    data_file2 = os.urandom((piece_size*2)+200)
    content_file2.write_binary(data_file2)
    content_file3.write_binary(os.urandom(piece_size+300))

    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        corruption_offset = piece_size
        data_file2_corrupt = data_file2[:corruption_offset] + data_file2[corruption_offset+10:]
        assert len(data_file2_corrupt) == len(data_file2) - 10
        content_file2.write_binary(data_file2_corrupt)

        # Without callback
        with pytest.raises(torf.VerifyContentError) as excinfo:
            torrent.verify(content_path, skip_file_on_first_error=False)
        # All file2 pieces after index 3 are corrupt and it's possible that
        # latter pieces are processed first
        assert (str(excinfo.value) == (f'Corruption in piece 3 in {content_file2}') or
                str(excinfo.value) == (f'Corruption in piece 4, at least one of these files is corrupt: '
                                       f'{content_file2}, {content_file3}'))

        # With callback
        # 1+2+1 + 1 (for the 100+200+300 extra bytes) = 5 pieces (max_piece_index=4)
        exp_piece_indexes = [
            0,  # file1[0:16384]: ok
            1,  # file1[-100:] + file2[:16284]: ok
            2,  # file2[16284:16284+16384]: corrupt, bytes file2[piece_size:piece_size+10] are missing
            3,  # file2[-300:] + file3[:16084]: still corrupt because of file2, skipped
            4,  # file3[-600:]: ok
        ]
        exp_call_count = len(exp_piece_indexes)

        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
            assert t == torrent
            assert pieces_total == torrent.pieces
            assert 0 <= pieces_done <= pieces_total
            if piece_index == 0:
                assert str(path) == str(content_file1)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert exc is None
            elif piece_index == 1:
                assert str(path) == str(content_file2)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert exc is None
            elif piece_index == 2:
                assert str(path) == str(content_file2)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert str(exc) == f'Corruption in piece {piece_index+1} in {content_file2}'
            elif piece_index == 3:
                assert str(path) == str(content_file3)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert str(exc) == (f'Corruption in piece {piece_index+1}, '
                                    'at least one of these files is corrupt: '
                                    f'{content_file2}, {content_file3}')
            elif piece_index == 4:
                assert str(path) == str(content_file3)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert exc is None
            exp_piece_indexes.remove(piece_index)
            return None
        cb.side_effect = assert_call
        assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False
        assert cb.call_count == exp_call_count
        assert len(exp_piece_indexes) == 0, exp_piece_indexes


def test_verify_content__file_is_smaller(tmpdir, create_torrent):
    piece_size = torf.Torrent.piece_size_min
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file2 = content_path.join('file2.jpg')
    content_file3 = content_path.join('file3.jpg')
    content_file1.write_binary(os.urandom(piece_size+100))
    data_file2 = os.urandom((piece_size*2)+200)
    content_file2.write_binary(data_file2)
    content_file3.write_binary(os.urandom(piece_size+300))

    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        corruption_offset = piece_size
        data_file2_corrupt = data_file2[:corruption_offset] + data_file2[corruption_offset+10:]
        assert len(data_file2_corrupt) == len(data_file2) - 10
        content_file2.write_binary(data_file2_corrupt)

        # Without callback
        with pytest.raises(torf.VerifyContentError) as excinfo:
            torrent.verify(content_path, skip_file_on_first_error=False)
        # All file2 pieces after index 3 are corrupt and it's possible that
        # latter pieces are processed first
        assert (str(excinfo.value) == (f'Corruption in piece 3 in {content_file2}') or
                str(excinfo.value) == (f'Corruption in piece 4, at least one of these files is corrupt: '
                                       f'{content_file2}, {content_file3}'))

        # With callback
        # 1+2+1 + 1 (for the 100+200+300 extra bytes) = 5 pieces (max_piece_index=4)
        exp_piece_indexes = [
            0,  # file1[0:16384]: ok
            1,  # file1[-100:] + file2[:16284]: ok
            2,  # file2[16284:16284+16384]: corrupt, bytes file2[piece_size:piece_size+10] are missing
            3,  # file2[-300:] + file3[:16084]: still corrupt because of file2, skipped
            4,  # file3[-600:]: ok
        ]
        exp_call_count = len(exp_piece_indexes)

        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
            assert t == torrent
            assert pieces_total == torrent.pieces
            assert 0 <= pieces_done <= pieces_total
            if piece_index == 0:
                assert str(path) == str(content_file1)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert exc is None
            elif piece_index == 1:
                assert str(path) == str(content_file2)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert exc is None
            elif piece_index == 2:
                assert str(path) == str(content_file2)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert str(exc) == f'Corruption in piece {piece_index+1} in {content_file2}'
            elif piece_index == 3:
                assert str(path) == str(content_file3)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert str(exc) == (f'Corruption in piece {piece_index+1}, '
                                    'at least one of these files is corrupt: '
                                    f'{content_file2}, {content_file3}')
            elif piece_index == 4:
                assert str(path) == str(content_file3)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert exc is None
            exp_piece_indexes.remove(piece_index)
            return None
        cb.side_effect = assert_call
        assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False
        assert cb.call_count == exp_call_count
        assert len(exp_piece_indexes) == 0, exp_piece_indexes


def test_verify_content__file_is_bigger(tmpdir, create_torrent):
    piece_size = torf.Torrent.piece_size_min
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file2 = content_path.join('file2.jpg')
    content_file3 = content_path.join('file3.jpg')
    content_file1.write_binary(os.urandom(piece_size+100))
    data_file2 = os.urandom((piece_size*2)+200)
    content_file2.write_binary(data_file2)
    content_file3.write_binary(os.urandom(piece_size+300))

    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        corruption_offset = piece_size
        data_file2_corrupt = (data_file2[:corruption_offset] +
                              b'\x12'*10 +
                              data_file2[corruption_offset:])
        assert len(data_file2_corrupt) == len(data_file2) + 10
        content_file2.write_binary(data_file2_corrupt)

        # Without callback
        with pytest.raises(torf.TorfError) as excinfo:
            torrent.verify(content_path, skip_file_on_first_error=False)
        assert ((isinstance(excinfo.value, torf.VerifyFileSizeError) and
                 str(excinfo.value) == f'{content_file2}: Too big: 32978 instead of 32968 bytes') or
                (isinstance(excinfo.value, torf.VerifyContentError) and
                 str(excinfo.value) == f'Corruption in piece 3 in {content_file2}') or
                (isinstance(excinfo.value, torf.VerifyContentError) and
                 str(excinfo.value) == f'Corruption in piece 4 in {content_file3}'))

        # With callback
        # 1+2+1 + 1 (for the 100+200+300 extra bytes) = 5 pieces (max_piece_index=4)
        exp_piece_indexes = [
            0,  # file1[0:16384]: ok
            1,  # file1[-100:] + file2[:16284]: corrupt because file2 has wrong size
            2,  # file2[16284:16284+16384]: corrupt, additional bytes at file2[piece_size:piece_size+10]
            3,  # file2[-300:] + file3[:16084]: still corrupt because of file2
            4,  # file3[-600:]: ok
        ]
        exp_call_count = len(exp_piece_indexes)

        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
            assert t == torrent
            assert pieces_total == torrent.pieces
            assert 0 <= pieces_done <= pieces_total
            if piece_index == 0:
                assert str(path) == str(content_file1)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert exc is None
            elif piece_index == 1:
                assert str(path) == str(content_file2)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert str(exc) == f'{content_file2}: Too big: 32978 instead of 32968 bytes'
            elif piece_index == 2:
                assert str(path) == str(content_file2)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert str(exc) == f'Corruption in piece {piece_index+1} in {content_file2}'
            elif piece_index == 3:
                assert str(path) == str(content_file3)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert str(exc) == (f'Corruption in piece {piece_index+1}, '
                                    'at least one of these files is corrupt: '
                                    f'{content_file2}, {content_file3}')
            elif piece_index == 4:
                assert str(path) == str(content_file3)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert exc is None
            exp_piece_indexes.remove(piece_index)
            return None
        cb.side_effect = assert_call
        assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False
        assert cb.call_count == exp_call_count
        assert len(exp_piece_indexes) == 0, exp_piece_indexes


def test_verify_content__file_is_ok_but_has_extra_bytes_at_the_end(tmpdir, create_torrent):
    piece_size = torf.Torrent.piece_size_min
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file2 = content_path.join('file2.jpg')
    content_file3 = content_path.join('file3.jpg')
    content_file1.write_binary(os.urandom(piece_size+100))
    data_file2 = os.urandom((piece_size*2)+200)
    content_file2.write_binary(data_file2)
    content_file3.write_binary(os.urandom(piece_size+300))

    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        corruption_offset = piece_size
        data_file2_corrupt = data_file2 + os.urandom(10)
        assert len(data_file2_corrupt) == len(data_file2) + 10
        content_file2.write_binary(data_file2_corrupt)

        # Without callback
        with pytest.raises(torf.VerifyFileSizeError) as excinfo:
            torrent.verify(content_path, skip_file_on_first_error=False)
        assert str(excinfo.value) == f'{content_file2}: Too big: 32978 instead of 32968 bytes'

        # With callback
        # 1+2+1 + 1 (for the 100+200+300 extra bytes) = 5 pieces (max_piece_index=4)
        exp_piece_indexes = [
            0,  # file1[0:16384]: ok
            1,  # file1[-100:] + file2[:16284]: corrupt because file2 has wrong size
            2,  # file2[16284:16284+16384]: ok
            3,  # file2[-300:] + file3[:16084]: ok
            4,  # file3[-600:]: ok
        ]
        exp_call_count = len(exp_piece_indexes)
        exp_reported_errors = []
        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
            assert t == torrent
            assert pieces_total == torrent.pieces
            assert 0 <= pieces_done <= pieces_total
            if exc:
                exp_reported_errors.append(exc)
            if piece_index == 0:
                assert str(path) == str(content_file1)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert exc is None
            elif piece_index == 1:
                assert str(path) == str(content_file2)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert (str(exc) == f'{content_file2}: Too big: 32978 instead of 32968 bytes'
                        or exc is None)
            elif piece_index == 2:
                assert str(path) == str(content_file2)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert (str(exc) == f'{content_file2}: Too big: 32978 instead of 32968 bytes'
                        or exc is None)
            elif piece_index == 3:
                assert str(path) == str(content_file3)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert exc is None
            elif piece_index == 4:
                assert str(path) == str(content_file3)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert exc is None
            exp_piece_indexes.remove(piece_index)
            return None
        cb.side_effect = assert_call
        assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False
        assert cb.call_count == exp_call_count
        assert len(exp_piece_indexes) == 0, exp_piece_indexes
        assert len(exp_reported_errors) == 1
        assert str(exp_reported_errors[0]) == f'{content_file2}: Too big: 32978 instead of 32968 bytes'


def test_verify_content__file_is_same_size_but_corrupt(tmpdir, create_torrent):
    piece_size = torf.Torrent.piece_size_min
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file2 = content_path.join('file2.jpg')
    content_file3 = content_path.join('file3.jpg')
    content_file1.write_binary(os.urandom(piece_size+100))
    data_file2 = os.urandom((piece_size*2)+200)
    content_file2.write_binary(data_file2)
    content_file3.write_binary(os.urandom(piece_size+300))

    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        corruption_offset = piece_size
        data_file2_corrupt = (data_file2[:corruption_offset] +
                              b'\x12'*10 +
                              data_file2[corruption_offset+10:])
        assert len(data_file2_corrupt) == len(data_file2)
        content_file2.write_binary(data_file2_corrupt)

        # Without callback
        with pytest.raises(torf.VerifyContentError) as excinfo:
            torrent.verify(content_path, skip_file_on_first_error=False)
        assert excinfo.match(f'^Corruption in piece 3 in {content_file2}')

        # With callback
        # 1+2+1 + 1 (for the 100+200+300 extra bytes) = 5 pieces (max_piece_index=4)
        exp_piece_indexes = [
            0,  # file1[0:16384]: ok
            1,  # file1[-100:] + file2[:16284]: ok
            2,  # file2[16284:16284+16384]: corrupt
            3,  # file2[-300:] + file3[:16084]: ok
            4,  # file3[-600:]: ok
        ]
        exp_call_count = len(exp_piece_indexes)

        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
            assert t == torrent
            assert pieces_total == torrent.pieces
            assert 0 <= pieces_done <= pieces_total
            if piece_index == 0:
                assert str(path) == str(content_file1)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert exc is None
            elif piece_index == 1:
                assert str(path) == str(content_file2)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert exc is None
            elif piece_index == 2:
                assert str(path) == str(content_file2)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert str(exc) == f'Corruption in piece {piece_index+1} in {content_file2}'
            elif piece_index == 3:
                assert str(path) == str(content_file3)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert exc is None
            elif piece_index == 4:
                assert str(path) == str(content_file3)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert exc is None
            exp_piece_indexes.remove(piece_index)
            return None
        cb.side_effect = assert_call
        assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False
        assert cb.call_count == exp_call_count
        assert len(exp_piece_indexes) == 0, exp_piece_indexes


def test_verify_content__skip_file_on_first_error(tmpdir, create_torrent):
    piece_size = torf.Torrent.piece_size_min
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file2 = content_path.join('file2.jpg')
    content_file3 = content_path.join('file3.jpg')
    content_file1.write_binary(os.urandom((piece_size*3)+100))
    data_file2 = os.urandom((piece_size*10)+200)
    content_file2.write_binary(data_file2)
    content_file3.write_binary(os.urandom((piece_size*2)+300))

    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        # Corrupt multiple pieces
        data_file2_corrupt = bytearray(data_file2)
        for pos in (4*piece_size+1000, 8*piece_size+1000):
            data_file2_corrupt[pos] = (data_file2_corrupt[pos] + 1) % 256
        assert data_file2_corrupt != data_file2
        content_file2.write_binary(data_file2_corrupt)

        reported_goodies = set()
        reported_badies = set()

        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
            assert t == torrent
            assert pieces_total == torrent.pieces
            assert 0 <= pieces_done <= pieces_total
            assert type(piece_hash) is bytes and len(piece_hash) == 20
            if exc is None:
                assert piece_hash
                reported_goodies.add((piece_index, path))
            else:
                reported_badies.add((piece_index, path, str(exc)))
            return None
        cb.side_effect = assert_call
        assert torrent.verify(content_path, skip_file_on_first_error=True, callback=cb, interval=0) == False

        # Exactly one of the two corruptions in file2 must have been reported,
        # but we can't predict which one was found first.
        assert any(badie in reported_badies
                   for badie in [(7, content_file2, f'Corruption in piece 8 in {content_file2}'),
                                 (11, content_file2, f'Corruption in piece 12 in {content_file2}')])
        assert len(reported_badies) == 1

        # All pieces of non-corrupt file1 and file3 must have been reported, and
        # we expect at least one reported good piece of file2 before a corrupt
        # piece is found.
        assert (0, content_file1) in reported_goodies
        assert (1, content_file1) in reported_goodies
        assert (2, content_file1) in reported_goodies
        assert any(goodie in reported_goodies
                   for goodie in [(3, content_file2),
                                  (4, content_file2),
                                  (5, content_file2),
                                  (6, content_file2),
                                  (8, content_file2),
                                  (9, content_file2),
                                  (10, content_file2),
                                  (12, content_file2)])
        assert (7, content_file2) not in reported_goodies
        assert (11, content_file2) not in reported_goodies
        assert (13, content_file3) in reported_goodies
        assert (14, content_file3) in reported_goodies
        assert (15, content_file3) in reported_goodies


def test_verify_content__skip_file_on_first_error_with_corrupt_piece_overlapping_multiple_files(tmpdir, create_torrent):
    piece_size = torf.Torrent.piece_size_min
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file2 = content_path.join('file2.jpg')
    content_file3 = content_path.join('file3.jpg')
    content_file1.write_binary(os.urandom((piece_size*3)+100))
    data_file2 = os.urandom((piece_size*10)+200)
    content_file2.write_binary(data_file2)
    content_file3.write_binary(os.urandom((piece_size*2)+300))

    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        # Corrupt multiple pieces
        data_file2_corrupt = bytearray(data_file2)
        for pos in (4*piece_size+1000, 8*piece_size+1000, -100):
            data_file2_corrupt[pos] = (data_file2_corrupt[pos] + 1) % 256
        assert data_file2_corrupt != data_file2
        content_file2.write_binary(data_file2_corrupt)

        reported_goodies = set()
        reported_badies = set()

        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
            assert t == torrent
            assert pieces_total == torrent.pieces
            assert 0 <= pieces_done <= pieces_total
            assert type(piece_hash) is bytes and len(piece_hash) == 20
            if exc is None:
                assert piece_hash
                reported_goodies.add((piece_index, path))
            else:
                reported_badies.add((piece_index, path, str(exc)))
            return None
        cb.side_effect = assert_call
        assert torrent.verify(content_path, skip_file_on_first_error=True, callback=cb, interval=0) == False

        # The first corruption (index 7) is reported, the second corruption
        # (index 11) is ignored, but the third corruption (index 13) is reported
        # again, because it also corrupts file3.
        assert reported_badies == set(((7, content_file2, f'Corruption in piece 8 in {content_file2}'),
                                       (13, content_file3, (f'Corruption in piece 14, at least one of these files is corrupt: '
                                                            f'{content_file2}, {content_file3}'))))

        # All pieces of non-corrupt file1 must have been reported, and we expect
        # at least one reported good piece of file2 before a corrupt piece is
        # found.  file3 is also skipped because its first piece is corrupt.
        assert (0, content_file1) in reported_goodies
        assert (1, content_file1) in reported_goodies
        assert (2, content_file1) in reported_goodies
        assert any(goodie in reported_goodies
                   for goodie in [(3, content_file2),
                                  (4, content_file2),
                                  (5, content_file2),
                                  (6, content_file2),
                                  (8, content_file2),
                                  (9, content_file2),
                                  (10, content_file2),
                                  (12, content_file2)])
        assert (7, content_file2) not in reported_goodies
        assert (11, content_file2) not in reported_goodies
        assert (13, content_file2) not in reported_goodies
        # Not checking for 14 and 15 because they may be in reported_goodies if
        # they were checked before 13, but there is no way to tell.


def test_verify_content__path_is_directory_and_torrent_contains_single_file(tmpdir, create_torrent):
    content_path = tmpdir.join('content')
    content_path.write('some data')
    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        os.remove(content_path)
        content_path = tmpdir.mkdir('content')
        content_file = content_path.join('file.jpg')
        content_file.write('some data')
        assert os.path.isdir(content_path)

        # Without callback
        with pytest.raises(torf.VerifyNotDirectoryError) as excinfo:
            torrent.verify(content_path)
        assert excinfo.match(f'^{content_path}: Is a directory$')

        # With callback
        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
            assert t == torrent
            assert pieces_done == 0
            assert pieces_total == torrent.pieces
            assert piece_index == 0
            assert piece_hash is None
            assert str(path) == str(content_path)
            assert str(exc) == f'{content_path}: Is a directory'
            return None
        cb.side_effect = assert_call
        assert torrent.verify(content_path, callback=cb, interval=0) == False
        assert cb.call_count == 1


def test_verify_content__path_is_single_file_and_torrent_contains_directory(tmpdir, create_torrent):
    content_path = tmpdir.mkdir('content')
    content_file = content_path.join('file.jpg')
    content_file.write('some data')
    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        shutil.rmtree(content_path)
        content_path = tmpdir.join('content')
        content_path.write('some data')
        assert os.path.isfile(content_path)

        # Without callback
        with pytest.raises(torf.VerifyIsDirectoryError) as excinfo:
            torrent.verify(content_path)
        assert excinfo.match(f'^{content_path}: Not a directory$')

        # With callback
        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
            assert t == torrent
            assert pieces_done == 0
            assert pieces_total == torrent.pieces
            assert piece_index == 0
            assert piece_hash is None
            assert str(path) == str(content_file)
            assert str(exc) == f'{content_path}: Not a directory'
            return None
        cb.side_effect = assert_call
        assert torrent.verify(content_path, callback=cb, interval=0) == False
        assert cb.call_count == 1


def test_verify_content__parent_path_is_unreadable(tmpdir, create_torrent):
    content_path = tmpdir.mkdir('content')
    unreadable_path1 = content_path.mkdir('unreadable1').mkdir('b').mkdir('c')
    unreadable_path2 = content_path.mkdir('unreadable2').mkdir('b').mkdir('c')
    readable_path = content_path.mkdir('readable').mkdir('b').mkdir('c')
    content_file1 = unreadable_path1.join('file1.jpg')
    content_file2 = unreadable_path2.join('file2.jpg')
    content_file3 = readable_path.join('file3.jpg')
    content_file1.write('some data')
    content_file2.write('some more data')
    content_file3.write('some other data')
    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        unreadable_path1_mode = os.stat(unreadable_path1).st_mode
        unreadable_path2_mode = os.stat(unreadable_path2).st_mode
        try:
            os.chmod(unreadable_path1, mode=0o222)
            os.chmod(unreadable_path2, mode=0o222)

            # Without callback
            with pytest.raises(torf.ReadError) as excinfo:
                torrent.verify(content_path)
            assert excinfo.match(f'^{content_file1}: Permission denied$')

            # With callback
            exp_paths = [str(content_file1),  # file1: unreadable
                         str(content_file2),  # file2: unreadable
                         str(content_file2)]  # file2: file2 corrupted exception
                                              # no call for file3 because there is only 1 piece
            exp_file2_exceptions = [f'{str(content_file2)}: Permission denied',
                                    ('Corruption in piece 1, at least one of these files is corrupt: '
                                     f'{content_file3}, {content_file1}, {content_file2}')]
            cb = mock.MagicMock()
            def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                assert t == torrent
                assert pieces_total == torrent.pieces
                assert str(path) in (str(content_file1), str(content_file2))
                if str(path) == str(content_file1):
                    assert pieces_done == 0
                    assert piece_hash is None
                    assert str(exc) == f'{content_file1}: Permission denied'
                elif str(path) == str(content_file2):
                    exp_exc = exp_file2_exceptions.pop(0)
                    assert str(exc) == exp_exc
                    if 'Permission' in exp_exc:
                        assert pieces_done == 0
                        assert piece_hash is None
                    else:
                        assert pieces_done == 1
                        assert type(piece_hash) is bytes and len(piece_hash) == 20
                exp_paths.remove(str(path))
                return None
            cb.side_effect = assert_call
            assert torrent.verify(content_path, callback=cb, interval=0) == False
            assert cb.call_count == 3
            assert len(exp_paths) == 0
        finally:
            os.chmod(unreadable_path1, mode=unreadable_path1_mode)
            os.chmod(unreadable_path2, mode=unreadable_path2_mode)


def test_verify_content__corruption_in_singlefile_torrent(tmpdir, create_torrent):
    piece_size = torf.Torrent.piece_size_min
    content_path = tmpdir.join('content.jpg')
    content_data = os.urandom(int(piece_size * 3.12345))
    content_path.write_binary(content_data)
    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)
        for offset in (0, piece_size, piece_size*2, piece_size*3):
            for error_pos in (offset, offset + int(piece_size/3), offset+piece_size-1):
                error_pos = min(error_pos, len(content_data)-1)
                corrupt_content_data = bytearray(content_data)
                corrupt_content_data[error_pos] = (content_data[error_pos] + 1) % 256
                assert len(corrupt_content_data) == len(content_data)
                assert corrupt_content_data != content_data
                content_path.write_binary(corrupt_content_data)

                corrupt_piece_index = int(offset / piece_size)

                # Without callback
                with pytest.raises(torf.VerifyContentError) as excinfo:
                    torrent.verify(content_path, skip_file_on_first_error=False)
                assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1}$')

                # With callback
                cb = mock.MagicMock(return_value=None)
                def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                    assert t == torrent
                    assert pieces_total == torrent.pieces
                    assert str(path) == str(content_path)
                    assert 1 <= pieces_done <= pieces_total
                    assert 0 <= piece_index <= pieces_total - 1
                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    if piece_index == 0 and corrupt_piece_index == 0:
                        assert str(exc) == f'Corruption in piece {piece_index+1}'
                    elif piece_index == 1 and corrupt_piece_index == 1:
                        assert str(exc) == f'Corruption in piece {piece_index+1}'
                    elif piece_index == 2 and corrupt_piece_index == 2:
                        assert str(exc) == f'Corruption in piece {piece_index+1}'
                    elif piece_index == 3 and corrupt_piece_index == 3:
                        assert str(exc) == f'Corruption in piece {piece_index+1}'
                    else:
                        assert exc is None
                    return None
                cb.side_effect = assert_call
                assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False
                assert cb.call_count == 4


def test_verify_content__corruption_in_multifile_torrent_and_pieces_aligning_to_files(tmpdir, create_torrent):
    piece_size = torf.Torrent.piece_size_min
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file2 = content_path.join('file2.jpg')
    content_file3 = content_path.join('file3.jpg')
    content_files = [str(content_file1), str(content_file2), str(content_file3)]
    content_data1 = os.urandom(piece_size)
    content_data2 = os.urandom(piece_size * 2)
    content_data3 = os.urandom(piece_size * 3)
    content_file1.write_binary(content_data1)
    content_file2.write_binary(content_data2)
    content_file3.write_binary(content_data3)

    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)
        for file,data,offset in ((content_file1, content_data1, 0),
                                 (content_file2, content_data2, len(content_data1)),
                                 (content_file3, content_data3, len(content_data1) + len(content_data2))):
            for error_pos in (0, int(len(data)/2), int(len(data)/2)+1, len(data)-1):
                error_pos_abs = offset + error_pos
                corrupt_data = bytearray(data)
                corrupt_data[error_pos] = (data[error_pos] + 1) % 256
                assert len(corrupt_data) == len(data)
                assert corrupt_data != data
                file.write_binary(corrupt_data)

                corrupt_piece_index = error_pos_abs // piece_size
                assert 0 <= corrupt_piece_index <= 6

                # Without callback
                with pytest.raises(torf.VerifyContentError) as excinfo:
                    torrent.verify(content_path, skip_file_on_first_error=False)
                assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1} in {file}$')

                # With callback
                cb = mock.MagicMock(return_value=None)
                def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                    assert t == torrent
                    assert pieces_total == torrent.pieces
                    assert 1 <= pieces_done <= 6

                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert 0 <= piece_index <= 5
                    if piece_index == 0:
                        assert str(path) == str(content_file1)
                    elif 1 <= piece_index <= 2:
                        assert str(path) == str(content_file2)
                    elif 3 <= piece_index <= 5:
                        assert str(path) == str(content_file3)

                    if piece_index == 0 and corrupt_piece_index == 0:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {content_file1}'
                    elif piece_index == 1 and corrupt_piece_index == 1:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {content_file2}'
                    elif piece_index == 2 and corrupt_piece_index == 2:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {content_file2}'
                    elif piece_index == 3 and corrupt_piece_index == 3:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {content_file3}'
                    elif piece_index == 4 and corrupt_piece_index == 4:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {content_file3}'
                    elif piece_index == 5 and corrupt_piece_index == 5:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {content_file3}'
                    else:
                        assert exc is None

                    return None
                cb.side_effect = assert_call
                assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False
                assert cb.call_count == 6

                # Restore original data so it we don't get the same error in the
                # next iteration
                file.write_binary(data)


def test_verify_content__corruption_in_multifile_torrent_and_pieces_not_aligning_to_files(tmpdir, create_torrent):
    piece_size = torf.Torrent.piece_size_min
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file2 = content_path.join('file2.jpg')
    content_file3 = content_path.join('file3.jpg')
    content_files = [str(content_file1), str(content_file2), str(content_file3)]
    content_data1 = os.urandom(int(piece_size * 1.5))
    content_data2 = os.urandom(int(piece_size * 2) + 1)
    content_data3 = os.urandom(int(piece_size * 3) - 1)
    content_file1.write_binary(content_data1)
    content_file2.write_binary(content_data2)
    content_file3.write_binary(content_data3)

    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)
        for file,data,offset in ((content_file1, content_data1, 0),
                                 (content_file2, content_data2, len(content_data1)),
                                 (content_file3, content_data3, len(content_data1) + len(content_data2))):
            for error_pos in (0, int(len(data)/2), int(len(data)/2)+1, len(data)-1):
                error_pos_abs = offset + error_pos
                corrupt_data = bytearray(data)
                corrupt_data[error_pos] = (data[error_pos] + 1) % 256
                assert len(corrupt_data) == len(data)
                assert corrupt_data != data
                file.write_binary(corrupt_data)

                corrupt_piece_index = error_pos_abs // piece_size
                assert 0 <= corrupt_piece_index <= 7

                # Without callback
                with pytest.raises(torf.VerifyContentError) as excinfo:
                    torrent.verify(content_path, skip_file_on_first_error=False)
                if corrupt_piece_index == 0:
                    assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1} in {content_file1}$')
                elif corrupt_piece_index == 1:
                    assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1}, '
                                         f'at least one of these files is corrupt: '
                                         f'{content_file1}, {content_file2}$')
                elif corrupt_piece_index == 2:
                    assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1} in {content_file2}$')
                elif corrupt_piece_index == 3:
                    assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1}, '
                                         f'at least one of these files is corrupt: '
                                         f'{content_file2}, {content_file3}$')
                elif corrupt_piece_index == 4:
                    assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1}, '
                                         f'at least one of these files is corrupt: '
                                         f'{content_file2}, {content_file3}$')
                else:
                    assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1} in {content_file3}$')

                # With callback
                cb = mock.MagicMock()
                def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                    assert t == torrent
                    assert pieces_total == torrent.pieces
                    assert 1 <= pieces_done <= 7

                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert 0 <= piece_index <= 6
                    if piece_index == 0:
                        assert str(path) == str(content_file1)
                    elif piece_index == 1:
                        assert str(path) == str(content_file2)
                    elif piece_index == 2:
                        assert str(path) == str(content_file2)
                    else:
                        assert str(path) == str(content_file3)

                    if piece_index == 0 and corrupt_piece_index == 0:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {content_file1}'
                    elif piece_index == 1 and corrupt_piece_index == 1:
                        assert str(exc) == (f'Corruption in piece {piece_index+1}, '
                                            f'at least one of these files is corrupt: '
                                            f'{content_file1}, {content_file2}')
                    elif piece_index == 2 and corrupt_piece_index == 2:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {content_file2}'
                    elif piece_index == 3 and corrupt_piece_index == 3:
                        assert str(exc) == (f'Corruption in piece {piece_index+1}, '
                                            f'at least one of these files is corrupt: '
                                            f'{content_file2}, {content_file3}')
                    elif piece_index == 5 and corrupt_piece_index == 5:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {content_file3}'
                    elif piece_index == 6 and corrupt_piece_index == 6:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {content_file3}'
                    else:
                        assert exc is None

                    return None
                cb.side_effect = assert_call
                assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False
                assert cb.call_count == 7

                # Restore original data so it we don't get the same error in the
                # next iteration
                file.write_binary(data)


def test_verify_content_corruption_in_multifile_torrent_and_one_piece_covering_multiple_files(tmpdir, create_torrent):
    piece_size = torf.Torrent.piece_size_min
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file2 = content_path.join('file2.jpg')
    content_file3 = content_path.join('file3.jpg')
    content_file4 = content_path.join('file4.jpg')
    content_files = [str(content_file1), str(content_file2), str(content_file3)]
    content_data1 = os.urandom(16000)
    content_data2 = os.urandom(2000)
    content_data3 = os.urandom(3000)
    content_data4 = os.urandom(30000)
    assert len(content_data2) + len(content_data3) < piece_size
    content_file1.write_binary(content_data1)
    content_file2.write_binary(content_data2)
    content_file3.write_binary(content_data3)
    content_file4.write_binary(content_data4)
    content_offset1 = 0
    content_offset2 = len(content_data1)
    content_offset3 = len(content_data1) + len(content_data2)
    content_offset4 = len(content_data1) + len(content_data2) + len(content_data3)

    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)
        assert torrent.pieces == 4
        for file,data,offset in ((content_file1, content_data1, content_offset1),
                                 (content_file2, content_data2, content_offset2),
                                 (content_file3, content_data3, content_offset3),
                                 (content_file4, content_data4, content_offset4)):
            for error_pos in (0, int(len(data)/2), int(len(data)/2)+1, len(data)-1):
                error_pos_abs = offset + error_pos
                corrupt_data = bytearray(data)
                corrupt_data[error_pos] = (data[error_pos] + 1) % 256
                assert len(corrupt_data) == len(data)
                assert corrupt_data != data
                file.write_binary(corrupt_data)

                corrupt_piece_index = error_pos_abs // piece_size
                assert 0 <= corrupt_piece_index <= 3

                # Without callback
                with pytest.raises(torf.VerifyContentError) as excinfo:
                    torrent.verify(content_path, skip_file_on_first_error=False)
                if corrupt_piece_index == 0:
                    assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1}, '
                                         'at least one of these files is corrupt: '
                                         f'{content_file1}, {content_file2}$')
                elif corrupt_piece_index == 1:
                    assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1}, '
                                         'at least one of these files is corrupt: '
                                         f'{content_file2}, {content_file3}, {content_file4}$')
                elif corrupt_piece_index == 2:
                    assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1} in {content_file4}$')
                else:
                    assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1} in {content_file4}$')

                # With callback
                cb = mock.MagicMock()
                def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                    assert t == torrent
                    assert pieces_total == torrent.pieces
                    assert pieces_done == cb.call_count

                    assert type(piece_hash) is bytes and len(piece_hash) == 20
                    assert 0 <= piece_index <= pieces_total
                    if piece_index == 0:
                        assert str(path) == str(content_file2)
                    elif piece_index == 1:
                        assert str(path) == str(content_file4)
                    elif piece_index == 2:
                        assert str(path) == str(content_file4)
                    else:
                        assert str(path) == str(content_file4)

                    if piece_index == 0 and corrupt_piece_index == 0:
                        assert str(exc) == (f'Corruption in piece {piece_index+1}, '
                                            'at least one of these files is corrupt: '
                                            f'{content_file1}, {content_file2}')
                    elif piece_index == 1 and corrupt_piece_index == 1:
                        assert str(exc) == (f'Corruption in piece {piece_index+1}, '
                                            'at least one of these files is corrupt: '
                                            f'{content_file2}, {content_file3}, {content_file4}')
                    elif piece_index == 2 and corrupt_piece_index == 2:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {content_file4}'
                    elif piece_index == 3 and corrupt_piece_index == 3:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {content_file4}'
                    else:
                        assert exc is None

                    return None
                cb.side_effect = assert_call
                assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False
                assert cb.call_count == 4

                # Restore original data so it we don't get the same error in the
                # next iteration
                file.write_binary(data)


def test_verify_content__callback_is_called_at_intervals(tmpdir, create_torrent, monkeypatch):
    content_file = tmpdir.join('content.jpg')
    content_file.write_binary(os.urandom(torf.Torrent.piece_size_min * 20))
    with create_torrent(path=content_file) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        import time
        monkeypatch.setattr(time, 'monotonic',
                            mock.MagicMock(side_effect=range(1, 100)))

        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
            assert t == torrent
            assert str(path) == str(content_file)
            assert 1 <= pieces_done <= pieces_total
            assert pieces_total == torrent.pieces
            assert 0 <= piece_index <= pieces_total - 1
            assert type(piece_hash) is bytes and len(piece_hash) == 20
            assert exc is None
            return None
        cb.side_effect = assert_call
        assert torrent.verify(content_file, skip_file_on_first_error=False, callback=cb, interval=2) == True
        assert cb.call_count == (torrent.pieces / 2) + 1


def test_verify_content__callback_interval_is_ignored_with_exception(tmpdir, create_torrent, monkeypatch):
    piece_size = torf.Torrent.piece_size_min
    content_file = tmpdir.join('content.jpg')
    content_data = os.urandom(piece_size * 30)
    content_file.write_binary(content_data)
    with create_torrent(path=content_file) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        import time
        monkeypatch.setattr(time, 'monotonic',
                            mock.MagicMock(side_effect=range(1, 100)))

        corrupt_data = bytearray(content_data)
        errpos = (6, 7, 24)  # Positions of corrupt bytes
        corrupt_data[piece_size*errpos[0]] = (content_data[piece_size*errpos[0]] + 1) % 256
        corrupt_data[piece_size*errpos[1]] = (content_data[piece_size*errpos[0]] + 1) % 256
        corrupt_data[piece_size*errpos[2]] = (content_data[piece_size*errpos[2]] + 1) % 256
        assert len(corrupt_data) == len(content_data)
        assert corrupt_data != content_data
        content_file.write_binary(corrupt_data)

        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
            assert t == torrent
            assert str(path) == str(content_file)
            assert 1 <= pieces_done <= pieces_total
            assert pieces_total == torrent.pieces
            assert 0 <= piece_index <= pieces_total - 1
            assert type(piece_hash) is bytes and len(piece_hash) == 20
            if piece_index in errpos:
                assert exc is not None
            else:
                assert exc is None
            return None
        cb.side_effect = assert_call
        assert torrent.verify(content_file, skip_file_on_first_error=False, callback=cb, interval=3) == False
        assert 11 <= cb.call_count <= 13


def test_callback_raises_exception(tmpdir, create_torrent, monkeypatch):
    content = tmpdir.join('file.jpg')
    content.write_binary(os.urandom(5*torf.Torrent.piece_size_min))

    with create_torrent(path=content) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
            if cb.call_count == 3:
                raise RuntimeError("I'm off")
            return None
        cb.side_effect = assert_call
        with pytest.raises(RuntimeError) as excinfo:
            torrent.verify(content, skip_file_on_first_error=False, callback=cb)
        assert excinfo.match(f"^I'm off$")
        assert cb.call_count == 3
