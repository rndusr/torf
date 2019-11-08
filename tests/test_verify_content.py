import torf

import pytest
from unittest import mock
import os


def test_validate_is_called_first(monkeypatch):
    torrent = torf.Torrent()
    mock_validate = mock.MagicMock(side_effect=torf.MetainfoError('Mock error'))
    monkeypatch.setattr(torrent, 'validate', mock_validate)
    with pytest.raises(torf.MetainfoError) as excinfo:
        torrent.verify('some/path')
    assert excinfo.match(f'^Invalid metainfo: Mock error$')
    mock_validate.assert_called_once_with()


def test_file_in_singlefile_torrent_doesnt_exist(tmpdir, create_torrent):
    content_path = tmpdir.join('content.jpg')
    content_path.write('some data')
    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        # Without callback
        with pytest.raises(torf.ReadError) as excinfo:
            torrent.verify('nonexisting/path')
        assert excinfo.match(f'^nonexisting/path: No such file or directory$')

        # With callback
        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
            assert t == torrent
            assert str(path) == 'nonexisting/path'
            assert pieces_done == 0
            assert pieces_total == torrent.pieces
            assert piece_index == 0
            assert piece_hash is None
            assert str(exc) == 'nonexisting/path: No such file or directory'
            return None
        cb.side_effect = assert_call
        assert torrent.verify('nonexisting/path', callback=cb, interval=0) == False
        assert cb.call_count == 1


def test_file_in_multifile_torrent_doesnt_exist(tmpdir, create_torrent):
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file2 = content_path.join('file2.jpg')
    content_file3 = content_path.join('file3.jpg')
    content_file1.write_binary(os.urandom(torf.Torrent.piece_size_min*2))
    content_file2.write_binary(os.urandom(torf.Torrent.piece_size_min*3))
    content_file3.write_binary(os.urandom(torf.Torrent.piece_size_min*4))

    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)
        print(len(torrent.metainfo['info']['pieces']))

        os.remove(content_file1)
        os.remove(content_file3)

        # Without callback
        with pytest.raises(torf.ReadError) as excinfo:
            torrent.verify(content_path)
        assert excinfo.match(f'^{content_file1}: No such file or directory$')

        # With callback
        exp_piece_indexes = [0,        # file1: one call for the "no such file" error
                             2, 3, 4,  # file2: one call per piece
                             5]        # file3: one call for the "no such file" error
        cb = mock.MagicMock()
        import logging
        logging.debug('#'*50)
        import threading
        lock = threading.Lock()
        def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
            with lock:
                logging.debug(f'CALL: {path}, {pieces_done}, {pieces_total}, {piece_index}, {piece_hash}, {exc}')
            assert t == torrent
            assert pieces_total == torrent.pieces
            assert 0 <= pieces_done <= pieces_total
            if piece_index == 0:
                assert str(path) == str(content_file1)
                assert piece_hash is None
                assert str(exc) == f'{content_file1}: No such file or directory'
            elif piece_index in (2, 3, 4):
                assert str(path) == str(content_file2)
                assert type(piece_hash) is bytes and len(piece_hash) == 20
                assert exc is None
            elif piece_index == 5:
                assert str(path) == str(content_file3)
                assert piece_hash is None
                assert str(exc) == f'{content_file3}: No such file or directory'
            logging.debug(f'removing {piece_index} from {exp_piece_indexes}')
            exp_piece_indexes.remove(piece_index)
            return None
        cb.side_effect = assert_call
        assert torrent.verify(content_path, callback=cb, interval=0) == False
        #   9 pieces/calls total
        # - 2 pieces in file1 - 4 pieces in file3
        # + 1 error for file1 + 1 error for file3
        assert cb.call_count == 5
        assert len(exp_piece_indexes) == 0


def test_path_is_directory_and_torrent_contains_single_file(tmpdir, create_torrent):
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
        with pytest.raises(torf.ReadError) as excinfo:
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


def test_parent_path_is_unreadable(tmpdir, create_torrent):
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

            import logging
            logging.debug('#'*50)

            import threading
            lock = threading.Lock()

            # With callback
            exp_paths = [str(content_file1),  # file1: unreadable exception
                         str(content_file2),  # file2: unreadable exception
                         str(content_file2)]  # file2: file2 corrupted exception
                                              # no call for file3 because there is only 1 piece
            exp_file2_exceptions = [f'{str(content_file2)}: Permission denied',
                                    ('Corruption in piece 1, at least one of these files is corrupt: '
                                     'content/readable/b/c/file3.jpg, '
                                     'content/unreadable1/b/c/file1.jpg, '
                                     'content/unreadable2/b/c/file2.jpg')]
            cb = mock.MagicMock()
            def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
                with lock:
                    logging.debug(f'CALL: {path}, {pieces_done}, {pieces_total}, {piece_index}, {piece_hash}, {exc}')
                assert t == torrent
                assert pieces_total == torrent.pieces
                assert piece_index == 0
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

                logging.debug(f'removing {str(path)} from {exp_paths}')
                exp_paths.remove(str(path))
                return None
            cb.side_effect = assert_call
            assert torrent.verify(content_path, callback=cb, interval=0) == False
            assert cb.call_count == 3
            assert len(exp_paths) == 0


        finally:
            os.chmod(unreadable_path1, mode=unreadable_path1_mode)
            os.chmod(unreadable_path2, mode=unreadable_path2_mode)


def test_hash_check_with_singlefile_torrent(tmpdir, create_torrent):
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
                with pytest.raises(torf.ContentError) as excinfo:
                    torrent.verify(content_path)
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
                assert torrent.verify(content_path, callback=cb, interval=0) == False
                assert cb.call_count == 4


def test_hash_check_with_multifile_torrent_and_pieces_aligning_to_files(tmpdir, create_torrent):
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

    def real_path(path):
        return os.sep.join(str(path).split(os.sep)[-2:])

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
                with pytest.raises(torf.ContentError) as excinfo:
                    torrent.verify(content_path)
                assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1} in {real_path(file)}$')

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
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {real_path(content_file1)}'
                    elif piece_index == 1 and corrupt_piece_index == 1:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {real_path(content_file2)}'
                    elif piece_index == 2 and corrupt_piece_index == 2:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {real_path(content_file2)}'
                    elif piece_index == 3 and corrupt_piece_index == 3:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {real_path(content_file3)}'
                    elif piece_index == 4 and corrupt_piece_index == 4:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {real_path(content_file3)}'
                    elif piece_index == 5 and corrupt_piece_index == 5:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {real_path(content_file3)}'
                    else:
                        assert exc is None

                    return None
                cb.side_effect = assert_call
                assert torrent.verify(content_path, callback=cb, interval=0) == False
                assert cb.call_count == 6

                # Restore original data so it we don't get the same error in the
                # next iteration
                file.write_binary(data)


def test_hash_check_with_multifile_torrent_and_pieces_not_aligning_to_files(tmpdir, create_torrent):
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

    def real_path(path):
        return os.sep.join(str(path).split(os.sep)[-2:])

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
                with pytest.raises(torf.ContentError) as excinfo:
                    torrent.verify(content_path)
                if corrupt_piece_index == 0:
                    assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1} in {real_path(content_file1)}$')
                elif corrupt_piece_index == 1:
                    assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1}, '
                                         f'at least one of these files is corrupt: '
                                         f'{real_path(content_file1)}, {real_path(content_file2)}$')
                elif corrupt_piece_index == 2:
                    assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1} in {real_path(content_file2)}$')
                elif corrupt_piece_index == 3:
                    assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1}, '
                                         f'at least one of these files is corrupt: '
                                         f'{real_path(content_file2)}, {real_path(content_file3)}$')
                elif corrupt_piece_index == 4:
                    assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1}, '
                                         f'at least one of these files is corrupt: '
                                         f'{real_path(content_file2)}, {real_path(content_file3)}$')
                else:
                    assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1} in {real_path(content_file3)}$')

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
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {real_path(content_file1)}'
                    elif piece_index == 1 and corrupt_piece_index == 1:
                        assert str(exc) == (f'Corruption in piece {piece_index+1}, '
                                            f'at least one of these files is corrupt: '
                                            f'{real_path(content_file1)}, {real_path(content_file2)}')
                    elif piece_index == 2 and corrupt_piece_index == 2:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {real_path(content_file2)}'
                    elif piece_index == 3 and corrupt_piece_index == 3:
                        assert str(exc) == (f'Corruption in piece {piece_index+1}, '
                                            f'at least one of these files is corrupt: '
                                            f'{real_path(content_file2)}, {real_path(content_file3)}')
                    # NOTE: Piece index 4 is never corrupted because file3.jpg is so big.
                    elif piece_index == 5 and corrupt_piece_index == 5:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {real_path(content_file3)}'
                    elif piece_index == 6 and corrupt_piece_index == 6:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {real_path(content_file3)}'
                    else:
                        assert exc is None

                    return None
                cb.side_effect = assert_call
                assert torrent.verify(content_path, callback=cb, interval=0) == False
                assert cb.call_count == 7

                # Restore original data so it we don't get the same error in the
                # next iteration
                file.write_binary(data)


def test_hash_check_with_multifile_torrent_and_one_piece_covering_multiple_files(tmpdir, create_torrent):
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

    def real_path(path):
        return os.sep.join(str(path).split(os.sep)[-2:])

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
                with pytest.raises(torf.ContentError) as excinfo:
                    torrent.verify(content_path)
                if corrupt_piece_index == 0:
                    assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1}, '
                                         'at least one of these files is corrupt: '
                                         f'{real_path(content_file1)}, {real_path(content_file2)}$')
                elif corrupt_piece_index == 1:
                    assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1}, '
                                         'at least one of these files is corrupt: '
                                         f'{real_path(content_file2)}, {real_path(content_file3)}, {real_path(content_file4)}$')
                elif corrupt_piece_index == 2:
                    assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1} in {real_path(content_file4)}$')
                else:
                    assert excinfo.match(f'^Corruption in piece {corrupt_piece_index+1} in {real_path(content_file4)}$')

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
                                            f'{real_path(content_file1)}, {real_path(content_file2)}')
                    elif piece_index == 1 and corrupt_piece_index == 1:
                        assert str(exc) == (f'Corruption in piece {piece_index+1}, '
                                            'at least one of these files is corrupt: '
                                            f'{real_path(content_file2)}, {real_path(content_file3)}, {real_path(content_file4)}')
                    elif piece_index == 2 and corrupt_piece_index == 2:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {real_path(content_file4)}'
                    elif piece_index == 3 and corrupt_piece_index == 3:
                        assert str(exc) == f'Corruption in piece {piece_index+1} in {real_path(content_file4)}'
                    else:
                        assert exc is None

                    return None
                cb.side_effect = assert_call
                assert torrent.verify(content_path, callback=cb, interval=0) == False
                assert cb.call_count == 4

                # Restore original data so it we don't get the same error in the
                # next iteration
                file.write_binary(data)


def test_callback_is_called_at_intervals(tmpdir, create_torrent, monkeypatch):
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
        assert torrent.verify(content_file, callback=cb, interval=2) == True
        assert cb.call_count == (torrent.pieces / 2) + 1


def test_callback_interval_is_ignored_with_exception(tmpdir, create_torrent, monkeypatch):
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
        assert torrent.verify(content_file, callback=cb, interval=3) == False
        assert cb.call_count == 12


def test_callback_raises_exception(tmpdir, create_torrent, monkeypatch):
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file2 = content_path.join('file2.jpg')
    content_file3 = content_path.join('file3.jpg')
    content_file1.write('some data')
    content_file2.write('some other data')
    content_file3.write('some more data')

    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        cb = mock.MagicMock()
        def assert_call(t, fs_path, t_path, files_done, files_total, exc):
            assert t == torrent
            assert files_done == cb.call_count
            assert files_total == 3
            if cb.call_count == 1:
                assert fs_path == str(content_file1)
                assert t_path == os.sep.join(str(content_file1).split(os.sep)[-2:])
                assert exc is None
            elif cb.call_count == 2:
                raise RuntimeError("I'm off")
            elif cb.call_count == 3:
                assert fs_path == str(content_file3)
                assert t_path == os.sep.join(str(content_file3).split(os.sep)[-2:])
                assert exc is None
            return None
        cb.side_effect = assert_call

        with pytest.raises(RuntimeError) as excinfo:
            torrent.verify_filesize(content_path, callback=cb)
        assert excinfo.match(f"^I'm off$")
        assert cb.call_count == 2
