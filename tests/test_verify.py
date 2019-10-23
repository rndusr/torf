import torf

import pytest
from unittest import mock
import os
import errno

def test_verify__validate_is_called_first(monkeypatch):
    torrent = torf.Torrent()
    mock_validate = mock.MagicMock(side_effect=torf.MetainfoError('Mock error'))
    monkeypatch.setattr(torrent, 'validate', mock_validate)
    with pytest.raises(torf.MetainfoError) as excinfo:
        torrent.verify('some/path')
    assert excinfo.match(f'^Invalid metainfo: Mock error$')
    mock_validate.assert_called_once_with()

def test_verify__file_in_singlefile_torrent_doesnt_exist(tmpdir, create_torrent):
    content_path = tmpdir.join('content.jpg')
    content_path.write('some data')
    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        # Without callback
        with pytest.raises(torf.PathNotFoundError) as excinfo:
            torrent.verify('nonexisting/path')
        assert excinfo.match(f'^nonexisting/path: No such file or directory$')

        # With callback
        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, exc):
            assert t == torrent
            assert str(path) == str('nonexisting/path')
            assert pieces_done == 0
            assert pieces_total == torrent.pieces
            assert str(exc) == 'nonexisting/path: No such file or directory'
            return None
        cb.side_effect = assert_call
        assert torrent.verify('nonexisting/path', callback=cb, interval=0) == False
        assert cb.call_count == 1

def test_verify__file_in_multifile_torrent_doesnt_exist(tmpdir, create_torrent):
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file2 = content_path.join('file2.jpg')
    content_file3 = content_path.join('file3.jpg')
    content_file1.write('some data')
    content_file2.write('some other data')
    content_file3.write('some more data')

    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        os.remove(content_file1)
        os.remove(content_file3)

        # Without callback
        with pytest.raises(torf.PathNotFoundError) as excinfo:
            torrent.verify(content_path)
        assert excinfo.match(f'^{content_file1}: No such file or directory$')

        # With callback
        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, exc):
            assert t == torrent
            assert pieces_done == 0
            assert pieces_total == torrent.pieces
            if cb.call_count == 1:
                assert str(path) == str(content_file1)
                assert str(exc) == f'{content_file1}: No such file or directory'
            elif cb.call_count == 2:
                assert str(path) == str(content_file3)
                assert str(exc) == f'{content_file3}: No such file or directory'
            return None
        cb.side_effect = assert_call
        assert torrent.verify(content_path, callback=cb, interval=0) == False
        assert cb.call_count == 2

def test_verify__file_in_singlefile_torrent_has_wrong_size(tmpdir, create_torrent):
    content_path = tmpdir.join('content.jpg')
    content_path.write('some data')
    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        content_path.write('different data')
        assert os.path.getsize(content_path) != torrent.size

        # Without callback
        with pytest.raises(torf.FileSizeError) as excinfo:
            torrent.verify(content_path)
        assert excinfo.match(f'^{content_path}: Unexpected file size: 14 instead of 9 bytes$')

        # With callback
        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, exc):
            assert t == torrent
            assert pieces_done == 0
            assert pieces_total == torrent.pieces
            assert str(path) == str(content_path)
            assert str(exc) == f'{content_path}: Unexpected file size: 14 instead of 9 bytes'
            return None
        cb.side_effect = assert_call
        assert torrent.verify(content_path, callback=cb, interval=0) == False
        assert cb.call_count == 1

def test_verify__file_in_multifile_torrent_has_wrong_size(tmpdir, create_torrent):
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file2 = content_path.join('file2.jpg')
    content_file3 = content_path.join('file3.jpg')
    content_file1.write('some data')
    content_file2.write('some other data')
    content_file3.write('some more data')

    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        content_file2.write('some data')
        content_file3.write('some more data!')

        # Without callback
        with pytest.raises(torf.FileSizeError) as excinfo:
            torrent.verify(content_path)
        assert excinfo.match(f'^{content_file2}: Unexpected file size: 9 instead of 15 bytes$')

        # With callback
        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, exc):
            assert t == torrent
            assert pieces_done == 0
            assert pieces_total == torrent.pieces
            if cb.call_count == 1:
                assert str(path) == str(content_file2)
                assert str(exc) == f'{content_file2}: Unexpected file size: 9 instead of 15 bytes'
            elif cb.call_count == 2:
                assert str(path) == str(content_file3)
                assert str(exc) == f'{content_file3}: Unexpected file size: 15 instead of 14 bytes'
            return None
        cb.side_effect = assert_call
        assert torrent.verify(content_path, callback=cb, interval=0) == False
        assert cb.call_count == 2

def test_verify__path_is_directory_and_torrent_contains_single_file(tmpdir, create_torrent):
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
        with pytest.raises(torf.IsDirectoryError) as excinfo:
            torrent.verify(content_path)
        assert excinfo.match(f'^{content_path}: Is a directory$')

        # With callback
        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, exc):
            assert t == torrent
            assert pieces_done == 0
            assert pieces_total == torrent.pieces
            assert str(path) == str(content_path)
            assert str(exc) == f'{content_path}: Is a directory'
            return None
        cb.side_effect = assert_call
        assert torrent.verify(content_path, callback=cb, interval=0) == False
        assert cb.call_count == 1

def test_verify__parent_path_is_unreadable(tmpdir, create_torrent):
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

            # NOTE: We would expect "Permission denied" here, but os.path.exists()
            # can't look inside .../content/unreadable1/ and thus raises "No such
            # file or directory".

            # Without callback
            with pytest.raises(torf.PathNotFoundError) as excinfo:
                torrent.verify(content_path)
            assert excinfo.match(f'^{content_file1}: No such file or directory$')

            # With callback
            cb = mock.MagicMock()
            def assert_call(t, path, pieces_done, pieces_total, exc):
                assert t == torrent
                assert pieces_done == 0
                assert pieces_total == torrent.pieces
                if cb.call_count == 1:
                    assert str(path) == str(content_file1)
                    assert str(exc) == f'{content_file1}: No such file or directory'
                elif cb.call_count == 2:
                    assert str(path) == str(content_file2)
                    assert str(exc) == f'{content_file2}: No such file or directory'
                return None
            cb.side_effect = assert_call
            assert torrent.verify(content_path, callback=cb, interval=0) == False
            assert cb.call_count == 2
        finally:
            os.chmod(unreadable_path1, mode=unreadable_path1_mode)
            os.chmod(unreadable_path2, mode=unreadable_path2_mode)

def test_verify__allow_different_name_argument_with_singlefile_torrent(tmpdir, create_torrent):
    content_file = tmpdir.join('file.jpg')
    content_file_data = os.urandom(torf.Torrent.piece_size_min * 10)
    content_file.write_binary(content_file_data)
    with create_torrent(path=content_file) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        new_content_file = os.path.join(os.path.dirname(content_file),
                                        'different_name.jpg')
        os.rename(content_file, new_content_file)
        assert os.path.exists(new_content_file)
        assert not os.path.exists(content_file)

        # Without callback
        assert torrent.verify(new_content_file, allow_different_name=True) == True
        with pytest.raises(torf.PathNotFoundError) as excinfo:
            torrent.verify(new_content_file, allow_different_name=False)
        assert excinfo.match(f'^{content_file}: No such file or directory$')

        # With callback
        # allow_different_name=True
        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, exc):
            assert t == torrent
            assert pieces_done == cb.call_count
            assert pieces_total == torrent.pieces
            assert str(path) == str(new_content_file)
            assert exc == None
            return None
        cb.side_effect = assert_call
        assert torrent.verify(new_content_file, callback=cb, interval=0, allow_different_name=True) == True
        assert cb.call_count == 10

        # allow_different_name=False
        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, exc):
            assert t == torrent
            assert pieces_done == 0
            assert pieces_total == torrent.pieces
            assert str(path) == str(content_file)
            assert str(exc) == f'{content_file}: No such file or directory'
            return None
        cb.side_effect = assert_call
        assert torrent.verify(new_content_file, callback=cb, interval=0, allow_different_name=False) == False
        assert cb.call_count == 1

def test_verify__allow_different_name_argument_with_multifile_torrent(tmpdir, create_torrent):
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file2 = content_path.join('file2.jpg')
    content_data1 = os.urandom(torf.Torrent.piece_size_min * 5)
    content_data2 = os.urandom(torf.Torrent.piece_size_min * 2)
    content_file1.write_binary(content_data1)
    content_file2.write_binary(content_data2)

    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        new_content_path = os.path.join(os.path.dirname(content_path), 'different_name')
        content_path.rename(new_content_path)
        assert not os.path.exists(content_path)
        assert os.path.isdir(new_content_path)

        # Without callback
        assert torrent.verify(new_content_path, allow_different_name=True) == True
        with pytest.raises(torf.PathNotFoundError) as excinfo:
            torrent.verify(new_content_path, allow_different_name=False)
        assert excinfo.match(f'^{content_file1}: No such file or directory$')

        # With callback
        # allow_different_name=True
        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, exc):
            assert t == torrent
            assert pieces_done == cb.call_count
            assert pieces_total == torrent.pieces
            if pieces_done <= 5:
                assert str(path) == os.path.join(new_content_path, 'file1.jpg')
            else:
                assert str(path) == os.path.join(new_content_path, 'file2.jpg')
            assert exc == None
            return None
        cb.side_effect = assert_call
        assert torrent.verify(new_content_path, callback=cb, interval=0, allow_different_name=True) == True
        assert cb.call_count == 7

        # allow_different_name=False
        cb = mock.MagicMock()
        def assert_call(t, path, pieces_done, pieces_total, exc):
            assert t == torrent
            assert pieces_done == 0
            assert pieces_total == torrent.pieces
            if cb.call_count == 1:
                assert str(path) == str(content_file1)
                assert str(exc) == f'{content_file1}: No such file or directory'
            else:
                assert str(path) == str(content_file2)
                assert str(exc) == f'{content_file2}: No such file or directory'
            return None
        cb.side_effect = assert_call
        assert torrent.verify(new_content_path, callback=cb, interval=0, allow_different_name=False) == False
        assert cb.call_count == 2

def test_verify__singlefile__hash_check(tmpdir, create_torrent):
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
                assert excinfo.match(f'^Unexpected bytes in piece {corrupt_piece_index+1}$')

                # With callback
                cb = mock.MagicMock(return_value=None)
                def assert_call(t, path, pieces_done, pieces_total, exc):
                    assert t == torrent
                    assert pieces_total == torrent.pieces
                    assert str(path) == str(content_path)
                    assert 1 <= pieces_done <= 4
                    if pieces_done == 1 and corrupt_piece_index == 0:
                        assert str(exc) == f'Unexpected bytes in piece {pieces_done}'
                    elif pieces_done == 2 and corrupt_piece_index == 1:
                        assert str(exc) == f'Unexpected bytes in piece {pieces_done}'
                    elif pieces_done == 3 and corrupt_piece_index == 2:
                        assert str(exc) == f'Unexpected bytes in piece {pieces_done}'
                    elif pieces_done == 4 and corrupt_piece_index == 3:
                        assert str(exc) == f'Unexpected bytes in piece {pieces_done}'
                    else:
                        assert exc is None
                    return None
                cb.side_effect = assert_call
                assert torrent.verify(content_path, callback=cb, interval=0) == False
                assert cb.call_count == 4

def test_verify__multifile__hash_check__pieces_align_to_files(tmpdir, create_torrent):
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
                with pytest.raises(torf.ContentError) as excinfo:
                    torrent.verify(content_path)
                assert excinfo.match(f'^Unexpected bytes in piece {corrupt_piece_index+1} in {file}$')

                # With callback
                cb = mock.MagicMock(return_value=None)
                def assert_call(t, path, pieces_done, pieces_total, exc):
                    assert t == torrent
                    assert pieces_total == torrent.pieces

                    assert 1 <= pieces_done <= 6
                    if pieces_done == 1:
                        assert str(path) == str(content_file1)
                    elif 2 <= pieces_done <= 3:
                        assert str(path) == str(content_file2)
                    elif 4 <= pieces_done <= 6:
                        assert str(path) == str(content_file3)

                    if pieces_done == 1 and corrupt_piece_index == 0:
                        assert str(exc) == f'Unexpected bytes in piece {pieces_done} in {content_file1}'
                    elif pieces_done == 2 and corrupt_piece_index == 1:
                        assert str(exc) == f'Unexpected bytes in piece {pieces_done} in {content_file2}'
                    elif pieces_done == 3 and corrupt_piece_index == 2:
                        assert str(exc) == f'Unexpected bytes in piece {pieces_done} in {content_file2}'
                    elif pieces_done == 4 and corrupt_piece_index == 3:
                        assert str(exc) == f'Unexpected bytes in piece {pieces_done} in {content_file3}'
                    elif pieces_done == 5 and corrupt_piece_index == 4:
                        assert str(exc) == f'Unexpected bytes in piece {pieces_done} in {content_file3}'
                    elif pieces_done == 6 and corrupt_piece_index == 5:
                        assert str(exc) == f'Unexpected bytes in piece {pieces_done} in {content_file3}'
                    else:
                        assert exc is None

                    return None
                cb.side_effect = assert_call
                assert torrent.verify(content_path, callback=cb, interval=0) == False
                assert cb.call_count == 6

                # Restore original data so it we don't get the same error in the
                # next iteration
                file.write_binary(data)

def test_verify__multifile__hash_check__pieces_dont_align_to_files(tmpdir, create_torrent):
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
                with pytest.raises(torf.ContentError) as excinfo:
                    torrent.verify(content_path)
                if corrupt_piece_index == 0:
                    assert excinfo.match(f'^Unexpected bytes in piece {corrupt_piece_index+1} in {content_file1}$')
                elif corrupt_piece_index == 1:
                    assert excinfo.match(f'^Unexpected bytes in piece {corrupt_piece_index+1}, '
                                         f'at least one of these files is corrupt: {content_file1}, {content_file2}$')
                elif corrupt_piece_index == 2:
                    assert excinfo.match(f'^Unexpected bytes in piece {corrupt_piece_index+1} in {content_file2}$')
                elif corrupt_piece_index == 3:
                    assert excinfo.match(f'^Unexpected bytes in piece {corrupt_piece_index+1}, '
                                         f'at least one of these files is corrupt: {content_file2}, {content_file3}$')
                elif corrupt_piece_index == 4:
                    assert excinfo.match(f'^Unexpected bytes in piece {corrupt_piece_index+1}, '
                                         f'at least one of these files is corrupt: {content_file2}, {content_file3}$')
                else:
                    assert excinfo.match(f'^Unexpected bytes in piece {corrupt_piece_index+1} in {content_file3}$')

                # With callback
                cb = mock.MagicMock()
                def assert_call(t, path, pieces_done, pieces_total, exc):
                    assert t == torrent
                    assert pieces_total == torrent.pieces

                    assert 1 <= pieces_done <= 7
                    if pieces_done == 1:
                        assert str(path) == str(content_file1)
                    elif pieces_done == 2:
                        assert str(path) == str(content_file2)
                    elif pieces_done == 3:
                        assert str(path) == str(content_file2)
                    else:
                        assert str(path) == str(content_file3)

                    if pieces_done == 1 and corrupt_piece_index == 0:
                        assert str(exc) == f'Unexpected bytes in piece {corrupt_piece_index+1} in {content_file1}'
                    elif pieces_done == 2 and corrupt_piece_index == 1:
                        assert str(exc) == (f'Unexpected bytes in piece {corrupt_piece_index+1}, '
                                            f'at least one of these files is corrupt: {content_file1}, {content_file2}')
                    elif pieces_done == 3 and corrupt_piece_index == 2:
                        assert str(exc) == f'Unexpected bytes in piece {corrupt_piece_index+1} in {content_file2}'
                    elif pieces_done == 4 and corrupt_piece_index == 3:
                        assert str(exc) == (f'Unexpected bytes in piece {corrupt_piece_index+1}, '
                                            f'at least one of these files is corrupt: {content_file2}, {content_file3}')
                    # NOTE: Piece index 4 is never corrupted because file3.jpg is so big.
                    elif pieces_done == 6 and corrupt_piece_index == 5:
                        assert str(exc) == f'Unexpected bytes in piece {corrupt_piece_index+1} in {content_file3}'
                    elif pieces_done == 7 and corrupt_piece_index == 6:
                        assert str(exc) == f'Unexpected bytes in piece {corrupt_piece_index+1} in {content_file3}'
                    else:
                        assert exc is None

                    return None
                cb.side_effect = assert_call
                assert torrent.verify(content_path, callback=cb, interval=0) == False
                assert cb.call_count == 7

                # Restore original data so it we don't get the same error in the
                # next iteration
                file.write_binary(data)

def test_verify__multifile__hash_check__one_piece_covers_multiple_files(tmpdir, create_torrent):
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
                with pytest.raises(torf.ContentError) as excinfo:
                    torrent.verify(content_path)
                if corrupt_piece_index == 0:
                    assert excinfo.match(f'^Unexpected bytes in piece {corrupt_piece_index+1}, '
                                         'at least one of these files is corrupt: '
                                         f'{content_file1}, {content_file2}$')
                elif corrupt_piece_index == 1:
                    assert excinfo.match(f'^Unexpected bytes in piece {corrupt_piece_index+1}, '
                                         'at least one of these files is corrupt: '
                                         f'{content_file2}, {content_file3}, {content_file4}$')
                elif corrupt_piece_index == 2:
                    assert excinfo.match(f'^Unexpected bytes in piece {corrupt_piece_index+1} in {content_file4}$')
                else:
                    assert excinfo.match(f'^Unexpected bytes in piece {corrupt_piece_index+1} in {content_file4}$')

                # With callback
                cb = mock.MagicMock()
                def assert_call(t, path, pieces_done, pieces_total, exc):
                    assert t == torrent
                    assert pieces_total == torrent.pieces
                    assert pieces_done == cb.call_count

                    if cb.call_count == 1:
                        assert str(path) == str(content_file2)
                    elif cb.call_count == 2:
                        assert str(path) == str(content_file4)
                    elif cb.call_count == 3:
                        assert str(path) == str(content_file4)
                    else:
                        assert str(path) == str(content_file4)

                    if pieces_done == 1 and corrupt_piece_index == 0:
                        assert str(exc) == (f'Unexpected bytes in piece {corrupt_piece_index+1}, '
                                            'at least one of these files is corrupt: '
                                            f'{content_file1}, {content_file2}')
                    elif pieces_done == 2 and corrupt_piece_index == 1:
                        assert str(exc) == (f'Unexpected bytes in piece {corrupt_piece_index+1}, '
                                            'at least one of these files is corrupt: '
                                            f'{content_file2}, {content_file3}, {content_file4}')
                    elif pieces_done == 3 and corrupt_piece_index == 2:
                        assert str(exc) == f'Unexpected bytes in piece {corrupt_piece_index+1} in {content_file4}'
                    elif pieces_done == 4 and corrupt_piece_index == 3:
                        assert str(exc) == f'Unexpected bytes in piece {corrupt_piece_index+1} in {content_file4}'
                    else:
                        assert exc is None

                    return None
                cb.side_effect = assert_call
                assert torrent.verify(content_path, callback=cb, interval=0) == False
                assert cb.call_count == 4

                # Restore original data so it we don't get the same error in the
                # next iteration
                file.write_binary(data)

def test_verify__callback_is_called_at_intervals(tmpdir, create_torrent, monkeypatch):
    content_file = tmpdir.join('content.jpg')
    content_file.write_binary(os.urandom(torf.Torrent.piece_size_min * 20))
    with create_torrent(path=content_file) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        import time
        monkeypatch.setattr(time, 'time',
                            mock.MagicMock(side_effect=range(1, 100)))

        cb = mock.MagicMock()
        exp_pieces_done = list(range(1, 20, 2)) + [20]
        exp_call_count = len(exp_pieces_done)
        def assert_call(t, path, pieces_done, pieces_total, exc):
            assert t == torrent
            assert str(path) == str(content_file)
            assert pieces_done == exp_pieces_done.pop(0)
            assert pieces_total == torrent.pieces
            assert exc is None
            return None
        cb.side_effect = assert_call
        assert torrent.verify(content_file, callback=cb, interval=2) == True
        assert cb.call_count == exp_call_count
        assert len(exp_pieces_done) == 0

def test_verify__callback_interval_is_ignored_with_exception(tmpdir, create_torrent, monkeypatch):
    piece_size = torf.Torrent.piece_size_min
    content_file = tmpdir.join('content.jpg')
    content_data = os.urandom(piece_size * 30)
    content_file.write_binary(content_data)
    with create_torrent(path=content_file) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        import time
        monkeypatch.setattr(time, 'time',
                            mock.MagicMock(side_effect=range(1, 100)))

        corrupt_data = bytearray(content_data)
        corrupt_data[piece_size*7] = (content_data[piece_size*7] + 1) % 256
        corrupt_data[piece_size*8] = (content_data[piece_size*8] + 1) % 256
        corrupt_data[piece_size*22] = (content_data[piece_size*22] + 1) % 256
        assert len(corrupt_data) == len(content_data)
        assert corrupt_data != content_data
        content_file.write_binary(corrupt_data)

        cb = mock.MagicMock()
        exp_pieces_done = [2, 5, 8, 9, 12, 15, 18, 21, 23, 26, 29, 30]
        exp_call_count = len(exp_pieces_done)
        def assert_call(t, path, pieces_done, pieces_total, exc):
            assert t == torrent
            assert str(path) == str(content_file)
            print(f'pieces_done={pieces_done}, exp_pieces_done={exp_pieces_done}')
            assert pieces_done == exp_pieces_done.pop(0)
            assert pieces_total == torrent.pieces
            if pieces_done in (8, 9, 23):
                assert exc is not None
            else:
                assert exc is None
            return None
        cb.side_effect = assert_call
        assert torrent.verify(content_file, callback=cb, interval=3) == False
        assert cb.call_count == exp_call_count
        assert len(exp_pieces_done) == 0
