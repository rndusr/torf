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
        torrent.verify_filesize('some/path')
    assert excinfo.match(f'^Invalid metainfo: Mock error$')
    mock_validate.assert_called_once_with()


def test_file_in_singlefile_torrent_doesnt_exist(tmpdir, create_torrent):
    content_path = tmpdir.join('content.jpg')
    content_path.write('some data')
    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        # Without callback
        with pytest.raises(torf.PathNotFoundError) as excinfo:
            torrent.verify_filesize('/some/nonexisting/path')
        assert excinfo.match(f'^/some/nonexisting/path: No such file or directory$')

        # With callback
        cb = mock.MagicMock()
        def assert_call(t, fs_path, t_path, files_done, files_total, exc):
            assert t == torrent
            assert str(fs_path) == str('/some/nonexisting/path')
            assert files_done == 1
            assert files_total == 1
            assert str(exc) == '/some/nonexisting/path: No such file or directory'
            return None
        cb.side_effect = assert_call
        assert torrent.verify_filesize('/some/nonexisting/path', callback=cb) == False
        assert cb.call_count == 1


def test_file_in_multifile_torrent_doesnt_exist(tmpdir, create_torrent):
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
        assert not os.path.exists(content_file1)
        assert os.path.exists(content_file2)
        assert not os.path.exists(content_file3)

        # Without callback
        with pytest.raises(torf.PathNotFoundError) as excinfo:
            torrent.verify_filesize(content_path)
        assert excinfo.match(f'^{content_file1}: No such file or directory$')

        # With callback
        cb = mock.MagicMock()
        def assert_call(t, fs_path, t_path, files_done, files_total, exc):
            assert t == torrent
            assert files_done == cb.call_count
            assert files_total == 3
            if cb.call_count == 1:
                assert fs_path == str(content_file1)
                assert t_path == os.sep.join(str(content_file1).split(os.sep)[-2:])
                assert str(exc) == f'{content_file1}: No such file or directory'
            elif cb.call_count == 2:
                assert fs_path == str(content_file2)
                assert t_path == os.sep.join(str(content_file2).split(os.sep)[-2:])
                assert exc is None
            elif cb.call_count == 3:
                assert fs_path == str(content_file3)
                assert t_path == os.sep.join(str(content_file3).split(os.sep)[-2:])
                assert str(exc) == f'{content_file3}: No such file or directory'
            return None
        cb.side_effect = assert_call
        assert torrent.verify_filesize(content_path, callback=cb) == False
        assert cb.call_count == 3


def test_file_in_singlefile_torrent_has_wrong_size(tmpdir, create_torrent):
    content_path = tmpdir.join('content.jpg')
    content_path.write('some data')
    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        content_path.write('different data')
        assert os.path.getsize(content_path) != torrent.size

        # Without callback
        with pytest.raises(torf.VerifyFileSizeError) as excinfo:
            torrent.verify_filesize(content_path)
        assert excinfo.match(f'^{content_path}: Too big: 14 instead of 9 bytes$')

        # With callback
        cb = mock.MagicMock()
        def assert_call(t, fs_path, t_path, files_done, files_total, exc):
            assert t == torrent
            assert fs_path == str(content_path)
            assert t_path == os.path.basename(content_path)
            assert files_done == cb.call_count
            assert files_total == 1
            assert str(exc) == f'{content_path}: Too big: 14 instead of 9 bytes'
            return None
        cb.side_effect = assert_call
        assert torrent.verify_filesize(content_path, callback=cb) == False
        assert cb.call_count == 1


def test_file_in_multifile_torrent_has_wrong_size(tmpdir, create_torrent):
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file2 = content_path.join('file2.jpg')
    content_file3 = content_path.join('file3.jpg')
    content_file1.write('some data')
    content_file2.write('some other data')
    content_file3.write('some more data')

    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        content_file2.write('some other different data')
        content_file3.write('some more different data')
        assert open(content_file2).read() == 'some other different data'
        assert open(content_file3).read() == 'some more different data'

        # Without callback
        with pytest.raises(torf.VerifyFileSizeError) as excinfo:
            torrent.verify_filesize(content_path)
        assert excinfo.match(f'^{content_file2}: Too big: 25 instead of 15 bytes$')

        # With callback
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
                assert fs_path == str(content_file2)
                assert t_path == os.sep.join(str(content_file2).split(os.sep)[-2:])
                assert str(exc)  == f'{content_file2}: Too big: 25 instead of 15 bytes'
            elif cb.call_count == 3:
                assert fs_path == str(content_file3)
                assert t_path == os.sep.join(str(content_file3).split(os.sep)[-2:])
                assert str(exc) == f'{content_file3}: Too big: 24 instead of 14 bytes'
            return None
        cb.side_effect = assert_call
        assert torrent.verify_filesize(content_path, callback=cb) == False
        assert cb.call_count == 3


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
        with pytest.raises(torf.VerifyNotDirectoryError) as excinfo:
            torrent.verify_filesize(content_path)
        assert excinfo.match(f'^{content_path}: Is a directory$')

        # With callback
        cb = mock.MagicMock()
        def assert_call(t, fs_path, t_path, files_done, files_total, exc):
            assert t == torrent
            assert files_done == 1
            assert files_total == 1
            assert fs_path == str(content_path)
            assert t_path == os.path.basename(content_path)
            assert str(exc) == f'{content_path}: Is a directory'
            return None
        cb.side_effect = assert_call
        assert torrent.verify_filesize(content_path, callback=cb) == False
        assert cb.call_count == 1


def test_path_is_file_and_torrent_contains_directory(tmpdir, create_torrent):
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file2 = content_path.join('file2.jpg')
    content_file1.write('some data')
    content_file2.write('some other data')

    with create_torrent(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        shutil.rmtree(content_path)
        assert not os.path.exists(content_path)

        content_file = tmpdir.join('content')
        content_file.write('some data')
        assert os.path.isfile(content_file)

        # Without callback
        with pytest.raises(torf.PathNotFoundError) as excinfo:
            torrent.verify_filesize(content_path)
        assert excinfo.match(f'^{content_file1}: No such file or directory$')

        # With callback
        cb = mock.MagicMock()
        def assert_call(t, fs_path, t_path, files_done, files_total, exc):
            assert t == torrent
            assert files_done == cb.call_count
            assert files_total == 2
            if cb.call_count == 1:
                assert fs_path == str(content_file1)
                assert t_path == os.sep.join(str(content_file1).split(os.sep)[-2:])
                assert str(exc) == f'{content_file1}: No such file or directory'
            elif cb.call_count == 2:
                assert fs_path == str(content_file2)
                assert t_path == os.sep.join(str(content_file2).split(os.sep)[-2:])
                assert str(exc) == f'{content_file2}: No such file or directory'
            return None
        cb.side_effect = assert_call
        assert torrent.verify_filesize(content_path, callback=cb) == False
        assert cb.call_count == 2


def test_parent_path_of_multifile_torrent_is_unreadable(tmpdir, create_torrent):
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

            # NOTE: We would expect "Permission denied" here, but
            # os.path.exists() can't look inside .../content/unreadable1/ and
            # thus raises "No such file or directory".

            # Without callback
            with pytest.raises(torf.PathNotFoundError) as excinfo:
                torrent.verify_filesize(content_path)
            assert excinfo.match(f'^{content_file1}: No such file or directory$')

            # With callback
            cb = mock.MagicMock()
            def assert_call(t, fs_path, t_path, files_done, files_total, exc):
                assert t == torrent
                assert files_done == cb.call_count
                assert files_total == 3
                if cb.call_count == 1:
                    assert fs_path == str(content_file3)
                    assert t_path == os.sep.join(str(content_file3).split(os.sep)[-5:])
                    assert exc is None
                elif cb.call_count == 2:
                    assert fs_path == str(content_file1)
                    assert t_path == os.sep.join(str(content_file1).split(os.sep)[-5:])
                    assert str(exc) == f'{content_file1}: No such file or directory'
                elif cb.call_count == 3:
                    assert fs_path == str(content_file2)
                    assert t_path == os.sep.join(str(content_file2).split(os.sep)[-5:])
                    assert str(exc) == f'{content_file2}: No such file or directory'
                return None
            cb.side_effect = assert_call
            assert torrent.verify_filesize(content_path, callback=cb) == False
            assert cb.call_count == 3
        finally:
            os.chmod(unreadable_path1, mode=unreadable_path1_mode)
            os.chmod(unreadable_path2, mode=unreadable_path2_mode)


def test_parent_path_of_singlefile_torrent_is_unreadable(tmpdir, create_torrent):
    content_path = tmpdir.mkdir('content')
    content_file = content_path.join('file.jpg')
    content_file.write('some data')

    with create_torrent(path=content_file) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        content_path_mode = os.stat(content_path).st_mode
        try:
            os.chmod(content_path, mode=0o222)

            # NOTE: We would expect "Permission denied" here, but
            # os.path.exists() can't look inside .../content/ and thus raises
            # "No such file or directory".

            # Without callback
            with pytest.raises(torf.PathNotFoundError) as excinfo:
                torrent.verify_filesize(content_file)
            assert excinfo.match(f'^{content_file}: No such file or directory$')

            # With callback
            cb = mock.MagicMock()
            def assert_call(t, fs_path, t_path, files_done, files_total, exc):
                assert t == torrent
                assert files_done == 1
                assert files_total == 1
                assert fs_path == str(content_file)
                assert t_path == os.path.basename(content_file)
                assert str(exc) == f'{content_file}: No such file or directory'
                return None
            cb.side_effect = assert_call
            assert torrent.verify_filesize(content_file, callback=cb) == False
            assert cb.call_count == 1
        finally:
            os.chmod(content_path, mode=content_path_mode)


def test_verify__callback_raises_exception(tmpdir, create_torrent):
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
