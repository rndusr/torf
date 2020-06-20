import os
import shutil
from pathlib import Path
from unittest import mock

import pytest

import torf


def test_validate_is_called_first(monkeypatch):
    torrent = torf.Torrent()
    mock_validate = mock.MagicMock(side_effect=torf.MetainfoError('Mock error'))
    monkeypatch.setattr(torrent, 'validate', mock_validate)
    with pytest.raises(torf.MetainfoError) as excinfo:
        torrent.verify_filesize('some/path')
    assert excinfo.match('^Invalid metainfo: Mock error$')
    mock_validate.assert_called_once_with()


def test_file_in_singlefile_torrent_doesnt_exist(create_file, create_torrent_file):
    content_path = create_file('file.jpg', '<image data>')
    with create_torrent_file(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        # Without callback
        with pytest.raises(torf.ReadError) as excinfo:
            torrent.verify_filesize('/some/nonexisting/path')
        assert excinfo.match('^/some/nonexisting/path: No such file or directory$')

        # With callback
        def assert_call(t, fs_path, t_path, files_done, files_total, exc):
            assert t == torrent
            assert fs_path == Path('/some/nonexisting/path')
            assert files_done == 1
            assert files_total == 1
            assert str(exc) == '/some/nonexisting/path: No such file or directory'
            return None

        cb = mock.MagicMock()
        cb.side_effect = assert_call
        assert torrent.verify_filesize('/some/nonexisting/path', callback=cb) is False
        assert cb.call_count == 1


def test_file_in_multifile_torrent_doesnt_exist(create_dir, create_torrent_file):
    content_path = create_dir('content',
                              ('a.jpg', 'some data'),
                              ('b.jpg', 'some other data'),
                              ('c.jpg', 'some more data'))
    with create_torrent_file(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        os.remove(content_path / 'a.jpg')
        os.remove(content_path / 'c.jpg')
        assert not os.path.exists(content_path / 'a.jpg')
        assert os.path.exists(content_path / 'b.jpg')
        assert not os.path.exists(content_path / 'c.jpg')

        # Without callback
        with pytest.raises(torf.ReadError) as excinfo:
            torrent.verify_filesize(content_path)
        assert excinfo.match(f'^{content_path / "a.jpg"}: No such file or directory$')

        # With callback
        def assert_call(t, fs_path, t_path, files_done, files_total, exc):
            assert t == torrent
            assert files_done == cb.call_count
            assert files_total == 3
            if cb.call_count == 1:
                assert fs_path == content_path / 'a.jpg'
                assert t_path == Path(*(content_path / 'a.jpg').parts[-2:])
                assert str(exc) == f'{fs_path}: No such file or directory'
            elif cb.call_count == 2:
                assert fs_path == content_path / 'b.jpg'
                assert t_path == Path(*(content_path / 'b.jpg').parts[-2:])
                assert exc is None
            elif cb.call_count == 3:
                assert fs_path == content_path / 'c.jpg'
                assert t_path == Path(*(content_path / 'c.jpg').parts[-2:])
                assert str(exc) == f'{fs_path}: No such file or directory'
            return None

        cb = mock.MagicMock()
        cb.side_effect = assert_call
        assert torrent.verify_filesize(content_path, callback=cb) is False
        assert cb.call_count == 3


def test_file_in_singlefile_torrent_has_wrong_size(create_file, create_torrent_file):
    content_path = create_file('file.jpg', '<image data>')
    with create_torrent_file(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        content_path.write_text('<different image data>')
        assert os.path.getsize(content_path) != torrent.size

        # Without callback
        with pytest.raises(torf.VerifyFileSizeError) as excinfo:
            torrent.verify_filesize(content_path)
        assert excinfo.match(f'^{content_path}: Too big: 22 instead of 12 bytes$')

        # With callback
        def assert_call(t, fs_path, t_path, files_done, files_total, exc):
            assert t == torrent
            assert fs_path == content_path
            assert t_path == Path(Path(content_path).name)
            assert files_done == cb.call_count
            assert files_total == 1
            assert str(exc) == f'{content_path}: Too big: 22 instead of 12 bytes'
            return None

        cb = mock.MagicMock()
        cb.side_effect = assert_call
        assert torrent.verify_filesize(content_path, callback=cb) is False
        assert cb.call_count == 1


def test_file_in_multifile_torrent_has_wrong_size(create_dir, create_torrent_file):
    content_path = create_dir('content',
                              ('a.jpg', 100),
                              ('b.jpg', 200),
                              ('c.jpg', 300))
    with create_torrent_file(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        (content_path / 'b.jpg').write_bytes(b'\x00' * 201)
        (content_path / 'c.jpg').write_bytes(b'\x00' * 299)
        assert len((content_path / 'b.jpg').read_bytes()) == 201
        assert len((content_path / 'c.jpg').read_bytes()) == 299

        # Without callback
        with pytest.raises(torf.VerifyFileSizeError) as excinfo:
            torrent.verify_filesize(content_path)
        assert excinfo.match(f'^{content_path / "b.jpg"}: Too big: 201 instead of 200 bytes$')

        # With callback
        def assert_call(t, fs_path, t_path, files_done, files_total, exc):
            assert t == torrent
            assert files_done == cb.call_count
            assert files_total == 3
            if cb.call_count == 1:
                assert fs_path == content_path / 'a.jpg'
                assert t_path == Path(content_path.name, 'a.jpg')
                assert exc is None
            elif cb.call_count == 2:
                assert fs_path == content_path / 'b.jpg'
                assert t_path == Path(content_path.name, 'b.jpg')
                assert str(exc)  == f'{fs_path}: Too big: 201 instead of 200 bytes'
            elif cb.call_count == 3:
                assert fs_path == content_path / 'c.jpg'
                assert t_path == Path(content_path.name, 'c.jpg')
                assert str(exc) == f'{fs_path}: Too small: 299 instead of 300 bytes'
            return None

        cb = mock.MagicMock()
        cb.side_effect = assert_call
        assert torrent.verify_filesize(content_path, callback=cb) is False
        assert cb.call_count == 3


def test_path_is_directory_and_torrent_contains_single_file(create_file, create_dir, create_torrent_file):
    content_data = b'\x00' * 1001
    content_path = create_file('content', content_data)
    with create_torrent_file(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        os.remove(content_path)
        content_path = create_dir('content', ('content', content_data))
        assert os.path.isdir(content_path)

        # Without callback
        with pytest.raises(torf.VerifyNotDirectoryError) as excinfo:
            torrent.verify_filesize(content_path)
        assert excinfo.match(f'^{content_path}: Is a directory$')

        # With callback
        def assert_call(t, fs_path, t_path, files_done, files_total, exc):
            assert t == torrent
            assert files_done == 1
            assert files_total == 1
            assert fs_path == Path(content_path)
            assert t_path == Path(Path(content_path).name)
            assert str(exc) == f'{content_path}: Is a directory'
            return None

        cb = mock.MagicMock()
        cb.side_effect = assert_call
        assert torrent.verify_filesize(content_path, callback=cb) is False
        assert cb.call_count == 1


def test_path_is_file_and_torrent_contains_directory(create_file, create_dir, create_torrent_file):
    content_path = create_dir('content',
                              ('a.jpg', b'\x00' * 1234),
                              ('b.jpg', b'\x00' * 234))
    with create_torrent_file(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        shutil.rmtree(content_path)
        assert not os.path.exists(content_path)

        create_file('content', 'some data')
        assert os.path.isfile(content_path)

        # Without callback
        with pytest.raises(torf.ReadError) as excinfo:
            torrent.verify_filesize(content_path)
        assert excinfo.match(f'^{content_path / "a.jpg"}: No such file or directory$')

        # With callback
        def assert_call(t, fs_path, t_path, files_done, files_total, exc):
            assert t == torrent
            assert files_done == cb.call_count
            assert files_total == 2
            if cb.call_count == 1:
                assert fs_path == content_path / 'a.jpg'
                assert t_path == Path(content_path.name, 'a.jpg')
                assert str(exc) == f'{fs_path}: No such file or directory'
            elif cb.call_count == 2:
                assert fs_path == content_path / 'b.jpg'
                assert t_path == Path(content_path.name, 'b.jpg')
                assert str(exc) == f'{fs_path}: No such file or directory'
            return None

        cb = mock.MagicMock()
        cb.side_effect = assert_call
        assert torrent.verify_filesize(content_path, callback=cb) is False
        assert cb.call_count == 2


def test_parent_path_of_multifile_torrent_is_unreadable(create_dir, create_torrent_file):
    content_path = create_dir('content',
                              ('unreadable1/b/c/a.jpg', 'a data'),
                              ('unreadable2/b/c/b.jpg', 'b data'),
                              ('readable/b/c/c.jpg', 'c data'))
    with create_torrent_file(path=content_path) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)
        unreadable_path1_mode = os.stat(content_path / 'unreadable1').st_mode
        unreadable_path2_mode = os.stat(content_path / 'unreadable2').st_mode
        try:
            os.chmod((content_path / 'unreadable1'), mode=0o222)
            os.chmod((content_path / 'unreadable2'), mode=0o222)

            # NOTE: We would expect "Permission denied" here, but
            # os.path.exists() can't look inside .../content/unreadable1/ and
            # thus raises "No such file or directory".

            # Without callback
            with pytest.raises(torf.ReadError) as excinfo:
                torrent.verify_filesize(content_path)
            assert excinfo.match(f'^{content_path / "unreadable1/b/c/a.jpg"}: No such file or directory$')

            # With callback
            def assert_call(t, fs_path, t_path, files_done, files_total, exc):
                assert t == torrent
                assert files_done == cb.call_count
                assert files_total == 3
                if cb.call_count == 1:
                    assert fs_path == content_path / 'readable/b/c/c.jpg'
                    assert t_path == Path(content_path.name, 'readable/b/c/c.jpg')
                    assert exc is None
                elif cb.call_count == 2:
                    assert fs_path == content_path / 'unreadable1/b/c/a.jpg'
                    assert t_path == Path(content_path.name, 'unreadable1/b/c/a.jpg')
                    assert str(exc) == f'{fs_path}: No such file or directory'
                elif cb.call_count == 3:
                    assert fs_path == Path(content_path / 'unreadable2/b/c/b.jpg')
                    assert t_path == Path(content_path.name, 'unreadable2/b/c/b.jpg')
                    assert str(exc) == f'{fs_path}: No such file or directory'
                return None

            cb = mock.MagicMock()
            cb.side_effect = assert_call
            assert torrent.verify_filesize(content_path, callback=cb) is False
            assert cb.call_count == 3
        finally:
            os.chmod((content_path / 'unreadable1'), mode=unreadable_path1_mode)
            os.chmod((content_path / 'unreadable2'), mode=unreadable_path2_mode)


def test_parent_path_of_singlefile_torrent_is_unreadable(create_dir, create_torrent_file):
    parent_path = create_dir('parent',
                             ('file.jpg', b'\x00' * 123))
    content_file = str(parent_path / 'file.jpg')
    with create_torrent_file(path=content_file) as torrent_file:
        torrent = torf.Torrent.read(torrent_file)

        parent_path_mode = os.stat(parent_path).st_mode
        try:
            os.chmod(parent_path, mode=0o222)

            # NOTE: We would expect "Permission denied" here, but
            # os.path.exists() can't look inside "parent" directory and thus
            # raises "No such file or directory".

            # Without callback
            with pytest.raises(torf.ReadError) as excinfo:
                torrent.verify_filesize(content_file)
            assert excinfo.match(f'^{content_file}: No such file or directory$')

            # With callback
            def assert_call(t, fs_path, t_path, files_done, files_total, exc):
                assert t == torrent
                assert files_done == 1
                assert files_total == 1
                assert fs_path == Path(content_file)
                assert t_path == Path(Path(content_file).name)
                assert str(exc) == f'{content_file}: No such file or directory'
                return None

            cb = mock.MagicMock()
            cb.side_effect = assert_call
            assert torrent.verify_filesize(content_file, callback=cb) is False
            assert cb.call_count == 1
        finally:
            os.chmod(parent_path, mode=parent_path_mode)


def test_verify__callback_raises_exception(create_dir, create_torrent_file):
    content_path = create_dir('content',
                              ('a.jpg', b'\x00' * 123),
                              ('b.jpg', b'\x00' * 456),
                              ('c.jpg', b'\x00' * 789))
    with create_torrent_file(path=content_path) as torrent_file:
        def assert_call(t, fs_path, t_path, files_done, files_total, exc):
            assert t == torrent
            assert files_done == cb.call_count
            assert files_total == 3
            if cb.call_count == 1:
                assert fs_path == content_path / 'a.jpg'
                assert t_path == Path(content_path.name, 'a.jpg')
                assert exc is None
            elif cb.call_count == 2:
                raise RuntimeError("I'm off")
            elif cb.call_count == 3:
                assert fs_path == content_path / 'c.jpg'
                assert t_path == Path(content_path.name, 'c.jpg')
                assert exc is None
            return None

        torrent = torf.Torrent.read(torrent_file)
        cb = mock.MagicMock()
        cb.side_effect = assert_call

        with pytest.raises(RuntimeError) as excinfo:
            torrent.verify_filesize(content_path, callback=cb)
        assert excinfo.match("^I'm off$")
        assert cb.call_count == 2
