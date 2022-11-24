import base64
import os
from collections import defaultdict
from pathlib import Path
from unittest import mock

import pytest

import torf

from . import *  # noqa: F403


def test_no_path():
    t = torf.Torrent()
    with pytest.raises(RuntimeError) as e:
        t.generate()
    assert str(e.value) == 'generate() called with no path specified'


def test_with_empty_file(create_file):
    # Create content so we can set path
    content_path = create_file('file.jpg', '<image data>')
    t = torf.Torrent(content_path)
    content_path.write_text('')
    with pytest.raises(torf.PathError) as e:
        t.generate()
    assert str(e.value) == f'{t.path}: Empty or all files excluded'


def test_with_empty_directory(create_dir):
    # Create content so we can set path
    content_path = create_dir('empty', ('a file', '<data>'))
    t = torf.Torrent(content_path)
    (content_path / 'a file').unlink()
    with pytest.raises(torf.ReadError) as e:
        t.generate()
    assert str(e.value) == f'{content_path / "a file"}: No such file or directory'


def test_nonexisting_path(create_file):
    content_path = create_file('file.jpg', '<image data>')
    t = torf.Torrent(content_path)
    content_path.unlink()
    with pytest.raises(torf.ReadError) as e:
        t.generate()
    assert str(e.value) == f'{content_path}: No such file or directory'


def test_with_all_files_excluded(create_dir):
    # Create content so we can set path
    content_path = create_dir('content',
                              ('a.jpg', '<image data>'),
                              ('b.jpg', '<image data>'),
                              ('c.jpg', '<image data>'))
    t = torf.Torrent(content_path, exclude_globs=['*.jpg'])
    with pytest.raises(torf.PathError) as e:
        t.generate()
    assert str(e.value) == f'{t.path}: Empty or all files excluded'


def test_unreadable_basedir_in_multifile_torrent(create_dir):
    content_path = create_dir('content',
                              ('a.jpg', '<image data>'),
                              ('b.jpg', '<image data>'),
                              ('c.jpg', '<image data>'))
    t = torf.Torrent(content_path)
    old_mode = os.stat(content_path).st_mode
    try:
        os.chmod(content_path, mode=0o222)
        with pytest.raises(torf.ReadError) as e:
            t.generate()
        assert str(e.value) == f'{content_path / "a.jpg"}: Permission denied'
    finally:
        os.chmod(content_path, mode=old_mode)


def test_unreadable_file_in_multifile_torrent(create_dir):
    content_path = create_dir('content',
                              ('a.jpg', '<image data>'),
                              ('b.jpg', '<image data>'),
                              ('c.jpg', '<image data>'))
    t = torf.Torrent(content_path)
    old_mode = os.stat(content_path).st_mode
    try:
        os.chmod(content_path / 'b.jpg', mode=0o222)
        with pytest.raises(torf.ReadError) as e:
            t.generate()
        assert str(e.value) == f'{content_path / "b.jpg"}: Permission denied'
    finally:
        os.chmod(content_path, mode=old_mode)


def test_metainfo_with_singlefile_torrent(create_file, random_seed):
    with random_seed(0):
        content_path = create_file('file.jpg', torf.Torrent.piece_size_min_default * 10.123)
    # exp_* values come from these commands:
    # $ mktorrent -l 15 /tmp/pytest-of-*/pytest-current/test_metainfo_with_singlefile_current/file.jpg
    # $ btcheck -i file.jpg.torrent -n | grep Hash
    # $ python3 -c "from flatbencode import decode; print(decode(open('file.jpg.torrent', 'rb').read())[b'info'][b'pieces'])"
    exp_infohash = 'e7e02c57df57f30f5e66a69bfa210e9c61a5a8f6'
    exp_pieces = (b"<\x9c7\x80\xa5\xf6-\xb7)\xd0A\x1d\xb5\x1b\xacw\x10\x91\x9c\xe8\xb4\x16"
                  b"\x00bg\xbc`\xc5\xc2\xf86\x88\xb2~\xd6E\xeeZ\xb0d\xcd\x9ek(\xc746G\x17"
                  b"\xab\xa6'/D\xba\xd9\xf0d\x81\xe3\xf5C\x82JQ\xde\xb5\x17w\xda\xbc\xb7Ek"
                  b"\nHU\xcd\x1f\xd6C\xcb!\xb0CW\\\xc4\x8d\xad9\xbe\xb4V\x8a7\xdf\x9a\xabV"
                  b"\xa6\xe5\xee3\x81\xe5I\xa7\xfe#\xcb\xea\xc3\x8e\xc4\x00\x91\xdb\x00\xaf")
    _check_metainfo(content_path, 2**15, exp_infohash, exp_pieces)

def test_metainfo_with_multifile_torrent(create_dir, random_seed):
    with random_seed(0):
        content_path = create_dir('content',
                                  ('a.jpg', torf.Torrent.piece_size_min_default * 1.123),
                                  ('b.jpg', torf.Torrent.piece_size_min_default * 2.456),
                                  ('c.jpg', torf.Torrent.piece_size_min_default * 3.789))
    # exp_* values come from these commands:
    # $ mktorrent -l 15 /tmp/pytest-of-*/pytest-current/test_metainfo_with_multifile_tcurrent/content/
    # $ btcheck -i content.torrent -n | grep Hash
    # $ python3 -c "from flatbencode import decode; print(decode(open('content.torrent', 'rb').read())[b'info'][b'pieces'])"
    exp_infohash = 'b36eeca9231867ebf650ed82a54216617408d2ce'
    exp_pieces = (b'\x84{\x9eM\x16\xa9\xe9\xf7V\xb8\xb3\xc2\xb8Q\xfaw\xea \xb9\xdc'
                  b'\xf2\xc0\x0e\rXE\x85g\xe6k\x1dt\xa6\xca\x7f/\xb5)A"5!\xb9\xda\xe2'
                  b'"\x15c^\x0e\xf7\x91|\x06V\xdc}\xd9\xb0<./\x0fBe\xcb\xd8*\xae\xd1"'
                  b'\x05\n\x1b\xf3\x18\x1c\xd7u\xe3')
    _check_metainfo(content_path, 2**15, exp_infohash, exp_pieces)

def _check_metainfo(content_path, piece_size, exp_infohash, exp_pieces):
    exp_hashes = tuple(exp_pieces[i : i + 20]
                       for i in range(0, len(exp_pieces), 20))
    t = torf.Torrent(content_path)
    t.piece_size = piece_size
    t.generate()
    assert t.infohash == exp_infohash
    assert t.infohash_base32 == base64.b32encode(base64.b16decode(exp_infohash.upper()))
    assert t.metainfo['info']['pieces'] == exp_pieces
    assert t.hashes == exp_hashes
    assert t.piece_size == piece_size
    assert t.metainfo['info']['piece length'] == piece_size


def test_callback_is_called_with_correct_arguments(filespecs, piece_size, create_file, create_dir, forced_piece_size):
    display_filespecs(filespecs, piece_size)  # noqa: F405
    if len(filespecs) == 1:
        content_path = create_file(filespecs[0][0], filespecs[0][1])
    else:
        content_path = create_dir('content', *filespecs)

    exp_pieces_done = 1
    seen_filepaths = defaultdict(lambda: 0)

    def assert_cb_args(torrent, filepath, pieces_done, pieces_total):
        nonlocal exp_pieces_done
        assert torrent is t
        assert pieces_done == exp_pieces_done
        exp_pieces_done += 1
        assert isinstance(filepath, os.PathLike)
        seen_filepaths[filepath.name] += 1
        assert pieces_total == t.pieces

    with forced_piece_size(piece_size):
        t = torf.Torrent(content_path)
        cb = mock.Mock(side_effect=assert_cb_args)
        success = t.generate(callback=cb, interval=0)

    assert success is True
    assert t.piece_size == piece_size
    assert cb.call_count == t.pieces

    exp_filepaths = defaultdict(lambda: 0)
    for pos in range(0, t.size, piece_size):
        files = pos2files(pos, filespecs, piece_size)  # noqa: F405
        exp_filepaths[files[-1]] += 1

    assert seen_filepaths == exp_filepaths


def test_callback_is_called_at_interval(filespecs, piece_size, create_file, create_dir,
                                        forced_piece_size, monkeypatch):
    display_filespecs(filespecs, piece_size)  # noqa: F405
    if len(filespecs) == 1:
        content_path = create_file(filespecs[0][0], filespecs[0][1])
    else:
        content_path = create_dir('content', *filespecs)

    with forced_piece_size(piece_size):
        t = torf.Torrent(content_path)
        monkeypatch.setattr(torf._generate, 'time_monotonic',
                            mock.Mock(side_effect=range(int(1e9))))
        for interval in (1, 2, 3):
            cb = mock.Mock(return_value=None)
            success = t.generate(callback=cb, interval=interval)
            assert success is True

            if interval > 1 and t.pieces % interval == 0:
                exp_call_count = t.pieces // interval + t.pieces % interval + 1
            else:
                exp_call_count = t.pieces // interval + t.pieces % interval
            assert cb.call_count == exp_call_count


def test_callback_cancels(piece_size, create_file, forced_piece_size, mocker):
    def maybe_cancel(torrent, filepath, pieces_done, pieces_total):
        if pieces_done / pieces_total > 0.1:
            return 'STOP THE PRESSES!'

    cb = mock.Mock(side_effect=maybe_cancel)
    piece_count = 1000
    content_path = create_file('file.jpg', piece_size * piece_count)

    with forced_piece_size(piece_size):
        t = torf.Torrent(content_path)
        success = t.generate(callback=cb, interval=0, threads=1)
        assert success is False
        assert cb.call_count < piece_count


def test_callback_raises_exception(piece_size, create_file, forced_piece_size):
    # We need a large file size so we can test that the hashers actually stop
    # before all pieces are hashed.
    content_path = create_file('file.jpg', piece_size * 1000)
    with forced_piece_size(piece_size):
        with mock.patch('torf._generate.sha1') as sha1_mock:
            def mock_digest():
                return b'\x00' * 20

            sha1_mock.return_value.digest.side_effect = mock_digest
            cb = mock.Mock(side_effect=Exception('Argh!'))

            t = torf.Torrent(content_path)
            with pytest.raises(Exception) as e:
                t.generate(callback=cb)

            assert str(e.value) == 'Argh!'
            cb.assert_called_once_with(t, Path(content_path), 1, t.pieces)
            # The pool of hashers should be stopped before all pieces are hashed
            assert sha1_mock.call_count < t.pieces
            assert not t.is_ready
