import torf

import pytest
from unittest import mock
import math
import os
import base64
from collections import defaultdict

piece_size = torf.Torrent.piece_size_min
def _generate_file_sizes(*piece_counts):
    for piece_count in piece_counts:
        yield piece_size * piece_count
        yield (piece_size * piece_count) - 1
        yield (piece_size * piece_count) + 1


def test_no_path():
    t = torf.Torrent()
    with pytest.raises(RuntimeError) as e:
        t.generate()
    assert str(e.value) == 'generate() called with no path specified'


def test_with_empty_file(create_content_file):
    # Create content so we can set path
    content_path = create_content_file('file.jpg', '<image data>')
    t = torf.Torrent(content_path)
    content_path.write_text('')
    with pytest.raises(torf.PathEmptyError) as e:
        t.generate()
    assert str(e.value) == f'{t.path}: Empty file'


def test_with_empty_directory(create_content_dir):
    # Create content so we can set path
    content_path = create_content_dir('empty', ('a file', '<data>'))
    t = torf.Torrent(content_path)
    (content_path / 'a file').unlink()
    with pytest.raises(torf.PathEmptyError) as e:
        t.generate()
    assert str(e.value) == f'{t.path}: Empty directory'


def test_nonexisting_path(create_content_file):
    content_path = create_content_file('file.jpg', '<image data>')
    t = torf.Torrent(content_path)
    content_path.unlink()
    with pytest.raises(torf.PathNotFoundError) as e:
        t.generate()
    assert str(e.value) == f'{content_path}: No such file or directory'


def test_unreadable_basedir_in_multifile_torrent(create_content_dir):
    content_path = create_content_dir('content',
                                      ('a.jpg', '<image data>'),
                                      ('b.jpg', '<image data>'),
                                      ('c.jpg', '<image data>'))
    t = torf.Torrent(content_path)
    old_mode = os.stat(content_path).st_mode
    try:
        os.chmod(content_path, mode=0o222)
        with pytest.raises(torf.ReadError) as e:
            t.generate()
        assert str(e.value) == f'{content_path}: Permission denied'
    finally:
        os.chmod(content_path, mode=old_mode)


def test_unreadable_file_in_multifile_torrent(create_content_dir):
    content_path = create_content_dir('content',
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


def test_generate_singlefile_torrent_sets_correct_metainfo(create_content_file, random_seed):
    with random_seed(0):
        content_path = create_content_file('file.jpg', torf.Torrent.piece_size_min * 10.123)
    # exp_* values come from these commands:
    # $ mktorrent -l 15 /tmp/pytest-of-*/pytest-current/test_generate_singlefile_torre0/file.jpg
    # $ btcheck -i file.jpg.torrent -n | grep Hash
    # $ python3 -c "from flatbencode import decode; print(decode(open('file.jpg.torrent', 'rb').read())[b'info'][b'pieces'])"
    exp_infohash = '55d85dda866f823eb23b6d9a0cb555af9851885b'
    exp_pieces = (b"F\n\x94Oc\x9c'\x15\x9cdC\x9e\xe7\x03\xe2:\xfc\xdf\xde\xf0\xbd'\xc8"
                  b".v8\xfe~\xeav\xe8\xd5@\x08\x1d\xb2\x05\x1fjK\xb2\xdbI\xd75\x10\x14"
                  b"\x8dn!'B\xcd\x1b;\x14\xda+\x1e1lB\x87\xca\xbbD\xf9\x98\xf1\x00e 7M"
                  b"\xe4\xf3\x05qa\xcaV\x0e\xe2`\xc0\x07\xf3\xf6\xb5\xed]\x1e\xea9F\x1b"
                  b"\xd1\xbd\x8cL\x1cy\x82\xbe\x0b%\xf3\xa2\xa4\x9a\xd3\xd6\x90\x9f\xd7\x10\x7f\x93\x87Q")
    _check_metainfo(content_path, 2**15, exp_infohash, exp_pieces)

def test_generate_multifile_torrent_sets_correct_metainfo(create_content_dir, random_seed):
    with random_seed(0):
        content_path = create_content_dir('content',
                                          ('a.jpg', torf.Torrent.piece_size_min * 1.123),
                                          ('b.jpg', torf.Torrent.piece_size_min * 2.456),
                                          ('c.jpg', torf.Torrent.piece_size_min * 3.789))
    # exp_* values come from these commands:
    # $ mktorrent -l 15 /tmp/pytest-of-*/pytest-current/test_generate_multifile_torren0/content
    # $ btcheck -i content.torrent -n | grep Hash
    # $ python3 -c "from flatbencode import decode; print(decode(open('content.torrent', 'rb').read())[b'info'][b'pieces'])"
    exp_infohash = '0426d41d73433d2813738d281436be52ffd82df4'
    exp_pieces = (b"F\n\x94Oc\x9c'\x15\x9cdC\x9e\xe7\x03\xe2:\xfc\xdf\xde\xf0"
                  b"\xbd'\xc8.v8\xfe~\xeav\xe8\xd5@\x08\x1d\xb2\x05\x1fjK\xb2"
                  b"\xdbI\xd75\x10\x14\x8dn!'B\xcd\x1b;\x14\xda+\x1e1\xb6G\xc0"
                  b"\xb9\xd0x\xf7\xd0\xcb(\xed]@&F\xc4\x11\x81\x9f\x8d")
    _check_metainfo(content_path, 2**15, exp_infohash, exp_pieces)

def _check_metainfo(content_path, piece_size, exp_infohash, exp_pieces):
    exp_hashes = tuple(exp_pieces[i:i+20]
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


@pytest.mark.parametrize('file_size', _generate_file_sizes(1, 2, 5, 8))
def test_callback_is_called_with_singlefile_torrent(file_size, create_content_file):
    content_path = create_content_file('file.jpg', file_size)
    t = torf.Torrent(content_path)
    cb = mock.Mock(return_value=None)
    success = t.generate(callback=cb, interval=0)
    assert success is True
    assert cb.call_count == t.pieces
    exp_call_args_list = [mock.call(t, str(content_path), i, t.pieces)
                          for i in range(1, t.pieces+1)]
    for call in cb.call_args_list:
        exp_call_args_list.remove(call)
    assert exp_call_args_list == []


@pytest.mark.parametrize('file_size_a', _generate_file_sizes(1, 2, 6, 7))
@pytest.mark.parametrize('file_size_b', _generate_file_sizes(1, 2, 4, 6))
def test_callback_is_called_with_multifile_torrent(file_size_a, file_size_b, create_content_dir):
    content_path = create_content_dir('content',
                                      ('a.jpg', file_size_a),
                                      ('b.jpg', file_size_b))
    file_a_piece_count = file_size_a / create_content_dir.piece_size
    file_b_piece_count = file_size_b / create_content_dir.piece_size
    print('a.jpg:', file_size_a, '-', file_a_piece_count, 'pieces')
    print('b.jpg:', file_size_b, '-', file_b_piece_count, 'pieces')

    exp_pieces_done = 1
    seen_filepaths = defaultdict(lambda: 0)
    def assert_cb_args(torrent, filepath, pieces_done, pieces_total):
        nonlocal exp_pieces_done
        assert torrent is t
        assert pieces_done == exp_pieces_done
        exp_pieces_done += 1
        seen_filepaths[filepath] += 1

    t = torf.Torrent(content_path)
    cb = mock.Mock(side_effect=assert_cb_args)
    success = t.generate(callback=cb, interval=0)
    assert success is True

    print('Callback calls:')
    for call in cb.call_args_list:
        print(call)
    assert cb.call_count == t.pieces

    print('Seen filepaths:', dict(seen_filepaths))
    exp_seen_filepaths_a = math.floor(file_a_piece_count)
    exp_seen_filepaths_b = math.ceil(
        file_b_piece_count +
        file_a_piece_count - math.floor(file_a_piece_count)
    )
    assert seen_filepaths[str(content_path / 'a.jpg')] == exp_seen_filepaths_a
    assert seen_filepaths[str(content_path / 'b.jpg')] == exp_seen_filepaths_b


def assert_callback_is_called_at_intervals(content_path, monkeypatch):
    t = torf.Torrent(content_path)
    import time
    monkeypatch.setattr(time, 'monotonic',
                        mock.Mock(side_effect=range(100)))

    for interval in (1, 2, 3):
        cb = mock.Mock(return_value=None)
        success = t.generate(callback=cb, interval=interval)
        assert success is True

        if interval > 1 and t.pieces % interval == 0:
            exp_call_count = t.pieces // interval + t.pieces % interval + 1
            print(f'exp_call_count = {t.pieces} // {interval} + {t.pieces} % {interval} + 1 = {exp_call_count}')
        else:
            exp_call_count = t.pieces // interval + t.pieces % interval
            print(f'exp_call_count = {t.pieces} // {interval} + {t.pieces} % {interval} = {exp_call_count}')

        print('Actual calls:', cb.call_count)
        assert cb.call_count == exp_call_count

@pytest.mark.parametrize('file_size', _generate_file_sizes(1, 2, 9, 10))
def test_callback_is_called_at_interval_with_singlefile_torrent(file_size, create_content_file, monkeypatch):
    content_path = create_content_file('file.jpg', file_size)
    assert_callback_is_called_at_intervals(content_path, monkeypatch)

@pytest.mark.parametrize('file_size_b', _generate_file_sizes(1, 2, 5, 6))
@pytest.mark.parametrize('file_size_a', _generate_file_sizes(1, 2, 4, 7))
def test_callback_is_called_at_interval_with_multifile_torrent(file_size_a, file_size_b, create_content_dir, monkeypatch):
    content_path = create_content_dir('content',
                                      ('a.jpg', file_size_a),
                                      ('b.jpg', file_size_b))
    assert_callback_is_called_at_intervals(content_path, monkeypatch)


@pytest.mark.parametrize('file_size', _generate_file_sizes(4, 7))
def test_callback_cancels(file_size, create_content_file):
    content_path = create_content_file('file.jpg', file_size)

    def maybe_cancel(torrent, filepath, pieces_done, pieces_total):
        print(f'{pieces_done} / {pieces_total}')
        if pieces_done / pieces_total > 0.5:
            return 'STOP THE PRESSES!'
    cb = mock.Mock(side_effect=maybe_cancel)

    t = torf.Torrent(content_path)
    success = t.generate(callback=cb)
    assert success is False
    assert cb.call_count == math.floor(t.pieces * 0.5) + 1


def test_callback_raises_exception(create_content_file):
    content_path = create_content_file(
        'file.jpg', create_content_file.random_size(min_pieces=100, max_pieces=200))

    with mock.patch('torf._generate.sha1') as sha1_mock:
        def mock_digest():
            return b'\x00' * 20
        sha1_mock.return_value.digest.side_effect = mock_digest
        cb = mock.Mock(side_effect=Exception('Argh!'))
        t = torf.Torrent(content_path)
        with pytest.raises(Exception) as e:
            t.generate(callback=cb)
        assert str(e.value) == 'Argh!'
        cb.assert_called_once_with(t, str(content_path), 1, t.pieces)
        # The pool of hashers should be stopped before all pieces are hashed
        assert sha1_mock.call_count < t.pieces
        assert not t.is_ready
