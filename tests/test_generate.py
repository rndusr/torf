import torf

import pytest
from unittest import mock
import math
import os
import shutil


def test_no_path():
    t = torf.Torrent()
    with pytest.raises(RuntimeError) as excinfo:
        t.generate()
    assert excinfo.match(r'^generate\(\) called with no path specified$')


def test_with_empty_path(tmpdir):
    content_path = tmpdir.mkdir('empty')
    # Create content so we can set path
    content_file = content_path.join('file.jpg')
    content_file.write('foo')
    t = torf.Torrent(content_path)
    content_file.write('')

    with pytest.raises(torf.PathEmptyError) as excinfo:
        t.generate()
    assert excinfo.match(f'^{str(t.path)}: Empty directory$')


def test_nonexisting_path(singlefile_content):
    content_path = singlefile_content.path + '.deletable'
    shutil.copyfile(singlefile_content.path, content_path)
    t = torf.Torrent(content_path)
    os.remove(content_path)

    with pytest.raises(torf.PathNotFoundError) as excinfo:
        t.generate()
    assert excinfo.match(f'^{content_path}: No such file or directory$')


def test_unreadable_file_in_multifile_torrent(multifile_content):
    t = torf.Torrent(multifile_content.path)

    old_mode = os.stat(multifile_content.path).st_mode
    try:
        os.chmod(multifile_content.path, mode=0o222)

        with pytest.raises(torf.ReadError) as excinfo:
            t.generate()
            assert excinfo.match(f'^{multifile_content.path}.*: Permission denied$')
    finally:
        os.chmod(multifile_content.path, mode=old_mode)


def check_metainfo(content, tmpdir):
    t = torf.Torrent(content.path)
    t.piece_size = content.exp_metainfo['info']['piece length']
    t.generate()
    t.write(tmpdir.join('torf.torrent'), overwrite=True)
    assert t.metainfo['info']['piece length'] == content.exp_metainfo['info']['piece length']
    assert t.metainfo['info']['pieces'] == content.exp_metainfo['info']['pieces']
    assert t.infohash == content.exp_attrs.infohash
    assert t.infohash_base32 == content.exp_attrs.infohash_base32

def test_generate_with_singlefile_torrent(singlefile_content, tmpdir):
    check_metainfo(singlefile_content, tmpdir)

def test_generate_with_multifile_torrent(multifile_content, tmpdir):
    check_metainfo(multifile_content, tmpdir)


def assert_callback_called(torrent):
    t = torf.Torrent(torrent.path)
    cb = mock.Mock()
    cb.return_value = None
    success = t.generate(callback=cb, interval=0)
    assert success

    # Compare number of callback calls
    number_of_pieces = math.ceil(t.size / t.piece_size)
    exp_call_count = pytest.approx(number_of_pieces, abs=1)
    assert cb.call_count == exp_call_count

    # Compare arguments without filepaths (too complex for me)
    stripped_call_args_list = [mock.call(args[0][0], args[0][2], args[0][3])
                               for args in cb.call_args_list]
    exp_call_args_list = [mock.call(t, i, number_of_pieces)
                          for i in range(1, number_of_pieces+1)]
    # There can be slightly more actual calls than expected calls or vice versa
    call_num = min(len(stripped_call_args_list), len(exp_call_args_list))
    stripped_call_args_list = stripped_call_args_list[:call_num]
    exp_call_args_list = exp_call_args_list[:call_num]
    assert stripped_call_args_list == exp_call_args_list

    # Make sure that the expected filepaths were reported to callback
    processed_filepaths = []
    for args in cb.call_args_list:
        filepath = args[0][1]
        if filepath not in processed_filepaths:
            processed_filepaths.append(filepath)
    exp_filepaths = list(t.filepaths)
    assert processed_filepaths == exp_filepaths

def test_callback_is_called_with_singlefile_torrent(singlefile_content):
    assert_callback_called(singlefile_content)

def test_callback_is_called_with_multifile_torrent(multifile_content):
    assert_callback_called(multifile_content)


def assert_callback_called_at_interval(torrent, monkeypatch):
    import time
    monkeypatch.setattr(time, 'monotonic',
                        mock.MagicMock(side_effect=range(100)))

    t = torf.Torrent(torrent.path)
    cb = mock.Mock(return_value=None)
    interval = 3
    success = t.generate(callback=cb, interval=interval)
    assert success

    # Compare number of callback calls
    number_of_pieces = math.ceil(t.size / t.piece_size)
    exp_call_count = math.ceil(number_of_pieces / interval)
    assert cb.call_count == exp_call_count

def test_callback_is_called_at_interval_with_singlefile_torrent(singlefile_content, monkeypatch):
    assert_callback_called_at_interval(singlefile_content, monkeypatch)

def test_callback_is_called_at_interval_with_multifile_torrent(multifile_content, monkeypatch):
    assert_callback_called_at_interval(multifile_content, monkeypatch)


def test_callback_cancels(multifile_content):
    hashed_pieces = []
    def callback(torrent, filepath, pieces_done, pieces_total):
        hashed_pieces.append(pieces_done)
        # Cancel after 50 % of the pieces are hashed
        if pieces_done / pieces_total > 0.5:
            return 'STOP THE PRESSES!'

    t = torf.Torrent(multifile_content.path)
    success = t.generate(callback=callback)
    assert success is False
    assert hashed_pieces[-1] > 0
    assert hashed_pieces[-1] < t.pieces


def test_callback_raises_exception(singlefile_content):
    with mock.patch('torf._generate.sha1') as sha1_mock:
        cb = mock.MagicMock(side_effect=Exception('Argh!'))
        t = torf.Torrent(singlefile_content.path)
        with pytest.raises(Exception) as excinfo:
            t.generate(callback=cb)
        assert excinfo.match(f'^Argh!$')
        cb.assert_called_once_with(t, singlefile_content.path, 1, singlefile_content.exp_attrs.pieces)
        # The pool of hashers should be stopped before all pieces are hashed
        assert sha1_mock.call_count < singlefile_content.exp_attrs.pieces
        # TODO: Add a similar test for the reader.
        assert not t.is_ready
