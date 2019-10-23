import torf

import pytest
from unittest import mock
import math
from hashlib import md5
import os
import shutil


def test_generate_with_nonexisting_path(singlefile_content):
    content_path = singlefile_content.path + '.deletable'
    shutil.copyfile(singlefile_content.path, content_path)
    t = torf.Torrent(content_path)
    os.remove(content_path)

    with pytest.raises(torf.PathNotFoundError) as excinfo:
        t.generate()
    assert excinfo.match(f'^{content_path}: No such file or directory$')


def test_generate_with_one_unreadable(multifile_content):
    t = torf.Torrent(multifile_content.path)

    old_mode = os.stat(multifile_content.path).st_mode
    try:
        os.chmod(multifile_content.path, mode=0o222)

        with pytest.raises(torf.ReadError) as excinfo:
            t.generate()
            assert excinfo.match(f'^{multifile_content.path}.*: Permission denied$')
    finally:
        os.chmod(multifile_content.path, mode=old_mode)


def check_metainfo(content):
    # Without md5 included
    t = torf.Torrent(content.path, include_md5=False)
    t.piece_size = content.exp_metainfo['info']['piece length']
    t.generate()
    assert t.metainfo['info']['piece length'] == content.exp_metainfo['info']['piece length']
    assert t.metainfo['info']['pieces'] == content.exp_metainfo['info']['pieces']

    # 'md5sum' shouldn't be available
    if 'length' in t.metainfo['info']:   # Singlefile
        assert 'md5sum' not in t.metainfo['info']
    elif 'files' in t.metainfo['info']:  # Multifile
        for fileinfo in t.metainfo['info']['files']:
            assert 'md5sum' not in fileinfo

    # With md5 included
    t = torf.Torrent(content.path,
                     piece_size=content.exp_metainfo['info']['piece length'])
    t.include_md5 = True
    t.generate()
    assert t.metainfo['info']['piece length'] == content.exp_metainfo['info']['piece length']
    assert t.metainfo['info']['pieces'] == content.exp_metainfo['info']['pieces']

    # 'md5sum' should be available
    if 'length' in t.metainfo['info']:   # Singlefile
        assert 'md5sum' in t.metainfo['info']
        with open(t.path, 'rb') as f:
            file_content = f.read()
        exp_md5sum = md5(file_content).hexdigest()
        assert t.metainfo['info']['md5sum'] == exp_md5sum

    elif 'files' in t.metainfo['info']:  # Multifile
        for fileinfo in t.metainfo['info']['files']:
            assert 'md5sum' in fileinfo
            filepath = os.path.join(t.path, os.path.join(*fileinfo['path']))
            with open(filepath, 'rb') as f:
                file_content = f.read()
            exp_md5sum = md5(file_content).hexdigest()
            assert fileinfo['md5sum'] == exp_md5sum

def test_generate_with_singlefile(singlefile_content):
    check_metainfo(singlefile_content)

def test_generate_with_multifile(multifile_content):
    check_metainfo(multifile_content)


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

def test_singlefile_generate_with_callback(singlefile_content):
    assert_callback_called(singlefile_content)

def test_multifile_generate_with_callback(multifile_content):
    assert_callback_called(multifile_content)


def assert_callback_called_at_interval(torrent, monkeypatch):
    import time
    monkeypatch.setattr(time, 'time',
                        mock.MagicMock(side_effect=range(100)))

    t = torf.Torrent(torrent.path)
    cb = mock.Mock(return_value=None)
    interval = 3
    success = t.generate(callback=cb, interval=interval)
    assert success

    # Compare number of callback calls
    number_of_pieces = math.ceil(t.size / t.piece_size)
    exp_call_count = math.ceil(number_of_pieces / interval)
    if t.mode == 'singlefile':
        # The singlefile generator makes one semi-superfluous call to report the
        # generation of the MD5 sum
        exp_call_count += 1
    assert cb.call_count == exp_call_count

def test_singlefile_generate_with_callback_interval(singlefile_content, monkeypatch):
    assert_callback_called_at_interval(singlefile_content, monkeypatch)

def test_multifile_generate_with_callback_interval(multifile_content, monkeypatch):
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
