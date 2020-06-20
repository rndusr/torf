import os
import time

import flatbencode as bencode
import pytest

import torf


def test_successful_write(generated_singlefile_torrent, tmp_path):
    f = tmp_path / 'a.torrent'
    generated_singlefile_torrent.write(f)
    bytes_written = open(f, 'rb').read()
    bytes_expected = generated_singlefile_torrent.dump()
    assert bytes_written == bytes_expected


def test_write_with_creation_date(generated_singlefile_torrent, tmp_path):
    f = tmp_path / 'a.torrent'
    now = int(time.time())
    generated_singlefile_torrent.creation_date = now
    generated_singlefile_torrent.write(f)
    metainfo = bencode.decode(open(f, 'rb').read())
    assert metainfo[b'creation date'] == now


def test_write_to_file_without_permission(generated_singlefile_torrent, tmp_path):
    (tmp_path / 'test_dir').mkdir()
    (tmp_path / 'test_dir').chmod(0o444)
    (tmp_path / 'test_dir').chmod(0o444)
    with pytest.raises(torf.WriteError) as excinfo:
        generated_singlefile_torrent.write(tmp_path / 'test_dir' / 'a.torrent')
    assert excinfo.match(f'^{tmp_path / "test_dir" / "a.torrent"}: Permission denied$')


def test_write_to_existing_file(generated_singlefile_torrent, tmp_path):
    (tmp_path / 'a.torrent').write_text('something')

    with pytest.raises(torf.WriteError) as excinfo:
        generated_singlefile_torrent.write(tmp_path / 'a.torrent')
    assert excinfo.match(f'^{tmp_path / "a.torrent"}: File exists$')

    generated_singlefile_torrent.write(tmp_path / 'a.torrent', overwrite=True)
    bytes_written = open(tmp_path / 'a.torrent', 'rb').read()
    bytes_expected = generated_singlefile_torrent.dump()
    assert bytes_written == bytes_expected


def test_existing_file_is_unharmed_if_dump_fails(generated_singlefile_torrent, tmp_path):
    (tmp_path / 'a.torrent').write_text('something')
    del generated_singlefile_torrent.metainfo['info']['length']

    with pytest.raises(torf.MetainfoError):
        generated_singlefile_torrent.write(tmp_path / 'a.torrent', overwrite=True)
    old_content = open(tmp_path / 'a.torrent', 'r').read()
    assert old_content == 'something'


def test_new_file_is_not_created_if_dump_fails(generated_singlefile_torrent, tmp_path):
    f = tmp_path / 'a.torrent'
    del generated_singlefile_torrent.metainfo['info']['length']

    with pytest.raises(torf.MetainfoError):
        generated_singlefile_torrent.write(f)
    assert not os.path.exists(f)


def test_overwriting_larger_torrent_file_truncates_first(generated_singlefile_torrent, tmp_path):
    f = (tmp_path / 'large.file')
    f.write_text('x' * 1000000)
    assert os.path.getsize(f) == 1e6

    generated_singlefile_torrent.write(str(f), overwrite=True)
    assert os.path.exists(f)
    assert os.path.getsize(f) < 1e6
    assert torf.Torrent.read(str(f)).name == os.path.basename(generated_singlefile_torrent.path)
