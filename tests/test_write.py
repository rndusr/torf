import torf

import pytest
import io
from bencoder import bdecode
import time
import os
from unittest.mock import MagicMock


def test_successful_write(generated_singlefile_torrent, tmpdir):
    f = tmpdir.join('a.torrent')

    generated_singlefile_torrent.write(str(f))
    bytes_written = open(str(f), 'rb').read()
    bytes_expected = generated_singlefile_torrent.dump()
    assert bytes_written == bytes_expected


def test_write_with_creation_date(generated_singlefile_torrent, tmpdir):
    f = tmpdir.join('a.torrent')

    now = int(time.time())
    generated_singlefile_torrent.creation_date = now
    generated_singlefile_torrent.write(str(f))
    metainfo = bdecode(open(str(f), 'rb').read())
    assert metainfo[b'creation date'] == now


def test_write_to_file_without_permission(generated_singlefile_torrent, tmpdir):
    d = tmpdir.mkdir('test_dir')
    d.chmod(mode=0o444)
    f = d.join('a.torrent')

    with pytest.raises(torf.WriteError) as excinfo:
        generated_singlefile_torrent.write(str(f))
    assert excinfo.match(f'^{str(f)}: Permission denied$')


def test_write_to_existing_file(generated_singlefile_torrent, tmpdir):
    f = tmpdir.join('a.torrent')
    f.write('any content')

    with pytest.raises(torf.WriteError) as excinfo:
        generated_singlefile_torrent.write(str(f))
    assert excinfo.match(f'^{str(f)}: File exists$')

    generated_singlefile_torrent.write(str(f), overwrite=True)
    bytes_written = open(str(f), 'rb').read()
    bytes_expected = generated_singlefile_torrent.dump()
    assert bytes_written == bytes_expected


def test_existing_file_is_unharmed_if_dump_fails(generated_singlefile_torrent, tmpdir):
    f = tmpdir.join('a.torrent')
    f.write('something')
    del generated_singlefile_torrent.metainfo['info']['length']

    with pytest.raises(torf.MetainfoError):
        generated_singlefile_torrent.write(str(f), overwrite=True)
    old_content = open(str(f), 'r').read()
    assert old_content == 'something'


def test_new_file_is_not_created_if_dump_fails(generated_singlefile_torrent, tmpdir):
    f = tmpdir.join('a.torrent')
    del generated_singlefile_torrent.metainfo['info']['length']

    with pytest.raises(torf.MetainfoError):
        generated_singlefile_torrent.write(str(f))
    assert not os.path.exists(f)


def test_overwriting_larger_torrent_file_truncates_first(generated_singlefile_torrent, tmpdir):
    f = tmpdir.join('large.file')
    f.write('x' * 1000000)
    assert os.path.getsize(f) == 1e6

    generated_singlefile_torrent.write(str(f), overwrite=True)
    assert os.path.exists(f)
    assert os.path.getsize(f) < 1e6
    assert torf.Torrent.read(str(f)).name == os.path.basename(generated_singlefile_torrent.path)
