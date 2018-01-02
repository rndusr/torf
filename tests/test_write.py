import torf

import pytest
import io
from bencoder import bdecode
import time
import os


def test_write_without_permission(generated_singlefile_torrent, tmpdir):
    d = tmpdir.mkdir('test_dir')
    d.chmod(mode=0o444)
    f = d.join('a.torrent')

    with pytest.raises(torf.WriteError) as excinfo:
        generated_singlefile_torrent.write(str(f))
    assert excinfo.match(f'Permission denied: {str(f)!r}')


def test_write_to_existing_file(generated_singlefile_torrent, tmpdir):
    f = tmpdir.join('a.torrent')
    f.write('any content')

    with pytest.raises(torf.WriteError) as excinfo:
        generated_singlefile_torrent.write(str(f))
    assert excinfo.match(f'File exists: {str(f)!r}')

    generated_singlefile_torrent.write(str(f), overwrite=True)
    bytes_written = open(str(f), 'rb').read()
    bytes_expected = generated_singlefile_torrent.dump()


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


def test_existing_file_is_unharmed_if_dump_fails(generated_singlefile_torrent, tmpdir):
    f = tmpdir.join('a.torrent')
    f.write('something')

    now = int(time.time())
    del generated_singlefile_torrent.metainfo['info']['length']
    with pytest.raises(torf.MetainfoError):
        generated_singlefile_torrent.write(str(f), overwrite=True)
    old_content = open(str(f), 'r').read()
    assert old_content == 'something'
