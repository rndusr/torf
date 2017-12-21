import torf

import pytest
import io
from bencoder import bdecode
import time

def test_write_to_closed_file(generated_singlefile_torrent):
    stream = io.BytesIO()
    stream.close()
    with pytest.raises(RuntimeError) as excinfo:
        generated_singlefile_torrent.write(stream)
    assert excinfo.match(f'{stream!r} is closed')

def test_write_to_readonly_file(generated_singlefile_torrent):
    stream = io.BufferedReader(io.BytesIO())
    with pytest.raises(RuntimeError) as excinfo:
        generated_singlefile_torrent.write(stream)
    assert excinfo.match(f'{stream!r} is opened in read-only mode')

def test_write_to_textmode_file(generated_singlefile_torrent):
    stream = io.StringIO()
    with pytest.raises(RuntimeError) as excinfo:
        generated_singlefile_torrent.write(stream)
    assert excinfo.match(f'{stream!r} is not opened in binary mode')

def test_successful_write(generated_singlefile_torrent):
    stream = io.BytesIO()
    generated_singlefile_torrent.write(stream)
    bytes_written = stream.getvalue()
    bytes_expected = generated_singlefile_torrent.dump()
    assert bytes_written == bytes_expected

def test_write_with_creation_date(generated_singlefile_torrent):
    stream = io.BytesIO()
    now = int(time.time())
    generated_singlefile_torrent.creation_date = now
    generated_singlefile_torrent.write(stream)
    metainfo = bdecode(stream.getvalue())
    assert metainfo[b'creation date'] == now
