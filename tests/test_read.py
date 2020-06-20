import io
from collections import OrderedDict
from datetime import datetime
from hashlib import sha1
from pathlib import Path

import flatbencode as bencode
import pytest

import torf


def test_non_bencoded_data():
    fo = io.BytesIO(b'not valid bencoded data')
    with pytest.raises(torf.BdecodeError) as excinfo:
        torf.Torrent.read_stream(fo)
    assert excinfo.match('^Invalid metainfo format$')


def test_unreadable_stream():
    class Unreadable(io.BytesIO):
        def read(self, *args, **kwargs):
            raise OSError('Refusing to read')
    fo = Unreadable(b'foo')
    with pytest.raises(torf.ReadError) as excinfo:
        torf.Torrent.read_stream(fo)
    assert excinfo.match('^Unable to read$')


def test_validate_when_reading_stream(valid_singlefile_metainfo):
    del valid_singlefile_metainfo[b'info'][b'name']
    fo = io.BytesIO(bencode.encode(valid_singlefile_metainfo))

    with pytest.raises(torf.MetainfoError) as excinfo:
        torf.Torrent.read_stream(fo, validate=True)
    assert excinfo.match(r"^Invalid metainfo: Missing 'name' in \['info'\]$")
    fo.seek(0)
    t = torf.Torrent.read_stream(fo, validate=False)
    assert isinstance(t, torf.Torrent)

def test_validate_when_reading_file(tmp_path, valid_singlefile_metainfo):
    del valid_singlefile_metainfo[b'info'][b'length']
    torrent_file = tmp_path / 'invalid.torrent'
    with open(torrent_file, 'wb') as f:
        f.write(bencode.encode(valid_singlefile_metainfo))

    with pytest.raises(torf.MetainfoError) as excinfo:
        torf.Torrent.read(torrent_file, validate=True)
    assert excinfo.match("^Invalid metainfo: Missing 'length' or 'files' in 'info'$")
    t = torf.Torrent.read(torrent_file, validate=False)
    assert isinstance(t, torf.Torrent)


def test_successful_read(valid_singlefile_metainfo):
    fo = io.BytesIO(bencode.encode(valid_singlefile_metainfo))
    t = torf.Torrent.read_stream(fo)
    assert t.path is None
    assert t.files == (Path(str(valid_singlefile_metainfo[b'info'][b'name'], encoding='utf-8')),)
    assert t.filepaths == ()
    assert t.name == str(valid_singlefile_metainfo[b'info'][b'name'], encoding='utf-8')
    assert t.size == valid_singlefile_metainfo[b'info'][b'length']
    assert t.infohash == sha1(bencode.encode(valid_singlefile_metainfo[b'info'])).hexdigest()
    assert t.comment == str(valid_singlefile_metainfo[b'comment'], encoding='utf-8')
    assert t.creation_date == datetime.fromtimestamp(valid_singlefile_metainfo[b'creation date'])
    assert t.created_by == str(valid_singlefile_metainfo[b'created by'], encoding='utf-8')
    assert t.private is bool(valid_singlefile_metainfo[b'info'][b'private'])
    assert t.piece_size == valid_singlefile_metainfo[b'info'][b'piece length']


def test_single_tracker(valid_singlefile_metainfo):
    valid_singlefile_metainfo[b'announce'] = b'http://lonelyhost/announce'
    valid_singlefile_metainfo.pop(b'announce-list', None)
    fo = io.BytesIO(bencode.encode(valid_singlefile_metainfo))
    t = torf.Torrent.read_stream(fo)
    assert t.trackers == [[str(valid_singlefile_metainfo[b'announce'], encoding='utf-8')]]

def test_multiple_trackers(valid_singlefile_metainfo):
    valid_singlefile_metainfo[b'announce-list'] = [[b'http://localhost', b'http://foohost'],
                                                   [b'http://bazhost']]
    valid_singlefile_metainfo.pop(b'announce', None)
    fo = io.BytesIO(bencode.encode(valid_singlefile_metainfo))
    t = torf.Torrent.read_stream(fo)
    assert t.trackers == [[str(url, encoding='utf-8') for url in tier] for tier
                          in valid_singlefile_metainfo[b'announce-list']]


def test_validate_nondict():
    data = b'3:foo'
    with pytest.raises(torf.BdecodeError) as excinfo:
        torf.Torrent.read_stream(io.BytesIO(data), validate=True)
    assert excinfo.match("^Invalid metainfo format$")

    with pytest.raises(torf.BdecodeError) as excinfo:
        torf.Torrent.read_stream(io.BytesIO(data), validate=False)
    assert excinfo.match("^Invalid metainfo format$")

def test_validate_missing_info():
    data = OrderedDict([(b'foo', b'bar')])
    with pytest.raises(torf.MetainfoError) as excinfo:
        torf.Torrent.read_stream(io.BytesIO(bencode.encode(data)), validate=True)
    assert excinfo.match(r"^Invalid metainfo: Missing 'info'$")

    t = torf.Torrent.read_stream(io.BytesIO(bencode.encode(data)), validate=False)
    assert t.metainfo == {'foo': 'bar', 'info': {}}

def test_validate_info_not_a_dictionary():
    data = OrderedDict([(b'info', 1)])

    with pytest.raises(torf.MetainfoError) as excinfo:
        torf.Torrent.read_stream(io.BytesIO(bencode.encode(data)), validate=True)
    assert excinfo.match(r"^Invalid metainfo: \['info'\] must be dict, not int: 1$")

    with pytest.raises(torf.MetainfoError) as excinfo:
        torf.Torrent.read_stream(io.BytesIO(bencode.encode(data)), validate=False)
    assert excinfo.match(r"^Invalid metainfo: \['info'\] must be dict, not int: 1$")

def test_validate_missing_pieces():
    data = OrderedDict([(b'info', {b'name': b'Foo',
                                   b'piece length': 1024})])
    fo = io.BytesIO(bencode.encode(data))
    with pytest.raises(torf.MetainfoError) as excinfo:
        torf.Torrent.read_stream(fo, validate=True)
    assert excinfo.match(r"^Invalid metainfo: Missing 'pieces' in \['info'\]$")


def test_read_nonstandard_data_without_validation():
    data = OrderedDict([
        (b'foo', b'bar'),
        (b'number', 17),
        (b'list', [1, b'two']),
        (b'dict', OrderedDict([
            (b'yes', 1),
            (b'no', 0),
        ]))
    ])
    fo = io.BytesIO(bencode.encode(data))
    t = torf.Torrent.read_stream(fo, validate=False)
    assert t.metainfo['foo'] == 'bar'
    assert t.metainfo['number'] == 17
    assert t.metainfo['list'] == [1, 'two']
    assert t.metainfo['dict'] == {'yes': 1, 'no': 0}
    assert t.metainfo['info'] == {}

def test_read_from_unreadable_file(valid_singlefile_metainfo, tmp_path):
    f = (tmp_path / 'a.torrent')
    f.write_bytes(bencode.encode(valid_singlefile_metainfo))
    f.chmod(mode=0o222)
    with pytest.raises(torf.ReadError) as excinfo:
        torf.Torrent.read(str(f))
    assert excinfo.match(f'^{f}: Permission denied$')

def test_read_from_invalid_file(tmp_path):
    f = tmp_path / 'a.torrent'
    f.write_bytes(b'this is not metainfo')
    with pytest.raises(torf.BdecodeError) as excinfo:
        torf.Torrent.read(f)
    assert excinfo.match(f'^{f}: Invalid torrent file format$')

def test_read_from_nonexisting_file(tmp_path):
    f = tmp_path / 'a.torrent'
    with pytest.raises(torf.ReadError) as excinfo:
        torf.Torrent.read(f)
    assert excinfo.match(f'^{f}: No such file or directory$')

def test_read_from_proper_torrent_file(valid_multifile_metainfo, tmp_path):
    f = tmp_path / 'a.torrent'
    f.write_bytes(bencode.encode(valid_multifile_metainfo))
    t = torf.Torrent.read(f)
    exp_info = valid_multifile_metainfo[b'info']
    assert t.path is None
    assert t.files == tuple(Path(str(b'/'.join([exp_info[b'name']] + f[b'path']), encoding='utf-8'))
                            for f in exp_info[b'files'])
    assert t.filepaths == ()
    assert t.name == str(exp_info[b'name'], encoding='utf-8')
    assert t.size == sum(f[b'length'] for f in exp_info[b'files'])
    assert t.infohash == sha1(bencode.encode(exp_info)).hexdigest()
    assert t.comment == str(valid_multifile_metainfo[b'comment'], encoding='utf-8')
    assert t.creation_date == datetime.fromtimestamp(valid_multifile_metainfo[b'creation date'])
    assert t.created_by == str(valid_multifile_metainfo[b'created by'], encoding='utf-8')
    assert t.private is bool(exp_info[b'private'])
    assert t.piece_size == exp_info[b'piece length']


def test_reading_converts_private_flag_to_bool(tmp_path, valid_singlefile_metainfo):
    valid_singlefile_metainfo[b'info'][b'private'] = 1
    fo = io.BytesIO(bencode.encode(valid_singlefile_metainfo))
    torrent = torf.Torrent.read_stream(fo)
    assert torrent.metainfo['info']['private'] is True

    valid_singlefile_metainfo[b'info'][b'private'] = 0
    fo = io.BytesIO(bencode.encode(valid_singlefile_metainfo))
    torrent = torf.Torrent.read_stream(fo)
    assert torrent.metainfo['info']['private'] is False

def test_reading_torrent_without_private_flag(tmp_path, valid_singlefile_metainfo):
    valid_singlefile_metainfo[b'info'][b'private'] = 1
    fo = io.BytesIO(bencode.encode(valid_singlefile_metainfo))
    torrent = torf.Torrent.read_stream(fo)
    assert torrent.metainfo['info']['private'] is True
    assert torrent.private is True

    del valid_singlefile_metainfo[b'info'][b'private']
    fo = io.BytesIO(bencode.encode(valid_singlefile_metainfo))
    torrent = torf.Torrent.read_stream(fo)
    assert 'private' not in torrent.metainfo['info']
    assert torrent.private is None

def test_reading_torrent_without_creation_date(tmp_path, valid_singlefile_metainfo):
    del valid_singlefile_metainfo[b'creation date']
    fo = io.BytesIO(bencode.encode(valid_singlefile_metainfo))
    torrent = torf.Torrent.read_stream(fo)
    assert 'creation date' not in torrent.metainfo['info']
    assert torrent.creation_date is None


def test_read_from_torrent_file_with_empty_path_components(valid_multifile_metainfo, tmp_path):
    valid_multifile_metainfo[b'info'][b'files'][0][b'path'] = [b'', b'foo', b'', b'', b'bar', b'']
    f = (tmp_path / 'foo.torrent')
    f.write_bytes(bencode.encode(valid_multifile_metainfo))
    t = torf.Torrent.read(str(f))
    exp_path = f'{valid_multifile_metainfo[b"info"][b"name"].decode()}/foo/bar'
    assert exp_path in tuple(str(f) for f in t.files)
