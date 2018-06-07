import torf

import pytest
import io
from datetime import datetime
from bencoder import bencode, bdecode
from hashlib import sha1
from collections import OrderedDict


def test_read_from_unreadable_file(valid_singlefile_metainfo, tmpdir):
    f = tmpdir.join('a.torrent')
    f.write_binary(bencode(valid_singlefile_metainfo))
    f.chmod(mode=0o222)

    with pytest.raises(torf.ReadError) as excinfo:
        torf.Torrent.read(str(f))
    assert excinfo.match(f'^{str(f)}: Permission denied$')


def test_read_non_bencoded_file(tmpdir):
    f = tmpdir.join('not_a.torrent')
    f.write('foo')

    with pytest.raises(torf.ParseError) as excinfo:
        torf.Torrent.read(str(f))
    assert excinfo.match(f'^{str(f)}: Invalid torrent file format$')


def test_validate_after_read(valid_singlefile_metainfo, tmpdir):
    del valid_singlefile_metainfo[b'info']
    f = tmpdir.join('a.torrent')
    f.write_binary(bencode(valid_singlefile_metainfo))

    with pytest.raises(torf.MetainfoError) as excinfo:
        torf.Torrent.read(str(f))
    assert excinfo.match(f"^Invalid metainfo: Missing 'info'$")


def test_successful_read(valid_singlefile_metainfo, tmpdir):
    f = tmpdir.join('a.torrent')
    f.write_binary(bencode(valid_singlefile_metainfo))

    t = torf.Torrent.read(str(f))
    assert t.path is None
    assert tuple(t.files) == (str(valid_singlefile_metainfo[b'info'][b'name'], encoding='utf-8'),)
    assert tuple(t.filepaths) == ()
    assert t.name == str(valid_singlefile_metainfo[b'info'][b'name'], encoding='utf-8')
    assert t.size == valid_singlefile_metainfo[b'info'][b'length']
    assert t.infohash == sha1(bencode(valid_singlefile_metainfo[b'info'])).hexdigest()
    assert t.comment == str(valid_singlefile_metainfo[b'comment'], encoding='utf-8')
    assert t.creation_date == datetime.fromtimestamp(valid_singlefile_metainfo[b'creation date'])
    assert t.created_by == str(valid_singlefile_metainfo[b'created by'], encoding='utf-8')
    assert t.private is bool(valid_singlefile_metainfo[b'info'][b'private'])
    assert t.piece_size == valid_singlefile_metainfo[b'info'][b'piece length']


def test_read_single_tracker(valid_singlefile_metainfo, tmpdir):
    valid_singlefile_metainfo[b'announce'] = b'http://lonelyhost/announce'
    valid_singlefile_metainfo.pop(b'announce-list', None)
    f = tmpdir.join('a.torrent')
    f.write_binary(bencode(valid_singlefile_metainfo))

    t = torf.Torrent.read(str(f))
    assert t.trackers == [[str(valid_singlefile_metainfo[b'announce'], encoding='utf-8')]]

def test_read_multiple_trackers(valid_singlefile_metainfo, tmpdir):
    valid_singlefile_metainfo[b'announce-list'] = [[b'http://localhost', b'http:/foohost'],
                                                   [b'http://bazhost']]
    valid_singlefile_metainfo.pop(b'announce', None)
    f = tmpdir.join('a.torrent')
    f.write_binary(bencode(valid_singlefile_metainfo))

    t = torf.Torrent.read(str(f))
    assert t.trackers == [ [str(url, encoding='utf-8') for url in tier] for tier
                           in valid_singlefile_metainfo[b'announce-list'] ]


def test_read_include_md5_singlefile(valid_singlefile_metainfo, tmpdir):
    valid_singlefile_metainfo[b'info'][b'md5sum'] = b'd8e8fca2dc0f896fd7cb4cb0031ba249'
    f = tmpdir.join('a.torrent')
    f.write_binary(bencode(valid_singlefile_metainfo))
    t = torf.Torrent.read(str(f))
    assert t.include_md5 is True

    valid_singlefile_metainfo[b'info'].pop(b'md5sum', None)
    f.write_binary(bencode(valid_singlefile_metainfo))
    t = torf.Torrent.read(str(f))
    assert t.include_md5 is False

def test_read_include_md5_multifile(valid_multifile_metainfo, tmpdir):
    for fileinfo in valid_multifile_metainfo[b'info'][b'files']:
        fileinfo[b'md5sum'] = b'd8e8fca2dc0f896fd7cb4cb0031ba249'
    f = tmpdir.join('a.torrent')
    f.write_binary(bencode(valid_multifile_metainfo))
    t = torf.Torrent.read(str(f))
    assert t.include_md5 is True

    for fileinfo in valid_multifile_metainfo[b'info'][b'files']:
        fileinfo.pop(b'md5sum', None)
    f.write_binary(bencode(valid_multifile_metainfo))
    t = torf.Torrent.read(str(f))
    assert t.include_md5 is False


def test_read_nonstandard_data_with_validation(tmpdir):
    f = tmpdir.join('a.torrent')
    data = OrderedDict([
        (b'foo', b'bar'),
    ])
    f.write_binary(bencode(data))
    with pytest.raises(torf.MetainfoError) as excinfo:
        t = torf.Torrent.read(str(f))
    assert excinfo.match("^Invalid metainfo: Missing 'info'$")

    data[b'info'] = 1
    f.write_binary(bencode(data))
    with pytest.raises(torf.MetainfoError) as excinfo:
        t = torf.Torrent.read(str(f))
    assert excinfo.match("^Invalid metainfo: 'info' is not a dictionary$")

    data[b'info'] = {}
    f.write_binary(bencode(data))
    with pytest.raises(torf.MetainfoError) as excinfo:
        t = torf.Torrent.read(str(f))
    assert excinfo.match("^Invalid metainfo: Missing 'pieces' in \['info'\]$")


def test_read_nonstandard_data_without_validation(tmpdir):
    f = tmpdir.join('a.torrent')
    data = OrderedDict([
        (b'foo', b'bar'),
        (b'number', 17),
        (b'list', [1, b'two']),
        (b'dict', OrderedDict([
            (b'yes', 1),
            (b'no', 0),
        ]))
    ])
    f.write_binary(bencode(data))
    t = torf.Torrent.read(str(f), validate=False)
    assert t.metainfo['foo'] == 'bar'
    assert t.metainfo['number'] == 17
    assert t.metainfo['list'] == [1, 'two']
    assert t.metainfo['dict'] == {'yes': 1, 'no': 0}
