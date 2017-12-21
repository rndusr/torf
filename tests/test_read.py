import torf

import pytest
import io
from datetime import datetime
from bencoder import bencode, bdecode
from hashlib import sha1


def test_read_from_closed_file():
    stream = io.BytesIO()
    stream.close()
    with pytest.raises(RuntimeError) as excinfo:
        torf.Torrent.read(stream)
    assert excinfo.match(f'{stream!r} is closed')

def test_read_from_writeonly_file():
    stream = io.BufferedWriter(io.BytesIO())
    with pytest.raises(RuntimeError) as excinfo:
        torf.Torrent.read(stream)
    assert excinfo.match(f'{stream!r} is opened in write-only mode')

def test_read_from_textmode_file():
    stream = io.StringIO()
    with pytest.raises(RuntimeError) as excinfo:
        torf.Torrent.read(stream)
    assert excinfo.match(f'{stream!r} is not opened in binary mode')

def test_read_from_non_bencoded_file():
    stream = io.BytesIO(b'this cannot be bdecoded')
    with pytest.raises(torf.MetainfoParseError) as excinfo:
        torf.Torrent.read(stream)
    assert excinfo.match(f'Invalid metainfo')


def test_read(valid_singlefile_metainfo):
    stream = io.BytesIO(bencode(valid_singlefile_metainfo))
    t = torf.Torrent.read(stream)

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


def test_read_single_tracker(valid_singlefile_metainfo):
    valid_singlefile_metainfo[b'announce'] = b'http://lonelyhost/announce'
    valid_singlefile_metainfo.pop(b'announce-list', None)

    stream = io.BytesIO(bencode(valid_singlefile_metainfo))
    t = torf.Torrent.read(stream)
    assert t.trackers == [[str(valid_singlefile_metainfo[b'announce'], encoding='utf-8')]]

def test_read_multiple_trackers(valid_singlefile_metainfo):
    valid_singlefile_metainfo[b'announce-list'] = [[b'http://localhost', b'http:/foohost'],
                                                   [b'http://bazhost']]
    valid_singlefile_metainfo.pop(b'announce', None)

    stream = io.BytesIO(bencode(valid_singlefile_metainfo))
    t = torf.Torrent.read(stream)
    assert t.trackers == [ [str(url, encoding='utf-8') for url in tier] for tier
                           in valid_singlefile_metainfo[b'announce-list'] ]


def test_read_include_md5_singlefile(valid_singlefile_metainfo):
    valid_singlefile_metainfo[b'info'][b'md5sum'] = b'd8e8fca2dc0f896fd7cb4cb0031ba249'
    stream = io.BytesIO(bencode(valid_singlefile_metainfo))
    t = torf.Torrent.read(stream)
    assert t.include_md5 is True

    valid_singlefile_metainfo[b'info'].pop(b'md5sum', None)
    stream = io.BytesIO(bencode(valid_singlefile_metainfo))
    t = torf.Torrent.read(stream)
    assert t.include_md5 is False

def test_read_include_md5_multifile(valid_multifile_metainfo):
    for fileinfo in valid_multifile_metainfo[b'info'][b'files']:
        fileinfo[b'md5sum'] = b'd8e8fca2dc0f896fd7cb4cb0031ba249'
    stream = io.BytesIO(bencode(valid_multifile_metainfo))
    t = torf.Torrent.read(stream)
    assert t.include_md5 is True

    for fileinfo in valid_multifile_metainfo[b'info'][b'files']:
        fileinfo.pop(b'md5sum', None)
    stream = io.BytesIO(bencode(valid_multifile_metainfo))
    t = torf.Torrent.read(stream)
    assert t.include_md5 is False


# This doesn't work currently because all bencoders I know of sort the data
# (according to BEP3). The problem is that this can change the info hash if the
# parsed torrent is unsorted.
#
# It is possible that this is a non-issue, I don't know if these torrents
# actually exist, so I'm just leaving this here for now.

# def test_read_write_makes_no_diff(valid_singlefile_metainfo):
#     unsorted = b'd7:comment22:This is a test comment10:created by11:The creator4:infod4:name19:Torrent for testing12:piece lengthi32768e6:lengthi500000e6:pieces320:\xbc\xda\xf1\xe97\x08\x90\x07\x14\xa5&8\x84\xa2\xaf\xef\x18\x1b*\x88\x02\x0f\xb4J.8\xad\xe1\xae\xe0\xd9\x15\xb1\xcb*\x18\xbeq\xb6\x84\x8an]\xf5A\xe1\xfb\xed\\A\xcc\xc4\xa5F\x8d\xc0l*\xfb\x19\x16\x9c\xb0\xe7\xc0\x13\x81F\xdd\xd5\xe7GN\xb5\x0e\xd8\xaf\x99\xa8\x85UQ\x06\xd6L[\xacz\xff\x96"h\xfc`n\xf0?\xb3\xd0r\xa1\xeb\x0e\xae\xe6u\'bI\xe0\xf3\x9fyD\xb2\xad\xb1\x00\x1d\x18\\\nR\xd9\xdaa3\x10\x06\x97\xbb1\xbb\xa6.V\x08\xc6j\x16\xff3l\xe2\xf6v\x8d\xd2q\x1b"\xf07\x18n\x11\xd3D\xf8\x1b\xb4X\x84\x94n\xa1\x12\xfd\x1c\xa8\x08\x89\x87\x18\xbdc\x02f\\\xab\xd6)\x15E|T4\x0b\xa4\x80 \x1f\x98\xfe\xabV\xad\xc5\x96\xad\xd0z\xf8\xe9\xc7\xb4\\\x18\x1d\xe8\xdcQ\x0c\x10\xe3FAr\t\xc2m\xba\xe0\x06\xa2\xf8tX\xadeME,/o~\xe6 \x94\x9d@\x03\xef\x9d\xd9\xf3\x17ukA93\x93\xb7\xe21\xd0\xd1]\xbb4\xbe-\xf95\x82\xed+#\x19\xf4rZ\x001~\xbe`\xeeN\x9f\xfd\t\xf6\xeb[\x86\xc2\x87c[\x019i\xd3\xae\xf0\x8c\x97\x171U\xe0V\xe7Q\xea\xaa\x19\x95\xba\x14$\x1d\xa4\xbcZ\xf2/\x827:privatei1ee13:creation datei1513440897e8:announce16:http://localhoste'
#     stream1 = io.BytesIO(unsorted)
#     t = torf.Torrent.read(stream1)
#     print(t.metainfo)

#     stream2 = io.BytesIO()
#     t.write(stream2)

#     print(f'{stream1.getvalue()}')
#     print(f'{stream2.getvalue()}')
#     assert stream1.getvalue() == stream2.getvalue()
