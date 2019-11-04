import torf

import pytest
import os
import random
import string
from types import SimpleNamespace
import time
from collections import OrderedDict
import contextlib
import functools


TESTDIR_BASE = 'test_files'

letters = string.ascii_letters + string.digits + '    ²öäåóíéëúæøœœï©®¹³¤óíïœ®øï'
def _randstr():
    length = random.randint(10, 20)
    return ''.join(random.choice(letters) for _ in range(length))


def _mktempdir(tmpdir_factory, subdir=None):
    path = tmpdir_factory.mktemp(TESTDIR_BASE, numbered=True)
    if subdir is None:
        subdir = ''
    subdir += ':' + _randstr()
    return path.mkdir(subdir)


def _generate_random_file(dirpath, filename=None, hidden=False):
    filesize = random.randint(1e3, 1e6)
    filecontent = bytearray(random.getrandbits(8) for _ in range(filesize))
    if filename is None:
        filename = ''
    filename += ':' + _randstr()
    if hidden:
        filename = '.' + filename
    filepath = os.path.join(TESTDIR_BASE, dirpath, filename)
    with open(filepath, 'wb') as f:
        f.write(filecontent)
    assert os.path.getsize(filepath) == filesize
    return filepath

def _generate_empty_file(dirpath, filename=None, hidden=False):
    if filename is None:
        filename = ''
    filename += ':' + _randstr()
    if hidden:
        filename = '.' + filename
    filepath = os.path.join(TESTDIR_BASE, dirpath, filename)
    with open(filepath, 'w') as f:
        f.write('')
    assert os.path.getsize(filepath) == 0
    return str(filepath)



@pytest.fixture
def valid_singlefile_metainfo():
    return OrderedDict([
        (b'announce', b'http://localhost'),
        (b'comment', b'This is a test comment'),
        (b'created by', b'The creator'),
        (b'creation date', 1513440897),
        (b'info', OrderedDict([
            (b'length', 500000),
            (b'name', b'Torrent for testing'),
            (b'piece length', 32768),
            (b'pieces', b'\xbc\xda\xf1\xe97\x08\x90\x07\x14\xa5&8\x84\xa2\xaf\xef\x18\x1b*\x88\x02\x0f\xb4J.8\xad\xe1\xae\xe0\xd9\x15\xb1\xcb*\x18\xbeq\xb6\x84\x8an]\xf5A\xe1\xfb\xed\\A\xcc\xc4\xa5F\x8d\xc0l*\xfb\x19\x16\x9c\xb0\xe7\xc0\x13\x81F\xdd\xd5\xe7GN\xb5\x0e\xd8\xaf\x99\xa8\x85UQ\x06\xd6L[\xacz\xff\x96"h\xfc`n\xf0?\xb3\xd0r\xa1\xeb\x0e\xae\xe6u\'bI\xe0\xf3\x9fyD\xb2\xad\xb1\x00\x1d\x18\\\nR\xd9\xdaa3\x10\x06\x97\xbb1\xbb\xa6.V\x08\xc6j\x16\xff3l\xe2\xf6v\x8d\xd2q\x1b"\xf07\x18n\x11\xd3D\xf8\x1b\xb4X\x84\x94n\xa1\x12\xfd\x1c\xa8\x08\x89\x87\x18\xbdc\x02f\\\xab\xd6)\x15E|T4\x0b\xa4\x80 \x1f\x98\xfe\xabV\xad\xc5\x96\xad\xd0z\xf8\xe9\xc7\xb4\\\x18\x1d\xe8\xdcQ\x0c\x10\xe3FAr\t\xc2m\xba\xe0\x06\xa2\xf8tX\xadeME,/o~\xe6 \x94\x9d@\x03\xef\x9d\xd9\xf3\x17ukA93\x93\xb7\xe21\xd0\xd1]\xbb4\xbe-\xf95\x82\xed+#\x19\xf4rZ\x001~\xbe`\xeeN\x9f\xfd\t\xf6\xeb[\x86\xc2\x87c[\x019i\xd3\xae\xf0\x8c\x97\x171U\xe0V\xe7Q\xea\xaa\x19\x95\xba\x14$\x1d\xa4\xbcZ\xf2/\x82'),
            (b'private', 1)
        ]))
    ])

@pytest.fixture
def valid_multifile_metainfo():
    return OrderedDict([
        (b'announce', b'http://localhost'),
        (b'comment', b'This is a test comment'),
        (b'created by', b'The creator'),
        (b'creation date', 1513440897),
        (b'info', OrderedDict([
            (b'files', [{b'length': 123, b'path': [b'A file']},
                        {b'length': 456, b'path': [b'Another file']},
                        {b'length': 789, b'path': [b'A', b'third', b'file in a subdir']}]),
            (b'name', b'Torrent for testing'),
            (b'piece length', 32768),
            (b'pieces', b'\xbc\xda\xf1\xe97\x08\x90\x07\x14\xa5&8\x84\xa2\xaf\xef\x18\x1b*\x88\x02\x0f\xb4J.8\xad\xe1\xae\xe0\xd9\x15\xb1\xcb*\x18\xbeq\xb6\x84\x8an]\xf5A\xe1\xfb\xed\\A\xcc\xc4\xa5F\x8d\xc0l*\xfb\x19\x16\x9c\xb0\xe7\xc0\x13\x81F\xdd\xd5\xe7GN\xb5\x0e\xd8\xaf\x99\xa8\x85UQ\x06\xd6L[\xacz\xff\x96"h\xfc`n\xf0?\xb3\xd0r\xa1\xeb\x0e\xae\xe6u\'bI\xe0\xf3\x9fyD\xb2\xad\xb1\x00\x1d\x18\\\nR\xd9\xdaa3\x10\x06\x97\xbb1\xbb\xa6.V\x08\xc6j\x16\xff3l\xe2\xf6v\x8d\xd2q\x1b"\xf07\x18n\x11\xd3D\xf8\x1b\xb4X\x84\x94n\xa1\x12\xfd\x1c\xa8\x08\x89\x87\x18\xbdc\x02f\\\xab\xd6)\x15E|T4\x0b\xa4\x80 \x1f\x98\xfe\xabV\xad\xc5\x96\xad\xd0z\xf8\xe9\xc7\xb4\\\x18\x1d\xe8\xdcQ\x0c\x10\xe3FAr\t\xc2m\xba\xe0\x06\xa2\xf8tX\xadeME,/o~\xe6 \x94\x9d@\x03\xef\x9d\xd9\xf3\x17ukA93\x93\xb7\xe21\xd0\xd1]\xbb4\xbe-\xf95\x82\xed+#\x19\xf4rZ\x001~\xbe`\xeeN\x9f\xfd\t\xf6\xeb[\x86\xc2\x87c[\x019i\xd3\xae\xf0\x8c\x97\x171U\xe0V\xe7Q\xea\xaa\x19\x95\xba\x14$\x1d\xa4\xbcZ\xf2/\x82'),
            (b'private', 1)
        ]))
    ])


@pytest.fixture
def generate_random_Torrent_args():
    def f():
        args = {
            'exclude' : random.choice(([], ['no*matches'])),
            'trackers' : random.choice(([],
                                        ['http://localhost:123/announce'],
                                        ['http://localhost:123/announce', 'http://localhost:456/announce'],
                                        [['http://localhost:123/announce', 'http://localhost:456/announce'],
                                         ['http://localhost:789/announce', 'http://localhost:111/announce']])),
            'webseeds' : random.choice(([],
                                        ['http://localhost:123/webseed'],
                                        ['http://localhost:123/webseed', 'http://localhost:456/webseed'])),
            'httpseeds' : random.choice(([],
                                         ['http://localhost:123/httpseed'],
                                         ['http://localhost:123/httpseed', 'http://localhost:456/httpseed'])),
            'comment'       : _randstr(),
            'creation_date' : random.randint(0, int(time.time())),
            'created_by'    : _randstr(),
            'source'        : _randstr(),
            'piece_size'    : random.choice((None, 2**14, 2**15, 2**16, 2**17, 2**18, 2**19, 2**20)),
        }

        # Remove random items from args
        return dict(random.sample(tuple(args.items()), random.randint(0, len(args))))
    return f

@pytest.fixture
def torrent(generate_random_Torrent_args):
    return torf.Torrent(**generate_random_Torrent_args())


@pytest.fixture(scope='session')
def singlefile_content_empty(tmpdir_factory):
    content_path = _mktempdir(tmpdir_factory)
    filepath = _generate_empty_file(content_path, filename='empty Ꝼile')
    return SimpleNamespace(path=str(filepath))

@pytest.fixture(scope='session')
def multifile_content_empty(tmpdir_factory):
    content_path = _mktempdir(tmpdir_factory, subdir='ęmpty directorý')
    for _ in range(2):
        _generate_empty_file(content_path)
    _generate_empty_file(content_path, hidden=True)
    return SimpleNamespace(path=str(content_path))


@pytest.fixture(scope='session')
def singlefile_content(tmpdir_factory):
    random.seed(0)  # Make sure random file names and content are identical every time
    content_path = _mktempdir(tmpdir_factory)
    filepath = _generate_random_file(content_path, filename='sinģle fíle')
    random.seed()  # Re-enable randomness

    exp_pieces = b'BHG\xb7[\xdf\xaa\xf1\xf3<\xd3C\xeb\xab\xecjZ3\x06\x97\x0c*\xb7G3\xc5G\xe3\x0e\xdb\x96\xf1V-D@\xdd\t\xcf\x88GB\xa3\xdf\xdd\x1fxCQd=8\xc7\x81\x96\x0f\xaf(-\xe6FB\x10\xd1\xbf\xad\x88\x1d\x1d\xc3\x03\xb3\x08\xc0\xe0\x0b\x8a\\\x19\xdf\xed\x03\xdb\x7f\x17o3uI\xef(\n\x80\xdbbF\x91\xd90%\xe6\xfay\x16O\x06n-\xad\x1b\x06\x98SJ:\xf3d64=\xf2\xc8\t~\xbf\x08\xdd\x1am\xae\xbe\xed\xf1\x94\x8f\x08X5\x85\x0e\xa2wM\xa3\x14K,\x9dO\xd2n\xb6\x98\x16\xe6s\xa2\t\t0\xa4\x05\xd1\x95*\x02S\xf1y\x14\xf3G\xf8]eUD\x81`_m\xeaW\x0e\xb5\xc1r\n2\xf0Qo\r\xba\x07\xb3!Vr\xacn\x06\xeb\x1a\xce9\x0e\xa1j\xb1\xf9\xc9\xe0J\xda\xa2v\xe4d\'\x8cf5!Z\xd4g[\x9b\xf4fr\xc2\xee\xb3;\xe7\xe3\x9e\xe0\x06}\xe3\xe6\xc9\xa2\xf9t\x0c\xe1\xf5h\xfe\x13\xf5\xe4\xaa\xd6\x01\x91\xe3\xb7\xb2x\xe1\xd7\xb1o\x10\xe7\xd6\xd2b%d\xae\xe4\x8a\x910\x1b\xb6\x1b\xda\x944\xce\t\xd6\xdf%*n\x05\x16\xd9\x8ft\xed\xb7\xeb"\xfd\xb0Q+t\xbdy|\xed\x01<\xb9\xd2"@\xa2\x85\xa6\x8a\x1d|\x89Z\x13w\xdb\xe7\xdd\xe2\xcey\x00R\xa3[k\x8e\xde\x98""\xfd\xc0]{\xc2H\n%8 \xd3\x01\xd2i\x9f\xf0n\x05^\x90\xbc\xcb\xb5\x8a\xde$\xef\xbd\x02\x83\xe2m\x93:K\x10\xfc9\x8c*\xe5y.\\h\xf4$\xf9V\x07+\xbe\x8c\t\x8d\xa5\xfd'

    exp_metainfo = {'announce'      : 'http://localhost:123',
                    'created by'    : 'mktorrent 1.0',
                    'creation date' : 1513522263,
                    'info': {'name'         : os.path.basename(filepath),
                             'piece length' : 2**15,
                             'pieces'       : exp_pieces,
                             'length'       : os.path.getsize(filepath)}}

    exp_attrs = SimpleNamespace(path=str(filepath),
                                infohash='b900befd41f181b446f0849883fb4d2edaaa0949',
                                infohash_base32=b'XEAL57KB6GA3IRXQQSMIH62NF3NKUCKJ',
                                size=os.path.getsize(filepath))

    return SimpleNamespace(path=exp_attrs.path,
                           exp_metainfo=exp_metainfo,
                           exp_attrs=exp_attrs)

@pytest.fixture(scope='session')
def multifile_content(tmpdir_factory):
    random.seed(0)  # Make sure random file names and content are identical every time
    content_path = _mktempdir(tmpdir_factory, subdir='Multifile torrent')
    for n in range(2):
        _generate_random_file(content_path, filename=f'File {n}')
    subdir_path = content_path.mkdir('subdir')
    _generate_random_file(subdir_path, filename='File in subdir')
    random.seed()  # Re-enable randomness

    exp_files=[{'length': 649406, 'path': ['File 0:JïYR WN93kœ']},
               {'length': 199019, 'path': ['File 1:aä¤ELYœPTófsdtœe©í']},
               {'length': 333198, 'path': ['subdir', 'File in subdir:F³bæ¹inRf ¤RTggTSóz']}]

    exp_pieces = b'BHG\xb7[\xdf\xaa\xf1\xf3<\xd3C\xeb\xab\xecjZ3\x06\x97\x0c*\xb7G3\xc5G\xe3\x0e\xdb\x96\xf1V-D@\xdd\t\xcf\x88GB\xa3\xdf\xdd\x1fxCQd=8\xc7\x81\x96\x0f\xaf(-\xe6FB\x10\xd1\xbf\xad\x88\x1d\x1d\xc3\x03\xb3\x08\xc0\xe0\x0b\x8a\\\x19\xdf\xed\x03\xdb\x7f\x17o3uI\xef(\n\x80\xdbbF\x91\xd90%\xe6\xfay\x16O\x06n-\xad\x1b\x06\x98SJ:\xf3d64=\xf2\xc8\t~\xbf\x08\xdd\x1am\xae\xbe\xed\xf1\x94\x8f\x08X5\x85\x0e\xa2wM\xa3\x14K,\x9dO\xd2n\xb6\x98\x16\xe6s\xa2\t\t0\xa4\x05\xd1\x95*\x02S\xf1y\x14\xf3G\xf8]eUD\x81`_m\xeaW\x0e\xb5\xc1r\n2\xf0Qo\r\xba\x07\xb3!Vr\xacn\x06\xeb\x1a\xce9\x0e\xa1j\xb1\xf9\xc9\xe0J\xda\xa2v\xe4d\'\x8cf5!Z\xd4g[\x9b\xf4fr\xc2\xee\xb3;\xe7\xe3\x9e\xe0\x06}\xe3\xe6\xc9\xa2\xf9t\x0c\xe1\xf5h\xfe\x13\xf5\xe4\xaa\xd6\x01\x91\xe3\xb7\xb2x\xe1\xd7\xb1o\x10\xe7\xd6\xd2b%d\xae\xe4\x8a\x910\x1b\xb6\x1b\xda\x944\xce\t\xd6\xdf%*n\x05\x16\xd9\x8ft\xed\xb7\xeb"\xfd\xb0Q+t\xbdy|\xed\x01<\xb9\xd2"@\xa2\x85\xa6\x8a\x1d|\x89Z\x13w\xdb\xe7\xdd\xe2\xcey\x00R\xa3[k\x8e\xde\x98""\xfd\xc0]{\xc2H\n%8 \xd3\x01\xd2i\x9f\xf0n\x05^\x90\xbc\xcb\xb5\x8a\xde$\xef\xbd\x02\x83\xe2m\x93:K\x10\xfc\xc7\xb6\xf5\xcf\x9a!\xe06as\x8b`\xda\x12\xf3\x13\xc73\xbf\xad\xcc\x86V\x14Tm5\xb4&C\x8c\x89\x17*\x83A\xc9o\x04\x9e\xe8p\x0e\x1fIx\xf2\\\xc9\xca\x8c\xd1\xfb#\x08\xeb\x0eq\xf3\r].\xacfH\xea\xc1q\xcc\x1bw\xe3\xe6-o\xf6Hb\x85\xc7\xefk\xa5\xc7\xea\xd1\xa0\xb4h\xb7\xdd\x9fe/\x98g\xef\xea6\x02f\x1a\xc1\xe5N\xf3\x10\x04\xe0\x004!\xca\x81\xa4\xfc\x12\xceS\x9c\x8e,L82\xbb\x83\x8f\x95#\x93\xe2\x83\xaf\xfd\xe9T|@oy\x07x[rp;\x89\xe0a\xdc\xee\xcekW\xaf/\xe8g\x19 \x1b\xd8\x8e.\xc2B\xaf\x94\xd9\xa5X\x94\x85\xc0\xa8\x047\xa6\xcc\xa0i( \x04\x98\xce>A\x87\x92\x8d_\xe8\x8d\xa4\xf2(\xa6\x88\xc7\xfe \xee\xdbe\xc9\r\x19{\xc8T\xc9JU[\x1d\xd3\xb0\xc6-\xdc\xc0YS\xae\x01\x12t(\xc7`m\xc6\x8c\xa8Xr\xb27\xf2\xec\xa3\x0b\r\xfe\xc4\xc0\xf0At\x00Y\xb5\x1b\xebE\x8c:p\xd4\xc1\x80k\x13\xc8I\xfe$\xday\xd2\xcc/\x00\n\t\x02B\xfa\r\x13o\x0f\x8d\xd9<7\xb5\xd0\xa3/\xee\xac\xae&"\x83\xa4)\x10L\xd0-q\xab \x9c\\\xc0\x92\x07MC\x85D\x17Z\xa49\xe3U\xa9\xc4\xc8z|\x1c\xe2\x03\t\x1d\x03\xe2J\x0fM\xfa5!\x98>5\x19h\xbc;{H\xa1\x14\xe7\xcb.X\x93\x7f\x0c\x15\xad'

    exp_metainfo = {'announce'      : 'http://localhost:123',
                    'created by'    : 'mktorrent 1.0',
                    'creation date' : 1513521463,
                    'info': {'name'         : os.path.basename(content_path),
                             'piece length' : 2**15,
                             'pieces'       : exp_pieces,
                             'files'        : exp_files}}

    exp_attrs = SimpleNamespace(path=str(content_path),
                                infohash='0e2e012468101efec5b1ac81ded6b8d95591c1fb',
                                infohash_base32=b'BYXACJDICAPP5RNRVSA55VVY3FKZDQP3',
                                size=sum(fileinfo['length'] for fileinfo in exp_files))

    return SimpleNamespace(path=exp_attrs.path,
                           exp_metainfo=exp_metainfo,
                           exp_attrs=exp_attrs)


@pytest.fixture
def generated_singlefile_torrent(torrent, singlefile_content):
    torrent.path = singlefile_content.path
    torrent.generate()
    return torrent

@pytest.fixture
def generated_multifile_torrent(torrent, multifile_content):
    torrent.path = multifile_content.path
    torrent.generate()
    return torrent


@contextlib.contextmanager
def _create_torrent(tmpdir, **kwargs):
    torrent_file = str(tmpdir.join('test.torrent'))
    try:
        t = torf.Torrent(**kwargs)
        t.generate()
        t.write(torrent_file)
        yield torrent_file
    finally:
        if os.path.exists(torrent_file):
            os.remove(torrent_file)

@pytest.fixture
def create_torrent(tmpdir):
    return functools.partial(_create_torrent, tmpdir)
