import torf

import pytest
import os
from hashlib import md5


def test_path_doesnt_exist(torrent, tmpdir):
    with pytest.raises(torf.PathNotFoundError) as excinfo:
        torrent.path = '/this/path/does/not/exist'
    assert excinfo.match('/this/path/does/not/exist')

def test_path_empty(torrent, tmpdir):
    empty_dir = tmpdir.mkdir('empty')
    with pytest.raises(torf.PathEmptyError) as excinfo:
        torrent.path = empty_dir
    assert excinfo.match(str(empty_dir))

def test_path_reset(torrent, singlefile_content, multifile_content):
    torrent.path = singlefile_content.path
    torrent.private = True
    torrent.path = multifile_content.path
    torrent.generate()
    assert torrent.metainfo['info']['private'] == True
    torrent.path = None
    assert torrent.metainfo['info']['private'] == True
    for key in ('piece length', 'pieces', 'name', 'length', 'md5sum', 'files'):
        assert key not in torrent.metainfo['info']

def test_path_switch_from_singlefile_to_multifile(torrent, singlefile_content, multifile_content):
    torrent.path = singlefile_content.path
    torrent.generate()
    for key in ('piece length', 'pieces', 'name', 'length'):
        assert key in torrent.metainfo['info']
    assert 'files' not in torrent.metainfo['info']

    torrent.path = multifile_content.path
    torrent.generate()
    for key in ('piece length', 'pieces', 'name', 'files'):
        assert key in torrent.metainfo['info']
    assert 'length' not in torrent.metainfo['info']

def test_path_switch_from_multifile_to_singlefile(torrent, singlefile_content, multifile_content):
    torrent.path = multifile_content.path
    torrent.generate()
    for key in ('piece length', 'pieces', 'name', 'files'):
        assert key in torrent.metainfo['info']
    assert 'length' not in torrent.metainfo['info']

    torrent.path = singlefile_content.path
    torrent.generate()
    for key in ('piece length', 'pieces', 'name', 'length'):
        assert key in torrent.metainfo['info']
    assert 'files' not in torrent.metainfo['info']


def test_files_singlefile(torrent, singlefile_content):
    torrent.path = singlefile_content.path
    exp_files1 = (singlefile_content.exp_metainfo['info']['name'],)
    exp_files2 = (torrent.name,)
    assert tuple(torrent.files) == exp_files1
    assert tuple(torrent.files) == exp_files2

def test_files_multifile(torrent, multifile_content):
    torrent.path = multifile_content.path
    torrent_name = os.path.basename(multifile_content.path)
    exp_files1 = tuple(os.path.join(torrent_name, os.path.join(*fileinfo['path']))
                       for fileinfo in multifile_content.exp_metainfo['info']['files'])
    exp_files2 = tuple(os.path.join(torrent.name, os.path.join(*fileinfo['path']))
                       for fileinfo in torrent.metainfo['info']['files'])
    assert tuple(torrent.files) == exp_files1
    assert tuple(torrent.files) == exp_files2

def test_files_with_no_path(torrent):
    assert tuple(torrent.files) == ()


def test_filepaths_singlefile(torrent, singlefile_content):
    torrent.path = singlefile_content.path
    exp_filepaths1 = (singlefile_content.path,)
    exp_filepaths2 = (torrent.path,)
    assert tuple(torrent.filepaths) == exp_filepaths1
    assert tuple(torrent.filepaths) == exp_filepaths2

def test_filepaths_multifile(torrent, multifile_content):
    torrent.path = multifile_content.path
    exp_filepaths1 = tuple(os.path.join(multifile_content.path, os.path.join(*fileinfo['path']))
                           for fileinfo in multifile_content.exp_metainfo['info']['files'])
    exp_filepaths2 = tuple(os.path.join(torrent.path, os.path.join(*fileinfo['path']))
                           for fileinfo in torrent.metainfo['info']['files'])
    assert tuple(torrent.filepaths) == exp_filepaths1
    assert tuple(torrent.filepaths) == exp_filepaths2

def test_filepaths_with_no_path(torrent):
    assert tuple(torrent.filepaths) == ()


def test_name_singlefile(torrent, singlefile_content):
    torrent.path = singlefile_content.path
    assert torrent.name == os.path.basename(torrent.path)
    torrent.name = 'Any name should be allowed'
    assert torrent.name == 'Any name should be allowed'

def test_name_multifile(torrent, multifile_content):
    torrent.path = multifile_content.path
    assert torrent.name == os.path.basename(torrent.path)
    torrent.name = 'Any name should be allowed'
    assert torrent.name == 'Any name should be allowed'


def test_size_singlefile(torrent, singlefile_content):
    assert torrent.size is None
    torrent.path = singlefile_content.path
    assert torrent.size == singlefile_content.exp_attrs.size

def test_size_multifile(torrent, multifile_content):
    assert torrent.size is None
    torrent.path = multifile_content.path
    assert torrent.size == multifile_content.exp_attrs.size



def test_piece_size(torrent, multifile_content):
    torrent.path = multifile_content.path

    torrent.piece_size = None
    assert 'piece length' not in torrent.metainfo['info']
    torrent.piece_size
    assert 'piece length' in torrent.metainfo['info']

    torrent.piece_size = torf.Torrent.MIN_PIECE_SIZE
    assert torrent.piece_size == torf.Torrent.MIN_PIECE_SIZE
    assert torrent.metainfo['info']['piece length'] == torf.Torrent.MIN_PIECE_SIZE

    with pytest.raises(torf.PieceSizeError) as excinfo:
        torrent.piece_size = torf.Torrent.MIN_PIECE_SIZE - 1
    assert excinfo.match(str(torf.Torrent.MIN_PIECE_SIZE))

    torrent.piece_size = torf.Torrent.MAX_PIECE_SIZE
    assert torrent.piece_size == torf.Torrent.MAX_PIECE_SIZE
    assert torrent.metainfo['info']['piece length'] == torf.Torrent.MAX_PIECE_SIZE

    with pytest.raises(torf.PieceSizeError) as excinfo:
        torrent.piece_size = torf.Torrent.MAX_PIECE_SIZE + 1
    assert excinfo.match(str(torf.Torrent.MAX_PIECE_SIZE))

    with pytest.raises(ValueError) as excinfo:
        torrent.piece_size = 'hello'
    assert excinfo.match('hello')


def test_trackers(torrent):
    torrent.trackers = ['http://foo:123/announce',
                        ('http://bar:456/', 'http://baz:789')]
    exp = [['http://foo:123/announce'],
           ['http://bar:456/', 'http://baz:789']]
    assert torrent.trackers == exp
    assert torrent.metainfo['announce-list'] == exp
    assert torrent.metainfo['announce'] == exp[0]

    torrent.trackers = []
    assert torrent.trackers == None
    assert 'announce-list' not in torrent.metainfo
    assert 'announce' not in torrent.metainfo

    for invalid_url in ('foo', 'http://localhost:70000/announce'):
        with pytest.raises(torf.URLError) as excinfo:
            torrent.trackers = [invalid_url]
        assert excinfo.match(repr(invalid_url))


def test_private(torrent):
    torrent.private = 'this evaluates to True'
    assert torrent.private is True
    assert torrent.metainfo['info']['private'] is True

    torrent.private = 0  # This evaluates to False
    assert torrent.private is False
    assert 'private' not in torrent.metainfo['info']


def test_comment(torrent):
    torrent.comment = ''
    assert torrent.comment == ''
    assert torrent.metainfo['comment'] == ''

    torrent.comment = None
    assert torrent.comment is None
    assert 'comment' not in torrent.metainfo


def test_source(torrent):
    torrent.source = ''
    assert torrent.source == ''
    assert torrent.metainfo['source'] == ''

    torrent.source = None
    assert torrent.source is None
    assert 'source' not in torrent.metainfo


def test_creation_date(torrent):
    from datetime import datetime

    torrent.creation_date = 1234
    assert isinstance(torrent.creation_date, datetime)
    assert isinstance(torrent.metainfo['creation date'], datetime)

    now = datetime.now()
    torrent.creation_date = now
    assert torrent.creation_date is now
    assert torrent.metainfo['creation date'] is now

    torrent.creation_date = None
    assert torrent.creation_date is None
    assert 'creation date' not in torrent.metainfo

    with pytest.raises(ValueError):
        torrent.creation_date = [1, 2, 3]


def test_created_by(torrent):
    torrent.created_by = 'somebody'
    assert torrent.created_by == 'somebody'
    assert torrent.metainfo['created by'] == 'somebody'

    torrent.created_by = None
    assert torrent.created_by is None
    assert 'created by' not in torrent.metainfo


def test_repr_string(singlefile_content, generate_random_Torrent_args):
    from datetime import datetime

    t = torf.Torrent()
    assert repr(t) == 'Torrent()'
    t.private = True
    assert repr(t) == 'Torrent(private=True)'
    t.private = False
    assert repr(t) == 'Torrent()'

    now = datetime.now()
    t.creation_date = now
    assert repr(t) == f'Torrent(creation_date={now!r})'

    t.piece_size = 2**20
    assert repr(t) == f'Torrent(creation_date={now!r}, piece_size={2**20})'

    t.creation_date = None

    for name in ('comment', 'created_by', 'source'):
        setattr(t, name, 'foo')
    assert repr(t) == f"Torrent(comment='foo', created_by='foo', source='foo', piece_size={2**20})"


def check_hash(content, hashname):
    t = torf.Torrent(content.path, trackers=['http://localhost/'],
                     piece_size=content.exp_metainfo['info']['piece length'])
    assert t.piece_size == content.exp_metainfo['info']['piece length']
    t.generate()
    exp_attrs = content.exp_attrs
    assert getattr(t, hashname) == getattr(exp_attrs, hashname)

def test_infohash_singlefile(singlefile_content):
    check_hash(singlefile_content, 'infohash')

def test_infohash_base32_singlefile(singlefile_content):
    check_hash(singlefile_content, 'infohash_base32')

def test_infohash_multifile(multifile_content):
    check_hash(multifile_content, 'infohash')

def test_infohash_base32_multifile(multifile_content):
    check_hash(multifile_content, 'infohash_base32')


def test_randomize_infohash(singlefile_content):
    t1 = torf.Torrent(singlefile_content.path)
    t2 = torf.Torrent(singlefile_content.path)

    t1.randomize_infohash = False
    t2.randomize_infohash = False
    assert t1.infohash == t2.infohash

    t1.randomize_infohash = True
    t2.randomize_infohash = True
    assert t1.infohash != t2.infohash
