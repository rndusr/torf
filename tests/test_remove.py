import torf

import pytest
import os

def test_remove__singlefile_torrent(mktorrent, singlefile_content):
    t = mktorrent(path=singlefile_content.path)
    with pytest.raises(RuntimeError) as e:
        t.remove('anything')
    assert str(e.value) == 'Cannot remove files from single-file torrent'
    assert t.path == singlefile_content.path

def test_remove__no_path_set(mktorrent):
    t = mktorrent()
    assert t.path is None
    with pytest.raises(RuntimeError) as e:
        t.remove('anything')
    assert str(e.value) == 'No files specified in torrent'
    assert t.path is None

def test_remove__path_is_string(mktorrent):
    t = mktorrent()
    t.metainfo['info']['name'] = 'Torrent'
    t.metainfo['info']['files'] = [
        {'length': 123, 'path': ['foo']},
        {'length': 123, 'path': ['bar']},
        {'length': 123, 'path': ['baz', 'one']},
        {'length': 123, 'path': ['baz', 'two']},
    ]
    t.remove(os.path.join('Torrent', 'foo'),
             os.path.join('Torrent', 'baz', 'one'))
    assert t.metainfo['info']['files'] == [
        {'length': 123, 'path': ['bar']},
        {'length': 123, 'path': ['baz', 'two']},
    ]

def test_remove__path_is_iterable(mktorrent):
    t = mktorrent()
    t.metainfo['info']['name'] = 'Torrent'
    t.metainfo['info']['files'] = [
        {'length': 123, 'path': ['foo']},
        {'length': 123, 'path': ['bar']},
        {'length': 123, 'path': ['baz', 'one']},
        {'length': 123, 'path': ['baz', 'two']},
    ]
    def path_iterable(*args):
        for arg in args:
            yield arg
    t.remove(path_iterable('Torrent', 'foo'),
             path_iterable('Torrent', 'baz', 'two'))
    assert t.metainfo['info']['files'] == [
        {'length': 123, 'path': ['bar']},
        {'length': 123, 'path': ['baz', 'one']},
    ]

def test_remove__path_is_directory(mktorrent):
    t = mktorrent()
    t.metainfo['info']['name'] = 'Torrent'
    t.metainfo['info']['files'] = [
        {'length': 123, 'path': ['foo']},
        {'length': 123, 'path': ['bar']},
        {'length': 123, 'path': ['baz', 'one']},
        {'length': 123, 'path': ['baz', 'two']},
    ]
    t.remove(('Torrent', 'baz'))
    assert t.metainfo['info']['files'] == [
        {'length': 123, 'path': ['foo']},
        {'length': 123, 'path': ['bar']},
    ]

def test_remove__after_generate(mktorrent, tmpdir):
    content = tmpdir.mkdir('Torrent')
    content_file1 = content.join('foo')
    content_file2 = content.join('bar')
    content_file3 = content.join('bar')
    content_file1.write('something')
    content_file2.write('something else')
    content_file3.write('this')
    t = mktorrent(path=content)
    t.generate()
    assert 'pieces' in t.metainfo['info']
    t.remove(('Torrent', 'foo'))
    assert 'pieces' not in t.metainfo['info']
    t.generate()
    assert 'pieces' in t.metainfo['info']
    t.remove(('Torrent', 'this', 'path', 'doesnt', 'exist'))
    assert 'pieces' in t.metainfo['info']
