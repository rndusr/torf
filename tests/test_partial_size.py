import torf

import pytest


def test_partial_size__singlefile__providing_correct_name(tmpdir):
    content_path = tmpdir.join('content.jpg')
    content_path.write('some data')
    t = torf.Torrent(content_path)
    assert t.partial_size('content.jpg') == 9
    assert t.partial_size(['content.jpg']) == 9

def test_partial_size__singlefile__providing_wrong_name(tmpdir):
    content_path = tmpdir.join('content.jpg')
    content_path.write('some data')
    t = torf.Torrent(content_path)
    for path in ('foo.jpg', ['foo.jpg']):
        with pytest.raises(torf.PathNotFoundError) as excinfo:
            t.partial_size(path)
        assert excinfo.match(f'^foo.jpg: No such file or directory$')

def test_partial_size__singlefile__providing_path(tmpdir):
    content_path = tmpdir.join('content.jpg')
    content_path.write('some data')
    t = torf.Torrent(content_path)
    for path in ('bar/foo.jpg', ['bar', 'foo.jpg']):
        with pytest.raises(torf.PathNotFoundError) as excinfo:
            t.partial_size(path)
        assert excinfo.match(f'^bar/foo.jpg: No such file or directory$')


def test_partial_size__multifile__providing_path_to_file(tmpdir):
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file1.write('some data')
    content_file2 = content_path.join('file2.jpg')
    content_file2.write('some other data')
    content_subpath = content_path.mkdir('subcontent')
    content_file3 = content_subpath.join('file3.jpg')
    content_file3.write('some more data')
    t = torf.Torrent(content_path)
    for path in ('content/file1.jpg', ['content', 'file1.jpg']):
        assert t.partial_size(path) == 9
    for path in ('content/file2.jpg', ['content', 'file2.jpg']):
        assert t.partial_size(path) == 15
    for path in ('content/subcontent/file3.jpg', ['content', 'subcontent', 'file3.jpg']):
        assert t.partial_size(path) == 14

def test_partial_size__multifile__providing_path_to_dir(tmpdir):
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file1.write('some data')
    content_file2 = content_path.join('file2.jpg')
    content_file2.write('some other data')
    content_subpath1 = content_path.mkdir('subcontent1')
    content_file3 = content_subpath1.join('file3.jpg')
    content_file3.write('some more data')
    content_subpath2 = content_path.mkdir('subcontent2')
    content_file4 = content_subpath2.join('file4.jpg')
    content_file4.write('some more data again')
    t = torf.Torrent(content_path)
    for path in ('content', ['content']):
        assert t.partial_size(path) == 58
    for path in ('content/subcontent1', ['content', 'subcontent1']):
        assert t.partial_size(path) == 14
    for path in ('content/subcontent2', ['content', 'subcontent2']):
        assert t.partial_size(path) == 20

def test_partial_size__multifile__providing_unknown_path(tmpdir):
    content_path = tmpdir.mkdir('content')
    content_file1 = content_path.join('file1.jpg')
    content_file1.write('some data')
    content_file2 = content_path.join('file2.jpg')
    content_file2.write('some other data')
    content_subpath = content_path.mkdir('subcontent')
    content_file3 = content_subpath.join('file3.jpg')
    content_file3.write('some more data')
    t = torf.Torrent(content_path)
    for path in ('content/subcontent/file1.jpg', ['content', 'subcontent', 'file1.jpg']):
        with pytest.raises(torf.PathNotFoundError) as excinfo:
            t.partial_size(path)
        assert excinfo.match(f'^content/subcontent/file1.jpg: No such file or directory$')
    for path in ('content/file3.jpg', ['content', 'file3.jpg']):
        with pytest.raises(torf.PathNotFoundError) as excinfo:
            t.partial_size(path)
        assert excinfo.match(f'^content/file3.jpg: No such file or directory$')
    for path in ('file1.jpg', ['file1.jpg']):
        with pytest.raises(torf.PathNotFoundError) as excinfo:
            t.partial_size(path)
        assert excinfo.match(f'^file1.jpg: No such file or directory$')
