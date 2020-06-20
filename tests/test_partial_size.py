import pytest

import torf


def test_partial_size__singlefile__providing_correct_name(tmp_path):
    (tmp_path / 'content.jpg').write_text('some data')
    t = torf.Torrent(tmp_path / 'content.jpg')
    assert t.partial_size('content.jpg') == 9
    assert t.partial_size(['content.jpg']) == 9

def test_partial_size__singlefile__providing_wrong_name(tmp_path):
    (tmp_path / 'content.jpg').write_text('some data')
    t = torf.Torrent(tmp_path / 'content.jpg')
    for path in ('foo.jpg', ['foo.jpg']):
        with pytest.raises(torf.PathError) as excinfo:
            t.partial_size(path)
        assert excinfo.match('^foo.jpg: Unknown path$')

def test_partial_size__singlefile__providing_path(tmp_path):
    (tmp_path / 'content.jpg').write_text('some data')
    t = torf.Torrent(tmp_path / 'content.jpg')
    for path in ('bar/foo.jpg', ['bar', 'foo.jpg']):
        with pytest.raises(torf.PathError) as excinfo:
            t.partial_size(path)
        assert excinfo.match('^bar/foo.jpg: Unknown path$')


def test_partial_size__multifile__providing_path_to_file(tmp_path):
    (tmp_path / 'content').mkdir()
    (tmp_path / 'content' / 'file1.jpg').write_text('some data')
    (tmp_path / 'content' / 'file2.jpg').write_text('some other data')
    (tmp_path / 'content' / 'subcontent').mkdir()
    (tmp_path / 'content' / 'subcontent' / 'file3.jpg').write_text('some more data')
    t = torf.Torrent(tmp_path / 'content')
    for path in ('content/file1.jpg', ['content', 'file1.jpg']):
        assert t.partial_size(path) == 9
    for path in ('content/file2.jpg', ['content', 'file2.jpg']):
        assert t.partial_size(path) == 15
    for path in ('content/subcontent/file3.jpg', ['content', 'subcontent', 'file3.jpg']):
        assert t.partial_size(path) == 14

def test_partial_size__multifile__providing_path_to_dir(tmp_path):
    (tmp_path / 'content').mkdir()
    (tmp_path / 'content' / 'file1.jpg').write_text('some data')
    (tmp_path / 'content' / 'file2.jpg').write_text('some other data')
    (tmp_path / 'content' / 'subcontent1').mkdir()
    (tmp_path / 'content' / 'subcontent1' / 'file3.jpg').write_text('some more data')
    (tmp_path / 'content' / 'subcontent1' / 'file4.jpg').write_text('and even more data')
    (tmp_path / 'content' / 'subcontent2').mkdir()
    (tmp_path / 'content' / 'subcontent2' / 'file5.jpg').write_text('some more other data')
    (tmp_path / 'content' / 'subcontent2' / 'file6.jpg').write_text('and even more other data')
    t = torf.Torrent(tmp_path / 'content')
    for path in ('content', ['content']):
        assert t.partial_size(path) == 100
    for path in ('content/subcontent1', ['content', 'subcontent1']):
        assert t.partial_size(path) == 32
    for path in ('content/subcontent2', ['content', 'subcontent2']):
        assert t.partial_size(path) == 44

def test_partial_size__multifile__providing_unknown_path(tmp_path):
    (tmp_path / 'content').mkdir()
    (tmp_path / 'content' / 'file1.jpg').write_text('some data')
    (tmp_path / 'content' / 'file2.jpg').write_text('some other data')
    (tmp_path / 'content' / 'subcontent').mkdir()
    (tmp_path / 'content' / 'subcontent' / 'file3.jpg').write_text('some more data')
    t = torf.Torrent(tmp_path / 'content')
    for path in ('content/subcontent/file1.jpg', ['content', 'subcontent', 'file1.jpg']):
        with pytest.raises(torf.PathError) as excinfo:
            t.partial_size(path)
        assert excinfo.match('^content/subcontent/file1.jpg: Unknown path$')
    for path in ('content/file3.jpg', ['content', 'file3.jpg']):
        with pytest.raises(torf.PathError) as excinfo:
            t.partial_size(path)
        assert excinfo.match('^content/file3.jpg: Unknown path$')
    for path in ('file1.jpg', ['file1.jpg']):
        with pytest.raises(torf.PathError) as excinfo:
            t.partial_size(path)
        assert excinfo.match('^file1.jpg: Unknown path$')
