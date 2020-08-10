import os

import pytest


@pytest.fixture
def content(tmp_path):
    content = tmp_path / 'content' ; content.mkdir()  # noqa: E702
    for i in range(1, 5):
        ext = 'jpg' if i % 2 == 0 else 'txt'
        (content / f'file{i}.{ext}').write_text('<data>')
    subdir = content / 'subdir' ; subdir.mkdir()  # noqa: E702

    for i in range(1, 4):
        ext = 'jpg' if i % 2 == 0 else 'pdf'
        (subdir / f'file{i}.{ext}').write_text('<data>')
    return content

def test_exclude_when_path_is_None(create_torrent):
    torrent = create_torrent()
    torrent.metainfo['info']['files'] = [{'length': 6, 'path': ['file1.txt']},
                                         {'length': 6, 'path': ['file2.jpg']},
                                         {'length': 6, 'path': ['file3.txt']}]
    torrent.path = None
    torrent.exclude_globs.append('*.jpg')
    assert torrent.metainfo['info']['files'] == [{'length': 6, 'path': ['file1.txt']},
                                                 {'length': 6, 'path': ['file3.txt']}]
    torrent.exclude_regexs.append('file3')
    assert torrent.metainfo['info']['files'] == [{'length': 6, 'path': ['file1.txt']}]
    assert torrent.path is None

def test_exclude_with_singlefile_torrent_and_existing_path(create_torrent, content):
    torrent = create_torrent(path=content / 'file1.txt')
    assert torrent.metainfo['info']['name'] == 'file1.txt'
    assert torrent.metainfo['info']['length'] == 6
    torrent.exclude_globs.append('*.txt')
    assert torrent.metainfo['info']['name'] == 'file1.txt'
    assert 'length' not in torrent.metainfo['info']

def test_exclude_with_singlefile_torrent_and_nonexisting_path(create_torrent):
    torrent = create_torrent()
    torrent.metainfo['info']['name'] = 'foo.txt'
    torrent.metainfo['info']['length'] = 123
    torrent.exclude_regexs.append(r'fo+\.txt')
    assert torrent.metainfo['info']['name'] == 'foo.txt'
    assert 'length' not in torrent.metainfo['info']

def test_exclude_with_multifile_torrent_and_existing_path(create_torrent, content):
    torrent = create_torrent(path=content)
    assert torrent.metainfo['info']['files'] == [{'length': 6, 'path': ['file1.txt']},
                                                 {'length': 6, 'path': ['file2.jpg']},
                                                 {'length': 6, 'path': ['file3.txt']},
                                                 {'length': 6, 'path': ['file4.jpg']},
                                                 {'length': 6, 'path': ['subdir', 'file1.pdf']},
                                                 {'length': 6, 'path': ['subdir', 'file2.jpg']},
                                                 {'length': 6, 'path': ['subdir', 'file3.pdf']}]
    torrent.exclude_regexs.extend((r'.*1\....$', rf'^{torrent.name}/subdir/.*\.pdf$'))
    assert torrent.metainfo['info']['files'] == [{'length': 6, 'path': ['file2.jpg']},
                                                 {'length': 6, 'path': ['file3.txt']},
                                                 {'length': 6, 'path': ['file4.jpg']},
                                                 {'length': 6, 'path': ['subdir', 'file2.jpg']}]

def test_exclude_with_multifile_torrent_and_nonexisting_path(create_torrent):
    torrent = create_torrent()
    torrent.metainfo['info']['name'] = 'content'
    torrent.metainfo['info']['files'] = [{'length': 6, 'path': ['file1.txt']},
                                         {'length': 6, 'path': ['file2.jpg']},
                                         {'length': 6, 'path': ['file3.txt']},
                                         {'length': 6, 'path': ['subdir', 'file1.pdf']},
                                         {'length': 6, 'path': ['subdir', 'file2.jpg']},
                                         {'length': 6, 'path': ['subdir', 'file3.pdf']}]
    torrent.exclude_globs.extend(('*.jpg', '*/subdir/*3.*'))
    assert torrent.metainfo['info']['files'] == [{'length': 6, 'path': ['file1.txt']},
                                                 {'length': 6, 'path': ['file3.txt']},
                                                 {'length': 6, 'path': ['subdir', 'file1.pdf']}]

def test_exclude_globs_can_be_set(create_torrent, content):
    torrent = create_torrent(path=content)
    torrent.exclude_globs = (f'*{os.sep}file2.*',)
    assert torrent.metainfo['info']['files'] == [{'length': 6, 'path': ['file1.txt']},
                                                 {'length': 6, 'path': ['file3.txt']},
                                                 {'length': 6, 'path': ['file4.jpg']},
                                                 {'length': 6, 'path': ['subdir', 'file1.pdf']},
                                                 {'length': 6, 'path': ['subdir', 'file3.pdf']}]

def test_exclude_regexs_can_be_set(create_torrent, content):
    torrent = create_torrent(path=content)
    torrent.exclude_regexs = (f'{os.sep}subdir{os.sep}',)
    assert torrent.metainfo['info']['files'] == [{'length': 6, 'path': ['file1.txt']},
                                                 {'length': 6, 'path': ['file2.jpg']},
                                                 {'length': 6, 'path': ['file3.txt']},
                                                 {'length': 6, 'path': ['file4.jpg']}]

def test_exclude_globs_and_exclude_regexs_are_combined(create_torrent, content):
    torrent = create_torrent(path=content)
    torrent.exclude_globs = ('*.jpg',)
    torrent.exclude_regexs = ('txt$',)
    assert torrent.metainfo['info']['files'] == [{'length': 6, 'path': ['subdir', 'file1.pdf']},
                                                 {'length': 6, 'path': ['subdir', 'file3.pdf']}]

def test_more_exclude_globs_tests(create_torrent, tmp_path):
    (tmp_path / 'content' / 'foo' / 'bar').mkdir(parents=True)
    (tmp_path / 'content' / 'bar' / 'baz').mkdir(parents=True)
    (tmp_path / 'content' / 'foo' / 'file_bar').write_text('data')
    (tmp_path / 'content' / 'foo' / 'bar' / 'file2').write_text('data')
    (tmp_path / 'content' / 'bar' / 'file3').write_text('data')
    (tmp_path / 'content' / 'bar' / 'baz' / 'file4').write_text('data')

    torrent = create_torrent(path=tmp_path / 'content')
    assert torrent.metainfo['info']['name'] == 'content'
    assert torrent.metainfo['info']['files'] == [{'length': 4, 'path': ['bar', 'baz', 'file4']},
                                                 {'length': 4, 'path': ['bar', 'file3']},
                                                 {'length': 4, 'path': ['foo', 'bar', 'file2']},
                                                 {'length': 4, 'path': ['foo', 'file_bar']}]
    torrent.exclude_globs = ('*oo/*',)
    assert torrent.metainfo['info']['files'] == [{'length': 4, 'path': ['bar', 'baz', 'file4']},
                                                 {'length': 4, 'path': ['bar', 'file3']}]
    torrent.exclude_globs = ('*/ba*',)
    assert torrent.metainfo['info']['files'] == [{'length': 4, 'path': ['foo', 'file_bar']}]
    torrent.exclude_globs = ('*baz*',)
    assert torrent.metainfo['info']['files'] == [{'length': 4, 'path': ['bar', 'file3']},
                                                 {'length': 4, 'path': ['foo', 'bar', 'file2']},
                                                 {'length': 4, 'path': ['foo', 'file_bar']}]
    torrent.exclude_globs = ('*/file[23]',)
    assert torrent.metainfo['info']['files'] == [{'length': 4, 'path': ['bar', 'baz', 'file4']},
                                                 {'length': 4, 'path': ['foo', 'file_bar']}]
    torrent.exclude_globs = ('*Z*',)
    assert torrent.metainfo['info']['files'] == [{'length': 4, 'path': ['bar', 'file3']},
                                                 {'length': 4, 'path': ['foo', 'bar', 'file2']},
                                                 {'length': 4, 'path': ['foo', 'file_bar']}]

def test_more_exclude_regexs_tests(create_torrent, tmp_path):
    (tmp_path / 'content' / 'foo' / 'bar').mkdir(parents=True)
    (tmp_path / 'content' / 'bar' / 'baz').mkdir(parents=True)
    (tmp_path / 'content' / 'foo' / 'file_bar').write_text('data')
    (tmp_path / 'content' / 'foo' / 'bar' / 'file2').write_text('data')
    (tmp_path / 'content' / 'bar' / 'file3').write_text('data')
    (tmp_path / 'content' / 'bar' / 'baz' / 'file4').write_text('data')

    torrent = create_torrent(path=tmp_path / 'content')
    assert torrent.metainfo['info']['name'] == 'content'
    assert torrent.metainfo['info']['files'] == [{'length': 4, 'path': ['bar', 'baz', 'file4']},
                                                 {'length': 4, 'path': ['bar', 'file3']},
                                                 {'length': 4, 'path': ['foo', 'bar', 'file2']},
                                                 {'length': 4, 'path': ['foo', 'file_bar']}]
    torrent.exclude_regexs = ('^content/foo',)
    assert torrent.metainfo['info']['files'] == [{'length': 4, 'path': ['bar', 'baz', 'file4']},
                                                 {'length': 4, 'path': ['bar', 'file3']}]
    torrent.exclude_regexs = ('.*(?:_bar|2)$',)
    assert torrent.metainfo['info']['files'] == [{'length': 4, 'path': ['bar', 'baz', 'file4']},
                                                 {'length': 4, 'path': ['bar', 'file3']}]

def test_include_globs_take_precedence(create_torrent, tmp_path):
    (tmp_path / 'content' / 'foo' / 'bar').mkdir(parents=True)
    (tmp_path / 'content' / 'bar' / 'baz').mkdir(parents=True)
    (tmp_path / 'content' / 'foo' / 'file_bar').write_text('data')
    (tmp_path / 'content' / 'foo' / 'bar' / 'file2').write_text('data')
    (tmp_path / 'content' / 'bar' / 'file3').write_text('data')
    (tmp_path / 'content' / 'bar' / 'baz' / 'file4').write_text('data')

    torrent = create_torrent(path=tmp_path / 'content')
    assert torrent.metainfo['info']['files'] == [{'length': 4, 'path': ['bar', 'baz', 'file4']},
                                                 {'length': 4, 'path': ['bar', 'file3']},
                                                 {'length': 4, 'path': ['foo', 'bar', 'file2']},
                                                 {'length': 4, 'path': ['foo', 'file_bar']}]
    torrent.exclude_globs = ('*foo*',)
    torrent.include_globs = ('*foo/*/file?',)
    assert torrent.metainfo['info']['files'] == [{'length': 4, 'path': ['bar', 'baz', 'file4']},
                                                 {'length': 4, 'path': ['bar', 'file3']},
                                                 {'length': 4, 'path': ['foo', 'bar', 'file2']}]

def test_include_regexs_take_precedence(create_torrent, tmp_path):
    (tmp_path / 'content' / 'foo' / 'bar').mkdir(parents=True)
    (tmp_path / 'content' / 'bar' / 'baz').mkdir(parents=True)
    (tmp_path / 'content' / 'foo' / 'file_bar').write_text('data')
    (tmp_path / 'content' / 'foo' / 'bar' / 'file2').write_text('data')
    (tmp_path / 'content' / 'bar' / 'file3').write_text('data')
    (tmp_path / 'content' / 'bar' / 'baz' / 'file4').write_text('data')

    torrent = create_torrent(path=tmp_path / 'content')
    assert torrent.metainfo['info']['files'] == [{'length': 4, 'path': ['bar', 'baz', 'file4']},
                                                 {'length': 4, 'path': ['bar', 'file3']},
                                                 {'length': 4, 'path': ['foo', 'bar', 'file2']},
                                                 {'length': 4, 'path': ['foo', 'file_bar']}]
    torrent.exclude_regexs = ('file.$',)
    torrent.include_regexs = ('file[23]',)
    assert torrent.metainfo['info']['files'] == [{'length': 4, 'path': ['bar', 'file3']},
                                                 {'length': 4, 'path': ['foo', 'bar', 'file2']},
                                                 {'length': 4, 'path': ['foo', 'file_bar']}]
