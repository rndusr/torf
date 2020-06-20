import glob
import os
from pathlib import Path
from unittest.mock import patch

import pytest

import torf
from torf import _errors as errors
from torf import _utils as utils


def test_path_doesnt_exist(create_torrent):
    torrent = create_torrent()
    with pytest.raises(torf.ReadError) as excinfo:
        torrent.path = '/this/path/does/not/exist'
    assert excinfo.match('^/this/path/does/not/exist: No such file or directory$')
    assert torrent.path is None
    for key in ('name', 'files', 'length', 'pieces'):
        assert key not in torrent.metainfo['info']

def test_path_is_empty_directory(create_torrent, tmp_path):
    (tmp_path / 'empty').mkdir()
    torrent = create_torrent(path=tmp_path / 'empty')
    assert torrent.path == tmp_path / 'empty'
    for key in ('name', 'files', 'length', 'pieces'):
        assert key not in torrent.metainfo['info']

def test_path_is_empty_file(create_torrent, tmp_path):
    (tmp_path / 'empty').write_text('')
    torrent = create_torrent(path=tmp_path / 'empty')
    assert torrent.path == tmp_path / 'empty'
    for key in ('name', 'files', 'length', 'pieces'):
        assert key not in torrent.metainfo['info']

def test_path_is_directory_with_empty_file(create_torrent, tmp_path):
    (tmp_path / 'content').mkdir()
    (tmp_path / 'content' / 'empty_file').write_text('')
    torrent = create_torrent(path=tmp_path / 'content')
    assert torrent.path == tmp_path / 'content'
    for key in ('name', 'files', 'length', 'pieces'):
        assert key not in torrent.metainfo['info']

def test_path_reset(create_torrent, singlefile_content, multifile_content):
    torrent = create_torrent()
    torrent.path = singlefile_content.path
    assert torrent.path == Path(singlefile_content.path)
    torrent.private = True
    torrent.generate()
    assert 'pieces' in torrent.metainfo['info']
    assert torrent.metainfo['info']['private'] is True
    assert torrent.metainfo['info']['name'] == os.path.basename(singlefile_content.path)
    assert 'length' in torrent.metainfo['info']
    assert 'files' not in torrent.metainfo['info']

    torrent.path = multifile_content.path
    assert torrent.path == Path(multifile_content.path)
    assert 'pieces' not in torrent.metainfo['info']
    assert torrent.metainfo['info']['private'] is True
    torrent.generate()
    assert 'pieces' in torrent.metainfo['info']
    assert torrent.metainfo['info']['private'] is True
    assert torrent.metainfo['info']['name'] == os.path.basename(multifile_content.path)
    assert 'files' in torrent.metainfo['info']
    assert 'length' not in torrent.metainfo['info']

    torrent.path = None
    assert torrent.path is None
    assert 'pieces' not in torrent.metainfo['info']
    assert 'length' not in torrent.metainfo['info']
    assert torrent.metainfo['info']['private'] is True
    assert torrent.metainfo['info']['name'] == os.path.basename(multifile_content.path)
    assert torrent.metainfo['info']['files'] == multifile_content.exp_metainfo['info']['files']

def test_path_switch_from_singlefile_to_multifile(create_torrent, singlefile_content, multifile_content):
    torrent = create_torrent()
    torrent.path = singlefile_content.path
    assert torrent.path == Path(singlefile_content.path)
    assert torrent.metainfo['info']['name'] == singlefile_content.exp_metainfo['info']['name']
    assert torrent.metainfo['info']['length'] == singlefile_content.exp_metainfo['info']['length']
    assert 'files' not in torrent.metainfo['info']

    torrent.path = multifile_content.path
    assert torrent.path == Path(multifile_content.path)
    assert torrent.metainfo['info']['name'] == multifile_content.exp_metainfo['info']['name']
    assert torrent.metainfo['info']['files'] == multifile_content.exp_metainfo['info']['files']
    assert 'length' not in torrent.metainfo['info']

def test_path_switch_from_multifile_to_singlefile(create_torrent, singlefile_content, multifile_content):
    torrent = create_torrent()
    torrent.path = multifile_content.path
    assert torrent.path == Path(multifile_content.path)
    assert torrent.metainfo['info']['name'] == multifile_content.exp_metainfo['info']['name']
    assert torrent.metainfo['info']['files'] == multifile_content.exp_metainfo['info']['files']
    assert 'length' not in torrent.metainfo['info']

    torrent.path = singlefile_content.path
    assert torrent.path == Path(singlefile_content.path)
    assert torrent.metainfo['info']['name'] == singlefile_content.exp_metainfo['info']['name']
    assert torrent.metainfo['info']['length'] == singlefile_content.exp_metainfo['info']['length']
    assert 'files' not in torrent.metainfo['info']

def test_path_is_period(create_torrent, multifile_content):
    torrent = create_torrent()
    cwd = os.getcwd()
    try:
        os.chdir(multifile_content.path)
        torrent.path = os.curdir
        assert torrent.path == Path(os.curdir)
        assert torrent.metainfo['info']['name'] == os.path.basename(multifile_content.path)
        assert torrent.metainfo['info']['files'] == multifile_content.exp_metainfo['info']['files']
    finally:
        os.chdir(cwd)

def test_path_is_double_period(create_torrent, multifile_content):
    torrent = create_torrent()
    cwd = os.getcwd()
    try:
        os.chdir(os.path.join(multifile_content.path, 'subdir'))
        torrent.path = os.pardir
        assert torrent.path == Path(os.pardir)
        assert torrent.metainfo['info']['name'] == os.path.basename(multifile_content.path)
        assert torrent.metainfo['info']['files'] == multifile_content.exp_metainfo['info']['files']
    finally:
        os.chdir(cwd)

def test_path_ends_with_period(create_torrent, multifile_content):
    torrent = create_torrent()
    torrent.path = Path(multifile_content.path, os.curdir)
    assert torrent.path == Path(multifile_content.path)
    assert torrent.metainfo['info']['name'] == multifile_content.exp_metainfo['info']['name']
    assert torrent.metainfo['info']['files'] == multifile_content.exp_metainfo['info']['files']

def test_path_ends_with_double_period(create_torrent, multifile_content):
    torrent = create_torrent()
    torrent.path = Path(multifile_content.path, 'subdir' , os.pardir)
    assert torrent.path == Path(multifile_content.path, 'subdir', os.pardir)
    assert torrent.metainfo['info']['name'] == multifile_content.exp_metainfo['info']['name']
    assert torrent.metainfo['info']['files'] == multifile_content.exp_metainfo['info']['files']


def test_mode(singlefile_content, multifile_content):
    torrent = torf.Torrent()
    assert torrent.mode is None
    torrent.path = singlefile_content.path
    assert torrent.mode == 'singlefile'
    torrent.path = multifile_content.path
    assert torrent.mode == 'multifile'
    torrent.path = None
    assert torrent.mode == 'multifile'


def test_files_singlefile(create_torrent, singlefile_content):
    torrent = create_torrent(path=singlefile_content.path)
    exp_files1 = (torf.File(singlefile_content.exp_metainfo['info']['name'],
                            size=singlefile_content.exp_metainfo['info']['length']),)
    exp_files2 = (torf.File(torrent.name, size=torrent.size),)
    assert torrent.files == exp_files1
    assert torrent.files == exp_files2

def test_files_multifile(create_torrent, multifile_content):
    torrent = create_torrent(path=multifile_content.path)
    torrent_name = os.path.basename(multifile_content.path)
    exp_files1 = tuple(torf.File([torrent_name] + fileinfo['path'],
                                 size=fileinfo['length'])
                       for fileinfo in multifile_content.exp_metainfo['info']['files'])
    exp_files2 = tuple(torf.File([torrent.name] + fileinfo['path'],
                                 size=fileinfo['length'])
                       for fileinfo in torrent.metainfo['info']['files'])
    assert torrent.files == exp_files1
    assert torrent.files == exp_files2

def test_files_with_no_path(create_torrent):
    torrent = create_torrent()
    assert torrent.files == ()

def test_files_with_no_name(create_torrent, singlefile_content, multifile_content):
    for content in (singlefile_content, multifile_content):
        torrent = create_torrent(path=content.path)
        del torrent.metainfo['info']['name']
        assert all(f.parts[0] == 'UNNAMED TORRENT'
                   for f in torrent.files)

def test_files_only_accepts_Iterables(create_torrent, tmp_path):
    (tmp_path / 'foo').write_text('asdf')
    torrent = create_torrent(path=tmp_path / 'foo')
    torrent.generate()

    assert torrent.metainfo['info']['name'] == 'foo'
    assert torrent.metainfo['info']['length'] == 4
    with pytest.raises(ValueError) as excinfo:
        torrent.files = 'foo/bar'
    assert str(excinfo.value) == 'Not an Iterable: foo/bar'

    # metainfo did not change
    assert torrent.metainfo['info']['name'] == 'foo'
    assert torrent.metainfo['info']['length'] == 4
    assert 'pieces' in torrent.metainfo['info']
    assert torrent.files == (torf.File(Path('foo'), size=4),)

def test_files_only_accepts_File_objects(create_torrent, tmp_path):
    (tmp_path / 'foo').write_text('asdf')
    torrent = create_torrent(path=tmp_path / 'foo')
    torrent.generate()
    with pytest.raises(ValueError) as excinfo:
        torrent.files = ('foo/bar',)
    assert str(excinfo.value) == 'Not a File object: foo/bar'

    # metainfo did not change
    assert torrent.metainfo['info']['name'] == 'foo'
    assert torrent.metainfo['info']['length'] == 4
    assert 'pieces' in torrent.metainfo['info']
    assert torrent.files == (torf.File(Path('foo'), size=4),)

def test_files_only_accepts_relative_paths(create_torrent, tmp_path):
    (tmp_path / 'foo').write_text('asdf')
    torrent = create_torrent(path=tmp_path / 'foo')
    torrent.generate()
    with pytest.raises(torf.PathError) as excinfo:
        torrent.files = (torf.File('/1/2/3', size=123),)
    assert str(excinfo.value) == '/1/2/3: Not a relative path'

    # metainfo did not change
    assert torrent.metainfo['info']['name'] == 'foo'
    assert torrent.metainfo['info']['length'] == 4
    assert 'pieces' in torrent.metainfo['info']
    assert torrent.files == (torf.File(Path('foo'), size=4),)

def test_files_needs_common_path(create_torrent, tmp_path):
    content = tmp_path / 'asdf' ; content.mkdir()  # noqa: E702
    for i in range(1, 3): (content / f'file{i}').write_text('<data>')
    torrent = create_torrent(path=content)
    torrent.generate()
    with pytest.raises(torf.CommonPathError) as excinfo:
        torrent.files = (torf.File(Path('foo/bar/baz'), size=123),
                         torf.File(Path('quux/bar/bam'), size=456),)
    assert str(excinfo.value) == 'No common parent path: foo/bar/baz, quux/bar/bam'

    # metainfo did not change
    assert torrent.metainfo['info']['files'] == [{'path': ['file1'], 'length': 6},
                                                 {'path': ['file2'], 'length': 6}]
    assert torrent.metainfo['info']['name'] == 'asdf'
    assert 'pieces' in torrent.metainfo['info']
    assert 'length' not in torrent.metainfo['info']

def test_files_updates_metainfo_when_manipulated(create_torrent, tmp_path):
    content = tmp_path / 'bar' ; content.mkdir()  # noqa: E702
    for i in range(1, 3): (content / f'file{i}').write_text('<data>')
    torrent = create_torrent(path=content)
    torrent.generate()
    assert torrent.metainfo['info']['name'] == 'bar'
    assert 'pieces' in torrent.metainfo['info']
    assert 'length' not in torrent.metainfo['info']
    assert torrent.files == (torf.File(Path('bar', 'file1'), size=6),
                             torf.File(Path('bar', 'file2'), size=6),)

    torrent.files.append(torf.File(Path('bar', 'subdir', 'file3'), size=123))
    assert torrent.metainfo['info']['files'] == [{'path': ['file1'], 'length': 6},
                                                 {'path': ['file2'], 'length': 6},
                                                 {'path': ['subdir', 'file3'], 'length': 123}]
    assert torrent.metainfo['info']['name'] == 'bar'
    assert 'length' not in torrent.metainfo['info']
    assert 'pieces' not in torrent.metainfo['info']
    assert torrent.files == (torf.File(Path('bar', 'file1'), size=6),
                             torf.File(Path('bar', 'file2'), size=6),
                             torf.File(Path('bar', 'subdir', 'file3'), size=123))

def test_files_switch_from_singlefile_to_multifile(create_torrent, tmp_path):
    (tmp_path / 'foo').write_text('asdf')
    torrent = create_torrent(path=tmp_path / 'foo')
    torrent.generate()
    assert torrent.metainfo['info']['length'] == 4
    assert torrent.metainfo['info']['name'] == 'foo'
    assert 'pieces' in torrent.metainfo['info']
    assert torrent.files == (torf.File(Path('foo'), size=4),)

    torrent.files = (torf.File(Path('bar', 'file1'), size=123),
                     torf.File(Path('bar', 'file2'), size=456))
    assert torrent.metainfo['info']['files'] == [{'path': ['file1'], 'length': 123},
                                                 {'path': ['file2'], 'length': 456}]
    assert torrent.metainfo['info']['name'] == 'bar'
    assert 'length' not in torrent.metainfo['info']
    assert 'pieces' not in torrent.metainfo['info']
    assert torrent.files == (torf.File(Path('bar', 'file1'), size=123),
                             torf.File(Path('bar', 'file2'), size=456))

def test_files_switch_from_multifile_to_singlefile(create_torrent, tmp_path):
    (tmp_path / 'bar').mkdir()
    for i in range(1, 3):
        (tmp_path / 'bar' / f'file{i}').write_text('<data>')
    torrent = create_torrent(path=tmp_path / 'bar')
    torrent.generate()
    assert torrent.metainfo['info']['name'] == 'bar'
    assert torrent.metainfo['info']['files'] == [{'path': ['file1'], 'length': 6},
                                                 {'path': ['file2'], 'length': 6}]
    assert 'pieces' in torrent.metainfo['info']
    assert 'length' not in torrent.metainfo['info']
    assert torrent.files == (torf.File(Path('bar', 'file1'), size=6),
                             torf.File(Path('bar', 'file2'), size=6))

    torrent.files = (torf.File(Path('foo'), size=123),)
    assert torrent.metainfo['info']['name'] == 'foo'
    assert torrent.metainfo['info']['length'] == 123
    assert 'pieces' not in torrent.metainfo['info']
    assert 'files' not in torrent.metainfo['info']
    assert torrent.files == (torf.File(Path('foo'), size=123),)


def test_filepaths_singlefile(create_torrent, singlefile_content):
    torrent = create_torrent(path=singlefile_content.path)
    exp_filepaths1 = [Path(singlefile_content.path)]
    exp_filepaths2 = [Path(torrent.path)]
    assert torrent.filepaths == exp_filepaths1
    assert torrent.filepaths == exp_filepaths2

def test_filepaths_multifile(create_torrent, multifile_content):
    torrent = create_torrent(path=multifile_content.path)
    exp_filepaths1 = tuple(Path(multifile_content.path, *fileinfo['path'])
                           for fileinfo in multifile_content.exp_metainfo['info']['files'])
    exp_filepaths2 = tuple(Path(torrent.path, *fileinfo['path'])
                           for fileinfo in torrent.metainfo['info']['files'])
    assert torrent.filepaths == exp_filepaths1
    assert torrent.filepaths == exp_filepaths2

def test_filepaths_is_set_to_empty_tuple(create_torrent, multifile_content, singlefile_content):
    for content in (singlefile_content, multifile_content):
        torrent = create_torrent(path=content.path)
        torrent.generate()
        assert 'name' in torrent.metainfo['info']
        assert 'pieces' in torrent.metainfo['info']
        if content is singlefile_content:
            assert 'length' in torrent.metainfo['info']
        else:
            assert 'files' in torrent.metainfo['info']
        torrent.filepaths = ()
        assert torrent.filepaths == ()
        for key in ('files', 'length', 'pieces'):
            assert key not in torrent.metainfo['info']
        assert torrent.metainfo['info']['name'] == os.path.basename(content.path)

def test_filepaths_with_single_file_in_directory(create_torrent, tmp_path):
    (tmp_path / 'content').mkdir()
    (tmp_path / 'content' / 'file1').write_text('not empty')
    torrent = create_torrent(path=tmp_path / 'content')
    assert torrent.filepaths == (tmp_path / 'content' / 'file1',)
    assert torrent.mode == 'multifile'
    assert torrent.metainfo['info']['name'] == 'content'
    assert torrent.metainfo['info']['files'] == [{'path': ['file1'], 'length': 9}]

def test_filepaths_with_single_file_is_changed_to_different_file(create_torrent, tmp_path):
    (tmp_path / 'content').write_bytes(b'foo')
    torrent = create_torrent(path=tmp_path / 'content')
    assert torrent.filepaths == (tmp_path / 'content',)
    assert torrent.metainfo['info']['name'] == 'content'
    assert torrent.metainfo['info']['length'] == 3

    (tmp_path / 'content2').write_bytes(b'fooo')
    torrent.filepaths = (tmp_path / 'content2',)
    assert torrent.filepaths == [tmp_path / 'content2']
    assert torrent.metainfo['info']['name'] == 'content2'
    assert torrent.metainfo['info']['length'] == 4

def test_filepaths_with_single_file_is_changed_to_multiple_files(create_torrent, tmp_path):
    (tmp_path / 'content').write_bytes(b'foo')
    torrent = create_torrent(path=tmp_path / 'content')
    assert torrent.filepaths == (tmp_path / 'content',)
    assert torrent.metainfo['info']['name'] == 'content'
    assert torrent.metainfo['info']['length'] == 3

    (tmp_path / 'content2').mkdir()
    (tmp_path / 'content2' / 'foo').write_bytes(b'one')
    (tmp_path / 'content2' / 'bar').write_bytes(b'three')
    torrent.filepaths = (tmp_path / 'content2' / 'foo',
                         tmp_path / 'content2' / 'bar')
    assert torrent.filepaths == [tmp_path / 'content2' / 'foo',
                                 tmp_path / 'content2' / 'bar']
    assert torrent.metainfo['info']['name'] == 'content2'
    assert 'length' not in  torrent.metainfo['info']
    assert torrent.metainfo['info']['files'] == [{'path': ['bar'], 'length': 5},
                                                 {'path': ['foo'], 'length': 3}]

def test_filepaths_with_multiple_files_is_changed_to_different_files(create_torrent, tmp_path):
    (tmp_path / 'content').mkdir()
    (tmp_path / 'content' / 'foo').write_bytes(b'one')
    (tmp_path / 'content' / 'bar').write_bytes(b'three')
    torrent = create_torrent(path=tmp_path / 'content')
    assert torrent.filepaths == [tmp_path / 'content' / 'foo',
                                 tmp_path / 'content' / 'bar']
    assert torrent.metainfo['info']['name'] == 'content'
    assert torrent.metainfo['info']['files'] == [{'path': ['bar'], 'length': 5},
                                                 {'path': ['foo'], 'length': 3}]

    (tmp_path / 'content2').mkdir()
    (tmp_path / 'content2' / 'one').write_bytes(b'foo')
    (tmp_path / 'content2' / 'two').write_bytes(b'bar')
    (tmp_path / 'content2' / 'nope').write_bytes(b'unwanted')
    torrent.filepaths = (tmp_path / 'content2' / 'one',
                         tmp_path / 'content2' / 'two')
    assert torrent.filepaths == [tmp_path / 'content2' / 'one',
                                 tmp_path / 'content2' / 'two']
    assert torrent.metainfo['info']['name'] == 'content2'
    assert torrent.metainfo['info']['files'] == [{'path': ['one'], 'length': 3},
                                                 {'path': ['two'], 'length': 3}]

def test_filepaths_with_multiple_files_is_changed_to_single_file(create_torrent, tmp_path):
    (tmp_path / 'content').mkdir()
    (tmp_path / 'content' / 'foo').write_bytes(b'one')
    (tmp_path / 'content' / 'bar').write_bytes(b'three')
    torrent = create_torrent(path=tmp_path / 'content')
    assert torrent.filepaths == [tmp_path / 'content' / 'foo',
                                 tmp_path / 'content' / 'bar']
    assert torrent.metainfo['info']['name'] == 'content'
    assert torrent.metainfo['info']['files'] == [{'path': ['bar'], 'length': 5},
                                                 {'path': ['foo'], 'length': 3}]

    (tmp_path / 'content2').write_bytes(b'foo')
    torrent.filepaths = (tmp_path / 'content2',)
    assert torrent.filepaths == (tmp_path / 'content2',)
    assert torrent.metainfo['info']['name'] == 'content2'
    assert torrent.metainfo['info']['length'] == 3
    assert 'files' not in  torrent.metainfo['info']

def test_filepaths_with_single_file_manipulated_into_multifile(create_torrent, tmp_path):
    (tmp_path / 'content').mkdir()
    (tmp_path / 'content' / 'file1').write_bytes(b'foo')
    (tmp_path / 'content' / 'file2').write_bytes(b'foo')
    torrent = create_torrent(path=tmp_path / 'content' / 'file1')
    assert torrent.filepaths == (tmp_path / 'content' / 'file1',)
    assert torrent.metainfo['info']['name'] == 'file1'
    assert torrent.metainfo['info']['length'] == 3
    assert 'files' not in  torrent.metainfo['info']

    torrent.filepaths.append(tmp_path / 'content' / 'file2')
    assert torrent.metainfo['info']['name'] == 'content'
    assert torrent.metainfo['info']['files'] == [{'path': ['file1'], 'length': 3},
                                                 {'path': ['file2'], 'length': 3}]
    assert 'length' not in  torrent.metainfo['info']

def test_filepaths_updates_metainfo_automatically_when_manipulated(create_torrent, tmp_path):
    (tmp_path / 'content').mkdir()
    for i in range(1, 5):
        (tmp_path / 'content' / f'file{i}').write_text('<data>')
    torrent = create_torrent(path=tmp_path / 'content')

    assert torrent.metainfo['info']['files'] == [{'path': ['file1'], 'length': 6},
                                                 {'path': ['file2'], 'length': 6},
                                                 {'path': ['file3'], 'length': 6},
                                                 {'path': ['file4'], 'length': 6}]
    torrent.filepaths.remove(tmp_path / 'content' / 'file3')
    assert torrent.metainfo['info']['files'] == [{'path': ['file1'], 'length': 6},
                                                 {'path': ['file2'], 'length': 6},
                                                 {'path': ['file4'], 'length': 6}]
    torrent.filepaths.append(tmp_path / 'content' / 'file3')
    assert torrent.metainfo['info']['files'] == [{'path': ['file1'], 'length': 6},
                                                 {'path': ['file2'], 'length': 6},
                                                 {'path': ['file3'], 'length': 6},
                                                 {'path': ['file4'], 'length': 6}]
    torrent.filepaths.remove(tmp_path / 'content' / 'file2')
    assert torrent.metainfo['info']['files'] == [{'path': ['file1'], 'length': 6},
                                                 {'path': ['file3'], 'length': 6},
                                                 {'path': ['file4'], 'length': 6}]

def test_filepaths_gets_information_from_metainfo(create_torrent, tmp_path):
    (tmp_path / 'content').mkdir()
    for i in range(1, 5):
        (tmp_path / 'content' / f'file{i}').write_text('<data>')
    torrent = create_torrent(path=tmp_path / 'content')
    torrent.metainfo['info']['files'].remove({'path': ['file1'], 'length': 6})
    torrent.metainfo['info']['files'].remove({'path': ['file2'], 'length': 6})
    torrent.metainfo['info']['files'].append({'path': ['file9'], 'length': 6000})
    assert torrent.filepaths == [tmp_path / 'content' / 'file3',
                                 tmp_path / 'content' / 'file4',
                                 tmp_path / 'content' / 'file9']
    with pytest.raises(torf.ReadError) as excinfo:
        torrent.generate()
    assert str(excinfo.value) == f'{tmp_path / "content" / "file9"}: No such file or directory'


def test_filepaths_uses_common_parent_directory(create_torrent, tmp_path):
    (tmp_path / 'content').mkdir()
    for i in range(1, 4):
        (tmp_path / 'content' / f'file{i}').write_text('<data>')
    (tmp_path / 'content' / 'subdir').mkdir()
    for i in range(4, 6):
        (tmp_path / 'content' / 'subdir' / f'file{i}').write_text('<more data>')
    torrent = create_torrent(path=tmp_path / 'content' / 'subdir')
    assert torrent.metainfo['info']['name'] == 'subdir'
    assert torrent.metainfo['info']['files'] == [{'path': ['file4'], 'length': 11},
                                                 {'path': ['file5'], 'length': 11}]

    torrent.filepaths.append(tmp_path / 'content' / 'file3')
    assert torrent.metainfo['info']['name'] == 'content'
    assert torrent.metainfo['info']['files'] == [{'path': ['file3'], 'length': 6},
                                                 {'path': ['subdir', 'file4'], 'length': 11},
                                                 {'path': ['subdir', 'file5'], 'length': 11}]

def test_filepaths_resolves_directories(create_torrent, tmp_path):
    (tmp_path / 'content').mkdir()
    for i in range(1, 3):
        (tmp_path / 'content' / f'file{i}').write_text('<data>')
    (tmp_path / 'content' / 'subdir').mkdir()
    for i in range(3, 5):
        (tmp_path / 'content' / 'subdir' / f'file{i}').write_text('<subdata>')
    (tmp_path / 'content' / 'subdir' / 'subsubdir').mkdir()
    for i in range(5, 7):
        (tmp_path / 'content' / 'subdir' / 'subsubdir' / f'file{i}').write_text('<subsubdata>')
    torrent = create_torrent(path=tmp_path / 'content')
    assert torrent.metainfo['info']['name'] == 'content'
    assert torrent.metainfo['info']['files'] == [{'path': ['file1'], 'length': 6},
                                                 {'path': ['file2'], 'length': 6},
                                                 {'path': ['subdir', 'file3'], 'length': 9},
                                                 {'path': ['subdir', 'file4'], 'length': 9},
                                                 {'path': ['subdir', 'subsubdir', 'file5'], 'length': 12},
                                                 {'path': ['subdir', 'subsubdir', 'file6'], 'length': 12}]
    torrent.filepaths = (tmp_path / 'content' / 'subdir' / 'subsubdir',)
    assert torrent.metainfo['info']['name'] == 'subsubdir'
    assert torrent.metainfo['info']['files'] == [{'path': ['file5'], 'length': 12},
                                                 {'path': ['file6'], 'length': 12}]
    torrent.filepaths = (tmp_path / 'content' / 'subdir',)
    assert torrent.metainfo['info']['name'] == 'subdir'
    assert torrent.metainfo['info']['files'] == [{'path': ['file3'], 'length': 9},
                                                 {'path': ['file4'], 'length': 9},
                                                 {'path': ['subsubdir', 'file5'], 'length': 12},
                                                 {'path': ['subsubdir', 'file6'], 'length': 12}]

def test_filepaths_understands_relative_paths(create_torrent, tmp_path):
    (tmp_path / 'parent' / 'content').mkdir(parents=True)
    for i in range(1, 4):
        (tmp_path / 'parent' / 'content' / f'file{i}').write_text('<data>')
    cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        abspath = tmp_path / 'parent' / 'content'
        relpath = Path('parent', 'content')
        torrent = create_torrent(path=relpath)
        # File paths are relative
        assert torrent.filepaths == [relpath / 'file1', relpath / 'file2', relpath / 'file3']
        assert torrent.name == 'content'

        # Remove file3 as absolute path
        torrent.filepaths.remove(abspath / 'file3')
        assert torrent.filepaths == [relpath / 'file1', relpath / 'file2']
        assert torrent.name == 'content'

        # Append file3 as absolute path
        torrent.filepaths.append(abspath / 'file3')
        assert torrent.filepaths == [relpath / 'file1', relpath / 'file2', abspath / 'file3']
        assert torrent.name == 'content'

        # Add file outside of torrent.path as relative path
        (tmp_path / 'parent' / 'outsider').write_text('<data>')
        torrent.filepaths.append(tmp_path / 'parent' / 'outsider')
        assert torrent.name == 'parent'
        assert torrent.filepaths == [relpath / 'file1', relpath / 'file2', abspath / 'file3',
                                     Path('parent', 'outsider')]
    finally:
        os.chdir(cwd)

def test_filepaths_does_not_accept_nonexisting_files(create_torrent, tmp_path):
    (tmp_path / 'content').mkdir()
    for i in range(1, 5):
        (tmp_path / 'content' / f'file{i}').write_text('<data>')
    torrent = create_torrent(path=tmp_path / 'content')

    with pytest.raises(torf.ReadError) as excinfo:
        torrent.filepaths.append(tmp_path / 'content' / 'asdf')
    assert str(excinfo.value) == f'{tmp_path / "content" / "asdf"}: No such file or directory'

    assert torrent.metainfo['info']['name'] == 'content'
    assert torrent.metainfo['info']['files'] == [{'path': ['file1'], 'length': 6},
                                                 {'path': ['file2'], 'length': 6},
                                                 {'path': ['file3'], 'length': 6},
                                                 {'path': ['file4'], 'length': 6}]

def test_filepaths_updates_path(create_torrent, tmp_path):
    torrent = create_torrent()
    assert torrent.path is None
    assert torrent.filepaths == ()
    (tmp_path / 'some_file').write_text('<data>')
    torrent.filepaths.append(tmp_path / 'some_file')
    assert torrent.path == tmp_path / 'some_file'
    assert torrent.filepaths == [tmp_path / 'some_file']


def test_filetree_with_no_path(create_torrent):
    torrent = create_torrent()
    assert torrent.filetree == {}

def test_filetree_with_subdirectories(create_torrent, tmp_path):
    (tmp_path / 'content').mkdir()
    for i in range(1, 3):
        (tmp_path / 'content' / f'file{i}').write_text('<data>')
    (tmp_path / 'content' / 'subdir').mkdir()
    for i in range(3, 5):
        (tmp_path / 'content' / 'subdir' / f'file{i}').write_text('<subdata>')
    (tmp_path / 'content' / 'subdir' / 'subsubdir').mkdir()
    for i in range(5, 7):
        (tmp_path / 'content' / 'subdir' / 'subsubdir' / f'file{i}').write_text('<subsubdata>')
    torrent = create_torrent(path=tmp_path / 'content')
    File = torf.File
    assert torrent.filetree == {'content': {
        'file1': File(Path('content', 'file1'), size=6),
        'file2': File(Path('content', 'file2'), size=6),
        'subdir': {'file3': File(Path('content', 'subdir', 'file3'), size=9),
                   'file4': File(Path('content', 'subdir', 'file4'), size=9),
                   'subsubdir': {'file5': File(Path('content/subdir/subsubdir/file5'), size=12),
                                 'file6': File(Path('content/subdir/subsubdir/file6'), size=12)}}}}

def test_filetree_with_single_file_in_directory(create_torrent, tmp_path):
    (tmp_path / 'content').mkdir()
    (tmp_path / 'content' / 'file').write_text('<data>')
    torrent = create_torrent(path=tmp_path / 'content')
    File = torf.File
    assert torrent.filetree == {'content': {'file': File(Path('content', 'file'), size=6)}}

def test_filetree_with_single_file(create_torrent, tmp_path):
    (tmp_path / 'content').write_text('<data>')
    torrent = create_torrent(path=tmp_path / 'content')
    File = torf.File
    assert torrent.filetree == {'content': File(Path('content'), size=6)}


def test_name(create_torrent, singlefile_content, multifile_content):
    def generate_exp_files(content, torrent_name):
        if content is singlefile_content:
            return (Path(torrent_name),)
        else:
            filewalker = (Path(f) for f in glob.iglob(os.path.join(content.path, '**'), recursive=True)
                          if os.path.isfile(f))
            rel_paths = sorted(path.relative_to(content.path) for path in filewalker)
            exp_files = tuple(torf.File(Path(torrent_name, path),
                                        size=os.path.getsize(Path(content.path, path)))
                              for path in rel_paths)
            return exp_files

    def generate_exp_filepaths(content):
        if content is singlefile_content:
            return (Path(content.path),)
        else:
            return tuple(sorted(Path(f) for f in glob.iglob(os.path.join(content.path, '**'), recursive=True)
                                if os.path.isfile(f)))

    torrent = create_torrent()
    for content in (singlefile_content, multifile_content):
        torrent.name = None

        torrent.path = content.path
        assert torrent.name == os.path.basename(torrent.path)
        assert torrent.files == generate_exp_files(content, os.path.basename(content.path))
        assert torrent.filepaths == generate_exp_filepaths(content)
        for fp in torrent.filepaths:
            assert os.path.exists(fp)

        torrent.name = 'Any name should be allowed'
        assert torrent.name == 'Any name should be allowed'
        assert torrent.files == generate_exp_files(content, 'Any name should be allowed')
        assert torrent.filepaths == generate_exp_filepaths(content)
        for fp in torrent.filepaths:
            assert os.path.exists(fp)

        torrent.path = None
        assert torrent.name == 'Any name should be allowed'
        assert torrent.files == generate_exp_files(content, 'Any name should be allowed')
        assert torrent.filepaths == ()

        torrent.name = 'foo'
        assert torrent.name == 'foo'
        assert torrent.files == generate_exp_files(content, 'foo')
        assert torrent.filepaths == ()

        torrent.path = content.path
        assert torrent.name == os.path.basename(torrent.path)
        assert torrent.files == generate_exp_files(content, os.path.basename(torrent.path))
        assert torrent.filepaths == generate_exp_filepaths(content)
        for fp in torrent.filepaths:
            assert os.path.exists(fp)


def test_size(create_torrent, singlefile_content, multifile_content):
    torrent = create_torrent()
    assert torrent.size == 0
    for content in (singlefile_content, multifile_content):
        torrent.path = content.path
        assert torrent.size == content.exp_attrs.size


def test_piece_size_of_empty_torrent_is_zero():
    assert torf.Torrent().piece_size == 0

def test_piece_size_is_set_automatically(create_torrent, multifile_content):
    torrent = create_torrent(path=multifile_content.path)
    assert torrent.piece_size != 0
    assert 'piece length' in torrent.metainfo['info']

    torrent = torf.Torrent()
    assert torrent.piece_size == 0
    assert 'piece length' not in torrent.metainfo['info']

    torrent.path = multifile_content.path
    assert torrent.piece_size != 0
    assert 'piece length' in torrent.metainfo['info']

def test_piece_size_is_set_manually(create_torrent, multifile_content):
    torrent = create_torrent(path=multifile_content.path, piece_size=16 * 2**20)
    assert torrent.piece_size == 16 * 2**20
    assert torrent.metainfo['info']['piece length'] == 16 * 2**20

    torrent = torf.Torrent(piece_size=16 * 2**20)
    assert torrent.piece_size == 16 * 2**20
    assert torrent.metainfo['info']['piece length'] == 16 * 2**20

    torrent.path = multifile_content.path
    assert torrent.piece_size != 16 * 2**20
    assert torrent.metainfo['info']['piece length'] != 16 * 2**20

def test_piece_size_defaults_to_return_value_of_calculate_piece_size(create_torrent, multifile_content):
    torrent = create_torrent(path=multifile_content.path)
    assert torrent.piece_size != 4 * 2**20
    assert torrent.metainfo['info']['piece length'] != 4 * 2**20
    with patch.object(torf.Torrent, 'calculate_piece_size', lambda self, size: 4 * 2**20):
        torrent.piece_size = None
    assert torrent.piece_size == 4 * 2**20
    assert torrent.metainfo['info']['piece length'] == 4 * 2**20

def test_piece_size_when_torrent_size_is_zero(create_torrent, multifile_content):
    torrent = torf.Torrent(path=multifile_content.path, exclude_globs=('*',))
    assert torrent.size == 0
    assert torrent.piece_size == 0
    assert 'piece length' not in torrent.metainfo['info']

def test_piece_size_is_set_to_wrong_type(create_torrent):
    torrent = create_torrent()
    with pytest.raises(ValueError) as excinfo:
        torrent.piece_size = 'hello'
    assert str(excinfo.value) == "piece_size must be int, not str: 'hello'"

def test_piece_size_is_set_manually_to_not_power_of_two(create_torrent):
    torrent = create_torrent()
    with pytest.raises(torf.PieceSizeError) as excinfo:
        torrent.piece_size = 123 * 1000
    assert str(excinfo.value) == 'Piece size must be a power of 2: 123000'

def test_piece_size_min_and_piece_size_max(create_torrent):
    torrent = create_torrent()
    with patch.multiple(torf.Torrent, piece_size_min=16, piece_size_max=128):
        with pytest.raises(torf.PieceSizeError) as excinfo:
            torrent.piece_size = 8
        assert str(excinfo.value) == 'Piece size must be between 16 and 128: 8'
        with pytest.raises(torf.PieceSizeError) as excinfo:
            torrent.piece_size = 256
        assert str(excinfo.value) == 'Piece size must be between 16 and 128: 256'

def test_piece_size_can_be_invalid_in_metainfo(create_torrent):
    torrent = create_torrent()
    torrent.metainfo['info']['piece length'] = 123
    torrent.metainfo['info']['piece length'] = 'foo'
    torrent.metainfo['info']['piece length'] = -12

def test_calculate_piece_size(monkeypatch):
    monkeypatch.setattr(torf.Torrent, 'piece_size_min', 1024)
    monkeypatch.setattr(torf.Torrent, 'piece_size_max', 256 * 2**20)
    calc = torf.Torrent.calculate_piece_size
    for size in (1, 10, 100):
        assert calc(size) == 1024
    assert calc(100 * 2**20) == 128 * 1024      # noqa: E201,E222
    assert calc(500 * 2**20) == 512 * 1024      # noqa: E201,E222
    assert calc(  1 * 2**30) ==      2**20      # noqa: E201,E222
    assert calc(  2 * 2**30) ==      2**20      # noqa: E201,E222
    assert calc(  3 * 2**30) ==  2 * 2**20      # noqa: E201,E222
    assert calc(  6 * 2**30) ==  2 * 2**20      # noqa: E201,E222
    assert calc(  7 * 2**30) ==  4 * 2**20      # noqa: E201,E222
    assert calc(  8 * 2**30) ==  4 * 2**20      # noqa: E201,E222
    assert calc(  9 * 2**30) ==  8 * 2**20      # noqa: E201,E222
    assert calc( 16 * 2**30) ==  8 * 2**20      # noqa: E201,E222
    assert calc( 32 * 2**30) ==  8 * 2**20      # noqa: E201,E222
    assert calc( 64 * 2**30) ==  8 * 2**20      # noqa: E201,E222
    assert calc( 80 * 2**30) ==  8 * 2**20      # noqa: E201,E222
    assert calc( 81 * 2**30) == 16 * 2**20      # noqa: E201,E222
    assert calc(160 * 2**30) == 16 * 2**20      # noqa: E201,E222
    assert calc(165 * 2**30) == 32 * 2**20      # noqa: E201,E222
    assert calc(200 * 2**30) == 32 * 2**20      # noqa: E201,E222
    assert calc(300 * 2**30) == 32 * 2**20      # noqa: E201,E222
    assert calc(400 * 2**30) == 64 * 2**20      # noqa: E201,E222
    assert calc(1000 * 2**30) == 128 * 2**20    # noqa: E201,E222
    assert calc(4000 * 2**30) == 256 * 2**20    # noqa: E201,E222
    assert calc(40000 * 2**30) == 256 * 2**20   # noqa: E201,E222
    assert calc(400000 * 2**30) == 256 * 2**20  # noqa: E201,E222


def test_hashes(create_torrent, multifile_content):
    torrent = create_torrent()
    assert torrent.hashes == ()
    torrent.path = multifile_content.path
    torrent.piece_size = multifile_content.exp_metainfo['info']['piece length']
    assert torrent.hashes == ()
    torrent.generate()
    hashes_string = multifile_content.exp_metainfo['info']['pieces']
    assert torrent.hashes == tuple(hashes_string[pos : pos + 20]
                                   for pos in range(0, len(hashes_string), 20))
    torrent.path = None
    assert torrent.hashes == ()


def test_trackers__correct_type(create_torrent):
    torrent = create_torrent()
    assert isinstance(torrent.trackers, utils.Trackers)
    torrent.trackers = ('http://foo', ('http://bar', 'http://baz'))
    assert isinstance(torrent.trackers, utils.Trackers)

def test_trackers__set_to_invalid_type(create_torrent):
    torrent = create_torrent()
    with pytest.raises(ValueError) as e:
        torrent.trackers = 17
    assert str(e.value) == 'Must be Iterable, str or None, not int: 17'

def test_trackers__set_to_None(create_torrent):
    torrent = create_torrent()
    torrent.trackers = ('http://foo', ('http://bar', 'http://baz'))
    torrent.trackers = None
    assert torrent.trackers == []
    assert 'announce' not in torrent.metainfo
    assert 'announce-list' not in torrent.metainfo

def test_trackers__sync_to_metainfo(create_torrent):
    torrent = create_torrent()
    torrent.trackers = ('http://foo', 'http://bar')
    assert torrent.trackers == [['http://foo'], ['http://bar']]
    assert torrent.metainfo['announce'] == 'http://foo'
    assert torrent.metainfo['announce-list'] == [['http://foo'], ['http://bar']]
    torrent.trackers.append('http://asdf')
    assert torrent.trackers == [['http://foo'], ['http://bar'], ['http://asdf']]
    assert torrent.metainfo['announce'] == 'http://foo'
    assert torrent.metainfo['announce-list'] == [['http://foo'], ['http://bar'], ['http://asdf']]
    torrent.trackers[0].insert(0, 'http://quux')
    assert torrent.trackers == [['http://quux', 'http://foo'], ['http://bar'], ['http://asdf']]
    assert torrent.metainfo['announce'] == 'http://quux'
    assert torrent.metainfo['announce-list'] == [['http://quux', 'http://foo'], ['http://bar'], ['http://asdf']]
    torrent.trackers[1].remove('http://bar')
    assert torrent.trackers == [['http://quux', 'http://foo'], ['http://asdf']]
    assert torrent.metainfo['announce'] == 'http://quux'
    assert torrent.metainfo['announce-list'] == [['http://quux', 'http://foo'], ['http://asdf']]
    del torrent.trackers[0]
    assert torrent.trackers == [['http://asdf']]
    assert torrent.metainfo['announce'] == 'http://asdf'
    assert 'announce-list' not in torrent.metainfo
    del torrent.trackers[0]
    assert torrent.trackers == []
    assert 'announce' not in torrent.metainfo
    assert 'announce-list' not in torrent.metainfo

def test_trackers__announce_in_metainfo_is_automatically_included_in_announce_list(create_torrent):
    torrent = create_torrent()
    torrent.metainfo['announce'] = 'http://foo:123'
    torrent.metainfo['announce-list'] = [['http://bar:456', 'http://baz:789'],
                                         ['http://quux']]
    assert torrent.trackers == [['http://foo:123'], ['http://bar:456', 'http://baz:789'], ['http://quux']]
    assert torrent.metainfo['announce-list'] == [['http://bar:456', 'http://baz:789'], ['http://quux']]
    assert torrent.metainfo['announce'] == 'http://foo:123'

def test_trackers__announce_in_metainfo_is_not_duplicated(create_torrent):
    torrent = create_torrent()
    torrent.metainfo['announce'] = 'http://foo:123'

    torrent.metainfo['announce-list'] = [['http://foo:123'], ['http://bar:456', 'http://baz:789']]
    exp = [['http://foo:123'], ['http://bar:456', 'http://baz:789']]
    assert torrent.trackers == exp
    assert torrent.metainfo['announce-list'] == exp
    assert torrent.metainfo['announce'] == 'http://foo:123'

    torrent.metainfo['announce-list'] = [['http://foo:123', 'http://bar:456', 'http://baz:789']]
    exp = [['http://foo:123', 'http://bar:456', 'http://baz:789']]
    assert torrent.trackers == exp
    assert torrent.metainfo['announce-list'] == exp
    assert torrent.metainfo['announce'] == 'http://foo:123'

    torrent.metainfo['announce-list'] = [['http://bar:456', 'http://foo:123', 'http://baz:789']]
    exp = [['http://bar:456', 'http://foo:123', 'http://baz:789']]
    assert torrent.trackers == exp
    assert torrent.metainfo['announce-list'] == exp
    assert torrent.metainfo['announce'] == 'http://foo:123'

def test_trackers__single_url_only_sets_announce_in_metainfo(create_torrent):
    torrent = create_torrent()
    torrent.metainfo['announce-list'] = [['http://foo:123'], ['http://bar:456']]
    torrent.trackers = 'http://foo:123'
    assert torrent.trackers == [['http://foo:123']]
    assert 'announce-list' not in torrent.metainfo
    assert torrent.metainfo['announce'] == 'http://foo:123'

def test_trackers__multiple_urls_sets_announce_and_announcelist_in_metainfo(create_torrent):
    torrent = create_torrent()
    torrent.trackers = ['http://foo:123', 'http://bar:456', 'http://baz:789']
    exp = [['http://foo:123'], ['http://bar:456'], ['http://baz:789']]
    assert torrent.trackers == exp
    assert torrent.metainfo['announce-list'] == exp
    assert torrent.metainfo['announce'] == 'http://foo:123'

def test_trackers__multiple_lists_of_urls_sets_announce_and_announcelist_in_metainfo(create_torrent):
    torrent = create_torrent()
    torrent.trackers = [['http://foo:123', 'http://bar:456'],
                        ['http://asdf'],
                        ['http://a', 'http://b', 'http://c']]
    exp = [['http://foo:123', 'http://bar:456'],
           ['http://asdf'],
           ['http://a', 'http://b', 'http://c']]
    assert torrent.trackers == exp
    assert torrent.metainfo['announce-list'] == exp
    assert torrent.metainfo['announce'] == 'http://foo:123'

def test_trackers__no_trackers(create_torrent):
    torrent = create_torrent()
    torrent.metainfo['announce'] = 'http://foo:123'
    torrent.metainfo['announce-list'] = [['http://foo:123'], ['http://bar:456', 'http://baz:789']]
    torrent.trackers = ()
    assert torrent.trackers == []
    assert 'announce-list' not in torrent.metainfo
    assert 'announce' not in torrent.metainfo

def test_trackers__addition(create_torrent):
    torrent = create_torrent()
    torrent.trackers = 'http://foo'
    torrent.trackers += ('http://bar',)
    assert torrent.trackers == [['http://foo'], ['http://bar']]
    assert torrent.metainfo['announce-list'] == [['http://foo'], ['http://bar']]
    assert torrent.metainfo['announce'] == 'http://foo'


def test_webseeds__correct_type(create_torrent):
    torrent = create_torrent()
    for value in ((), 'http://foo', ['http://foo', 'http://bar'], None):
        torrent.webseeds = value
        assert isinstance(torrent.webseeds, utils.URLs)

def test_webseeds__sync_to_metainfo(create_torrent):
    torrent = create_torrent(webseeds=())
    assert torrent.webseeds == []
    assert 'url-list' not in torrent.metainfo
    torrent.webseeds = ['http://foo']
    assert torrent.webseeds == ['http://foo']
    assert torrent.metainfo['url-list'] == ['http://foo']
    torrent.webseeds.clear()
    assert torrent.webseeds == []
    assert 'url-list' not in torrent.metainfo

def test_webseeds__sync_from_metainfo(create_torrent):
    torrent = create_torrent(webseeds=())
    assert torrent.webseeds == []
    assert 'url-list' not in torrent.metainfo
    torrent.metainfo['url-list'] = ('http://foo', 'http://bar')
    assert torrent.webseeds == ('http://foo', 'http://bar')
    torrent.metainfo['url-list'] = ()
    assert torrent.webseeds == []

def test_webseeds__urls_are_validated(create_torrent):
    torrent = create_torrent()
    with pytest.raises(errors.URLError) as e:
        torrent.webseeds.append('http://foo:bar')
    assert str(e.value) == 'http://foo:bar: Invalid URL'
    with pytest.raises(errors.URLError) as e:
        torrent.webseeds = ['http://foo', 'http://foo:bar']
    assert str(e.value) == 'http://foo:bar: Invalid URL'

def test_webseeds__setting_to_invalid_type(create_torrent):
    torrent = create_torrent()
    with pytest.raises(ValueError) as e:
        torrent.webseeds = 23
    assert str(e.value) == 'Must be Iterable, str or None, not int: 23'

def test_webseeds__addition(create_torrent):
    torrent = create_torrent()
    torrent.webseeds = ['http://foo']
    torrent.webseeds += ['http://bar']
    assert torrent.webseeds == ['http://foo', 'http://bar']


def test_httpseeds__correct_type(create_torrent):
    torrent = create_torrent()
    for value in ((), 'http://foo', ['http://foo', 'http://bar'], None):
        torrent.httpseeds = value
        assert isinstance(torrent.httpseeds, utils.URLs)

def test_httpseeds__sync_to_metainfo(create_torrent):
    torrent = create_torrent(httpseeds=())
    assert torrent.httpseeds == []
    assert 'httpseeds' not in torrent.metainfo
    torrent.httpseeds = ['http://foo']
    assert torrent.httpseeds == ['http://foo']
    assert torrent.metainfo['httpseeds'] == ['http://foo']
    torrent.httpseeds.clear()
    assert torrent.httpseeds == []
    assert 'httpseeds' not in torrent.metainfo

def test_httpseeds__sync_from_metainfo(create_torrent):
    torrent = create_torrent(httpseeds=())
    torrent.metainfo['httpseeds'] = ['http://foo']
    assert torrent.httpseeds == ['http://foo']
    torrent.metainfo['httpseeds'].append('http://bar')
    assert torrent.httpseeds == ['http://foo', 'http://bar']
    torrent.metainfo['httpseeds'] = []
    assert torrent.httpseeds == []

def test_httpseeds__urls_are_validated(create_torrent):
    torrent = create_torrent()
    with pytest.raises(errors.URLError) as e:
        torrent.httpseeds = ['http://foo', 'http://foo:bar']
    assert str(e.value) == 'http://foo:bar: Invalid URL'

def test_httpseeds__setting_to_invalid_type(create_torrent):
    torrent = create_torrent()
    with pytest.raises(ValueError) as e:
        torrent.httpseeds = 23
    assert str(e.value) == 'Must be Iterable, str or None, not int: 23'

def test_httpseeds__addition(create_torrent):
    torrent = create_torrent()
    torrent.httpseeds = ['http://foo']
    torrent.httpseeds += ['http://bar']
    assert torrent.httpseeds == ['http://foo', 'http://bar']


def test_leaving_private_unset_does_not_include_it_in_metainfo(create_torrent):
    torrent = create_torrent()
    assert torrent.private is None
    assert 'private' not in torrent.metainfo['info']

def test_setting_private_always_includes_it_in_metainfo(create_torrent):
    torrent = create_torrent()
    for private in (True, False):
        torrent = create_torrent(private=private)
        assert torrent.private is private
        assert 'private' in torrent.metainfo['info']

def test_setting_private_to_None_removes_it_from_metainfo(create_torrent):
    torrent = create_torrent()
    for private in (True, False):
        torrent = create_torrent(private=private)
        assert torrent.private is private
        torrent.private = None
        assert torrent.private is None
        assert 'private' not in torrent.metainfo['info']

def test_setting_private_enforces_boolean_values(create_torrent):
    torrent = create_torrent()
    torrent.private = 'this evaluates to True'
    assert torrent.private is True
    assert torrent.metainfo['info']['private'] is True

    torrent.private = []  # This evaluates to False
    assert torrent.private is False
    assert torrent.metainfo['info']['private'] is False


def test_comment(create_torrent):
    torrent = create_torrent()
    torrent.comment = ''
    assert torrent.comment == ''
    assert torrent.metainfo['comment'] == ''

    torrent.comment = None
    assert torrent.comment is None
    assert 'comment' not in torrent.metainfo


def test_source(create_torrent):
    torrent = create_torrent()
    torrent.source = ''
    assert torrent.source == ''
    assert torrent.metainfo['info']['source'] == ''

    torrent.source = None
    assert torrent.source is None
    assert 'source' not in torrent.metainfo['info']


def test_creation_date(create_torrent):
    from datetime import datetime

    torrent = create_torrent()
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


def test_created_by(create_torrent):
    torrent = create_torrent()
    torrent.created_by = 'somebody'
    assert torrent.created_by == 'somebody'
    assert torrent.metainfo['created by'] == 'somebody'

    torrent.created_by = None
    assert torrent.created_by is None
    assert 'created by' not in torrent.metainfo


def test_repr_string(singlefile_content):
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
    assert repr(t) == f"Torrent(comment='foo', source='foo', created_by='foo', piece_size={2**20})"


def test_equality(singlefile_content):
    kwargs = {'trackers': ['https://localhost/'],
              'comment': 'Foo',
              'created_by': 'Bar'}
    t1 = torf.Torrent(singlefile_content.path, **kwargs)
    t2 = torf.Torrent(singlefile_content.path, **kwargs)
    assert t1 == t2
    t1.metainfo['foo'] = 'bar'
    assert t1 != t2
    del t1.metainfo['foo']
    assert t1 == t2
    t2.comment = 'asdf'
    assert t1 != t2
    t2.comment = t1.comment
    assert t1 == t2
    t1.trackers += ['https://remotehost']
    assert t1 != t2
    del t1.trackers[-1]
    assert t1 == t2


def check_hash(content, hashname):
    t = torf.Torrent(content.path, trackers=['http://localhost/'],
                     piece_size=content.exp_metainfo['info']['piece length'])
    assert t.piece_size == content.exp_metainfo['info']['piece length']
    t.generate()
    exp_attrs = content.exp_attrs
    assert getattr(t, hashname) == getattr(exp_attrs, hashname)

    del t.metainfo['info']['piece length']
    with pytest.raises(torf.MetainfoError) as excinfo:
        getattr(t, hashname)
    assert str(excinfo.value) == "Invalid metainfo: Missing 'piece length' in ['info']"

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
    t1.generate()
    t2.generate()

    t1.randomize_infohash = False
    t2.randomize_infohash = False
    assert t1.infohash == t2.infohash

    t1.randomize_infohash = True
    t2.randomize_infohash = True
    assert t1.infohash != t2.infohash


def test_copy_before_ready(singlefile_content):
    t1 = torf.Torrent(singlefile_content.path, comment='Asdf.',
                      randomize_infohash=True, webseeds=['http://foo'])
    assert not t1.is_ready
    t2 = t1.copy()
    assert t1 == t2
    assert t1 is not t2

def test_copy_when_ready(singlefile_content):
    t1 = torf.Torrent(singlefile_content.path, comment='Asdf.',
                      randomize_infohash=True, webseeds=['http://foo'])
    t1.generate()
    assert t1.is_ready
    t2 = t1.copy()
    assert t1 == t2
    assert t1 is not t2

def test_copy_with_copy_module(singlefile_content):
    import copy
    t1 = torf.Torrent(singlefile_content.path, comment='Asdf.',
                      randomize_infohash=True, webseeds=['http://foo'])
    t1.generate()

    t2 = copy.copy(t1)
    assert t1 == t2
    assert t1 is not t2

    t2 = copy.deepcopy(t1)
    assert t1 == t2
    assert t1 is not t2
