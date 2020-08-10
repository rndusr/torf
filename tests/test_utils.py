import os
import re
from collections import OrderedDict
from pathlib import Path
from unittest import mock

import pytest

import torf
from torf import _errors as errors
from torf import _utils as utils

ALPHABET = 'abcdefghijklmnopqrstuvwxyz'

def test_read_chunks__unreadable_file():
    with pytest.raises(torf.ReadError) as excinfo:
        tuple(utils.read_chunks('no/such/file', 10))
    assert excinfo.match(r'^no/such/file: No such file or directory$')

def test_read_chunks__readable_file(create_file):
    filepath = create_file('some_file', ALPHABET[:16])
    assert tuple(utils.read_chunks(filepath, 4)) == (b'abcd', b'efgh', b'ijkl', b'mnop')
    assert tuple(utils.read_chunks(filepath, 5)) == (b'abcde', b'fghij', b'klmno', b'p')
    assert tuple(utils.read_chunks(filepath, 5)) == (b'abcde', b'fghij', b'klmno', b'p')

def test_read_chunks__normal_read(create_file):
    filepath = create_file('some_file', ALPHABET[:5])
    assert tuple(utils.read_chunks(filepath, 1)) == (b'a', b'b', b'c', b'd', b'e')
    assert tuple(utils.read_chunks(filepath, 2)) == (b'ab', b'cd', b'e')
    assert tuple(utils.read_chunks(filepath, 3)) == (b'abc', b'de')
    assert tuple(utils.read_chunks(filepath, 4)) == (b'abcd', b'e')
    assert tuple(utils.read_chunks(filepath, 5)) == (b'abcde',)
    assert tuple(utils.read_chunks(filepath, 6)) == (b'abcde',)

def test_read_chunks__prepend_bytes_to_file(create_file):
    filepath = create_file('some_file', ALPHABET[:10])

    assert tuple(utils.read_chunks(filepath, 2, prepend=b'1')) == (b'1a', b'bc', b'de', b'fg', b'hi', b'j')
    assert tuple(utils.read_chunks(filepath, 2, prepend=b'12')) == (b'12', b'ab', b'cd', b'ef', b'gh', b'ij')
    assert tuple(utils.read_chunks(filepath, 2, prepend=b'123')) == (b'12', b'3a', b'bc', b'de', b'fg', b'hi', b'j')
    assert tuple(utils.read_chunks(filepath, 2, prepend=b'1234')) == (b'12', b'34', b'ab', b'cd', b'ef', b'gh', b'ij')

    assert tuple(utils.read_chunks(filepath, 3, prepend=b'1')) == (b'1ab', b'cde', b'fgh', b'ij')
    assert tuple(utils.read_chunks(filepath, 3, prepend=b'12')) == (b'12a', b'bcd', b'efg', b'hij')
    assert tuple(utils.read_chunks(filepath, 3, prepend=b'123')) == (b'123', b'abc', b'def', b'ghi', b'j')
    assert tuple(utils.read_chunks(filepath, 3, prepend=b'1234')) == (b'123', b'4ab', b'cde', b'fgh', b'ij')
    assert tuple(utils.read_chunks(filepath, 3, prepend=b'12345')) == (b'123', b'45a', b'bcd', b'efg', b'hij')
    assert tuple(utils.read_chunks(filepath, 3, prepend=b'123456')) == (b'123', b'456', b'abc', b'def', b'ghi', b'j')


def test_is_power_of_2():
    assert utils.is_power_of_2(0) is False
    for n in range(1, 30):
        assert utils.is_power_of_2(2**n) is True
        assert utils.is_power_of_2(-2**n) is True
        assert utils.is_power_of_2(3**n) is False
        assert utils.is_power_of_2(-5**n) is False


def test_iterable_startswith():
    a = ['a', 'b', 'c', 'd']
    b = ['a', 'b', 'c']
    assert utils.iterable_startswith(a, b)
    assert not utils.iterable_startswith(b, a)
    a = ['a', 'b', 'c']
    b = ['a', 'b', 'c']
    assert utils.iterable_startswith(a, b)
    assert utils.iterable_startswith(b, a)
    a = ['a', 'b', 'c']
    b = []
    assert utils.iterable_startswith(a, b)
    assert not utils.iterable_startswith(b, a)
    a = []
    b = []
    assert utils.iterable_startswith(a, b)
    assert utils.iterable_startswith(b, a)


def test_URL__max_port_number():
    utils.URL(f'http://foohost:{2**16-1}')
    with pytest.raises(torf.URLError):
        utils.URL(f'http://foohost:{2**16}')

def test_URL__min_port_number():
    utils.URL('http://foohost:0')
    with pytest.raises(torf.URLError):
        utils.URL('http://foohost:-1')


def test_real_size_of_directory(tmp_path):
    dir = tmp_path / 'dir' ; dir.mkdir()  # noqa: E702
    subdir = dir / 'subdir' ; subdir.mkdir()  # noqa: E702
    (dir / 'file1').write_bytes(b'\x00' * 100)
    (dir / 'file2').write_bytes(b'\x00' * 200)
    (subdir / 'file3').write_bytes(b'\x00' * 300)
    (subdir / 'file4').write_bytes(b'\x00' * 400)
    assert utils.real_size(dir) == 1000

def test_real_size_of_directory_with_unreadable_file(tmp_path):
    dir = tmp_path / 'dir' ; dir.mkdir()  # noqa: E702
    subdir = dir / 'subdir' ; subdir.mkdir()  # noqa: E702
    (dir / 'file1').write_bytes(b'\x00' * 100)
    (subdir / 'file2').write_bytes(b'\x00' * 200)
    subdir_mode = os.stat(subdir).st_mode
    os.chmod(subdir, mode=0o222)
    try:
        with pytest.raises(errors.ReadError) as exc_info:
            utils.real_size(dir)
        assert str(exc_info.value) == f'{subdir}: Permission denied'
    finally:
        os.chmod(subdir, mode=subdir_mode)

def test_real_size_of_file(tmp_path):
    (tmp_path / 'file').write_bytes(b'\x00' * 123)
    assert utils.real_size(tmp_path / 'file') == 123

def test_real_size_of_nonexising_path():
    with pytest.raises(errors.ReadError) as exc_info:
        utils.real_size('path/doesnt/exist')
    assert str(exc_info.value) == 'path/doesnt/exist: No such file or directory'


@pytest.fixture
def testdir(tmp_path):
    base = tmp_path / 'base'
    base.mkdir()
    foo = base / 'foo'
    foo.mkdir()
    bar = base / '.bar'
    bar.mkdir()
    baz = bar / 'baz'
    baz.mkdir()
    for path in (foo, bar, baz):
        (path / 'empty').write_text('')
        (path / '.empty').write_text('')
        (path / 'not_empty').write_text('dummy content')
        (path / '.not_empty').write_text('more dummy content')
    return base


def test_list_files_with_file(testdir):
    files = [Path(filepath).relative_to(testdir.parent)
             for filepath in utils.list_files(testdir / 'foo/empty')]
    exp = ['base/foo/empty']
    assert files == [Path(p) for p in exp]

def test_list_files_with_directory(testdir):
    files = [Path(filepath).relative_to(testdir.parent)
             for filepath in utils.list_files(testdir)]
    exp = sorted(['base/foo/.empty', 'base/foo/.not_empty', 'base/foo/empty', 'base/foo/not_empty',
                  'base/.bar/.empty', 'base/.bar/.not_empty', 'base/.bar/empty', 'base/.bar/not_empty',
                  'base/.bar/baz/.empty', 'base/.bar/baz/.not_empty', 'base/.bar/baz/empty', 'base/.bar/baz/not_empty'])
    assert files == [Path(p) for p in exp]

def test_list_files_with_unreadable_file(tmp_path):
    file = tmp_path / 'foo.jpg'
    file.write_text('asdf')
    file_mode = os.stat(file).st_mode
    os.chmod(file, mode=0o222)
    try:
        with pytest.raises(errors.ReadError) as exc_info:
            utils.list_files(file)
        assert str(exc_info.value) == f'{file}: Permission denied'
    finally:
        os.chmod(file, mode=file_mode)

def test_list_files_with_unreadable_directory(tmp_path):
    dir = tmp_path / 'dir'
    dir.mkdir()
    file = dir / 'foo.jpg'
    file.write_text('asdf')
    dir_mode = os.stat(dir).st_mode
    os.chmod(dir, mode=0o222)
    try:
        for path in (dir, file):
            with pytest.raises(errors.ReadError) as exc_info:
                utils.list_files(path)
            assert str(exc_info.value) == f'{path}: Permission denied'
    finally:
        os.chmod(dir, mode=dir_mode)

def test_list_files_with_unreadable_file_in_directory(tmp_path):
    dir = tmp_path / 'dir'
    dir.mkdir()
    file = dir / 'foo.jpg'
    file.write_text('asdf')
    file_mode = os.stat(file).st_mode
    os.chmod(file, mode=0o222)
    try:
        with pytest.raises(errors.ReadError) as exc_info:
            utils.list_files(dir)
        assert str(exc_info.value) == f'{file}: Permission denied'
    finally:
        os.chmod(file, mode=file_mode)


def test_filter_files_with_default_arguments():
    filelist = ['base/foo/.hidden', 'base/foo/not_hidden',
                'base/.hidden/.hidden', 'base/.hiddendir/not_hidden',
                'base/.hidden/not_hidden/.hidden', 'base/.hidden/not_hidden/not_hidden']
    assert utils.filter_files(filelist) == filelist

def test_filter_files_without_hidden_files_or_directories():
    filelist = ['base/foo/.hidden', 'base/foo/not_hidden',
                'base/.hidden/.hidden', 'base/.hiddendir/not_hidden',
                'base/.hidden/not_hidden/.hidden', 'base/.hidden/not_hidden/not_hidden']
    assert utils.filter_files(filelist, hidden=False) == ['base/foo/not_hidden']

def test_filter_files_ignores_hidden_parent_directories():
    filelist = ['.base/foo/.hidden',                '.base/foo/not_hidden',
                '.base/.hidden/.hidden',            '.base/.hiddendir/not_hidden',
                '.base/.hidden/not_hidden/.hidden', '.base/.hidden/not_hidden/not_hidden']
    assert utils.filter_files(filelist, hidden=False) == ['.base/foo/not_hidden']

    filelist = ['path/to/.hidden/base/foo/.hidden',                'path/to/.hidden/base/foo/not_hidden',
                'path/to/.hidden/base/.hidden/.hidden',            'path/to/.hidden/base/.hiddendir/not_hidden',
                'path/to/.hidden/base/.hidden/not_hidden/.hidden', 'path/to/.hidden/base/.hidden/not_hidden/not_hidden']
    assert utils.filter_files(filelist, hidden=False) == ['path/to/.hidden/base/foo/not_hidden']

def test_filter_files_without_empty_files(testdir):
    filelist = [str(Path(filepath).relative_to(testdir.parent))
                for filepath in utils.list_files(testdir)]
    cwd = os.getcwd()
    try:
        os.chdir(testdir.parent)
        assert utils.filter_files(filelist, empty=False) == sorted(['base/foo/.not_empty', 'base/foo/not_empty',
                                                                    'base/.bar/.not_empty', 'base/.bar/not_empty',
                                                                    'base/.bar/baz/.not_empty', 'base/.bar/baz/not_empty'])
    finally:
        os.chdir(cwd)

def test_filter_files_exclude_argument(testdir):
    filelist = ['base/foo/bar/baz',
                'base/foo/two/three',
                'base/one/two/foo']
    assert utils.filter_files(filelist, exclude=(re.compile(r'two'),)) == ['base/foo/bar/baz']
    assert utils.filter_files(filelist, exclude=(re.compile(r'foo$'),)) == ['base/foo/bar/baz', 'base/foo/two/three']
    assert utils.filter_files(filelist, exclude=('base/foo/*',)) == ['base/one/two/foo']
    assert utils.filter_files(filelist, exclude=(re.compile(r'foo/bar'),
                                                 '*/one/*')) == ['base/foo/two/three']

def test_filter_files_with_no_common_path(testdir):
    filelist = ['foo/bar/baz',
                'bar/two/three',
                'one/two/foo']
    assert utils.filter_files(filelist) == filelist
    assert utils.filter_files(filelist, exclude=(re.compile(r'bar'),)) == ['one/two/foo']

def test_filter_files_with_absolute_and_relative_paths(testdir):
    filelist = ['foo/bar/one',
                'foo/bar/two',
                '/some/where/foo/bar/three',
                '/some/where/foo/bar/four']
    assert utils.filter_files(filelist) == filelist

def test_filter_files_with_getter_argument(testdir):
    items = [(123, 'foo/bar/baz', 456),
             (123, 'bar/two/three', 456),
             (123, 'one/two/foo', 456)]
    assert utils.filter_files(items, getter=lambda i: i[1],
                              exclude=(re.compile(r'foo'),)) == [(123, 'bar/two/three', 456)]

def test_decoding():
    encoded = {
        b'one': b'foo',
        b'two': 17,
        b'three': [1, b'twelve', [b'x', {b'boo': 800}]],
        b'something': {
            b'four': b'baz',
            b'five': [{b'a': [1, 2, 3], b'b': 4}],
        }
    }
    decoded = {
        'one': 'foo',
        'two': 17,
        'three': [1, 'twelve', ['x', {'boo': 800}]],
        'something': {
            'four': 'baz',
            'five': [{'a': [1, 2, 3], 'b': 4}],
        }
    }
    assert utils.decode_dict(encoded) == decoded


def test_encoding():
    class SillyStr(str):
        def __str__(self):
            return f'This is silly: {super().__str__()}'
        __repr__ = __str__

    decoded = {
        'one': SillyStr('foo'),
        'two': 17.3,
        'three': (1, 'twelve', ['x', OrderedDict([('boo', range(3))])]),
        'something': {
            'four': 'baz',
            'five': [{'a': (1, 2, 3), 'b': -4}],
        }
    }
    encoded = {
        b'one': b'This is silly: foo',
        b'two': 17,
        b'three': [1, b'twelve', [b'x', {b'boo': [0, 1, 2]}]],
        b'something': {
            b'four': b'baz',
            b'five': [{b'a': [1, 2, 3], b'b': -4}],
        }
    }
    assert utils.encode_dict(decoded) == encoded


def test_Filepath_is_equal_to_absolute_path():
    assert utils.Filepath('/some/path/to/a/file') == utils.Filepath('/some/path/to/a/file')
    assert utils.Filepath('/some/path/to/a/file') == '/some/path/to/a/file'
    assert '/some/path/to/a/file' == utils.Filepath('/some/path/to/a/file')

def test_Filepath_is_equal_to_relative_path(tmp_path):
    orig_cwd = os.getcwd()
    os.chdir(tmp_path.parent)
    abspath = str(tmp_path / 'foo')
    relpath = f'{tmp_path.parts[-1]}/foo'
    Path(abspath).write_text('bar')
    try:
        assert utils.Filepath(abspath) == utils.Filepath(relpath)
        assert utils.Filepath(abspath) == relpath
        assert relpath == utils.Filepath(abspath)
        assert utils.Filepath(relpath) == utils.Filepath(abspath)
        assert utils.Filepath(relpath) == abspath
        assert abspath == utils.Filepath(relpath)
        assert relpath == utils.Filepath(relpath)
        assert utils.Filepath(relpath) == relpath
    finally:
        os.chdir(orig_cwd)
    assert utils.Filepath(abspath) != utils.Filepath(relpath)
    assert utils.Filepath(abspath) != relpath
    assert relpath != utils.Filepath(abspath)
    assert utils.Filepath(relpath) != utils.Filepath(abspath)
    assert utils.Filepath(relpath) != abspath
    assert abspath != utils.Filepath(relpath)
    assert utils.Filepath(relpath) == relpath
    assert relpath == utils.Filepath(relpath)

def test_Filepath_is_equal_to_symlink(tmp_path):
    path = tmp_path / 'foo'
    Path(path).write_text('bar')
    abspath = str(tmp_path / 'foo.link')
    relpath = './foo.link'
    Path(abspath).symlink_to('foo')

    assert utils.Filepath(abspath) == utils.Filepath(path)
    assert utils.Filepath(abspath) == path
    assert path == utils.Filepath(abspath)
    assert utils.Filepath(path) == abspath

    orig_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        assert utils.Filepath(relpath) == utils.Filepath(path)
        assert utils.Filepath(relpath) == path
        assert path == utils.Filepath(relpath)
        assert utils.Filepath(path) == relpath
    finally:
        os.chdir(orig_cwd)


def test_Filepaths_accepts_string_or_iterable():
    assert utils.Filepaths('path/to/foo.jpg') == [Path('path/to/foo.jpg')]
    assert utils.Filepaths(('path/to/foo.jpg',)) == [Path('path/to/foo.jpg')]
    assert utils.Filepaths(['path/to/foo.jpg']) == [Path('path/to/foo.jpg')]

def test_Filepaths_deduplicates_when_initializing():
    fps = utils.Filepaths(('path/to/foo.jpg', 'path/to/bar.jpg', 'path/to/foo.jpg'))
    assert fps == (Path('path/to/foo.jpg'), Path('path/to/bar.jpg'))

def test_Filepaths_deduplicates_when_setting():
    fps = utils.Filepaths(('path/to/foo.jpg', 'path/to/bar.jpg'))
    fps.append('path/to/foo.jpg')
    fps.extend(('path/to/bar.jpg',))
    assert fps == (Path('path/to/foo.jpg'), Path('path/to/bar.jpg'))

def test_Filepaths_deduplicates_when_inserting():
    fps = utils.Filepaths(('path/to/foo.jpg', 'path/to/bar.jpg'))
    fps.insert(0, 'path/to/bar.jpg')
    assert fps == (Path('path/to/foo.jpg'), Path('path/to/bar.jpg'))

def test_Filepaths_treats_relative_paths_as_equal_to_their_absolute_versions(tmp_path):
    (tmp_path / 'cwd').mkdir()
    cwd = os.getcwd()
    try:
        os.chdir(tmp_path / 'cwd')
        fps = utils.Filepaths((Path('foo'),))
        assert fps == ('foo',)

        fps.append(tmp_path / 'cwd' / 'foo')
        assert fps == ('foo',)
        fps.append(tmp_path / 'cwd' / 'bar')
        fps.append('bar')
        assert fps == ('foo', tmp_path / 'cwd' / 'bar')
    finally:
        os.chdir(cwd)

def test_Filepaths_handles_directories(tmp_path):
    # Create directory with 2 files
    content = tmp_path / 'content' ; content.mkdir()  # noqa: E702
    for f in ('a', 'b'): (content / f).write_text('<data>')
    fps = utils.Filepaths((content,))
    assert fps == (content / 'a', content / 'b')

    # Replace one file with multilevel subdirectory
    subdir = content / 'b' ; subdir.unlink() ; subdir.mkdir()  # noqa: E702
    for f in ('c', 'd'): (subdir / f).write_text('<subdata>')
    subsubdir = subdir / 'subsubdir' ; subsubdir.mkdir()  # noqa: E702
    for f in ('e', 'f'): (subsubdir / f).write_text('<subdata>')
    fps[1] = content / 'b'
    assert fps == (content / 'a', subdir / 'c', subdir / 'd', subsubdir / 'e', subsubdir / 'f')

    # Replace subdirectory with file again
    for f in (subdir / 'c', subdir / 'd', subsubdir / 'e', subsubdir / 'f'):
        f.unlink()
    subsubdir.rmdir()
    subdir.rmdir()
    (content / 'b').write_text('I AM BACK')
    fps[1] = content / 'b'
    assert fps == (content / 'a', content / 'b')

def test_Filepaths_calls_callback_after_appending():
    cb = mock.MagicMock()
    fps = utils.Filepaths(('path/to/foo.jpg',), callback=cb)
    fps.append('path/to/baz.jpg')
    cb.assert_called_once_with(fps)

def test_Filepaths_calls_callback_after_removing():
    cb = mock.MagicMock()
    fps = utils.Filepaths(('path/to/foo.jpg',), callback=cb)
    del fps[0]
    cb.assert_called_once_with(fps)

def test_Filepaths_calls_callback_after_inserting():
    cb = mock.MagicMock()
    fps = utils.Filepaths(('path/to/foo.jpg',), callback=cb)
    fps.insert(0, 'path/to/baz.jpg')
    cb.assert_called_once_with(fps)

def test_Filepaths_calls_callback_after_clearing():
    cb = mock.MagicMock()
    fps = utils.Filepaths(('path/to/foo.jpg',), callback=cb)
    fps.clear()
    cb.assert_called_once_with(fps)


def test_URLs_accepts_string_or_iterable():
    urls = utils.URLs('http://foo:123')
    assert urls == utils.URLs(('http://foo:123',))
    assert urls == utils.URLs(['http://foo:123'])

def test_URLs_interprets_empty_string_as_empty_list():
    urls = utils.URLs('')
    assert urls == ()

def test_URLs_deduplicates_when_initializing():
    urls = utils.URLs(('http://foo:123', 'http://bar:456', 'http://foo:123'))
    assert urls == ['http://foo:123', 'http://bar:456']

def test_URLs_deduplicates_when_setting():
    urls = utils.URLs(('http://foo:123', 'http://bar:456'))
    urls.append('http://foo:123')
    urls.append('http://bar:456')
    urls.extend(('http://foo:123', 'http://bar:456'))
    assert urls == ['http://foo:123', 'http://bar:456']

def test_URLs_deduplicates_when_inserting():
    urls = utils.URLs(('http://foo:123', 'http://bar:456'))
    urls.insert(1, 'http://foo:123')
    urls.insert(0, 'http://bar:456')
    urls.insert(0, 'http://foo:123')
    urls.insert(1, 'http://bar:456')
    assert urls == ['http://foo:123', 'http://bar:456']

def test_URLs_validates_initial_urls():
    with pytest.raises(errors.URLError) as e:
        utils.URLs(('http://foo:123', 'http://bar:456:789'))
    assert str(e.value) == 'http://bar:456:789: Invalid URL'

def test_URLs_validates_appended_urls():
    urls = utils.URLs('http://foo:123')
    with pytest.raises(errors.URLError) as e:
        urls.append('http://bar:456:789')
    assert str(e.value) == 'http://bar:456:789: Invalid URL'
    assert urls == ('http://foo:123',)

def test_URLs_validates_changed_urls():
    urls = utils.URLs('http://foo:123')
    with pytest.raises(errors.URLError) as e:
        urls[0] = 'http://bar:456:789'
    assert str(e.value) == 'http://bar:456:789: Invalid URL'
    assert urls == ('http://foo:123',)

def test_URLs_validates_inserted_urls():
    urls = utils.URLs(('http://foo:123', 'http://bar:456'))
    with pytest.raises(errors.URLError) as e:
        urls.insert(1, 'http://baz:789:abc')
    assert str(e.value) == 'http://baz:789:abc: Invalid URL'
    assert urls == ('http://foo:123', 'http://bar:456')

def test_URLs_does_not_empty_when_replacing_with_invalid_URLs():
    urls = utils.URLs(('http://foo:123', 'http://bar:456'))
    with pytest.raises(errors.URLError):
        urls.replace(('http://baz:789:abc',))
    assert urls == ('http://foo:123', 'http://bar:456')

def test_URLs_is_equal_to_URLs_instances():
    t1 = utils.URLs(('http://foo:123', 'http://bar:456'))
    t2 = utils.URLs(('http://foo:123', 'http://bar:456'))
    assert t1 == t2
    t2 = utils.URLs(('http://foo:123', 'http://baz:789'))
    assert t1 != t2

def test_URLs_is_equal_to_iterables():
    urls = utils.URLs(('http://foo:123', 'http://bar:456'))
    assert urls == ['http://foo:123', 'http://bar:456']
    assert urls == ('http://foo:123', 'http://bar:456')

def test_URLs_is_equal_to_any_combination_of_the_same_urls():
    urls = utils.URLs(('http://foo:123', 'http://bar:456', 'http://baz:789'))
    assert urls == ('http://foo:123', 'http://bar:456', 'http://baz:789')
    assert urls == ('http://bar:456', 'http://foo:123', 'http://baz:789')
    assert urls == ('http://bar:456', 'http://foo:123', 'http://baz:789')
    assert urls == ('http://foo:123', 'http://baz:789', 'http://bar:456')

def test_URLs_calls_callback_after_appending():
    cb = mock.MagicMock()
    urls = utils.URLs(('http://foo:123', 'http://bar:456'), callback=cb)
    urls.append('http://baz:789')
    cb.assert_called_once_with(urls)

def test_URLs_calls_callback_after_removing():
    cb = mock.MagicMock()
    urls = utils.URLs(('http://foo:123', 'http://bar:456'), callback=cb)
    urls.remove('http://bar:456')
    cb.assert_called_once_with(urls)

def test_URLs_calls_callback_after_inserting():
    cb = mock.MagicMock()
    urls = utils.URLs(('http://foo:123', 'http://bar:456'), callback=cb)
    urls.insert(0, 'http://baz:789')
    cb.assert_called_once_with(urls)

def test_URLs_calls_callback_after_clearing():
    cb = mock.MagicMock()
    urls = utils.URLs(('http://foo:123', 'http://bar:456'), callback=cb)
    urls.clear()
    cb.assert_called_once_with(urls)

def test_URLs_equality():
    urls = utils.URLs(('http://foo:123', 'http://bar:456'))
    assert urls == ('http://foo:123', 'http://bar:456')
    assert urls == ['http://foo:123', 'http://bar:456']
    assert urls != ['http://foo:124', 'http://bar:456']
    assert urls != 'http://bar:456'
    assert urls != 5
    assert urls is not None

def test_URLs_can_be_added():
    urls1 = utils.URLs(('http://foo:123', 'http://bar:456'))
    urls2 = utils.URLs(('http://bar', 'http://baz'))
    assert urls1 + urls2 == ('http://foo:123', 'http://bar:456',
                             'http://bar', 'http://baz')
    assert urls1 + ('http://bar',) == ('http://foo:123', 'http://bar:456',
                                       'http://bar')
    assert urls1 + 'http://baz' == ('http://foo:123', 'http://bar:456',
                                    'http://baz')

def test_URLs_replace():
    cb = mock.MagicMock()
    urls = utils.URLs(('http://foo:123', 'http://bar:456'),
                      callback=cb)
    urls.replace(['http://asdf', 'http://quux'])
    assert urls == ['http://asdf', 'http://quux']
    assert cb.call_args_list == [mock.call(urls)]


def test_Trackers_ensures_tiers_when_initializing():
    for args in (('http://foo:123', 'http://bar:456'),
                 (['http://foo:123'], 'http://bar:456'),
                 ('http://foo:123', ['http://bar:456']),
                 (['http://foo:123'], ['http://bar:456'])):
        tiers = utils.Trackers(args)
        for tier in tiers:
            assert isinstance(tier, utils.URLs)
        assert tiers == [['http://foo:123'], ['http://bar:456']]

def test_Trackers_ensures_tiers_when_setting():
    for args in (('http://foo:123', 'http://bar:456'),
                 (['http://foo:123'], 'http://bar:456'),
                 ('http://foo:123', ['http://bar:456']),
                 (['http://foo:123'], ['http://bar:456'])):
        tiers = utils.Trackers('http://quux')
        tiers.extend(args)
        for tier in tiers:
            assert isinstance(tier, utils.URLs)
        assert tiers == [['http://quux'], ['http://foo:123'], ['http://bar:456']]

        tiers = utils.Trackers('http://quux')
        tiers.append(args)
        for tier in tiers:
            assert isinstance(tier, utils.URLs)
        assert tiers == [['http://quux'], ['http://foo:123', 'http://bar:456']]

def test_Trackers_ensures_tiers_when_inserting():
    for args in (('http://foo:123', 'http://bar:456'),
                 (['http://foo:123'], 'http://bar:456'),
                 ('http://foo:123', ['http://bar:456']),
                 (['http://foo:123'], ['http://bar:456'])):
        tiers = utils.Trackers('http://quux')
        tiers.insert(1, args)
        for tier in tiers:
            assert isinstance(tier, utils.URLs)
        assert tiers == [['http://quux'], ['http://foo:123', 'http://bar:456']]

def test_Trackers_equality():
    urls = utils.Trackers(('http://foo:123', 'http://bar:456'))
    assert urls == utils.Trackers(('http://foo:123', 'http://bar:456'))
    assert urls != utils.Trackers(('http://foo:123', 'http://bar:4567'))
    assert urls == utils.Trackers(('http://foo:123', 'http://bar:456'), callback=lambda _: None)
    assert urls == [['http://foo:123'], ('http://bar:456',)]
    assert urls != [['http://foo:123'], 'http://bar:456']
    assert urls == (('http://foo:123',), ['http://bar:456'])
    assert urls != (('http://foo:123',), [['http://bar:456']])
    urls_ = utils.Trackers('http://foo:123')
    assert urls != urls_
    urls_.append('http://bar:456')
    assert urls == urls_

def test_Trackers_can_be_added():
    urls1 = utils.Trackers((('http://foo', 'http://bar'), 'http://baz'))
    urls2 = utils.Trackers(('http://a', ('http://b', 'http://c'), 'http://d'))
    assert urls1 + urls2 == (('http://foo', 'http://bar', 'http://a'),
                             ('http://baz', 'http://b', 'http://c'),
                             ('http://d',))
    assert urls1 + ('http://x',) == (('http://foo', 'http://bar', 'http://x'),
                                     ('http://baz',))
    assert urls2 + ('http://x',) == (('http://a','http://x'),
                                     ('http://b', 'http://c'),
                                     ('http://d',))
    assert urls1 + (('http://x', 'http://y'),
                    'http://z') == (('http://foo', 'http://bar', 'http://x', 'http://y'),
                                    ('http://baz', 'http://z'))
    assert urls2 + (('http://x', 'http://y'),
                    'http://z') == (('http://a', 'http://x', 'http://y'),
                                    ('http://b', 'http://c', 'http://z'),
                                    ('http://d',))
    assert urls1 + (('http://x',),
                    'http://z',
                    ('http://123', 'http://456')) == (('http://foo', 'http://bar', 'http://x'),
                                                      ('http://baz', 'http://z'),
                                                      ('http://123', 'http://456'))

def test_Trackers_callback():
    def assert_type(arg):
        assert type(arg) is utils.Trackers

    cb = mock.MagicMock()
    cb.side_effect = assert_type
    tiers = utils.Trackers(('http://foo:123', 'http://bar:456'), callback=cb)
    assert cb.call_args_list == []
    tiers.append('http://baz:789')
    assert cb.call_args_list == [mock.call(tiers)]
    del tiers[0]
    assert cb.call_args_list == [mock.call(tiers)] * 2
    tiers.insert(0, ['http://quux'])
    assert cb.call_args_list == [mock.call(tiers)] * 3
    tiers[0].append('http://asdf')
    assert cb.call_args_list == [mock.call(tiers)] * 4
    tiers[2].remove('http://baz:789')
    assert cb.call_args_list == [mock.call(tiers)] * 5
    tiers.clear()
    assert cb.call_args_list == [mock.call(tiers)] * 6

def test_Trackers_removes_empty_tier_automatically():
    tiers = utils.Trackers(('http://foo:123', 'http://bar:456'))
    assert tiers == [['http://foo:123'], ['http://bar:456']]
    tiers[0].remove('http://foo:123')
    assert tiers == [['http://bar:456']]

def test_Trackers_deduplicates_urls_automatically_when_initializing():
    tiers = utils.Trackers((['http://foo:123', 'http://bar:456', 'http://baz:789'],
                            ['http://quux', 'http://foo:123', 'http://asdf'],
                            ['http://asdf', 'http://baz:789', 'http://flim']))
    assert tiers == [['http://foo:123', 'http://bar:456', 'http://baz:789'],
                     ['http://quux', 'http://asdf'],
                     ['http://flim']]

def test_Trackers_deduplicates_urls_automatically_when_setting():
    tiers = utils.Trackers((['http://foo:123', 'http://bar:456', 'http://baz:789'],))
    tiers.append(['http://quux', 'http://foo:123'])
    assert tiers == [['http://foo:123', 'http://bar:456', 'http://baz:789'],
                     ['http://quux']]
    tiers.append(['http://foo:123', 'http://bar:456', 'http://baz:789'])
    tiers.append('http://quux')
    assert tiers == [['http://foo:123', 'http://bar:456', 'http://baz:789'],
                     ['http://quux']]

def test_Trackers_deduplicates_urls_automatically_when_inserting():
    tiers = utils.Trackers((['http://foo:123', 'http://bar:456', 'http://baz:789'],))
    tiers.insert(0, ['http://asdf', 'http://baz:789', 'http://quux', 'http://foo:123'])
    assert tiers == [['http://asdf', 'http://quux'],
                     ['http://foo:123', 'http://bar:456', 'http://baz:789']]
    tiers = utils.Trackers((['http://foo:123', 'http://bar:456', 'http://baz:789'],))
    tiers.insert(1, ['http://asdf', 'http://baz:789', 'http://quux', 'http://foo:123'])
    assert tiers == [['http://foo:123', 'http://bar:456', 'http://baz:789'],
                     ['http://asdf', 'http://quux']]

def test_Trackers_flat_property():
    tiers = utils.Trackers((['http://foo:123'], ['http://bar:456']))
    assert tiers.flat == ('http://foo:123', 'http://bar:456')

def test_Trackers_replace():
    cb = mock.MagicMock()
    tiers = utils.Trackers((['http://foo:123'], ['http://bar:456']), callback=cb)
    cb.reset_mock()
    tiers.replace(('http://asdf', ('http://qux', 'http://quux'), 'http://qaax'))
    assert tiers == (['http://asdf'], ['http://qux', 'http://quux'], ['http://qaax'])
    assert cb.call_args_list == [mock.call(tiers)]


def test_download_from_invalid_url():
    with pytest.raises(torf.URLError) as excinfo:
        utils.download('http://foo:bar')
    assert str(excinfo.value) == 'http://foo:bar: Invalid URL'

def test_download_from_url_with_unsupported_protocol():
    with pytest.raises(torf.ConnectionError) as excinfo:
        utils.download('asdf://foo:bar')
    assert str(excinfo.value) == 'asdf://foo:bar: Unsupported protocol'

def test_download_from_unconnectable_url(free_port):
    with pytest.raises(torf.ConnectionError) as excinfo:
        utils.download(f'http://localhost:{free_port}')
    assert str(excinfo.value) == f'http://localhost:{free_port}: Connection refused'

def test_download_from_connectable_url(httpserver):
    httpserver.expect_request('/foo').respond_with_data(b'bar')
    assert utils.download(httpserver.url_for('/foo')) == b'bar'

def test_download_with_zero_timeout(httpserver):
    with pytest.raises(torf.ConnectionError) as excinfo:
        utils.download('some/url', timeout=0)
    assert str(excinfo.value) == 'some/url: Timed out'
    with pytest.raises(torf.ConnectionError) as excinfo:
        utils.download('some/url', timeout=-1)
    assert str(excinfo.value) == 'some/url: Timed out'
