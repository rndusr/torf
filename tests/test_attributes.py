import torf
from torf import _utils as utils
from torf import _errors as errors

import pytest
from unittest.mock import patch
import os


def test_path_doesnt_exist(mktorrent, tmpdir):
    torrent = mktorrent()
    with pytest.raises(torf.PathNotFoundError) as excinfo:
        torrent.path = '/this/path/does/not/exist'
    assert excinfo.match('^/this/path/does/not/exist: No such file or directory$')

def test_path_is_empty_directory(mktorrent, tmpdir):
    torrent = mktorrent()
    empty = tmpdir.mkdir('empty')
    with pytest.raises(torf.PathEmptyError) as excinfo:
        torrent.path = empty
    assert excinfo.match(f'^{str(empty)}: Empty directory$')

def test_path_is_empty_file(mktorrent, tmpdir):
    torrent = mktorrent()
    empty = tmpdir.join('empty')
    empty.write('')
    with pytest.raises(torf.PathEmptyError) as excinfo:
        torrent.path = empty
    assert excinfo.match(f'^{str(empty)}: Empty file$')

def test_path_is_directory_with_empty_file(mktorrent, tmpdir):
    torrent = mktorrent()
    empty = tmpdir.mkdir('empty')
    empty_file = empty.join('nothing')
    empty_file.write('')
    with pytest.raises(torf.PathEmptyError) as excinfo:
        torrent.path = empty
    assert excinfo.match(f'^{str(empty)}: Empty directory$')

def test_path_reset(mktorrent, singlefile_content, multifile_content):
    torrent = mktorrent()
    torrent.path = singlefile_content.path
    torrent.private = True
    torrent.path = multifile_content.path
    assert torrent.metainfo['info']['private'] == True
    torrent.path = None
    assert torrent.metainfo['info']['private'] == True
    assert 'pieces' not in torrent.metainfo['info']

def test_path_switch_from_singlefile_to_multifile(mktorrent, singlefile_content, multifile_content):
    torrent = mktorrent()
    torrent.path = singlefile_content.path
    for key in ('piece length', 'name', 'length'):
        assert key in torrent.metainfo['info']
    assert 'files' not in torrent.metainfo['info']

    torrent.path = multifile_content.path
    for key in ('piece length', 'name', 'files'):
        assert key in torrent.metainfo['info']
    assert 'length' not in torrent.metainfo['info']

def test_path_switch_from_multifile_to_singlefile(mktorrent, singlefile_content, multifile_content):
    torrent = mktorrent()
    torrent.path = multifile_content.path
    for key in ('piece length', 'name', 'files'):
        assert key in torrent.metainfo['info']
    assert 'length' not in torrent.metainfo['info']

    torrent.path = singlefile_content.path
    for key in ('piece length', 'name', 'length'):
        assert key in torrent.metainfo['info']
    assert 'files' not in torrent.metainfo['info']

def test_path_is_period(mktorrent, multifile_content):
    torrent = mktorrent()
    cwd = os.getcwd()
    try:
        os.chdir(multifile_content.path)
        torrent.path = '.'
        assert torrent.name == os.path.basename(multifile_content.path)
    finally:
        os.chdir(cwd)

def test_path_is_double_period(mktorrent, multifile_content):
    torrent = mktorrent()
    cwd = os.getcwd()
    try:
        os.chdir(multifile_content.path)
        torrent.path = '..'
        assert torrent.name == os.path.basename(os.path.dirname(multifile_content.path))
    finally:
        os.chdir(cwd)


def test_mode(singlefile_content, multifile_content):
    torrent = torf.Torrent()
    assert torrent.mode is None
    torrent.path = singlefile_content.path
    assert torrent.mode == 'singlefile'
    torrent.path = multifile_content.path
    assert torrent.mode == 'multifile'


def test_files_singlefile(mktorrent, singlefile_content):
    torrent = mktorrent()
    torrent.path = singlefile_content.path
    exp_files1 = (singlefile_content.exp_metainfo['info']['name'],)
    exp_files2 = (torrent.name,)
    assert tuple(torrent.files) == exp_files1
    assert tuple(torrent.files) == exp_files2

def test_files_multifile(mktorrent, multifile_content):
    torrent = mktorrent()
    torrent.path = multifile_content.path
    torrent_name = os.path.basename(multifile_content.path)
    exp_files1 = tuple(os.path.join(torrent_name, os.path.join(*fileinfo['path']))
                       for fileinfo in multifile_content.exp_metainfo['info']['files'])
    exp_files2 = tuple(os.path.join(torrent.name, os.path.join(*fileinfo['path']))
                       for fileinfo in torrent.metainfo['info']['files'])
    assert tuple(torrent.files) == exp_files1
    assert tuple(torrent.files) == exp_files2

def test_files_with_no_path(mktorrent):
    torrent = mktorrent()
    assert tuple(torrent.files) == ()

def test_files_with_no_name(mktorrent, multifile_content):
    torrent = mktorrent(path=multifile_content.path)
    torrent.path = None
    del torrent.metainfo['info']['name']
    with pytest.raises(RuntimeError) as e:
        tuple(torrent.files)
    assert str(e.value) == 'Torrent has no name'


def test_filepaths_singlefile(mktorrent, singlefile_content):
    torrent = mktorrent()
    torrent.path = singlefile_content.path
    exp_filepaths1 = (singlefile_content.path,)
    exp_filepaths2 = (torrent.path,)
    assert tuple(torrent.filepaths) == exp_filepaths1
    assert tuple(torrent.filepaths) == exp_filepaths2

def test_filepaths_multifile(mktorrent, multifile_content):
    torrent = mktorrent()
    torrent.path = multifile_content.path
    exp_filepaths1 = tuple(os.path.join(multifile_content.path, os.path.join(*fileinfo['path']))
                           for fileinfo in multifile_content.exp_metainfo['info']['files'])
    exp_filepaths2 = tuple(os.path.join(torrent.path, os.path.join(*fileinfo['path']))
                           for fileinfo in torrent.metainfo['info']['files'])
    assert tuple(torrent.filepaths) == exp_filepaths1
    assert tuple(torrent.filepaths) == exp_filepaths2

def test_filepaths_with_no_path(mktorrent):
    torrent = mktorrent()
    assert tuple(torrent.filepaths) == ()


def test_filetree_with_no_path(mktorrent):
    torrent = mktorrent()
    assert torrent.filetree == {}


def test_exclude(mktorrent, multifile_content, tmpdir):
    torrent = mktorrent()
    root = tmpdir.mkdir('content')
    subdir1 = root.mkdir('subdir1')
    file1 = subdir1.join('file1.jpg')
    file1.write('data1')
    file2 = subdir1.join('file2.jpg')
    file2.write('data2')
    subdir2 = root.mkdir('subdir2')
    file3 = subdir2.join('file3.txt')
    file3.write('data3')
    file4 = subdir2.join('file4.txt')
    file4.write('data4')

    torrent.path = str(root)
    assert tuple(torrent.filepaths) == (file1, file2, file3, file4)

    torrent.exclude = ['*.txt']
    assert tuple(torrent.filepaths) == (file1, file2)


def test_name(mktorrent, singlefile_content, multifile_content):
    torrent = mktorrent()
    def generate_exp_files(content, torrent_name):
        if content is singlefile_content:
            return (torrent_name,)
        else:
            import glob
            filewalker = (f for f in glob.iglob(os.path.join(content.path, '**'), recursive=True)
                          if os.path.isfile(f))
            rootdir_len = len(os.path.dirname(content.path)) + 1  # Include final '/'
            rel_paths = sorted(path[rootdir_len:] for path in filewalker)
            exp_files = tuple(torrent_name + os.sep + os.path.join(*path.split(os.sep)[1:])
                              for path in rel_paths)
            return exp_files

    def generate_exp_filepaths(content):
        if content is singlefile_content:
            return (content.path,)
        else:
            import glob
            return tuple(sorted(f for f in glob.iglob(os.path.join(content.path, '**'), recursive=True)
                                if os.path.isfile(f)))

    for content in (singlefile_content, multifile_content):
        torrent.name = None

        torrent.path = content.path
        assert torrent.name == os.path.basename(torrent.path)
        assert tuple(torrent.files) == generate_exp_files(content, os.path.basename(content.path))
        assert tuple(torrent.filepaths) == generate_exp_filepaths(content)
        for fp in torrent.filepaths:
            assert os.path.exists(fp)

        torrent.name = 'Any name should be allowed'
        assert torrent.name == 'Any name should be allowed'
        assert tuple(torrent.files) == generate_exp_files(content, 'Any name should be allowed')
        assert tuple(torrent.filepaths) == generate_exp_filepaths(content)
        for fp in torrent.filepaths:
            assert os.path.exists(fp)

        torrent.path = None
        assert torrent.name == 'Any name should be allowed'
        assert tuple(torrent.files) == generate_exp_files(content, 'Any name should be allowed')
        assert tuple(torrent.filepaths) == ()

        torrent.name = 'foo'
        assert torrent.name == 'foo'
        assert tuple(torrent.files) == generate_exp_files(content, 'foo')
        assert tuple(torrent.filepaths) == ()

        torrent.path = content.path
        assert torrent.name == os.path.basename(torrent.path)
        assert tuple(torrent.files) == generate_exp_files(content, os.path.basename(torrent.path))
        assert tuple(torrent.filepaths) == generate_exp_filepaths(content)
        for fp in torrent.filepaths:
            assert os.path.exists(fp)


def test_size(mktorrent, singlefile_content, multifile_content):
    torrent = mktorrent()
    assert torrent.size is None
    for content in (singlefile_content, multifile_content):
        torrent.path = content.path
        assert torrent.size == content.exp_attrs.size


def test_piece_size(mktorrent, multifile_content):
    torrent = mktorrent()
    torrent.path = multifile_content.path

    with pytest.raises(RuntimeError) as excinfo:
        torf.Torrent().piece_size = None
    assert excinfo.match('^Cannot calculate piece size with no "path" specified$')

    assert torf.Torrent().piece_size is None

    with patch.object(torf.Torrent, 'calculate_piece_size', lambda self, size: 512 * 1024):
        torrent.piece_size = None
    assert 'piece length' in torrent.metainfo['info']
    assert torrent.metainfo['info']['piece length'] == 512 * 1024

    torrent.piece_size = 32 * 1024
    assert torrent.piece_size == 32 * 1024
    assert torrent.metainfo['info']['piece length'] == 32 * 1024

    with pytest.raises(torf.PieceSizeError) as excinfo:
        torrent.piece_size = 123 * 1000
    assert excinfo.match('^Piece size must be a power of 2: 123000$')

    with patch.multiple(torf.Torrent, piece_size_min=16, piece_size_max=128):
        with pytest.raises(torf.PieceSizeError) as excinfo:
            torrent.piece_size = 8
        assert excinfo.match('^Piece size must be between 16 and 128: 8$')
        with pytest.raises(torf.PieceSizeError) as excinfo:
            torrent.piece_size = 256
        assert excinfo.match('^Piece size must be between 16 and 128: 256$')

    with pytest.raises(ValueError) as excinfo:
        torrent.piece_size = 'hello'
    assert excinfo.match("^piece_size must be int, not 'hello'$")

    # Anything goes if the metainfo is edited directly
    torrent.metainfo['info']['piece length'] = 256
    torrent.metainfo['info']['piece length'] = 123
    torrent.metainfo['info']['piece length'] = -12


def test_hashes(mktorrent, multifile_content):
    torrent = mktorrent()
    assert torrent.hashes is None
    torrent.path = multifile_content.path
    torrent.piece_size = multifile_content.exp_metainfo['info']['piece length']
    assert torrent.hashes is None
    torrent.generate()
    hashes_string = multifile_content.exp_metainfo['info']['pieces']
    assert torrent.hashes == tuple(hashes_string[pos:pos+20]
                                   for pos in range(0, len(hashes_string), 20))
    torrent.path = None
    assert torrent.hashes is None


def test_calculate_piece_size():
    assert torf.Torrent().calculate_piece_size(1) == 16 * 1024             # minimum is 16 KiB
    assert torf.Torrent().calculate_piece_size(100 * 2**20) == 128 * 1024  # 100 MiB => 128 KiB
    assert torf.Torrent().calculate_piece_size(500 * 2**20) == 512 * 1024  # 100 MiB => 512 KiB
    assert torf.Torrent().calculate_piece_size(999 * 2**20) ==      2**20  # 999 MiB =>  1 MiB
    assert torf.Torrent().calculate_piece_size(  2 * 2**30) ==      2**20  #   2 GiB =>  1 MiB
    assert torf.Torrent().calculate_piece_size(  4 * 2**30) ==  2 * 2**20  #   4 GiB =>  2 MiB
    assert torf.Torrent().calculate_piece_size(  8 * 2**30) ==  4 * 2**20  #   8 GiB =>  4 MiB
    assert torf.Torrent().calculate_piece_size( 16 * 2**30) ==  8 * 2**20  #  16 GiB =>  8 MiB
    assert torf.Torrent().calculate_piece_size( 32 * 2**30) ==  8 * 2**20  #  32 GiB =>  8 MiB
    assert torf.Torrent().calculate_piece_size( 64 * 2**30) ==  8 * 2**20  #  64 GiB =>  8 MiB
    assert torf.Torrent().calculate_piece_size( 80 * 2**30) == 16 * 2**20  #  80 GiB => 16 MiB
    assert torf.Torrent().calculate_piece_size(2**1000)     == 16 * 2**20  #  16 MiB is max


def test_trackers__correct_type(mktorrent):
    torrent = mktorrent()
    assert isinstance(torrent.trackers, utils.Trackers)
    torrent.trackers = ('http://foo', ('http://bar', 'http://baz'))
    assert isinstance(torrent.trackers, utils.Trackers)

def test_trackers__set_to_invalid_type(mktorrent):
    torrent = mktorrent()
    with pytest.raises(ValueError) as e:
        torrent.trackers = 17
    assert str(e.value) == 'Must be Iterable, str or None, not int: 17'

def test_trackers__set_to_None(mktorrent):
    torrent = mktorrent()
    torrent.trackers = ('http://foo', ('http://bar', 'http://baz'))
    trackers_id = id(torrent.trackers)
    torrent.trackers = None
    assert torrent.trackers == []
    assert 'announce' not in torrent.metainfo
    assert 'announce-list' not in torrent.metainfo

def test_trackers__metainfo_is_updated(mktorrent):
    torrent = mktorrent()
    trackers_id = id(torrent.trackers)
    torrent.trackers = ('http://foo', 'http://bar')
    assert id(torrent.trackers) == trackers_id
    assert torrent.trackers == [['http://foo'], ['http://bar']]
    assert torrent.metainfo['announce'] == 'http://foo'
    assert torrent.metainfo['announce-list'] == [['http://foo'], ['http://bar']]
    torrent.trackers.append('http://asdf')
    assert id(torrent.trackers) == trackers_id
    assert torrent.trackers == [['http://foo'], ['http://bar'], ['http://asdf']]
    assert torrent.metainfo['announce'] == 'http://foo'
    assert torrent.metainfo['announce-list'] == [['http://foo'], ['http://bar'], ['http://asdf']]
    torrent.trackers[0].insert(0, 'http://quux')
    assert id(torrent.trackers) == trackers_id
    assert torrent.trackers == [['http://quux', 'http://foo'], ['http://bar'], ['http://asdf']]
    assert torrent.metainfo['announce'] == 'http://quux'
    assert torrent.metainfo['announce-list'] == [['http://quux', 'http://foo'], ['http://bar'], ['http://asdf']]
    torrent.trackers[1].remove('http://bar')
    assert id(torrent.trackers) == trackers_id
    assert torrent.trackers == [['http://quux', 'http://foo'], ['http://asdf']]
    assert torrent.metainfo['announce'] == 'http://quux'
    assert torrent.metainfo['announce-list'] == [['http://quux', 'http://foo'], ['http://asdf']]
    del torrent.trackers[0]
    assert id(torrent.trackers) == trackers_id
    assert torrent.trackers == [['http://asdf']]
    assert torrent.metainfo['announce'] == 'http://asdf'
    assert 'announce-list' not in torrent.metainfo
    del torrent.trackers[0]
    assert id(torrent.trackers) == trackers_id
    assert torrent.trackers == []
    assert 'announce' not in torrent.metainfo
    assert 'announce-list' not in torrent.metainfo

def test_trackers__announce_in_metainfo_is_automatically_included_in_announce_list(mktorrent):
    torrent = mktorrent()
    torrent.metainfo['announce'] = 'http://foo:123'
    torrent.metainfo['announce-list'] = [['http://bar:456', 'http://baz:789'],
                                         ['http://quux']]
    assert torrent.trackers == [['http://foo:123'], ['http://bar:456', 'http://baz:789'], ['http://quux']]
    assert torrent.metainfo['announce-list'] == [['http://bar:456', 'http://baz:789'], ['http://quux']]
    assert torrent.metainfo['announce'] == 'http://foo:123'

def test_trackers__announce_in_metainfo_is_not_duplicated(mktorrent):
    torrent = mktorrent()
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

def test_trackers__single_url_only_sets_announce_in_metainfo(mktorrent):
    torrent = mktorrent()
    torrent.trackers = 'http://foo:123'
    assert torrent.trackers == [['http://foo:123']]
    assert 'announce-list' not in torrent.metainfo
    assert torrent.metainfo['announce'] == 'http://foo:123'

def test_trackers__multiple_urls_sets_announce_and_announcelist_in_metainfo(mktorrent):
    torrent = mktorrent()
    torrent.trackers = ['http://foo:123', 'http://bar:456', 'http://baz:789']
    exp = [['http://foo:123'], ['http://bar:456'], ['http://baz:789']]
    assert torrent.trackers == exp
    assert torrent.metainfo['announce-list'] == exp
    assert torrent.metainfo['announce'] == 'http://foo:123'

def test_trackers__multiple_lists_of_urls_sets_announce_and_announcelist_in_metainfo(mktorrent):
    torrent = mktorrent()
    torrent.trackers = [['http://foo:123', 'http://bar:456'],
                        ['http://asdf'],
                        ['http://a', 'http://b', 'http://c']]
    exp = [['http://foo:123', 'http://bar:456'],
           ['http://asdf'],
           ['http://a', 'http://b', 'http://c']]
    assert torrent.trackers == exp
    assert torrent.metainfo['announce-list'] == exp
    assert torrent.metainfo['announce'] == 'http://foo:123'

def test_trackers__no_trackers(mktorrent):
    torrent = mktorrent()
    torrent.trackers = ()
    assert torrent.trackers == []
    assert 'announce-list' not in torrent.metainfo
    assert 'announce' not in torrent.metainfo

def test_trackers__addition(mktorrent):
    torrent = mktorrent()
    torrent.trackers = 'http://foo'
    torrent.trackers += ('http://bar',)
    assert torrent.trackers == [['http://foo'], ['http://bar']]
    assert torrent.metainfo['announce-list'] == [['http://foo'], ['http://bar']]
    assert torrent.metainfo['announce'] == 'http://foo'


def test_webseeds__correct_type(mktorrent):
    torrent = mktorrent()
    for value in ((), 'http://foo', ['http://foo', 'http://bar'], None):
        torrent.webseeds = value
        assert isinstance(torrent.webseeds, utils.URLs)

def test_webseeds__metainfo_is_updated_automatically(mktorrent):
    torrent = mktorrent(webseeds=())
    assert torrent.webseeds == []
    assert 'url-list' not in torrent.metainfo
    torrent.webseeds = ['http://foo']
    assert torrent.webseeds == ['http://foo']
    assert torrent.metainfo['url-list'] ==  ['http://foo']
    torrent.webseeds.clear()
    assert torrent.webseeds == []
    assert 'url-list' not in torrent.metainfo

def test_webseeds__urls_are_validated(mktorrent):
    torrent = mktorrent()
    with pytest.raises(errors.URLError) as e:
        torrent.webseeds = ['http://foo', 'http://foo:bar']
    assert str(e.value) == 'http://foo:bar: Invalid URL'

def test_webseeds__non_iterable(mktorrent):
    torrent = mktorrent()
    with pytest.raises(ValueError) as e:
        torrent.webseeds = 23
    assert str(e.value) == 'Must be Iterable, str or None, not int: 23'

def test_webseeds__addition(mktorrent):
    torrent = mktorrent()
    torrent.webseeds = ['http://foo']
    torrent.webseeds += ['http://bar']
    assert torrent.webseeds == ['http://foo', 'http://bar']


def test_httpseeds__correct_type(mktorrent):
    torrent = mktorrent()
    for value in ((), 'http://foo', ['http://foo', 'http://bar'], None):
        torrent.httpseeds = value
        assert isinstance(torrent.httpseeds, utils.URLs)

def test_httpseeds__metainfo_is_updated_automatically(mktorrent):
    torrent = mktorrent(httpseeds=())
    assert torrent.httpseeds == []
    assert 'httpseeds' not in torrent.metainfo
    torrent.httpseeds = ['http://foo']
    assert torrent.httpseeds == ['http://foo']
    assert torrent.metainfo['httpseeds'] ==  ['http://foo']
    torrent.httpseeds.clear()
    assert torrent.httpseeds == []
    assert 'httpseeds' not in torrent.metainfo

def test_httpseeds__urls_are_validated(mktorrent):
    torrent = mktorrent()
    with pytest.raises(errors.URLError) as e:
        torrent.httpseeds = ['http://foo', 'http://foo:bar']
    assert str(e.value) == 'http://foo:bar: Invalid URL'

def test_httpseeds__non_iterable(mktorrent):
    torrent = mktorrent()
    with pytest.raises(ValueError) as e:
        torrent.httpseeds = 23
    assert str(e.value) == 'Must be Iterable, str or None, not int: 23'

def test_httpseeds__addition(mktorrent):
    torrent = mktorrent()
    torrent.httpseeds = ['http://foo']
    torrent.httpseeds += ['http://bar']
    assert torrent.httpseeds == ['http://foo', 'http://bar']


def test_leaving_private_unset_does_not_include_it_in_metainfo(mktorrent):
    torrent = mktorrent()
    assert torrent.private is False
    assert 'private' not in torrent.metainfo['info']

def test_setting_private_always_includes_it_in_metainfo(mktorrent):
    torrent = mktorrent()
    for private in (True, False):
        torrent = mktorrent(private=private)
        assert torrent.private is private
        assert 'private' in torrent.metainfo['info']

def test_setting_private_to_None_removes_it_from_metainfo(mktorrent):
    torrent = mktorrent()
    for private in (True, False):
        torrent = mktorrent(private=private)
        assert torrent.private is private
        torrent.private = None
        assert torrent.private is False
        assert 'private' not in torrent.metainfo['info']

def test_setting_private_enforces_boolean_values(mktorrent):
    torrent = mktorrent()
    torrent.private = 'this evaluates to True'
    assert torrent.private is True
    assert torrent.metainfo['info']['private'] is True

    torrent.private = []  # This evaluates to False
    assert torrent.private is False
    assert torrent.metainfo['info']['private'] is False


def test_comment(mktorrent):
    torrent = mktorrent()
    torrent.comment = ''
    assert torrent.comment == ''
    assert torrent.metainfo['comment'] == ''

    torrent.comment = None
    assert torrent.comment is None
    assert 'comment' not in torrent.metainfo


def test_source(mktorrent):
    torrent = mktorrent()
    torrent.source = ''
    assert torrent.source == ''
    assert torrent.metainfo['info']['source'] == ''

    torrent.source = None
    assert torrent.source is None
    assert 'source' not in torrent.metainfo['info']


def test_creation_date(mktorrent):
    from datetime import datetime

    torrent = mktorrent()
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


def test_created_by(mktorrent):
    torrent = mktorrent()
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


def test_hashability(singlefile_content):
    d = {'t1': torf.Torrent(singlefile_content.path, comment='One'),
         't2': torf.Torrent(singlefile_content.path, comment='Two')}
    assert d['t1'].comment == 'One'
    assert d['t2'].comment == 'Two'


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
