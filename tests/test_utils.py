import torf
from torf import _utils as utils
from torf import _errors as errors

import pytest
import os
from collections import OrderedDict
from unittest import mock


ALPHABET = 'abcdefghijklmnopqrstuvwxyz'

def test_read_chunks__unreadable_file(tmpdir):
    with pytest.raises(torf.ReadError) as excinfo:
        tuple(utils.read_chunks('no/such/file', 10))
    assert excinfo.match(r'^no/such/file: No such file or directory$')

def test_read_chunks__readable_file(tmpdir):
    filepath = tmpdir.join('some_file')
    filepath.write(ALPHABET[:16])

    chunks = tuple(utils.read_chunks(filepath, 4))
    assert chunks == (b'abcd', b'efgh', b'ijkl', b'mnop')

    chunks = tuple(utils.read_chunks(filepath, 5))
    assert chunks == (b'abcde', b'fghij', b'klmno', b'p')

    chunks = tuple(utils.read_chunks(filepath, 5))
    assert chunks == (b'abcde', b'fghij', b'klmno', b'p')

def test_read_chunks__fixed_size__unreadable_file(tmpdir):
    with pytest.raises(torf.ReadError) as excinfo:
        tuple(utils.read_chunks('nonexisting/file', 10))
    assert excinfo.match(r'^nonexisting/file: No such file or directory$')

def test_read_chunks__fixed_size__file_smaller_than_wanted_size(tmpdir):
    filepath = tmpdir.join('some_file')
    filepath.write(ALPHABET[:10])

    chunks = tuple(utils.read_chunks(filepath, 3, 15))
    assert chunks == (b'abc', b'def', b'ghi', b'j\x00\x00', b'\x00\x00\x00')

    chunks = tuple(utils.read_chunks(filepath, 5, 15))
    assert chunks == (b'abcde', b'fghij', b'\x00'*5)

    chunks = tuple(utils.read_chunks(filepath, 3, 16))
    assert chunks == (b'abc', b'def', b'ghi', b'j\x00\x00', b'\x00\x00\x00', b'\x00')

    chunks = tuple(utils.read_chunks(filepath, 2, 16))
    assert chunks == (b'ab', b'cd', b'ef', b'gh', b'ij', b'\x00\x00', b'\x00\x00', b'\x00\x00')

def test_read_chunks__fixed_size__file_larger_than_wanted_size(tmpdir):
    filepath = tmpdir.join('some_file')
    filepath.write(ALPHABET[:15])

    chunks = tuple(utils.read_chunks(filepath, 4, 8))
    assert chunks == (b'abcd', b'efgh')

    chunks = tuple(utils.read_chunks(filepath, 4, 10))
    assert chunks == (b'abcd', b'efgh', b'ij')

    chunks = tuple(utils.read_chunks(filepath, 5, 8))
    assert chunks == (b'abcde', b'fgh')

    chunks = tuple(utils.read_chunks(filepath, 5, 10))
    assert chunks == (b'abcde', b'fghij')

def test_read_chunks__fixed_size__file_size_divisible_by_chunk_size(tmpdir):
    filepath = tmpdir.join('some_file')
    filepath.write(ALPHABET[:12])

    chunks = tuple(utils.read_chunks(filepath, 3, 12))
    assert chunks == (b'abc', b'def', b'ghi', b'jkl')

    chunks = tuple(utils.read_chunks(filepath, 4, 12))
    assert chunks == (b'abcd', b'efgh', b'ijkl')

    chunks = tuple(utils.read_chunks(filepath, 5, 15))
    assert chunks == (b'abcde', b'fghij', b'kl\x00\x00\x00')

    chunks = tuple(utils.read_chunks(filepath, 5, 6))
    assert chunks == (b'abcde', b'f')

def test_read_chunks__fixed_size__file_size_not_divisible_by_chunk_size(tmpdir):
    filepath = tmpdir.join('some_file')
    filepath.write(ALPHABET[:13])

    chunks = tuple(utils.read_chunks(filepath, 3, 13))
    assert chunks == (b'abc', b'def', b'ghi', b'jkl', b'm')

    chunks = tuple(utils.read_chunks(filepath, 3, 14))
    assert chunks == (b'abc', b'def', b'ghi', b'jkl', b'm\x00')

    chunks = tuple(utils.read_chunks(filepath, 3, 11))
    assert chunks == (b'abc', b'def', b'ghi', b'jk')

def test_read_chunks__fixed_size__file_smaller_than_chunk_size(tmpdir):
    filepath = tmpdir.join('some_file')
    filepath.write(ALPHABET[:5])

    chunks = tuple(utils.read_chunks(filepath, 10, 5))
    assert chunks == (b'abcde',)

    chunks = tuple(utils.read_chunks(filepath, 7, 5))
    assert chunks == (b'abcde',)


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


def test_validated_url__max_port_number():
    utils.validated_url(f'http://foohost:{2**16-1}')
    with pytest.raises(torf.URLError):
        utils.validated_url(f'http://foohost:{2**16}')

def test_validated_url__min_port_number():
    utils.validated_url('http://foohost:0')
    with pytest.raises(torf.URLError):
        utils.validated_url('http://foohost:-1')


@pytest.fixture
def testdir(tmpdir):
    base = tmpdir.mkdir('base')
    foo = base.mkdir('foo')
    bar = base.mkdir('.bar')
    baz = bar.mkdir('baz')
    for path in (foo, bar, baz):
        path.join('empty').write('')
        path.join('.empty').write('')
        path.join('not_empty').write('dummy content')
        path.join('.not_empty').write('more dummy content')
    return base

def test_filepaths(testdir):
    files = [filepath[len(os.path.dirname(str(testdir))):]
             for filepath in utils.filepaths(testdir)]
    exp = sorted(['/base/foo/.empty', '/base/foo/.not_empty', '/base/foo/empty', '/base/foo/not_empty',
                  '/base/.bar/.empty', '/base/.bar/.not_empty', '/base/.bar/empty', '/base/.bar/not_empty',
                  '/base/.bar/baz/.empty', '/base/.bar/baz/.not_empty', '/base/.bar/baz/empty', '/base/.bar/baz/not_empty'])
    assert files == exp

def test_for_each_filepath__without_hidden(testdir):
    files = [filepath[len(os.path.dirname(str(testdir))):]
             for filepath in utils.filepaths(testdir, hidden=False)]
    exp = sorted(['/base/foo/empty', '/base/foo/not_empty'])
    assert files == exp

def test_for_each_filepath__without_empty(testdir):
    files = [filepath[len(os.path.dirname(str(testdir))):]
             for filepath in utils.filepaths(testdir, empty=False)]
    exp = sorted(['/base/foo/.not_empty', '/base/foo/not_empty',
                  '/base/.bar/.not_empty', '/base/.bar/not_empty',
                  '/base/.bar/baz/.not_empty', '/base/.bar/baz/not_empty'])
    assert files == exp

def test_for_each_filepath__exclude(testdir):
    files = [filepath[len(os.path.dirname(str(testdir))):]
             for filepath in utils.filepaths(testdir, exclude=('.*',))]
    exp = sorted(['/base/foo/empty', '/base/foo/not_empty'])
    assert files == exp

    for pattern in ('foo', 'fo?', 'f*', '?oo', '*o', 'f?o'):
        files = [filepath[len(os.path.dirname(str(testdir))):]
                 for filepath in utils.filepaths(testdir, exclude=(pattern,))]
        exp = sorted(['/base/.bar/.empty', '/base/.bar/.not_empty', '/base/.bar/empty', '/base/.bar/not_empty',
                      '/base/.bar/baz/.empty', '/base/.bar/baz/.not_empty', '/base/.bar/baz/empty', '/base/.bar/baz/not_empty'])
        assert files == exp

    files = [filepath[len(os.path.dirname(str(testdir))):]
             for filepath in utils.filepaths(testdir, exclude=('*ba?',))]
    exp = sorted(['/base/foo/.empty', '/base/foo/.not_empty', '/base/foo/empty', '/base/foo/not_empty'])
    assert files == exp

    files = [filepath[len(os.path.dirname(str(testdir))):]
             for filepath in utils.filepaths(testdir, exclude=('not_*',))]
    exp = sorted(['/base/foo/.empty', '/base/foo/.not_empty', '/base/foo/empty',
                  '/base/.bar/.empty', '/base/.bar/.not_empty', '/base/.bar/empty',
                  '/base/.bar/baz/.empty', '/base/.bar/baz/.not_empty', '/base/.bar/baz/empty'])
    assert files == exp

    files = [filepath[len(os.path.dirname(str(testdir))):]
             for filepath in utils.filepaths(testdir, exclude=('*not_*',))]
    exp = sorted(['/base/foo/.empty', '/base/foo/empty',
                  '/base/.bar/.empty', '/base/.bar/empty',
                  '/base/.bar/baz/.empty', '/base/.bar/baz/empty'])
    assert files == exp



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


def test_URLs_accepts_string_or_iterable():
    urls = utils.URLs(None, 'http://foo:123')
    assert urls == utils.URLs(None, ('http://foo:123',))
    assert urls == utils.URLs(None, ['http://foo:123'])

def test_URLs_deduplicates_when_initializing():
    urls = utils.URLs(None, ('http://foo:123', 'http://bar:456', 'http://foo:123'))
    assert urls == ['http://foo:123', 'http://bar:456']

def test_URLs_deduplicates_when_setting():
    urls = utils.URLs(None, ('http://foo:123', 'http://bar:456'))
    urls.append('http://foo:123')
    urls.append('http://bar:456')
    urls.extend(('http://foo:123', 'http://bar:456'))
    assert urls == ['http://foo:123', 'http://bar:456']

def test_URLs_deduplicates_when_inserting():
    urls = utils.URLs(None, ('http://foo:123', 'http://bar:456'))
    urls.insert(1, 'http://foo:123')
    urls.insert(0, 'http://bar:456')
    urls.insert(0, 'http://foo:123')
    urls.insert(1, 'http://bar:456')
    assert urls == ['http://foo:123', 'http://bar:456']

def test_URLs_validates_initial_urls():
    with pytest.raises(errors.URLError) as e:
        utils.URLs(None, ('http://foo:123', 'http://bar:456:789'))
    assert str(e.value) == 'http://bar:456:789: Invalid URL'

def test_URLs_validates_appended_urls():
    urls = utils.URLs(None, 'http://foo:123')
    with pytest.raises(errors.URLError) as e:
        urls.append('http://bar:456:789')
    assert str(e.value) == 'http://bar:456:789: Invalid URL'

def test_URLs_validates_changed_urls():
    urls = utils.URLs(None, 'http://foo:123')
    with pytest.raises(errors.URLError) as e:
        urls[0] = 'http://bar:456:789'
    assert str(e.value) == 'http://bar:456:789: Invalid URL'

def test_URLs_validates_inserted_urls():
    urls = utils.URLs(None, ('http://foo:123', 'http://bar:456'))
    with pytest.raises(errors.URLError) as e:
        urls.insert(1, 'http://baz:789:abc')
    assert str(e.value) == 'http://baz:789:abc: Invalid URL'

def test_URLs_is_equal_to_URLs_instances():
    t1 = utils.URLs(None, ('http://foo:123', 'http://bar:456'))
    t2 = utils.URLs(None, ('http://foo:123', 'http://bar:456'))
    assert t1 == t2
    t2 = utils.URLs(None, ('http://foo:123', 'http://baz:789'))
    assert t1 != t2

def test_URLs_is_equal_to_iterables():
    urls = utils.URLs(None, ('http://foo:123', 'http://bar:456'))
    assert urls == ['http://foo:123', 'http://bar:456']
    assert urls == ('http://foo:123', 'http://bar:456')

def test_URLs_is_equal_to_any_combination_of_the_same_urls():
    urls = utils.URLs(None, ('http://foo:123', 'http://bar:456', 'http://baz:789'))
    assert urls == ('http://foo:123', 'http://bar:456', 'http://baz:789')
    assert urls == ('http://bar:456', 'http://foo:123', 'http://baz:789')
    assert urls == ('http://bar:456', 'http://foo:123', 'http://baz:789')
    assert urls == ('http://foo:123', 'http://baz:789', 'http://bar:456')

def test_URLs_calls_callback_after_appending():
    cb = mock.MagicMock()
    urls = utils.URLs(cb, ('http://foo:123', 'http://bar:456'))
    cb.reset_mock()
    urls.append('http://baz:789')
    cb.assert_called_once_with(urls)

def test_URLs_calls_callback_after_removing():
    cb = mock.MagicMock()
    urls = utils.URLs(cb, ('http://foo:123', 'http://bar:456'))
    cb.reset_mock()
    urls.remove('http://bar:456')
    cb.assert_called_once_with(urls)

def test_URLs_calls_callback_after_inserting():
    cb = mock.MagicMock()
    urls = utils.URLs(cb, ('http://foo:123', 'http://bar:456'))
    cb.reset_mock()
    urls.insert(0, 'http://baz:789')
    cb.assert_called_once_with(urls)

def test_URLs_equality():
    urls = utils.URLs(None, ('http://foo:123', 'http://bar:456'))
    assert urls == ('http://foo:123', 'http://bar:456')
    assert urls == ['http://foo:123', 'http://bar:456']
    assert urls != ['http://foo:124', 'http://bar:456']
    assert urls != 'http://bar:456'
    assert urls != 5
    assert urls != None


def test_Trackers_ensures_tiers_when_initializing():
    for args in (('http://foo:123', 'http://bar:456'),
                 (['http://foo:123'], 'http://bar:456'),
                 ('http://foo:123', ['http://bar:456']),
                 (['http://foo:123'], ['http://bar:456'])):
        tiers = utils.Trackers(None, *args)
        for tier in tiers:
            assert isinstance(tier, utils.URLs)
        assert tiers == [['http://foo:123'], ['http://bar:456']]

def test_Trackers_ensures_tiers_when_setting():
    for args in (('http://foo:123', 'http://bar:456'),
                 (['http://foo:123'], 'http://bar:456'),
                 ('http://foo:123', ['http://bar:456']),
                 (['http://foo:123'], ['http://bar:456'])):
        tiers = utils.Trackers(None, 'http://quux')
        tiers.extend(args)
        for tier in tiers:
            assert isinstance(tier, utils.URLs)
        assert tiers == [['http://quux'], ['http://foo:123'], ['http://bar:456']]

        tiers = utils.Trackers(None, 'http://quux')
        tiers.append(args)
        for tier in tiers:
            assert isinstance(tier, utils.URLs)
        assert tiers == [['http://quux'], ['http://foo:123', 'http://bar:456']]

def test_Trackers_ensures_tiers_when_inserting():
    for args in (('http://foo:123', 'http://bar:456'),
                 (['http://foo:123'], 'http://bar:456'),
                 ('http://foo:123', ['http://bar:456']),
                 (['http://foo:123'], ['http://bar:456'])):
        tiers = utils.Trackers(None, 'http://quux')
        tiers.insert(1, args)
        for tier in tiers:
            assert isinstance(tier, utils.URLs)
        assert tiers == [['http://quux'], ['http://foo:123', 'http://bar:456']]

def test_Trackers_equality():
    urls = utils.Trackers(None, 'http://foo:123', 'http://bar:456')
    assert urls == utils.Trackers(None, 'http://foo:123', 'http://bar:456')
    assert urls != utils.Trackers(None, 'http://foo:123', 'http://bar:4567')
    assert urls == utils.Trackers(lambda _: None, 'http://foo:123', 'http://bar:456')
    assert urls == [['http://foo:123'], ('http://bar:456',)]
    assert urls != [['http://foo:123'], 'http://bar:456']
    assert urls == (('http://foo:123',), ['http://bar:456'])
    assert urls != (('http://foo:123',), [['http://bar:456']])
    urls_ = utils.Trackers(None, 'http://foo:123')
    assert urls != urls_
    urls_.append('http://bar:456')
    assert urls == urls_

def test_Trackers_callback():
    cb = mock.MagicMock()
    def assert_type(arg):
        assert type(arg) is utils.Trackers
    cb.side_effect = assert_type
    tiers = utils.Trackers(cb, 'http://foo:123', 'http://bar:456')
    assert cb.call_args_list == [mock.call(tiers)]
    tiers.append('http://baz:789')
    assert cb.call_args_list == [mock.call(tiers)] * 2
    del tiers[0]
    assert cb.call_args_list == [mock.call(tiers)] * 3
    tiers.insert(0, ['http://quux'])
    assert cb.call_args_list == [mock.call(tiers)] * 4
    tiers[0].append('http://asdf')
    assert cb.call_args_list == [mock.call(tiers)] * 5
    tiers[2].remove('http://baz:789')
    assert cb.call_args_list == [mock.call(tiers)] * 6

def test_Trackers_removes_empty_tier_automatically():
    tiers = utils.Trackers(None, 'http://foo:123', 'http://bar:456')
    assert tiers == [['http://foo:123'], ['http://bar:456']]
    tiers[0].remove('http://foo:123')
    assert tiers == [['http://bar:456']]

def test_Trackers_deduplicates_urls_automatically_when_initializing():
    tiers = utils.Trackers(None,
                           ['http://foo:123', 'http://bar:456', 'http://baz:789'],
                           ['http://quux', 'http://foo:123', 'http://asdf'],
                           ['http://asdf', 'http://baz:789', 'http://flim'])
    assert tiers == [['http://foo:123', 'http://bar:456', 'http://baz:789'],
                     ['http://quux', 'http://asdf'],
                     ['http://flim']]

def test_Trackers_deduplicates_urls_automatically_when_setting():
    tiers = utils.Trackers(None,
                           ['http://foo:123', 'http://bar:456', 'http://baz:789'])
    tiers.append(['http://quux', 'http://foo:123'])
    assert tiers == [['http://foo:123', 'http://bar:456', 'http://baz:789'],
                     ['http://quux']]
    tiers.append(['http://foo:123', 'http://bar:456', 'http://baz:789'])
    tiers.append('http://quux')
    assert tiers == [['http://foo:123', 'http://bar:456', 'http://baz:789'],
                     ['http://quux']]

def test_Trackers_deduplicates_urls_automatically_when_inserting():
    tiers = utils.Trackers(None,
                           ['http://foo:123', 'http://bar:456', 'http://baz:789'])
    tiers.insert(0, ['http://asdf', 'http://baz:789', 'http://quux', 'http://foo:123'])
    assert tiers == [['http://asdf', 'http://quux'],
                     ['http://foo:123', 'http://bar:456', 'http://baz:789']]
    tiers = utils.Trackers(None,
                           ['http://foo:123', 'http://bar:456', 'http://baz:789'])
    tiers.insert(1, ['http://asdf', 'http://baz:789', 'http://quux', 'http://foo:123'])
    assert tiers == [['http://foo:123', 'http://bar:456', 'http://baz:789'],
                     ['http://asdf', 'http://quux']]

def test_Trackers_flat_property():
    tiers = utils.Trackers(None, ['http://foo:123'], ['http://bar:456'])
    assert tiers.flat == ('http://foo:123', 'http://bar:456')
