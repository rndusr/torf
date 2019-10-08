import torf
from torf import _utils as utils

import pytest
import os
from collections import OrderedDict


def test_is_power_of_2():
    assert utils.is_power_of_2(0) is False
    for n in range(1, 30):
        assert utils.is_power_of_2(2**n) is True
        assert utils.is_power_of_2(-2**n) is True
        assert utils.is_power_of_2(3**n) is False
        assert utils.is_power_of_2(-5**n) is False


def test_validated_url():
    utils.validated_url(f'http://foohost:{2**16-1}')
    with pytest.raises(torf.URLError):
        utils.validated_url(f'http://foohost:{2**16}')

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
