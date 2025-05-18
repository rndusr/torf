import os
import re

import pytest

from torf import _utils


@pytest.mark.parametrize(
    argnames='obj, exp_return_value',
    argvalues=(
        pytest.param('foo', False, id='str'),
        pytest.param('foo'.encode('ascii'), False, id='bytes'),
        pytest.param(bytearray('foo'.encode('ascii')), False, id='bytearray'),
        pytest.param(['foo'], True, id='list'),
        pytest.param(('foo',), True, id='tuple'),
        pytest.param(iter(('foo',)), False, id='iterator'),
    ),
)
def test_is_sequence(obj, exp_return_value):
    assert _utils.is_sequence(obj) is exp_return_value


@pytest.mark.parametrize(
    argnames='obj, exp_return_value',
    argvalues=(
        pytest.param([], (), id='Empty sequence'),
        pytest.param(['foo', 'bar', 'baz'], ('foo', 'bar', 'baz'), id='Flat sequence'),
        pytest.param(['foo', ('bar', 'baz')], ('foo', 'bar', 'baz'), id='Nesting level 1'),
        pytest.param(['foo', ('bar', ['baz'])], ('foo', 'bar', 'baz'), id='Nesting level 2'),
        pytest.param([('foo', ('bar', ['baz']),)], ('foo', 'bar', 'baz'), id='Nesting level 3'),
    ),
)
def test_flatten(obj, exp_return_value):
    assert _utils.flatten(obj) == exp_return_value


@pytest.mark.parametrize(
    argnames='lst, other, exp_return_value',
    argvalues=(
        pytest.param(('a', 'b', 'c'), ('a', 'b', 'c'), True, id='a and b are identical'),
        pytest.param(('a', 'b', 'c', 'd'), ('a', 'b', 'c'), True, id='a is longer than b'),
        pytest.param(('a', 'b', 'c'), ('a', 'b', 'c', 'd'), False, id='a is shorter than b'),
        pytest.param(('a', 'b', 'c'), ('A', 'b', 'c'), False, id='b differes from a at index 0'),
        pytest.param(('a', 'b', 'c'), ('a', 'B', 'c'), False, id='b differes from a at index 1'),
        pytest.param(('a', 'b', 'c'), ('a', 'b', 'C'), False, id='b differes from a at index 2'),
        pytest.param((), (), True, id='a and b are empty'),
    ),
)
def test_iterable_startswith(lst, other, exp_return_value):
    assert _utils.iterable_startswith(lst, other) is exp_return_value


@pytest.mark.parametrize(
    argnames='dcts, exp_result',
    argvalues=(
        pytest.param(
            (
                {'a': 1},
                {'b': 2},
            ),
            {'a': 1, 'b': 2},
            id='No overlap',
        ),
        pytest.param(
            (
                {'a': 1, 'b': 1000},
                {'b': 2, 'c': 3000},
                {'c': 3, 'd': 4},
            ),
            {'a': 1, 'b': 2, 'c': 3, 'd': 4},
            id='Overlap',
        ),
        pytest.param(
            (
                {'a': 1, 'x': {'1': 'this', '2': 'that', '3': {'foo': 'bar', 'goo': 'gaa'}}},
                {'x': {'2': 'THAT', '3': {'baz': 'BAZ', 'goo': 'GAA'}}},
                {'x': {'3': {'goo': 'GAAAA', 'king': 'kong'}}, '4': 'hello'},
            ),
            {'a': 1, 'x': {'1': 'this', '2': 'THAT', '3': {'foo': 'bar', 'baz': 'BAZ', 'goo': 'GAAAA', 'king': 'kong'}}, '4': 'hello'},
            id='Nested dictionaries',
        ),
        pytest.param(
            (
                {'a': [1, 2, [3, 4, [5, 6], 7, 8], 9, 0]},
                {'a': [10, 20, [30, 40, [50, 60], 70, 80], 90]},
                {'a': [100, 200]},
            ),
            {'a': [100, 200]},
            id='Nested lists',
        ),
        pytest.param(
            (
                {'a': [1, 2, [3, 4, [5, 6], 7, 8], 9, 0]},
                'this is not a mapping',
                {'a': [100, 200]},
            ),
            TypeError("Expected Mapping, not str: 'this is not a mapping'"),
            id='Unexpected type',
        ),
    ),
)
def test_merge_dicts(dcts, exp_result):
    if isinstance(exp_result, Exception):
        with pytest.raises(type(exp_result), match=rf'^{re.escape(str(exp_result))}$'):
            _utils.merge_dicts(*dcts)
    else:
        return_value = _utils.merge_dicts(*dcts)
        assert return_value == exp_result


class Test_File:

    @pytest.mark.parametrize(
        argnames='path, size, exp_result',
        argvalues=(
            pytest.param(
                ('a', 'b', 'c'),
                123,
                _utils.File('a', 'b', 'c', size=123),
                id='path and size are valid',
            ),
            pytest.param(
                ('a', (), 'c'),
                123,
                ValueError("Unexpected path: ('a', (), 'c')"),
                id='path contains has unexpected type',
            ),
            pytest.param(
                ('a', 'b', 'c'),
                'one million',
                ValueError("size must be int, not str: 'one million'"),
                id='size is unexpected type',
            ),
            pytest.param(
                ('a', f'b{os.sep}c', 'd'),
                123,
                _utils.File('a', 'b', 'c', 'd', size=123),
                id='path contains os.sep',
            ),
        ),
    )
    def test___init__(self, path, size, exp_result):
        if isinstance(exp_result, Exception):
            with pytest.raises(type(exp_result), match=rf'^{re.escape(str(exp_result))}$'):
                _utils.File(*path, size=size)
        else:
            f = _utils.File(*path, size=size)
            assert f == exp_result

    def test_path(self, mocker):
        f = _utils.File('a', 'b', 'c', size=123)
        mocker.patch.object(f, '_path', 'this is the path')
        assert f.path == 'this is the path'

    def test_size(self, mocker):
        f = _utils.File('a', 'b', 'c', size=123)
        mocker.patch.object(f, '_size', 'this is the size')
        assert f.size == 'this is the size'

    def test_name(self, mocker):
        f = _utils.File('a', 'b', 'c', size=123)
        assert f.name == 'c'

    @pytest.mark.parametrize(
        argnames='a, b, exp_return_value',
        argvalues=(
            pytest.param(
                _utils.File('a', 'b', 'c', size=123),
                _utils.File('a', 'b', 'c', size=123),
                True,
                id='path is equal and size is equal',
            ),
            pytest.param(
                _utils.File('a', 'b', 'c', size=123),
                _utils.File('a', 'b', 'c', size=999),
                False,
                id='path is equal and size is different',
            ),
            pytest.param(
                _utils.File('a', 'b', size=123),
                _utils.File('a', 'b', 'c', size=123),
                False,
                id='path is different and size is equal',
            ),
            pytest.param(
                _utils.File('a', 'b', 'c', size=123),
                'not a File object',
                NotImplemented,
                id='Unsupported type',
            ),
        ),
    )
    def test___eq__(self, a, b, exp_return_value):
        assert a.__eq__(b) is exp_return_value

    def test___hash__(self):
        f = _utils.File('a', 'b', size=123)
        assert hash(f) == hash((('a', 'b'), 123))

    def test___repr__(self):
        f = _utils.File('a', 'b', size=123)
        assert repr(f) == "File('a', 'b', size=123)"


class Test_ImmutableDict:

    def test_subclass(self):
        assert issubclass(_utils.ImmutableDict, dict), _utils.ImmutableDict.__mro__

    def test___setitem__(self):
        d = _utils.ImmutableDict(foo='bar', baz='asdf')
        with pytest.raises(TypeError, match=r'^ImmutableDict is immutable$'):
            d['foo'] = 'baz'
        assert d == {'foo': 'bar', 'baz': 'asdf'}

    def test___delitem__(self):
        d = _utils.ImmutableDict(foo='bar', baz='asdf')
        with pytest.raises(TypeError, match=r'^ImmutableDict is immutable$'):
            del d['foo']
        assert d == {'foo': 'bar', 'baz': 'asdf'}

    def test_clear(self):
        d = _utils.ImmutableDict(foo='bar', baz='asdf')
        with pytest.raises(TypeError, match=r'^ImmutableDict is immutable$'):
            d.clear()
        assert d == {'foo': 'bar', 'baz': 'asdf'}

    def test_pop(self):
        d = _utils.ImmutableDict(foo='bar', baz='asdf')
        with pytest.raises(TypeError, match=r'^ImmutableDict is immutable$'):
            d.pop('foo')
        assert d == {'foo': 'bar', 'baz': 'asdf'}

    def test_popitem(self):
        d = _utils.ImmutableDict(foo='bar', baz='asdf')
        with pytest.raises(TypeError, match=r'^ImmutableDict is immutable$'):
            d.popitem('foo')
        assert d == {'foo': 'bar', 'baz': 'asdf'}

    def test_setdefault(self):
        d = _utils.ImmutableDict(foo='bar', baz='asdf')
        with pytest.raises(TypeError, match=r'^ImmutableDict is immutable$'):
            d.setdefault('this', 'that')
        assert d == {'foo': 'bar', 'baz': 'asdf'}

    def test_update(self):
        d = _utils.ImmutableDict(foo='bar', baz='asdf')
        with pytest.raises(TypeError, match=r'^ImmutableDict is immutable$'):
            d.update({'this': 'that'})
        assert d == {'foo': 'bar', 'baz': 'asdf'}

    def test_mutable(self):
        class MySequence(list):
            pass

        class MyMapping(dict):
            pass

        d = _utils.ImmutableDict(
            a='foo',
            b=123,
            c={'this': MySequence(('this', 'and', MyMapping({'t': 'h', 'a': 't'})))},
        )
        d_mutable = d.mutable()
        assert d_mutable == {
            'a': 'foo',
            'b': 123,
            'c': {'this': ['this', 'and', {'t': 'h', 'a': 't'}]},
        }
        assert not isinstance(d_mutable['c']['this'], MySequence)
        assert isinstance(d_mutable['c']['this'], list)
        assert not isinstance(d_mutable['c']['this'][2], MyMapping)
        assert isinstance(d_mutable['c']['this'][2], dict)

    def test___repr__(self):
        d = _utils.ImmutableDict(foo='bar', baz='asdf')
        assert repr(d) == f'{type(d).__name__}({dict(d)!r})'
