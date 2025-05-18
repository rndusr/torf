import datetime
import inspect
import os
import pathlib
import re
from unittest.mock import Mock, PropertyMock, call

import pytest

from torf import Torrent, _utils


def get_kwargs(method):
    signature = inspect.signature(method)
    return {
        name: parameter.default
        for name, parameter in signature.parameters.items()
        if name != 'self'
    }


@pytest.fixture
def handle_kwarg_mocks(mocker):
    mocks = Mock()
    for attribute_name in Torrent.__dict__:
        if attribute_name.startswith('_handle_kwarg_'):
            print(f'Mocking torf.Torrent.{attribute_name}')
            mocks.attach_mock(mocker.patch(f'torf.Torrent.{attribute_name}', Mock()), attribute_name)
    return mocks

HANDLE_KWARG_KEYWORDS = tuple(
    keyword for keyword in get_kwargs(Torrent.__init__)
    if keyword not in ('validate', 'metainfo')
)


class Test___init__:

    def _get_exp_handle_kwarg_call(self, keyword, value):
        exp_method = getattr(call, f'_handle_kwarg_{keyword}')
        return exp_method(value)

    @pytest.mark.parametrize('keyword', HANDLE_KWARG_KEYWORDS)
    def test_keyword_argument_provided(self, keyword, handle_kwarg_mocks):
        value = f'<{keyword} value>'
        Torrent(**{keyword: value})
        exp_call = self._get_exp_handle_kwarg_call(keyword, value)
        assert exp_call in handle_kwarg_mocks.mock_calls

    @pytest.mark.parametrize('keyword', HANDLE_KWARG_KEYWORDS)
    def test_keyword_argument_not_provided(self, keyword, handle_kwarg_mocks):
        Torrent()
        exp_method = getattr(call, f'_handle_kwarg_{keyword}')
        exp_call = exp_method(get_kwargs(Torrent.__init__)[keyword])
        assert exp_call not in handle_kwarg_mocks.mock_calls

    @pytest.mark.parametrize(
        argnames='keyword, value, modifier, exp_modified, exp_stored',
        argvalues=(
            pytest.param(
                'announce',
                [['a'], ['b'], ['c']],
                lambda v: (v.append(['d']) or v[1].append('b2')),
                [['a'], ['b', 'b2'], ['c'], ['d']],
                [['a'], ['b'], ['c']],
                id='announce',
            ),
            pytest.param(
                'files',
                [{'length': 1, 'path': ['a', 'b']}, {'length': 2, 'path': ['c']}],
                lambda v: (
                    v.append({'length': 3, 'path': ['d']})
                    or v.__delitem__(1)
                    or v[1].__setitem__('foo', 'bar')
                    or v[1]['path'].append('D')
                ),
                [{'length': 1, 'path': ['a', 'b']}, {'length': 3, 'path': ['d', 'D'], 'foo': 'bar'}],
                [{'length': 1, 'path': ['a', 'b']}, {'length': 2, 'path': ['c']}],
                id='files',
            ),
        ),
    )
    def test_keyword_argument_is_mutable(self, keyword, value, modifier, exp_modified, exp_stored):
        t = Torrent(**{keyword: value})
        modifier(value)
        print('original:', repr(value), id(value))
        print('  stored:', repr(t._kwargs.provided[keyword]), id(t._kwargs.provided[keyword]))
        assert value == exp_modified
        stored = t._kwargs.provided[keyword]
        assert stored == exp_stored

    @pytest.mark.parametrize('validate', (None, True, False))
    def test_keyword_validate(self, validate, handle_kwarg_mocks, mocker):
        mocker.patch.object(Torrent, 'validate', Mock())
        if validate is None:
            t = Torrent()
        else:
            t = Torrent(validate=validate)
        if validate in (True, None):
            assert t.validate.call_args_list == [call()]
        else:
            assert t.validate.call_args_list == []

    @pytest.mark.parametrize(
        argnames='metainfo, exp_result',
        argvalues=(
            pytest.param(None, {}, id='metainfo not provided'),
            pytest.param({'foo': 'bar'}, {'foo': 'bar'}, id='metainfo is flat dict'),
            pytest.param(
                {'foo': 'bar', 'nested': {'this': 'that', 'lst': [1, 2, 3]}},
                {'foo': 'bar', 'nested': {'this': 'that', 'lst': (1, 2, 3)}},
                id='metainfo is nested',
            ),
            pytest.param([1, 2, 3], TypeError('Expected Mapping, not list: [1, 2, 3]'), id='metainfo is not mapping'),
        ),
    )
    def test_keyword_metainfo(self, metainfo, exp_result):
        kwargs = {}
        if metainfo is not None:
            kwargs['metainfo'] = metainfo
        if isinstance(exp_result, Exception):
            with pytest.raises(type(exp_result), match=rf'^{re.escape(str(exp_result))}$'):
                Torrent(**kwargs)
        else:
            t = Torrent(**kwargs)
            assert t.metainfo == exp_result

    @pytest.mark.parametrize(
        argnames='metainfo, kwargs, exp_metainfo',
        argvalues=(
            pytest.param({'announce-list': ['a']}, {'announce': ['b']}, {'announce-list': (('b',),)}, id='announce'),
            pytest.param({'comment': 'foo'}, {'comment': 'bar'}, {'comment': 'bar'}, id='comment'),
            pytest.param({'created by': 'foo'}, {'created_by': 'bar'}, {'created by': 'bar'}, id='created_by'),
            pytest.param({'creation date': 123}, {'creation_date': 456}, {'creation date': 456}, id='creation_date'),
            pytest.param(
                {'info': {'files': [{'length': 1, 'path': ['a', 'b']}, {'length': 2, 'path': ['c']}]}},
                {'files': [{'length': 3, 'path': ['e', 'f']}, {'length': 4, 'path': ['g']}]},
                {'info': {'files': ({'length': 3, 'path': ('e', 'f')}, {'length': 4, 'path': ('g',)})}},
                id='files',
            ),
            pytest.param({'httpseeds': ['a', 'b']}, {'httpseeds': ['c', 'd']}, {'httpseeds': ('c', 'd')}, id='httpseeds'),
            pytest.param({'info': {'name': 'foo'}}, {'name': 'bar'}, {'info': {'name': 'bar'}}, id='name'),
            pytest.param({'info': {'pieces': b'foo'}}, {'pieces': b'bar'}, {'info': {'pieces': b'bar'}}, id='pieces'),
            pytest.param({'info': {'private': True}}, {'private': False}, {'info': {}}, id='private=False'),
            pytest.param({'info': {'private': False}}, {'private': True}, {'info': {'private': True}}, id='private=True'),
            pytest.param({'info': {'source': 'foo'}}, {'source': 'bar'}, {'info': {'source': 'bar'}}, id='source'),
            pytest.param({'url-list': ['a', 'b']}, {'webseeds': ['c', 'd']}, {'url-list': ('c', 'd')}, id='webseeds'),
        ),
    )
    def test_keyword_metainfo_with_keyword(self, metainfo, kwargs, exp_metainfo):
        kwargs['metainfo'] = metainfo
        t = Torrent(**kwargs)
        assert t.metainfo == exp_metainfo

    def test_keyword_argument_unknown(self, handle_kwarg_mocks):
        with pytest.raises(TypeError, match=r'unexpected keyword argument'):
            Torrent(foo='bar')

    @pytest.mark.parametrize('keyword', HANDLE_KWARG_KEYWORDS)
    @pytest.mark.parametrize('validate', (None, True, False))
    def test_subclass(self, keyword, validate, handle_kwarg_mocks, mocker):
        class MyTorrent(Torrent):
            def __init__(self, created_by='Me!', **kwargs):
                super().__init__(created_by=created_by, **kwargs)
                self.MyTorrent_was_initialized = True

        validate_mock = mocker.patch.object(Torrent, 'validate', Mock())
        handle_kwarg_mocks.attach_mock(validate_mock, 'validate')

        value = f'<{keyword} value>'
        if validate is None:
            t = MyTorrent(**{keyword: value})
        else:
            t = MyTorrent(**{keyword: value}, validate=validate)
        assert isinstance(t, MyTorrent), type(t).__name__
        assert t.MyTorrent_was_initialized is True
        exp_method = getattr(call, f'_handle_kwarg_{keyword}')
        exp_call = exp_method(value)
        assert exp_call in handle_kwarg_mocks.mock_calls
        if validate in (True, None):
            assert call.validate() in handle_kwarg_mocks.mock_calls
        else:
            assert call.validate() not in handle_kwarg_mocks.mock_calls


class Test__metainfo_raw:

    def test_is_dict(self):
        t = Torrent()
        assert type(t._metainfo_raw) is dict

    def test_is_singleton(self):
        t = Torrent()
        assert id(t._metainfo_raw) == id(t._metainfo_raw)


class Test__metainfo_pure:

    def test_is_CodecMapping(self, mocker):
        CodecMapping_mock = mocker.patch('torf._metainfo.CodecMapping')
        t = Torrent()
        assert t._metainfo_pure is CodecMapping_mock.return_value
        assert CodecMapping_mock.call_args_list == [call(
            t._metainfo_raw,
            keys_encoding='UTF-8',
            no_encoding_keypaths=(
                ('info', 'pieces'),
            ),
        )]

    def test_is_singleton(self):
        t = Torrent()
        assert id(t._metainfo_pure) == id(t._metainfo_pure)


class Test_metainfo:

    def test_access_before_fully_initialized(self):
        t = Torrent()
        delattr(t, '_metainfo_initialized')
        with pytest.raises(RuntimeError, match=r'^metainfo is not fully initialized yet$'):
            t.metainfo

    def test_values(self):
        t = Torrent(
            comment='hello',
            files=[
                (['foo'], 123),
                (['bar', 'baz'], 234),
            ],
            metainfo={'info': {'this': 'that', 'pieces': bytearray(b'd34db33f')}},
        )
        assert t.metainfo == {
            'info': {
                'this': 'that',
                'files': (
                    {'length': 123, 'path': ('foo',)},
                    {'length': 234, 'path': ('bar', 'baz',)},
                ),
                'pieces': b'd34db33f',
            },
            'comment': 'hello',
        }

    def test_types(self):
        t = Torrent(
            comment='hello',
            files=[
                (['foo'], 123),
                (['bar', 'baz'], 234),
            ],
            metainfo={'info': {'this': 'that'}},
        )
        assert isinstance(t.metainfo, _utils.ImmutableDict)
        assert isinstance(t.metainfo['comment'], str)
        assert isinstance(t.metainfo['info'], _utils.ImmutableDict)
        assert isinstance(t.metainfo['info']['files'], tuple)
        for file in t.metainfo['info']['files']:
            assert isinstance(file, _utils.ImmutableDict)
            assert isinstance(file['length'], int)
            assert isinstance(file['path'], tuple)


class Test___repr__:

    @pytest.mark.parametrize(
        argnames='infohash, name, exp_repr',
        argvalues=(
            pytest.param(
                None, None,
                '<Torrent {id}>',
                id='No infohash, no name',
            ),
            pytest.param(
                'd34db33f', None,
                "<Torrent {id} infohash='d34db33f'>",
                id='Infohash but no name',
            ),
            pytest.param(
                None, 'The Name',
                "<Torrent {id} name='The Name'>",
                id='No infohash but name',
            ),
            pytest.param(
                'd34db33f', 'The Name',
                "<Torrent {id} infohash='d34db33f' name='The Name'>",
                id='Infohash and name',
            ),
        ),
    )
    def test___repr__(self, infohash, name, exp_repr, mocker):
        t = Torrent()
        mocker.patch.object(type(t), 'infohash', PropertyMock(return_value=infohash))
        mocker.patch.object(type(t), 'name', PropertyMock(return_value=name))
        assert repr(t) == exp_repr.format(id=id(t))


class Test_copy:

    def _create_original(self, **kwargs):
        # Remove any keyword arguments with a `None` value.
        pruned_kwargs = {
            k: v
            for k, v in kwargs.items()
            if v is not None
        }
        return Torrent(**pruned_kwargs)

    def _create_copy(self, original, **kwargs):
        # Remove any keyword arguments with a `None` value.
        pruned_kwargs = {
            k: v
            for k, v in kwargs.items()
            if v is not None
        }
        return original.copy(**pruned_kwargs)

    @pytest.mark.parametrize(
        argnames='original_value, copy_value, exp_value',
        argvalues=(
            ('big5', None, 'big5'),
            ('big5', 'ascii', 'ascii'),
        ),
        ids=lambda x: str(x),
    )
    def test_copy_encoding(self, original_value, copy_value, exp_value):
        t1 = self._create_original(encoding=original_value)
        t2 = self._create_copy(t1, encoding=copy_value)
        assert t2._metainfo_pure.values_encoding == exp_value

    @pytest.mark.parametrize(
        argnames='original_value, copy_value, exp_value',
        argvalues=(
            (True, None, True),
            (True, False, False),
            (True, True, True),
            (False, None, False),
            (False, False, False),
            (False, True, True),
        ),
        ids=lambda x: str(x),
    )
    def test_copy_raise_on_decoding_error(self, original_value, copy_value, exp_value):
        t1 = self._create_original(raise_on_decoding_error=original_value)
        t2 = self._create_copy(t1, raise_on_decoding_error=copy_value)
        assert t2._metainfo_pure.raise_on_decoding_error == exp_value

    @pytest.mark.parametrize(
        argnames='original_value, copy_value, exp_validate_called_on_copy',
        argvalues=(
            (True, None, True),
            (True, False, False),
            (True, True, True),
            (False, None, False),
            (False, False, False),
            (False, True, True),
        ),
        ids=lambda x: str(x),
    )
    def test_copy_validate(self, original_value, copy_value, exp_validate_called_on_copy, mocker):
        t1 = Torrent(validate=original_value)
        mocker.patch('torf.Torrent.validate')
        t2 = self._create_copy(t1, validate=copy_value)
        if exp_validate_called_on_copy:
            assert t2.validate.call_args_list == [call()]
        else:
            assert t2.validate.call_args_list == []

    @pytest.mark.parametrize(
        argnames='original_value, copy_value, exp_value',
        argvalues=(
            ((), None, ()),
            (
                (('http://foo.1', 'http://bar.1'), ('http://foo.2',)),
                None,
                (('http://foo.1', 'http://bar.1'), ('http://foo.2',)),
            ),
            (
                (('http://foo.1', 'http://bar.1'), ('http://foo.2',)),
                (),
                (),
            ),
            (
                [('http://asdf',),],
                [('http://foo.1', 'http://bar.1'), ('http://foo.2',)],
                (('http://foo.1', 'http://bar.1'), ('http://foo.2',)),
            ),
        ),
        ids=lambda x: str(x),
    )
    def test_copy_announce(self, original_value, copy_value, exp_value):
        t1 = self._create_original(announce=original_value)
        t2 = self._create_copy(t1, announce=copy_value)
        assert t2.announce == exp_value

    @pytest.mark.parametrize(
        argnames='original_value, copy_value, exp_value',
        argvalues=(
            (None, None, None),
            (None, 'New comment', 'New comment'),
            ('Old comment', None, 'Old comment'),
            ('Old comment', 'New comment', 'New comment'),
        ),
        ids=lambda x: str(x),
    )
    def test_copy_comment(self, original_value, copy_value, exp_value):
        t1 = self._create_original(comment=original_value)
        t2 = self._create_copy(t1, comment=copy_value)
        assert t2.comment == exp_value

    @pytest.mark.parametrize(
        argnames='original_value, copy_value, exp_value',
        argvalues=(
            (None, None, None),
            (None, 'me', 'me'),
            ('you', None, 'you'),
            ('you', 'me', 'me'),
        ),
        ids=lambda x: str(x),
    )
    def test_copy_created_by(self, original_value, copy_value, exp_value):
        t1 = self._create_original(created_by=original_value)
        t2 = self._create_copy(t1, created_by=copy_value)
        assert t2.created_by == exp_value

    @pytest.mark.parametrize(
        argnames='original_value, copy_value, exp_value',
        argvalues=(
            (None, None, None),
            (None, 234, datetime.datetime.fromtimestamp(234)),
            (123, None, datetime.datetime.fromtimestamp(123)),
            (123, 234, datetime.datetime.fromtimestamp(234)),
        ),
        ids=lambda x: str(x),
    )
    def test_copy_creation_date(self, original_value, copy_value, exp_value):
        t1 = self._create_original(creation_date=original_value)
        t2 = self._create_copy(t1, creation_date=copy_value)
        assert t2.creation_date == exp_value

    @pytest.mark.parametrize(
        argnames='original_value, copy_value, exp_value',
        argvalues=(
            (None, None, ()),
            ((), None, ()),
            (
                ({'length': 123, 'path': ['foo']}, {'length': 234, 'path': ['bar', 'baz']}),
                None,
                (_utils.File('My Name', 'foo', size=123), _utils.File('My Name', 'bar', 'baz', size=234)),
            ),
            (
                ({'length': 123, 'path': ['foo']}, {'length': 234, 'path': ['bar', 'baz']}),
                (),
                (),
            ),
            (
                ({'length': 123, 'path': ['foo']}, {'length': 234, 'path': ['bar', 'baz']}),
                ({'length': 1230, 'path': ['this', 'that']}, {'length': 2340, 'path': ['also', 'this']}),
                (_utils.File('My Name', 'this', 'that', size=1230), _utils.File('My Name', 'also', 'this', size=2340)),
            ),
        ),
        ids=lambda x: str(x),
    )
    def test_copy_files(self, original_value, copy_value, exp_value):
        t1 = self._create_original(files=original_value, name='My Name')
        t2 = self._create_copy(t1, files=copy_value)
        assert t2.filelist == exp_value

    @pytest.mark.parametrize(
        argnames='original_value, copy_value, exp_value',
        argvalues=(
            (None, None, ()),
            ((), None, ()),
            (
                ('http://foo', 'http:/bar'),
                None,
                ('http://foo', 'http:/bar'),
            ),
            (
                ('http://foo', 'http:/bar'),
                (),
                (),
            ),
            (
                ('http://foo', 'http:/bar'),
                ('http://baz',),
                ('http://baz',),
            ),
        ),
        ids=lambda x: str(x),
    )
    def test_copy_httpseeds(self, original_value, copy_value, exp_value):
        t1 = self._create_original(httpseeds=original_value)
        t2 = self._create_copy(t1, httpseeds=copy_value)
        assert t2.httpseeds == exp_value

    @pytest.mark.parametrize(
        argnames='original_value, copy_value, exp_value',
        argvalues=(
            (None, None, None),
            (None, 'My Name', 'My Name'),
            ('Your Name', None, 'Your Name'),
            ('Your Name', 'My Name', 'My Name'),
        ),
        ids=lambda x: str(x),
    )
    def test_copy_name(self, original_value, copy_value, exp_value):
        t1 = self._create_original(name=original_value)
        t2 = self._create_copy(t1, name=copy_value)
        assert t2.name == exp_value

    @pytest.mark.parametrize(
        argnames='original_value, copy_value, exp_value',
        argvalues=(
            (None, None, ()),
            ((), None, ()),
            (
                (b'\x01' * 20, b'\x02' * 20, b'\x03' * 20),
                None,
                (b'\x01' * 20, b'\x02' * 20, b'\x03' * 20),
            ),
            (
                (b'\x01' * 20, b'\x02' * 20, b'\x03' * 20,),
                (),
                (),
            ),
            (
                (b'\x01' * 20, b'\x02' * 20, b'\x03' * 20),
                (b'\x02' * 20, b'\x03' * 20, b'\x04' * 20),
                (b'\x02' * 20, b'\x03' * 20, b'\x04' * 20),
            ),
        ),
        ids=lambda x: str(x),
    )
    @pytest.mark.parametrize('join_pieces', (True, False), ids=('pieces is bytes', 'pieces is tuple'))
    def test_copy_pieces(self, original_value, copy_value, exp_value, join_pieces):
        t1 = self._create_original(pieces=original_value)
        if copy_value is not None and join_pieces:
            copy_value = b''.join(copy_value)
        t2 = self._create_copy(t1, pieces=copy_value)
        assert t2.pieces == exp_value

    @pytest.mark.parametrize(
        argnames='original_value, copy_value, exp_value',
        argvalues=(
            (None, None, False),
            (None, False, False),
            (None, True, True),
            (False, None, False),
            (False, False, False),
            (False, True, True),
            (True, None, True),
            (True, False, False),
            (True, True, True),
        ),
        ids=lambda x: str(x),
    )
    def test_copy_private(self, original_value, copy_value, exp_value):
        t1 = self._create_original(private=original_value)
        t2 = self._create_copy(t1, private=copy_value)
        assert t2.private == exp_value

    @pytest.mark.parametrize(
        argnames='original_value, copy_value, exp_value',
        argvalues=(
            (None, None, None),
            (None, 'there', 'there'),
            ('here', None, 'here'),
            ('here', 'there', 'there'),
        ),
        ids=lambda x: str(x),
    )
    def test_copy_source(self, original_value, copy_value, exp_value):
        t1 = self._create_original(source=original_value)
        t2 = self._create_copy(t1, source=copy_value)
        assert t2.source == exp_value

    @pytest.mark.parametrize(
        argnames='original_value, copy_value, exp_value',
        argvalues=(
            (None, None, ()),
            ((), None, ()),
            (
                ('http://foo', 'http:/bar'),
                None,
                ('http://foo', 'http:/bar'),
            ),
            (
                ('http://foo', 'http:/bar'),
                (),
                (),
            ),
            (
                ('http://foo', 'http:/bar'),
                ('http://baz',),
                ('http://baz',),
            ),
        ),
        ids=lambda x: str(x),
    )
    def test_copy_webseeds(self, original_value, copy_value, exp_value):
        t1 = self._create_original(webseeds=original_value)
        t2 = self._create_copy(t1, webseeds=copy_value)
        assert t2.webseeds == exp_value

    @pytest.mark.parametrize(
        argnames='original_kwargs, copy_kwargs, exp_copy_metainfo',
        argvalues=(
            pytest.param(
                {'comment': 'foo'},
                {'metainfo': {'comment': 'bar'}},
                {'comment': 'bar'},
                id='metainfo overloads argument',
            ),
            pytest.param(
                {'metainfo': {'comment': 'foo'}},
                {'comment': 'bar'},
                {'comment': 'bar'},
                id='argument overloads metainfo',
            ),
            pytest.param(
                {'metainfo': {'comment': 'foo', 'info': {'name': 'The Name'}}},
                {'metainfo': {'comment': 'bar', 'info': {'source': 'SRC'}}},
                {'comment': 'bar', 'info': {'name': 'The Name', 'source': 'SRC'}},
                id='metainfo arguments are merged',
            ),
        ),
        ids=lambda x: str(x),
    )
    def test_copy_metainfo(self, original_kwargs, copy_kwargs, exp_copy_metainfo):
        t1 = self._create_original(**original_kwargs)
        t2 = self._create_copy(t1, **copy_kwargs)
        assert t2.metainfo == exp_copy_metainfo


class Test__get_metainfo:

    def test_keypath_is_found_at_level_1(self):
        t = Torrent(metainfo={'comment': 'foo'})
        return_value = t._get_metainfo('comment')
        assert return_value == 'foo'

    def test_keypath_is_found_at_level_2(self):
        t = Torrent(metainfo={'info': {'name': 'foo'}})
        return_value = t._get_metainfo('info', 'name')
        assert return_value == 'foo'

    def test_keypath_is_found_at_level_3(self):
        t = Torrent(metainfo={'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}})
        return_value = t._get_metainfo('info', 'files', 1)
        assert return_value == {'path': ('bcd', 'efg')}

    def test_keypath_is_found_at_level_4(self):
        t = Torrent(metainfo={'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}})
        return_value = t._get_metainfo('info', 'files', 1, 'path')
        assert return_value == ('bcd', 'efg')

    def test_keypath_is_found_at_level_5(self):
        t = Torrent(metainfo={'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}})
        return_value = t._get_metainfo('info', 'files', 1, 'path', 0)
        assert return_value == 'bcd'

    def test_keypath_is_found_at_level_6(self):
        t = Torrent(metainfo={'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}})
        return_value = t._get_metainfo('info', 'files', 1, 'path', 0, 2)
        assert return_value == 'd'

    def test_keypath_is_not_found_in_mapping_with_no_default(self):
        t = Torrent(metainfo={'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}})
        exp_error = 'info.files.1.asdf: Not found'
        with pytest.raises(ValueError, match=rf'^{re.escape(exp_error)}$'):
            t._get_metainfo('info', 'files', 1, 'asdf')

    def test_keypath_is_not_found_in_mapping_with_default(self):
        t = Torrent(metainfo={'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}})
        return_value = t._get_metainfo('info', 'files', 1, 'asdf', default='something')
        assert return_value == 'something'

    def test_keypath_is_not_found_in_mapping_with_default_None(self):
        t = Torrent(metainfo={'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}})
        return_value = t._get_metainfo('info', 'files', 1, 'asdf', default=None)
        assert return_value is None

    def test_keypath_is_not_found_in_list_with_no_default(self):
        t = Torrent(metainfo={'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}})
        exp_error = 'info.files.123: Not found'
        with pytest.raises(ValueError, match=rf'^{re.escape(exp_error)}$'):
            t._get_metainfo('info', 'files', 123)

    def test_keypath_is_not_found_in_list_with_default(self):
        t = Torrent(metainfo={'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}})
        return_value = t._get_metainfo('info', 'files', 123, default='something')
        assert return_value == 'something'

    def test_keypath_is_not_found_in_list_with_default_None(self):
        t = Torrent(metainfo={'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}})
        return_value = t._get_metainfo('info', 'files', 123, default=None)
        assert return_value is None

    def test_converting_value_to_type(self):
        t = Torrent(metainfo={'info': {'files': [{'length': 123}, {'length': 456}, {'length': 789}]}})
        return_value = t._get_metainfo('info', 'files', 0, 'length', type=str)
        assert return_value == '123'

    def test_converting_value_to_type_raises_ValueError_without_default(self):
        t = Torrent(metainfo={'info': {'files': [{'length': 'foo'}, {'length': 'bar'}, {'length': 'baz'}]}})
        exp_msg = "info.files.0.length: Invalid value for int: 'foo'"
        with pytest.raises(ValueError, match=rf'^{re.escape(str(exp_msg))}$'):
            t._get_metainfo('info', 'files', 0, 'length', type=int)

    def test_converting_value_to_type_raises_ValueError_with_default(self):
        t = Torrent(metainfo={'info': {'files': [{'length': 'foo'}, {'length': 'bar'}, {'length': 'baz'}]}})
        return_value = t._get_metainfo('info', 'files', 0, 'length', type=int, default='asdf')
        assert return_value == 'asdf'

    def test_converting_value_to_type_raises_TypeError_without_default(self):
        t = Torrent(metainfo={'info': {'files': [{'length': 123}, {'length': 456}, {'length': 789}]}})
        with pytest.raises(ValueError, match=r"^info.files.0.length: Invalid value for tuple: 123$"):
            t._get_metainfo('info', 'files', 0, 'length', type=tuple)

    def test_converting_value_to_type_raises_TypeError_with_default(self):
        t = Torrent(metainfo={'info': {'files': [{'length': 123}, {'length': 456}, {'length': 789}]}})
        return_value = t._get_metainfo('info', 'files', 0, 'length', type=tuple, default='asdf')
        assert return_value == 'asdf'


class Test__set_metainfo:

    @pytest.fixture
    def t(self):
        t = Torrent()
        delattr(t, '_metainfo_initialized')
        return t

    def test_keypath_info_is_created_on_demand(self, t):
        assert 'info' not in t._metainfo_pure
        return_value = t._set_metainfo(('info', 'name'), 'My Name')
        assert return_value is None
        assert t._metainfo_pure['info']['name'] == 'My Name'

    def test_keypath_is_at_level_1(self, t):
        return_value = t._set_metainfo(('created_by',), 'me')
        assert return_value is None
        assert t._metainfo_pure['created_by'] == 'me'

    def test_keypath_is_at_level_2(self, t):
        return_value = t._set_metainfo(('info', 'files'), [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}])
        assert return_value is None
        assert t._metainfo_pure['info']['files'] == [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]

    def test_keypath_is_at_level_3(self, t):
        t._metainfo_pure = {'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}}
        return_value = t._set_metainfo(('info', 'files', 1), {'path': ['xxx', 'yyy']})
        assert return_value is None
        assert t._metainfo_pure['info']['files'] == [{'path': ['abc', 'def']}, {'path': ['xxx', 'yyy']}, {'path': ['cde', 'fgh']}]

    def test_keypath_is_at_level_4(self, t):
        t._metainfo_pure = {'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}}
        return_value = t._set_metainfo(('info', 'files', 1, 'path'), ['xxx', 'yyy'])
        assert return_value is None
        assert t._metainfo_pure['info']['files'] == [{'path': ['abc', 'def']}, {'path': ['xxx', 'yyy']}, {'path': ['cde', 'fgh']}]

    def test_keypath_is_at_level_5(self, t):
        t._metainfo_pure = {'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}}
        return_value = t._set_metainfo(('info', 'files', 1, 'path', 1), 'zzz')
        assert return_value is None
        assert t._metainfo_pure['info']['files'] == [{'path': ['abc', 'def']}, {'path': ['bcd', 'zzz']}, {'path': ['cde', 'fgh']}]

    def test_keypath_is_at_level_6(self, t):
        t._metainfo_pure = {'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}}
        with pytest.raises(TypeError, match=r"^'str' object does not support item assignment$"):
            t._set_metainfo(('info', 'files', 1, 'path', 1, 2), 'Z')
        assert t._metainfo_pure['info']['files'] == [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]

    def test_value_is_None_at_level_1(self, t):
        t._metainfo_pure = {'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}}
        t._set_metainfo(('info',), None)
        assert t._metainfo_pure == {}

    def test_value_is_None_at_level_2(self, t):
        t._metainfo_pure = {'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}}
        t._set_metainfo(('info', 'files'), None)
        assert t._metainfo_pure == {'info': {}}

    def test_value_is_None_at_level_3(self, t):
        t._metainfo_pure = {'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}}
        t._set_metainfo(('info', 'files', 1), None)
        assert t._metainfo_pure == {'info': {'files': [{'path': ['abc', 'def']}, {'path': ['cde', 'fgh']}]}}

    def test_value_is_None_at_level_4(self, t):
        t._metainfo_pure = {'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}}
        t._set_metainfo(('info', 'files', 1, 'path'), None)
        assert t._metainfo_pure == {'info': {'files': [{'path': ['abc', 'def']}, {}, {'path': ['cde', 'fgh']}]}}

    def test_value_is_None_at_level_5(self, t):
        t._metainfo_pure = {'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}}
        t._set_metainfo(('info', 'files', 1, 'path', 0), None)
        assert t._metainfo_pure == {'info': {'files': [{'path': ['abc', 'def']}, {'path': ['efg']}, {'path': ['cde', 'fgh']}]}}

    def test_value_is_None_at_level_6(self, t):
        t._metainfo_pure = {'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}}
        with pytest.raises(TypeError, match=r"^'str' object doesn't support item deletion$"):
            t._set_metainfo(('info', 'files', 1, 'path', 0, 0), None)
        assert t._metainfo_pure == {'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}}

    def test_value_is_None_and_keypath_is_not_found(self, t):
        t._metainfo_pure = {'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}}
        t._set_metainfo(('info', 'fools'), None)
        assert t._metainfo_pure == {'info': {'files': [{'path': ['abc', 'def']}, {'path': ['bcd', 'efg']}, {'path': ['cde', 'fgh']}]}}


class Test_announce:

    def test_announce_is_ignored_if_announce_list_exists(self):
        t = Torrent(metainfo={
            'announce': 'http://announce',
            'announce-list': [['http://announce:1.1'], ['http://announce:2.1', 'http://announce:2.2']],
        })
        assert t.announce == (('http://announce:1.1',), ('http://announce:2.1', 'http://announce:2.2'))

    def test_announce_is_default(self):
        t = Torrent(metainfo={
                    'announce': 'http://announce',
        })
        assert 'announce-list' not in t.metainfo
        assert t.announce == (('http://announce',),)

    def test_empty_tuple_is_default(self):
        t = Torrent()
        assert 'announce' not in t.metainfo
        assert 'announce-list' not in t.metainfo
        assert t.announce == ()


class Test__handle_kwarg_announce:

    @pytest.mark.parametrize('existing_metainfo', (
        pytest.param({}, id='No existing metainfo'),
        pytest.param({'announce': 'http://old'}, id='"announce" exists in metainfo'),
        pytest.param({'announce-list': (('http://old:1',),)}, id='"announce-list" exists in metainfo'),
    ))
    def test_announce_is_None(self, existing_metainfo):
        t = Torrent(metainfo=existing_metainfo)
        return_value = t._handle_kwarg_announce(None)
        assert return_value is None
        assert 'announce' not in t.metainfo
        assert 'announce-list' not in t.metainfo

    @pytest.mark.parametrize('existing_metainfo', (
        pytest.param({}, id='No existing metainfo'),
        pytest.param({'announce': 'http://old'}, id='"announce" exists in metainfo'),
        pytest.param({'announce-list': (('http://old:1',),)}, id='"announce-list" exists in metainfo'),
    ))
    def test_announce_is_string(self, existing_metainfo):
        t = Torrent(metainfo=existing_metainfo)
        return_value = t._handle_kwarg_announce('http://announce')
        assert return_value is None
        assert t.metainfo['announce'] == 'http://announce'
        assert 'announce-list' not in t.metainfo

    @pytest.mark.parametrize('existing_metainfo', (
        pytest.param({}, id='No existing metainfo'),
        pytest.param({'announce': 'http://old'}, id='"announce" exists in metainfo'),
        pytest.param({'announce-list': (('http://old:1',),)}, id='"announce-list" exists in metainfo'),
    ))
    def test_announce_is_empty_string(self, existing_metainfo):
        t = Torrent(metainfo=existing_metainfo)
        with pytest.raises(ValueError, match=r"^announce is empty string$"):
            t._handle_kwarg_announce('')
        assert t.metainfo == existing_metainfo

    @pytest.mark.parametrize('existing_metainfo', (
        pytest.param({}, id='No existing metainfo'),
        pytest.param({'announce': 'http://old'}, id='"announce" exists in metainfo'),
        pytest.param({'announce-list': (('http://old:1',),)}, id='"announce-list" exists in metainfo'),
    ))
    @pytest.mark.parametrize('convert', (list, tuple, iter))
    def test_announce_is_iterable_of_urls(self, convert, existing_metainfo):
        t = Torrent(metainfo=existing_metainfo)
        urls = convert(('http://announce:1', 'http://announce:2', 'http://announce:3'))
        return_value = t._handle_kwarg_announce(urls)
        assert return_value is None
        assert t.metainfo['announce-list'] == (('http://announce:1',), ('http://announce:2',), ('http://announce:3',))
        assert 'announce' not in t.metainfo

    @pytest.mark.parametrize('existing_metainfo', (
        pytest.param({}, id='No existing metainfo'),
        pytest.param({'announce': 'http://old'}, id='"announce" exists in metainfo'),
        pytest.param({'announce-list': (('http://old:1',),)}, id='"announce-list" exists in metainfo'),
    ))
    @pytest.mark.parametrize('convert', (list, tuple, iter))
    def test_announce_is_iterable_of_tiers(self, convert, existing_metainfo):
        t = Torrent(metainfo=existing_metainfo)
        urls = convert([
            ('http://announce:1.1', 'http://announce:1.2'),
            convert(('http://announce:2.1', 'http://announce:2.2')),
        ])
        return_value = t._handle_kwarg_announce(urls)
        assert return_value is None
        assert t.metainfo['announce-list'] == (
            ('http://announce:1.1', 'http://announce:1.2'),
            ('http://announce:2.1', 'http://announce:2.2'),
        )
        assert 'announce' not in t.metainfo

    @pytest.mark.parametrize('existing_metainfo', (
        pytest.param({}, id='No existing metainfo'),
        pytest.param({'announce': 'http://old'}, id='"announce" exists in metainfo'),
        pytest.param({'announce-list': (('http://old:1',),)}, id='"announce-list" exists in metainfo'),
    ))
    @pytest.mark.parametrize('convert', (list, tuple, iter))
    def test_announce_is_empty_iterable(self, convert, existing_metainfo):
        t = Torrent(metainfo=existing_metainfo)
        urls = convert(())
        return_value = t._handle_kwarg_announce(urls)
        assert return_value is None
        assert 'announce' not in t.metainfo
        assert 'announce-list' not in t.metainfo

    def test_announce_is_garbage(self):
        t = Torrent()
        with pytest.raises(TypeError, match=r"^Unexpected announce type: int: 123$"):
            t._handle_kwarg_announce(123)

    @pytest.mark.parametrize('convert', (list, tuple, iter))
    def test_announce_is_iterable_that_contains_garbage(self, convert):
        t = Torrent()
        urls = convert(('http://announce:1', 123, 'http://announce:3'))
        with pytest.raises(TypeError, match=r"^Unexpected announce type: int: 123$"):
            t._handle_kwarg_announce(urls)

    @pytest.mark.parametrize('convert', (list, tuple, iter))
    def test_announce_is_iterable_that_contains_tier_that_contains_garbage(self, convert):
        t = Torrent()
        urls = convert((
            ('http://announce:1.1', 'http://announce:1.2'),
            ('http://announce:2.1', 123, 'http://announce:2.2'),
            ('http://announce:3.1', 'http://announce:3.2'),
        ))
        with pytest.raises(TypeError, match=r"^Unexpected announce type: int: 123$"):
            t._handle_kwarg_announce(urls)


class Test_webseeds:

    @pytest.fixture(autouse=True)
    def mock__get_metainfo(self, mocker):
        mocker.patch.object(Torrent, '_get_metainfo', Mock())

    def test_webseeds(self):
        t = Torrent()
        assert t.webseeds == t._get_metainfo.return_value
        assert t._get_metainfo.call_args_list == [
            call('url-list', type=tuple, default=()),
        ]


class Test__handle_kwarg_webseeds:

    @pytest.mark.parametrize('existing_metainfo', (
        pytest.param({}, id='"webseeds" does not exist in metainfo'),
        pytest.param({'webseeds': 'http://old'}, id='"webseeds" exists in metainfo'),
    ))
    def test_webseeds_is_None(self, existing_metainfo):
        t = Torrent(metainfo=existing_metainfo)
        return_value = t._handle_kwarg_webseeds(None)
        assert return_value is None
        assert 'url-list' not in t.metainfo

    @pytest.mark.parametrize('existing_metainfo', (
        pytest.param({}, id='"webseeds" does not exist in metainfo'),
        pytest.param({'webseeds': 'http://old'}, id='"webseeds" exists in metainfo'),
    ))
    def test_webseeds_is_string(self, existing_metainfo):
        t = Torrent(metainfo=existing_metainfo)
        return_value = t._handle_kwarg_webseeds('http://webseed')
        assert return_value is None
        assert t.metainfo['url-list'] == ('http://webseed',)

    @pytest.mark.parametrize('existing_metainfo', (
        pytest.param({}, id='"webseeds" does not exist in metainfo'),
        pytest.param({'webseeds': 'http://old'}, id='"webseeds" exists in metainfo'),
    ))
    def test_webseeds_is_empty_string(self, existing_metainfo):
        t = Torrent(metainfo=existing_metainfo)
        with pytest.raises(ValueError, match=r"^webseed is empty string$"):
            t._handle_kwarg_webseeds('')
        assert t.metainfo == existing_metainfo

    @pytest.mark.parametrize('existing_metainfo', (
        pytest.param({}, id='"webseeds" does not exist in metainfo'),
        pytest.param({'webseeds': 'http://old'}, id='"webseeds" exists in metainfo'),
    ))
    @pytest.mark.parametrize('convert', (list, tuple, iter))
    def test_webseeds_is_iterable(self, convert, existing_metainfo):
        t = Torrent(metainfo=existing_metainfo)
        webseeds = convert(('http://webseed1', 'http://webseed2', 'http://webseed3'))
        return_value = t._handle_kwarg_webseeds(webseeds)
        assert return_value is None
        assert t.metainfo['url-list'] == ('http://webseed1', 'http://webseed2', 'http://webseed3')

    @pytest.mark.parametrize('existing_metainfo', (
        pytest.param({}, id='"webseeds" does not exist in metainfo'),
        pytest.param({'webseeds': 'http://old'}, id='"webseeds" exists in metainfo'),
    ))
    @pytest.mark.parametrize('convert', (list, tuple, iter))
    def test_webseeds_is_iterable_that_contains_garbage(self, convert, existing_metainfo):
        t = Torrent(metainfo=existing_metainfo)
        webseeds = convert(('http://webseed1', 123, 'http://webseed3'))
        with pytest.raises(TypeError, match=r"^Unexpected webseed type: int: 123$"):
            t._handle_kwarg_webseeds(webseeds)
        assert t.metainfo == existing_metainfo

    @pytest.mark.parametrize('existing_metainfo', (
        pytest.param({}, id='"webseeds" does not exist in metainfo'),
        pytest.param({'webseeds': 'http://old'}, id='"webseeds" exists in metainfo'),
    ))
    @pytest.mark.parametrize('convert', (list, tuple, iter))
    def test_webseeds_is_empty_iterable(self, convert, existing_metainfo):
        t = Torrent(metainfo=existing_metainfo)
        webseeds = convert(())
        return_value = t._handle_kwarg_webseeds(webseeds)
        assert return_value is None
        assert 'url-list' not in t.metainfo

    @pytest.mark.parametrize('existing_metainfo', (
        pytest.param({}, id='"webseeds" does not exist in metainfo'),
        pytest.param({'webseeds': 'http://old'}, id='"webseeds" exists in metainfo'),
    ))
    @pytest.mark.parametrize('convert', (list, tuple, iter))
    def test_webseeds_is_garbage(self, convert, existing_metainfo):
        t = Torrent(metainfo=existing_metainfo)
        with pytest.raises(TypeError, match=r"^Unexpected webseeds type: int: 123$"):
            t._handle_kwarg_webseeds(123)
        assert t.metainfo == existing_metainfo


class Test_httpseeds:

    @pytest.fixture(autouse=True)
    def mock__get_metainfo(self, mocker):
        mocker.patch.object(Torrent, '_get_metainfo', Mock())

    def test_httpseeds(self):
        t = Torrent()
        assert t.httpseeds == t._get_metainfo.return_value
        assert t._get_metainfo.call_args_list == [
            call('httpseeds', type=tuple, default=()),
        ]


class Test__handle_kwarg_httpseeds:

    @pytest.mark.parametrize('existing_metainfo', (
        pytest.param({}, id='"httpseeds" does not exist in metainfo'),
        pytest.param({'httpseeds': 'http://old'}, id='"httpseeds" exists in metainfo'),
    ))
    def test_httpseeds_is_string(self, existing_metainfo):
        t = Torrent(metainfo=existing_metainfo)
        return_value = t._handle_kwarg_httpseeds('http://httpseed')
        assert return_value is None
        assert t.metainfo['httpseeds'] == ('http://httpseed',)

    @pytest.mark.parametrize('existing_metainfo', (
        pytest.param({}, id='"httpseeds" does not exist in metainfo'),
        pytest.param({'httpseeds': 'http://old'}, id='"httpseeds" exists in metainfo'),
    ))
    def test_httpseeds_is_empty_string(self, existing_metainfo):
        t = Torrent(metainfo=existing_metainfo)
        with pytest.raises(ValueError, match=r"^httpseed is empty string$"):
            t._handle_kwarg_httpseeds('')
        assert t.metainfo == existing_metainfo

    @pytest.mark.parametrize('existing_metainfo', (
        pytest.param({}, id='"httpseeds" does not exist in metainfo'),
        pytest.param({'httpseeds': 'http://old'}, id='"httpseeds" exists in metainfo'),
    ))
    @pytest.mark.parametrize('convert', (list, tuple, iter))
    def test_httpseeds_is_iterable(self, convert, existing_metainfo):
        t = Torrent(metainfo=existing_metainfo)
        httpseeds = convert(('http://httpseed1', 'http://httpseed2', 'http://httpseed3'))
        return_value = t._handle_kwarg_httpseeds(httpseeds)
        assert return_value is None
        assert t.metainfo['httpseeds'] == ('http://httpseed1', 'http://httpseed2', 'http://httpseed3')

    @pytest.mark.parametrize('existing_metainfo', (
        pytest.param({}, id='"httpseeds" does not exist in metainfo'),
        pytest.param({'httpseeds': 'http://old'}, id='"httpseeds" exists in metainfo'),
    ))
    @pytest.mark.parametrize('convert', (list, tuple, iter))
    def test_httpseeds_is_iterable_that_contains_garbage(self, convert, existing_metainfo):
        t = Torrent(metainfo=existing_metainfo)
        httpseeds = convert(('http://httpseed1', 123, 'http://httpseed3'))
        with pytest.raises(TypeError, match=r"^Unexpected httpseed type: int: 123$"):
            t._handle_kwarg_httpseeds(httpseeds)
        assert t.metainfo == existing_metainfo

    @pytest.mark.parametrize('existing_metainfo', (
        pytest.param({}, id='"httpseeds" does not exist in metainfo'),
        pytest.param({'httpseeds': 'http://old'}, id='"httpseeds" exists in metainfo'),
    ))
    @pytest.mark.parametrize('convert', (list, tuple, iter))
    def test_httpseeds_is_empty_iterable(self, convert, existing_metainfo):
        t = Torrent(metainfo=existing_metainfo)
        httpseeds = convert(())
        return_value = t._handle_kwarg_httpseeds(httpseeds)
        assert return_value is None
        assert 'httpseeds' not in t.metainfo

    @pytest.mark.parametrize('existing_metainfo', (
        pytest.param({}, id='"httpseeds" does not exist in metainfo'),
        pytest.param({'httpseeds': 'http://old'}, id='"httpseeds" exists in metainfo'),
    ))
    @pytest.mark.parametrize('convert', (list, tuple, iter))
    def test_httpseeds_is_garbage(self, convert, existing_metainfo):
        t = Torrent(metainfo=existing_metainfo)
        with pytest.raises(TypeError, match=r"^Unexpected httpseeds type: int: 123$"):
            t._handle_kwarg_httpseeds(123)
        assert t.metainfo == existing_metainfo


class Test_comment:

    @pytest.fixture(autouse=True)
    def mock__get_metainfo(self, mocker):
        mocker.patch.object(Torrent, '_get_metainfo', Mock())

    def test_comment(self):
        t = Torrent()
        assert t.comment == t._get_metainfo.return_value
        assert t._get_metainfo.call_args_list == [
            call('comment', type=str, default=None),
        ]


class Test__handle_kwarg_comment:

    @pytest.fixture(autouse=True)
    def mock__set_metainfo(self, mocker):
        mocker.patch.object(Torrent, '_set_metainfo', Mock())

    def test_comment_is_None(self):
        t = Torrent()
        return_value = t._handle_kwarg_comment(None)
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('comment',), None)]

    def test_comment_is_string(self):
        t = Torrent()
        return_value = t._handle_kwarg_comment('my new comment')
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('comment',), 'my new comment')]

    def test_comment_is_empty_string(self):
        t = Torrent()
        return_value = t._handle_kwarg_comment('')
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('comment',), None)]

    def test_comment_is_garbage(self):
        t = Torrent()
        with pytest.raises(TypeError, match=r"^Unexpected comment type: int: 123$"):
            t._handle_kwarg_comment(123)
        assert t._set_metainfo.call_args_list == []


class Test_created_by:

    @pytest.fixture(autouse=True)
    def mock__get_metainfo(self, mocker):
        mocker.patch.object(Torrent, '_get_metainfo', Mock())

    def test_created_by(self):
        t = Torrent()
        assert t.created_by == t._get_metainfo.return_value
        assert t._get_metainfo.call_args_list == [
            call('created by', type=str, default=None),
        ]


class Test__handle_kwarg_created_by:

    @pytest.fixture(autouse=True)
    def mock__set_metainfo(self, mocker):
        mocker.patch.object(Torrent, '_set_metainfo', Mock())

    def test_created_by_is_None(self):
        t = Torrent()
        return_value = t._handle_kwarg_created_by(None)
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('created by',), None)]

    def test_created_by_is_string(self):
        t = Torrent()
        return_value = t._handle_kwarg_created_by('me')
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('created by',), 'me')]

    def test_created_by_is_empty_string(self):
        t = Torrent()
        return_value = t._handle_kwarg_created_by('')
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('created by',), None)]

    def test_created_by_is_garbage(self):
        t = Torrent()
        with pytest.raises(TypeError, match=r"^Unexpected created_by type: int: 123$"):
            t._handle_kwarg_created_by(123)
        assert t._set_metainfo.call_args_list == []


class Test_creation_date:

    @pytest.fixture(autouse=True)
    def mock__get_metainfo(self, mocker):
        mocker.patch.object(Torrent, '_get_metainfo', Mock())

    def test_creation_date(self):
        t = Torrent()
        assert t.creation_date == t._get_metainfo.return_value
        assert t._get_metainfo.call_args_list == [
            call('creation date', type=datetime.datetime.fromtimestamp, default=None),
        ]


class Test__handle_kwarg_creation_date:

    @pytest.fixture(autouse=True)
    def mock__set_metainfo(self, mocker):
        mocker.patch.object(Torrent, '_set_metainfo', Mock())

    def test_creation_date_is_None(self):
        t = Torrent()
        return_value = t._handle_kwarg_creation_date(None)
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('creation date',), None)]

    @pytest.mark.parametrize('number_type', (int, float))
    @pytest.mark.parametrize('number', (0, 1, -1, 1234567890))
    def test_creation_date_is_number(self, number, number_type):
        t = Torrent()
        return_value = t._handle_kwarg_creation_date(number_type(number))
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('creation date',), int(number))]

    def test_creation_date_is_datetime(self):
        t = Torrent()
        return_value = t._handle_kwarg_creation_date(datetime.datetime.fromtimestamp(123))
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('creation date',), 123)]

    def test_creation_date_is_garbage(self):
        t = Torrent()
        with pytest.raises(TypeError, match=r"^Unexpected creation_date type: str: 'hello'$"):
            t._handle_kwarg_creation_date('hello')
        assert t._set_metainfo.call_args_list == []


class Test_filelist:

    @pytest.fixture(autouse=True)
    def mock__get_metainfo(self, mocker):
        mocker.patch.object(Torrent, '_get_metainfo', Mock())

    @pytest.mark.parametrize(
        argnames='metainfo, exp_return_value',
        argvalues=(
            pytest.param({}, (), id='No "info" section'),
            pytest.param(
                {
                    'files': [
                        {'length': 123, 'path': ['abc', 'def']},
                        {'length': 234, 'path': ['bcd', 'efg']},
                    ],
                },
                (),
                id='No "name" field',
            ),
            pytest.param({'name': 'The Name'}, (), id='No "files"'),
            pytest.param(
                {
                    'name': 'The Name',
                    'files': [
                        {'length': 123},
                        {'length': 234, 'path': ['bcd', 'efg']},
                    ],
                },
                (_utils.File('The Name', 'bcd', 'efg', size=234),),
                id='No "path" field',
            ),
            pytest.param(
                {
                    'name': 'The Name',
                    'files': [
                        {'length': 123, 'path': ['abc', 'def']},
                        {'path': ['bcd', 'efg']},
                    ],
                },
                (_utils.File('The Name', 'abc', 'def', size=123),),
                id='No "length" field',
            ),
            pytest.param(
                {
                    'name': 'The Name',
                    'files': [
                        {'length': 123, 'path': ['abc', 'def']},
                        {'length': 234, 'path': ['bcd', 'efg']},
                    ],
                },
                (
                    _utils.File('The Name', 'abc', 'def', size=123),
                    _utils.File('The Name', 'bcd', 'efg', size=234),
                ),
                id='Complete multi-file metainfo',
            ),
            pytest.param(
                {
                    'name': '',
                    'files': [
                        {'length': 123, 'path': ['abc', 'def']},
                        {'length': 234, 'path': ['bcd', 'efg']},
                    ],
                },
                (),
                id='"name" field is empty',
            ),
            pytest.param(
                {
                    'name': 'The Name',
                    'files': [],
                },
                (),
                id='"field" field is empty',
            ),
            pytest.param(
                {
                    'name': 'The Name',
                    'files': [
                        {'length': 0, 'path': ['abc', 'def']},
                        {'length': 234, 'path': ['bcd', 'efg']},
                    ],
                },
                (
                    _utils.File('The Name', 'bcd', 'efg', size=234),
                ),
                id='"length" field is 0',
            ),
            pytest.param(
                {'name': 'The Name', 'length': 123},
                (_utils.File('The Name', size=123),),
                id='Single-file torrent',
            ),
            pytest.param(
                {'name': 'The Name', 'length': 0},
                (),
                id='Single-file torrent with "length" of 0',
            ),
            pytest.param(
                {'length': 123},
                (),
                id='Single-file torrent with no "name"',
            ),
            pytest.param(
                {'name': '', 'length': 0},
                (),
                id='Single-file torrent with empty "name"',
            ),
        ),
    )
    def test_files(self, metainfo, exp_return_value):
        t = Torrent()
        t._get_metainfo.return_value = metainfo
        assert t.filelist == exp_return_value
        assert t._get_metainfo.call_args_list == [
            call('info', type=dict, default=None),
        ]


class Test__handle_kwarg_files:

    @pytest.fixture(autouse=True)
    def mock__set_metainfo(self, mocker):
        mocker.patch.object(Torrent, '_set_metainfo', Mock())

    def test_files_is_None(self):
        t = Torrent()
        return_value = t._handle_kwarg_files(None)
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('info', 'files'), None)]

    @pytest.mark.parametrize('convert', (list, tuple, iter))
    def test_files_is_iterable_of_mappings(self, convert):
        t = Torrent()
        files = (
            {'length': 123, 'path': ['foo', 'bar']},
            {'length': 234, 'path': ['baz'], 'hello': 'you'},
        )
        return_value = t._handle_kwarg_files(convert(files))
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('info', 'files'), list(files))]

    def test_files_is_iterable_of_File_instances(self):
        t = Torrent()
        files = (
            _utils.File('foo', 'bar', size=123),
            _utils.File('baz', size=234),
        )
        return_value = t._handle_kwarg_files(files)
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('info', 'files'), [
            {'length': 123, 'path': ('foo', 'bar')},
            {'length': 234, 'path': ('baz',)},
        ])]

    @pytest.mark.parametrize('convert', (list, tuple))
    def test_files_is_iterable_of_sequences(self, convert):
        t = Torrent()
        files = (
            convert((['foo', 'bar'], 123)),
            convert((['baz'], 234, 'hello', 'you')),
        )
        return_value = t._handle_kwarg_files(convert(files))
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('info', 'files'), [
            {'length': 123, 'path': ['foo', 'bar']},
            {'length': 234, 'path': ['baz']},
        ])]

    def test_file_in_sequence_is_iterator(self):
        t = Torrent()
        file_iter = iter((['baz'], 234, 'hello', 'you'))
        files = (
            [['foo', 'bar'], 123],
            file_iter,
        )
        exp_msg = f'Unexpected file type: {type(file_iter).__name__}: {file_iter!r}'
        with pytest.raises(TypeError, match=rf"^{re.escape(str(exp_msg))}$"):
            t._handle_kwarg_files(files)
        assert t._set_metainfo.call_args_list == []

    def test_file_in_sequence_is_too_short(self):
        t = Torrent()
        file = (['baz'],)
        files = (
            [['foo', 'bar'], 123],
            file,
        )
        exp_msg = f'Expected (<path>, <size>): {file!r}'
        with pytest.raises(ValueError, match=rf"^{re.escape(str(exp_msg))}$"):
            t._handle_kwarg_files(files)
        assert t._set_metainfo.call_args_list == []

    def test_file_in_sequence_has_garbage_length(self):
        t = Torrent()
        files = (
            (('foo', 'bar'), 123),
            (('my', 'path'), 'invalid size'),
        )
        exp_msg = "Expected int for size, not str: 'invalid size'"
        with pytest.raises(TypeError, match=rf"^{re.escape(str(exp_msg))}$"):
            t._handle_kwarg_files(files)
        assert t._set_metainfo.call_args_list == []

    def test_file_in_sequence_has_garbage_path(self):
        t = Torrent()
        files = (
            (('foo', 'bar'), 123),
            ('bad/path', 234),
        )
        exp_msg = "Expected sequence for path, not str: 'bad/path'"
        with pytest.raises(TypeError, match=rf"^{re.escape(str(exp_msg))}$"):
            t._handle_kwarg_files(files)
        assert t._set_metainfo.call_args_list == []

    def test_file_in_sequence_has_garbage_path_item(self):
        t = Torrent()
        files = (
            (('foo', 'bar'), 123),
            (('foo', ['i', 'am', 'nested!'], 'bar'), 234),
        )
        exp_msg = 'Expected str in path, not list: ' + str(['i', 'am', 'nested!'])
        with pytest.raises(TypeError, match=rf"^{re.escape(str(exp_msg))}$"):
            t._handle_kwarg_files(files)
        assert t._set_metainfo.call_args_list == []

    def test_files_is_garbage(self):
        t = Torrent()
        with pytest.raises(TypeError, match=r"^Unexpected files type: int: 123"):
            t._handle_kwarg_files(123)
        assert t._set_metainfo.call_args_list == []


class Test_filetree:

    @pytest.mark.parametrize(
        argnames='metainfo, exp_filetree',
        argvalues=(
            pytest.param({}, {}, id='No files'),

            pytest.param(
                {'info': {
                    'name': 'base',
                    'length': 123,
                }},
                {
                    _utils.File('base', size=123): _utils.File('base', size=123),
                },
                id='Single file',
            ),

            pytest.param(
                {'info': {
                    'name': 'base',
                    'files': [
                        {'length': 123, 'path': ['foo']},
                    ],
                }},
                {
                    _utils.File('base', size=123): {
                        _utils.File('base', 'foo', size=123): _utils.File('base', 'foo', size=123),
                    },
                },
                id='Single file in directory',
            ),

            pytest.param(
                {'info': {
                    'name': 'base',
                    'files': [
                        {'length': 1, 'path': ['foo']},
                        {'length': 2, 'path': ['bar', 'baz']},
                    ],
                }},
                {
                    _utils.File('base', size=3): {
                        _utils.File('base', 'foo', size=1): _utils.File('base', 'foo', size=1),
                        _utils.File('base', 'bar', size=2): {
                            _utils.File('base', 'bar', 'baz', size=2): _utils.File('base', 'bar', 'baz', size=2),
                        },
                    },
                },
                id='Simple tree',
            ),

            pytest.param(
                {'info': {
                    'name': 'base',
                    'files': [
                        {'path': ['a'], 'length': 1},
                        {'path': ['b'], 'length': 2},
                        {'path': ['foo', 'c'], 'length': 3},
                        {'path': ['foo', 'd'], 'length': 4},
                        {'path': ['foo', 'bar', 'e'], 'length': 5},
                        {'path': ['foo', 'bar', 'f'], 'length': 6},
                        {'path': ['baz', 'g'], 'length': 7},
                        {'path': ['baz', 'h'], 'length': 8},
                        {'path': ['deeply', 'nested', 'subtree', 'x'], 'length': 9},
                    ],
                }},
                {
                    _utils.File('base', size=45): {
                        _utils.File('base', 'a', size=1): _utils.File('base', 'a', size=1),
                        _utils.File('base', 'b', size=2): _utils.File('base', 'b', size=2),
                        _utils.File('base', 'foo', size=18): {
                            _utils.File('base', 'foo', 'c', size=3): _utils.File('base', 'foo', 'c', size=3),
                            _utils.File('base', 'foo', 'd', size=4): _utils.File('base', 'foo', 'd', size=4),
                            _utils.File('base', 'foo', 'bar', size=11): {
                                _utils.File('base', 'foo', 'bar', 'e', size=5): _utils.File('base', 'foo', 'bar', 'e', size=5),
                                _utils.File('base', 'foo', 'bar', 'f', size=6): _utils.File('base', 'foo', 'bar', 'f', size=6),
                            },
                        },
                        _utils.File('base', 'baz', size=15): {
                            _utils.File('base', 'baz', 'g', size=7): _utils.File('base', 'baz', 'g', size=7),
                            _utils.File('base', 'baz', 'h', size=8): _utils.File('base', 'baz', 'h', size=8),
                        },
                        _utils.File('base', 'deeply', size=9): {
                            _utils.File('base', 'deeply', 'nested', size=9): {
                                _utils.File('base', 'deeply', 'nested', 'subtree', size=9): {
                                    _utils.File('base', 'deeply', 'nested', 'subtree', 'x', size=9): \
                                    _utils.File('base', 'deeply', 'nested', 'subtree', 'x', size=9),
                                },
                            },
                        },
                    },
                },
                id='Complex tree',
            ),

            pytest.param(
                {'info': {
                    'name': 'mytorrent',
                    'files': [
                        {'length': 3, 'path': ['foo']},
                        {'length': 6, 'path': ['bar', 'baz']},
                    ],
                }},
                {
                    _utils.File('mytorrent', size=9): {
                        _utils.File('mytorrent', 'foo', size=3): _utils.File('mytorrent', 'foo', size=3),
                        _utils.File('mytorrent', 'bar', size=6): {
                            _utils.File('mytorrent', 'bar', 'baz', size=6): _utils.File('mytorrent', 'bar', 'baz', size=6),
                        },
                    },
                },
                id='Docstring example',
            ),
        ),
    )
    def test_filetree(self, metainfo, exp_filetree):
        t = Torrent(metainfo=metainfo)
        assert t.filetree == exp_filetree


class Test_infohash:

    def test_infohash_is_preset(self):
        t = Torrent()
        t._infohash = 'd34db33f'
        assert t.infohash == 'd34db33f'

    def test_infohash_is_calculated(self, mocker):
        t = Torrent(metainfo={'info': {'the': 'metainfo'}})
        mocks = Mock()
        mocks.attach_mock(mocker.patch.object(t, 'validate'), 'validate')
        mocks.attach_mock(mocker.patch('hashlib.sha1', Mock(hexdigest=Mock())), 'sha1')
        mocks.attach_mock(mocker.patch('torf._bencode.encode'), 'encode')
        assert not hasattr(t, '_infohash'), t._infohash
        assert t.infohash == mocks.sha1.return_value.hexdigest.return_value
        assert mocks.mock_calls == [
            call.validate(),
            call.encode({b'the': b'metainfo'}),
            call.sha1(mocks.encode.return_value),
            call.sha1().hexdigest(),
        ]


class Test__handle_kwarg_infohash:

    @pytest.mark.parametrize('existing_infohash', (None, 'd34db33f'))
    def test_infohash_is_None(self, existing_infohash):
        t = Torrent()
        if existing_infohash is not None:
            t._infohash = existing_infohash
        return_value = t._handle_kwarg_infohash(None)
        assert return_value is None
        assert not hasattr(t, '_infohash')

    def test_infohash_is_base16(self):
        t = Torrent()
        return_value = t._handle_kwarg_infohash('ABCDEF0123456789ABCDEF0123456789ABCDEF01')
        assert return_value is None
        assert t._infohash == 'abcdef0123456789abcdef0123456789abcdef01'

    def test_infohash_is_base32(self):
        t = Torrent()
        return_value = t._handle_kwarg_infohash('ABCDEFGHIJKLMNOPQRSTUVWXYZ234567')
        assert return_value is None
        assert t._infohash == '00443214c74254b635cf84653a56d7c675be77df'

    def test_infohash_is_not_hash(self):
        t = Torrent()
        with pytest.raises(ValueError, match=r"^Unexpected infohash format: 'foo'$"):
            t._handle_kwarg_infohash('foo')

    def test_infohash_is_not_str(self):
        t = Torrent()
        with pytest.raises(TypeError, match=r"^Unexpected infohash type: int: 123$"):
            t._handle_kwarg_infohash(123)


def test_infohash_base32(mocker):
    t = Torrent()
    mocker.patch.object(type(t), 'infohash', PropertyMock(return_value='d34db33f'))
    mocks = Mock()
    mocks.attach_mock(mocker.patch.object(t, 'validate'), 'validate')
    mocks.attach_mock(mocker.patch('base64.b16decode'), 'b16decode')
    mocks.attach_mock(mocker.patch('base64.b32encode', Mock()), 'b32encode')
    assert t.infohash_base32 == mocks.b32encode.return_value
    assert mocks.mock_calls == [
        call.b16decode('D34DB33F'),
        call.b32encode(mocks.b16decode.return_value),
    ]


class Test_name:

    @pytest.fixture(autouse=True)
    def mock__get_metainfo(self, mocker):
        mocker.patch.object(Torrent, '_get_metainfo', Mock())

    def test_name(self):
        t = Torrent()
        assert t.name == t._get_metainfo.return_value
        assert t._get_metainfo.call_args_list == [
            call('info', 'name', type=str, default=None),
        ]


class Test__handle_kwarg_name:

    @pytest.fixture(autouse=True)
    def mock__set_metainfo(self, mocker):
        mocker.patch.object(Torrent, '_set_metainfo', Mock())

    def test_name_is_None(self):
        t = Torrent()
        return_value = t._handle_kwarg_name(None)
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('info', 'name',), None)]

    def test_name_is_string(self):
        t = Torrent()
        return_value = t._handle_kwarg_name('my new name')
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('info', 'name',), 'my new name')]

    def test_name_is_empty_string(self):
        t = Torrent()
        return_value = t._handle_kwarg_name('')
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('info', 'name',), None)]

    def test_name_is_garbage(self):
        t = Torrent()
        with pytest.raises(TypeError, match=r"^Unexpected name type: int: 123$"):
            t._handle_kwarg_name(123)
        assert t._set_metainfo.call_args_list == []


class Test_pieces:

    @pytest.fixture(autouse=True)
    def mock__get_metainfo(self, mocker):
        mocker.patch.object(Torrent, '_get_metainfo', Mock())

    def test_pieces_is_not_known(self):
        t = Torrent()
        t._get_metainfo.return_value = None
        assert t.pieces == ()
        assert t._get_metainfo.call_args_list == [
            call('info', 'pieces', type=bytes, default=None),
        ]

    def test_pieces_is_known(self):
        t = Torrent()
        t._get_metainfo.return_value = (
            b'\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00'
            + b'\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01'
            + b'\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02'
        )
        assert t.pieces == (
            b'\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00',
            b'\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01',
            b'\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02',
        )
        assert t._get_metainfo.call_args_list == [
            call('info', 'pieces', type=bytes, default=None),
        ]


class Test__handle_kwarg_pieces:

    @pytest.fixture(autouse=True)
    def mock__set_metainfo(self, mocker):
        mocker.patch.object(Torrent, '_set_metainfo', Mock())

    def test_pieces_is_None(self):
        t = Torrent()
        return_value = t._handle_kwarg_pieces(None)
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('info', 'pieces',), None)]

    def test_pieces_is_bytes(self):
        t = Torrent()
        return_value = t._handle_kwarg_pieces(b'new pieces')
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('info', 'pieces',), b'new pieces')]

    def test_pieces_is_empty_bytes(self):
        t = Torrent()
        return_value = t._handle_kwarg_pieces(b'')
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('info', 'pieces',), None)]

    def test_pieces_is_bytearray(self):
        t = Torrent()
        return_value = t._handle_kwarg_pieces(bytearray(b'new pieces'))
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('info', 'pieces',), bytearray(b'new pieces'))]

    def test_pieces_is_empty_bytearray(self):
        t = Torrent()
        return_value = t._handle_kwarg_pieces(bytearray(b''))
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('info', 'pieces',), None)]

    @pytest.mark.parametrize('convert', (list, tuple, iter))
    def test_pieces_is_iterable_of_bytes_and_bytearrays(self, convert):
        t = Torrent()
        pieces = (b'\00', bytearray(b'\01'), b'\02', bytearray(b'\03'))
        return_value = t._handle_kwarg_pieces(convert(pieces))
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('info', 'pieces',), b'\00\01\02\03')]

    @pytest.mark.parametrize('convert', (list, tuple, iter))
    def test_pieces_is_iterable_that_contains_garbage(self, convert):
        t = Torrent()
        pieces = (b'\00', bytearray(b'\01'), 'this is not bytes', bytearray(b'\03'))
        with pytest.raises(TypeError, match=r"^Unexpected piece type: str: 'this is not bytes'$"):
            t._handle_kwarg_pieces(convert(pieces))
        assert t._set_metainfo.call_args_list == []

    @pytest.mark.parametrize('convert', (list, tuple, iter))
    def test_pieces_is_garbage(self, convert):
        t = Torrent()
        with pytest.raises(TypeError, match=r"^Unexpected pieces type: str: 'this is not bytes'$"):
            t._handle_kwarg_pieces('this is not bytes')
        assert t._set_metainfo.call_args_list == []


@pytest.mark.parametrize(
    argnames='size, piece_length, exp_pieces_count',
    argvalues=(
        pytest.param(900, 100, 9),
        pytest.param(900, 299, 4),
        pytest.param(900, 300, 3),
        pytest.param(900, 900, 1),
        pytest.param(900, 901, 1),
        pytest.param(1, 9000, 1),
        pytest.param(0, 9000, 0),
        pytest.param(900, 0, 0),
    ),
)
def test_pieces_count(size, piece_length, exp_pieces_count, mocker):
    t = Torrent()
    mocker.patch.object(type(t), 'size', PropertyMock(return_value=size))
    mocker.patch.object(type(t), 'piece_length', PropertyMock(return_value=piece_length))
    assert t.pieces_count == exp_pieces_count


class Test_piece_length:

    @pytest.fixture(autouse=True)
    def mock__get_metainfo(self, mocker):
        mocker.patch.object(Torrent, '_get_metainfo', Mock())

    def test_piece_length(self):
        t = Torrent()
        assert t.piece_length == t._get_metainfo.return_value
        assert t._get_metainfo.call_args_list == [
            call('info', 'piece length', type=int, default=None),
        ]


class Test_private:

    @pytest.mark.parametrize(
        argnames='metainfo, exp_private',
        argvalues=(
            pytest.param({'info': {'private': 1}}, True),
            pytest.param({'info': {'private': 0}}, False),
            pytest.param({'info': {}}, False),
            pytest.param({'info': {'private': 'yes'}}, True),
            pytest.param({'info': {'private': 'totally'}}, True),
            pytest.param({'info': {'private': ''}}, False),
        ),
        ids=lambda v: repr(v),
    )
    def test_private(self, metainfo, exp_private):
        t = Torrent(metainfo=metainfo)
        assert t.private is exp_private


class Test__handle_kwarg_private:

    @pytest.fixture(autouse=True)
    def mock__set_metainfo(self, mocker):
        mocker.patch.object(Torrent, '_set_metainfo', Mock())

    @pytest.mark.parametrize(
        argnames='private, exp_value',
        argvalues=(
            pytest.param(True, 1),
            pytest.param(False, None),
            pytest.param(1, 1),
            pytest.param(0, None),
            pytest.param('yes', 1),
            pytest.param('', None),
        ),
        ids=lambda v: repr(v),
    )
    def test_private(self, private, exp_value):
        t = Torrent()
        return_value = t._handle_kwarg_private(private)
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('info', 'private',), exp_value)]


class Test_size:

    def test_size_of_singlefile_torrent(self, mocker):
        t = Torrent()
        mocker.patch.object(t, '_get_metainfo', Mock(
            side_effect=(
                123,
            ),
        ))
        assert t.size == 123
        assert t._get_metainfo.call_args_list == [
            call('info', 'length', type=int, default=None),
        ]

    def test_size_of_multifile_torrent(self, mocker):
        t = Torrent()
        mocker.patch.object(t, '_get_metainfo', Mock(
            side_effect=(
                None,
                (
                    {'length': 1, 'path': ['whatever']},
                    'this is not a file object',
                    {'length': 2, 'path': ['whatever']},
                    {'path': ['whatever']},
                    {'length': 8, 'path': ['whatever']},
                ),
            ),
        ))
        assert t.size == 1 + 2 + 8
        assert t._get_metainfo.call_args_list == [
            call('info', 'length', type=int, default=None),
            call('info', 'files', type=tuple, default=None),
        ]

    def test_size_is_unknown(self, mocker):
        t = Torrent()
        mocker.patch.object(t, '_get_metainfo', Mock(
            side_effect=(
                None,
                None,
            ),
        ))
        assert t.size is None
        assert t._get_metainfo.call_args_list == [
            call('info', 'length', type=int, default=None),
            call('info', 'files', type=tuple, default=None),
        ]


class Test_size_partial:

    def test_path_is_unexpected_type(self):
        t = Torrent()
        with pytest.raises(TypeError, match=r"^Unexpected path type: int: 123$"):
            t.size_partial(123)

    @pytest.mark.parametrize('path', (
        '',
        [],
        (),
        iter(()),
    ), ids=lambda v: repr(v))
    def test_path_is_empty(self, path):
        t = Torrent()
        exp_msg = f'path must not be empty: {path}'
        with pytest.raises(ValueError, match=rf'^{re.escape(exp_msg)}$'):
            t.size_partial(path)

    @pytest.mark.parametrize('metainfo', (
        {},
        {'info': {}},
        {'info': {'name': 'My Torrent'}},
        {'info': {'length': 123}},
        {'info': {'files': [{'length': 123}, {'length': 456}]}},
        {'info': {'files': [{'path': ['a']}, {'path': ['b']}]}},
    ), ids=lambda v: repr(v))
    def test_missing_metainfo(self, metainfo, mocker):
        mocker.patch.object(Torrent, 'metainfo', PropertyMock(return_value=metainfo))
        t = Torrent()
        with pytest.raises(ValueError, match=r'^No such path: My Torrent$'):
            t.size_partial('My Torrent')

    @pytest.fixture()
    def metainfo_singlefile(self, mocker):
        mocker.patch.object(Torrent, 'metainfo', PropertyMock(return_value={
            'info': {
                'length': 123,
                'name': 'My Torrent',
            }
        }))

    @pytest.mark.parametrize('path', (
        'My Torrent',
        pathlib.PurePath('My Torrent'),
        ['My Torrent'],
        ('My Torrent',),
        iter(('My Torrent',)),
    ), ids=lambda v: repr(v))
    def test_singlefile(self, path, metainfo_singlefile):
        t = Torrent()
        return_value = t.size_partial(path)
        assert return_value == 123

    @pytest.fixture()
    def metainfo_multifile(self, mocker):
        mocker.patch.object(Torrent, 'metainfo', PropertyMock(return_value={
            'info': {
                'name': 'My Torrent',
                'files': [
                    {'length': 1, 'path': ['a']},
                    {'length': 2, 'path': ['b']},
                    {'length': 3, 'path': ['baz', 'g']},
                    {'length': 4, 'path': ['baz', 'h']},
                    {'length': 5, 'path': ['deeply', 'nested', 'subtree', 'x']},
                    {'length': 6, 'path': ['foo', 'bar', 'e']},
                    {'length': 70, 'path': ['foo', 'bar', 'f']},
                    {'length': 80, 'path': ['foo', 'c']},
                    {'length': 90, 'path': ['foo', 'd']}
                ],
            }
        }))

    @pytest.mark.parametrize('path, exp_size', (
        # Level 1
        (('My Torrent',), sum((1, 2, 3, 4, 5, 6, 70, 80, 90))),
        ((pathlib.PurePath('My Torrent'), sum((1, 2, 3, 4, 5, 6, 70, 80, 90)))),
        ((['My Torrent'], sum((1, 2, 3, 4, 5, 6, 70, 80, 90)))),
        ((('My Torrent',), sum((1, 2, 3, 4, 5, 6, 70, 80, 90)))),
        ((iter(('My Torrent',)), sum((1, 2, 3, 4, 5, 6, 70, 80, 90)))),
        # Level 2
        (os.path.join('My Torrent', 'a'), 1),
        (pathlib.PurePath('My Torrent', 'b'), 2),
        (['My Torrent', 'baz'], 3 + 4),
        (('My Torrent', 'deeply'), 5),
        (iter(('My Torrent', 'foo')), 6 + 70 + 80 + 90),
        # Level 3
        (os.path.join('My Torrent', 'baz', 'g'), 3),
        (pathlib.PurePath('My Torrent', 'baz', 'h'), 4),
        (['My Torrent', 'deeply', 'nested'], 5),
        (('My Torrent', 'foo', 'bar'), 6 + 70),
        (iter(('My Torrent', 'foo', 'c')), 80),
        (('My Torrent', 'foo', 'd'), 90),
        # Level 4
        (os.path.join('My Torrent', 'deeply', 'nested', 'subtree'), 5),
        # Level 5
        (('My Torrent', 'deeply', 'nested', 'subtree', 'x'), 5),
    ), ids=lambda v: repr(v))
    def test_multifile_size(self, path, exp_size, metainfo_multifile):
        t = Torrent()
        return_value = t.size_partial(path)
        assert return_value == exp_size


class Test_source:

    @pytest.fixture(autouse=True)
    def mock__get_metainfo(self, mocker):
        mocker.patch.object(Torrent, '_get_metainfo', Mock())

    def test_source(self):
        t = Torrent()
        assert t.source == t._get_metainfo.return_value
        assert t._get_metainfo.call_args_list == [
            call('info', 'source', type=str, default=None),
        ]


class Test__handle_kwarg_source:

    @pytest.fixture(autouse=True)
    def mock__set_metainfo(self, mocker):
        mocker.patch.object(Torrent, '_set_metainfo', Mock())

    def test_source_is_None(self):
        t = Torrent()
        return_value = t._handle_kwarg_source(None)
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('info', 'source',), None)]

    def test_source_is_string(self):
        t = Torrent()
        return_value = t._handle_kwarg_source('my new source')
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('info', 'source',), 'my new source')]

    def test_source_is_empty_string(self):
        t = Torrent()
        return_value = t._handle_kwarg_source('')
        assert return_value is None
        assert t._set_metainfo.call_args_list == [call(('info', 'source',), None)]

    def test_source_is_garbage(self):
        t = Torrent()
        with pytest.raises(TypeError, match=r"^Unexpected source type: int: 123$"):
            t._handle_kwarg_source(123)
        assert t._set_metainfo.call_args_list == []
