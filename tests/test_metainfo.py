import re

import pytest

from torf import _errors, _metainfo


def test_CodecMapping_keys_encoding():
    cdct = _metainfo.CodecMapping(raw={}, keys_encoding='utf8')
    assert cdct.keys_encoding == 'utf8'

    cdct.keys_encoding = 'iso8859-1'
    assert cdct.keys_encoding == 'iso8859-1'

    exp_exception = _errors.CodecError('Unknown encoding', value='no such encoding')
    with pytest.raises(type(exp_exception), match=rf'^{re.escape(str(exp_exception))}$'):
        cdct.keys_encoding = 'no such encoding'


def test_CodecMapping_values_encoding():
    cdct = _metainfo.CodecMapping(raw={}, values_encoding='utf8')
    assert cdct.values_encoding == 'utf8'

    cdct.values_encoding = 'iso8859-1'
    assert cdct.values_encoding == 'iso8859-1'

    exp_exception = _errors.CodecError('Unknown encoding', value='no such encoding')
    with pytest.raises(type(exp_exception), match=rf'^{re.escape(str(exp_exception))}$'):
        cdct.values_encoding = 'no such encoding'


def test_CodecMapping_raise_on_decoding_error():
    cdct = _metainfo.CodecMapping(())
    assert cdct.raise_on_decoding_error is False
    assert cdct._error_handling == 'replace'

    cdct.raise_on_decoding_error = 1
    assert cdct.raise_on_decoding_error is True
    assert cdct._error_handling == 'strict'

    cdct.raise_on_decoding_error = ''
    assert cdct.raise_on_decoding_error is False
    assert cdct._error_handling == 'replace'


def test_CodecMapping_key_does_not_exist():
    cdct = _metainfo.CodecMapping(raw={b'foo': b'bar'})
    with pytest.raises(KeyError, match=r"^'asdf'$"):
        cdct['asdf']

def test_CodecMapping_key_is_str():
    keys_encoding = 'iso8859-1'
    cdct = _metainfo.CodecMapping(
        raw={'cömment'.encode(keys_encoding): b'my comment'},
        keys_encoding=keys_encoding,
    )
    assert cdct['cömment'] == 'my comment'
    cdct['cömment'] = 'your comment'
    assert cdct['cömment'] == 'your comment'

def test_CodecMapping_key_is_bytes():
    keys_encoding = 'iso8859-1'
    cdct = _metainfo.CodecMapping(
        raw={'cömment'.encode(keys_encoding): b'my comment'},
        keys_encoding=keys_encoding,
    )
    assert cdct['cömment'.encode(keys_encoding)] == 'my comment'
    cdct['cömment'.encode(keys_encoding)] = 'your comment'
    assert cdct['cömment'.encode(keys_encoding)] == 'your comment'

def test_CodecMapping_key_is_unsupported_type():
    keys_encoding = 'iso8859-1'
    cdct = _metainfo.CodecMapping({}, keys_encoding=keys_encoding)
    with pytest.raises(TypeError, match=r'^Unsupported key type: float: 12\.3$'):
        cdct[12.3]

@pytest.mark.parametrize('raise_on_decoding_error', (
    pytest.param(False, id='raise_on_decoding_error=False'),
    pytest.param(True, id='raise_on_decoding_error=True'),
))
def test_CodecMapping_key_is_unencodable(raise_on_decoding_error):
    keys_encoding = 'ascii'
    cdct = _metainfo.CodecMapping(
        raw={},
        keys_encoding=keys_encoding,
        raise_on_decoding_error=raise_on_decoding_error,
    )
    exp_msg = (
        "Invalid metainfo: cömment: "
        r"'ascii' codec can't encode character '\xf6' in position 1: "
        "ordinal not in range(128)"
    )
    with pytest.raises(_errors.CodecError, match=rf'^{re.escape(str(exp_msg))}$'):
        cdct['cömment']

@pytest.mark.parametrize('raise_on_decoding_error', (
    pytest.param(False, id='raise_on_decoding_error=False'),
    pytest.param(True, id='raise_on_decoding_error=True'),
))
def test_CodecMapping_key_is_undecodable(raise_on_decoding_error):
    keys_encoding = 'ascii'
    cdct = _metainfo.CodecMapping(
        raw={'cömment'.encode('iso8859-1'): b'my comment'},
        keys_encoding=keys_encoding,
        raise_on_decoding_error=raise_on_decoding_error,
    )
    exp_msg = (
        r"Invalid metainfo: b'c\xf6mment': "
        "'ascii' codec can't decode byte 0xf6 in position 1: "
        "ordinal not in range(128)"
    )
    with pytest.raises(_errors.CodecError, match=rf'^{re.escape(str(exp_msg))}$'):
        tuple(cdct)

@pytest.mark.parametrize('raise_on_decoding_error', (
    pytest.param(False, id='raise_on_decoding_error=False'),
    pytest.param(True, id='raise_on_decoding_error=True'),
))
def test_CodecMapping_key_in_raw_is_unsupported_type(raise_on_decoding_error):
    cdct = _metainfo.CodecMapping(
        raw={12.3: b'my comment'},
        raise_on_decoding_error=raise_on_decoding_error,
    )
    with pytest.raises(TypeError, match=r'^Unsupported key type: float: 12\.3$'):
        tuple(cdct)


def test_CodecMapping_value_is_set_to_bytes():
    values_encoding = 'iso8859-1'
    cdct = _metainfo.CodecMapping(raw={}, values_encoding=values_encoding)
    cdct['foo'] = 'bär'.encode(values_encoding)
    assert cdct['foo'] == 'bär'
    assert cdct._raw[b'foo'] == 'bär'.encode(values_encoding)

def test_CodecMapping_value_is_set_to_bytearray():
    values_encoding = 'iso8859-1'
    cdct = _metainfo.CodecMapping(raw={}, values_encoding=values_encoding)
    cdct['foo'] = bytearray('bär'.encode(values_encoding))
    assert cdct['foo'] == 'bär'
    assert cdct._raw[b'foo'] == 'bär'.encode(values_encoding)
    assert isinstance(cdct._raw[b'foo'], bytearray), repr(cdct._raw[b'foo'])

def test_CodecMapping_value_is_set_to_int():
    values_encoding = 'iso8859-1'
    cdct = _metainfo.CodecMapping(raw={}, values_encoding=values_encoding)
    cdct['foo'] = 123
    assert cdct['foo'] == 123
    assert cdct._raw[b'foo'] == 123

def test_CodecMapping_value_is_set_to_str():
    values_encoding = 'iso8859-1'
    cdct = _metainfo.CodecMapping(raw={}, values_encoding=values_encoding)
    cdct['foo'] = 'bär'
    assert cdct['foo'] == 'bär'
    assert cdct._raw[b'foo'] == 'bär'.encode(values_encoding)

@pytest.mark.parametrize(
    argnames='raise_on_decoding_error, exp_exception',
    argvalues=(
        pytest.param(False, None, id='raise_on_decoding_error=False'),
        pytest.param(
            True,
            _errors.CodecError(
                "'ascii' codec can't decode byte 0xe4 in position 1: ordinal not in range(128)",
                value='bär'.encode('iso8859-1'),
                keypath=('foo',),
            ),
            id='raise_on_decoding_error=True',
        ),
    ),
)
def test_CodecMapping_value_in_raw_is_undecodable(raise_on_decoding_error, exp_exception):
    values_encoding = 'ascii'
    cdct = _metainfo.CodecMapping(
        raw={b'foo': 'bär'.encode('iso8859-1')},
        values_encoding=values_encoding,
        raise_on_decoding_error=raise_on_decoding_error,
    )
    if exp_exception:
        with pytest.raises(type(exp_exception), match=rf'^{re.escape(str(exp_exception))}$'):
            cdct['foo']
    else:
        assert cdct['foo'] == 'bär'.encode('iso8859-1').decode('ascii', errors='replace')


@pytest.mark.parametrize('raise_on_decoding_error', (
    pytest.param(False, id='raise_on_decoding_error=False'),
    pytest.param(True, id='raise_on_decoding_error=True'),
))
def test_CodecMapping_value_is_set_to_unencodable_string(raise_on_decoding_error):
    values_encoding = 'ascii'
    cdct = _metainfo.CodecMapping(
        raw={},
        values_encoding=values_encoding,
        raise_on_decoding_error=raise_on_decoding_error,
    )
    exp_msg = (
        "Invalid metainfo: foo: bär: "
        r"'ascii' codec can't encode character '\xe4' in position 1: "
        "ordinal not in range(128)"
    )
    with pytest.raises(_errors.CodecError, match=rf'^{re.escape(str(exp_msg))}$'):
        cdct['foo'] = 'bär'
    assert 'foo' not in cdct
    assert b'foo' not in cdct._raw

def test_CodecMapping_value_is_set_to_mapping():
    keys_encoding = 'cp850'
    values_encoding = 'ascii'
    cdct = _metainfo.CodecMapping(raw={}, values_encoding=values_encoding, keys_encoding=keys_encoding)
    cdct['foo'] = {'this': 'that', 'foo': 'bar'}
    assert cdct['foo'] == {'this': 'that', 'foo': 'bar'}
    assert isinstance(cdct['foo'], _metainfo.CodecMapping), repr(cdct['foo'])
    assert cdct['foo']._raw == {b'this': b'that', b'foo': b'bar'}
    assert cdct['foo'].keys_encoding == keys_encoding
    assert cdct['foo'].values_encoding == values_encoding

def test_CodecMapping_value_is_set_to_iterable():
    keys_encoding = 'cp850'
    values_encoding = 'ascii'
    cdct = _metainfo.CodecMapping(raw={}, values_encoding=values_encoding, keys_encoding=keys_encoding)

    def my_iterable():
        yield 'this'
        yield 'that'

    cdct['foo'] = my_iterable()
    assert cdct['foo'] == ['this', 'that']
    assert isinstance(cdct['foo'], _metainfo.CodecSequence), repr(cdct['foo'])
    assert cdct['foo']._raw == [b'this', b'that']
    assert cdct['foo'].keys_encoding == keys_encoding
    assert cdct['foo'].values_encoding == values_encoding

def test_CodecMapping_value_in_raw_is_unsupported_type():
    cdct = _metainfo.CodecMapping(raw={b'foo': 12.3})
    with pytest.raises(TypeError, match=r'^Unsupported value type: float: 12\.3$'):
        cdct['foo']
    assert cdct._raw[b'foo'] == 12.3

def test_CodecMapping_value_is_set_to_unsupported_type():
    cdct = _metainfo.CodecMapping(raw={})
    with pytest.raises(TypeError, match=r'^Unsupported value type: float: 12\.3$'):
        cdct['foo'] = 12.3
    assert b'foo' not in cdct._raw


def test_CodecMapping_key_is_removed():
    cdct = _metainfo.CodecMapping(
        raw={
            'föö'.encode('utf8'): b'whatever',
            'bär'.encode('utf8'): b'whatever',
            'bäz'.encode('utf8'): b'whatever',
        },
    )
    del cdct['föö']
    assert tuple(cdct) == ('bär', 'bäz')
    assert tuple(cdct._raw) == ('bär'.encode('utf8'), 'bäz'.encode('utf8'))


def test_CodecMapping_key_with_utf8_suffix_is_preferred():
    values_encoding = 'ascii'
    cdct = _metainfo.CodecMapping(
        raw={
            'föö'.encode('utf8'): 'ä'.encode('iso8859-1'),
            'föö.utf-8'.encode('utf8'): '¡Ä!'.encode('utf8'),
        },
        values_encoding=values_encoding,
    )
    assert cdct['föö'] == '¡Ä!'

def test_CodecMapping_key_with_utf8_suffix_is_set():
    values_encoding = 'cp850'
    cdct = _metainfo.CodecMapping(
        raw={
            'föö'.encode('utf8'): b'whatever',
            'föö.utf-8'.encode('utf8'): b'whatever',
        },
        values_encoding=values_encoding,
    )
    cdct['föö'] = 'bär'
    assert cdct['föö'] == 'bär'
    assert cdct._raw == {
        'föö'.encode('utf8'): 'bär'.encode('cp850'),
        'föö.utf-8'.encode('utf8'): 'bär'.encode('utf8'),
    }

@pytest.mark.parametrize(
    argnames='raise_on_decoding_error, exp_exception',
    argvalues=(
        pytest.param(False, None, id='raise_on_decoding_error=False'),
        pytest.param(
            True,
            _errors.CodecError(
                "'utf-8' codec can't decode byte 0xbe in position 0: invalid start byte",
                value='þ bad'.encode('utf8')[1:],
                keypath=('foo',),
            ),
            id='raise_on_decoding_error=True',
        ),
    ),
)
def test_CodecMapping_key_with_utf8_suffix_has_badly_encoded_value(raise_on_decoding_error, exp_exception):
    cdct = _metainfo.CodecMapping(
        raw={
            'foo'.encode('utf8'): b'whatever',
            'foo.utf-8'.encode('utf8'): 'þ bad'.encode('utf8')[1:],
            'bar'.encode('utf8'): b'something',
        },
        raise_on_decoding_error=raise_on_decoding_error,
    )
    if exp_exception:
        with pytest.raises(type(exp_exception), match=rf'^{re.escape(str(exp_exception))}$'):
            cdct['foo']
    else:
            assert cdct['foo'] == '� bad'

def test_CodecMapping_key_with_utf8_suffix_is_removed():
    cdct = _metainfo.CodecMapping(
        raw={
            'föö'.encode('utf8'): b'whatever',
            'föö.utf-8'.encode('utf8'): b'whatever',
        },
    )
    del cdct['föö']
    assert 'föö' not in cdct
    assert 'föö'.encode('utf8') not in cdct._raw
    assert 'föö.utf-8'.encode('utf8') not in cdct._raw

def test_CodecMapping_key_with_utf8_suffix_is_not_added_if_it_does_not_exist():
    values_encoding = 'cp850'
    cdct = _metainfo.CodecMapping(raw={'foo'.encode('utf8'): b'whatever'}, values_encoding=values_encoding)
    cdct['foo'] = 'bär'
    assert 'foo'.encode('utf8') in cdct._raw
    assert 'foo.utf-8'.encode('utf8') not in cdct._raw

def test_CodecMapping_key_with_utf8_suffix_is_not_iterated():
    cdct = _metainfo.CodecMapping(
        raw={
            'foo'.encode('utf8'): b'whatever',
            'foo.utf-8'.encode('utf8'): b'whatever',
            'bar'.encode('utf8'): b'something',
        },
    )
    assert tuple(cdct) == ('foo', 'bar')

def test_CodecMapping_key_with_utf8_suffix_maps_to_nested_sequence():
    keys_encoding = 'cp850'
    values_encoding = 'iso8859-1'
    cdct = _metainfo.CodecMapping(
        raw = {
            b'info': {
                b'files': [
                    {
                        b'length': 123,
                        b'path': [b'path', b'to', b'file'],
                        b'path.utf-8': [b'path', b'to', b'file.utf8'],
                    },
                    {
                        b'length': 456,
                        b'path': [b'path', b'to', b'other', b'file'],
                    },
                ],
            },
        },
        keys_encoding=keys_encoding,
        values_encoding=values_encoding,
    )
    assert cdct['info']['files'][0]['path'] == ['path', 'to', 'file.utf8']
    assert cdct['info']['files'][1]['path'] == ['path', 'to', 'other', 'file']


def test_CodecSequence_raise_on_decoding_error():
    cseq = _metainfo.CodecSequence(())
    assert cseq.raise_on_decoding_error is False
    assert cseq._error_handling == 'replace'

    cseq.raise_on_decoding_error = 1
    assert cseq.raise_on_decoding_error is True
    assert cseq._error_handling == 'strict'

    cseq.raise_on_decoding_error = ''
    assert cseq.raise_on_decoding_error is False
    assert cseq._error_handling == 'replace'


def test_CodecSequence_keys():
    cseq = _metainfo.CodecSequence(raw=[b'foo', b'bar', b'baz'])
    assert cseq[0] == 'foo'
    assert cseq[1] == 'bar'
    assert cseq[2] == 'baz'
    with pytest.raises(IndexError, match='^list index out of range$'):
        cseq[3]


def test_CodecSequence_value_is_decoded_when_accessed():
    values_encoding = 'iso8859-1'
    cseq = _metainfo.CodecSequence(
        raw=[
            'föö'.encode(values_encoding),
            'bär'.encode(values_encoding),
            'bäz'.encode(values_encoding),
        ],
        values_encoding=values_encoding,
    )
    assert cseq[0] == 'föö'
    assert cseq[1] == 'bär'
    assert cseq[2] == 'bäz'

@pytest.mark.parametrize(
    argnames='raise_on_decoding_error, exp_exception',
    argvalues=(
        pytest.param(False, None, id='raise_on_decoding_error=False'),
        pytest.param(
            True,
            _errors.CodecError(
                "'utf-8' codec can't decode byte 0xbe in position 0: invalid start byte",
                value='þ bär'.encode('utf8')[1:],
                keypath=(1,),
            ),
            id='raise_on_decoding_error=True',
        ),
    ),
)
def test_CodecSequence_value_in_raw_is_badly_encoded(raise_on_decoding_error, exp_exception):
    cseq = _metainfo.CodecSequence(
        raw=[
            'föö'.encode('utf8'),
            'þ bär'.encode('utf8')[1:],
            'bäz'.encode('utf8'),
        ],
        raise_on_decoding_error=raise_on_decoding_error,
    )
    assert cseq[0] == 'föö'
    if exp_exception:
        with pytest.raises(type(exp_exception), match=rf'^{re.escape(str(exp_exception))}$'):
            cseq[1]
    else:
        assert cseq[1] == '� bär'
    assert cseq[2] == 'bäz'

def test_CodecSequence_value_is_set_to_bytes():
    cseq = _metainfo.CodecSequence([b'foo', b'bar', b'baz'])
    cseq[2] = 'þoo'.encode('cp850')
    assert cseq._raw[2] == 'þoo'.encode('cp850')
    assert isinstance(cseq._raw[2], bytes)

def test_CodecSequence_value_is_set_to_bytearray():
    cseq = _metainfo.CodecSequence([b'foo', b'bar', b'baz'])
    cseq[2] = bytearray('þoo'.encode('cp850'))
    assert cseq._raw[2] == 'þoo'.encode('cp850')
    assert isinstance(cseq._raw[2], bytearray)

def test_CodecSequence_value_is_set_to_int():
    cseq = _metainfo.CodecSequence([b'foo', b'bar', b'baz'])
    cseq[1] = 123
    assert cseq._raw[1] == 123
    assert isinstance(cseq._raw[1], int)

def test_CodecSequence_value_is_set_to_str():
    values_encoding = 'iso8859-1'
    cseq = _metainfo.CodecSequence(
        raw=[b'foo', b'bar', b'baz'],
        values_encoding=values_encoding,
    )
    cseq[0] = 'þoo'
    assert cseq[0] == 'þoo'
    assert cseq._raw[0] == 'þoo'.encode(values_encoding)
    cseq[1] = 'þar'
    assert cseq[1] == 'þar'
    assert cseq._raw[1] == 'þar'.encode(values_encoding)
    cseq[2] = 'þaz'
    assert cseq[2] == 'þaz'
    assert cseq._raw[2] == 'þaz'.encode(values_encoding)
    assert cseq == ['þoo', 'þar', 'þaz']
    assert cseq._raw == ['þoo'.encode(values_encoding), 'þar'.encode(values_encoding), 'þaz'.encode(values_encoding)]

def test_CodecSequence_value_is_encoded_when_inserted():
    values_encoding = 'iso8859-1'
    cseq = _metainfo.CodecSequence(
        raw=[b'foo', b'bar', b'baz'],
        values_encoding=values_encoding,
    )
    cseq.insert(1, 'þoo')
    assert cseq == ['foo', 'þoo', 'bar', 'baz']
    assert cseq._raw == [
        'foo'.encode(values_encoding),
        'þoo'.encode(values_encoding),
        'bar'.encode(values_encoding),
        'baz'.encode(values_encoding),
    ]

@pytest.mark.parametrize('raise_on_decoding_error', (
    pytest.param(False, id='raise_on_decoding_error=False'),
    pytest.param(True, id='raise_on_decoding_error=True'),
))
def test_CodecSequence_value_is_set_to_unencodable_string(raise_on_decoding_error):
    cseq = _metainfo.CodecSequence(
        raw=[b'foo', b'bar', b'baz'],
        values_encoding='ascii',
        raise_on_decoding_error=raise_on_decoding_error,
    )
    exp_msg = (
        "Invalid metainfo: 0: þoo: "
        r"'ascii' codec can't encode character '\xfe' in position 0: "
        "ordinal not in range(128)"
    )
    with pytest.raises(_errors.CodecError, match=rf'^{re.escape(str(exp_msg))}$'):
        cseq[0] = 'þoo'
    assert cseq == ['foo', 'bar', 'baz']
    assert cseq._raw == [b'foo', b'bar', b'baz']

def test_CodecSequence_value_is_set_to_unsupported_type():
    cseq = _metainfo.CodecSequence([b'foo', b'bar', b'baz'])
    with pytest.raises(TypeError, match=r'^Unsupported value type: float: 12\.3$'):
        cseq[0] = 12.3
    assert cseq == ['foo', 'bar', 'baz']
    assert cseq._raw == [b'foo', b'bar', b'baz']

def test_CodecSequence_value_in_raw_is_unsupported_type():
    cseq = _metainfo.CodecSequence([b'foo', 12.3, b'baz'])
    assert cseq[0] == 'foo'
    with pytest.raises(TypeError, match=r'^Unsupported value type: float: 12\.3$'):
        cseq[1]
    assert cseq[2] == 'baz'
    with pytest.raises(TypeError, match=r'^Unsupported value type: float: 12\.3$'):
        tuple(cseq)
    assert cseq._raw == [b'foo', 12.3, b'baz']

@pytest.mark.parametrize(
    argnames='raise_on_decoding_error, exp_exception',
    argvalues=(
        pytest.param(False, None, id='raise_on_decoding_error=False'),
        pytest.param(
            True,
            _errors.CodecError(
                "'ascii' codec can't decode byte 0xe4 in position 1: ordinal not in range(128)",
                value='bär'.encode('iso8859-1'),
                keypath=(1,),
            ),
            id='raise_on_decoding_error=True',
        ),
    ),
)
def test_CodecSequence_value_in_raw_is_undecodable(raise_on_decoding_error, exp_exception):
    cseq = _metainfo.CodecSequence(
        raw=[b'foo', 'bär'.encode('iso8859-1'), b'baz'],
        values_encoding='ascii',
        raise_on_decoding_error=raise_on_decoding_error,
    )
    if exp_exception:
        with pytest.raises(type(exp_exception), match=rf'^{re.escape(str(exp_exception))}$'):
            cseq[1]
    else:
        assert cseq[1] == 'bär'.encode('iso8859-1').decode('ascii', errors='replace')

def test_CodecSequence_value_is_set_to_mapping():
    keys_encoding = 'cp850'
    values_encoding = 'ascii'
    cdct = _metainfo.CodecSequence(
        raw=[b'foo', b'bar', b'baz'],
        keys_encoding=keys_encoding,
        values_encoding=values_encoding,
    )
    cdct[1] = {'this': 'that', 'foo': 'bar'}
    assert cdct[1] == {'this': 'that', 'foo': 'bar'}
    assert isinstance(cdct[1], _metainfo.CodecMapping), repr(cdct[1])
    assert cdct[1]._raw == {b'this': b'that', b'foo': b'bar'}
    assert cdct[1].keys_encoding == keys_encoding
    assert cdct[1].values_encoding == values_encoding

def test_CodecSequence_value_is_set_to_iterable():
    keys_encoding = 'cp850'
    values_encoding = 'ascii'
    cdct = _metainfo.CodecSequence(
        raw=[b'foo', b'bar', b'baz'],
        keys_encoding=keys_encoding,
        values_encoding=values_encoding,
    )
    cdct[1] = ('this', 'that')
    assert cdct[1] == ['this', 'that']
    assert isinstance(cdct[1], _metainfo.CodecSequence), repr(cdct[1])
    assert cdct[1]._raw == [b'this', b'that']
    assert cdct[1].keys_encoding == keys_encoding
    assert cdct[1].values_encoding == values_encoding


def test_CodecSequence_value_is_removed():
    cdct = _metainfo.CodecSequence([b'foo', b'bar', b'baz'])
    del cdct[1]
    assert cdct == ['foo', 'baz']
    assert cdct._raw == [b'foo', b'baz']


def test_keypath():
    cdct = _metainfo.CodecMapping(
        raw = {
            b'info': {
                b'files': [
                    {
                        b'length': 123,
                        b'path': [b'path', b'to', b'file'],
                        b'path.utf-8': [b'path', b'to', b'file.utf8'],
                    },
                    {
                        b'length': 456,
                        b'path': [b'path', b'to', b'other', b'file'],
                    },
                ],
            },
        },
    )

    assert cdct._keypath == ()
    assert cdct['info']._keypath == ('info',)
    assert cdct['info']['files']._keypath == ('info', 'files')
    assert cdct['info']['files'][0]._keypath == ('info', 'files', 0)
    assert cdct['info']['files'][0]['path']._keypath == ('info', 'files', 0, 'path')
    assert cdct['info']['files'][1]._keypath == ('info', 'files', 1)
    assert cdct['info']['files'][1]['path']._keypath == ('info', 'files', 1, 'path')


def test___repr__():
    keys_encoding = 'cp850'
    values_encoding = 'iso8859-1'
    raw = {
        'cömment'.encode(keys_encoding): 'my cömment'.encode(values_encoding),
        'comment.utf-8'.encode(keys_encoding): 'comment in utf8'.encode('utf8'),
        'info'.encode(keys_encoding): {
            'name'.encode(keys_encoding): 'my näme'.encode(values_encoding),
            'piece length'.encode(keys_encoding): 16384,
            'files'.encode(keys_encoding): [
                {
                    'length'.encode(keys_encoding): 123,
                    'path'.encode(keys_encoding): ['föö'.encode(values_encoding), 'bär'.encode(values_encoding), 'bäz'.encode(values_encoding)]
                },
                {
                    'length'.encode(keys_encoding): 456,
                    'path'.encode(keys_encoding): [b'this', 'thät'.encode(values_encoding)]
                },
            ],
        },
    }
    cdct = _metainfo.CodecMapping(raw=raw, keys_encoding=keys_encoding, values_encoding=values_encoding)
    assert repr(cdct) == repr({
        'cömment': 'my cömment',
        'info': {
            'name': 'my näme',
            'piece length': 16384,
            'files': [
                {
                    'length': 123,
                    'path': ['föö', 'bär', 'bäz']
                },
                {
                    'length': 456,
                    'path': ['this', 'thät']
                },
            ],
        },
    })


def test_CodecMapping_no_encoding_keypaths():
    cdct = _metainfo.CodecMapping(
        raw={
            b'comment': b'This is a comment.',
            b'info': {
                b'pieces': b'\x01\x02\x03',
                b'files': [
                    {b'length': 123, b'path': [b'a', b'b', b'c', b'd']},
                    {b'length': 234, b'path': [b'e', b'f', b'g', b'h']},
                    {b'length': 345, b'path': [b'i', b'j', b'k', b'l']},
                    {b'length': 456, b'path': [b'm', b'n', b'o', b'p']},
                ],
            },
            b'foo': {
                b'something': b'hello',
                b'bar': {
                    b'sumthing': b'hola',
                    b'baz': b'some raw bytes',
                },
            },
        },
        no_encoding_keypaths=(
            ('info', 'pieces'),
            ('info', 'files', 1),
            ('info', 'files', 2, 'path'),
            ('info', 'files', 3, 'path', 2),
            ('foo', 'bar', 'baz'),
        ),
    )

    # Check if no_encoding_paths is inherited correctly.
    assert cdct._no_encoding_keypaths == (
        ('info', 'pieces'),
        ('info', 'files', 1),
        ('info', 'files', 2, 'path'),
        ('info', 'files', 3, 'path', 2),
        ('foo', 'bar', 'baz'),
    )
    assert cdct['info']._no_encoding_keypaths == (
        ('info', 'pieces'),
        ('info', 'files', 1),
        ('info', 'files', 2, 'path'),
        ('info', 'files', 3, 'path', 2),
    )
    assert cdct['info']['files']._no_encoding_keypaths == (
        ('info', 'files', 1),
        ('info', 'files', 2, 'path'),
        ('info', 'files', 3, 'path', 2),
    )
    assert cdct['info']['files'][0]._no_encoding_keypaths == ()
    assert cdct['info']['files'][1]._no_encoding_keypaths == (('info', 'files', 1),)
    assert cdct['info']['files'][2]._no_encoding_keypaths == (('info', 'files', 2, 'path'),)
    assert cdct['info']['files'][3]._no_encoding_keypaths == (('info', 'files', 3, 'path', 2),)
    assert cdct['info']['files'][0]['path']._no_encoding_keypaths == ()
    assert cdct['info']['files'][1]['path']._no_encoding_keypaths == (('info', 'files', 1),)
    assert cdct['info']['files'][2]['path']._no_encoding_keypaths == (('info', 'files', 2, 'path'),)
    assert cdct['info']['files'][3]['path']._no_encoding_keypaths == (('info', 'files', 3, 'path', 2),)
    assert cdct['foo']._no_encoding_keypaths == (('foo', 'bar', 'baz'),)
    assert cdct['foo']['bar']._no_encoding_keypaths == (('foo', 'bar', 'baz'),)

    # Check if no_encoding_paths is used correctly when getting and setting values.

    def get(keypath):
        obj = cdct
        for key in keypath:
            obj = obj[key]
        return obj

    def set(keypath, value):
        obj = cdct
        for key in keypath[:-1]:
            obj = obj[key]
        print(f'!!! SET {obj}[{keypath[-1]!r}] = {value!r}')
        obj[keypath[-1]] = value

    def assert_this(keypath, old=None, new=None, exp=None):
        if old is not None:
            assert get(keypath) == old

        if isinstance(exp, Exception):
            with pytest.raises(type(exp), match=rf'^{re.escape(str(exp))}$'):
                set(keypath, new)
            if old is not None:
                assert get(keypath) == old

        elif new is not None:
            set(keypath, new)
            if exp is not None and not isinstance(exp, Exception):
                assert get(keypath) == exp

    assert_this(('comment',), old='This is a comment.', new='My new comment.', exp='My new comment.')
    assert_this(('comment',), new=b'My new comment.', exp='My new comment.')

    assert_this(('info', 'pieces'), old=b'\x01\x02\x03', new=b'\x10\x20\x30', exp=b'\x10\x20\x30')
    assert_this(('info', 'pieces'), new='this is not bytes', exp=_errors.CodecError(
        'Is in no_encoding_keypaths and therefore must be bytes, not str',
        value='this is not bytes',
        keypath=('info', 'pieces'),
    ))

    # # ("info", "files", 0) is encoded.
    assert_this(
        ('info', 'files', 0),
        old={'length': 123, 'path': ['a', 'b', 'c', 'd']},
        new={'length': 321, 'path': ['A', b'B', 'C']},
        exp={'length': 321, 'path': ['A', 'B', 'C']},
    )

    # ("info", "files", 1) is encoded.
    assert_this(
        ('info', 'files', 1),
        old={'length': 234, 'path': [b'e', b'f', b'g', b'h']},
        new={'length': 432, 'path': [b'E', b'F', b'G']},
        exp={'length': 432, 'path': [b'E', b'F', b'G']},
    )
    assert_this(
        ('info', 'files', 1),
        new={'length': 432, 'path': [b'E', b'F', 'G']},
        exp=_errors.CodecError(
            'Is in no_encoding_keypaths and therefore must be bytes, not str',
            value='G',
            keypath=('info', 'files', 1, 'path', 2),
        ),
    )

    # ("info", "files", 2, "path") is encoded.
    assert_this(
        ('info', 'files', 2),
        old={'length': 345, 'path': [b'i', b'j', b'k', b'l']},
        new={'length': 543, 'path': [b'I', b'J', b'K']},
        exp={'length': 543, 'path': [b'I', b'J', b'K']},
    )
    assert_this(
        ('info', 'files', 2),
        new={'length': 543, 'path': [b'I', 'J', b'K']},
        exp=_errors.CodecError(
            'Is in no_encoding_keypaths and therefore must be bytes, not str',
            value='J',
            keypath=('info', 'files', 2, 'path', 1),
        ),
    )

    # ("info", "files", 3, "path", 2) is encoded.
    assert_this(
        ('info', 'files', 3),
        old={'length': 456, 'path': ['m', 'n', b'o', 'p']},
        new={'length': 654, 'path': ['M', 'N', b'O']},
        exp={'length': 654, 'path': ['M', 'N', b'O']},
    )
    assert_this(
        ('info', 'files', 3),
        new={'length': 654, 'path': ['M', 'N', 'O']},
        exp=_errors.CodecError(
            'Is in no_encoding_keypaths and therefore must be bytes, not str',
            value='O',
            keypath=('info', 'files', 3, 'path', 2),
        ),
    )

    assert_this(('info', 'files', 0, 'length'), old=321, new=300, exp=300)
    assert_this(('info', 'files', 1, 'length'), old=432, new=400, exp=400)
    assert_this(('info', 'files', 2, 'length'), old=543, new=500, exp=500)
    assert_this(('info', 'files', 3, 'length'), old=654, new=600, exp=600)

    assert_this(('info', 'files', 0, 'path'), old=['A', 'B', 'C'], new=['A', 'B', 'C', 'D'], exp=['A', 'B', 'C', 'D'])
    assert_this(('info', 'files', 1, 'path'), old=[b'E', b'F', b'G'], new=[b'E', b'F', b'G', b'H'], exp=[b'E', b'F', b'G', b'H'])
    assert_this(('info', 'files', 2, 'path'), old=[b'I', b'J', b'K'], new=[b'I', b'J', b'K', b'L'], exp=[b'I', b'J', b'K', b'L'])
    assert_this(('info', 'files', 3, 'path'), old=['M', 'N', b'O'], new=['M', 'N', b'O', 'P'], exp=['M', 'N', b'O', 'P'])

    assert_this(('info', 'files', 0, 'path', 0), old='A', new='A1', exp='A1')
    assert_this(('info', 'files', 0, 'path', 1), old='B', new=b'B1', exp='B1')
    assert_this(('info', 'files', 0, 'path', 2), old='C', new='C1', exp='C1')
    assert_this(('info', 'files', 0, 'path', 3), old='D', new=b'D1', exp='D1')

    assert_this(('info', 'files', 1, 'path', 0), old=b'E', new=b'E1', exp=b'E1')
    assert_this(('info', 'files', 1, 'path', 1), old=b'F', new=b'F1', exp=b'F1')
    assert_this(('info', 'files', 1, 'path', 2), old=b'G', new=b'G1', exp=b'G1')
    assert_this(('info', 'files', 1, 'path', 3), old=b'H', new=b'H1', exp=b'H1')

    assert_this(('info', 'files', 1, 'path', 0), new='E1', exp=_errors.CodecError(
        'Is in no_encoding_keypaths and therefore must be bytes, not str',
        value='E1',
        keypath=('info', 'files', 1, 'path', 0),
    ))
    assert_this(('info', 'files', 1, 'path', 1), new='F1', exp=_errors.CodecError(
        'Is in no_encoding_keypaths and therefore must be bytes, not str',
        value='F1',
        keypath=('info', 'files', 1, 'path', 1),
    ))
    assert_this(('info', 'files', 1, 'path', 2), new='G1', exp=_errors.CodecError(
        'Is in no_encoding_keypaths and therefore must be bytes, not str',
        value='G1',
        keypath=('info', 'files', 1, 'path', 2),
    ))
    assert_this(('info', 'files', 1, 'path', 3), new='H1', exp=_errors.CodecError(
        'Is in no_encoding_keypaths and therefore must be bytes, not str',
        value='H1',
        keypath=('info', 'files', 1, 'path', 3),
    ))

    assert_this(('info', 'files', 3, 'path', 0), old='M', new='M1', exp='M1')
    assert_this(('info', 'files', 3, 'path', 1), old='N', new='N1', exp='N1')
    assert_this(('info', 'files', 3, 'path', 2), old=b'O', new='O1', exp=_errors.CodecError(
        'Is in no_encoding_keypaths and therefore must be bytes, not str',
        value='O1',
        keypath=('info', 'files', 3, 'path', 2),
    ))
    assert_this(('info', 'files', 3, 'path', 3), old='P', new='P1', exp='P1')

    assert_this(('info', 'files', 3, 'path', 0), new=b'M2', exp='M2')
    assert_this(('info', 'files', 3, 'path', 1), new=b'N2', exp='N2')
    assert_this(('info', 'files', 3, 'path', 2), new=b'O2', exp=b'O2')
    assert_this(('info', 'files', 3, 'path', 3), new=b'P2', exp='P2')

    assert_this(
        ('foo',),
        old={'something': 'hello', 'bar': {'sumthing': 'hola', 'baz': b'some raw bytes'}},
        new={'something': 'else', 'no': ['bytes', 'here']},
        exp={'something': 'else', 'no': ['bytes', 'here']},
    )
    assert_this(('foo', 'bar'), new={'baz': 'not bytes'}, exp=_errors.CodecError(
        'Is in no_encoding_keypaths and therefore must be bytes, not str',
        value='not bytes',
        keypath=('foo', 'bar', 'baz'),
    ))
    assert_this(('foo', 'bar'), new={'baz': b'yes bytes'}, exp={'baz': b'yes bytes'})
    assert_this(('foo', 'bar', 'baz'), new=b'other bytes', exp=b'other bytes')
    assert_this(('foo', 'bar', 'baz'), new='not bytes again', exp=_errors.CodecError(
        'Is in no_encoding_keypaths and therefore must be bytes, not str',
        value='not bytes again',
        keypath=('foo', 'bar', 'baz'),
    ))
