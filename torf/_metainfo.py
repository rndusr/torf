"""
Transparent decoding of nested mappings and sequences that contain :class:`bytes` and
:class:`int`

This module provides the types :class:`~.CodecMapping` and :class:`~.CodecSequence` that wrap around
a :class:`dict` or :class:`list` (respectively). Keys and values are decoded when accessed and
encoded when set.

:class:`~.CodecMapping` and :class:`~.CodecSequence` take the same arguments:

:param raw: A :class:`dict` (for :class:`~.CodecMapping`) or :class:`list` (for
    :class:`~.CodecSequence`) that contains the encoded values

:param keypath: Sequence of parent keys to make error messages more informative

:param str values_encoding: Encoding for values (e.g. "UTF-8") (see :attr:`~.CodecBase.values_encoding)

:param str keys_encoding: Same as `values_encoding`, but for :class:`~.CodecMapping` keys

:param no_encoding_keypaths: Sequence of keypaths that are not encoded/decoded (usually this should
    include ``("info", "pieces")``)

:param bool raise_on_decoding_error: Whether badly encoded values should raise :class:`~.CodecError`
    or the badly encoded characters should be replaced (see
    :attr:`~.CodecBase.raise_on_decoding_error`)

.. warning:: In addition to :class:`KeyError` accessing keys may also raise :class:`~.CodecError`.

.. note:: Any keys with a ``".utf-8"`` suffix in a `raw` mapping take precedence and are always
    decoded as UTF-8 and provided without the ``".utf-8"`` suffix.
"""

import collections

from . import _errors, _utils

_NODEFAULT = object()


class CodecBase:
    """Base class for :class:`CodecMapping` and :class:`CodecSequence`"""

    def __init__(
            self,
            raw,
            *,
            values_encoding='UTF-8',
            keys_encoding='UTF-8',
            no_encoding_keypaths=(),
            raise_on_decoding_error=False,
            keypath=(),
    ):
        self._raw = raw
        self._keypath = tuple(keypath)
        self._values_encoding = values_encoding
        self._keys_encoding = keys_encoding
        self._no_encoding_keypaths = tuple(no_encoding_keypaths)
        self._raise_on_decoding_error = raise_on_decoding_error

    @property
    def values_encoding(self):
        """
        Encoding for values (e.g. "UTF-8")

        :class:`~.CodecError` is raised if this is set to an unknown encoding.
        """
        return self._values_encoding

    @values_encoding.setter
    def values_encoding(self, encoding):
        try:
            ''.encode(encoding)
        except LookupError:
            raise _errors.CodecError('Unknown encoding', value=encoding)
        else:
            self._values_encoding = encoding

    @property
    def keys_encoding(self):
        """Same as :attr:`values_encoding` but for :class:`~.CodecMapping` keys"""
        return self._keys_encoding

    @keys_encoding.setter
    def keys_encoding(self, encoding):
        try:
            ''.encode(encoding)
        except LookupError:
            raise _errors.CodecError('Unknown encoding', value=encoding)
        else:
            self._keys_encoding = encoding

    @property
    def raise_on_decoding_error(self):
        """
        Whether to raise :class:`~.CodecError` if any value cannot be decoded

        If this is ``False`` (the default), replace invalid characters with "�" (Unicode
        "REPLACEMENT CHARACTER").

        .. note::

            When encoding/decoding keys, :class:`~.CodecError` is always raised regardless of this
            property. Otherwise, `KeyError('ab�')` would be raised when accessing up ``"ab¢"`` with
            ``"ascii"`` encoding.

            Encoding values also always raises :class:`~.CodecError` when it fails.
        """
        return self._raise_on_decoding_error

    @raise_on_decoding_error.setter
    def raise_on_decoding_error(self, raise_on_decoding_error):
        self._raise_on_decoding_error = bool(raise_on_decoding_error)

    @property
    def _error_handling(self):
        return 'strict' if self.raise_on_decoding_error else 'replace'

    def _must_be_encoded(self, keypath):
        """Whether `keypath` is raw bytes or must be encoded/decoded"""
        for nekp in self._no_encoding_keypaths:
            if _utils.iterable_startswith(nekp[:len(keypath)], keypath[:len(nekp)]):
                return False
        return True

    def _encode_key(self, key):
        if isinstance(key, bytes):
            return key
        elif isinstance(key, str):
            try:
                return key.encode(self.keys_encoding, errors='strict')
            except UnicodeEncodeError as e:
                raise _errors.CodecError(e, value=key, keypath=self._keypath)
        else:
            raise TypeError(f'Unsupported key type: {type(key).__name__}: {key!r}')

    def _decode_key(self, key):
        if isinstance(key, bytes):
            try:
                return key.decode(self.keys_encoding, errors='strict')
            except UnicodeDecodeError as e:
                raise _errors.CodecError(e, value=key, keypath=self._keypath)
        else:
            raise TypeError(f'Unsupported key type: {type(key).__name__}: {key!r}')

    def _encode_value(self, value, *, key, values_encoding=None):
        if isinstance(value, (int, bytes, bytearray)):
            return value

        if not isinstance(key, tuple):
            key = (key,)
        sub_keypath = self._keypath + key
        values_encoding = self.values_encoding if values_encoding is None else values_encoding

        if isinstance(value, str):
            if self._must_be_encoded(sub_keypath):
                try:
                    return value.encode(values_encoding, errors='strict')
                except UnicodeEncodeError as e:
                    raise _errors.CodecError(e, value=value, keypath=sub_keypath)
            else:
                raise _errors.CodecError(
                    f'Is in no_encoding_keypaths and therefore must be bytes, not {type(value).__name__}',
                    value=value,
                    keypath=sub_keypath,
                )

        if isinstance(value, collections.abc.Mapping):
            return {
                self._encode_key(k): self._encode_value(v, key=key + (k,), values_encoding=values_encoding)
                for k, v in value.items()
            }

        if isinstance(value, collections.abc.Iterable):
            return [
                self._encode_value(v, key=key + (i,), values_encoding=values_encoding)
                for i, v in enumerate(value)
            ]

        raise TypeError(f'Unsupported value type: {type(value).__name__}: {value!r}')

    def _decode_value(self, value_raw, *, key, values_encoding=None):
        if isinstance(value_raw, (int, str)):
            return value_raw

        sub_keypath = self._keypath + (key,)
        values_encoding = self.values_encoding if values_encoding is None else values_encoding

        if isinstance(value_raw, (bytes, bytearray)):
            if self._must_be_encoded(sub_keypath):
                try:
                    return value_raw.decode(values_encoding, errors=self._error_handling)
                except UnicodeDecodeError as e:
                    raise _errors.CodecError(e, value=value_raw, keypath=sub_keypath)
            else:
                return value_raw

        sub_no_encoding_keypaths = (
            nekp
            for nekp in self._no_encoding_keypaths
            if _utils.iterable_startswith(nekp[:len(sub_keypath)], sub_keypath[:len(nekp)])
        )

        if isinstance(value_raw, collections.abc.Mapping):
            return CodecMapping(
                value_raw,
                keypath=sub_keypath,
                keys_encoding=self.keys_encoding,
                values_encoding=values_encoding,
                no_encoding_keypaths=sub_no_encoding_keypaths,
                raise_on_decoding_error=self.raise_on_decoding_error,
            )

        if _utils.is_sequence(value_raw):
            return CodecSequence(
                value_raw,
                keypath=sub_keypath,
                keys_encoding=self.keys_encoding,
                values_encoding=values_encoding,
                no_encoding_keypaths=sub_no_encoding_keypaths,
                raise_on_decoding_error=self.raise_on_decoding_error,
            )

        raise TypeError(f'Unsupported value type: {type(value_raw).__name__}: {value_raw!r}')


class CodecMapping(collections.abc.MutableMapping, CodecBase):
    """Decoding and encoding wrapper around :class:`dict`"""

    def __getitem__(self, key):
        key_raw = self._encode_key(key)
        # Some torrents contain the same key with ".utf-8" appended. It has the same value
        # encoded as UTF-8. The actual key value is usually in an unknown encoding.
        key_raw_utf8 = key_raw + b'.utf-8'
        try:
            value_raw = self._raw[key_raw_utf8]
        except KeyError:
            # Default to the actual key that was requested.
            try:
                value_raw = self._raw[key_raw]
            except KeyError:
                raise KeyError(key) from None
        else:
            key_raw = key_raw_utf8

        values_encoding = 'UTF-8' if key_raw.endswith(b'.utf-8') else self.values_encoding
        return self._decode_value(value_raw, key=key, values_encoding=values_encoding)

    def __setitem__(self, key, value):
        # We have one converter per key in `self._raw`.
        key_raw = self._encode_key(key)
        value_raw = self._encode_value(value, key=key)
        self._raw[key_raw] = value_raw

        # If we have a "<key>.utf-8", we also need to update that.
        key_raw_utf8 = key_raw + b'.utf-8'
        if key_raw_utf8 in self._raw:
            value_raw_utf8 = self._encode_value(value, key=key, values_encoding='UTF-8')
            self._raw[key_raw_utf8] = value_raw_utf8

    def __delitem__(self, key):
        key_raw = self._encode_key(key)
        key_raw_utf8 = key_raw + b'.utf-8'
        # Delete "<key>.utf-8" if it exists.
        self._raw.pop(key_raw_utf8, None)
        # Delete actual key or raise KeyError.
        del self._raw[key_raw]

    def __iter__(self):
        for key_raw in self._raw:
            # Exclude "<key>.utf-8" keys. They are handled transparently and are not exposed.
            # `key_raw` must be `bytes` according to BEP3. Theoretically it could be `int`, but we
            # worry about that when we have to.
            if not isinstance(key_raw, bytes) or not key_raw.endswith(b'.utf-8'):
                yield self._decode_key(key_raw)

    def __len__(self):
        return len(tuple(iter(self)))

    def __repr__(self):
        return repr(dict(self))


class CodecSequence(collections.abc.MutableSequence, CodecBase):
    """Decoding and encoding wrapper around :class:`list`"""

    def __getitem__(self, key):
        value_raw = self._raw[key]
        return self._decode_value(value_raw, key=key)

    def __setitem__(self, key, value):
        value_raw = self._encode_value(value, key=key)
        self._raw[key] = value_raw

    def insert(self, key, value):
        value_raw = self._encode_value(value, key=key)
        self._raw.insert(key, value_raw)

    def __delitem__(self, key):
        del self._raw[key]

    def __iter__(self):
        return iter(
            self._decode_value(v, key=i)
            for i, v in enumerate(self._raw)
        )

    def __len__(self):
        return len(self._raw)

    def __eq__(self, other):
        return list(self) == other

    def __repr__(self):
        return repr(list(self))
