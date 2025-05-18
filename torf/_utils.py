import collections
import os


def is_sequence(obj):
    """Whether `obj` is a sequence but not a :class:`str`"""
    return (
        not isinstance(obj, (str, bytes, bytearray))
        and isinstance(obj, collections.abc.Sequence)
    )


def flatten(items):
    """Yield all items from nested iterables"""
    flat_items = []
    for item in items:
        if isinstance(item, collections.abc.Iterable) and not isinstance(item, str):
            flat_items.extend(flatten(item))
        else:
            flat_items.append(item)
    return tuple(flat_items)


def iterable_startswith(a, b):
    """Whether sequence `a` starts with the items in sequence `b`"""
    return tuple(a[:len(b)]) == tuple(b)


def merge_dicts(*dcts):
    """Merge mulitiple nested dictionaries"""
    merged = {}
    for dct in dcts:
        if not isinstance(dct, collections.abc.Mapping):
            raise TypeError(f'Expected Mapping, not {type(dct).__name__}: {dct!r}')
        for k, v in dct.items():
            if k in merged:
                v_merged = merged[k]
                if isinstance(v, collections.abc.Mapping) and isinstance(v_merged, collections.abc.Mapping):
                    merged[k] = merge_dicts(v_merged, v)
                else:
                    merged[k] = v
            else:
                merged[k] = v
    return merged


class File(str):
    """
    :class:`str` with :attr:`size` and :attr:`path` attribute

    :param path: File path as sequence (e.g. ``("foo", "bar", "baz")`` -> "foo/bar/baz")
    :param size: Size of the file in bytes
    """

    def __new__(cls, *path, size):
        try:
            self = super().__new__(cls, os.path.join(*path))
        except TypeError:
            raise ValueError(f'Unexpected path: {path!r}')
        self._path = tuple(self.split(os.path.sep))
        try:
            self._size = int(size)
        except (ValueError, TypeError):
            raise ValueError(f'size must be int, not {type(size).__name__}: {size!r}')
        return self

    @property
    def path(self):
        """Individual path components"""
        return self._path

    @property
    def size(self):
        """Size of the file in bytes"""
        return self._size

    @property
    def name(self):
        """Last item in :attr:`path`"""
        return self._path[-1]

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return (
                self.path == other.path
                and self.size == other.size
            )
        else:
            return NotImplemented

    def __hash__(self):
        return hash((self.path, self.size))

    def __repr__(self):
        posargs = ', '.join(repr(part) for part in self.path)
        kwargs = f'size={self.size}'
        args = ', '.join((posargs, kwargs))
        return f'{type(self).__name__}({args})'


class ImmutableDict(dict):
    """Subclass of :class:`dict` that doesn't allow any mutating method to be called"""

    def __setitem__(self, *args, **kwargs):
        raise TypeError(f'{type(self).__name__} is immutable')

    def __delitem__(self, *args, **kwargs):
        raise TypeError(f'{type(self).__name__} is immutable')

    def clear(self, *args, **kwargs):
        raise TypeError(f'{type(self).__name__} is immutable')

    def pop(self, *args, **kwargs):
        raise TypeError(f'{type(self).__name__} is immutable')

    def popitem(self, *args, **kwargs):
        raise TypeError(f'{type(self).__name__} is immutable')

    def setdefault(self, *args, **kwargs):
        raise TypeError(f'{type(self).__name__} is immutable')

    def update(self, *args, **kwargs):
        raise TypeError(f'{type(self).__name__} is immutable')

    def mutable(self):
        """
        Return normal, mutable :class:`dict` of this instance

        Nested :class:`~.collections.abc.Mapping` objects are converted to :class:`dict` and
        non-string :class:`~.collections.abc.Sequence` objects are converted to :class:`list`.
        """

        def make_mutable(obj):
            if isinstance(obj, collections.abc.Mapping):
                return {
                    k: make_mutable(v)
                    for k, v in obj.items()
                }
            elif is_sequence(obj):
                return [
                    make_mutable(v)
                    for v in obj
                ]
            else:
                return obj

        return make_mutable(self)

    def __repr__(self):
        return f'{type(self).__name__}({dict(self)!r})'
