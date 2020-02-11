import torf

import pytest
from unittest import mock
import os
import shutil
import random
import collections
import itertools
import re
import math
import errno

import logging
debug = logging.getLogger('test').debug
from . import display_filespecs

class fuzzylist(list):
    """
    List that is fuzzily equal to other lists

    >>> x = fuzzylist('a', 'b', 'c', maybe=('x', 'y', 'z'))
    >>> x
    ['a', 'b', 'c']
    >>> x == ['z', 'b', 'a', 'c', 'y']
    True

    Limit the number of optional items:

    >>> x = fuzzylist('a', 'b', 'c', maybe=('x', 'x'))
    >>> x == ['a', 'x', 'b', 'x', 'c']
    True
    >>> x == ['a', 'x', 'b', 'x', 'c', 'x']
    False

    `max_maybe_items` also allows you to limit the number of optional items:

    >>> x = fuzzylist('a', 'b', 'c', maybe=('x', 'y', 'z'), max_maybe_items={'x':1})
    >>> x == ['a', 'x', 'b', 'z', 'c']
    True
    >>> x == ['a', 'x', 'b', 'x', 'c']
    False

    Unlike `set(...) == set(...)`, this doesn't remove duplicate items and
    allows unhashable items.
    """
    def __init__(self, *args, maybe=(), max_maybe_items={}):
        self.maybe = list(maybe)
        self.max_maybe_items = dict(max_maybe_items)
        super().__init__(args)

    def __eq__(self, other):
        if tuple(self) != tuple(other):
            # Check if either list contains any disallowed items, accepting
            # items from `maybe`.
            other_maybe = getattr(other, 'maybe', [])
            for item in self:
                if item not in other and item not in other_maybe:
                    return False
            self_maybe = self.maybe
            for item in other:
                if item not in self and item not in self_maybe:
                    return False
            # Check if either list contains an excess of items.
            other_max = getattr(other, 'max_maybe_items', {})
            for item in itertools.chain(self, self.maybe):
                maxcount = max(other_max.get(item, 1),
                               (other + other_maybe).count(item))
                if self.count(item) > maxcount:
                    return False
            self_max = self.max_maybe_items
            for item in itertools.chain(other, other_maybe):
                maxcount = max(self_max.get(item, 1),
                               (self + self_maybe).count(item))
                if other.count(item) > maxcount:
                    return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def __bool__(self):
        return len(self) > 0 or len(self.maybe) > 0

    def __add__(self, other):
        items = super().__add__(other)
        maybe = self.maybe + getattr(other, 'maybe', [])
        max_maybe_items = {**self.max_maybe_items, **getattr(other, 'max_maybe_items', {})}
        return type(self)(*items, maybe=maybe, max_maybe_items=max_maybe_items)

    def __repr__(self):
        s = f'{type(self).__name__}('
        if super().__len__() > 0:
            s += ', '.join(repr(item) for item in super().__iter__())
        if self.maybe:
            s += f', maybe={repr(self.maybe)}'
        if self.max_maybe_items:
            s += f', max_maybe_items={repr(self.max_maybe_items)}'
        return s + ')'

def test_fuzzylist():
    x = fuzzylist('a', 'b', 'c', maybe=('x', 'y', 'z'), max_maybe_items={'x':1})
    assert     x != ['a', 'b']
    assert not x == ['a', 'b']
    assert     x == ['a', 'c', 'b']
    assert not x != ['a', 'c', 'b']
    assert     x == ['a', 'x', 'c', 'y', 'b']
    assert not x != ['a', 'x', 'c', 'y', 'b']
    assert     x == ['a', 'x', 'b', 'z', 'c', 'y']
    assert not x != ['a', 'x', 'b', 'z', 'c', 'y']
    assert     x != ['a', 'l', 'b', 'z', 'c', 'y']
    assert not x == ['a', 'l', 'b', 'z', 'c', 'y']
    assert     x != ['x', 'b', 'x', 'a', 'c', 'y']
    assert not x == ['x', 'b', 'x', 'a', 'c', 'y']
    assert fuzzylist(0) == fuzzylist(maybe=(0,))
    assert fuzzylist(maybe=(0,)) == fuzzylist(0)
    assert fuzzylist(0) != fuzzylist(maybe=(1,))
    assert fuzzylist(maybe=(1,)) != fuzzylist(0)
    assert [1, 1, 2, 3] != fuzzylist(1, 2, 3)
    assert fuzzylist(1, 2, 3) != [1, 1, 2, 3]
    assert fuzzylist(0, 0, 1) == fuzzylist(0, 1, maybe=[0])
    assert fuzzylist(0, 1, maybe=[0]) == fuzzylist(0, 0, 1)

class fuzzydict(dict):
    """
    Dictionary that ignores empty `fuzzylist` values when determining equality,
    e.g. fuzzydict(x=fuzzylist()) == {}
    """
    def __eq__(self, other):
        if super().__eq__(other):
            return True
        elif not isinstance(other, dict):
            return NotImplemented
        keys_same = set(self).intersection(other)
        for k in keys_same:
            if self[k] != other[k]:
                return False
        keys_diff = set(self).difference(other)
        for k in keys_diff:
            sv = self.get(k, fuzzylist())
            ov = other.get(k, fuzzylist())
            if sv != ov:
                return False
        return True

    def __repr__(self):
        return f'{type(self).__name__}({super().__repr__()})'

def test_fuzzydict():
    x = fuzzydict(a='foo', b=fuzzylist(maybe=(1, 2, 3)))
    assert fuzzydict(a='foo', b=fuzzylist(maybe=(1, 2, 3))) == {'a': 'foo'}
    assert fuzzydict(a='foo', b=fuzzylist(maybe=(1, 2, 3))) == {'a': 'foo', 'b': []}
    assert fuzzydict(a='foo', b=fuzzylist(maybe=(1, 2, 3))) != {'a': 'foo', 'b': ['bar']}
    assert fuzzydict(a='foo', b=fuzzylist(maybe=(1, 2, 3))) != {'b': []}
    assert fuzzydict(a='foo', b=fuzzylist(maybe=(1, 2, 3))) != {}
    assert fuzzydict(b=fuzzylist(maybe=(1, 2, 3))) == {}
    assert fuzzydict(b=fuzzylist(maybe=(1, 2, 3))) == {'x': fuzzylist(maybe=(4, 5, 6))}

def ComparableException(exc):
    """
    Horrible hack that allows us to compare exceptions comfortably

    `exc1 == exc2` is True if both exceptions have the same type and the same
    message.  Type checking with issubclass() and isinstance() also works as
    expected.
    """
    # Make the returned class object an instance of the type of `exc` and the
    # returned Comparable* class.
    class ComparableExceptionMeta(type):
        _cls = type(exc)
        @classmethod
        def __subclasscheck__(mcls, cls):
            return issubclass(cls, mcls._cls) or issubclass(cls, mcls)
        @classmethod
        def __instancecheck__(mcls, inst):
            return isinstance(cls, mcls._cls) or isinstance(cls, mcls)

    # Make subclass of the same name with "Comparable" prepended
    clsname = 'Comparable' + type(exc).__name__
    bases = (type(exc),)
    attrs = {}
    def __eq__(self, other, _real_cls=type(exc)):
        return isinstance(other, (type(self), _real_cls)) and str(self) == str(other)
    attrs['__eq__'] = __eq__
    def __hash__(self):
        return hash(str(self))
    attrs['__hash__'] = __hash__
    cls = ComparableExceptionMeta(clsname, bases, attrs)
    if isinstance(exc, torf.TorfError):
        return cls(*exc.posargs, **exc.kwargs)
    else:
        raise exc

def random_positions(stream):
    """Return list of 1, 2 or 3 random indexes in `stream`"""
    positions = random.sample(range(len(stream)), k=3)
    return sorted(positions[:random.randint(1, len(positions))])

def round_up_to_multiple(n, x):
    """Round `n` up to the next multiple of `x`"""
    return n - n % -x

def round_down_to_multiple(n, x):
    """Round `n` down to the previous multiple of `x`"""
    if n % x != 0:
        return round_up_to_multiple(n, x) - x
    else:
        return n

def find_common_member(lista, listb, first=True, last=False):
    """Find members common to `lista` and `listb`, then return the first or last one"""
    intersection = tuple(set(lista).intersection(listb))
    if intersection:
        return intersection[0] if first else intersection[-1]
    return None

def file_range(filename, filespecs):
    """Return `filename`'s first and last byte index in stream"""
    pos = 0
    for fn,size in filespecs:
        if fn == filename:
            return pos, pos + size - 1
        pos += size
    raise RuntimeError(f'Could not find {filename} in {filespecs}')

def file_piece_indexes(filename, filespecs, piece_size, exclusive=False):
    """
    Return list of indexes of pieces that contain bytes from `filename`

    If `exclusive` is True, don't include pieces that contain bytes from
    multiple files.
    """
    file_beg,file_end = file_range(filename, filespecs)
    first_piece_index_pos = round_down_to_multiple(file_beg, piece_size)
    piece_indexes = []
    for pos in range(first_piece_index_pos, file_end + 1, piece_size):
        if not exclusive or len(pos2files(pos, filespecs, piece_size)) == 1:
            piece_indexes.append(pos // piece_size)
    return piece_indexes

def pos2files(pos, filespecs, piece_size, include_file_at_pos=True):
    """
    Calculate which piece the byte at `pos` belongs to and return a list of file
    names of those files that are covered by that piece.
    """
    p = 0
    filenames = []
    for filename,filesize in filespecs:
        filepos_beg = p
        filepos_end = filepos_beg + filesize - 1
        first_piece_index = filepos_beg // piece_size
        last_piece_index = filepos_end // piece_size
        first_piece_index_pos_beg = first_piece_index * piece_size
        last_piece_index_pos_end = (last_piece_index+1) * piece_size - 1
        if first_piece_index_pos_beg <= pos <= last_piece_index_pos_end:
            filenames.append(filename)
        p += filesize

    if not include_file_at_pos:
        file_at_pos,_ = pos2file(pos, filespecs, piece_size)
        return [f for f in filenames if f != file_at_pos]
    else:
        return filenames

def pos2file(pos, filespecs, piece_size):
    """Return file name and relative position of `pos` in file"""
    p = 0
    for filename,filesize in filespecs:
        if p <= pos < p + filesize:
            return (filename, pos - p)
        p += filesize
    raise RuntimeError(f'Could not find file at position {pos} in {filespecs}')

def calc_piece_indexes(filespecs, piece_size, files_missing):
    """
    Turn a list of (filename, filesize) tuples into a dictionary that maps file
    names to the piece indexes they cover. Pieces that overlap multiple files
    belong to the last file they cover.
    """
    piece_indexes = collections.defaultdict(lambda: fuzzylist())
    pos = 0
    for i,(filename,filesize) in enumerate(filespecs):
        first_pi = pos // piece_size
        # Last piece needs special treatment
        if i < len(filespecs)-1:
            pos_end = pos + filesize
        else:
            pos_end = round_up_to_multiple(pos + filesize, piece_size)
        last_pi = pos_end // piece_size
        piece_indexes[filename].extend(range(first_pi, last_pi))
        pos += filesize

    # For each missing file, the first piece of the file may get two calls, one
    # for the "no such file" error and one for the "corrupt piece" error
    for filepath in files_missing:
        filename = os.path.basename(filepath)
        file_beg,file_end = file_range(filename, filespecs)
        piece_index = file_beg // piece_size
        piece_indexes[filename].maybe.append(piece_index)

    # Remove empty lists, which we added to maintain file order
    for k in tuple(piece_indexes):
        if not piece_indexes[k]:
            del piece_indexes[k]

    return piece_indexes

def calc_good_pieces(filespecs, piece_size, files_missing, corruption_positions):
    """Same as `calc_piece_indexes`, but exclude corrupt pieces and pieces of missing files"""
    corr_pis = {corrpos // piece_size for corrpos in corruption_positions}
    debug(f'Calculating good pieces')
    debug(f'corrupt piece_indexes: {corr_pis}')
    debug(f'missing files: {files_missing}')

    # Find pieces that exclusively belong to missing files
    missing_pis = set()
    for filepath in files_missing:
        file_beg,file_end = file_range(os.path.basename(filepath), filespecs)
        first_missing_pi = file_beg // piece_size
        last_missing_pi = file_end // piece_size
        affected_files_beg = pos2files(file_beg, filespecs, piece_size)
        affected_files_end = pos2files(file_end, filespecs, piece_size)
        debug(f'affected_files_beg: {affected_files_beg}')
        debug(f'affected_files_end: {affected_files_end}')
        missing_pis.update(range(first_missing_pi, last_missing_pi+1))

    all_piece_indexes = calc_piece_indexes(filespecs, piece_size, files_missing)
    debug(f'all piece_indexes: {all_piece_indexes}')

    # Remove pieces that are either corrupt or in missing_pis
    good_pieces = collections.defaultdict(lambda: fuzzylist())
    for fname,all_pis in all_piece_indexes.items():
        for i,pi in enumerate(all_pis):
            if pi not in corr_pis and pi not in missing_pis:
                good_pieces[fname].append(pi)
    debug(f'corruptions and missing files removed: {good_pieces}')
    return good_pieces

def calc_corruptions(filespecs, piece_size, corruption_positions):
    """Map file names to (piece_index, exception) tuples"""
    corrupt_pieces = []
    reported = []
    for corrpos in sorted(corruption_positions):
        corr_pi = corrpos // piece_size
        if corr_pi not in reported:
            exc = torf.VerifyContentError(corr_pi, piece_size, filespecs)
            # debug(f'### Corruption position {corrpos} is in piece index {corr_pi}: {exc}')
            corrupt_pieces.append(exc)
            reported.append(corr_pi)
    return corrupt_pieces

def calc_pieces_done(filespecs_abspath, piece_size, files_missing):
    debug(f'Calculating pieces_done')
    # The callback gets the number of verified pieces (pieces_done).  This
    # function calculates the expected values for that argument.
    #
    # It's not as simple as range(1, <number of pieces>+1).  For example, if a
    # file is missing, we get the same pieces_done value two times, once for "No
    # such file" and maybe again for "Corrupt piece" if the piece contains parts
    # of another file.
    files_missing = [str(filepath) for filepath in files_missing]
    debug(f'missing_files: {files_missing}')
    # List of pieces_done values that are reported once
    pieces_done_list = []
    # List of pieces_done values that may appear multiple times
    maybe_double_pieces_done = []
    # Map pieces_done values to the number of times they may appear
    maybe_double_pieces_done_counts = {}
    pos = 0
    bytes_left = sum(filesize for _,filesize in filespecs_abspath)
    total_size = bytes_left
    pieces_done = 1
    calc_pd = lambda pos: (pos // piece_size) + 1   # pieces_done

    debug(f'{bytes_left} bytes left')
    prev_pi = -1
    while bytes_left > 0:
        filepath,_ = pos2file(pos, filespecs_abspath, piece_size)
        file_beg,file_end = file_range(filepath, filespecs_abspath)
        file_size = file_end - file_beg + 1
        current_pi = pos // piece_size
        file_beg_pi = file_beg // piece_size
        file_end_pi = file_end // piece_size

        debug(f'{pos}: {os.path.basename(filepath)}, pi={current_pi}, beg={file_beg}, end={file_end}, '
              f'size={file_size}, file_beg_pi={file_beg_pi}, file_end_pi={file_end_pi}')

        # If this piece contains the first byte of a file, find the last file in
        # this piece that is missing.  Any piece after this piece may be
        # reported twice: "No such file" and "corrupt piece" if adjacent file(s)
        # are affected.  We can't predict which piece will be reported twice,
        # but it must be one piece.
        if current_pi == file_beg_pi:
            first_piece_files = pos2files(file_beg, filespecs_abspath, piece_size)
            debug(f'  ? first_piece_files: {first_piece_files}')
            missing_file = find_common_member(files_missing, first_piece_files, last=True)
            if missing_file:
                debug(f'  ! missing: {os.path.basename(missing_file)}')
                # Because we're working in multiple threads, we the corruption
                # may be reported anywhere from the corrupt file's beginning to
                # the final piece in the stream.
                for pieces_done in range(calc_pd(file_beg), calc_pd(total_size-1)+1):
                    maybe_double_pieces_done.append(pieces_done)
                    maybe_double_pieces_done_counts[pieces_done] = 2
                files_missing.remove(missing_file)     # Don't report the same missing file twice

        # Report normal progress ("No such file" errors are additional)
        if current_pi != prev_pi:
            debug(f'  . progress: {calc_pd(pos)}')
            pieces_done_list.append(calc_pd(pos))

        debug(f'  bytes_done = min({file_size}, {piece_size}, {file_end} - {pos} + 1)')
        bytes_done = min(file_size, piece_size, file_end - pos + 1)
        bytes_left -= bytes_done
        pos += bytes_done
        debug(f'  {bytes_done} bytes done, {bytes_left} bytes left')
        prev_pi = current_pi

    # Does the final file end in an incomplete piece?
    if current_pi != file_end_pi:
        debug(f'  . progress: {calc_pd(file_end)}')
        pieces_done_list.append(calc_pd(file_end))

    fuzzy_pieces_done_list = fuzzylist(*pieces_done_list,
                                       maybe=maybe_double_pieces_done,
                                       max_maybe_items=maybe_double_pieces_done_counts)
    return fuzzy_pieces_done_list

class CollectingCallback():
    """Collect call arguments and make basic assertions"""
    def __init__(self, torrent):
        super().__init__()
        self.torrent = torrent
        self.seen_pieces_done = []
        self._seen_piece_indexes = collections.defaultdict(lambda: fuzzylist())
        self._seen_good_pieces = collections.defaultdict(lambda: fuzzylist())
        self._seen_skipped_pieces = collections.defaultdict(lambda: fuzzylist())
        self.seen_exceptions = fuzzylist()

    def __call__(self, t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
        assert t is self.torrent
        assert pieces_total == t.pieces
        assert 1 <= pieces_done <= pieces_total
        self.seen_pieces_done.append(pieces_done)
        self._seen_piece_indexes[os.path.basename(path)].append(piece_index)
        if exc is not None:
            if isinstance(exc, torf.VerifyContentError):
                assert type(piece_hash) is bytes and len(piece_hash) == 20
            else:
                assert piece_hash is None
            self.seen_exceptions.append(ComparableException(exc))
        elif piece_hash is None:
            assert exc is None
            self._seen_skipped_pieces[os.path.basename(path)].append(piece_index)
        else:
            assert exc is None
            assert type(piece_hash) is bytes and len(piece_hash) == 20
            self._seen_good_pieces[os.path.basename(path)].append(piece_index)

    @property
    def seen_piece_indexes(self):
        return dict(self._seen_piece_indexes)

    @property
    def seen_good_pieces(self):
        return dict(self._seen_good_pieces)

    @property
    def seen_skipped_pieces(self):
        return dict(self._seen_skipped_pieces)

class _TestCaseBase():
    """
    This class runs most of the tests while the test_*() functions mostly
    collect parametrized test values
    """
    def __init__(self, create_dir, create_file, create_torrent_file, forced_piece_size):
        self.create_dir = create_dir
        self.create_file = create_file
        self.create_torrent_file = create_torrent_file
        self.forced_piece_size = forced_piece_size
        self.reset()

    def run(self, *_, with_callback, exp_return_value=None, skip_file_on_first_error=False):
        debug(f'Original stream: {self.stream_original.hex()}')
        debug(f' Corrupt stream: {self.stream_corrupt.hex()}')
        debug(f'Corruption positions: {self.corruption_positions}')
        debug(f'Corrupt piece indexes: {set(corrpos // self.piece_size for corrpos in self.corruption_positions)}')

        kwargs = {'skip_file_on_first_error': skip_file_on_first_error,
                  'exp_return_value': exp_return_value}
        if not with_callback:
            exp_exceptions = self.exp_exceptions
            if not exp_exceptions:
                self._run_without_callback(**kwargs)
                self.raised_exception = None
            else:
                exp_exception_types = tuple(set(type(exc) for exc in exp_exceptions))
                with pytest.raises(exp_exception_types) as e:
                    self._run_without_callback(**kwargs)
                # Usually the first error in the stream is reported, but not
                # always, so we expect one of the possible exceptions to be
                # raised.
                assert e.value in exp_exceptions
                self.raised_exception = e.value
        else:
            return self._run_with_callback(**kwargs)

    def _run_without_callback(self, exp_return_value, **kwargs):
        debug(f'################ VERIFY WITHOUT CALLBACK: kwargs={kwargs}')
        if exp_return_value is not None:
            assert self.torrent.verify(self.content_path, **kwargs) is exp_return_value
        else:
            self.torrent.verify(self.content_path, **kwargs)

    def _run_with_callback(self, exp_return_value, **kwargs):
        debug(f'################ VERIFY WITH CALLBACK: kwargs={kwargs}')
        cb = CollectingCallback(self.torrent)
        kwargs['callback'] = cb
        kwargs['interval'] = 0
        if exp_return_value is not None:
            assert self.torrent.verify(self.content_path, **kwargs) is exp_return_value
        else:
            self.torrent.verify(self.content_path, **kwargs)
        # Last pieces_done value must be the total number of pieces so progress
        # is reported correctly
        assert cb.seen_pieces_done[-1] == self.torrent.pieces
        return cb

    def reset(self):
        self.raised_exception = None
        self.corruption_positions = set()
        self.files_missing = []
        for attr in ('_exp_exceptions', '_exp_pieces_done',
                     '_exp_piece_indexes', '_exp_good_pieces',
                     '_exp_corruptions', '_exp_files_missing'):
            if hasattr(self, attr):
                delattr(self, attr)

    @property
    def exp_exceptions(self):
        if not hasattr(self, '_exp_exceptions'):
            self._exp_exceptions = self.exp_files_missing + self.exp_corruptions
            debug(f'Expected exceptions:')
            for e in self._exp_exceptions:
                debug(e)
        return self._exp_exceptions

    @property
    def exp_pieces_done(self):
        if not hasattr(self, '_exp_pieces_done'):
            self._exp_pieces_done = calc_pieces_done(self.filespecs_abspath, self.piece_size, self.files_missing)
            debug(f'Expected pieces done: {self._exp_pieces_done}')
        return self._exp_pieces_done

    @property
    def exp_piece_indexes(self):
        if not hasattr(self, '_exp_piece_indexes'):
            self._exp_piece_indexes = calc_piece_indexes(self.filespecs, self.piece_size, self.files_missing)
            debug(f'Expected piece indexes: {dict(self._exp_piece_indexes)}')
        return dict(self._exp_piece_indexes)

    @property
    def exp_good_pieces(self):
        if not hasattr(self, '_exp_good_pieces'):
            self._exp_good_pieces = calc_good_pieces(self.filespecs, self.piece_size, self.files_missing,
                                                     self.corruption_positions)
            debug(f'Expected good pieces: {dict(self._exp_good_pieces)}')
        return dict(self._exp_good_pieces)

    @property
    def exp_corruptions(self):
        if not hasattr(self, '_exp_corruptions'):
            self._exp_corruptions = [ComparableException(exc) for exc in
                                     calc_corruptions(self.filespecs_abspath, self.piece_size, self.corruption_positions)]
            debug(f'Expected corruptions: {self._exp_corruptions}')
        return self._exp_corruptions

    @property
    def exp_files_missing(self):
        if not hasattr(self, '_exp_files_missing'):
            self._exp_files_missing = [ComparableException(torf.ReadError(errno.ENOENT, filepath))
                                       for filepath in self.files_missing]
            debug(f'Expected files missing: {self._exp_files_missing}')
        return self._exp_files_missing

class _TestCaseSinglefile(_TestCaseBase):
    @property
    def filespecs_abspath(self):
        return ((str(self.content_path), self.filesize),)

    def setup(self, filespecs, piece_size):
        self.filespecs = filespecs
        self.piece_size = piece_size
        self.filename = filespecs[0][0]
        self.filesize = filespecs[0][1]
        debug(f'Filename: {self.filename}, size: {self.filesize}, piece size: {piece_size}')
        self.stream_original = b'\x00' * self.filesize
        self.stream_corrupt = bytearray(self.stream_original)
        self.content_path = self.create_file(self.filename, self.stream_original)
        with self.forced_piece_size(piece_size):
            with self.create_torrent_file(path=self.content_path) as torrent_filepath:
                self.torrent = torf.Torrent.read(torrent_filepath)

    def corrupt_stream(self, *positions):
        # Introduce random number of corruptions without changing stream length
        corruption_positions = set(random_positions(self.stream_corrupt) if not positions else positions)
        for corrpos in corruption_positions:
            debug(f'Introducing corruption at index {corrpos}')
            self.stream_corrupt[corrpos] = (self.stream_corrupt[corrpos] + 1) % 256
            self.content_path.write_bytes(self.stream_corrupt)
        self.corruption_positions.update(corruption_positions)

    def delete_file(self, index):
        debug(f'Removing file from file system: {os.path.basename(self.content_path)}')
        os.rename(self.content_path, str(self.content_path) + '.deleted')
        self.files_missing = [self.content_path]
        self.stream_corrupt = b'\xCC' * self.torrent.size
        # No need to update self.corruption_positions.  A missing single file
        # does not produce any corruption errors because the "No such file"
        # error is enough.

class _TestCaseMultifile(_TestCaseBase):
    @property
    def filespecs_abspath(self):
        return tuple((str(self.content_path / filename), filesize)
                     for filename,filesize in self.filespecs)

    def setup(self, filespecs, piece_size):
        debug(f'File sizes: {", ".join(f"{n}={s}" for n,s in filespecs)}')
        debug(f'Stream size: {sum(s for _,s in filespecs)}')
        debug(f'Piece size: {piece_size}')
        self.filespecs = filespecs
        self.piece_size = piece_size
        self.content_original = {}
        self.content_corrupt = {}
        create_dir_args = []
        for filename,filesize in filespecs:
            data = b'\x00' * filesize
            self.content_original[filename] = {'size': filesize,
                                               'data': data}
            self.content_corrupt[filename] = {'size': filesize,
                                              'data': bytearray(data)}
            create_dir_args.append((filename, data))
        self.content_path = self.create_dir('content', *create_dir_args)
        for filename,fileinfo in self.content_original.items():
            fileinfo['path'] = self.content_path / filename
            self.content_corrupt[filename]['path'] = fileinfo['path']
        debug(f'Content: {self.content_original}')
        with self.forced_piece_size(piece_size):
            with self.create_torrent_file(path=self.content_path) as torrent_filepath:
                self.torrent = torf.Torrent.read(torrent_filepath)

    @property
    def stream_original(self):
        return b''.join((f['data'] for f in self.content_original.values()))

    @property
    def stream_corrupt(self):
        return b''.join((f['data'] for f in self.content_corrupt.values()))

    def corrupt_stream(self, *positions):
        # Introduce random number of corruptions in random files without
        # changing stream length
        corruption_positions = set(random_positions(self.stream_corrupt) if not positions else positions)
        for corrpos_in_stream in corruption_positions:
            filename,corrpos_in_file = pos2file(corrpos_in_stream, self.filespecs, self.piece_size)
            debug(f'Introducing corruption in {filename} at index {corrpos_in_stream} in stream, '
                  f'{corrpos_in_file} in file {filename}')
            fileinfo = self.content_corrupt[filename]
            fileinfo['data'][corrpos_in_file] = (fileinfo['data'][corrpos_in_file] + 1) % 256
            fileinfo['path'].write_bytes(fileinfo['data'])
        self.corruption_positions.update(corruption_positions)

    def delete_file(self, index):
        # Remove file at `index` in filespecs from file system
        filename,filesize = self.filespecs[index]
        debug(f'Removing file from file system: {os.path.basename(filename)}')
        filepath = self.content_path / filename
        os.rename(filepath, str(filepath) + '.deleted')
        self.files_missing.append(filepath)
        self.content_corrupt[os.path.basename(filename)]['data'] = b'\xCC' * filesize

        corruption_positions = set()
        files_missing = [str(filepath) for filepath in self.files_missing]

        for removed_filepath in self.files_missing:
            removed_filename = os.path.basename(removed_filepath)
            # Find the first byte of the first affected piece and the first byte
            # of the last affected piece
            file_beg,file_end = file_range(removed_filename, self.filespecs)
            debug(f'{removed_filename} starts at {file_beg} and ends at {file_end} in stream')
            first_affected_piece_pos = round_down_to_multiple(file_beg, self.piece_size)
            last_affected_piece_pos = round_down_to_multiple(file_end, self.piece_size)
            debug(f'  First affected piece starts at {first_affected_piece_pos} '
                  f'and last affected piece starts at {last_affected_piece_pos}')

            # Find files that share the first and last affected piece
            for piece_index_pos in set((first_affected_piece_pos, last_affected_piece_pos)):
                debug(f'Finding files affected by piece_index {piece_index_pos} being corrupt:')
                for filepath in pos2files(piece_index_pos, self.filespecs_abspath, self.piece_size):
                    debug(f'  {filepath}')
                    if filepath not in files_missing:
                        debug(f'  {os.path.basename(filepath)} exists, making '
                              f'piece_index {piece_index_pos // self.piece_size} corrupt')
                        corruption_positions.add(piece_index_pos)

        self.corruption_positions = corruption_positions
        debug(f'Corruption positions after removing file: {self.corruption_positions}')

@pytest.fixture
def mktestcase(create_dir, create_file, forced_piece_size, create_torrent_file):
    """Return instance of _TestCaseMultifile or _TestCaseSinglefile"""
    def mktestcase_(filespecs, piece_size):
        if len(filespecs) == 1:
            testcls = _TestCaseSinglefile
        else:
            testcls = _TestCaseMultifile
        testcase = testcls(create_dir, create_file, create_torrent_file, forced_piece_size)
        testcase.setup(filespecs, piece_size)
        debug(f'################ TEST TORRENT CREATED: {testcase.torrent}')
        return testcase
    return mktestcase_


def test_validate_is_called_first(monkeypatch):
    torrent = torf.Torrent()
    mock_validate = mock.Mock(side_effect=torf.MetainfoError('Mock error'))
    monkeypatch.setattr(torrent, 'validate', mock_validate)
    with pytest.raises(torf.MetainfoError) as excinfo:
        torrent.verify('some/path')
    assert str(excinfo.value) == f'Invalid metainfo: Mock error'
    mock_validate.assert_called_once_with()

def test_verify_content_successfully(mktestcase, piece_size, callback, filespecs):
    display_filespecs(filespecs, piece_size)
    tc = mktestcase(filespecs, piece_size)
    cb = tc.run(with_callback=callback['enabled'],
                exp_return_value=True)
    # TODO: Move these to _TestCaseBase.run or something
    if callback['enabled']:
        debug(f'seen_pieces_done: {cb.seen_pieces_done}')
        assert cb.seen_pieces_done == tc.exp_pieces_done
        debug(f'seen_piece_indexes: {cb.seen_piece_indexes}')
        assert cb.seen_piece_indexes == tc.exp_piece_indexes
        debug(f'seen_good_pieces: {cb.seen_good_pieces}')
        assert cb.seen_good_pieces == tc.exp_piece_indexes
        debug(f'seen_exceptions: {cb.seen_exceptions}')
        assert cb.seen_exceptions == []

def test_verify_content_with_random_corruptions_and_no_skipping(mktestcase, piece_size, callback, filespecs):
    display_filespecs(filespecs, piece_size)
    tc = mktestcase(filespecs, piece_size)
    tc.corrupt_stream()
    cb = tc.run(with_callback=callback['enabled'],
                exp_return_value=False)
    if callback['enabled']:
        debug(f'seen_pieces_done: {cb.seen_pieces_done}')
        assert cb.seen_pieces_done == tc.exp_pieces_done
        debug(f'seen_piece_indexes: {cb.seen_piece_indexes}')
        assert cb.seen_piece_indexes == tc.exp_piece_indexes
        debug(f'seen_good_pieces: {cb.seen_good_pieces}')
        assert cb.seen_good_pieces == tc.exp_good_pieces
        debug(f'seen_exceptions: {cb.seen_exceptions}')
        assert cb.seen_exceptions == tc.exp_exceptions

def test_verify_content_with_missing_files_and_no_skipping(mktestcase, piece_size, callback, filespecs, filespec_indexes):
    display_filespecs(filespecs, piece_size)
    tc = mktestcase(filespecs, piece_size)
    for index in filespec_indexes:
        tc.delete_file(index)
    cb = tc.run(with_callback=callback['enabled'],
                exp_return_value=False)
    if callback['enabled']:
        debug(f'seen_pieces_done: {cb.seen_pieces_done}')
        assert cb.seen_pieces_done == tc.exp_pieces_done
        debug(f'seen_piece_indexes: {cb.seen_piece_indexes}')
        assert cb.seen_piece_indexes == tc.exp_piece_indexes
        debug(f'seen_good_pieces: {cb.seen_good_pieces}')
        assert cb.seen_good_pieces == tc.exp_good_pieces
        debug(f'seen_exceptions: {cb.seen_exceptions}')
        assert cb.seen_exceptions == tc.exp_exceptions

def test_verify_content_with_missing_files_and_skipping(mktestcase, piece_size, callback, filespecs, filespec_indexes):
    display_filespecs(filespecs, piece_size)
    tc = mktestcase(filespecs, piece_size)
    for index in filespec_indexes:
        tc.delete_file(index)
    cb = tc.run(with_callback=callback['enabled'],
                skip_file_on_first_error=True,
                exp_return_value=False)
    if callback['enabled']:
        debug(f'seen_pieces_done: {cb.seen_pieces_done}')
        assert cb.seen_pieces_done == tc.exp_pieces_done
        debug(f'seen_piece_indexes: {cb.seen_piece_indexes}')
        assert cb.seen_piece_indexes == tc.exp_piece_indexes
        debug(f'seen_good_pieces: {cb.seen_good_pieces}')
        assert cb.seen_good_pieces == tc.exp_good_pieces
        debug(f'seen_exceptions: {cb.seen_exceptions}')
        assert cb.seen_exceptions == tc.exp_exceptions

# TODO: File is smaller
# TODO: File is bigger


# # def test_verify_content__file_is_smaller(create_dir, create_torrent_file, forced_piece_size):
# #     with forced_piece_size(8) as piece_size:
# #         b_data = create_dir.random_bytes(2*piece_size+4)
# #         content_path = create_dir('content',
# #                                   ('a', 1*piece_size+3),
# #                                   ('b', b_data),
# #                                   ('c', 1*piece_size+5))
# #         with create_torrent_file(path=content_path) as torrent_file:
# #             torrent = torf.Torrent.read(torrent_file)

# #             corruption_offset = piece_size + 2
# #             b_data_corrupt = b_data[:corruption_offset] + b_data[corruption_offset+1:]
# #             assert len(b_data_corrupt) == len(b_data) - 1
# #             (content_path / 'b').write_bytes(b_data_corrupt)

# #             log.debug('################ TEST WITHOUT CALLBACK ##################')
# #             with pytest.raises(torf.VerifyFileSizeError) as excinfo:
# #                 torrent.verify(content_path, skip_file_on_first_error=False)
# #             assert str(excinfo.value) == f'{content_path / "b"}: Too small: 19 instead of 20 bytes'

# #             log.debug('################ TEST WITH CALLBACK ##################')
# #             # (8+3) + (2*8+4) + (8+5) = 6 pieces (max_piece_index=5)
# #             exp_piece_indexes = [
# #                 0,    # stream slice  0 -  8: a[ 0: 8]         - ok
# #                 1,    # stream slice  8 - 16: a[-3:  ] + b[:5] - VerifyFileSizeError for b
# #                 1,    # stream slice  8 - 16: a[-3:  ] + b[:5] - ok
# #                 2,    # stream slice 16 - 24: b[ 5:13]         - missing byte at size(a) + corruption_offset
# #                 3,    # stream slice 24 - 32: b[13:20] + c[:1] - VerifyContentError for b
# #                 4,    # stream slice 32 - 40: c[ 1: 9]         - ok
# #                 5,    # stream slice 40 - 44: c[ 9:13]         - ok
# #             ]
# #             exp_call_count = len(exp_piece_indexes)
# #             exp_piece_1_exc = [torf.VerifyFileSizeError, type(None)]
# #             def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
# #                 assert t == torrent
# #                 assert pieces_total == torrent.pieces
# #                 assert 1 <= pieces_done <= pieces_total
# #                 if piece_index == 0:
# #                     assert str(path) == str(content_path / 'a')
# #                     assert type(piece_hash) is bytes and len(piece_hash) == 20
# #                     assert exc is None
# #                     exp_piece_indexes.remove(piece_index)
# #                 elif piece_index == 1:
# #                     exp_piece_1_exc.remove(type(exc))
# #                     if isinstance(exc, torf.VerifyFileSizeError):
# #                         assert str(path) == str(content_path / 'b')
# #                         assert piece_hash is None
# #                         assert str(exc) == f'{content_path / "b"}: Too small: 19 instead of 20 bytes'
# #                     else:
# #                         assert str(path) == str(content_path / 'b')
# #                         assert type(piece_hash) is bytes and len(piece_hash) == 20
# #                         assert exc is None
# #                     exp_piece_indexes.remove(piece_index)
# #                 elif piece_index == 2:
# #                     assert str(path) == str(content_path / 'b')
# #                     assert type(piece_hash) is bytes and len(piece_hash) == 20
# #                     assert str(exc) == f'Corruption in piece 3 in {content_path / "b"}'
# #                     exp_piece_indexes.remove(piece_index)
# #                 elif piece_index == 3:
# #                     assert isinstance(exc, torf.VerifyContentError)
# #                     assert str(path) == str(content_path / 'c')
# #                     assert type(piece_hash) is bytes and len(piece_hash) == 20
# #                     assert str(exc) == (f'Corruption in piece 4, at least one of these files is corrupt: '
# #                                         f'{content_path / "b"}, {content_path / "c"}')
# #                     exp_piece_indexes.remove(piece_index)
# #                 elif piece_index in (4, 5):
# #                     assert str(path) == str(content_path / 'c')
# #                     assert type(piece_hash) is bytes and len(piece_hash) == 20
# #                     assert exc is None
# #                     exp_piece_indexes.remove(piece_index)
# #             cb = mock.Mock(side_effect=assert_call)
# #             assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False
# #             assert len(exp_piece_indexes) == 0, exp_piece_indexes
# #             assert len(exp_piece_1_exc) == 0, exp_piece_1_exc
# #             assert cb.call_count == exp_call_count


# # def test_verify_content__file_contains_extra_bytes_in_the_middle(create_dir, create_torrent_file, forced_piece_size):
# #     with forced_piece_size(8) as piece_size:
# #         b_data = create_dir.random_bytes(2*piece_size+4)
# #         content_path = create_dir('content',
# #                                   ('a', 1*piece_size+3),
# #                                   ('b', b_data),
# #                                   ('c', 1*piece_size+5))
# #         with create_torrent_file(path=content_path) as torrent_file:
# #             torrent = torf.Torrent.read(torrent_file)

# #             corruption_offset = 2*piece_size + 1
# #             b_data_corrupt = b_data[:corruption_offset] + b'\x12' + b_data[corruption_offset:]
# #             assert len(b_data_corrupt) == len(b_data) + 1
# #             (content_path / 'b').write_bytes(b_data_corrupt)

# #             log.debug('################ TEST WITHOUT CALLBACK ##################')
# #             with pytest.raises(torf.TorfError) as excinfo:
# #                 torrent.verify(content_path, skip_file_on_first_error=False)
# #             assert str(excinfo.value) == f'{content_path / "b"}: Too big: 21 instead of 20 bytes'

# #             log.debug('################ TEST WITH CALLBACK ##################')
# #             # (8+3) + (2*8+4) + (8+5) = 6 pieces (max_piece_index=5)
# #             exp_piece_indexes = [
# #                 0,    # stream slice  0 -  8: a[ 0: 8]         - ok
# #                 1,    # stream slice  8 - 16: a[-3:  ] + b[:5] - VerifyFileSizeError for b
# #                 1,    # stream slice  8 - 16: a[-3:  ] + b[:5] - ok
# #                 2,    # stream slice 16 - 24: b[ 5:13]         - corrupt, byte 28 in stream has corrupt byte inserted
# #                 3,    # stream slice 24 - 32: b[13:20] + c[:1] - VerifyContentError for b
# #                 4,    # stream slice 32 - 40: c[ 1: 9]         - ok
# #                 5,    # stream slice 40 - 44: c[ 9:13]         - ok
# #             ]
# #             exp_call_count = len(exp_piece_indexes)
# #             exp_piece_1_exc = [torf.VerifyFileSizeError, type(None)]
# #             def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
# #                 assert t == torrent
# #                 assert pieces_total == torrent.pieces
# #                 assert 1 <= pieces_done <= pieces_total
# #                 if piece_index == 0:
# #                     assert str(path) == str(content_path / 'a')
# #                     assert type(piece_hash) is bytes and len(piece_hash) == 20
# #                     assert exc is None
# #                     exp_piece_indexes.remove(piece_index)
# #                 elif piece_index == 1:
# #                     exp_piece_1_exc.remove(type(exc))
# #                     if isinstance(exc, torf.VerifyFileSizeError):
# #                         assert str(path) == str(content_path / 'b')
# #                         assert piece_hash is None
# #                         assert str(exc) == f'{content_path / "b"}: Too big: 21 instead of 20 bytes'
# #                     else:
# #                         assert str(path) == str(content_path / 'b')
# #                         assert type(piece_hash) is bytes and len(piece_hash) == 20
# #                         assert exc is None
# #                     exp_piece_indexes.remove(piece_index)
# #                 elif piece_index == 2:
# #                     assert str(path) == str(content_path / 'b')
# #                     assert type(piece_hash) is bytes and len(piece_hash) == 20
# #                     assert exc is None
# #                     exp_piece_indexes.remove(piece_index)
# #                 elif piece_index == 3:
# #                     assert str(path) == str(content_path / 'c')
# #                     assert type(piece_hash) is bytes and len(piece_hash) == 20
# #                     assert str(exc) == (f'Corruption in piece {piece_index+1}, '
# #                                         'at least one of these files is corrupt: '
# #                                         f'{content_path / "b"}, {content_path / "c"}')
# #                     exp_piece_indexes.remove(piece_index)
# #                 elif piece_index in (4, 5):
# #                     assert str(path) == str(content_path / 'c')
# #                     assert type(piece_hash) is bytes and len(piece_hash) == 20
# #                     assert exc is None
# #                     exp_piece_indexes.remove(piece_index)
# #             cb = mock.Mock(side_effect=assert_call)
# #             assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False
# #             assert len(exp_piece_indexes) == 0, exp_piece_indexes
# #             assert len(exp_piece_1_exc) == 0, exp_piece_1_exc
# #             assert cb.call_count == exp_call_count


# # def test_verify_content__file_contains_extra_bytes_at_the_end(create_dir, create_torrent_file, forced_piece_size):
# #     with forced_piece_size(8) as piece_size:
# #         b_data = create_dir.random_bytes(2*piece_size+4)
# #         content_path = create_dir('content',
# #                                   ('a', 1*piece_size+3),
# #                                   ('b', b_data),
# #                                   ('c', 1*piece_size+5))
# #         with create_torrent_file(path=content_path) as torrent_file:
# #             torrent = torf.Torrent.read(torrent_file)

# #             corruption_offset = piece_size
# #             b_data_corrupt = b_data + b'\xff'
# #             assert len(b_data_corrupt) == len(b_data) + 1
# #             (content_path / 'b').write_bytes(b_data_corrupt)

# #             log.debug('################ TEST WITHOUT CALLBACK ##################')
# #             with pytest.raises(torf.VerifyFileSizeError) as excinfo:
# #                 torrent.verify(content_path, skip_file_on_first_error=False)
# #             assert str(excinfo.value) == f'{content_path / "b"}: Too big: 21 instead of 20 bytes'

# #             log.debug('################ TEST WITH CALLBACK ##################')
# #             # (8+3) + (2*8+4) + (8+5) = 6 pieces (max_piece_index=5)
# #             exp_piece_indexes = [
# #                 0,    # stream slice  0 -  8: a[ 0: 8]         - ok
# #                 1,    # stream slice  8 - 16: a[-3:  ] + b[:5] - VerifyFileSizeError for b
# #                 1,    # stream slice  8 - 16: a[-3:  ] + b[:5] - ok
# #                 2,    # stream slice 16 - 24: b[ 5:13]         - ok
# #                 3,    # stream slice 24 - 32: b[13:20] + c[:1] - ok
# #                 4,    # stream slice 32 - 40: c[ 1: 9]         - ok
# #                 5,    # stream slice 40 - 44: c[ 9:13]         - ok
# #             ]

# #             exp_call_count = len(exp_piece_indexes)
# #             exp_piece_1_exc = [torf.VerifyFileSizeError, type(None)]
# #             def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
# #                 assert t == torrent
# #                 assert pieces_total == torrent.pieces
# #                 assert 1 <= pieces_done <= pieces_total
# #                 if piece_index == 0:
# #                     assert str(path) == str(content_path / 'a')
# #                     assert type(piece_hash) is bytes and len(piece_hash) == 20
# #                     assert exc is None
# #                     exp_piece_indexes.remove(piece_index)
# #                 elif piece_index == 1:
# #                     exp_piece_1_exc.remove(type(exc))
# #                     if isinstance(exc, torf.VerifyFileSizeError):
# #                         assert str(path) == str(content_path / 'b')
# #                         assert piece_hash is None
# #                         assert str(exc) == f'{content_path / "b"}: Too big: 21 instead of 20 bytes'
# #                     else:
# #                         assert str(path) == str(content_path / 'b')
# #                         assert type(piece_hash) is bytes and len(piece_hash) == 20
# #                         assert exc is None
# #                     exp_piece_indexes.remove(piece_index)
# #                 elif piece_index == 2:
# #                     assert str(path) == str(content_path / 'b')
# #                     assert type(piece_hash) is bytes and len(piece_hash) == 20
# #                     assert exc is None
# #                     exp_piece_indexes.remove(piece_index)
# #                 elif piece_index == 3:
# #                     assert str(path) == str(content_path / 'c')
# #                     assert type(piece_hash) is bytes and len(piece_hash) == 20
# #                     assert exc is None
# #                     exp_piece_indexes.remove(piece_index)
# #                 elif piece_index in (4, 5):
# #                     assert str(path) == str(content_path / 'c')
# #                     assert type(piece_hash) is bytes and len(piece_hash) == 20
# #                     assert exc is None
# #                     exp_piece_indexes.remove(piece_index)
# #             cb = mock.Mock(side_effect=assert_call)
# #             assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False
# #             assert len(exp_piece_indexes) == 0, exp_piece_indexes
# #             assert len(exp_piece_1_exc) == 0, exp_piece_1_exc
# #             assert cb.call_count == exp_call_count


# # def test_verify_content__file_is_same_size_but_corrupt(create_dir, create_torrent_file, forced_piece_size):
# #     with forced_piece_size(8) as piece_size:
# #         b_data = create_dir.random_bytes(2*piece_size+4)
# #         content_path = create_dir('content',
# #                                   ('a', 1*piece_size+3),
# #                                   ('b', b_data),
# #                                   ('c', 1*piece_size+5))
# #         with create_torrent_file(path=content_path) as torrent_file:
# #             torrent = torf.Torrent.read(torrent_file)

# #             corruption_offset = 2*piece_size+4 - 1
# #             b_data_corrupt = b_data[:corruption_offset] + b'\x12' + b_data[corruption_offset+1:]
# #             assert len(b_data_corrupt) == len(b_data)
# #             (content_path / 'b').write_bytes(b_data_corrupt)

# #             log.debug('################ TEST WITHOUT CALLBACK ##################')
# #             with pytest.raises(torf.VerifyContentError) as excinfo:
# #                 torrent.verify(content_path, skip_file_on_first_error=False)
# #             assert str(excinfo.value) == (f'Corruption in piece 4, at least one of these files is corrupt: '
# #                                           f'{content_path / "b"}, {content_path / "c"}')

# #             log.debug('################ TEST WITH CALLBACK ##################')
# #             # (8+3) + (2*8+4) + (8+5) = 6 pieces (max_piece_index=5)
# #             exp_piece_indexes = [
# #                 0,    # stream slice  0 -  8: a[ 0: 8]         - ok
# #                 1,    # stream slice  8 - 16: a[-3:  ] + b[:5] - ok
# #                 2,    # stream slice 16 - 24: b[ 5:13]         - ok
# #                 3,    # stream slice 24 - 32: b[13:20] + c[:1] - VerifyContentError
# #                 4,    # stream slice 32 - 40: c[ 1: 9]         - ok
# #                 5,    # stream slice 40 - 44: c[ 9:13]         - ok
# #             ]

# #             exp_call_count = len(exp_piece_indexes)
# #             def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
# #                 assert t == torrent
# #                 assert pieces_total == torrent.pieces
# #                 assert 1 <= pieces_done <= pieces_total
# #                 if piece_index == 0:
# #                     assert str(path) == str(content_path / 'a')
# #                     assert type(piece_hash) is bytes and len(piece_hash) == 20
# #                     assert exc is None
# #                     exp_piece_indexes.remove(piece_index)
# #                 elif piece_index in (1, 2):
# #                     assert str(path) == str(content_path / 'b')
# #                     assert type(piece_hash) is bytes and len(piece_hash) == 20
# #                     assert exc is None
# #                     exp_piece_indexes.remove(piece_index)
# #                 elif piece_index == 3:
# #                     assert str(path) == str(content_path / 'c')
# #                     assert type(piece_hash) is bytes and len(piece_hash) == 20
# #                     assert str(exc) == (f'Corruption in piece 4, at least one of these files is corrupt: '
# #                                         f'{content_path / "b"}, {content_path / "c"}')
# #                     exp_piece_indexes.remove(piece_index)
# #                 elif piece_index in (4, 5):
# #                     assert str(path) == str(content_path / 'c')
# #                     assert type(piece_hash) is bytes and len(piece_hash) == 20
# #                     assert exc is None
# #                     exp_piece_indexes.remove(piece_index)
# #             cb = mock.Mock(side_effect=assert_call)
# #             assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False
# #             assert len(exp_piece_indexes) == 0, exp_piece_indexes
# #             assert cb.call_count == exp_call_count


# # def test_verify_content__skip_file_on_first_read_error(create_dir, create_torrent_file, forced_piece_size):
# #     with forced_piece_size(1024) as piece_size:
# #         b_data = create_dir.random_bytes(30*piece_size+4)
# #         content_path = create_dir('content',
# #                                   ('a', 1*piece_size+3),
# #                                   ('b', b_data),
# #                                   ('c', 20*piece_size+5))
# #         with create_torrent_file(path=content_path) as torrent_file:
# #             torrent = torf.Torrent.read(torrent_file)

# #             # Create one corruption at the beginning to trigger the skipping and
# #             # another corruption in the last piece so that the first piece of
# #             # "c" is also corrupt.
# #             os.rename(content_path / 'b', content_path / 'b.orig')
# #             assert not os.path.exists(content_path / 'b')

# #             log.debug('################ TEST WITHOUT CALLBACK ##################')
# #             with pytest.raises(torf.ReadError) as excinfo:
# #                 torrent.verify(content_path, skip_file_on_first_error=True)
# #             assert str(excinfo.value) == f'{content_path / "b"}: No such file or directory'

# #             log.debug('################ TEST WITH CALLBACK ##################')
# #             cb = CollectingCallback(torrent)
# #             assert torrent.verify(content_path, skip_file_on_first_error=True, callback=cb, interval=0) == False
# #             log.debug(f'good pieces: {dict(cb.good_pieces)}')
# #             log.debug(f'corrupt pieces: {dict(cb.corrupt_pieces)}')
# #             log.debug(f'skipped pieces: {dict(cb.skipped_pieces)}')

# #             assert cb.good_pieces['a'] == [0]
# #             assert cb.good_pieces['b'] == []
# #             assert cb.good_pieces['c'] == list(range(32, 52))
# #             assert cb.corrupt_pieces['a'] == []
# #             assert cb.corrupt_pieces['b'] == [(1, f'{content_path / "b"}: No such file or directory')]
# #             assert cb.corrupt_pieces['c'] == [(31, (f'Corruption in piece 32, at least one of these files is corrupt: '
# #                                                     f'{content_path / "b"}, {content_path / "c"}'))]
# #             assert cb.skipped_pieces['a'] == []
# #             assert cb.skipped_pieces['b'] == list(range(1, 31))
# #             assert cb.skipped_pieces['c'] == []


# # def test_verify_content__skip_file_on_first_file_size_error(create_dir, create_torrent_file, forced_piece_size):
# #     with forced_piece_size(1024) as piece_size:
# #         b_data = create_dir.random_bytes(30*piece_size+4)
# #         content_path = create_dir('content',
# #                                   ('a', 1*piece_size+3),
# #                                   ('b', b_data),
# #                                   ('c', 20*piece_size+5))
# #         with create_torrent_file(path=content_path) as torrent_file:
# #             torrent = torf.Torrent.read(torrent_file)

# #             # Provoke VerifyFileSizeError
# #             (content_path / 'b').write_bytes(b'nah')
# #             assert os.path.getsize(content_path / 'b') != len(b_data)

# #             log.debug('################ TEST WITHOUT CALLBACK ##################')
# #             with pytest.raises(torf.VerifyFileSizeError) as excinfo:
# #                 torrent.verify(content_path, skip_file_on_first_error=True)
# #             assert str(excinfo.value) == f'{content_path / "b"}: Too small: 3 instead of 30724 bytes'

# #             log.debug('################ TEST WITH CALLBACK ##################')
# #             cb = CollectingCallback(torrent)
# #             assert torrent.verify(content_path, skip_file_on_first_error=True, callback=cb, interval=0) == False
# #             log.debug(f'good pieces: {dict(cb.good_pieces)}')
# #             log.debug(f'corrupt pieces: {dict(cb.corrupt_pieces)}')
# #             log.debug(f'skipped pieces: {dict(cb.skipped_pieces)}')

# #             assert cb.good_pieces['a'] == [0]
# #             assert cb.good_pieces['b'] == []
# #             assert cb.good_pieces['c'] == list(range(32, 52))
# #             assert cb.corrupt_pieces['a'] == []
# #             assert cb.corrupt_pieces['b'] == [(1, f'{content_path / "b"}: Too small: 3 instead of 30724 bytes')]
# #             assert cb.corrupt_pieces['c'] == [(31, (f'Corruption in piece 32, at least one of these files is corrupt: '
# #                                                     f'{content_path / "b"}, {content_path / "c"}'))]
# #             assert cb.skipped_pieces['a'] == []
# #             assert cb.skipped_pieces['b'] == list(range(1, 31))
# #             assert cb.skipped_pieces['c'] == []


# # def test_verify_content__skip_file_on_first_hash_mismatch(create_dir, create_torrent_file, forced_piece_size):
# #     with forced_piece_size(1024) as piece_size:
# #         b_data = create_dir.random_bytes(30*piece_size+4)
# #         content_path = create_dir('content',
# #                                   ('a', 1*piece_size+3),
# #                                   ('b', b_data),
# #                                   ('c', 20*piece_size+5))
# #         with create_torrent_file(path=content_path) as torrent_file:
# #             torrent = torf.Torrent.read(torrent_file)

# #             # Corrupt multiple pieces
# #             b_data_corrupt = bytearray(b_data)
# #             b_data_len = len(b_data)
# #             for pos in (3*piece_size, 10*piece_size, b_data_len-2):
# #                 b_data_corrupt[pos] = (b_data_corrupt[pos] + 1) % 256
# #             assert b_data_corrupt != b_data
# #             (content_path / 'b').write_bytes(b_data_corrupt)

# #             log.debug('################ TEST WITHOUT CALLBACK ##################')
# #             with pytest.raises(torf.VerifyContentError) as excinfo:
# #                 torrent.verify(content_path, skip_file_on_first_error=True)
# #             assert str(excinfo.value) == f'Corruption in piece 5 in {content_path / "b"}'

# #             log.debug('################ TEST WITH CALLBACK ##################')
# #             cb = CollectingCallback(torrent)
# #             assert torrent.verify(content_path, skip_file_on_first_error=True,
# #                                   callback=cb, interval=0) == False
# #             log.debug(f'good pieces: {dict(cb.good_pieces)}')
# #             log.debug(f'corrupt pieces: {dict(cb.corrupt_pieces)}')
# #             log.debug(f'skipped pieces: {dict(cb.skipped_pieces)}')

# #             assert cb.good_pieces['a'] == [0]
# #             assert cb.good_pieces['b'] == [1, 2, 3]
# #             assert cb.good_pieces['c'] == list(range(32, 52))
# #             assert cb.corrupt_pieces['a'] == []
# #             assert cb.corrupt_pieces['b'] == [(4, f'Corruption in piece 5 in {content_path / "b"}')]
# #             assert cb.corrupt_pieces['c'] == [(31, (f'Corruption in piece 32, at least one of these files is corrupt: '
# #                                                     f'{content_path / "b"}, {content_path / "c"}'))]
# #             assert cb.skipped_pieces['a'] == []
# #             assert cb.skipped_pieces['b'] == list(range(5, 31))
# #             assert cb.skipped_pieces['c'] == []


# # def test_verify_content__torrent_contains_file_and_path_is_dir(forced_piece_size,
# #                                                                create_file, create_dir, create_torrent_file):
# #     with forced_piece_size(8) as piece_size:
# #         content_path = create_file('content', create_file.random_size(piece_size))
# #         with create_torrent_file(path=content_path) as torrent_file:
# #             torrent = torf.Torrent.read(torrent_file)

# #             os.remove(content_path)
# #             new_content_path = create_dir('content',
# #                                           ('a', create_dir.random_size(piece_size)),
# #                                           ('b', create_dir.random_size(piece_size)))
# #             assert os.path.isdir(content_path)

# #             log.debug('################ TEST WITHOUT CALLBACK ##################')
# #             with pytest.raises(torf.VerifyNotDirectoryError) as excinfo:
# #                 torrent.verify(content_path)
# #             assert str(excinfo.value) == f'{content_path}: Is a directory'

# #             log.debug('################ TEST WITH CALLBACK ##################')
# #             def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
# #                 assert t == torrent
# #                 assert pieces_done == 0
# #                 assert pieces_total == torrent.pieces
# #                 assert piece_index == 0
# #                 assert piece_hash is None
# #                 assert str(path) == str(content_path)
# #                 assert str(exc) == f'{content_path}: Is a directory'
# #             cb = mock.Mock(side_effect=assert_call)
# #             assert torrent.verify(content_path, callback=cb, interval=0) == False
# #             assert cb.call_count == 1


# # def test_verify_content__torrent_contains_dir_and_path_is_file(forced_piece_size,
# #                                                                create_file, create_dir, create_torrent_file):
# #     with forced_piece_size(8) as piece_size:
# #         content_path = create_dir('content',
# #                                   ('a', create_dir.random_size()),
# #                                   ('b', create_dir.random_size()))
# #         with create_torrent_file(path=content_path) as torrent_file:
# #             torrent = torf.Torrent.read(torrent_file)

# #             shutil.rmtree(content_path)
# #             new_content_path = create_file('content', create_file.random_size())
# #             assert os.path.isfile(content_path)

# #             log.debug('################ TEST WITHOUT CALLBACK ##################')
# #             with pytest.raises(torf.VerifyIsDirectoryError) as excinfo:
# #                 torrent.verify(content_path)
# #             assert str(excinfo.value) == f'{content_path}: Not a directory'

# #             log.debug('################ TEST WITH CALLBACK ##################')
# #             def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
# #                 assert t == torrent
# #                 assert pieces_done == 0
# #                 assert pieces_total == torrent.pieces
# #                 assert piece_index == 0
# #                 assert piece_hash is None
# #                 assert str(path) == str(content_path / 'a')
# #                 assert str(exc) == f'{content_path}: Not a directory'
# #             cb = mock.Mock(side_effect=assert_call)
# #             assert torrent.verify(content_path, callback=cb, interval=0) == False
# #             assert cb.call_count == 1


# # def test_verify_content__parent_path_is_unreadable(file_size_a, file_size_b, piece_size,
# #                                                    create_dir, forced_piece_size, create_torrent_file):
# #     with forced_piece_size(piece_size):
# #         content_path = create_dir('content',
# #                                   ('readable/x/a', file_size_a),
# #                                   ('unreadable/x/b', file_size_b))
# #         with create_torrent_file(path=content_path) as torrent_file:
# #             torrent = torf.Torrent.read(torrent_file)
# #             unreadable_path_mode = os.stat(content_path / 'unreadable').st_mode
# #             try:
# #                 os.chmod(content_path / 'unreadable', mode=0o222)

# #                 log.debug('################ TEST WITHOUT CALLBACK ##################')
# #                 with pytest.raises(torf.ReadError) as excinfo:
# #                     torrent.verify(content_path)
# #                 assert str(excinfo.value) == f'{content_path / "unreadable/x/b"}: Permission denied'

# #                 log.debug('################ TEST WITH CALLBACK ##################')
# #                 def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
# #                     assert str(path) in (str(content_path / 'readable/x/a'),
# #                                          str(content_path / 'unreadable/x/b'))
# #                     if str(path) == str(content_path / 'readable/x/a'):
# #                         assert type(piece_hash) is bytes and len(piece_hash) == 20
# #                         assert exc is None
# #                     elif str(path) == str(content_path / 'readable/x/a'):
# #                         assert str(exc) == f'{content_path / "unreadable/x/b"}: Permission denied'
# #                         assert piece_hash is None
# #                 cb = mock.Mock(side_effect=assert_call)
# #                 assert torrent.verify(content_path, skip_file_on_first_error=False, callback=cb, interval=0) == False

# #                 # One call for each piece + 1 extra call for the ReadError
# #                 exp_cb_calls = torrent.pieces + 1
# #                 assert cb.call_count == exp_cb_calls
# #             finally:
# #                 os.chmod(content_path / 'unreadable', mode=unreadable_path_mode)


# # def test_verify_content__torrent_contains_dir_and_path_is_file(forced_piece_size,
# #                                                                create_file, create_dir, create_torrent_file):
# #     with forced_piece_size(8) as piece_size:
# #         content_path = create_dir('content',
# #                                   ('a', create_dir.random_size()),
# #                                   ('b', create_dir.random_size()))
# #         with create_torrent_file(path=content_path) as torrent_file:
# #             torrent = torf.Torrent.read(torrent_file)

# #             shutil.rmtree(content_path)
# #             new_content_path = create_file('content', create_file.random_size())
# #             assert os.path.isfile(content_path)

# #             log.debug('################ TEST WITHOUT CALLBACK ##################')
# #             with pytest.raises(torf.VerifyIsDirectoryError) as excinfo:
# #                 torrent.verify(content_path)
# #             assert str(excinfo.value) == f'{content_path}: Not a directory'

# #             log.debug('################ TEST WITH CALLBACK ##################')
# #             def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
# #                 assert t == torrent
# #                 assert pieces_done == 0
# #                 assert pieces_total == torrent.pieces
# #                 assert piece_index == 0
# #                 assert piece_hash is None
# #                 assert str(path) == str(content_path / 'a')
# #                 assert str(exc) == f'{content_path}: Not a directory'
# #             cb = mock.Mock(side_effect=assert_call)
# #             assert torrent.verify(content_path, callback=cb, interval=0) == False
# #             assert cb.call_count == 1

# # def test_verify_content__callback_is_called_at_intervals(forced_piece_size, monkeypatch,
# #                                                          create_file, create_torrent_file):
# #     with forced_piece_size(8) as piece_size:
# #         content_path = create_file('content',
# #                                    create_file.random_size(min_pieces=10, max_pieces=20))
# #         with create_torrent_file(path=content_path) as torrent_file:
# #             torrent = torf.Torrent.read(torrent_file)
# #             monkeypatch.setattr(torf._generate, 'time_monotonic',
# #                                 mock.Mock(side_effect=range(int(1e9))))
# #             pieces_seen = []
# #             def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
# #                 pieces_seen.append(piece_index)
# #             cb = mock.Mock(side_effect=assert_call)
# #             assert torrent.verify(content_path, callback=cb, interval=2) == True
# #             assert cb.call_count == torrent.pieces // 2 + 1


# # def test_verify_content__last_callback_call_is_never_skipped_when_succeeding(forced_piece_size, monkeypatch,
# #                                                                              create_dir, create_torrent_file):
# #     with forced_piece_size(8) as piece_size:
# #         b_data = create_dir.random_bytes(create_dir.random_size(min_pieces=5))
# #         content_path = create_dir('content',
# #                                   ('a', create_dir.random_size()),
# #                                   ('b', b_data),
# #                                   ('c', create_dir.random_size()))
# #         with create_torrent_file(path=content_path) as torrent_file:
# #             torrent = torf.Torrent.read(torrent_file)

# #             monkeypatch.setattr(torf._generate, 'time_monotonic',
# #                                 mock.Mock(side_effect=range(int(1e9))))

# #             progresses = []
# #             def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
# #                 progresses.append((pieces_done, pieces_total))
# #             cb = mock.Mock(side_effect=assert_call)
# #             assert torrent.verify(content_path, callback=cb, interval=2, skip_file_on_first_error=True) == True
# #             print(progresses)
# #             assert progresses[-1] == (torrent.pieces, torrent.pieces)


# # def test_verify_content__last_callback_call_is_never_skipped_when_failing(forced_piece_size, monkeypatch,
# #                                                                           create_dir, create_torrent_file):
# #     with forced_piece_size(8) as piece_size:
# #         b_data = create_dir.random_bytes(create_dir.random_size(min_pieces=5))
# #         content_path = create_dir('content',
# #                                   ('a', create_dir.random_size()),
# #                                   ('b', b_data),
# #                                   ('c', create_dir.random_size()))
# #         with create_torrent_file(path=content_path) as torrent_file:
# #             torrent = torf.Torrent.read(torrent_file)

# #             b_data_corrupt = bytearray(b_data)
# #             b_data_corrupt[piece_size:piece_size] = b'foo'
# #             assert b_data_corrupt != b_data
# #             (content_path / 'b').write_bytes(b_data_corrupt)

# #             monkeypatch.setattr(torf._generate, 'time_monotonic',
# #                                 mock.Mock(side_effect=range(int(1e9))))

# #             progresses = []
# #             def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
# #                 print(path, pieces_done, pieces_total, piece_index, piece_hash, exc)
# #                 progresses.append((pieces_done, pieces_total))
# #             cb = mock.Mock(side_effect=assert_call)
# #             assert torrent.verify(content_path, callback=cb, interval=2, skip_file_on_first_error=True) == False
# #             print(progresses)
# #             assert progresses[-1] == (torrent.pieces, torrent.pieces)


# # def test_verify_content__callback_interval_is_ignored_when_error_occurs(forced_piece_size, monkeypatch,
# #                                                                         create_file, create_torrent_file):
# #     with forced_piece_size(8) as piece_size:
# #         data = create_file.random_bytes(9*piece_size)
# #         content_path = create_file('content', data)
# #         with create_torrent_file(path=content_path) as torrent_file:
# #             torrent = torf.Torrent.read(torrent_file)

# #             # Corrupt consecutive pieces
# #             errpos = (4, 5, 6)
# #             data_corrupt = bytearray(data)
# #             data_corrupt[piece_size*errpos[0]] = (data[piece_size*errpos[0]] + 1) % 256
# #             data_corrupt[piece_size*errpos[1]] = (data[piece_size*errpos[0]] + 1) % 256
# #             data_corrupt[piece_size*errpos[2]] = (data[piece_size*errpos[2]] + 1) % 256
# #             assert len(data_corrupt) == len(data)
# #             assert data_corrupt != data
# #             content_path.write_bytes(data_corrupt)

# #             monkeypatch.setattr(torf._generate, 'time_monotonic',
# #                                 mock.Mock(side_effect=range(int(1e9))))

# #             progresses = []
# #             def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
# #                 print(path, pieces_done, pieces_total, piece_index, piece_hash, exc)
# #                 progresses.append((pieces_done, pieces_total))
# #             cb = mock.Mock(side_effect=assert_call)
# #             assert torrent.verify(content_path, callback=cb, interval=3, skip_file_on_first_error=False) == False
# #             assert progresses == [(1, 9), (4, 9), (5, 9), (6, 9), (7, 9), (9, 9)]


# # # def test_callback_raises_exception(forced_piece_size, monkeypatch,
# # #                                    create_file, create_torrent_file)
# # #     content = tmpdir.join('file.jpg')
# # #     content.write_binary(os.urandom(5*torf.Torrent.piece_size_min))

# # #     with create_torrent_file(path=content) as torrent_file:
# # #         torrent = torf.Torrent.read(torrent_file)

# # #         cb = mock.MagicMock()
# # #         def assert_call(t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
# # #             if cb.call_count == 3:
# # #                 raise RuntimeError("I'm off")
# # #         cb.side_effect = assert_call
# # #         with pytest.raises(RuntimeError) as excinfo:
# # #             torrent.verify(content, skip_file_on_first_error=False, callback=cb)
# # #         assert excinfo.match(f"^I'm off$")
# # #         assert cb.call_count == 3
