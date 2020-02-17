import torf

import pytest
from unittest import mock
import os
import random
import collections
import itertools
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
    """Return list of 1 to 5 random indexes in `stream`"""
    positions = random.sample(range(len(stream)), k=min(len(stream), 5))
    return sorted(positions[:random.randint(1, len(positions))])

def change_file_size(filepath, original_size):
    """Randomly change size of `filepath` on disk and return new contents"""
    diff_range = list(range(-original_size, original_size+1))
    diff_range.remove(0)
    diff = random.choice(diff_range)
    data = open(filepath, 'rb').read()
    debug(f'  Original data ({len(data)} bytes): {data}')
    if diff > 0:
        # Make file longer
        if random.choice((1, 0)):
            # Insert at beginning of file
            data = b'\xA0' * diff + data
        else:
            # Insert at end of file
            data = data + b'\xA0' * diff
    elif diff < 0:
        if random.choice((1, 0)):
            # Remove bytes from beginning of file
            data = data[abs(diff):]
        else:
            # Remove bytes from end of file
            data = data[:diff]
    with open(filepath, 'wb') as f:
        f.write(data)
        f.truncate()
    assert os.path.getsize(filepath) == original_size + diff
    debug(f'  Changed data ({len(data)} bytes): {data}')
    with open(filepath, 'rb') as f:
        return f.read()

def round_up_to_multiple(n, x):
    """Round `n` up to the next multiple of `x`"""
    return n - n % -x

def round_down_to_multiple(n, x):
    """Round `n` down to the previous multiple of `x`"""
    if n % x != 0:
        return round_up_to_multiple(n, x) - x
    else:
        return n

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

def calc_piece_indexes(filespecs, piece_size, files_missing, files_missized):
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

    # For each missing/missized file, the first piece of the file may get two
    # calls, one for the "no such file"/"wrong file size" error and one for the
    # "corrupt piece" error.
    for filepath in itertools.chain(files_missing, files_missized):
        filename = os.path.basename(filepath)
        file_beg,file_end = file_range(filename, filespecs)
        piece_index = file_beg // piece_size
        piece_indexes[filename].maybe.append(piece_index)

    # Remove empty lists, which we added to maintain file order
    for k in tuple(piece_indexes):
        if not piece_indexes[k]:
            del piece_indexes[k]

    return piece_indexes

def calc_good_pieces(filespecs, piece_size, files_missing, corruption_positions, files_missized):
    """
    Same as `calc_piece_indexes`, but exclude corrupt pieces and pieces of
    missing or missized files
    """
    corr_pis = {corrpos // piece_size for corrpos in corruption_positions}
    debug(f'Calculating good pieces')
    debug(f'corrupt piece_indexes: {corr_pis}')
    debug(f'missing files: {files_missing}')
    debug(f'missized files: {files_missized}')

    # Find pieces that exclusively belong to missing or missized files
    missing_pis = set()
    for filepath in itertools.chain(files_missing, files_missized):
        file_beg,file_end = file_range(os.path.basename(filepath), filespecs)
        first_missing_pi = file_beg // piece_size
        last_missing_pi = file_end // piece_size
        affected_files_beg = pos2files(file_beg, filespecs, piece_size)
        affected_files_end = pos2files(file_end, filespecs, piece_size)
        debug(f'affected_files_beg: {affected_files_beg}')
        debug(f'affected_files_end: {affected_files_end}')
        missing_pis.update(range(first_missing_pi, last_missing_pi+1))

    all_piece_indexes = calc_piece_indexes(filespecs, piece_size, files_missing, files_missized)
    debug(f'all piece_indexes: {all_piece_indexes}')

    # Remove pieces that are either corrupt or in missing_pis
    good_pieces = collections.defaultdict(lambda: fuzzylist())
    for fname,all_pis in all_piece_indexes.items():
        for i,pi in enumerate(all_pis):
            if pi not in corr_pis and pi not in missing_pis:
                good_pieces[fname].append(pi)
    debug(f'corruptions and missing/missized files removed: {good_pieces}')
    return good_pieces

def skip_good_pieces(good_pieces, filespecs, piece_size, corruption_positions):
    """
    For each file in `good_pieces`, remove piece_indexes between the first
    corruption and the end of the file
    """
    # Find out which piece_indexes to skip
    skipped_pis = set()
    for corrpos in sorted(corruption_positions):
        corr_pi = corrpos // piece_size
        affected_files = pos2files(corrpos, filespecs, piece_size)
        debug(f'corruption at position {corrpos}, piece_index {corr_pi}: {affected_files}')
        for file in affected_files:
            file_pis_exclusive = file_piece_indexes(file, filespecs, piece_size, exclusive=True)
            debug(f'  {file} piece_indexes exclusive: {file_pis_exclusive}')
            file_pis = file_piece_indexes(file, filespecs, piece_size, exclusive=False)
            debug(f'  {file} piece_indexes non-exclusive: {file_pis}')
            try:
                first_corr_index_in_file = file_pis.index(corr_pi)
            except ValueError:
                # Skip all pieces in `file` that don't contain bytes from other files
                debug(f'  piece_index {corr_pi} is not part of {file}: {file_pis_exclusive}')
                skipped_pis.update(file_pis_exclusive)
            else:
                # Skip all pieces after the first corrupted piece in `file`
                debug(f'  first corruption in {file} is at {first_corr_index_in_file} in file {file}')
                skipped_pis.update(file_pis[first_corr_index_in_file+1:])
            debug(f'updated skipped_pis: {skipped_pis}')

    # Make skipped piece_indexes optional while unskipped piece_indexes stay
    # mandatory.
    debug(f'skipping piece_indexes: {skipped_pis}')
    good_pieces_skipped = collections.defaultdict(lambda: fuzzylist())
    for fname,pis in good_pieces.items():
        for pi in pis:
            if pi in skipped_pis:
                good_pieces_skipped[fname].maybe.append(pi)
            else:
                good_pieces_skipped[fname].append(pi)
    debug(f'skipped good_pieces: {good_pieces_skipped}')
    return fuzzydict(good_pieces_skipped)

def calc_corruptions(filespecs, piece_size, corruption_positions):
    """Map file names to (piece_index, exception) tuples"""
    corrupt_pieces = []
    reported = []
    for corrpos in sorted(corruption_positions):
        corr_pi = corrpos // piece_size
        if corr_pi not in reported:
            exc = ComparableException(torf.VerifyContentError(corr_pi, piece_size, filespecs))
            corrupt_pieces.append(exc)
            reported.append(corr_pi)
    return corrupt_pieces

def skip_corruptions(all_corruptions, filespecs, piece_size, corruption_positions):
    """Make every non-first corruption optional"""
    debug(f'Skipping corruptions: {all_corruptions}')
    pis_seen = set()
    files_seen = set()
    corruptions = fuzzylist()
    for exc in all_corruptions:
        # Corruptions for files we haven't seen yet must be reported
        if any(f not in files_seen for f in exc.files):
            debug(f'mandatory: {exc}')
            files_seen.update(exc.files)
            pis_seen.add(exc.piece_index)
            corruptions.append(exc)
        # Corruptions for files we already have seen may still be reported
        # because skipping is racy and it's impossible to predict how many
        # pieces are processed before the skip manifests.
        else:
            debug(f'optional: {exc}')
            corruptions.maybe.append(exc)
            pis_seen.add(exc.piece_index)

    # Because we fake skipped files, their last piece is be reported as corrupt
    # if it contains bytes from the next file even if there is no corruption in
    # the skipped file's last piece.  But this is not guaranteed because it's
    # possible the corrupt file is fully processed before its corruption is
    # noticed.
    for corrpos in corruption_positions:
        # Find all files that are affected by the corruption
        affected_files = pos2files(corrpos, filespecs, piece_size)
        debug(f'  affected_files: {affected_files}')
        # Find piece_index of the end of the last affected file
        _,file_end = file_range(affected_files[-1], filespecs)
        piece_index = file_end // piece_size
        debug(f'  {affected_files[-1]} ends at piece_index {piece_index}')
        # Add optional exception for that piece
        exc = ComparableException(torf.VerifyContentError(piece_index, piece_size, filespecs))
        debug(f'Adding possible exception for last affected file {affected_files[-1]}: {exc}')
        corruptions.maybe.append(exc)

    return corruptions

def calc_pieces_done(filespecs_abspath, piece_size, files_missing, files_missized):
    debug(f'Calculating pieces_done')
    # The callback gets the number of verified pieces (pieces_done).  This
    # function calculates the expected values for that argument.
    #
    # It's not as simple as range(1, <number of pieces>+1).  For example, if a
    # file is missing, we get the same pieces_done value two times, once for "No
    # such file" and maybe again for "Corrupt piece" if the piece contains parts
    # of another file.
    files_missing = {str(filepath) for filepath in files_missing}
    debug(f'files_missing: {files_missing}')
    files_missized = {str(filepath) for filepath in files_missized}
    debug(f'files_missized: {files_missized}')
    # List of pieces_done values that are reported at least once
    pieces_done_list = []
    # List of pieces_done values that may appear multiple times
    maybes = set()
    # Map pieces_done values to the number of times they may appear
    max_maybe_items = collections.defaultdict(lambda: 1)
    pos = 0
    bytes_left = sum(filesize for _,filesize in filespecs_abspath)
    total_size = bytes_left
    calc_pd = lambda pos: (pos // piece_size) + 1   # pieces_done
    debug(f'{bytes_left} bytes left')
    prev_pi = -1
    # Iterate over each piece
    while bytes_left > 0:
        current_pi = pos // piece_size
        debug(f'{pos}: pi={current_pi}')

        # Report normal progress (errors are additional)
        if current_pi != prev_pi:
            debug(f'  . progress: {calc_pd(pos)}')
            pieces_done_list.append(calc_pd(pos))

        # Find all files that begin in this piece
        all_files = pos2files(pos, filespecs_abspath, piece_size)
        debug(f'  ? all files: {all_files}')
        files_beg = [f for f in all_files
                     if file_range(f, filespecs_abspath)[0] // piece_size == current_pi]
        debug(f'  ? files beginning: {files_beg}')

        # Each file that begins in current_pi and is missing or missized may be
        # reported once again anywhere between now and the final piece.
        for f in files_beg:
            if f in files_missing or f in files_missized:
                debug(f'  ! missing or missized: {f}')
                # Because we're working in multiple threads, the corruption may
                # be reported anywhere from the missing/missized file's first
                # piece to the final piece in the stream.
                for pieces_done in range(calc_pd(pos), calc_pd(total_size-1)+1):
                    maybes.add(pieces_done)
                    max_maybe_items[pieces_done] += 1
                debug(f'    + optional: {pieces_done} * {max_maybe_items[pieces_done]}')
            # Don't report the same missing file again
            if f in files_missing: files_missing.remove(f)
            if f in files_missized: files_missized.remove(f)

        _,last_file_end = file_range(all_files[-1], filespecs_abspath)
        debug(f'  bytes_done = min({piece_size}, {last_file_end} - {pos} + 1)')
        bytes_done = min(piece_size, last_file_end - pos + 1)
        bytes_left -= bytes_done
        pos += bytes_done
        debug(f'  {bytes_done} bytes done, {bytes_left} bytes left')
        prev_pi = current_pi

    fuzzy_pieces_done_list = fuzzylist(*pieces_done_list,
                                       maybe=sorted(maybes),
                                       max_maybe_items=max_maybe_items)
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

    def reset(self):
        self.corruption_positions = set()
        self.files_missing = []
        self.files_missized = []
        for attr in ('_exp_exceptions', '_exp_pieces_done',
                     '_exp_piece_indexes', '_exp_good_pieces',
                     '_exp_exc_corruptions', '_exp_exc_files_missing', '_exp_exc_files_missized'):
            if hasattr(self, attr):
                delattr(self, attr)

    def run(self, *_, with_callback, exp_return_value=None, skip_file_on_first_error=False):
        debug(f'Original stream: {self.stream_original.hex()}')
        debug(f' Corrupt stream: {self.stream_corrupt.hex()}')
        debug(f'Corruption positions: {self.corruption_positions}')
        debug(f'Corrupt piece indexes: {set(corrpos // self.piece_size for corrpos in self.corruption_positions)}')

        self.skip_file_on_first_error = skip_file_on_first_error
        kwargs = {'skip_file_on_first_error': skip_file_on_first_error,
                  'exp_return_value': exp_return_value}
        if not with_callback:
            exp_exceptions = self.exp_exceptions
            if not exp_exceptions:
                self._run_without_callback(**kwargs)
            else:
                exp_exception_types = tuple(set(type(exc) for exc in exp_exceptions))
                with pytest.raises(exp_exception_types) as e:
                    self._run_without_callback(**kwargs)
                # Usually the first error in the stream is reported, but not
                # always, so we expect one of the possible exceptions to be
                # raised.
                assert e.value in exp_exceptions
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
        # is finalized correctly, e.g. progress bar ends at 100%
        assert cb.seen_pieces_done[-1] == self.torrent.pieces

        debug(f'seen_exceptions: {cb.seen_exceptions}')
        assert cb.seen_exceptions == self.exp_exceptions
        debug(f'seen_piece_indexes: {cb.seen_piece_indexes}')
        assert cb.seen_piece_indexes == self.exp_piece_indexes
        debug(f'seen_pieces_done: {cb.seen_pieces_done}')
        assert cb.seen_pieces_done == self.exp_pieces_done
        debug(f'seen_good_pieces: {cb.seen_good_pieces}')
        assert cb.seen_good_pieces == self.exp_good_pieces

    @property
    def exp_pieces_done(self):
        if not hasattr(self, '_exp_pieces_done'):
            self._exp_pieces_done = calc_pieces_done(self.filespecs_abspath, self.piece_size,
                                                     self.files_missing, self.files_missized)
            debug(f'Expected pieces done: {self._exp_pieces_done}')
        return self._exp_pieces_done

    @property
    def exp_piece_indexes(self):
        if not hasattr(self, '_exp_piece_indexes'):
            self._exp_piece_indexes = calc_piece_indexes(self.filespecs, self.piece_size,
                                                         self.files_missing, self.files_missized)
            debug(f'Expected piece indexes: {dict(self._exp_piece_indexes)}')
        return dict(self._exp_piece_indexes)

    @property
    def exp_good_pieces(self):
        if not hasattr(self, '_exp_good_pieces'):
            self._exp_good_pieces = calc_good_pieces(self.filespecs, self.piece_size, self.files_missing,
                                                     self.corruption_positions, self.files_missized)
            if self.skip_file_on_first_error:
                self._exp_good_pieces = skip_good_pieces(self._exp_good_pieces, self.filespecs, self.piece_size,
                                                         self.corruption_positions)
            debug(f'Expected good pieces: {self._exp_good_pieces}')
        return self._exp_good_pieces

    @property
    def exp_exc_corruptions(self):
        if not hasattr(self, '_exp_exc_corruptions'):
            self._exp_exc_corruptions = calc_corruptions(self.filespecs_abspath, self.piece_size, self.corruption_positions)
            if self.skip_file_on_first_error:
                self._exp_exc_corruptions = skip_corruptions(self._exp_exc_corruptions, self.filespecs_abspath,
                                                             self.piece_size, self.corruption_positions)
            debug(f'Expected corruptions: {self._exp_exc_corruptions}')
        return self._exp_exc_corruptions

    @property
    def exp_exc_files_missing(self):
        if not hasattr(self, '_exp_exc_files_missing'):
            self._exp_exc_files_missing = fuzzylist(*(ComparableException(torf.ReadError(errno.ENOENT, filepath))
                                                      for filepath in self.files_missing))
            debug(f'Expected files missing: {self._exp_exc_files_missing}')
        return self._exp_exc_files_missing

    @property
    def exp_exc_files_missized(self):
        if not hasattr(self, '_exp_exc_files_missized'):
            def mkexc(filepath):
                fsize_orig = self.get_original_filesize(filepath)
                fsize_actual = self.get_actual_filesize(filepath)
                return ComparableException(torf.VerifyFileSizeError(
                    filepath, actual_size=fsize_actual, expected_size=fsize_orig))
            self._exp_exc_files_missized = fuzzylist(*(mkexc(filepath) for filepath in self.files_missized))
            debug(f'Expected files missized: {self._exp_exc_files_missized}')
        return self._exp_exc_files_missized

    @property
    def exp_exceptions(self):
        if not hasattr(self, '_exp_exceptions'):
            debug(f'self._exp_exceptions = {self.exp_exc_files_missing!r} + {self.exp_exc_corruptions!r}')
            self._exp_exceptions = (self.exp_exc_files_missing
                                    + self.exp_exc_files_missized
                                    + self.exp_exc_corruptions)
            debug(f'                     = {self._exp_exceptions!r}')
            debug(f'Expected exceptions:')
            for e in self._exp_exceptions:
                debug(repr(e))
            debug(f'Tolerated exceptions:')
            for e in self._exp_exceptions.maybe:
                debug(repr(e))
        return self._exp_exceptions

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

    def change_file_size(self):
        debug(f'Changing file size in file system: {os.path.basename(self.content_path)}')
        self.stream_corrupt = change_file_size(self.content_path, self.torrent.size)
        self.files_missized.append(self.content_path)

    def get_original_filesize(self, filepath):
        return len(self.stream_original)

    def get_actual_filesize(self, filepath):
        return len(self.stream_corrupt)

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
            self.content_original[filename] = data
            self.content_corrupt[filename] = bytearray(data)
            create_dir_args.append((filename, data))
        self.content_path = self.create_dir('content', *create_dir_args)
        debug(f'Content: {self.content_original}')
        with self.forced_piece_size(piece_size):
            with self.create_torrent_file(path=self.content_path) as torrent_filepath:
                self.torrent = torf.Torrent.read(torrent_filepath)

    @property
    def stream_original(self):
        return b''.join((data for data in self.content_original.values()))

    @property
    def stream_corrupt(self):
        return b''.join((data for data in self.content_corrupt.values()))

    def corrupt_stream(self, *positions):
        # Introduce random number of corruptions in random files without
        # changing stream length
        corruption_positions = set(random_positions(self.stream_corrupt) if not positions else positions)
        for corrpos_in_stream in corruption_positions:
            filename,corrpos_in_file = pos2file(corrpos_in_stream, self.filespecs, self.piece_size)
            debug(f'Introducing corruption in {filename} at index {corrpos_in_stream} in stream, '
                  f'{corrpos_in_file} in file {filename}')
            data = self.content_corrupt[filename]
            data[corrpos_in_file] = (data[corrpos_in_file] + 1) % 256
            (self.content_path / filename).write_bytes(data)
        self.corruption_positions.update(corruption_positions)

    def delete_file(self, index):
        # Remove file at `index` in filespecs from file system
        filename,filesize = self.filespecs[index]
        debug(f'Removing file from file system: {os.path.basename(filename)}')
        filepath = self.content_path / filename
        os.rename(filepath, str(filepath) + '.deleted')
        self.files_missing.append(filepath)
        self.content_corrupt[os.path.basename(filename)] = b'\xCC' * filesize

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

    def change_file_size(self):
        # Pick random file
        filename = random.choice(tuple(self.content_original))
        filepath = self.content_path / filename
        debug(f'Changing file size in file system: {filepath}')

        # Change file size
        self.content_corrupt[filename] = change_file_size(
            filepath, len(self.content_original[filename]))
        self.files_missized.append(filepath)

        # Check if the beginning of adjacent files will be corrupted
        file_beg,file_end = file_range(filename, self.filespecs)
        debug(f'  Original file beginning and end in stream: {file_beg}, {file_end}')
        if file_beg % self.piece_size != 0:
            debug(f'  Beginning corrupts previous piece: {file_beg // self.piece_size}')
            self.corruption_positions.add(file_beg)

        # Check if the end of adjacent files will be corrupted
        if (file_end + 1) % self.piece_size != 0:
            filepath,_ = pos2file(file_end, self.filespecs_abspath, self.piece_size)
            if (filepath not in self.files_missing and
                filepath not in self.files_missized and
                filepath != self.filespecs_abspath[-1][0]):
                debug(f'  End corrupts next piece: {(file_end + 1) // self.piece_size}')
                self.corruption_positions.add(file_end)

        debug(f'Corruption positions after changing file size: {self.corruption_positions}')

    def get_original_filesize(self, filepath):
        return len(self.content_original[os.path.basename(filepath)])

    def get_actual_filesize(self, filepath):
        return len(self.content_corrupt[os.path.basename(filepath)])

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

def test_verify_content_with_random_corruptions_and_no_skipping(mktestcase, piece_size, callback, filespecs):
    display_filespecs(filespecs, piece_size)
    tc = mktestcase(filespecs, piece_size)
    tc.corrupt_stream()
    cb = tc.run(with_callback=callback['enabled'],
                exp_return_value=False)

def test_verify_content_with_random_corruptions_and_skipping(mktestcase, piece_size, callback, filespecs):
    display_filespecs(filespecs, piece_size)
    tc = mktestcase(filespecs, piece_size)
    tc.corrupt_stream()
    cb = tc.run(with_callback=callback['enabled'],
                skip_file_on_first_error=True,
                exp_return_value=False)

def test_verify_content_with_missing_files_and_no_skipping(mktestcase, piece_size, callback, filespecs, filespec_indexes):
    display_filespecs(filespecs, piece_size)
    tc = mktestcase(filespecs, piece_size)
    for index in filespec_indexes:
        tc.delete_file(index)
    cb = tc.run(with_callback=callback['enabled'],
                exp_return_value=False)

def test_verify_content_with_missing_files_and_skipping(mktestcase, piece_size, callback, filespecs, filespec_indexes):
    display_filespecs(filespecs, piece_size)
    tc = mktestcase(filespecs, piece_size)
    for index in filespec_indexes:
        tc.delete_file(index)
    cb = tc.run(with_callback=callback['enabled'],
                skip_file_on_first_error=True,
                exp_return_value=False)

def test_verify_content_with_changed_file_size_and_no_skipping(mktestcase, piece_size, callback, filespecs):
    display_filespecs(filespecs, piece_size)
    tc = mktestcase(filespecs, piece_size)
    tc.change_file_size()
    cb = tc.run(with_callback=callback['enabled'],
                exp_return_value=False)

def test_verify_content_with_changed_file_size_and_skipping(mktestcase, piece_size, callback, filespecs):
    display_filespecs(filespecs, piece_size)
    tc = mktestcase(filespecs, piece_size)
    tc.change_file_size()
    cb = tc.run(with_callback=callback['enabled'],
                skip_file_on_first_error=True,
                exp_return_value=False)



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
