import collections
import itertools
import logging
import os
import random

import torf

debug = logging.getLogger('test').debug

def display_filespecs(filespecs, piece_size):
    filecount = len(filespecs)
    header = ['.' + ' ' * (((4 * filecount) + (2 * filecount - 1)) + 2 - 1)]
    for i in range(8):
        header.append(str(i) + ' ' * (piece_size - 1))
    line = (', '.join(f'{fn}:{fs:2d}' for fn,fs in filespecs),
            ' - ',
            ''.join(fn * fs for fn,fs in filespecs))
    debug(f'\n{"".join(header)}\n{"".join(line)}')

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

    def __eq__(self, other, _real_cls=type(exc)):
        return isinstance(other, (type(self), _real_cls)) and str(self) == str(other)

    def __hash__(self):
        return hash(str(self))

    attrs = {}
    attrs['__eq__'] = __eq__
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
    diff_range = list(range(-original_size, original_size + 1))
    diff_range.remove(0)
    diff = random.choice(diff_range)
    data = bytearray(open(filepath, 'rb').read())
    debug(f'  Original data ({len(data)} bytes): {data}')
    if diff > 0:
        # Make add `diff` bytes at `pos`
        pos = random.choice(range(original_size + 1))
        data[pos:pos] = b'\xA0' * diff
    elif diff < 0:
        # Remove `abs(diff)` bytes at `pos`
        pos = random.choice(range(original_size - abs(diff) + 1))
        data[pos : pos + abs(diff)] = ()
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
        last_piece_index_pos_end = (last_piece_index + 1) * piece_size - 1
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

def calc_piece_indexes(filespecs, piece_size, files_missing=(), files_missized=()):
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
        if i < len(filespecs) - 1:
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
    debug('Calculating good pieces')
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
        missing_pis.update(range(first_missing_pi, last_missing_pi + 1))

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
                skipped_pis.update(file_pis[first_corr_index_in_file + 1:])
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
    return fuzzylist(*corrupt_pieces)

def skip_corruptions(all_corruptions, filespecs, piece_size, corruption_positions, files_missing, files_missized):
    """Make every non-first corruption optional"""
    debug(f'Skipping corruptions: {all_corruptions}')
    pis_seen = set()
    files_seen = set()
    corruptions = fuzzylist()
    files_autoskipped = set(str(f) for f in itertools.chain(files_missing, files_missized))
    debug(f'  missing or missized: {files_autoskipped}')
    for exc in all_corruptions:
        # Corruptions for files we haven't seen yet must be reported
        if any(f not in files_seen and f not in files_autoskipped
               for f in exc.files):
            debug(f'  mandatory: {exc}')
            files_seen.update(exc.files)
            pis_seen.add(exc.piece_index)
            corruptions.append(exc)
        # Corruptions for files we already have seen may still be reported
        # because skipping is racy and it's impossible to predict how many
        # pieces are processed before the skip manifests.
        else:
            debug(f'  optional: {exc}')
            corruptions.maybe.append(exc)
            pis_seen.add(exc.piece_index)

    # Because we fake skipped files, their last piece is reported as corrupt if
    # it contains bytes from the next file even if there is no corruption in the
    # skipped file's last piece.  But this is not guaranteed because it's
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
        if exc not in itertools.chain(corruptions, corruptions.maybe):
            debug(f'Adding possible exception for last affected file {affected_files[-1]}: {exc}')
            corruptions.maybe.append(exc)

    return corruptions

def calc_pieces_done(filespecs_abspath, piece_size, files_missing=(), files_missized=()):
    debug('Calculating pieces_done')
    # The callback gets the number of verified pieces (pieces_done).  This
    # function calculates the expected values for that argument.
    #
    # It's not as simple as range(1, <number of pieces>+1).  For example, if a
    # file is missing, we get the same pieces_done value two times, once for "No
    # such file" and maybe again for "Corrupt piece" if the piece contains parts
    # of another file.

    # Every pieces_done value is reported at least once
    total_size = sum(filesize for _,filesize in filespecs_abspath)
    pieces_done_list = list((pi // piece_size) + 1
                            for pi in range(0, total_size, piece_size))
    debug(f'  progress reports: {pieces_done_list}')
    # List of pieces_done values that may appear multiple times
    maybes = set()
    # Map pieces_done values to the number of times they may appear
    max_maybe_items = collections.defaultdict(lambda: 1)

    # Missing or missized files are reported in addition to progress reports
    files_missing = {str(filepath) for filepath in files_missing}
    debug(f'  files_missing: {files_missing}')
    files_missized = {str(filepath) for filepath in files_missized}
    debug(f'  files_missized: {files_missized}')
    for filepath in files_missing.union(files_missized):
        # Because we're multithreaded, we can't expect the missing/missized file
        # to be reported at its first piece.  We can't predict at all when the
        # error is reported.  The only thing we can savely say that for each
        # missing/missized file, every pieces_done_value *may* increase by 1.
        for pieces_done_value in pieces_done_list:
            maybes.add(pieces_done_value)
            max_maybe_items[pieces_done_value] += 1

    fuzzy_pieces_done_list = fuzzylist(*pieces_done_list,
                                       maybe=sorted(maybes),
                                       max_maybe_items=max_maybe_items)
    return fuzzy_pieces_done_list
