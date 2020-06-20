import collections
import errno
import itertools
import os
import random
from unittest import mock

import pytest

import torf

from . import *  # noqa: F403

import logging  # isort:skip
debug = logging.getLogger('test').debug

class CollectingCallback():
    """Collect call arguments and make basic assertions"""
    def __init__(self, torrent):
        super().__init__()
        self.torrent = torrent
        self.seen_pieces_done = []
        self._seen_piece_indexes = collections.defaultdict(lambda: fuzzylist())  # noqa: F405
        self._seen_good_pieces = collections.defaultdict(lambda: fuzzylist())  # noqa: F405
        self._seen_skipped_pieces = collections.defaultdict(lambda: fuzzylist())  # noqa: F405
        self.seen_exceptions = fuzzylist()  # noqa: F405

    def __call__(self, t, path, pieces_done, pieces_total, piece_index, piece_hash, exc):
        assert t is self.torrent
        assert pieces_total == t.pieces
        assert 1 <= pieces_done <= pieces_total
        self.seen_pieces_done.append(pieces_done)
        self._seen_piece_indexes[path.name].append(piece_index)
        if exc is not None:
            if isinstance(exc, torf.VerifyContentError):
                assert type(piece_hash) is bytes and len(piece_hash) == 20
            else:
                assert piece_hash is None
            self.seen_exceptions.append(ComparableException(exc))  # noqa: F405
        elif piece_hash is None:
            assert exc is None
            self._seen_skipped_pieces[path.name].append(piece_index)
        else:
            assert exc is None
            assert type(piece_hash) is bytes and len(piece_hash) == 20
            self._seen_good_pieces[path.name].append(piece_index)

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
        self.files_corrupt = []
        self.files_missing = []
        self.files_missized = []
        for attr in ('_exp_exceptions', '_exp_pieces_done',
                     '_exp_piece_indexes', '_exp_good_pieces',
                     '_exp_exc_corruptions', '_exp_exc_files_missing', '_exp_exc_files_missized'):
            if hasattr(self, attr):
                delattr(self, attr)

    def run(self, *_, with_callback, exp_return_value=None, skip_on_error=False):
        debug(f'Original stream: {self.stream_original.hex()}')
        debug(f' Corrupt stream: {self.stream_corrupt.hex()}')
        debug(f'Corruption positions: {self.corruption_positions}')
        debug(f'Corrupt piece indexes: {set(corrpos // self.piece_size for corrpos in self.corruption_positions)}')

        self.skip_on_error = skip_on_error
        kwargs = {'skip_on_error': skip_on_error,
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
            self._exp_pieces_done = calc_pieces_done(self.filespecs_abspath, self.piece_size,  # noqa: F405
                                                     self.files_missing, self.files_missized)
            debug(f'Expected pieces done: {self._exp_pieces_done}')
        return self._exp_pieces_done

    @property
    def exp_piece_indexes(self):
        if not hasattr(self, '_exp_piece_indexes'):
            self._exp_piece_indexes = calc_piece_indexes(self.filespecs, self.piece_size,  # noqa: F405
                                                         self.files_missing, self.files_missized)
            debug(f'Expected piece indexes: {dict(self._exp_piece_indexes)}')
        return dict(self._exp_piece_indexes)

    @property
    def exp_good_pieces(self):
        if not hasattr(self, '_exp_good_pieces'):
            self._exp_good_pieces = calc_good_pieces(self.filespecs,  # noqa: F405
                                                     self.piece_size,
                                                     self.files_missing,
                                                     self.corruption_positions,
                                                     self.files_missized)
            if self.skip_on_error:
                self._exp_good_pieces = skip_good_pieces(self._exp_good_pieces,  # noqa: F405
                                                         self.filespecs,
                                                         self.piece_size,
                                                         self.corruption_positions)
            debug(f'Expected good pieces: {self._exp_good_pieces}')
        return self._exp_good_pieces

    @property
    def exp_exc_corruptions(self):
        if not hasattr(self, '_exp_exc_corruptions'):
            self._exp_exc_corruptions = calc_corruptions(self.filespecs_abspath,  # noqa: F405
                                                         self.piece_size,
                                                         self.corruption_positions)
            if self.skip_on_error:
                self._exp_exc_corruptions = skip_corruptions(self._exp_exc_corruptions, self.filespecs_abspath,  # noqa: F405
                                                             self.piece_size, self.corruption_positions,
                                                             self.files_missing, self.files_missized)
            debug(f'Expected corruptions: {self._exp_exc_corruptions}')
        return self._exp_exc_corruptions

    @property
    def exp_exc_files_missing(self):
        if not hasattr(self, '_exp_exc_files_missing'):
            self._exp_exc_files_missing = fuzzylist(*(ComparableException(torf.ReadError(errno.ENOENT, filepath))  # noqa: F405
                                                      for filepath in self.files_missing))
            debug(f'Expected files missing: {self._exp_exc_files_missing}')
        return self._exp_exc_files_missing

    @property
    def exp_exc_files_missized(self):
        if not hasattr(self, '_exp_exc_files_missized'):
            def mkexc(filepath):
                fsize_orig = self.get_original_filesize(filepath)
                fsize_actual = self.get_actual_filesize(filepath)
                return ComparableException(torf.VerifyFileSizeError(  # noqa: F405
                    filepath, actual_size=fsize_actual, expected_size=fsize_orig))
            self._exp_exc_files_missized = fuzzylist(*(mkexc(filepath) for filepath in self.files_missized))  # noqa: F405
            debug(f'Expected files missized: {self._exp_exc_files_missized}')
        return self._exp_exc_files_missized

    @property
    def exp_exceptions(self):
        if not hasattr(self, '_exp_exceptions'):
            debug('Calculating expected exceptions:')

            # Exceptions that must be reported
            mandatory = set(self.exp_exc_files_missing)
            maybe = set()

            # Files with wrong size must be reported if they are not also missing
            mandatory_files = set(exc.path for exc in mandatory)
            for exc in self.exp_exc_files_missized:
                if exc.filepath not in mandatory_files:
                    mandatory.add(exc)

            # If there are no missing or missized files, corruptions are mandatory
            if not mandatory:
                debug('  all corruption exceptions are mandatory')
                mandatory.update(self.exp_exc_corruptions)
                maybe.update(self.exp_exc_corruptions.maybe)
            else:
                # Corruptions must be reported if they don't exist in missing or
                # missized files.
                for exc in self.exp_exc_corruptions:
                    if any(filepath in itertools.chain(self.files_missing, self.files_missized)
                           for filepath in exc.files):
                        debug(f'  expecting non-missing/missized: {str(exc)}')
                        mandatory.add(exc)
                    elif not all(filepath in itertools.chain(self.files_missing, self.files_missized)
                                 for filepath in exc.files):
                        debug(f'  expecting side-effect: {str(exc)}')
                        mandatory.add(exc)

                # Also allow corruptions that are already classified as optional.
                for exc in self.exp_exc_corruptions.maybe:
                    debug(f'  also allowing {str(exc)}')
                    maybe.add(exc)

            self._exp_exceptions = fuzzylist(*mandatory, maybe=maybe)  # noqa: F405
            debug('Expected exceptions:')
            for e in self._exp_exceptions:
                debug(repr(e))
            debug('Tolerated exceptions:')
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
        # Check if this file already has other errors
        if self.files_missing or self.files_missized:
            return
        # Introduce random number of corruptions without changing stream length
        corruption_positions = set(random_positions(self.stream_corrupt) if not positions else positions)  # noqa: F405
        for corrpos in corruption_positions:
            debug(f'Introducing corruption at index {corrpos}')
            self.stream_corrupt[corrpos] = (self.stream_corrupt[corrpos] + 1) % 256
            self.content_path.write_bytes(self.stream_corrupt)
        self.corruption_positions.update(corruption_positions)

    def delete_file(self, index=None):
        # Check if this file already has other errors
        if self.corruption_positions or self.files_missized:
            return
        debug(f'Removing file from file system: {os.path.basename(self.content_path)}')
        os.rename(self.content_path, str(self.content_path) + '.deleted')
        self.files_missing = [self.content_path]
        self.stream_corrupt = b'\xCC' * self.torrent.size
        # No need to update self.corruption_positions.  A missing single file
        # does not produce any corruption errors because the "No such file"
        # error is enough.

    def change_file_size(self, index=None):
        # Check if this file already has other errors
        if self.corruption_positions or self.files_missing:
            return
        debug(f'Changing file size in file system: {os.path.basename(self.content_path)}')
        self.stream_corrupt = change_file_size(self.content_path, self.torrent.size)  # noqa: F405
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
        # Introduce corruptions without changing stream length
        error_files = set(os.path.basename(f) for f in itertools.chain(
            self.files_missing, self.files_missized))
        corruption_positions = set(random_positions(self.stream_original) if not positions else positions)  # noqa: F405
        for corrpos_in_stream in corruption_positions:
            filename,corrpos_in_file = pos2file(corrpos_in_stream, self.filespecs, self.piece_size)  # noqa: F405
            if filename in error_files:
                continue
            else:
                debug(f'Introducing corruption in {filename} at index {corrpos_in_stream} in stream, '
                      f'{corrpos_in_file} in file {filename}')
                self.corruption_positions.add(corrpos_in_stream)
                data = self.content_corrupt[filename]
                data[corrpos_in_file] = (data[corrpos_in_file] + 1) % 256
                (self.content_path / filename).write_bytes(data)
                self.files_corrupt.append(str(self.content_path / filename))
        debug(f'Corruption positions after corrupting stream: {self.corruption_positions}')

    def delete_file(self, index=None):
        if index is None:
            index = random.choice(range(len(self.filespecs)))
        # Remove file at `index` in filespecs from file system
        filename,filesize = self.filespecs[index]

        # Don't delete corrupt/missing file
        error_files = set(os.path.basename(f) for f in itertools.chain(
            self.files_corrupt, self.files_missized))
        if filename in error_files:
            return

        debug(f'Removing file from file system: {os.path.basename(filename)}')
        filepath = self.content_path / filename
        os.rename(filepath, str(filepath) + '.deleted')
        self.files_missing.append(filepath)
        self.content_corrupt[os.path.basename(filename)] = b'\xCC' * filesize

        # Re-calculate corruptions for adjacent files of all missing files
        corruption_positions = set()
        for removed_filepath in self.files_missing:
            # Find the first byte of the first affected piece and the first byte
            # of the last affected piece and mark them as corrupt
            removed_filename = os.path.basename(removed_filepath)
            file_beg,file_end = file_range(removed_filename, self.filespecs)  # noqa: F405
            debug(f'  {removed_filename} starts at {file_beg} and ends at {file_end} in stream')
            first_affected_piece_pos = round_down_to_multiple(file_beg, self.piece_size)  # noqa: F405
            last_affected_piece_pos = round_down_to_multiple(file_end, self.piece_size)  # noqa: F405
            debug(f'  First affected piece starts at {first_affected_piece_pos} '
                  f'and last affected piece starts at {last_affected_piece_pos}')
            corruption_positions.add(first_affected_piece_pos)
            corruption_positions.add(last_affected_piece_pos)

        self.corruption_positions.update(corruption_positions)
        self._remove_skipped_corruptions()
        debug(f'Corruption positions after removing file: {self.corruption_positions}')

    def _remove_skipped_corruptions(self):
        # Finally, remove corruptions that exclusively belong to
        # missing/missized files because they are always skipped
        skipped_files = {str(filepath) for filepath in itertools.chain(self.files_missing, self.files_missized)}
        debug(f'  skipped_files: {skipped_files}')
        for corrpos in tuple(self.corruption_positions):
            affected_files = pos2files(corrpos, self.filespecs_abspath, self.piece_size)  # noqa: F405
            if all(f in skipped_files for f in affected_files):
                debug(f'  only skipped files are affected by corruption at position {corrpos}')
                self.corruption_positions.remove(corrpos)

    def change_file_size(self, index=None):
        # Pick random file
        if index is None:
            filename = random.choice(tuple(self.content_original))
        else:
            filename = tuple(self.content_original)[index]
        filepath = self.content_path / filename
        debug(f'Changing file size in file system: {filepath}')

        # Don't change corrupt/missing file
        error_files = set(os.path.basename(f) for f in itertools.chain(
            self.files_missing, self.files_corrupt))
        if filename in error_files:
            return

        # Change file size
        self.content_corrupt[filename] = change_file_size(  # noqa: F405
            filepath, len(self.content_original[filename]))
        self.files_missized.append(filepath)

        # Check if the beginning of adjacent files will be corrupted
        file_beg,file_end = file_range(filename, self.filespecs)  # noqa: F405
        debug(f'  Original file beginning and end in stream: {file_beg}, {file_end}')
        if file_beg % self.piece_size != 0:
            debug(f'  Beginning corrupts previous file at piece_index {file_beg // self.piece_size}')
            self.corruption_positions.add(file_beg)

        # Check if the end of adjacent files will be corrupted
        if (file_end + 1) % self.piece_size != 0:
            filepath,_ = pos2file(file_end, self.filespecs_abspath, self.piece_size)  # noqa: F405
            if (filepath not in self.files_missing and
                filepath not in self.files_missized and
                filepath != self.filespecs_abspath[-1][0]):
                debug(f'  End corrupts next file at piece_index {(file_end + 1) // self.piece_size}')
                self.corruption_positions.add(file_end)

        self._remove_skipped_corruptions()
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
    assert str(excinfo.value) == 'Invalid metainfo: Mock error'
    mock_validate.assert_called_once_with()

def test_verify_content_successfully(mktestcase, piece_size, callback, filespecs):
    display_filespecs(filespecs, piece_size)  # noqa: F405
    tc = mktestcase(filespecs, piece_size)
    tc.run(with_callback=callback['enabled'],
           exp_return_value=True)

def test_verify_content_with_random_corruptions_and_no_skipping(mktestcase, piece_size, callback, filespecs):
    display_filespecs(filespecs, piece_size)  # noqa: F405
    tc = mktestcase(filespecs, piece_size)
    tc.corrupt_stream()
    tc.run(with_callback=callback['enabled'],
           exp_return_value=False)

def test_verify_content_with_random_corruptions_and_skipping(mktestcase, piece_size, callback, filespecs):
    display_filespecs(filespecs, piece_size)  # noqa: F405
    tc = mktestcase(filespecs, piece_size)
    tc.corrupt_stream()
    tc.run(with_callback=callback['enabled'],
           skip_on_error=True,
           exp_return_value=False)

def test_verify_content_with_missing_files_and_no_skipping(mktestcase, piece_size, callback, filespecs, filespec_indexes):
    display_filespecs(filespecs, piece_size)  # noqa: F405
    tc = mktestcase(filespecs, piece_size)
    for index in filespec_indexes:
        tc.delete_file(index)
    tc.run(with_callback=callback['enabled'],
           exp_return_value=False)

def test_verify_content_with_missing_files_and_skipping(mktestcase, piece_size, callback, filespecs, filespec_indexes):
    display_filespecs(filespecs, piece_size)  # noqa: F405
    tc = mktestcase(filespecs, piece_size)
    for index in filespec_indexes:
        tc.delete_file(index)
    tc.run(with_callback=callback['enabled'],
           skip_on_error=True,
           exp_return_value=False)

def test_verify_content_with_changed_file_size_and_no_skipping(mktestcase, piece_size, callback, filespecs):
    display_filespecs(filespecs, piece_size)  # noqa: F405

    tc = mktestcase(filespecs, piece_size)
    tc.change_file_size()
    tc.run(with_callback=callback['enabled'],
           exp_return_value=False)

def test_verify_content_with_changed_file_size_and_skipping(mktestcase, piece_size, callback, filespecs):
    display_filespecs(filespecs, piece_size)  # noqa: F405

    tc = mktestcase(filespecs, piece_size)
    tc.change_file_size()
    tc.run(with_callback=callback['enabled'],
           skip_on_error=True,
           exp_return_value=False)

def test_verify_content_with_multiple_error_types(mktestcase, piece_size, callback, filespecs):
    display_filespecs(filespecs, piece_size)  # noqa: F405
    tc = mktestcase(filespecs, piece_size)
    # Introduce 2 or 3 errors in random order
    errorizers = [tc.corrupt_stream, tc.delete_file, tc.change_file_size]
    for _ in range(random.randint(2, len(errorizers))):
        errorizer = errorizers.pop(random.choice(range(len(errorizers))))
        errorizer()
    tc.run(with_callback=callback['enabled'],
           skip_on_error=random.choice((True, False)),
           exp_return_value=False)
