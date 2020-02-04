# This file is part of torf.
#
# torf is free software: you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# torf is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with torf.  If not, see <https://www.gnu.org/licenses/>.

from . import _utils as utils
from ._debug import debug

from hashlib import sha1
import threading
import queue
import os
from time import monotonic as time_monotonic
from collections import defaultdict
from . import _errors as error


class ExhaustableQueue(queue.Queue):
    """
    If `exhausted` method is called, unblock all calls raise `queue.Empty` in
    all calls to get().  The `put` method is disabled after all remaining tasks
    are consumed.
    """
    _EXHAUSTED = object()

    def __init__(self, *args, name=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.__is_exhausted = False
        self.__name = name

    def get(self):
        with self.not_empty:
            while not self.__is_exhausted and not self._qsize():
                self.not_empty.wait()
            if self.__is_exhausted and not self._qsize():
                # Tell all other get() callers to stop blocking
                self.not_empty.notify_all()
                raise queue.Empty()
            task = self._get()
            if task is self._EXHAUSTED:
                # Mark this queue as exhausted so it won't accept any new tasks
                # via put()
                debug(f'{self} is now exhausted')
                self.__is_exhausted = True
                # Tell all other get() callers to stop blocking
                self.not_empty.notify_all()
                raise queue.Empty()
            self.not_full.notify()
            return task

    def put(self, task):
        if self.__is_exhausted:
            raise RuntimeError('Cannot call put() on exhausted queue: {self}')
        else:
            super().put(task)

    def exhausted(self):
        if not self.__is_exhausted:
            # Unblock one of the get() calls.  If nobody is currently calling
            # get(), this still marks the end of the queue and will eventually
            # be consumed after all real tasks.
            self.put(self._EXHAUSTED)

    @property
    def is_exhausted(self):
        return self.__is_exhausted

    # TODO: Add clear() method that get()s tasks from queue until it is empty,
    #       but only if __is_exhausted is True.  This might be useful when
    #       skipping files: The reader can clear() its own output queue to stop
    #       hashers from getting any new pieces.

    @property
    def name(self):
        return self.__name

    def __repr__(self):
        if self.__name:
            return f'<{type(self).__name__} {self.__name!r} [{self._qsize()}]>'
        else:
            return f'<{type(self).__name__} [{self._qsize()}]>'


class Worker():
    def __init__(self, name, worker):
        self._exception = None
        self._name = str(name)
        self._worker = worker
        self._thread = threading.Thread(name=self._name,
                                        target=self._run_and_catch_exceptions)
        self._thread.start()

    @property
    def exception(self):
        return self._exception

    @property
    def name(self):
        return self._name

    def _run_and_catch_exceptions(self):
        try:
            self._worker()
        except BaseException as e:
            self._exception = e

    def join(self):
        self._thread.join()
        if self._exception:
            raise self._exception
        return self


class Reader():
    def __init__(self, filepaths, piece_size, queue_size,
                 file_sizes=defaultdict(lambda: None),
                 skip_file_on_first_error=False):
        self._filepaths = tuple(filepaths)
        assert self._filepaths, 'No file paths given'
        self._file_sizes = file_sizes
        self._piece_size = piece_size
        self._piece_queue = ExhaustableQueue(name='pieces', maxsize=queue_size)
        self._bytes_chunked = 0  # Number of bytes sent off as piece_size'd chunks
        self._fake = _FileFaker(self, self._filepaths, file_sizes, piece_size)
        self._skip_file_on_first_error = skip_file_on_first_error
        self._skipped_files = set()
        self._noskip_piece_indexes = set()
        self._stop = False

    def read(self):
        if self._stop:
            raise RuntimeError(f'Cannot read from the same instance multiple times.')
        try:
            trailing_bytes = b''
            for filepath in self._filepaths:
                if self._stop:
                    debug(f'reader: Stopped reading after piece_index {self._calc_piece_index()}')
                    break
                elif self.file_was_skipped(filepath):
                    debug(f'reader: Skipping {os.path.basename(filepath)} before opening it')
                    bytes_chunked, trailing_bytes = self._fake(
                        filepath, self._bytes_chunked, len(trailing_bytes))
                else:
                    self._check_file_size(filepath)
                    bytes_chunked, trailing_bytes = self._read_file(filepath, trailing_bytes)
                self._bytes_chunked += bytes_chunked
                debug(f'reader: Finished reading {os.path.basename(filepath)}: '
                      f'{self._bytes_chunked} bytes chunked, '
                      f'{len(trailing_bytes)} trailing bytes: {debug.pretty_bytes(trailing_bytes)}')

                assert len(trailing_bytes) < self._piece_size, trailing_bytes

            # Unless the torrent's total size is divisible by its piece size,
            # the last bytes from the last file aren't processed yet.
            if len(trailing_bytes) > 0 and not self._stop:
                debug(f'reader: {len(trailing_bytes)} final bytes of all files: {debug.pretty_bytes(trailing_bytes)}')
                self._bytes_chunked += len(trailing_bytes)
                self._push(self._calc_piece_index(), trailing_bytes, filepath, exc=None)
            debug(f'reader: Chunked {self._bytes_chunked} bytes in total')
        finally:
            self._piece_queue.exhausted()
            self._stop = True
            debug(f'reader: Bye')

    def _check_file_size(self, filepath):
        spec_filesize = self._file_sizes[filepath]
        if spec_filesize is not None:
            try:
                filesize = os.path.getsize(filepath)
            except OSError:
                pass  # Let self._read_file() handle this
            else:
                debug(f'reader: Checking size of {os.path.basename(filepath)}: {filesize} (expected: {spec_filesize})')
                if filesize != spec_filesize:
                    exc = error.VerifyFileSizeError(filepath,
                                                    actual_size=filesize,
                                                    expected_size=spec_filesize)
                    piece_index = self._calc_piece_index() + 1
                    self._push(piece_index, None, filepath, exc)
                    # No need to read this file
                    self.skip_file(filepath, piece_index, force=True)
                    self._dont_skip_piece(piece_index)

    def _read_file(self, filepath, trailing_bytes):
        piece_size = self._piece_size
        spec_filesize = self._file_sizes[filepath]
        bytes_chunked = 0
        try:
            # If file size is specified, ensure that we read exactly the
            # expected number of bytes.  Otherwise a shorter/longer file would
            # shift piece offsets of following files and make the whole stream
            # look corrupted even if it isn't.
            chunks = utils.read_chunks(filepath, piece_size,
                                       filesize=spec_filesize,
                                       prepend=trailing_bytes)
            for chunk in chunks:
                # debug(f'reader: Read {len(chunk)} bytes from {os.path.basename(filepath)}: {debug.pretty_bytes(chunk)}')
                if self._stop:
                    debug(f'reader: Found stop signal while reading from {os.path.basename(filepath)}')
                    break
                elif self.file_was_skipped(filepath):
                    debug(f'reader: Skipping {os.path.basename(filepath)} while chunking it')
                    bytes_chunked, trailing_bytes = self._fake(
                        filepath, self._bytes_chunked+bytes_chunked, len(trailing_bytes))
                    break
                else:
                    # Concatenate piece_size'd chunks across files until we have
                    # enough for a new piece
                    if len(chunk) == piece_size:
                        bytes_chunked += len(chunk)
                        piece_index = self._calc_piece_index(bytes_chunked)
                        debug(f'reader: {piece_index}: Read {bytes_chunked} bytes from {os.path.basename(filepath)}, '
                              f'{self._bytes_chunked + bytes_chunked} bytes in total: {debug.pretty_bytes(chunk)}')
                        self._push(piece_index, chunk, filepath, exc=None)
                        trailing_bytes = b''
                    else:
                        # Last chunk in file might be shorter than piece_size
                        trailing_bytes = chunk

        except Exception as exc:
            if spec_filesize is None:
                # We cannot calculate piece_index unless we know file's size,
                # and there's no point in going on if we don't know where a
                # piece begins and ends
                debug(f'reader: Raising read exception: {exc!r}')
                raise
            else:
                # Report error with piece_index pointing to the first corrupt piece
                file_beg,_ = self._calc_file_range(filepath)
                piece_index = self._calc_piece_index(absolute_pos=file_beg)
                debug(f'reader: Reporting read exception for piece index {piece_index} (file pos {file_beg}): {exc!r}')
                self._push(piece_index, None, filepath, exc)
                self.skip_file(filepath, piece_index, force=True)
                bytes_chunked, trailing_bytes = self._fake(
                    filepath, self._bytes_chunked+bytes_chunked, len(trailing_bytes))

        debug(f'reader: Remembering {len(trailing_bytes)} trailing bytes '
              f'from {os.path.basename(filepath)}: {debug.pretty_bytes(trailing_bytes)}')
        return bytes_chunked, trailing_bytes

    def file_was_skipped(self, filepath):
        if self._skip_file_on_first_error and filepath in self._skipped_files:
            file_beg,_ = self._calc_file_range(filepath)
            if self._calc_piece_index(absolute_pos=file_beg) not in self._noskip_piece_indexes:
                return True
        return False

    def skip_file(self, filepath, piece_index, force=False):
        if (self._skip_file_on_first_error or force) and filepath not in self._skipped_files:
            if piece_index not in self._noskip_piece_indexes:
                debug(f'reader: Marking {os.path.basename(filepath)} for skipping because of piece_index {piece_index} '
                      f'after chunking {int(self._bytes_chunked / self._piece_size)} chunks')
                self._skipped_files.add(filepath)
            else:
                debug(f'reader: Not skipping {os.path.basename(filepath)} because of expected '
                      f'corrupt piece_index {piece_index}: {self._noskip_piece_indexes}')

    # When we fake-read a file, the first piece of the file after the faked file
    # will produce an error because it (likely) contains padding bytes from the
    # previous/faked file.  If skip_file_on_first_error is True, that means the
    # next file is skipped even if it is completely fine and we just couldn't
    # confirm that.
    def _dont_skip_piece(self, piece_index):
        if self._skip_file_on_first_error:
            debug(f'reader: Never skipping file at piece_index {piece_index}')
            self._noskip_piece_indexes.add(piece_index)

    def _calc_piece_index(self, additional_bytes_chunked=0, absolute_pos=0):
        if absolute_pos:
            return absolute_pos // self._piece_size
        else:
            bytes_chunked = self._bytes_chunked + additional_bytes_chunked
            # bytes_chunked is the number of bytes, but we want the index of the
            # last byte that was chunked, hence -1.
            return max(0, bytes_chunked - 1) // self._piece_size

    def _calc_file_range(self, filepath):
        # Return the index of `filepath`'s first and last byte in the
        # concatenated stream of all files
        pos = 0
        for fp in self._filepaths:
            if fp == filepath:
                beg = pos
                end = beg + self._file_sizes[fp] - 1
                return beg, end
            else:
                pos += self._file_sizes[fp]
        raise RuntimeError(f'Unknown file path: {filepath}')

    def _push(self, piece_index, piece=None, filepath=None, exc=None):
        if self._stop:
            debug(f'reader: Found stop signal just before sending piece_index {piece_index}')
            return
        elif piece_index in self._fake.forced_error_piece_indexes and exc is None and piece is not None:
            # We know this piece is corrupt, even if padding bytes replicate the
            # missing data.  Exceptions from upstream (e.g. ReadError) take
            # precedence over corruption errors.  Ignore faked pieces (`piece`
            # is None) because they only exist to report progress.
            debug(f'reader: Forcing hash mismatch for piece_index {piece_index} (original piece: {piece}, exc: {exc})')
            piece = b''
        elif piece is not None:
            piece = bytes(piece)
        self._piece_queue.put((int(piece_index), piece, filepath, exc))
        debug(f'reader: >>> Pushed piece_index {piece_index}')

    def stop(self):
        if not self._stop:
            debug(f'reader: Setting stop flag')
            self._stop = True
            self._fake.stop = True
        return self

    @property
    def piece_queue(self):
        return self._piece_queue


class _FileFaker():
    # Pretend to read `filepath` to properly report progress and read following
    # files in the stream without shifting their pieces.
    def __init__(self, reader, filepaths, file_sizes, piece_size):
        self._reader = reader
        self._filepaths = filepaths
        self._file_sizes = file_sizes
        self._piece_size = piece_size
        self._faked_files = set()
        self.forced_error_piece_indexes = set()
        self.stop = False

    def __call__(self, filepath, bytes_chunked_total, trailing_bytes):
        # `bytes_chunked_total` is the number of bytes we've already read from
        # the stream, excluding any trailing bytes.  `trailing_bytes` is the
        # number of bytes from the previous file that couldn't fill a piece.
        if self._file_sizes[filepath] is None:
            raise RuntimeError(f'Unable to fake reading {filepath} without file size')

        # Calculate how many bytes we need to fake.
        debug(f'reader: Fake reading {os.path.basename(filepath)} after chunking {bytes_chunked_total} bytes from stream')
        file_beg,_ = self._reader._calc_file_range(filepath)
        remaining_bytes = file_beg - bytes_chunked_total + self._file_sizes[filepath]
        debug(f'faker: Remaining bytes to fake: {file_beg} - {bytes_chunked_total} + '
              f'{self._file_sizes[filepath]} = {remaining_bytes}')

        new_bytes_chunked_total, remaining_bytes = self._fake_first_piece(
            filepath, bytes_chunked_total, remaining_bytes, trailing_bytes)
        new_bytes_chunked_total, remaining_bytes = self._fake_middle_pieces(
            filepath, new_bytes_chunked_total, remaining_bytes)
        new_bytes_chunked_total, trailing_bytes = self._fake_last_piece(
            filepath, new_bytes_chunked_total, remaining_bytes)

        self._faked_files.add(filepath)
        debug(f'faker: Done faking: {new_bytes_chunked_total} bytes chunked from stream, {trailing_bytes} trailing bytes')
        bytes_chunked_diff = new_bytes_chunked_total - bytes_chunked_total
        return bytes_chunked_diff, b'\x00' * trailing_bytes

    def _fake_first_piece(self, filepath, bytes_chunked_total, remaining_bytes, trailing_bytes):
        # Fake the first piece if there are any `trailing_bytes` from the
        # previous file.  Note that we might not have enough bytes for a full
        # piece.
        piece_index = self._calc_piece_index(bytes_chunked_total + trailing_bytes)
        first_piece_contains_bytes_from_previous_file = bool(trailing_bytes)
        we_have_enough_bytes_for_complete_piece = remaining_bytes + trailing_bytes >= self._piece_size
        file_beg, file_end = self._reader._calc_file_range(filepath)
        first_piece_is_not_last_piece = file_beg // self._piece_size != file_end // self._piece_size
        final_piece_ends_at_piece_boundary = remaining_bytes % self._piece_size == 0
        debug(f'faker: Faking first piece_index {piece_index}: bytes_chunked_total: {bytes_chunked_total}, '
              f'remaining_bytes: {remaining_bytes}, trailing_bytes: {trailing_bytes}, '
              f'file_beg: {file_beg}, file_end: {file_end}')
        debug(f'faker:   first_piece_contains_bytes_from_previous_file: {first_piece_contains_bytes_from_previous_file}')
        debug(f'faker:         we_have_enough_bytes_for_complete_piece: {we_have_enough_bytes_for_complete_piece}')
        debug(f'faker:                   first_piece_is_not_last_piece: {first_piece_is_not_last_piece}')
        debug(f'faker:              final_piece_ends_at_piece_boundary: {final_piece_ends_at_piece_boundary}')

        if (we_have_enough_bytes_for_complete_piece
            and first_piece_contains_bytes_from_previous_file
            and (first_piece_is_not_last_piece or final_piece_ends_at_piece_boundary)):
            debug(f'faker: Faking first piece_index: {piece_index}')
            prev_affected_files = self._files_in_piece(piece_index, exclude=filepath)
            debug(f'faker: Files affected by first faked piece_index:')
            for fp in prev_affected_files:
                debug(f'faker:   {fp}')

            # Report completed piece as corrupt if `filepath` is not the only
            # file in it (corrupting files we didn't fake).
            if any(fpath not in self._faked_files for fpath in prev_affected_files):
                # Send b'' to guarantee a hash mismatch.  Sending fake bytes
                # might *not* raise an error if they replicate the missing data.
                debug(f'faker: Sending first fake piece_index {piece_index} as corrupt')
                self._reader._push(piece_index, b'', filepath, None)
                self._reader._dont_skip_piece(piece_index)

            # Process first piece
            remaining_bytes -= self._piece_size
            bytes_chunked_total += self._piece_size
        debug(f'faker: {piece_index}: First piece done: Chunked {bytes_chunked_total} bytes, {remaining_bytes} bytes remaining')
        return bytes_chunked_total, remaining_bytes

    def _fake_middle_pieces(self, filepath, bytes_chunked_total, remaining_bytes):
        # Fake pieces that exclusively belong to `filepath`.  Send `None` as
        # piece chunks to indicate that they are fake while sending information
        # about progress.
        while remaining_bytes >= self._piece_size:
            remaining_bytes -= self._piece_size
            bytes_chunked_total += self._piece_size
            if self.stop:
                debug(f'faker: Found stop signal while fake-reading from {os.path.basename(filepath)}')
                break
            piece_index = self._calc_piece_index(bytes_chunked_total)
            debug(f'faker: {piece_index}: Chunked {bytes_chunked_total} bytes, {remaining_bytes} bytes remaining')
            self._reader._push(piece_index, None, filepath, None)
        return bytes_chunked_total, remaining_bytes

    def _fake_last_piece(self, filepath, bytes_chunked_total, remaining_bytes):
        # Figure out what we want to do with remaining_bytes.
        debug(f'faker: Last piece: Chunked {bytes_chunked_total} bytes, {remaining_bytes} bytes remaining')
        next_filepath = self._get_next_filepath(filepath)
        piece_index = self._calc_piece_index(bytes_chunked_total)
        next_piece_index = self._calc_piece_index(bytes_chunked_total + remaining_bytes)
        debug(f'faker: piece_index={piece_index}, next_piece_index={next_piece_index}, '
              f'next_filepath={next_filepath}')

        if next_filepath is not None:
            # Fake trailing_bytes so the next piece isn't shifted in the stream.
            trailing_bytes = remaining_bytes
            debug(f'faker: Pretending to read {trailing_bytes} trailing bytes from {os.path.basename(filepath)}')

            # Does processing remaining_bytes start a new piece?  (This is not
            # the case if multiple files end in the same piece.)
            if next_piece_index != piece_index:
                # The next piece will be corrupt, but we don't want to skip any
                # files because because of that.
                self._reader._dont_skip_piece(next_piece_index)

            if next_piece_index not in self.forced_error_piece_indexes:
                # Force error in the piece that contains `filepath`'s
                # trailing_bytes.  This is necessary because any padding/fake
                # bytes can be identical to the original/missing bytes, meaning
                # that we don't report an error for the next piece.
                self.forced_error_piece_indexes.add(next_piece_index)
                debug(f'faker: Updated forced error piece_indexes: {self.forced_error_piece_indexes}')

        else:
            # This is the last file in the stream
            next_affected_files = self._files_in_piece(next_piece_index, exclude=filepath)
            debug(f'faker: Other affected files: {next_affected_files}')
            debug(f'faker: Faked files: {self._faked_files}')

            # Do not report corruption in final piece if all of the files that
            # end in it have been faked.
            if all(fpath in self._faked_files for fpath in next_affected_files):
                # Don't report error by returning no trailing_bytes,
                # but update progress to 100%.
                debug(f'faker: Suppressing corruption in final piece_index {next_piece_index}')
                self.forced_error_piece_indexes.discard(next_piece_index)
                self._reader._push(next_piece_index, None, filepath, None)
                bytes_chunked_total += remaining_bytes
                trailing_bytes = 0
            else:
                # Fake last few bytes in the stream
                trailing_bytes = remaining_bytes
                debug(f'faker: Pretending to read {trailing_bytes} trailing bytes from {os.path.basename(filepath)}')
                # trailing_bytes could be identical to the missing bytes which
                # means the next piece would not raise a hash mismatch, so we
                # must remember to enforce that.
                self.forced_error_piece_indexes.add(next_piece_index)
                debug(f'faker: Updated forced error piece_indexes: {self.forced_error_piece_indexes} '
                      f'because other files are affected')
                if not remaining_bytes:
                    # If we already pushed all pieces, marking the final piece
                    # for a forced error doesn't do anything.  Reader.read()
                    # will do nothing after looping over all files.)  In that
                    # case push a corrupt piece with the final piece index again
                    # to trigger the error.
                    debug(f'faker: Forcing corruption in final piece_index {piece_index} immediately')
                    assert piece_index == next_piece_index
                    self._reader._push(piece_index, b'', filepath, None)
                    trailing_bytes = 0

        debug(f'faker: {piece_index}: Finished: Chunked {bytes_chunked_total} bytes, {remaining_bytes} bytes remaining')
        return bytes_chunked_total, trailing_bytes

    def _calc_piece_index(self, bytes_chunked_total):
        # `bytes_chunked_total` is the number of processed bytes, but we want
        # the index of the last processed byte.
        return max(0, bytes_chunked_total - 1) // self._piece_size

    def _files_in_piece(self, piece_index, exclude=()):
        # Return list of filepaths that have bytes in piece at `piece_index`
        piece_beg = piece_index * self._piece_size
        piece_end = piece_beg + self._piece_size - 1
        # debug(f'First byte in piece_index {piece_index} is at pos {piece_beg}, last at {piece_end}')
        pos = 0
        filepaths = []
        for fpath,fsize in self._file_sizes.items():
            file_beg = pos
            file_end = file_beg + fsize -1
            # File's last/first byte is between piece's first/last byte?
            if piece_beg <= file_beg <= piece_end or piece_beg <= file_end <= piece_end:
                if fpath not in exclude:
                    filepaths.append(fpath)
            pos += fsize
        return filepaths

    def _get_next_filepath(self, filepath):
        try:
            index = self._filepaths.index(filepath)
        except ValueError:
            pass
        else:
            try:
                return self._filepaths[index+1]
            except IndexError:
                pass


class HasherPool():
    def __init__(self, workers_count, piece_queue, file_was_skipped=None):
        self._piece_queue = piece_queue
        self._hash_queue = ExhaustableQueue(name='hashes')
        if file_was_skipped is not None:
            self._file_was_skipped = file_was_skipped
        else:
            self._file_was_skipped = lambda _: False
        self._stop = False
        self._workers = [Worker(f'hasher{i}', self._worker)
                         for i in range(1, workers_count+1)]

    def _worker(self):
        name = threading.current_thread().name
        piece_queue = self._piece_queue
        while not self._stop:
            # debug(f'{name}: Waiting for next task [{piece_queue.qsize()}]')
            try:
                task = piece_queue.get()
            except queue.Empty:
                # debug(f'{name}: {piece_queue} is exhausted')
                break
            else:
                self._work(*task)
        # debug(f'{name}: Bye, piece_queue has {piece_queue.qsize()} items left')

    def _work(self, piece_index, piece, filepath, exc):
        # name = threading.current_thread().name
        if exc is not None:
            # debug(f'{name}: Forwarding exception for piece_index {piece_index}: {exc!r}')
            self._hash_queue.put((piece_index, piece, filepath, exc))
        elif self._file_was_skipped(filepath):
            # debug(f'{name}: Sending dummy for piece_index {piece_index} of skipped file {os.path.basename(filepath)}')
            self._hash_queue.put((piece_index, None, filepath, None))
        else:
            piece_hash = sha1(piece).digest() if piece is not None else None
            # debug(f'{name}: Sending hash of piece_index {piece_index}: '
            #       f'{debug.pretty_bytes(piece)}: {debug.pretty_bytes(piece_hash)}')
            self._hash_queue.put((piece_index, piece_hash, filepath, exc))

    def stop(self):
        if not self._stop:
            # debug(f'hasherpool: Setting stop flag')
            self._stop = True
        return self

    def join(self):
        for worker in self._workers:
            # debug(f'hasherpool: Joining {worker.name}')
            worker.join()
        # debug('hasherpool: Joined all workers')
        self._hash_queue.exhausted()
        return self

    @property
    def hash_queue(self):
        return self._hash_queue


class Collector(Worker):
    def __init__(self, hash_queue, callback=None, file_was_skipped=None):
        self._hash_queue = hash_queue
        self._callback = callback
        self._stop = False
        self._hashes_unsorted = []
        self._hashes = bytes()
        self._pieces_seen = set()
        if file_was_skipped is not None:
            self._file_was_skipped = file_was_skipped
        else:
            self._file_was_skipped = lambda _: False
        super().__init__(name='collector', worker=self._collect_hashes)

    def _collect_hashes(self):
        while not self._stop:
            # debug(f'collector: Waiting for next piece_hash [{self._hash_queue.qsize()}]')
            try:
                task = self._hash_queue.get()
            except queue.Empty:
                # debug(f'collector: {self._hash_queue} is exhausted')
                break
            else:
                self._work(*task)

        # Sort hashes by piece_index and concatenate them
        self._hashes = b''.join(hash for index,hash in sorted(self._hashes_unsorted))
        # debug(f'collector: Collected {len(self._hashes_unsorted)} pieces')
        # debug(f'collector: Bye, hash_queue has {self._hash_queue.qsize()} items left')

    def _work(self, piece_index, piece_hash, filepath, exc):
        # A piece can be reported twice, but we don't want to increase
        # pieces_done in that case; the set's length is the number of seen
        # pieces.
        self._pieces_seen.add(piece_index)
        if self._file_was_skipped(filepath):
            piece_hash = None
        if exc is not None:
            if self._callback:
                self._callback(filepath, len(self._pieces_seen), piece_index, piece_hash, exc)
            else:
                raise exc
        else:
            if piece_hash is not None:
                self._hashes_unsorted.append((piece_index, piece_hash))
            if self._callback:
                self._callback(filepath, len(self._pieces_seen), piece_index, piece_hash, exc)

    @property
    def hashes(self):
        return self._hashes

    def stop(self):
        if not self._stop:
            # debug(f'collector: Setting stop flag')
            self._stop = True
        return self


class CancelCallback():
    """
    Callable that calls `callback` after `interval` seconds between calls and
    does nothing on all other calls
    """
    def __init__(self, callback, interval=0):
        self._callback = callback
        self._interval = interval
        self._prev_call_time = None
        self._cancel_callbacks = []

    def __call__(self, cb_args, force_call=False):
        now = time_monotonic()
        prev_call_time = self._prev_call_time
        # debug(f'CancelCallback: force_call={force_call}, prev_call_time={prev_call_time}, '
        #       f'now={now}, self._interval={self._interval}: {cb_args[1:]}')
        if (force_call or                             # Special case (e.g. exception in Torrent.verify())
            prev_call_time is None or                 # This is the first call
            now - prev_call_time >= self._interval):  # Previous call was at least `interval` seconds ago
            self._prev_call_time = now
            try:
                debug(f'CancelCallback: Calling callback with {cb_args[1:]}')
                return_value = self._callback(*cb_args)
                if return_value is not None:
                    debug(f'CancelCallback: Callback cancelled: {return_value!r}')
                    self._cancelled()
                    return True
                return False
            except BaseException as e:
                debug(f'CancelCallback: Caught exception: {e!r}')
                self._cancelled()
                raise

    def _cancelled(self):
        debug('CancelCallback: Cancelling')
        for cb in self._cancel_callbacks:
            cb()

    def on_cancel(self, *funcs):
        self._cancel_callbacks.extend(funcs)
