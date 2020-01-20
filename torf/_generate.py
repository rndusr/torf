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
from multiprocessing.pool import ThreadPool
import queue
import os
from time import monotonic as time_monotonic
from itertools import count as _count
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
                debug('{self} is now exhausted')
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
    #       but only if __is_exhausted is True.
    @property
    def name(self):
        return self.__name

    def __repr__(self):
        if self.__name:
            return f'<{type(self).__name__} {self.__name!r} [{self.qsize()}]>'
        else:
            return f'<{type(self).__name__} [{self.qsize()}]>'


class Worker():
    def __init__(self, name, worker):
        self._exception = None
        self._name = str(name)
        self._worker = worker
        self._thread = threading.Thread(name=self._name,
                                        target=self.run_and_catch_exceptions)
        self._thread.start()

    @property
    def exception(self):
        return self._exception

    @property
    def name(self):
        return self._name

    def run_and_catch_exceptions(self):
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
        self._trailing_bytes = b''
        self._skip_file_on_first_error = skip_file_on_first_error
        self._skip_files = set()
        self._expected_corruptions = set()
        self._stop = False

    def read(self):
        if self._stop:
            raise RuntimeError(f'Cannot read from the same instance multiple times.')
        try:
            for filepath in self._filepaths:
                if self._stop:
                    debug(f'reader: Stopped reading after piece_index {self._calc_piece_index()}')
                    break
                elif self.file_was_skipped(filepath):
                    debug(f'reader: Skipping {filepath} before opening it')
                    self._bytes_chunked += self._fake_read_file(filepath)
                    self._expect_corruption(filepath=filepath)
                    continue
                else:
                    self._check_file_size(filepath)
                    self._bytes_chunked += self._read_file(filepath)

                debug(f'reader: Finished reading {os.path.basename(filepath)}: '
                      f'{self._bytes_chunked} bytes chunked, '
                      f'{len(self._trailing_bytes)} trailing bytes: {debug.pretty_bytes(self._trailing_bytes)}')
                assert len(self._trailing_bytes) < self._piece_size, self._trailing_bytes

            # Unless the torrent's total size is divisible by its piece size,
            # the last bytes from the last file aren't processed yet
            if len(self._trailing_bytes) > 0 and not self._stop:
                self._bytes_chunked += len(self._trailing_bytes)
                piece_index = self._calc_piece_index()
                debug(f'reader: {len(self._trailing_bytes)} final bytes of all files: '
                      f'{debug.pretty_bytes(self._trailing_bytes)}')
                self._push(piece_index, self._trailing_bytes, filepath, exc=None)
        finally:
            self._trailing_bytes = b''
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
                    self.skip_file(filepath, piece_index)
                    next_filepath = self._get_next_filepath(filepath)
                    if next_filepath is not None:
                        self._expect_corruption(next_filepath)

    def _read_file(self, filepath):
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
                                       prepend=self._trailing_bytes)
            self._trailing_bytes = b''
            for chunk in chunks:
                # debug(f'reader: Read {len(chunk)} bytes from {os.path.basename(filepath)}: {debug.pretty_bytes(chunk)}')

                if self._stop:
                    debug(f'reader: Found stop signal while reading from {os.path.basename(filepath)}')
                    break
                elif self.file_was_skipped(filepath):
                    debug(f'reader: Skipping {filepath} while chunking it')
                    bytes_chunked = self._fake_read_file(filepath, bytes_chunked)
                    next_filepath = self._get_next_filepath(filepath)
                    if next_filepath is not None:
                        self._expect_corruption(next_filepath)
                    break
                else:
                    # Concatenate piece_size'd chunks across files until we have
                    # enough for a new piece
                    if len(chunk) == piece_size:
                        bytes_chunked += len(chunk)
                        piece_index = self._calc_piece_index(bytes_chunked)
                        debug(f'reader: {piece_index}: Read {bytes_chunked} bytes from {os.path.basename(filepath)}, '
                              f'{self._bytes_chunked + bytes_chunked} bytes in total: {debug.pretty_bytes(chunk)}')

                        # debug(f'reader: Sending piece_index {piece_index} of {os.path.basename(filepath)} '
                        #       f'to {self._piece_queue} [{self._piece_queue.qsize()}]: {debug.pretty_bytes(chunk)}')
                        self._push(piece_index, chunk, filepath, exc=None)
                        # debug(f'reader: Sent piece_index {piece_index} to {self._piece_queue} [{self._piece_queue.qsize()}]')
                    else:
                        # Last chunk in file might be shorter than piece_size
                        self._trailing_bytes = chunk
                        debug(f'reader: Remembering {len(self._trailing_bytes)} trailing bytes '
                              f'from {os.path.basename(filepath)}: {debug.pretty_bytes(self._trailing_bytes)}')

        except Exception as exc:
            if spec_filesize is None:
                # We cannot calculate piece_index unless we know file's size,
                # and there's no point in going on if we don't know where a
                # piece begins and ends
                debug(f'reader: Raising read exception: {exc!r}')
                raise
            else:
                # Report error with piece_index pointing to the first corrupt piece
                piece_index = self._calc_piece_index(bytes_chunked) + 1
                debug(f'reader: Reporting read exception for piece index {piece_index}: {exc!r}')
                self._push(piece_index, None, filepath, exc)
                self.skip_file(filepath, piece_index)
                bytes_chunked += self._fake_read_file(filepath, bytes_chunked)
                next_filepath = self._get_next_filepath(filepath)
                if next_filepath is not None:
                    self._expect_corruption(next_filepath)

        return bytes_chunked

    def _fake_read_file(self, filepath, bytes_chunked=0):
        # Pretend we did read `filepath` so we can calculate piece_index after
        # skipping files or if a file is missing.  `bytes_chunked` is the amount
        # of bytes we've already read from `filepath`, excluding any trailing
        # bytes from previous file.
        piece_size = self._piece_size
        spec_filesize = self._file_sizes[filepath]
        if spec_filesize is None:
            raise RuntimeError(f'Unable to fake reading {filepath} without file size')

        file_index = self._calc_file_start(filepath)
        remaining_bytes = file_index - self._bytes_chunked + spec_filesize - bytes_chunked
        debug(f'reader: Pretending to read {self._bytes_chunked} - {file_index} + {spec_filesize} - {bytes_chunked} '
              f'= {remaining_bytes} remaining bytes from {os.path.basename(filepath)}')
        fake_bytes_chunked = bytes_chunked
        debug(f'reader: Bytes chunked so far: {self._bytes_chunked} + {fake_bytes_chunked}')
        while remaining_bytes >= piece_size:
            if self._stop:
                debug(f'reader: Found stop signal while fake-reading from {os.path.basename(filepath)}')
                return fake_bytes_chunked
            remaining_bytes -= piece_size
            fake_bytes_chunked += piece_size
            piece_index = self._calc_piece_index(fake_bytes_chunked)
            debug(f'reader: {piece_index}: Fake read {fake_bytes_chunked} bytes from {os.path.basename(filepath)}, '
                  f'{self._bytes_chunked + fake_bytes_chunked} bytes in total: '
                  f'fake_bytes_chunked={fake_bytes_chunked}, remaining_bytes={remaining_bytes}')
            self._push(piece_index, None, filepath, None)

        # We must fake any bytes from `filepath` that didn't fit into its last
        # piece.
        try:
            # Read the beginning of the first piece of the next file from the
            # end of the skipped file.
            debug(f'reader: Seeking to {-remaining_bytes} in {filepath}')
            with open(filepath, 'rb') as f:
                f.seek(-remaining_bytes, os.SEEK_END)
                self._trailing_bytes = f.read(remaining_bytes)
            debug(f'reader: Read {len(self._trailing_bytes)} trailing bytes '
                  f'from {filepath}: {debug.pretty_bytes(self._trailing_bytes)}')
        except OSError:
            # If the file is missing, fill trailing_bytes with the expected
            # amount of bytes to maintain correct piece offsets.
            self._trailing_bytes = b'\x00' * remaining_bytes
            debug(f'reader: Pretending to read {len(self._trailing_bytes)} trailing bytes '
                  f'from {filepath}: {debug.pretty_bytes(self._trailing_bytes)}')

        return fake_bytes_chunked

    @property
    def skipped_files(self):
        return self._skip_files

    def file_was_skipped(self, filepath):
        if self._skip_file_on_first_error and filepath in self._skip_files:
            return True
        return False

    def skip_file(self, filepath, piece_index):
        if self._skip_file_on_first_error and filepath not in self._skip_files:
            if piece_index not in self._expected_corruptions:
                debug(f'Marking {filepath} for skipping because of piece_index {piece_index} '
                      f'after chunking {int(self._bytes_chunked / self._piece_size)} chunks')
                self._skip_files.add(filepath)
            else:
                debug(f'Not skipping {filepath} because of expected '
                      f'corrupt piece_index {piece_index}: {self._expected_corruptions}')

    # When we fake-read a file, we must also fake the trailing bytes that
    # overlap into the next file.  This will cause an error for the first piece
    # of the next file.  If skip_file_on_first_error is True, that means the
    # next file is skipped even if it is completely fine and we just couldn't
    # confirm that because we don't have enough information to compute its first
    # piece hash.
    def _expect_corruption(self, filepath):
        # Store piece index of filepath's first byte
        debug(f'reader: Expecting corruption in first piece of {filepath}')
        file_index = self._calc_file_start(filepath)
        piece_index = self._calc_piece_index(absolute_pos=file_index)
        debug(f'reader: {os.path.basename(filepath)} starts at byte {file_index}, piece_index {piece_index}')
        self._expected_corruptions.add(piece_index)
        debug(f'reader: Never skipping {filepath} because of the following piece_indexes: {self._expected_corruptions}')

    def _calc_piece_index(self, additional_bytes_chunked=0, absolute_pos=0):
        if absolute_pos:
            return absolute_pos // self._piece_size
        else:
            bytes_chunked = self._bytes_chunked + additional_bytes_chunked
            return (bytes_chunked - 1) // self._piece_size

    def _calc_file_start(self, filepath):
        # Return the index of `filepath`'s first byte in the concatenated stream
        # of all files
        index = 0  # File's first byte in stream of files
        for fp in self._filepaths:
            if fp == filepath:
                return index
            else:
                index += self._file_sizes[fp]
        raise RuntimeError(f'Unknown file path: {filepath}')

    def _push(self, piece_index, piece=None, filepath=None, exc=None):
        piece = None if piece is None else bytes(piece)
        self._piece_queue.put((int(piece_index), piece, filepath, exc))
        # debug(f'reader: Pushed piece {piece_index} [{self._piece_queue.qsize()}]')

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

    def stop(self):
        debug(f'reader: Setting stop flag')
        self._stop = True
        return self

    @property
    def piece_queue(self):
        return self._piece_queue


class HashWorkerPool():
    def __init__(self, workers_count, piece_queue, file_was_skipped=None):
        self._piece_queue = piece_queue
        self._workers_count = workers_count
        self._hash_queue = ExhaustableQueue(name='hashes')
        if file_was_skipped is not None:
            self._file_was_skipped = file_was_skipped
        else:
            self._file_was_skipped = lambda _: False
        self._stop = False
        self._name_counter = _count().__next__
        self._name_counter_lock = threading.Lock()
        self._pool = ThreadPool(workers_count, self._worker)

    def _get_new_worker_name(self):
        with self._name_counter_lock:
            return f'hasher #{self._name_counter()}'

    def _worker(self):
        name = self._get_new_worker_name()
        piece_queue = self._piece_queue
        hash_queue = self._hash_queue
        file_was_skipped = self._file_was_skipped
        while True:
            try:
                piece_index, piece, filepath, exc = piece_queue.get()
            except QueueExhausted:
                # debug(f'{name}: {piece_queue} is exhausted')
                break
            else:
                if exc is not None:
                    # debug(f'{name}: Forwarding exception for piece_index {piece_index}: {exc!r}')
                    hash_queue.put((piece_index, piece, filepath, exc))
                elif file_was_skipped(filepath):
                    # debug(f'{name}: Sending dummy for piece_index {piece_index} of skipped file {os.path.basename(filepath)}')
                    hash_queue.put((piece_index, None, filepath, None))
                else:
                    piece_hash = sha1(piece).digest() if piece is not None else None
                    # debug(f'{name}: Sending hash of piece_index {piece_index} to {hash_queue}: '
                    #       f'{debug.pretty_bytes(piece)}: {debug.pretty_bytes(piece_hash)}')
                    hash_queue.put((piece_index, piece_hash, filepath, exc))
                    # debug(f'{name}: Sent hash: {debug.pretty_bytes(piece_hash)}')

            if self._stop:
                # debug(f'{name}: Stop flag found')
                break
        # debug(f'{name}: Bye')

    def stop(self):
        # debug(f'hasherpool: Stopping hasher pool')
        self._stop = True
        return self

    def join(self):
        # debug(f'hasherpool: Joining {self._workers_count} workers')
        self._pool.close()
        self._pool.join()
        self._hash_queue.exhausted()
        # debug(f'hasherpool: All workers joined')
        return self

    @property
    def hash_queue(self):
        return self._hash_queue


class CollectorWorker(Worker):
    def __init__(self, hash_queue, callback=None, file_was_skipped=None):
        self._hash_queue = hash_queue
        self._callback = callback
        self._stop = False
        self._hashes = bytes()
        if file_was_skipped is not None:
            self._file_was_skipped = file_was_skipped
        else:
            self._file_was_skipped = lambda _: False
        super().__init__(name='collector', worker=self._collect_hashes)

    def _collect_hashes(self):
        hash_queue = self._hash_queue
        callback = self._callback
        file_was_skipped = self._file_was_skipped
        hashes_unsorted = []
        pieces_seen = set()

        while True:
            try:
                # debug(f'collector: Getting from {hash_queue}')
                piece_index, piece_hash, filepath, exc = hash_queue.get()
            except QueueExhausted:
                # debug(f'collector: {hash_queue} is exhausted')
                break
            else:
                if file_was_skipped(filepath):
                    piece_hash = None

                # A piece can be reported twice
                pieces_seen.add(piece_index)

                # debug(f'collector: Got {piece_index}, {debug.pretty_bytes(piece_hash)}, {filepath}, {exc}')
                if exc is not None:
                    if callback:
                        # debug(f'collector: Forwarding exception for piece_index {piece_index}: {exc!r}')
                        callback(filepath, len(pieces_seen), piece_index, piece_hash, exc)
                    else:
                        # debug(f'collector: Raising forwarded exception: {exc}')
                        raise exc
                else:
                    # debug(f'collector: Collected piece hash of piece_index {piece_index} of {filepath}: '
                    #       f'{debug.pretty_bytes(piece_hash)}')
                    if piece_hash is not None:
                        hashes_unsorted.append((piece_index, piece_hash))
                    if callback:
                        # debug(f'collector: Calling callback: pieces_done={len(pieces_seen)}')
                        callback(filepath, len(pieces_seen), piece_index, piece_hash, exc)
            if self._stop:
                # debug(f'collector: Stop flag found while getting piece hash')
                break
        # debug(f'collector: Collected {len(hashes_unsorted)} pieces')
        # Sort hashes by piece_index and concatenate them
        self._hashes = b''.join(hash for index,hash in sorted(hashes_unsorted))

    @property
    def hashes(self):
        return self._hashes

    def stop(self):
        if not self._stop:
            debug(f'collector: Setting stop flag')
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
        if (force_call or                             # Special case (e.g. exception in Torrent.verify())
            prev_call_time is None or                 # This is the first call
            now - prev_call_time >= self._interval):  # Previous call was at least `interval` seconds ago
            self._prev_call_time = now
            try:
                # debug(f'CancelCallback: force_call={force_call}, prev_call_time={prev_call_time}, '
                #       f'now={now}, self._interval={self._interval}')
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
