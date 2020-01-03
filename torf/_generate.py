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
import time
from itertools import count as _count
from collections import defaultdict


class NamedQueueMixin():
    """Add `name` property and a useful `__repr__`"""
    def __init__(self, *args, name=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.__name = name

    @property
    def name(self):
        return self.__name

    def __repr__(self):
        if self.__name:
            return f'<{type(self).__name__} {self.__name!r} [{self.qsize()}]>'
        else:
            return f'<{type(self).__name__} [{self.qsize()}]>'

class QueueExhausted(Exception): pass
class ExhaustQueueMixin():
    """
    Add `exhausted` method that marks this queue as dead

    `get` blocks until there is a new value or until `exhausted` is called. All
    calls to `get` on an exhausted queue raise `QueueExhausted` if it is empty.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__is_exhausted = False

    def get(self):
        while True:
            try:
                return super().get(timeout=0.01)
            except queue.Empty:
                if self.__is_exhausted:
                    # debug(f'{self} is exhausted')
                    raise QueueExhausted()

    def exhausted(self):
        if not self.__is_exhausted:
            self.__is_exhausted = True
            # debug(f'Marked {self} as exhausted')

    @property
    def is_exhausted(self):
        return self.__is_exhausted

class ExhaustQueue(ExhaustQueueMixin, NamedQueueMixin, queue.Queue):
    pass


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
            # debug(f'{self.name}: Setting exception: {e!r}')
            self._exception = e
        # debug(f'{self.name}: Bye')

    def join(self):
        self._thread.join()
        if self._exception:
            raise self._exception
        return self


class Reader():
    def __init__(self, filepaths, piece_size, queue_size, file_sizes=defaultdict(lambda: None)):
        self._filepaths = tuple(filepaths)
        assert self._filepaths, 'No file paths given'
        self._file_sizes = file_sizes
        self._piece_size = piece_size
        self._piece_queue = ExhaustQueue(name='pieces', maxsize=queue_size)
        self._bytes_read = 0
        self._piece_buffer = bytearray()
        self._skip_files = []
        self._stop = False

        self._pieces_pushed = 0  # TODO: Remove when everything works

    def _push(self, piece_index, piece=None, filepath=None, exc=None):
        self._piece_queue.put((int(piece_index), bytes(piece), filepath, exc))
        self._pieces_pushed += 1
        debug(f'reader: Pushed {self._pieces_pushed} pieces')

    def read(self):
        if self._stop or self._piece_queue.is_exhausted:
            raise RuntimeError(f'Cannot read from the same instance multiple times.')
        try:
            for filepath in self._filepaths:
                if self._stop:
                    debug(f'reader: Stopped reading after piece_index {self._calc_piece_index()}')
                    break
                elif filepath in self._skip_files:
                    debug(f'reader: Found {filepath} in {self._skip_files} before opening it')
                    self._fake_read_file(filepath)
                    continue
                else:
                    self._read_file(filepath)

            # Unless the torrent's total size is divisible by its piece size,
            # the last bytes from the last file are still in piece_buffer
            if (not self._stop and
                filepath not in self._skip_files and
                len(self._piece_buffer) > 0):
                debug(f'reader: Left over bytes in buffer: {len(self._piece_buffer)}')
                # Piece index is only incremented if the buffered bytes don't
                # fit in the current piece
                piece_index = self._calc_piece_index()
                if self._bytes_read + len(self._piece_buffer) > self._piece_size:
                    piece_index += 1
                debug(f'reader: Sending last piece_index {piece_index} to {self._piece_queue}')
                self._push(piece_index, self._piece_buffer, filepath, exc=None)
        finally:
            self._piece_queue.exhausted()
            self._stop = True

    def _read_file(self, filepath):
        bytes_read = 0
        piece_buffer = self._piece_buffer
        piece_size = self._piece_size
        spec_filesize = self._file_sizes[filepath]
        try:
            # If file size is specified, ensure that we read exactly the
            # expected number of bytes.  Otherwise a shorter/longer file would
            # shift piece offsets of following files and make the whole stream
            # look corrupted even if it isn't.
            debug(f'reader: Reading {filepath}')
            chunks = utils.read_chunks(filepath, piece_size, filesize=spec_filesize)
            for chunk in chunks:
                if self._stop:
                    return
                elif filepath in self._skip_files:
                    debug(f'reader: Found {filepath} in {self._skip_files} while chunking')
                    self._fake_read_file(filepath)
                    return

                # Concatenate piece_size'd chunks across files until we have
                # enough for a new piece
                piece_buffer.extend(chunk)
                bytes_read += len(chunk)
                debug(f'reader: Read {len(chunk)} bytes, {bytes_read} bytes in total')
                if len(piece_buffer) >= piece_size:
                    piece = piece_buffer[:piece_size]
                    del piece_buffer[:piece_size]

                    piece_index = self._calc_piece_index(self._bytes_read + bytes_read)
                    debug(f'reader: Sending piece {piece_index} of {os.path.basename(filepath)} to {self._piece_queue}: '
                          f'{bytes(piece[:10])} .. {bytes(piece[-10:])}')
                    self._push(piece_index, piece, filepath, exc=None)

                debug(f'reader: Piece buffer contains {len(piece_buffer)} bytes')

            if spec_filesize:
                assert bytes_read == spec_filesize
            self._bytes_read += bytes_read

        except Exception as exc:
            if spec_filesize is None:
                # We cannot calculate piece_index unless we know file's size,
                # and there's no point in going on if we don't know where a
                # piece begins and ends
                debug(f'reader: Re-raising read exception: {exc!r}')
                raise
            else:
                debug(f'reader: Forwarding read exception: {exc!r}')
                # Add the bytes we actually read from disk to calculate the
                # correct piece index for this error
                # debug(f'reader: Calculating piece_index from {self._bytes_read} + {bytes_read} + 1')
                # piece_index = self._calc_piece_index(self._bytes_read + bytes_read) + 1
                debug(f'reader: Calculating piece_index from {self._bytes_read} + {bytes_read}')
                piece_index = self._calc_piece_index(self._bytes_read + bytes_read)
                self._push(piece_index, None, filepath, exc)
                # Pretend we've read the whole file so we can calculate
                # correct piece indexes for future files
                self._bytes_read += spec_filesize

    def _fake_read_file(self, filepath):
        # Pretend we did read `filepath` so we can calculate piece_index after
        # skipping files or if a file is missing.
        spec_filesize = self._file_sizes[filepath]
        if spec_filesize is None:
            raise RuntimeError(f'Unable to fake reading {filepath} without file size')
        debug(f'reader: Pretending to have read {self._bytes_read} + {spec_filesize} '
              f'= {self._bytes_read+spec_filesize} bytes in total')
        self._bytes_read += spec_filesize

        # Unless all previous files were divisible by piece_size, the
        # skipped/missing file's last piece also contains bytes from the
        # beginning of the next file.
        self._piece_buffer.clear()
        trailing_bytes_len = self._bytes_read % self._piece_size
        try:
            # Read the beginning of the first piece of the next file from the
            # end of the skipped file.
            with open(filepath, 'rb') as f:
                f.seek(-trailing_bytes_len, os.SEEK_END)
                self._piece_buffer.extend(f.read(trailing_bytes_len))
            debug(f'reader: Read {len(self._piece_buffer)} trailing bytes '
                  f'from {filepath}: {self._piece_buffer}')
        except OSError:
            # If the file is not skipped but missing, we can at least fill
            # piece_buffer with the expected amount of bytes to maintain correct
            # piece offsets.
            self._piece_buffer.extend(b'\x00' * trailing_bytes_len)
            debug(f'reader: Pretending to have read {len(self._piece_buffer)} trailing bytes '
                  f'from {filepath}: {self._piece_buffer}')

    @property
    def skipped_files(self):
        return self._skip_files

    def skip_file(self, filepath):
        debug(f'Skipping {filepath}')
        self._skip_files.append(filepath)

    def _calc_piece_index(self, bytes_read=None):
        bytes_read = bytes_read or self._bytes_read
        # piece_index = max(0, int(bytes_read / self._piece_size) - 1)
        # debug(f'reader: Calculated piece_index: {piece_index}: ({bytes_read} / {self._piece_size}) - 1')
        piece_index = max(0, int(bytes_read / self._piece_size))
        debug(f'reader: Calculated piece_index: {piece_index}: {bytes_read} / {self._piece_size}')
        return piece_index

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
        self._hash_queue = ExhaustQueue(name='hashes')
        self._workers_count = workers_count
        self._file_was_skipped = file_was_skipped
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
                    debug(f'{name}: Forwarding exception: {exc}')
                    hash_queue.put((piece_index, piece, filepath, exc))
                else:
                    if file_was_skipped is None or not file_was_skipped(filepath):
                        # debug(f'{name}: Sending hash of piece_index {piece_index} to {hash_queue}')
                        hash_queue.put((piece_index, sha1(piece).digest(), filepath, exc))
                    # else:
                    #     debug(f'{name}: Skipping hash of piece_index {piece_index} to {hash_queue}')
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
    def __init__(self, hash_queue, callback=None):
        self._hash_queue = hash_queue
        self._callback = callback
        self._stop = False
        self._hashes = bytes()
        super().__init__(name='collector', worker=self._collect_hashes)

    def _collect_hashes(self):
        hash_queue = self._hash_queue
        callback = self._callback
        hashes_unsorted = []
        while True:
            try:
                debug(f'collector: Getting from {hash_queue}')
                piece_index, piece_hash, filepath, exc = hash_queue.get()
            except QueueExhausted:
                debug(f'collector: {hash_queue} is exhausted')
                break
            else:
                debug(f'collector: Got {piece_index}, {piece_hash}, {filepath}, {exc}')
                if exc is not None:
                    if callback:
                        debug(f'collector: Forwarding exception: {exc}')
                        callback(filepath, len(hashes_unsorted), piece_index, piece_hash, exc)
                    else:
                        debug(f'collector: Raising forwarded exception: {exc}')
                        raise exc
                else:
                    debug(f'collector: Collected piece hash of piece_index {piece_index} of {filepath}')
                    hashes_unsorted.append((piece_index, piece_hash))
                    if callback:
                        debug(f'collector: Calling callback: pieces_done={len(hashes_unsorted)}')
                        callback(filepath, len(hashes_unsorted), piece_index, piece_hash, exc)
            if self._stop:
                debug(f'collector: Stop flag found while getting piece hash')
                break
        debug(f'collector: Collected {len(hashes_unsorted)} pieces')
        # Sort hashes by piece_index and concatenate them
        self._hashes = b''.join(hash for index,hash in sorted(hashes_unsorted))

    def stop(self):
        if not self._stop:
            debug(f'collector: Setting stop flag')
            self._stop = True
        return self

    @property
    def hashes(self):
        return self._hashes


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
        now = time.monotonic()
        prev_call_time = self._prev_call_time
        if (force_call or                             # Special case (e.g. exception during Torrent.verify())
            prev_call_time is None or                 # This is the first call
            now - prev_call_time >= self._interval):  # Previous call was at least `interval` seconds ago
            self._prev_call_time = now
            try:
                debug(f'CancelCallback: Calling callback with {cb_args[1:]}')
                if self._callback(*cb_args) is not None:
                    self._cancelled()
                    return True
                return False
            except BaseException as e:
                self._cancelled()
                raise

    def _cancelled(self):
        debug('CancelCallback: Cancelling')
        for cb in self._cancel_callbacks:
            cb()

    def on_cancel(self, *funcs):
        self._cancel_callbacks.extend(funcs)
