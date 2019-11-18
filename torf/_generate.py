# MIT License

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from . import _utils as utils
from . import debug

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
                    debug(f'{self} is exhausted')
                    raise QueueExhausted()

    def exhausted(self):
        if not self.__is_exhausted:
            self.__is_exhausted = True
            debug(f'Marked {self} as exhausted')

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
            debug(f'{self.name}: Setting exception: {e!r}')
            self._exception = e
        debug(f'{self.name}: Bye')

    def join(self):
        self._thread.join()
        if self._exception:
            raise self._exception
        return self


class Reader():
    def __init__(self, filepaths, piece_size, queue_size,
                 file_sizes=defaultdict(lambda: None), error_callback=None):
        self._filepaths = tuple(filepaths)
        self._file_sizes = file_sizes
        self._piece_size = piece_size
        self._error_callback = error_callback
        self._piece_queue = ExhaustQueue(name='pieces', maxsize=queue_size)
        self._bytes_read = 0
        self._piece_buffer = bytearray()
        self._skip_files = []
        self._stop = False

    def read(self):
        if self._stop or self._piece_queue.is_exhausted:
            raise RuntimeError(f'Cannot read from the same instance multiple times.')
        try:
            for filepath in self._filepaths:
                if self._stop:
                    debug(f'reader: Stopped reading after {self._calc_piece_index()} pieces')
                    break
                elif filepath in self._skip_files:
                    debug(f'reader: Found {filepath} in {self._skip_files} before opening it')
                    self._fake_read_file(filepath, self._file_sizes[filepath])
                    continue
                else:
                    self._read_file(filepath, self._piece_size)

            # Unless the torrent's total size is divisible by its piece size,
            # the last bytes from the last file are still in piece_buffer
            if (not self._stop and
                filepath not in self._skip_files and
                len(self._piece_buffer) > 0):
                debug(f'Left over bytes in buffer: {len(self._piece_buffer)}')
                # Piece index is only incremented if the buffered bytes don't
                # fit in the current piece
                piece_index = self._calc_piece_index()
                if self._bytes_read + len(self._piece_buffer) > self._piece_size:
                    piece_index += 1
                debug(f'reader: Sending last piece {piece_index} to {self._piece_queue}')
                self._piece_queue.put((piece_index, bytes(self._piece_buffer), filepath))
        finally:
            self._piece_queue.exhausted()
            self._stop = True

    def _read_file(self, filepath, piece_size):
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
                    self._fake_read_file(filepath, spec_filesize)
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
                    self._piece_queue.put((piece_index, piece, filepath))

                debug(f'Piece buffer contains {len(piece_buffer)} bytes')

            if spec_filesize:
                assert bytes_read == spec_filesize
            self._bytes_read += bytes_read

        except Exception as e:
            debug(f'reader: {e!r}')
            if spec_filesize is None:
                # We cannot calculate piece_index unless we know file's size,
                # and there's no point in going on if we don't know where a
                # piece begins and ends
                raise
            else:
                # Add the bytes we actually read from disk to calculate the
                # correct piece index for this error
                piece_index = self._calc_piece_index(self._bytes_read + bytes_read) + 1
                self._handle_error(e, filepath, piece_index)
                if spec_filesize is not None:
                    # Pretend we've read the whole file so we can calculate
                    # correct piece indexes for future files
                    self._bytes_read += spec_filesize

    def _fake_read_file(self, filepath, spec_filesize):
        # Pretend we did read `filepath` so we can calculate piece_index after
        # skipping files
        if spec_filesize is None:
            raise RuntimeError(f'Unable to fake reading {filepath} without file size')
        debug(f'reader: Pretending to have read {self._bytes_read} + {spec_filesize} '
              f'= {self._bytes_read+spec_filesize} bytes in total')
        self._bytes_read += spec_filesize

        # Unless all previous files were divisible by piece_size, the missing
        # file's last piece also contains bytes from the beginning of the next
        # file.
        self._piece_buffer.clear()
        trailing_bytes_len = self._bytes_read % self._piece_size
        try:
            # Read the beginning of the first piece of file3 from the end of
            # file2.
            with open(filepath, 'rb') as f:
                f.seek(-trailing_bytes_len, os.SEEK_END)
                self._piece_buffer.extend(f.read(trailing_bytes_len))
                debug(f'reader: Read {len(self._piece_buffer)} trailing bytes '
                      f'from {filepath}: {self._piece_buffer}')
        except OSError:
            # Pretend we read and buffered the last bytes from `filepath`.
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
        piece_index = max(0, int(bytes_read / self._piece_size) - 1)
        return piece_index

    def _handle_error(self, exc, *args):
        if self._error_callback is not None:
            self._error_callback(exc, *args)
        else:
            raise exc

    def stop(self):
        debug(f'reader: Setting stop flag')
        self._stop = True
        return self

    @property
    def piece_queue(self):
        return self._piece_queue


class HashWorkerPool():
    def __init__(self, workers_count, piece_queue):
        self._piece_queue = piece_queue
        self._hash_queue = ExhaustQueue(name='hashes')
        self._workers_count = workers_count
        self._stop = False
        self._name_counter = _count().__next__
        self._name_counter()  # Consume 0 so first worker is 1
        self._name_counter_lock = threading.Lock()
        self._pool = ThreadPool(workers_count, self._worker)

    def _get_new_worker_name(self):
        with self._name_counter_lock:
            return f'hasher #{self._name_counter()}'

    def _worker(self):
        name = self._get_new_worker_name()
        piece_queue = self._piece_queue
        hash_queue = self._hash_queue
        while True:
            try:
                piece_index, piece, filepath = piece_queue.get()
            except QueueExhausted:
                debug(f'{name}: {piece_queue} is exhausted')
                break
            else:
                piece_hash = sha1(piece).digest()
                debug(f'{name}: Sending hash of piece {piece_index} to {hash_queue}')
                hash_queue.put((piece_index, piece_hash, filepath))
            if self._stop:
                debug(f'{name}: Stop flag found')
                break
        debug(f'{name}: Bye')

    def stop(self):
        debug(f'hasherpool: Stopping hasher pool')
        self._stop = True
        return self

    def join(self):
        debug(f'hasherpool: Joining {self._workers_count} workers')
        self._pool.close()
        self._pool.join()
        self._hash_queue.exhausted()
        debug(f'hasherpool: All workers joined')
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
                piece_index, piece_hash, filepath = hash_queue.get()
            except QueueExhausted:
                debug(f'collector: {hash_queue} is exhausted')
                break
            else:
                debug(f'collector: Collected piece hash of piece {piece_index} of {filepath}')
                hashes_unsorted.append((piece_index, piece_hash))
                if callback:
                    callback(filepath, len(hashes_unsorted), piece_index, piece_hash)
            if self._stop:
                debug(f'collector: Stop flag found while getting piece hash')
                break
        # Sort hashes by piece_index and concatenate them
        self._hashes = b''.join(hash for index,hash in sorted(hashes_unsorted))

    def stop(self):
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
                if self._callback(*cb_args) is not None:
                    self._cancelled()
                    return True
                return False
            except BaseException as e:
                self._cancelled()
                raise

    def _cancelled(self):
        for cb in self._cancel_callbacks:
            cb()

    def on_cancel(self, *funcs):
        self._cancel_callbacks.extend(funcs)
