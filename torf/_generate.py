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
import time
from itertools import count as _count


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


class ReadWorker(Worker):
    def __init__(self, torrent, queue_size):
        self._torrent = torrent
        self._piece_queue = ExhaustQueue(name='pieces', maxsize=queue_size)
        self._stop = False
        super().__init__(name='reader', worker=self._run)

    def _run(self):
        try:
            if self._torrent.mode == 'singlefile':
                self._run_singlefile()
            elif self._torrent.mode == 'multifile':
                self._run_multifile()
            else:
                raise RuntimeError('Unexpected mode: {self._torrent.mode}')
        finally:
            self._piece_queue.exhausted()

    def _run_singlefile(self):
        torrent = self._torrent
        filepath = torrent.path
        piece_queue = self._piece_queue
        debug(f'{self.name}: Reading single file from {filepath}')
        for piece_index,piece in enumerate(utils.read_chunks(filepath, torrent.piece_size)):
            item = (piece_index, piece, filepath)
            debug(f'{self.name}: Sending hash piece {piece_index} to {piece_queue}')
            piece_queue.put(item)
            if self._stop:
                debug(f'{self.name}: Stopped reading after {piece_index+1} pieces')
                return

    def _run_multifile(self):
        torrent = self._torrent
        piece_buffer = bytearray()
        piece_index = 0
        piece_size = torrent.piece_size
        piece_queue = self._piece_queue
        debug(f'{self.name}: Reading from multiple files: {torrent.filepaths}')
        for filepath in torrent.filepaths:
            for chunk in utils.read_chunks(filepath, piece_size):
                # Concatenate chunks across files; if we have enough for a new
                # piece, put it in piece_queue
                piece_buffer.extend(chunk)
                if len(piece_buffer) >= piece_size:
                    piece = piece_buffer[:piece_size]
                    del piece_buffer[:piece_size]
                    debug(f'{self.name}: Sending piece {piece_index} to {piece_queue}')
                    piece_queue.put((piece_index, piece, filepath))
                    piece_index += 1
                if self._stop:
                    debug(f'{self.name}: Stopped reading after {piece_index+1} pieces')
                    return

        # Unless torrent.size is divisible by torrent.piece_size, there are some
        # bytes left in piece_buffer
        if len(piece_buffer) > 0:
            debug(f'{self.name}: Sending piece {piece_index} to {piece_queue}')
            piece_queue.put((piece_index, piece_buffer, filepath))

    def stop(self):
        debug(f'{self.name}: Setting stop flag')
        self._stop = True
        return self

    @property
    def piece_queue(self):
        return self._piece_queue


class HashWorkerPool():
    def __init__(self, workers_count, piece_queue):
        self.piece_queue = piece_queue
        self.hash_queue = ExhaustQueue(name='hashes')
        self._workers_count = workers_count
        self._stop = False
        self._pool = ThreadPool(workers_count, self._worker)

    _name_counter = _count().__next__
    _name_counter_lock = threading.Lock()
    @classmethod
    def _get_new_worker_name(cls):
        with cls._name_counter_lock:
            return f'hasher #{cls._name_counter()}'

    def _worker(self):
        name = self._get_new_worker_name()
        piece_queue = self.piece_queue
        hash_queue = self.hash_queue
        while True:
            try:
                debug(f'{name}: Getting from {piece_queue}')
                piece_index, piece, filepath = piece_queue.get()
                debug(f'{name}: Got piece {piece_index} from {piece_queue}')
            except QueueExhausted:
                debug(f'{name}: {piece_queue} is exhausted')
                break
            else:
                debug(f'{name}: Hashing piece {piece_index}')
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
        self.hash_queue.exhausted()
        debug(f'hasherpool: All workers joined')
        return self


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
                debug(f'collector: Got piece hash {piece_index} from {hash_queue}')
            except QueueExhausted:
                debug(f'collector: {hash_queue} is exhausted')
                break
            else:
                debug(f'collector: Collected piece hash of piece {piece_index}')
                hashes_unsorted.append((piece_index, piece_hash))
                if callback:
                    callback(filepath, len(hashes_unsorted))
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
    def __init__(self, callback, interval):
        self._callback = callback
        self._interval = interval
        self._prev_callback_time = None

    def __call__(self, torrent, filepath, pieces_done, pieces_total):
        now = time.monotonic()
        if (self._prev_callback_time is None or
            now - self._prev_callback_time >= self._interval or
            pieces_done >= pieces_total):
            self._prev_callback_time = now
            return self._callback(torrent, filepath, pieces_done, pieces_total)
