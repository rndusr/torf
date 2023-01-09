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

import errno
import logging
import os
import queue
import threading
from hashlib import sha1
from time import monotonic as time_monotonic

from . import _errors as errors
from ._stream import TorrentFileStream

QUEUE_CLOSED = object()

_debug = logging.getLogger('torf').debug

def _thread_name():
    return threading.current_thread().name

def _pretty_bytes(b):
    if isinstance(b, (bytes, bytearray)) and len(b) > 8:
        # return b[:8].hex() + '...' + b[-8:].hex()
        return b[:8] + b'...' + b[-8:]
    else:
        return b


class Worker:
    """
    :class:`threading.Thread` subclass that re-raises any exceptions from the
    thread when joined
    """

    def __init__(self, name, worker, start=True, fail_ok=False):
        self._exception = None
        self._name = str(name)
        self._worker = worker
        self._thread = threading.Thread(name=self._name, target=self._run_and_catch_exceptions)
        if start:
            self.start(fail_ok=fail_ok)

    @property
    def exception(self):
        return self._exception

    @property
    def name(self):
        return self._name

    @property
    def is_running(self):
        return self._thread.is_alive()

    def _run_and_catch_exceptions(self):
        try:
            self._worker()
        except BaseException as e:
            self._exception = e

    def start(self, fail_ok=False):
        if not self._thread.is_alive():
            try:
                self._thread.start()
            except RuntimeError as e:
                if fail_ok:
                    _debug(f'{self.name}: Failed to start thread: {e!r} - but that\'s ok')
                else:
                    _debug(f'{self.name}: Failed to start thread: {e!r}')
                    raise
            else:
                _debug(f'{self.name}: Started')

    def join(self, *args, **kwargs):
        if self.is_running:
            self._thread.join(*args, **kwargs)
        if self._exception:
            raise self._exception


class Reader(Worker):
    """
    :class:`Worker` subclass that reads files in pieces and pushes them to a
    queue
    """

    def __init__(self, *, torrent, queue_size, path=None):
        self._torrent = torrent
        self._path = path
        self._piece_queue = queue.Queue(maxsize=queue_size)
        self._stop = False
        self._memory_error_timestamp = -1
        super().__init__(name='reader', worker=self._push_pieces)

    def _push_pieces(self):
        stream = TorrentFileStream(self._torrent)
        try:
            iter_pieces = stream.iter_pieces(self._path, oom_callback=self._handle_oom)
            for piece_index, (piece, filepath, exceptions) in enumerate(iter_pieces):
                # _debug(f'{_thread_name()}: Read #{piece_index}')
                if self._stop:
                    _debug(f'{_thread_name()}: Stopped reading')
                    break
                elif exceptions:
                    self._push_piece(piece_index=piece_index, filepath=filepath, exceptions=exceptions)
                elif piece:
                    self._push_piece(piece_index=piece_index, filepath=filepath, piece=piece)
                else:
                    # `piece` is None because of missing file, and the exception
                    # was already sent for the first `piece_index` of that file
                    self._push_piece(piece_index=piece_index, filepath=filepath)

                # _debug(f'{_thread_name()}: {self._piece_queue.qsize()} pieces queued')

        except BaseException as e:
            _debug(f'{_thread_name()}: Exception while reading: {e!r}')
            raise

        finally:
            self._piece_queue.put(QUEUE_CLOSED)
            _debug(f'{_thread_name()}: Piece queue is now exhausted')
            stream.close()

    def _push_piece(self, *, piece_index, filepath, piece=None, exceptions=()):
        # _debug(f'{_thread_name()}: Pushing #{piece_index}: {filepath}: {_pretty_bytes(piece)}, {exceptions!r}')
        self._piece_queue.put((piece_index, filepath, piece, exceptions))

    def _handle_oom(self, exception):
        # Reduce piece_queue.maxsize by 1 every 100ms until the MemoryErrors stop
        now = time_monotonic()
        time_diff = now - self._memory_error_timestamp
        if time_diff >= 0.1:
            old_maxsize = self._piece_queue.maxsize
            new_maxsize = max(1, int(old_maxsize * 0.9))
            if new_maxsize != old_maxsize:
                _debug(f'{_thread_name()}: Reducing piece_queue.maxsize to {new_maxsize}')
                self._piece_queue.maxsize = new_maxsize
                self._memory_error_timestamp = now
            else:
                raise errors.ReadError(errno.ENOMEM, exception)

    def stop(self):
        """Stop reading and close the piece queue"""
        if not self._stop:
            _debug(f'{_thread_name()}: {type(self).__name__}: Setting stop flag')
            self._stop = True

    @property
    def piece_queue(self):
        """
        :class:`queue.Queue` instance that gets evenly sized pieces from the
        concatenated stream of files
        """
        return self._piece_queue


class HasherPool:
    """
    Wrapper around one or more :class:`Worker` instances that each read a piece
    from :attr:`Reader.piece_queue`, feed it to :func:`~.hashlib.sha1`, and push
    the resulting hash to :attr:`hash_queue`
    """

    def __init__(self, hasher_threads, piece_queue):
        self._piece_queue = piece_queue
        self._hash_queue = queue.Queue()
        self._finalize_event = threading.Event()

        # Janitor takes care of closing the hash queue, removing idle hashers, etc
        self._janitor = Worker(
            name='janitor',
            worker=self._janitor_thread,
            start=False,
        )

        # Hashers read from piece_queue and push to hash_queue
        self._hashers = [
            Worker(
                name='hasher1',
                # One hasher is vital an may not die from boredom
                worker=lambda: self._hasher_thread(is_vital=True),
                start=False,
            ),
        ]
        for i in range(2, hasher_threads + 1):
            self._hashers.append(
                Worker(
                    name=f'hasher{i}',
                    # All other hashers should die if they are bored
                    worker=lambda: self._hasher_thread(is_vital=False),
                    start=False,
                )
            )

        # Start threads manually after they were created to prevent race
        # conditions and make sure all required threads are running
        self._janitor.start(fail_ok=False)

        # Hashers are allowed to fail (e.g. because of OS limits), but we need
        # at least one to start successfully
        self._hashers[0].start(fail_ok=False)
        for hasher in self._hashers[1:]:
            hasher.start(fail_ok=True)

    def _hasher_thread(self, is_vital=True):
        piece_queue = self._piece_queue
        handle_piece = self._handle_piece
        while True:
            # _debug(f'{_thread_name()}: Waiting for next task')
            try:
                task = piece_queue.get(timeout=0.5)
            except queue.Empty:
                if not is_vital:
                    _debug(f'{_thread_name()}: I am bored, byeee!')
                    break
                else:
                    _debug(f'{_thread_name()}: I am bored, but needed.')
            else:
                if task is QUEUE_CLOSED:
                    _debug(f'{_thread_name()}: piece_queue is closed')
                    # Repeat QUEUE_CLOSED to the next sibling. This ensures
                    # there is always one more QUEUE_CLOSED queued than running
                    # threads. Otherwise, one thread might consume multiple
                    # QUEUE_CLOSED and leave other threads running forvever.
                    piece_queue.put(QUEUE_CLOSED)
                    # Signal janitor to initiate shutdown procedure
                    self._finalize_event.set()
                    break
                else:
                    handle_piece(*task)

    def _handle_piece(self, piece_index, filepath, piece, exceptions):
        if exceptions:
            # _debug(f'{_thread_name()}: Forwarding exceptions for #{piece_index}: {exceptions!r}')
            self._hash_queue.put((piece_index, filepath, None, exceptions))

        elif piece:
            piece_hash = sha1(piece).digest()
            # _debug(f'{_thread_name()}: Hashed #{piece_index}: {_pretty_bytes(piece)} [{len(piece)} bytes] -> {piece_hash}')
            self._hash_queue.put((piece_index, filepath, piece_hash, ()))

        else:
            # _debug(f'{_thread_name()}: Nothing to hash for #{piece_index}: {piece!r}')
            self._hash_queue.put((piece_index, filepath, None, ()))

    def _janitor_thread(self):
        while True:
            _debug(f'{_thread_name()}: Waiting for finalize event')
            finalization_initiated = self._finalize_event.wait(timeout=1.0)
            if finalization_initiated:
                self._wait_for_hashers()
                _debug(f'{_thread_name()}: Closing hash queue')
                self._hash_queue.put(QUEUE_CLOSED)
                break

            else:
                # Remove terminated idle hashers
                for hasher in tuple(self._hashers):
                    if not hasher.is_running:
                        _debug(f'{_thread_name()}: Pruning {hasher.name}')
                        self._hashers.remove(hasher)

        _debug(f'{_thread_name()}: Terminating')

    def _wait_for_hashers(self):
        while True:
            # _debug(f'{_thread_name()}: Hashers running: {[h.name for h in self._hashers if h.is_running]}')
            if all(not h.is_running for h in self._hashers):
                _debug(f'{_thread_name()}: All hashers terminated')
                break

    def join(self):
        """Block until all threads have terminated"""
        for hasher in self._hashers:
            _debug(f'{_thread_name()}: Joining {hasher.name}')
            hasher.join()
        _debug(f'{_thread_name()}: Joined all hashers')

        _debug(f'{_thread_name()}: Joining {self._janitor.name}')
        self._janitor.join()
        _debug(f'{_thread_name()}: Joined {self._janitor.name}')

    @property
    def hash_queue(self):
        """:class:`queue.Queue` instance that gets piece hashes"""
        return self._hash_queue


class Collector:
    """
    Consume items from :attr:`HasherPool.hash_queue` and ensure proper
    termination of all threads if anything goes wrong or the user cancels the
    operation
    """

    def __init__(self, torrent, reader, hashers, callback=None):
        self._reader = reader
        self._hashers = hashers
        self._callback = callback
        self._hashes_unsorted = []
        self._pieces_seen = []
        self._pieces_total = torrent.pieces

    def collect(self):
        """
        Read piece hashes from :attr:`HasherPool.hash_queue`

        When this method returns, :attr:`hashes` is an ordered sequence of
        collected piece hashes.

        Exceptions from :class:`Reader`, :class:`HasherPool` or the provided
        callback are raised after all threads are terminated and joined.

        :return: the same value as :attr:`hashes`
        """
        try:
            hash_queue = self._hashers.hash_queue
            while True:
                # _debug(f'{_thread_name()}: Waiting for next piece hash')
                task = hash_queue.get()
                # _debug(f'{_thread_name()}: Got task: {task}')
                if task is QUEUE_CLOSED:
                    break
                else:
                    self._collect(*task)

        except BaseException as e:
            _debug(f'{_thread_name()}: Exception while dequeueing piece hashes: {e!r}')
            self._cancel()
            raise

        finally:
            self._finalize()

        return self.hashes

    def _collect(self, piece_index, filepath, piece_hash, exceptions):
        # _debug(f'{_thread_name()}: Collecting #{piece_index}: {_pretty_bytes(piece_hash)}, {exceptions}')

        # Remember which pieces where hashed to count them and for sanity checking
        assert piece_index not in self._pieces_seen
        self._pieces_seen.append(piece_index)

        # Collect piece
        if not exceptions and piece_hash:
            self._hashes_unsorted.append((piece_index, piece_hash))

        # If there is no callback, raise first exception
        if exceptions and not self._callback:
            raise exceptions[0]

        # Report progress/exceptions and allow callback to cancel
        if self._callback:
            # _debug(f'{_thread_name()}: Collector callback: {self._callback}')
            maybe_cancel = self._callback(
                piece_index, len(self._pieces_seen), self._pieces_total,
                filepath, piece_hash, exceptions,
            )
            # _debug(f'{_thread_name()}: Collector callback return value: {maybe_cancel}')
            if maybe_cancel is not None:
                self._cancel()

    def _cancel(self):
        # NOTE: We don't need to stop HasherPool or Collector.collect() because
        #       they will stop when Reader._push_pieces() pushes QUEUE_CLOSED.
        #       They will process the pieces in the queue, but that shouldn't
        #       take long unless the Reader's queue size is too big.
        self._reader.stop()

    def _finalize(self):
        _debug(f'{_thread_name()}: Joining {self._reader}')
        self._reader.join()
        _debug(f'{_thread_name()}: Joining {self._hashers}')
        self._hashers.join()
        _debug(f'{_thread_name()}: hash_queue has {self._hashers.hash_queue.qsize()} items left')

    @property
    def hashes(self):
        """Ordered sequence of piece hashes"""
        return tuple(hash for index, hash in sorted(self._hashes_unsorted))


class _IntervaledCallback:
    """
    Callable that calls `callback`, but only if at least `interval` seconds
    elapsed since the previous call
    """
    def __init__(self, callback, interval=0):
        self._callback = callback
        self._interval = interval
        self._prev_call_time = -1

    def __call__(self, *args, force=False):
        now = time_monotonic()
        diff = now - self._prev_call_time
        # _debug(f'{_thread_name()}: Callback? {force=} or {diff=} >= {self._interval=}')
        if force or diff >= self._interval:
            self._prev_call_time = now
            # _debug(f'{_thread_name()}: Callback! {args=}')
            return self._callback(*args)


class _TranslatingCallback:
    def __init__(self, callback, interval, torrent):
        self._callback = callback
        self._torrent = torrent
        self._intervaled_callback = _IntervaledCallback(
            callback=self._call_callback,
            interval=interval,
        )

    def __call__(self, piece_index, pieces_done, pieces_total, filepath, piece_hash, exceptions):
        force = self._force_callback(piece_index, pieces_done, pieces_total,
                                     filepath, piece_hash, exceptions)
        return self._intervaled_callback(piece_index, pieces_done, pieces_total,
                                         filepath, piece_hash, exceptions,
                                         force=force)

    def _force_callback(self, piece_index, pieces_done, pieces_total, filepath, piece_hash, exceptions):
        # Figure out if we must ignore the interval for this call. This method
        # is called for every hashed piece and should be as efficient as
        # possible.
        raise NotImplementedError('You must implement this method!')

    def _call_callback(self, piece_index, pieces_done, pieces_total, filepath, piece_hash, exceptions):
        # Translate arguments for the actual callback. This method is only
        # called at intervals (e.g. once per second).
        raise NotImplementedError('You must implement this method!')


class GenerateCallback(_TranslatingCallback):
    """
    Translate arguments from :class:`Collector` to what's specified by
    :meth:`~.Torrent.generate`
    """

    def _force_callback(self, piece_index, pieces_done, pieces_total, filepath, piece_hash, exceptions):
        return exceptions or pieces_done >= pieces_total

    def _call_callback(self, piece_index, pieces_done, pieces_total, filepath, piece_hash, exceptions):
        if exceptions:
            # Torrent creation errors are always fatal and must be raised
            raise exceptions[0]
        elif self._callback:
            # Report progress and allow callback to cancel
            return self._callback(self._torrent, filepath, pieces_done, pieces_total)


class VerifyCallback(_TranslatingCallback):
    """
    Translate arguments from :class:`Collector` to what's specified by
    :meth:`~.Torrent.verify`
    """
    def __init__(self, *args, path, **kwargs):
        super().__init__(*args, **kwargs)

        # Store piece hashes from the torrent for quick access
        self._exp_hashes = self._torrent.hashes

        # Map expected file system paths to expected file sizes
        # NOTE: The last segment in `path` is supposed to be the torrent name so
        #       we must remove the name stored in the torrent file from each
        #       `file`. This allows verification of any renamed file/directory
        #       against a torrent.
        self._exp_file_sizes = tuple(
            (
                os.sep.join((str(path), *file.parts[1:])),
                self._torrent.partial_size(file),
            )
            for file in self._torrent.files
        )

    def _force_callback(self, piece_index, pieces_done, pieces_total, filepath, piece_hash, exceptions):
        return (
            # Always report exceptions
            exceptions
            # Always report completion
            or pieces_done >= pieces_total
            # Always report hash mismatch
            or piece_hash is not None and piece_hash != self._exp_hashes[piece_index]
        )

    def _call_callback(self, piece_index, pieces_done, pieces_total, filepath, piece_hash, exceptions):
        if (
            # Don't add verification error if there are other errors
            not exceptions
            # Piece hash was calculated and doesn't match
            and piece_hash is not None and piece_hash != self._exp_hashes[piece_index]
        ):
            exceptions = (errors.VerifyContentError(
                filepath, piece_index, self._torrent.piece_size, self._exp_file_sizes,
            ),)

        if self._callback:
            # Callback can raise exception or handle it otherwise
            def call_callback(fpath, exception):
                return self._callback(
                    self._torrent, fpath,
                    pieces_done, pieces_total, piece_index,
                    piece_hash, exception,
                )

            if exceptions:
                # Call callback for each exception until it indicates
                # cancellation by returning anything truthy
                for exception in exceptions:
                    fpath = self._get_path_from_exception(exception)
                    maybe_cancel = call_callback(fpath, exception)
                    if maybe_cancel is not None:
                        return maybe_cancel
            else:
                # Report progress and return cancellation indicator
                return call_callback(filepath, None)

        elif exceptions:
            # Default to raising first exception
            raise exceptions[0]

    @staticmethod
    def _get_path_from_exception(exception):
        for attr in ('filepath', 'path'):
            try:
                return getattr(exception, attr)
            except AttributeError:
                pass

        raise RuntimeError(f'Failed to get path from {exception!r}')
