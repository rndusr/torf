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
        self._skip_file_on_first_error = skip_file_on_first_error
        self._skip_files = set()
        self._noskip_piece_indexes = set()
        self._forced_error_piece_indexes = set()
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
                    bytes_chunked, trailing_bytes = self._fake_read_file(filepath, 0, trailing_bytes)
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
                    self.skip_file(filepath, piece_index)
                    self._expect_corruption(self._get_next_filepath(filepath))

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
                    bytes_chunked, trailing_bytes = self._fake_read_file(filepath, bytes_chunked, trailing_bytes)
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
                piece_index = self._calc_piece_index(bytes_chunked + len(trailing_bytes) + 1)
                debug(f'reader: Reporting read exception for piece index {piece_index}: {exc!r}')
                self._push(piece_index, None, filepath, exc)
                self.skip_file(filepath, piece_index)
                bytes_chunked, trailing_bytes = self._fake_read_file(filepath, bytes_chunked, trailing_bytes)

        debug(f'reader: Remembering {len(trailing_bytes)} trailing bytes '
              f'from {os.path.basename(filepath)}: {debug.pretty_bytes(trailing_bytes)}')
        return bytes_chunked, trailing_bytes

    def _fake_read_file(self, filepath, bytes_chunked, trailing_bytes):
        # Pretend to read `filepath` to properly report progress and to allow us
        # tocalculate piece_index after skipping files or if a file is missing.
        # `bytes_chunked` is the number of bytes we've already read from
        # `filepath`, excluding any trailing bytes.
        piece_size = self._piece_size
        spec_filesize = self._file_sizes[filepath]
        if spec_filesize is None:
            raise RuntimeError(f'Unable to fake reading {filepath} without file size')
        debug(f'reader: Fake reading {os.path.basename(filepath)} after chunking {bytes_chunked} bytes from it '
              f'and {self._bytes_chunked} in from previous files')
        remaining_bytes = spec_filesize - bytes_chunked + len(trailing_bytes)
        debug(f'reader: Remaining bytes to fake: {remaining_bytes}')

        # Report the first piece of `filepath` as broken if it contains bytes
        # from the previous file.  It's possible that we don't have enough bytes
        # for a full piece, which happens if we fake the first file and it is
        # smaller than piece_size.  In that case, the error will be reported
        # when the next file is read.
        if trailing_bytes and len(trailing_bytes) + remaining_bytes >= piece_size:
            remaining_bytes -= piece_size
            bytes_chunked += piece_size
            piece_index = self._calc_piece_index(bytes_chunked)
            debug(f'reader: Sending fake piece_index {piece_index} from previous file')
            # Send b'' to guarantee a hash mismatch.  Sending fake bytes might
            # *not* raise an error if they replicate the missing data.
            self._push(piece_index, b'', filepath, None)
        trailing_bytes = b''

        # Fake pieces that exclusively belong to `filepath`.  Send `None` as
        # piece chunks to indicate the fake while providing progress status.
        while remaining_bytes >= piece_size:
            if self._stop:
                debug(f'reader: Found stop signal while fake-reading from {os.path.basename(filepath)}')
                return bytes_chunked, b''
            remaining_bytes -= piece_size
            bytes_chunked += piece_size
            piece_index = self._calc_piece_index(bytes_chunked)
            debug(f'reader: {piece_index}: Faked {bytes_chunked} bytes from {os.path.basename(filepath)}, '
                  f'{self._bytes_chunked + bytes_chunked} bytes in total, {remaining_bytes} bytes remaining')
            self._push(piece_index, None, filepath, None)

        debug(f'reader: Remaining bytes to fake: {remaining_bytes}')
        if remaining_bytes > 0:
            next_filepath = self._get_next_filepath(filepath)
            # Expect corruption in next piece
            self._expect_corruption(next_filepath)
            next_piece_index = self._calc_piece_index(bytes_chunked + remaining_bytes)
            if next_filepath is not None:
                # This is not the last file.  Fake trailing_bytes so the next
                # piece isn't shifted in the stream.
                trailing_bytes = b'\x00' * remaining_bytes
                debug(f'reader: Pretending to read {len(trailing_bytes)} trailing bytes '
                      f'from {os.path.basename(filepath)}: {debug.pretty_bytes(trailing_bytes)}')
                # It's possible that our faked trailing_bytes are identical to
                # the missing data, meaning that the next piece will not raise a
                # hash mismatch.
                self._forced_error_piece_indexes.add(next_piece_index)
                debug(f'reader: Forcing hash mismatch for piece_indexes: {self._forced_error_piece_indexes}')
            else:
                # This is the last file in the stream.  We don't want the final
                # piece to produce an error, so we fake it right now.  The error is
                # avoided by returning no trailing_bytes.
                debug(f'reader: Faking final piece_index {next_piece_index}')
                self._push(next_piece_index, None, filepath, None)
                bytes_chunked += remaining_bytes

        return bytes_chunked, trailing_bytes

    @property
    def skipped_files(self):
        return self._skip_files

    def file_was_skipped(self, filepath):
        if self._skip_file_on_first_error and filepath in self._skip_files:
            return True
        return False

    def skip_file(self, filepath, piece_index):
        if self._skip_file_on_first_error and filepath not in self._skip_files:
            if piece_index not in self._noskip_piece_indexes:
                debug(f'Marking {os.path.basename(filepath)} for skipping because of piece_index {piece_index} '
                      f'after chunking {int(self._bytes_chunked / self._piece_size)} chunks')
                self._skip_files.add(filepath)
            else:
                debug(f'Not skipping {os.path.basename(filepath)} because of expected '
                      f'corrupt piece_index {piece_index}: {self._noskip_piece_indexes}')

    # When we fake-read a file, we cannot verify the first piece of the next
    # file (unless the faked file perfectly ends at a piece boundary), so we
    # report an error for that first piece If skip_file_on_first_error is True,
    # that means the next file is skipped even if it is completely fine and we
    # just couldn't confirm that.
    def _expect_corruption(self, filepath):
        if filepath is not None:
            debug(f'reader: Expecting corruption in first piece of {os.path.basename(filepath)}')
            file_beg = self._calc_file_start(filepath)
            piece_index = self._calc_piece_index(absolute_pos=file_beg)
            debug(f'reader: {os.path.basename(filepath)} starts at byte {file_beg}, piece_index {piece_index}')
            self._noskip_piece_indexes.add(piece_index)
            debug(f'reader: Never skipping {os.path.basename(filepath)} because of '
                  f'the following piece_indexes: {self._noskip_piece_indexes}')

    def _calc_piece_index(self, additional_bytes_chunked=0, absolute_pos=0):
        if absolute_pos:
            return absolute_pos // self._piece_size
        else:
            bytes_chunked = self._bytes_chunked + additional_bytes_chunked
            # bytes_chunked is the number of bytes, but we want the index of the
            # last byte that was chunked, hence -1.
            return max(0, bytes_chunked - 1) // self._piece_size

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
        if self._stop:
            debug(f'reader: Found stop signal just before sending piece_index {piece_index}')
            return
        elif piece_index in self._forced_error_piece_indexes:
            # We know this piece is corrupt, even if our padding replicates the missing data.
            debug(f'reader: Forcing hash mismatch for piece_index {piece_index}')
            piece = b''
        elif piece is not None:
            piece = bytes(piece)
        self._piece_queue.put((int(piece_index), piece, filepath, exc))
        debug(f'reader: Pushed piece_index {piece_index} [{self._piece_queue.qsize()}]')

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
        if not self._stop:
            debug(f'reader: Setting stop flag')
            self._stop = True
        return self

    @property
    def piece_queue(self):
        return self._piece_queue


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
        debug(f'{name}: Bye, piece_queue has {piece_queue.qsize()} items left')

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
        debug(f'collector: Collected {len(self._hashes_unsorted)} pieces')
        debug(f'collector: Bye, hash_queue has {self._hash_queue.qsize()} items left')

    def _work(self, piece_index, piece_hash, filepath, exc):
        # A piece can be reported twice, but we don't want to increase
        # pieces_done in that case
        self._pieces_seen.add(piece_index)
        # In case a piece from a skipped file was already hashed and enqueued,
        # act like drop the hash and report it as skipped.
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
