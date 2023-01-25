import errno
import functools
import hashlib
import itertools
import math
import os

from . import _errors as error


class TorrentFileStream:
    """
    Traverse concatenated files as they are described in a torrent

    :param torrent: :class:`~.torf.Torrent` object

    Files are opened on demand and kept open for re-use. It is recommended to
    make use of the context manager protocol to make sure they are properly
    closed when no longer needed.

    Example:

    >>> torrent = torf.Torrent(...)
    >>> with TorrentFileStream(torrent) as tfs:
    >>>     # Get the 29th piece of the concatenated file stream
    >>>     piece = tfs.get_piece(29)
    """

    def __init__(self, torrent, content_path=None):
        self._torrent = torrent
        self._content_path = content_path
        self._open_files = {}

    def _get_content_path(self, content_path, none_ok=False, file=None):
        # Get content_path argument from class or method call or from
        # Torrent.path attribute
        if content_path is not None:
            content_path = content_path
        elif self._content_path is not None:
            content_path = self._content_path
        elif self._torrent.path is not None:
            content_path = self._torrent.path
        elif none_ok:
            content_path = None
        else:
            raise ValueError('Missing content_path argument and torrent has no path specified')

        if self._torrent.mode == 'singlefile':
            # Torrent contains no directory, just a file
            return content_path or file

        # Torrent contains directory with one or more files in it
        if file is None:
            return content_path
        else:
            # Append internal path from torrent file
            if content_path:
                # Use the torrent name from `content_path`, not the one from the
                # torrent (i.e. the first path segment of `file`) so the user
                # can operate on renamed a directory/file (files and
                # subdirectories in multifile torrents still have to have the
                # same names)
                file_parts = list(file.parts)
                assert len(file_parts) >= 2, file_parts
                file_parts.pop(0)
                content_file_path = os.path.join(content_path, *file_parts)
                return type(file)(content_file_path, file.size)

            else:
                return file

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        """
        Close all opened files

        This is called automatically when the instance is used as a context
        manager.
        """
        for filepath, fh in tuple(self._open_files.items()):
            fh.close()
            del self._open_files[filepath]

    @property
    def max_piece_index(self):
        """Largest valid piece index (smallest is always 0)"""
        return math.floor((self._torrent.size - 1) / self._torrent.piece_size)

    def get_file_position(self, file):
        """
        Return index of first byte of `file` in stream of concatenated files

        :param file: :class:`~torf.File` object

        :raise ValueError: if `file` is not specified in the torrent
        """
        try:
            file_index = self._torrent.files.index(file)
        except ValueError:
            raise ValueError(f'File not specified: {file}')
        else:
            stream_pos = sum(f.size for f in self._torrent.files[:file_index])
            return stream_pos

    def get_file_at_position(self, position, content_path=None):
        """
        Return file that belongs to the byte at `position` in stream of concatenated
        files

        :param position: Byte index in the stream; minimum is 0, maximum is the
            torrent's size minus 1
        :param content_path: Path to file or directory (defaults to class
            argument of the same name, :attr:`~.Torrent.path` or the file path
            from the torrent)
        """
        if position >= 0:
            pos = 0
            for file in self._torrent.files:
                pos += file.size - 1
                if pos >= position:
                    return self._get_content_path(content_path, none_ok=True, file=file)
                else:
                    pos += 1

        raise ValueError(f'position is out of bounds (0 - {self._torrent.size - 1}): {position}')

    def get_piece_indexes_of_file(self, file, exclusive=False):
        """
        Return indexes of pieces that contain at least one byte of `file`

        :param bool exclusive: Whether to include only indexes of pieces that
            don't contain bytes of any other files, i.e. only return piece
            indexes that belong to `file` exlusively

        :param file: :class:`~torf.File` object
        """
        piece_size = self._torrent.piece_size
        stream_pos = self.get_file_position(file)
        first_piece_index = math.floor(stream_pos / piece_size)
        last_piece_index = math.floor((stream_pos + file.size - 1) / piece_size)
        piece_indexes = list(range(first_piece_index, last_piece_index + 1))

        if exclusive:
            # Remove first piece index if it's not exclusive
            files_in_first_piece = self.get_files_at_piece_index(first_piece_index)
            if files_in_first_piece != [file]:
                piece_indexes.remove(first_piece_index)

            # Remove last piece index if it's not exclusive
            files_in_last_piece = self.get_files_at_piece_index(last_piece_index)
            if last_piece_index in piece_indexes and files_in_last_piece != [file]:
                piece_indexes.remove(last_piece_index)

        return piece_indexes

    def get_files_at_byte_range(self, first_byte_index, last_byte_index, content_path=None):
        """
        Return list of files that have at least one byte at `first_byte_index`,
        `last_byte_index` or between those two in the stream of concatenated
        files

        :param content_path: Path to file or directory (defaults to class
            argument of the same name, :attr:`~.Torrent.path` or the file path
            from the torrent)
        """
        assert first_byte_index <= last_byte_index, (first_byte_index, last_byte_index)
        pos = 0
        files = []
        for file in self._torrent.files:
            file_first_byte_index = pos
            file_last_byte_index = pos + file.size - 1
            if (
                # Is first byte of file inside of range?
                first_byte_index <= file_first_byte_index <= last_byte_index or
                # Is last byte of file inside of range?
                first_byte_index <= file_last_byte_index <= last_byte_index or
                # Are all bytes of file inside of range?
                (first_byte_index >= file_first_byte_index and last_byte_index <= file_last_byte_index)
            ):
                content_file_path = self._get_content_path(content_path, none_ok=True, file=file)
                files.append(content_file_path)
            pos += file.size
        return files

    def get_byte_range_of_file(self, file):
        """
        Return index of first and last byte in the stream of concatenated files that
        contains at least one byte of `file`
        """
        start = self.get_file_position(file)
        return start, start + file.size - 1

    def get_files_at_piece_index(self, piece_index, content_path=None):
        """
        Return list of files that have 1 or more bytes in piece at `piece_index`

        :param piece_index: Index of the piece; minimum is 0, maximum is the
            torrent's number of pieces minus 1
        :param content_path: Path to file or directory (defaults to class
            argument of the same name, :attr:`~.Torrent.path` or the file path
            from the torrent)
        """
        if piece_index >= 0:
            piece_size = self._torrent.piece_size
            piece_start_pos = piece_index * piece_size
            piece_end_pos = ((piece_index + 1) * piece_size) - 1
            files = self.get_files_at_byte_range(
                piece_start_pos,
                piece_end_pos,
                content_path=content_path,
            )
            if files:
                return files

        raise ValueError(f'piece_index is out of bounds (0 - {self.max_piece_index}): {piece_index}')

    def get_absolute_piece_indexes(self, file, relative_piece_indexes):
        """
        Return list of validated absolute piece indexes

        :param file: :class:`~torf.File` object
        :param relative_piece_indexes: Sequence of piece indexes within `file`;
            negative values address pieces at the end of `file`, e.g. [0, 12,
            -1, -2]

        Example:

        >>> # Assume `file` starts in the 50th piece in the stream of
        >>> # concatenated files and is 100 pieces long. `1000` and `-1000` are
        >>> # ignored because they are out of bounds.
        >>> tfs.get_absolute_piece_indexes(file, (0, 1, 70, 75, 1000, -1000, -3, -2, -1))
        [50, 51, 120, 125, 147, 148, 149]
        """
        file_piece_indexes = self.get_piece_indexes_of_file(file)
        pi_abs_min = file_piece_indexes[0]
        pi_abs_max = file_piece_indexes[-1]
        pi_rel_min = 0
        pi_rel_max = pi_abs_max - pi_abs_min

        validated_piece_indexes = set()
        for pi_rel in relative_piece_indexes:
            pi_rel = int(pi_rel)

            # Convert negative to absolute index
            if pi_rel < 0:
                pi_rel = pi_rel_max - abs(pi_rel) + 1

            # Ensure relative piece_index is within bounds
            pi_rel = max(pi_rel_min, min(pi_rel_max, pi_rel))

            # Convert to absolute piece_index
            pi_abs = pi_abs_min + pi_rel
            validated_piece_indexes.add(pi_abs)

        return sorted(validated_piece_indexes)

    def get_relative_piece_indexes(self, file, relative_piece_indexes):
        """
        Return list of validated relative piece indexes

        :param file: :class:`~torf.File` object
        :param relative_piece_indexes: Sequence of piece indexes within `file`;
            negative values address pieces at the end of `file`, e.g. [0, 12,
            -1, -2]

        Example:

        >>> # Assume `file` starts in the 50th piece in the stream of
        >>> # concatenated files and is 100 pieces long. `1000` and `-1000` are
        >>> # ignored because they are out of bounds.
        >>> tfs.get_absolute_piece_indexes(file, (0, 1, 70, 75, 1000, -1000, -3, -2, -1))
        [0, 1, 70, 75, 97, 98, 99]
        """
        validated_piece_indexes = set()
        min_piece_index = 0
        max_piece_index = math.floor((file.size - 1) / self._torrent.piece_size)
        for rpi in relative_piece_indexes:
            valid_rpi = int(rpi)
            if rpi < 0:
                valid_rpi = max_piece_index - abs(rpi) + 1
            valid_rpi = max(min_piece_index, min(max_piece_index, valid_rpi))
            validated_piece_indexes.add(valid_rpi)
        return sorted(validated_piece_indexes)

    def get_piece(self, piece_index, content_path=None):
        """
        Return piece at `piece_index` or `None` for nonexisting file(s)

        :param piece_index: Index of the piece; minimum is 0, maximum is the
            torrent's number of pieces minus 1
        :param content_path: Path to file or directory to read piece from
            (defaults to class argument of the same name or
            :attr:`~.Torrent.path`)

        :raise ReadError: if a file exists but cannot be read
        :raise VerifyFileSizeError: if a file has unexpected size
        """
        piece_size = self._torrent.piece_size
        torrent_size = sum(f.size for f in self._torrent.files)

        min_piece_index = 0
        max_piece_index = math.floor((torrent_size - 1) / piece_size)
        if not min_piece_index <= piece_index <= max_piece_index:
            raise ValueError(
                'piece_index must be in range '
                f'{min_piece_index} - {max_piece_index}: {piece_index}'
            )

        # Find out which files we need to read from
        first_byte_index_of_piece = piece_index * piece_size
        last_byte_index_of_piece = min(
            first_byte_index_of_piece + piece_size - 1,
            torrent_size - 1,
        )
        relevant_files = self.get_files_at_byte_range(
            first_byte_index_of_piece,
            last_byte_index_of_piece,
            # Ensure we get the torrent path, not the file system path
            content_path='',
        )

        # Find out where to start reading in the first relevant file
        if len(relevant_files) == 1:
            # Our piece belongs to a single file
            file_pos = self.get_file_position(relevant_files[0])
            seek_to = first_byte_index_of_piece - file_pos
        else:
            # Our piece is spread over multiple files
            file = self.get_file_at_position(first_byte_index_of_piece, content_path='')
            file_pos = self.get_file_position(file)
            seek_to = file.size - ((file_pos + file.size) % piece_size)

        # Read piece data from `relevant_files`
        bytes_to_read = piece_size
        piece = bytearray()
        for file in relevant_files:
            # Translate path within torrent into path within file system
            filepath = self._get_content_path(content_path, none_ok=False, file=file)
            fh = self._get_open_file(filepath)

            # Complain about wrong file size. It's theoretically possible that a
            # file with the wrong size can produce the correct pieces, but that
            # would be unexpected.
            actual_file_size = self._get_file_size_from_fs(filepath)
            if actual_file_size != file.size:
                raise error.VerifyFileSizeError(filepath, actual_file_size, file.size)

            try:
                fh.seek(seek_to)
                seek_to = 0

                content = fh.read(bytes_to_read)
                bytes_to_read -= len(content)
                piece.extend(content)
            except OSError as e:
                raise error.ReadError(e.errno, file)

        # Ensure expected `piece` length
        if last_byte_index_of_piece == torrent_size - 1:
            exp_piece_size = torrent_size % piece_size
            if exp_piece_size == 0:
                exp_piece_size = piece_size
        else:
            exp_piece_size = piece_size
        assert len(piece) == exp_piece_size, (len(piece), exp_piece_size)
        return bytes(piece)

    def _get_file_size_from_fs(self, filepath):
        if os.path.exists(filepath):
            try:
                return os.path.getsize(filepath)
            except OSError:
                pass

    # Maximum number of open files (1024 seems to be a common maximum)
    max_open_files = 10

    def _get_open_file(self, filepath):
        if filepath not in self._open_files:
            # Prevent "Too many open files" (EMFILE)
            while len(self._open_files) > self.max_open_files:
                old_filepath = tuple(self._open_files)[0]
                self._open_files[old_filepath].close()
                del self._open_files[old_filepath]

            try:
                self._open_files[filepath] = open(filepath, 'rb')
            except OSError as e:
                raise error.ReadError(e.errno, filepath)

        return self._open_files.get(filepath, None)

    def iter_pieces(self, content_path=None, oom_callback=None):
        """
        Iterate over `(piece, filepath, (exception1, exception2, ...))`

        Each piece consists of :attr:`~.Torrent.piece_size` bytes, except for
        the final piece in the stream of concatenated files, which may be
        shorter.

        Filepaths are generated from `content_path` and the relative file paths
        from the torrent.

        Exceptions are :class:`~.TorfError` subclasses.

        If a file is not readable, pieces are `None` for each missing piece.
        This usually includes the last piece of the previous file and the first
        piece of the next file unless the unreadable file starts/ends right on a
        piece boundary.

        You can wrap this iterator in :func:`enumerate` to get the piece index
        for each piece:

        >>> for piece_index, (piece, filepath, exceptions) in stream.iter_pieces():
        >>>     ...

        :param content_path: Path to file or directory to read pieces from
            (defaults to class argument of the same name or
            :attr:`~.Torrent.path`)
        :param oom_callback: Callable that gets :class:`~.errors.MemoryError`
            instance

            Between calls to `oom_callback`, the piece that caused the exception
            is read again and again until it fits into memory. This callback
            offers a way to free more memory. If it fails, it is up to the
            callback to raise the exception or deal with it in some other way.

            If this is `None`, :class:`~.errors.MemoryError` is raised normally.

        :raise ReadError: if file exists but is not readable
        :raise VerifyFileSizeError: if file has unexpected size
        """
        trailing_bytes = b''
        missing_pieces = _MissingPieces(torrent=self._torrent, stream=self)
        skip_bytes = 0

        for file in self._torrent.files:
            if file in missing_pieces.bycatch_files:
                continue

            # Get expected file system path
            filepath = self._get_content_path(content_path, none_ok=False, file=file)

            # Get file handle or exception
            fh = exception = None
            actual_file_size = self._get_file_size_from_fs(filepath)
            if actual_file_size is not None and file.size != actual_file_size:
                exception = error.VerifyFileSizeError(filepath, actual_file_size, file.size)
            else:
                try:
                    fh = self._get_open_file(filepath)
                except error.ReadError as e:
                    exception = e

            # Make generator that yields `(piece, filepath, exceptions)` tuples
            if fh:
                # _debug(f'{file}: Reading {filepath}')
                # Read pieces from opened file
                pieces, skip_bytes = self._iter_from_file_handle(
                    fh,
                    prepend=trailing_bytes,
                    skip_bytes=skip_bytes,
                    oom_callback=oom_callback,
                )
                trailing_bytes = b''
                piece_size = self._torrent.piece_size
                for piece in pieces:
                    if len(piece) == piece_size:
                        yield (piece, filepath, ())
                    else:
                        trailing_bytes = piece

            else:
                # _debug(f'{file}: Faking {filepath}')
                # We can't complete the current piece
                trailing_bytes = b''
                # Opening file failed
                items, skip_bytes = missing_pieces(file, content_path, reason=exception)
                for item in items:
                    yield item

        # Yield last few bytes in stream unless stream size is perfectly
        # divisible by piece size
        if trailing_bytes:
            yield (trailing_bytes, filepath, ())

    def _iter_from_file_handle(self, fh, prepend, skip_bytes, oom_callback):
        # Read pieces from from file handle.
        # `prepend` is the incomplete piece from the previous file, i.e. the
        # leading bytes of the next piece.
        # `skip_bytes` is the number of bytes from `fh` to dump before
        # reading the next piece.

        if skip_bytes:
            skipped = fh.seek(skip_bytes)
            skip_bytes -= skipped

        def iter_pieces(fh, prepend):
            piece_size = self._torrent.piece_size
            piece = b''

            # Iterate over pieces in `prepend`ed bytes, store incomplete piece
            # in `piece`
            for pos in range(0, len(prepend), piece_size):
                piece = prepend[pos:pos + piece_size]
                if len(piece) == piece_size:
                    yield piece
                    piece = b''

            try:
                # Fill incomplete piece with first bytes from `fh`
                if piece:
                    piece += self._read_from_fh(
                        fh=fh,
                        size=piece_size - len(piece),
                        oom_callback=oom_callback,
                    )
                    yield piece

                # Iterate over `piece_size`ed chunks from `fh`
                while True:
                    piece = self._read_from_fh(
                        fh=fh,
                        size=piece_size,
                        oom_callback=oom_callback,
                    )
                    if piece:
                        yield piece
                    else:
                        break  # EOF

            except OSError as e:
                raise error.ReadError(e.errno, fh.name)

        return iter_pieces(fh, prepend), skip_bytes

    def _read_from_fh(self, fh, size, oom_callback):
        while True:
            try:
                return fh.read(size)
            except MemoryError as e:
                e = error.MemoryError(f'Out of memory while reading from {fh.name} at position {fh.tell()}')
                if oom_callback is None:
                    raise e
                else:
                    oom_callback(e)

    def get_piece_hash(self, piece_index, content_path=None):
        """
        Read piece at `piece_index` from file(s) and return its SHA1 hash

        :param piece_index: Index of the piece; minimum is 0, maximum is the
            torrent's number of pieces minus 1
        :param content_path: Path to file or directory to read piece from
            (defaults to class argument of the same name or
            :attr:`~.Torrent.path`)

        :raise ReadError: if a file exists but cannot be read
        :raise VerifyFileSizeError: if a file has unexpected size

        :return: :class:`bytes`
        """
        try:
            piece = self.get_piece(piece_index, content_path=content_path)
        except error.ReadError as e:
            if e.errno is errno.ENOENT:
                # No such file
                return None
            else:
                # Other read error, e.g. permission denied
                raise
        else:
            return hashlib.sha1(piece).digest()

    def verify_piece(self, piece_index, content_path=None):
        """
        Generate SHA1 hash for piece at `piece_index` and compare to the expected
        hash in the torrent

        :param piece_index: Index of the piece; minimum is 0, maximum is the
            torrent's number of pieces minus 1
        :param content_path: Path to file or directory to read piece from
            (defaults to class argument of the same name or
            :attr:`~.Torrent.path`)

        :raise ReadError: if a file exists but cannot be read
        :raise VerifyFileSizeError: if a file has unexpected size

        :return: result of the hash comparision (:class:`bool`) or `None` if a
            file at `piece_index` does not exist
        """
        try:
            stored_piece_hash = self._torrent.hashes[piece_index]
        except IndexError:
            raise ValueError(f'piece_index must be in range 0 - {self.max_piece_index}: {piece_index}')

        generated_piece_hash = self.get_piece_hash(piece_index, content_path=content_path)
        if generated_piece_hash is not None:
            return stored_piece_hash == generated_piece_hash


class _MissingPieces:
    """Calculate the missing pieces for a given file"""

    def __init__(self, torrent, stream):
        self._torrent = torrent
        self._stream = stream
        self._piece_indexes_seen = set()
        self._bycatch_files = []

    def __call__(self, file, content_path, reason):
        # Get the number of pieces covered by `file` minus all pieces we have
        # already reported due to overlaps
        piece_indexes = self._stream.get_piece_indexes_of_file(file)
        for piece_index in piece_indexes:
            if piece_index in self._piece_indexes_seen:
                piece_indexes.remove(piece_index)
        self._piece_indexes_seen.update(piece_indexes)

        # Figure out which subsequent files are affected by the missing last
        # piece of `file`
        affected_files = self._stream.get_files_at_piece_index(piece_indexes[-1], content_path='')
        affected_files.remove(file)
        # _debug(f'{affected_files=}')

        # Files that are processed as a side effect because they only exist in a
        # piece that also belongs to `file`
        bycatch_files = []

        # Unless `file` is the last file or it ends perfectly at a piece
        # boundary, we must calculate where the next piece starts in the next
        # file
        skip_bytes = 0

        if affected_files:
            # There are multiple files in the last piece of `file`
            # NOTE: `next_file` is the first file in the next piece, not the
            #       file after `file` in the stream (remember: each piece can
            #       fit lots and lots of files)
            next_file = affected_files[-1]
            next_file_start, next_file_end = self._stream.get_byte_range_of_file(next_file)

            # Stream index of the last byte of the last missing piece of `file`
            next_piece_boundary_index = (
                (piece_indexes[-1] * self._torrent.piece_size)
                + self._torrent.piece_size - 1
            )

            if next_file_end > next_piece_boundary_index:
                # The last file in this last missing piece continues in the next
                # piece. When we read from that file to create the next piece,
                # we must skip the first few bytes.
                skip_bytes = next_piece_boundary_index - next_file_start + 1

                # Mark all files between `file` and `next_file` as bycatch,
                # excluding `file` and `next_file`
                bycatch_files.extend(affected_files[:-1])
            else:
                # Include `next_file` in bycatch because it doesn't reach into
                # the next piece
                bycatch_files.extend(affected_files)

        self._bycatch_files.extend(bycatch_files)

        def iter_yields():
            # _debug(f'Calculated missing pieces: {piece_indexes}')
            # _debug(f'Calculated bycatch files: {bycatch_files}')
            # _debug(f'Skipping {skip_bytes} bytes at the start of next file')
            piece_count = len(piece_indexes)
            it = itertools.chain(
                self._first_yield(piece_count, file, content_path, bycatch_files, reason),
                self._middle_yields(piece_count, file, content_path),
                self._last_yield(piece_count, file, content_path, bycatch_files),
            )
            yield from it

        return iter_yields(), skip_bytes

    def _first_yield(self, piece_count, file, content_path, bycatch_files, reason):
        assert isinstance(reason, BaseException), repr(reason)
        exceptions = [reason]
        if piece_count == 1:
            # First piece is also last piece, so we must add bycatch exceptions
            # to the original exception (`reason`)
            bycatch_exceptions = self._get_bycatch_exceptions(bycatch_files, content_path)
            # _debug(f'First yield: Stream has only one piece - adding bycatch exceptions: {bycatch_exceptions}')
            exceptions.extend(bycatch_exceptions)
        filepath = self._stream._get_content_path(content_path, none_ok=False, file=file)
        yield (None, filepath, tuple(exceptions))

    def _middle_yields(self, piece_count, file, content_path):
        # Subtract first and last piece
        middle_piece_count = piece_count - 2
        # _debug(f'Middle yields: {max(0, middle_piece_count)} middle pieces found')
        if middle_piece_count >= 1:
            # Yield second to second-to-last pieces (exceptions are reported by
            # _first/last_yield())
            filepath = self._stream._get_content_path(content_path, none_ok=False, file=file)
            middle_piece = (None, filepath, ())
            for i in range(middle_piece_count):
                yield middle_piece

    def _last_yield(self, piece_count, file, content_path, bycatch_files):
        # Yield bycatch exceptions unless _first_yield() already did it
        if piece_count > 1:
            # Report bycatch exceptions with last piece
            exceptions = self._get_bycatch_exceptions(bycatch_files, content_path)
            # _debug(f'Last yield: Exceptions: {exceptions}')
            filepath = self._stream._get_content_path(content_path, none_ok=False, file=file)
            yield (None, filepath, tuple(exceptions))
        # else:
        #     _debug(f'Last yield: First piece is last piece')

    def _get_bycatch_exceptions(self, bycatch_files, content_path):
        exceptions = []
        for bc_file in bycatch_files:
            bc_filepath = self._stream._get_content_path(content_path, none_ok=False, file=bc_file)
            actual_size = self._stream._get_file_size_from_fs(bc_filepath)
            if actual_size is None:
                # No such file
                exceptions.append(error.ReadError(errno.ENOENT, bc_filepath))
            elif bc_filepath.size != actual_size:
                exceptions.append(error.VerifyFileSizeError(bc_filepath, actual_size, bc_filepath.size))
        # if exceptions:
        #     _debug(f'bycatch: {exceptions[-1]!r}')
        return exceptions

    @property
    def bycatch_files(self):
        """
        Files that only exist within a missing file's piece

        It is important that these files are not read to maintain the correct
        piece positions in the stream.
        """
        return tuple(self._bycatch_files)
