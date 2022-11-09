import errno
import os

from . import _errors as error
from . import _generate as generate
from . import _stream as stream


class find_torrent_files:
    """Iterator over ``(torrent_file, torrent_file_counter, exception)`` tuples"""

    def __init__(self, *paths, max_file_size=float('inf')):
        self._paths = paths
        self._counter = 0
        self._max_file_size = max_file_size

    def __iter__(self):
        """
        Find torrent files recursively beneath each path in `paths`

        Each list item is a 4-tuple that contains the torrent file path or ``None``,
        a counter that increments for each torrent file, the total number of torrent
        files and an exception or ``None``.
        """
        for path in self._paths:
            yield from self._find(path)

    def _find(self, path):
        if os.path.isdir(path):
            try:
                for name in os.listdir(path):
                    subpath = os.sep.join((str(path), name))
                    yield from self._find(subpath)
            except OSError as e:
                yield None, self._counter, error.ReadError(e.errno, str(path))

        elif os.path.basename(path).lower().endswith('.torrent'):
            try:
                file_size = os.path.getsize(path)
            except OSError:
                self._counter += 1
                yield path, self._counter, error.ReadError(errno.ENOENT, str(path))
            else:
                if file_size <= self._max_file_size:
                    self._counter += 1
                    yield path, self._counter, None

        elif not os.path.exists(path):
            yield None, self._counter, error.ReadError(errno.ENOENT, str(path))

    @property
    def total(self):
        """Total number of torrents beneath all paths"""
        # Get a sequence of all torrents without changing self._counter.
        items = tuple(type(self)(*self._paths, max_file_size=self._max_file_size))
        if items:
            # Last item should contain the number of torrents found.
            return items[-1][1]
        else:
            return 0


def is_file_match(torrent, candidate):
    """
    Whether `torrent` contains the same files as `candidate`

    Both arugments are :class:`~.Torrent` objects.

    The torrents match if they both share the same ``name`` and ``files`` or
    ``name`` and ``length`` fields in their :attr:`~.Torrent.metainfo`.
    `candidate`'s :attr:`~.Torrent.piece_size` of must also not exceed
    `torrent`'s :attr:`~.Torrent.piece_size_max`.

    This is a quick check that doesn't require any system calls.
    """
    # Compare relative file paths and file sizes.
    # Order of files is important.
    torrent_info, candidate_info = torrent.metainfo['info'], candidate.metainfo['info']

    # Don't bother doing anything else if the names are different
    if torrent_info['name'] != candidate_info['name']:
        return False

    torrent_id = _get_filepaths_and_sizes(torrent_info)
    candidate_id = _get_filepaths_and_sizes(candidate_info)
    if torrent_id == candidate_id:
        if torrent.piece_size_min <= candidate.piece_size <= torrent.piece_size_max:
            return True

    return False

def _get_filepaths_and_sizes(info):
    name = info['name']

    # Singlefile torrent
    length = info.get('length', None)
    if length:
        return [(name, length)]

    # Multifile torrent
    files = info.get('files', None)
    if files:
        files_and_sizes = []
        for file in files:
            files_and_sizes.append((
                os.sep.join((name, *file['path'])),
                file['length'],
            ))
        return sorted(files_and_sizes)

    else:
        raise RuntimeError(f'Unable to find files: {info!r}')


def is_content_match(torrent, candidate):
    """
    Whether `torrent` contains the same files as `candidate`

    Both arugments are :class:`~.Torrent` objects.

    If a `candidate` matches, a few piece hashes from each file are compared to
    the corresponding hashes from `candidate` to detect files name/size
    collisions.

    This is relatively slow and should only be used after :func:`is_file_match`
    returned `True`.
    """
    if not torrent.path:
        raise RuntimeError(f'Torrent does not have a file system path: {torrent!r}')

    # Compare some piece hashes for each file
    with stream.TorrentFileStream(candidate, content_path=torrent.path) as tfs:
        check_piece_indexes = set()
        for file in torrent.files:
            all_file_piece_indexes = tfs.get_piece_indexes_of_file(file)
            middle_piece_index = int(len(all_file_piece_indexes) / 2)
            some_file_piece_indexes = (
                all_file_piece_indexes[:1]
                + [middle_piece_index]
                + all_file_piece_indexes[-1:]
            )
            check_piece_indexes.update(some_file_piece_indexes)

        for piece_index in sorted(check_piece_indexes):
            if not tfs.verify_piece(piece_index):
                return False
    return True


def copy(from_torrent, to_torrent):
    """
    Copy ``pieces``, ``piece length`` and ``files`` from `from_torrent` to
    `to_torrent`
    """
    source_info = from_torrent.metainfo['info']
    to_torrent.metainfo['info']['pieces'] = source_info['pieces']
    to_torrent.metainfo['info']['piece length'] = source_info['piece length']
    if 'files' in from_torrent.metainfo['info']:
        # Confirm both file lists are identical while ignoring order
        def make_sortable(files):
            return [tuple(f.items()) for f in files]

        # Only include "length" and "files" fields
        source_files = [
            {'length': file['length'], 'path': file['path']}
            for file in source_info['files']
        ]

        assert sorted(make_sortable(to_torrent.metainfo['info']['files'])) \
            == sorted(make_sortable(source_files))

        # Copy file order from `source_info`
        to_torrent.metainfo['info']['files'] = source_files


class ReuseCallback(generate._IntervaledCallback):
    def __init__(self, *args, torrent, torrent_files_total, **kwargs):
        super().__init__(*args, **kwargs)
        self._torrent = torrent
        self._torrent_files_total = torrent_files_total

    def __call__(self, torrent_filepath, torrent_files_done, is_match, exception):
        if self._callback:
            force = bool(
                # Call callback if there is an error, e.g. "Permission denied"
                exception
                # Call callback if we found a match of if we are verifying file contents
                or is_match in (True, None)
                # Call callback if this is the last torrent file
                or torrent_files_done >= self._torrent_files_total
            )
            return super().__call__(
                self._torrent,
                torrent_filepath,
                torrent_files_done,
                self._torrent_files_total,
                is_match,
                exception,
                force=force,
            )
        elif exception:
            raise exception
