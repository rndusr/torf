import collections
import copy
import errno
import os
import re
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, call

import pytest

import torf

from . import ComparableException


@pytest.fixture(autouse=True)
def ordered_listdir(mocker):
    def ordered_listdir(*args, _real_listdir=os.listdir, **kwargs):
        return sorted(_real_listdir(*args, **kwargs))
    mocker.patch('os.listdir', ordered_listdir)


@pytest.fixture
def existing_torrents(create_dir, create_file, tmp_path):
    class ExistingTorrents:
        def __init__(self, **torrent_directories):
            self._torrents = {}
            for dirname, info in torrent_directories.items():
                self._torrents[dirname] = self._create_torrents(dirname, *info)

            # Sprinkle in some non-torrent files
            for dirname in self._torrents:
                (tmp_path / dirname / 'foo.jpg').write_bytes(b"Ceci n'est pas une JPEG")
                (tmp_path / dirname / 'foo.txt').write_text('But this looks like text')

        @staticmethod
        def _create_torrents(directory, *items):
            torrents_directory = tmp_path / directory
            torrents_directory.mkdir(exist_ok=True)
            torrents = []
            for item in items:
                torrent_name = item[0]
                create_args = item[1]
                torrent_kwargs = item[2]
                if isinstance(create_args, collections.abc.Sequence) and not isinstance(create_args, str):
                    content_path = create_dir(torrent_name, *create_args)
                else:
                    content_path = create_file(torrent_name, create_args)
                torrent = torf.Torrent(path=content_path, **torrent_kwargs)
                torrent_filepath = torrents_directory / f'{torrent_name}.torrent'
                # Add some non-standard fields into each file list
                if 'files' in torrent.metainfo['info']:
                    for i in range(len(torrent.metainfo['info']['files'])):
                        torrent.metainfo['info']['files'][i]['foohash'] = 'This could be your MD5 sum'
                torrent.generate()
                torrent.write(torrent_filepath)
                torrents.append(SimpleNamespace(
                    torrent=torrent,
                    torrent_path=torrent_filepath,
                    content_path=content_path,
                ))
                print('created torrent:\n', torrents[-1].torrent_path, '\n', torrents[-1].torrent.metainfo)
            return torrents

        def __del__(self, *args, **kwargs):
            # Make sure pytest can delete files and directories
            for dirname in self._torrents:
                (tmp_path / dirname).chmod(0o700)
                for rootdir, dirnames, filenames in os.walk(tmp_path / dirname):
                    for dirname in dirnames:
                        os.chmod(os.path.join(rootdir, dirname), 0o700)
                    for filename in filenames:
                        os.chmod(os.path.join(rootdir, filename), 0o600)

        def __getattr__(self, name):
            return self._torrents[name]

        @property
        def locations(self):
            return {dirname: (tmp_path / dirname) for dirname in self._torrents}

        @property
        def location_paths(self):
            return tuple(tmp_path / dirname for dirname in self._torrents)

        @property
        def torrent_filepaths(self):
            return tuple(
                tmp_path / dirname / info.torrent_path
                for dirname, infos in self._torrents.items()
                for info in infos
            )

    return ExistingTorrents


@pytest.mark.parametrize(
    argnames='path, exp_find_torrent_files_args, exp_exception',
    argvalues=(
        ('a/path', ('a/path',), None),
        (('a/path', 'another/path'), ('a/path', 'another/path'), None),
        (iter(('a/path', 'another/path')), ('a/path', 'another/path'), None),
        (123, (), ValueError('Invalid path argument: 123')),
    ),
)
def test_path_argument(path, exp_find_torrent_files_args, exp_exception, create_file, mocker):
    find_torrent_files_mock = mocker.patch('torf._reuse.find_torrent_files', MagicMock(
        __iter__=MagicMock(return_value=()),
        total=0,
    ))

    torrent = torf.Torrent(path=create_file('just_a_file', 'foo'))

    if exp_exception:
        with pytest.raises(type(exp_exception), match=rf'^{re.escape(str(exp_exception))}$'):
            torrent.reuse(path)
    else:
        return_value = torrent.reuse(path)
        assert return_value is False
        assert find_torrent_files_mock.call_args_list == [call(
            *exp_find_torrent_files_args,
            max_file_size=torf.Torrent.MAX_TORRENT_FILE_SIZE,
        )]


def test_max_torrent_file_size(create_file, existing_torrents, mocker):
    # Create and prepare existing torrents
    existing_torrents = existing_torrents(
        subpath1=(
            ('a', 'foo', {'creation_date': 123}),
            ('b', 'bar', {'creation_date': 456}),
            ('c', 'baz', {'creation_date': 789}),
        ),
        subpath2=(
            ('d', 'hey', {'private': True}),
            ('e', 'ho', {'comment': 'yo'}),
            ('f', 'oh', {'comment': 'oy'}),
            ('g', 'ohh', {'comment': 'oyy'}),
        ),
    )
    # Make some torrents really big
    with open(existing_torrents.torrent_filepaths[1], 'wb') as f:
        f.truncate(20 * 1048576)
    with open(existing_torrents.torrent_filepaths[3], 'wb') as f:
        f.truncate(30 * 1048576)

    callback = Mock(return_value=None)
    new_torrent = torf.Torrent(path=create_file('just_a_file', 'foo'))
    return_value = new_torrent.reuse(existing_torrents.location_paths, callback=callback)

    assert return_value is False
    assert callback.call_args_list == [
        call(new_torrent, str(existing_torrents.subpath1[0].torrent_path), 1, 5, False, None),
        call(new_torrent, str(existing_torrents.subpath1[2].torrent_path), 2, 5, False, None),
        call(new_torrent, str(existing_torrents.subpath2[1].torrent_path), 3, 5, False, None),
        call(new_torrent, str(existing_torrents.subpath2[2].torrent_path), 4, 5, False, None),
        call(new_torrent, str(existing_torrents.subpath2[3].torrent_path), 5, 5, False, None),
    ]


@pytest.mark.parametrize('with_callback', (True, False), ids=('with_callback', 'without_callback'))
def test__singlefile__no_exceptions(with_callback, existing_torrents):
    # Create and prepare existing torrents
    existing_torrents = existing_torrents(
        my_torrents=(
            ('a', 'foo', {'creation_date': 123}),
            ('b', 'bar', {'creation_date': 456}),
            ('c', 'baz', {'creation_date': 789}),
            ('d', 'arf', {'created_by': 'me!'}),
            ('e', 'barf', {'source': 'you!'}),
        ),
    )

    # Create and prepare the torrent we want to generate
    reused = existing_torrents.my_torrents[2]
    new_torrent = torf.Torrent(
        path=reused.content_path,
        trackers=('http://foo:1000', 'http://foo:2000'),
        webseeds=('http://bar:1000',),
        httpseeds=('http://baz:1000',),
        private=True,
        comment='This is a custom torrent',
        creation_date=123000,
        created_by='CREATOR',
        source='SRC',
        piece_size=8 * 1048576,
        randomize_infohash=True,
    )

    # Expect the same metainfo, but with important parts copied
    exp_joined_metainfo = copy.deepcopy(new_torrent.metainfo)
    exp_joined_metainfo['info']['piece length'] = reused.torrent.metainfo['info']['piece length']
    exp_joined_metainfo['info']['pieces'] = reused.torrent.metainfo['info']['pieces']

    # Reuse existing torrent
    if with_callback:
        callback = Mock(return_value=None)
        return_value = new_torrent.reuse(existing_torrents.location_paths, callback=callback)

        # Confirm everything happened as expected
        assert return_value is True
        assert new_torrent.metainfo == exp_joined_metainfo
        assert callback.call_args_list == [
            call(new_torrent, str(existing_torrents.my_torrents[0].torrent_path), 1, 5, False, None),
            call(new_torrent, str(existing_torrents.my_torrents[1].torrent_path), 2, 5, False, None),
            call(new_torrent, str(existing_torrents.my_torrents[2].torrent_path), 3, 5, None, None),
            call(new_torrent, str(existing_torrents.my_torrents[2].torrent_path), 3, 5, True, None),
        ]
    else:
        return_value = new_torrent.reuse(existing_torrents.location_paths)

        # Confirm everything happened as expected
        assert return_value is True
        assert new_torrent.metainfo == exp_joined_metainfo


@pytest.mark.parametrize('with_callback', (True, False), ids=('with_callback', 'without_callback'))
def test__multifile__no_exceptions(with_callback, existing_torrents):
    # Create and prepare existing torrents with some of them sharing the same
    # (torrent name, file name, file size) but different file contents
    existing_torrents = existing_torrents(
        torrents1=(
            ('a', (
                ('this.jpg', 16380 * 30),
                ('that.txt', 'text data'),
            ), {'creation_date': 123}),
            ('b', (
                ('this.jpg', 16380 * 30),
                ('that.txt', 'text doto'),
            ), {'creation_date': 456}),
            ('c', (
                ('this.jpg', 16380 * 30),
                ('that.txt', 'text diti'),
            ), {'creation_date': 789}),
        ),
        torrents2=(
            ('a', (
                ('this.jpg', 16380 * 30),
                ('that.txt', 'more text'),
            ), {'creation_date': 234}),
            ('b', (
                ('this.jpg', 16380 * 30),
                ('that.txt', 'mare text'),
            ), {'creation_date': 345}),
            ('c', (
                ('this.jpg', 16380 * 30),
                ('that.txt', 'mire text'),
            ), {'creation_date': 456}),
        ),
    )

    # Create and prepare the torrent we want to generate
    reused = existing_torrents.torrents2[1]
    new_torrent = torf.Torrent(
        path=reused.content_path,
        trackers=('http://foo:1000', 'http://foo:2000'),
        webseeds=('http://bar:1000',),
        httpseeds=('http://baz:1000',),
        private=True,
        comment='This is a custom torrent',
        creation_date=123000,
        created_by='CREATOR',
        source='SRC',
        piece_size=1048576,
        randomize_infohash=True,
    )

    # Expect the same metainfo, but with important parts copied
    exp_joined_metainfo = copy.deepcopy(new_torrent.metainfo)
    exp_joined_metainfo['info']['piece length'] = reused.torrent.metainfo['info']['piece length']
    exp_joined_metainfo['info']['pieces'] = reused.torrent.metainfo['info']['pieces']
    exp_joined_metainfo['info']['files'] = [
        {'length': f['length'], 'path': f['path']}
        for f in reused.torrent.metainfo['info']['files']
    ]

    # Reuse existing torrent
    if with_callback:
        callback = Mock(return_value=None)
        return_value = new_torrent.reuse(existing_torrents.location_paths, callback=callback)

        # Confirm everything happened as expected
        assert return_value is True
        assert new_torrent.metainfo == exp_joined_metainfo
        assert callback.call_args_list == [
            call(new_torrent, str(existing_torrents.torrents1[0].torrent_path), 1, 6, False, None),
            call(new_torrent, str(existing_torrents.torrents1[1].torrent_path), 2, 6, None, None),
            call(new_torrent, str(existing_torrents.torrents1[1].torrent_path), 2, 6, False, None),
            call(new_torrent, str(existing_torrents.torrents1[2].torrent_path), 3, 6, False, None),
            call(new_torrent, str(existing_torrents.torrents2[0].torrent_path), 4, 6, False, None),
            call(new_torrent, str(reused.torrent_path), 5, 6, None, None),
            call(new_torrent, str(reused.torrent_path), 5, 6, True, None),
        ]

    else:
        return_value = new_torrent.reuse(existing_torrents.location_paths)

        # Confirm everything happened as expected
        assert return_value is True
        assert new_torrent.metainfo == exp_joined_metainfo


@pytest.mark.parametrize('with_callback', (True, False), ids=('with_callback', 'without_callback'))
def test_exceptions(with_callback, existing_torrents):
    # Create and prepare existing torrents
    existing_torrents = existing_torrents(
        readable1=(
            ('a', 'foo', {'creation_date': 123}),
            ('b', 'bar', {'creation_date': 456}),
            ('c', 'baz', {'creation_date': 789}),
        ),
        unreadable=(),
        readable2=(
            ('d', 'hey', {'private': True}),
            ('e', 'ho', {'comment': 'yo'}),
            ('f', 'oh', {'comment': 'oy'}),
            ('g', 'ohh', {'comment': 'oyy'}),
        ),
    )
    # Unreadable directory
    existing_torrents.locations['unreadable'].chmod(0o300)
    # Unreadable torrent file
    existing_torrents.readable2[1].torrent_path.chmod(0o300)
    # Nonexisting torrent file
    nonexisting_torrent_file = 'no/such/path.torrent'

    # Create and prepare the torrent we want to generate
    reused = existing_torrents.readable2[2]
    new_torrent = torf.Torrent(
        path=reused.content_path,
        trackers=('http://foo:1000', 'http://foo:2000'),
        webseeds=('http://bar:1000',),
        httpseeds=('http://baz:1000',),
        private=True,
        comment='This is a custom torrent',
        creation_date=123000,
        created_by='CREATOR',
        source='SRC',
        piece_size=8 * 1048576,
        randomize_infohash=True,
    )

    # Reuse existing torrent
    if with_callback:
        # Expect the same metainfo, but with important parts copied
        exp_joined_metainfo = copy.deepcopy(new_torrent.metainfo)
        exp_joined_metainfo['info']['piece length'] = reused.torrent.metainfo['info']['piece length']
        exp_joined_metainfo['info']['pieces'] = reused.torrent.metainfo['info']['pieces']

        callback = Mock(return_value=None)
        location_paths = (nonexisting_torrent_file,) + existing_torrents.location_paths
        return_value = new_torrent.reuse(location_paths, callback=callback)

        # Confirm everything happened as expected
        assert return_value is True
        assert new_torrent.metainfo == exp_joined_metainfo
        for c in callback.call_args_list:
            print(c)

        assert callback.call_args_list == [
            call(
                new_torrent, nonexisting_torrent_file,
                1, 8, False,
                ComparableException(
                    torf.ReadError(errno.ENOENT, nonexisting_torrent_file),
                ),
            ),
            call(new_torrent, str(existing_torrents.readable1[0].torrent_path), 2, 8, False, None),
            call(new_torrent, str(existing_torrents.readable1[1].torrent_path), 3, 8, False, None),
            call(new_torrent, str(existing_torrents.readable1[2].torrent_path), 4, 8, False, None),
            call(
                new_torrent,
                None,
                4, 8, False,
                ComparableException(
                    torf.ReadError(errno.EACCES, str(existing_torrents.locations['unreadable'])),
                ),
            ),
            call(new_torrent, str(existing_torrents.readable2[0].torrent_path), 5, 8, False, None),
            call(
                new_torrent,
                str(existing_torrents.readable2[1].torrent_path),
                6, 8, False,
                ComparableException(
                    torf.ReadError(errno.EACCES, str(existing_torrents.readable2[1].torrent_path)),
                ),
            ),
            call(new_torrent, str(existing_torrents.readable2[2].torrent_path), 7, 8, None, None),
            call(new_torrent, str(existing_torrents.readable2[2].torrent_path), 7, 8, True, None),
        ]
    else:
        # Expect identical metainfo
        exp_joined_metainfo = copy.deepcopy(new_torrent.metainfo)

        exp_exception = torf.ReadError(errno.EACCES, str(existing_torrents.locations['unreadable']))
        with pytest.raises(type(exp_exception), match=rf'^{re.escape(str(exp_exception))}$'):
            new_torrent.reuse(existing_torrents.location_paths)

        # Confirm everything happened as expected
        assert new_torrent.metainfo == exp_joined_metainfo


@pytest.mark.parametrize(
    argnames='cancel_condition, exp_callback_calls_count',
    argvalues=(
        # cancel_condition gets torrent_filepath and is_match and returns True
        # for cancelling, False otherwise.
        pytest.param(
            lambda tfp, is_match: is_match is False,
            1,
            id='mismatch',
        ),
        pytest.param(
            lambda tfp, is_match: tfp is None,
            4,
            id='unreadable directory',
        ),
        pytest.param(
            lambda tfp, is_match: os.path.basename(tfp or '') == 'e.torrent',
            6,
            id='unreadable torrent file',
        ),
        pytest.param(
            lambda tfp, is_match: os.path.basename(tfp or '') == 'f.torrent',
            7,
            id='invalid bencoded data',
        ),
        pytest.param(
            lambda tfp, is_match: os.path.basename(tfp or '') == 'g.torrent',
            8,
            id='invalid metainfo',
        ),
        pytest.param(
            lambda tfp, is_match: is_match is None,
            9,
            id='verification',
        ),
    ),
)
def test_callback_cancels_when_handling(cancel_condition, exp_callback_calls_count, existing_torrents, create_file):
    # Create and prepare existing torrents
    existing_torrents = existing_torrents(
        readable1=(
            ('a', 'foo', {'creation_date': 123}),
            ('b', 'bar', {'creation_date': 456}),
            ('c', 'baz', {'creation_date': 789}),
        ),
        # Unreadable directory
        unreadable=(),
        readable2=(
            ('d', 'hey', {'private': True}),
            ('e', 'ho', {'comment': 'yo'}),
            ('f', 'oh', {'comment': 'oy'}),
            ('g', 'ohh', {'comment': 'oyy'}),
            ('h', 'ohy', {'comment': 'hoyo'}),
        ),
    )
    # ReadError (directory)
    existing_torrents.locations['unreadable'].chmod(0o300)
    # ReadError (torrent file)
    existing_torrents.readable2[1].torrent_path.chmod(0o300)
    # BdecodeError
    data = bytearray(existing_torrents.readable2[2].torrent_path.read_bytes())
    data[0] = ord('x')
    existing_torrents.readable2[2].torrent_path.write_bytes(data)
    # MetainfoError
    del existing_torrents.readable2[3].torrent.metainfo['info']['piece length']
    existing_torrents.readable2[3].torrent.write(
        existing_torrents.readable2[3].torrent_path,
        validate=False, overwrite=True,
    )

    # Create and prepare the torrent we want to generate
    reused = existing_torrents.readable2[4]
    new_torrent = torf.Torrent(path=reused.content_path)
    exp_joined_metainfo = copy.deepcopy(new_torrent.metainfo)

    def callback(torrent, torrent_path, done, total, is_match, exception):
        if cancel_condition(torrent_path, is_match):
            return 'cancel'

    callback_wrapper = Mock(side_effect=callback)

    # Reuse existing torrent
    return_value = new_torrent.reuse(existing_torrents.location_paths, callback=callback_wrapper)

    # Confirm everything happened as expected
    assert return_value is False
    assert new_torrent.metainfo == exp_joined_metainfo

    all_callback_calls = [
        call(new_torrent, str(existing_torrents.readable1[0].torrent_path), 1, 8, False, None),
        call(new_torrent, str(existing_torrents.readable1[1].torrent_path), 2, 8, False, None),
        call(new_torrent, str(existing_torrents.readable1[2].torrent_path), 3, 8, False, None),
        call(
            new_torrent,
            None,
            3, 8, False,
            ComparableException(
                torf.ReadError(errno.EACCES, str(existing_torrents.locations['unreadable'])),
            ),
        ),
        call(new_torrent, str(existing_torrents.readable2[0].torrent_path), 4, 8, False, None),
        call(
            new_torrent,
            str(existing_torrents.readable2[1].torrent_path),
            5, 8, False,
            ComparableException(
                torf.ReadError(errno.EACCES, str(existing_torrents.readable2[1].torrent_path)),
            ),
        ),
        call(
            new_torrent,
            str(existing_torrents.readable2[2].torrent_path),
            6, 8, False,
            ComparableException(
                torf.BdecodeError(str(existing_torrents.readable2[2].torrent_path)),
            ),
        ),
        call(
            new_torrent,
            str(existing_torrents.readable2[3].torrent_path),
            7, 8, False,
            ComparableException(
                torf.MetainfoError("Missing 'piece length' in ['info']"),
            ),
        ),
        call(new_torrent, str(existing_torrents.readable2[4].torrent_path), 8, 8, None, None),
        call(new_torrent, str(existing_torrents.readable2[4].torrent_path), 8, 8, False, None),
    ]
    assert callback_wrapper.call_args_list == all_callback_calls[:exp_callback_calls_count]


@pytest.mark.parametrize('with_callback', (True, False), ids=('with_callback', 'without_callback'))
def test_handling_of_nonexisting_path(with_callback, existing_torrents):
    # Create and prepare existing torrents
    existing_torrents = existing_torrents(
        my_torrents=(
            ('a', 'foo', {'creation_date': 123}),
            ('b', 'bar', {'creation_date': 456}),
            ('c', 'baz', {'creation_date': 789}),
        ),
    )

    # Create and prepare the torrent we want to generate
    reused = existing_torrents.my_torrents[0]
    new_torrent = torf.Torrent(path=reused.content_path)

    # Expect identical metainfo
    exp_joined_metainfo = copy.deepcopy(new_torrent.metainfo)

    # Reuse existing torrent
    reuse_torrent_path = 'path/to/nonexisting/directory'
    if with_callback:
        callback = Mock(return_value=None)
        return_value = new_torrent.reuse(reuse_torrent_path, callback=callback)

        # Confirm everything happened as expected
        assert return_value is False
        assert new_torrent.metainfo == exp_joined_metainfo
        assert callback.call_args_list == [
            call(
                new_torrent, None,
                0, 0, False,
                ComparableException(
                    torf.ReadError(errno.ENOENT, reuse_torrent_path),
                ),
            ),
        ]

    else:
        exp_exception = torf.ReadError(errno.ENOENT, reuse_torrent_path)
        with pytest.raises(type(exp_exception), match=rf'^{re.escape(str(exp_exception))}$'):
            new_torrent.reuse(reuse_torrent_path)
        assert new_torrent.metainfo == exp_joined_metainfo


@pytest.mark.parametrize('with_callback', (True, False), ids=('with_callback', 'without_callback'))
def test_reuse_with_empty_file_list(with_callback, existing_torrents, create_file):
    # Create and prepare existing torrents
    existing_torrents = existing_torrents(
        my_torrents=(
            ('a.jpg', 'foo', {'creation_date': 123}),
            ('b.txt', 'bar', {'creation_date': 456}),
            ('c.mp4', 'baz', {'creation_date': 789}),
        ),
    )

    # Create and prepare the torrent we want to generate
    new_torrent = torf.Torrent(
        path=create_file('just_a_file.jpg', 'foo'),
        exclude_globs=['*.jpg'],
    )

    # Expect identical metainfo
    exp_joined_metainfo = copy.deepcopy(new_torrent.metainfo)

    exp_exception = RuntimeError('reuse() called while file list is empty')
    with pytest.raises(type(exp_exception), match=rf'^{re.escape(str(exp_exception))}$'):
        if with_callback:
            new_torrent.reuse(existing_torrents.location_paths, callback=Mock())
        else:
            new_torrent.reuse(existing_torrents.location_paths)

    assert new_torrent.metainfo == exp_joined_metainfo


def test_reuse_considers_piece_size_min_max(existing_torrents):
    # Create and prepare existing torrents
    existing_torrents = existing_torrents(
        small=(
            ('a.jpg', 'foo', {'piece_size': 1048576 / 2}),
            ('b.txt', 'bar', {'piece_size': 1048576 * 1}),
            ('c.mp4', 'baz', {'piece_size': 1048576 / 2}),
        ),
        big=(
            ('a.jpg', 'foo', {'piece_size': 1048576 / 2}),
            ('b.txt', 'bar', {'piece_size': 1048576 * 4}),
            ('c.mp4', 'baz', {'piece_size': 1048576 / 2}),
        ),
        medium=(
            ('a.jpg', 'foo', {'piece_size': 1048576 / 2}),
            ('b.txt', 'bar', {'piece_size': 1048576 * 2}),
            ('c.mp4', 'baz', {'piece_size': 1048576 / 2}),
        ),
        large=(
            ('a.jpg', 'foo', {'piece_size': 1048576 / 2}),
            ('b.txt', 'bar', {'piece_size': 1048576 * 8}),
            ('c.mp4', 'baz', {'piece_size': 1048576 / 2}),
        ),
        giant=(
            ('a.jpg', 'foo', {'piece_size': 1048576 / 2}),
            ('b.txt', 'bar', {'piece_size': 1048576 * 16}),
            ('c.mp4', 'baz', {'piece_size': 1048576 / 2}),
        ),
    )

    # Create and prepare the torrent we want to generate
    reused = existing_torrents.medium[1]
    new_torrent = torf.Torrent(path=reused.content_path)
    exp_joined_metainfo = copy.deepcopy(new_torrent.metainfo)

    # Limit piece size to 1 - 2 MiB
    new_torrent.piece_size_min = 1 * 1048576
    new_torrent.piece_size_max = 2 * 1048576
    exp_joined_metainfo['info']['piece length'] = 1048576 * 1
    exp_joined_metainfo['info']['pieces'] = existing_torrents.medium[1].torrent.metainfo['info']['pieces']
    new_torrent.reuse(existing_torrents.location_paths)
    assert new_torrent.metainfo == exp_joined_metainfo

    # Limit piece size to 2 - 4 MiB
    new_torrent.piece_size_min = 2 * 1048576
    new_torrent.piece_size_max = 4 * 1048576
    exp_joined_metainfo['info']['piece length'] = 1048576 * 4
    exp_joined_metainfo['info']['pieces'] = existing_torrents.small[1].torrent.metainfo['info']['pieces']
    new_torrent.reuse(existing_torrents.location_paths)
    assert new_torrent.metainfo == exp_joined_metainfo


    # Limit piece size to 4 - 8 MiB
    new_torrent.piece_size_min = 4 * 1048576
    new_torrent.piece_size_max = 8 * 1048576
    exp_joined_metainfo['info']['piece length'] = 1048576 * 4
    exp_joined_metainfo['info']['pieces'] = existing_torrents.big[1].torrent.metainfo['info']['pieces']
    new_torrent.reuse(existing_torrents.location_paths)
    assert new_torrent.metainfo == exp_joined_metainfo

    # Limit piece size to 8 - 16 MiB
    new_torrent.piece_size_min = 8 * 1048576
    new_torrent.piece_size_max = 16 * 1048576
    exp_joined_metainfo['info']['piece length'] = 1048576 * 8
    exp_joined_metainfo['info']['pieces'] = existing_torrents.small[1].torrent.metainfo['info']['pieces']
    new_torrent.reuse(existing_torrents.location_paths)
    assert new_torrent.metainfo == exp_joined_metainfo


@pytest.mark.parametrize('with_callback', (True, False), ids=('with_callback', 'without_callback'))
def test_reuse_copies_file_order(with_callback, existing_torrents):
    # Create and prepare existing torrents with some of them sharing the same
    # (torrent name, file name, file size) but different file contents
    existing_torrents = existing_torrents(
        my_torrents=(
            ('a', (
                ('this.jpg', 16380 * 30),
                ('that.txt', 'text data'),
            ), {'creation_date': 123}),
            ('b', (
                ('this.jpg', 16380 * 30),
                ('that.txt', 'text doto'),
            ), {'creation_date': 456}),
            ('c', (
                ('this.jpg', 16380 * 30),
                ('that.txt', 'text diti'),
            ), {'creation_date': 789}),
        ),
    )

    # Create and prepare the torrent we want to generate
    reused = existing_torrents.my_torrents[1]
    new_torrent = torf.Torrent(reused.content_path)

    # Differing file order shouldn't matter, the new torrent should have the
    # same order as the reused torrent
    new_torrent.metainfo['info']['files'][0], new_torrent.metainfo['info']['files'][1] = \
        new_torrent.metainfo['info']['files'][1], new_torrent.metainfo['info']['files'][0]

    # Expect the same metainfo, but with important parts copied
    exp_joined_metainfo = copy.deepcopy(new_torrent.metainfo)
    exp_joined_metainfo['info']['piece length'] = reused.torrent.metainfo['info']['piece length']
    exp_joined_metainfo['info']['pieces'] = reused.torrent.metainfo['info']['pieces']
    exp_joined_metainfo['info']['files'] = [
        {'length': f['length'], 'path': f['path']}
        for f in reused.torrent.metainfo['info']['files']
    ]

    # Reuse existing torrent
    if with_callback:
        callback = Mock(return_value=None)
        return_value = new_torrent.reuse(existing_torrents.location_paths, callback=callback)

        # Confirm everything happened as expected
        assert return_value is True
        assert new_torrent.metainfo == exp_joined_metainfo
        assert callback.call_args_list == [
            call(new_torrent, str(existing_torrents.my_torrents[0].torrent_path), 1, 3, False, None),
            call(new_torrent, str(existing_torrents.my_torrents[1].torrent_path), 2, 3, None, None),
            call(new_torrent, str(existing_torrents.my_torrents[1].torrent_path), 2, 3, True, None),
        ]

    else:
        return_value = new_torrent.reuse(existing_torrents.location_paths)

        # Confirm everything happened as expected
        assert return_value is True
        assert new_torrent.metainfo == exp_joined_metainfo
