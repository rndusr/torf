import os

import pytest

import torf


def test_wrong_info_type(generated_singlefile_torrent):
    t = generated_singlefile_torrent
    for typ in (bytearray, list, tuple):
        t.metainfo['info'] = typ()
        with pytest.raises(torf.MetainfoError) as excinfo:
            t.validate()
        assert str(excinfo.value) == (f"Invalid metainfo: ['info'] "
                                      f"must be dict, not {typ.__qualname__}: {t.metainfo['info']}")

def test_length_and_files_in_info(generated_multifile_torrent):
    t = generated_multifile_torrent
    t.metainfo['info']['length'] = 123
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.validate()
    assert str(excinfo.value) == "Invalid metainfo: ['info'] includes both 'length' and 'files'"


def test_wrong_name_type(generated_singlefile_torrent):
    t = generated_singlefile_torrent
    t.metainfo['info']['name'] = 123
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.validate()
    assert str(excinfo.value) == ("Invalid metainfo: ['info']['name'] "
                                  "must be str or bytes, not int: 123")

def test_wrong_piece_length_type(generated_singlefile_torrent):
    t = generated_singlefile_torrent
    t.metainfo['info']['piece length'] = [700]
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.validate()
    assert str(excinfo.value) == ("Invalid metainfo: ['info']['piece length'] "
                                  "must be int, not list: [700]")

@pytest.mark.parametrize(
    argnames='piece_length, exp_exception',
    argvalues=(
        (-1, torf.MetainfoError("['info']['piece length'] is invalid: -1")),
        (0, torf.MetainfoError("['info']['piece length'] is invalid: 0")),
        (16385, torf.MetainfoError("['info']['piece length'] is invalid: 16385")),
    ),
)
def test_piece_length_not_divisible_by_16_kib(piece_length, exp_exception, generated_singlefile_torrent):
    t = generated_singlefile_torrent
    t.metainfo['info']['piece length'] = piece_length
    with pytest.raises(type(exp_exception)) as excinfo:
        t.validate()
    assert str(excinfo.value) == str(exp_exception)

def test_wrong_pieces_type(generated_singlefile_torrent):
    t = generated_singlefile_torrent
    t.metainfo['info']['pieces'] = 'many'
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.validate()
    assert str(excinfo.value) == ("Invalid metainfo: ['info']['pieces'] "
                                  "must be bytes, not str: 'many'")

def test_pieces_is_empty(generated_singlefile_torrent):
    t = generated_singlefile_torrent
    t.metainfo['info']['pieces'] = bytes()
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.validate()
    assert str(excinfo.value) == "Invalid metainfo: ['info']['pieces'] is empty"

def test_invalid_number_of_bytes_in_pieces(generated_singlefile_torrent):
    t = generated_singlefile_torrent
    t.path = None
    t.metainfo['info']['piece length'] = 512 * 1024
    for i in range(1, 10):
        t.metainfo['info']['length'] = i * t.metainfo['info']['piece length']
        t.metainfo['info']['pieces'] = bytes(os.urandom(i * 20))
        t.validate()

        for j in ((i * 20) + 1, (i * 20) - 1):
            t.metainfo['info']['pieces'] = bytes(os.urandom(j))
            with pytest.raises(torf.MetainfoError) as excinfo:
                t.validate()
            assert str(excinfo.value) == ("Invalid metainfo: length of ['info']['pieces'] "
                                          "is not divisible by 20")

def test_singlefile__unexpected_number_of_bytes_in_pieces(generated_singlefile_torrent):
    t = generated_singlefile_torrent
    t.path = None  # Don't complain about wrong file size
    t.metainfo['info']['length'] = 1024 * 1024
    t.metainfo['info']['piece length'] = int(1024 * 1024 / 8)

    t.metainfo['info']['pieces'] = os.urandom(20 * 9)
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.validate()
    assert str(excinfo.value) == 'Invalid metainfo: Expected 8 pieces but there are 9'

    t.metainfo['info']['pieces'] = os.urandom(20 * 7)
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.validate()
    assert str(excinfo.value) == 'Invalid metainfo: Expected 8 pieces but there are 7'

def test_multifile__unexpected_number_of_bytes_in_pieces(generated_multifile_torrent):
    t = generated_multifile_torrent
    t.path = None  # Don't complain about wrong file size

    total_size = 0
    for i,file in enumerate(t.metainfo['info']['files'], start=1):
        file['length'] = 1024 * 1024 * i + 123
        total_size += file['length']

    import math
    t.metainfo['info']['piece length'] = int(1024 * 1024 / 8)
    piece_count = math.ceil(total_size / t.metainfo['info']['piece length'])

    t.metainfo['info']['pieces'] = os.urandom(20 * (piece_count + 1))
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.validate()
    assert str(excinfo.value) == 'Invalid metainfo: Expected 49 pieces but there are 50'

    t.metainfo['info']['pieces'] = os.urandom(20 * (piece_count - 1))
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.validate()
    assert str(excinfo.value) == 'Invalid metainfo: Expected 49 pieces but there are 48'


def test_no_announce_is_ok(generated_singlefile_torrent):
    t = generated_singlefile_torrent
    if 'announce' in t.metainfo:
        del t.metainfo['announce']
    t.validate()

def test_wrong_announce_type(generated_singlefile_torrent):
    t = generated_singlefile_torrent
    for typ in (bytearray, list, tuple):
        t.metainfo['announce'] = typ()
        with pytest.raises(torf.MetainfoError) as excinfo:
            t.validate()
        assert str(excinfo.value) == (f"Invalid metainfo: ['announce'] "
                                      f"must be str, not {typ.__qualname__}: {t.metainfo['announce']}")

def test_invalid_announce_url(generated_singlefile_torrent):
    t = generated_singlefile_torrent
    for url in ('123', 'http://123:xxx/announce'):
        t.metainfo['announce'] = url
        with pytest.raises(torf.MetainfoError) as excinfo:
            t.validate()
        assert str(excinfo.value) == f"Invalid metainfo: ['announce'] is invalid: {url!r}"

def test_no_announce_list_is_ok(generated_singlefile_torrent):
    t = generated_singlefile_torrent
    if 'announce-list' in t.metainfo:
        del t.metainfo['announce-list']
    t.validate()

def test_wrong_announce_list_type(generated_singlefile_torrent):
    t = generated_singlefile_torrent

    # announce-list must be a list
    for value in (3, 'foo', None, lambda: None):
        t.metainfo['announce-list'] = value
        with pytest.raises(torf.MetainfoError) as excinfo:
            t.validate()
        assert str(excinfo.value) == (f"Invalid metainfo: ['announce-list'] "
                                      f"must be Iterable, not {type(value).__qualname__}: "
                                      f"{t.metainfo['announce-list']!r}")

    # Each item in announce-list must be a list
    for tier in (3, 'foo', None, lambda: None):
        for lst in ([tier],
                    [tier, []],
                    [[], tier],
                    [[], tier, []]):
            t.metainfo['announce-list'] = lst
            with pytest.raises(torf.MetainfoError) as excinfo:
                t.validate()
            tier_index = lst.index(tier)
            assert str(excinfo.value) == (f"Invalid metainfo: ['announce-list'][{tier_index}] "
                                          f"must be Iterable, not {type(tier).__qualname__}: {tier!r}")

    # Each item in each list in announce-list must be a string
    for typ in (bytearray, set):
        url = typ()
        for tier in ([url],
                     ['http://localhost:123/', url],
                     [url, 'http://localhost:123/'],
                     ['http://localhost:123/', url, 'http://localhost:456/']):
            url_index = tier.index(url)
            for lst in ([tier],
                        [tier, []],
                        [[], tier],
                        [[], tier, []]):
                tier_index = lst.index(tier)
                t.metainfo['announce-list'] = lst
                with pytest.raises(torf.MetainfoError) as excinfo:
                    t.validate()
                assert str(excinfo.value) == (f"Invalid metainfo: ['announce-list'][{tier_index}][{url_index}] "
                                              f"must be str, not {typ.__qualname__}: {url!r}")

def test_invalid_url_in_announce_list(generated_singlefile_torrent):
    t = generated_singlefile_torrent
    for url in ('123', 'http://123:xxx/announce'):
        for tier in ([url],
                     ['http://localhost:123/', url],
                     [url, 'http://localhost:123/'],
                     ['http://localhost:123/', url, 'http://localhost:456/']):
            url_index = tier.index(url)
            for lst in ([tier],
                        [tier, []],
                        [[], tier],
                        [[], tier, []]):
                tier_index = lst.index(tier)
                t.metainfo['announce-list'] = lst
                with pytest.raises(torf.MetainfoError) as excinfo:
                    t.validate()
                assert str(excinfo.value) == (f"Invalid metainfo: ['announce-list'][{tier_index}][{url_index}] "
                                              f"is invalid: {url!r}")

def test_no_announce_and_no_announce_list_when_torrent_is_private(generated_singlefile_torrent):
    t = generated_singlefile_torrent
    t.metainfo['info']['private'] = True
    if 'announce' in t.metainfo:
        del t.metainfo['announce']
    if 'announce-list' in t.metainfo:
        del t.metainfo['announce-list']
    t.validate()
    assert t.generate() is True
    assert t.is_ready is True


def test_singlefile_wrong_length_type(generated_singlefile_torrent):
    t = generated_singlefile_torrent
    t.metainfo['info']['length'] = 'foo'
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.validate()
    assert str(excinfo.value) == ("Invalid metainfo: ['info']['length'] "
                                  "must be int or float, not str: 'foo'")

def test_singlefile_wrong_md5sum_type(generated_singlefile_torrent):
    t = generated_singlefile_torrent
    t.metainfo['info']['md5sum'] = 0
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.validate()
    assert str(excinfo.value) == ("Invalid metainfo: ['info']['md5sum'] "
                                  "must be str, not int: 0")

    t.metainfo['info']['md5sum'] = 'Z8b329da9893e34099c7d8ad5cb9c940'
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.validate()
    assert str(excinfo.value) == ("Invalid metainfo: ['info']['md5sum'] is invalid: "
                                  "'Z8b329da9893e34099c7d8ad5cb9c940'")


def test_multifile_wrong_files_type(generated_multifile_torrent):
    t = generated_multifile_torrent
    t._path = None
    t.metainfo['info']['files'] = 'foo'
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.validate()
    assert str(excinfo.value) == ("Invalid metainfo: ['info']['files'] "
                                  "must be Iterable, not str: 'foo'")

def test_multifile_wrong_path_type(generated_multifile_torrent):
    t = generated_multifile_torrent
    t._path = None
    t.metainfo['info']['files'][0]['path'] = 'foo/bar/baz'
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.validate()
    assert str(excinfo.value) == ("Invalid metainfo: ['info']['files'][0]['path'] "
                                  "must be Iterable, not str: 'foo/bar/baz'")

def test_multifile_wrong_path_item_type(generated_multifile_torrent):
    t = generated_multifile_torrent
    t._path = None
    t.metainfo['info']['files'][1]['path'][0] = 17
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.validate()
    assert str(excinfo.value) == ("Invalid metainfo: ['info']['files'][1]['path'][0] "
                                  "must be str or bytes, not int: 17")

def test_multifile_wrong_length_type(generated_multifile_torrent):
    t = generated_multifile_torrent
    t._path = None
    t.metainfo['info']['files'][2]['length'] = ['this', 'is', 'not', 'a', 'length']
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.validate()
    assert str(excinfo.value) == ("Invalid metainfo: ['info']['files'][2]['length'] "
                                  "must be int or float, not list: ['this', 'is', 'not', 'a', 'length']")

def test_multifile_wrong_md5sum_type(generated_multifile_torrent):
    t = generated_multifile_torrent
    t.metainfo['info']['files'][0]['md5sum'] = 0
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.validate()
    assert str(excinfo.value) == ("Invalid metainfo: ['info']['files'][0]['md5sum'] "
                                  "must be str, not int: 0")

    t.metainfo['info']['files'][0]['md5sum'] = 'Z8b329da9893e34099c7d8ad5cb9c940'
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.validate()
    assert str(excinfo.value) == ("Invalid metainfo: ['info']['files'][0]['md5sum'] is invalid: "
                                  "'Z8b329da9893e34099c7d8ad5cb9c940'")


def assert_missing_metainfo(torrent, *keys):
    md = torrent.metainfo
    for key in keys[:-1]:
        md = md[key]
    del md[keys[-1]]
    with pytest.raises(torf.MetainfoError) as excinfo:
        torrent.validate()
    assert excinfo.match(rf"Invalid metainfo: Missing {keys[-1]!r} in \['info'\]")

def test_singlefile_missing_info_path(generated_singlefile_torrent):
    assert_missing_metainfo(generated_singlefile_torrent, 'info', 'name')

def test_singlefile_missing_info_piece_length(generated_singlefile_torrent):
    assert_missing_metainfo(generated_singlefile_torrent, 'info', 'piece length')

def test_singlefile_missing_info_pieces(generated_singlefile_torrent):
    assert_missing_metainfo(generated_singlefile_torrent, 'info', 'pieces')

def test_multifile_missing_info_path(generated_multifile_torrent):
    assert_missing_metainfo(generated_multifile_torrent, 'info', 'name')

def test_multifile_missing_info_piece_length(generated_multifile_torrent):
    assert_missing_metainfo(generated_multifile_torrent, 'info', 'piece length')

def test_multifile_missing_info_pieces(generated_multifile_torrent):
    assert_missing_metainfo(generated_multifile_torrent, 'info', 'pieces')

def test_multifile_missing_info_files_0_length(generated_multifile_torrent):
    assert_missing_metainfo(generated_multifile_torrent, 'info', 'files', 0, 'length')

def test_multifile_missing_info_files_1_length(generated_multifile_torrent):
    assert_missing_metainfo(generated_multifile_torrent, 'info', 'files', 1, 'length')

def test_multifile_missing_info_files_1_path(generated_multifile_torrent):
    assert_missing_metainfo(generated_multifile_torrent, 'info', 'files', 1, 'path')

def test_multifile_missing_info_files_2_path(generated_multifile_torrent):
    assert_missing_metainfo(generated_multifile_torrent, 'info', 'files', 2, 'path')


def assert_mismatching_filesizes(torrent):
    torrent.validate()  # Should validate

    for torrent_path, fs_path in zip(torrent.files, torrent.filepaths):
        # Remember file content
        with open(fs_path, 'rb') as f:
            orig_fs_path_content = f.read()

        # Change file size
        with open(fs_path, 'ab') as f:
            f.write(b'foo')

        # Expect validation error
        mi_size = torrent.partial_size(torrent_path)
        fs_size = os.path.getsize(fs_path)
        assert fs_size == mi_size + len('foo')
        with pytest.raises(torf.MetainfoError) as excinfo:
            torrent.validate()
        assert str(excinfo.value) == (f'Invalid metainfo: Mismatching file sizes in metainfo ({mi_size}) '
                                      f'and file system ({fs_size}): {fs_path}')

        # Restore original file content
        with open(fs_path, 'wb') as f:
            f.write(orig_fs_path_content)

    torrent.validate()  # Should validate again

def test_singlefile_mismatching_filesize(generated_singlefile_torrent):
    assert_mismatching_filesizes(generated_singlefile_torrent)

def test_multifile_mismatching_filesize(generated_multifile_torrent):
    assert_mismatching_filesizes(generated_multifile_torrent)
