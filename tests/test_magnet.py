import base64
import binascii
import hashlib
import time
import urllib
from unittest import mock
from urllib.parse import quote_plus

import pytest

import torf

from . import ComparableException


@pytest.fixture
def hash16():
    def make_base16_hash(data):
        return hashlib.sha1(data).hexdigest()
    return make_base16_hash

@pytest.fixture
def hash32():
    def make_base32_hash(data):
        return base64.b32encode(hashlib.sha1(data).digest()).decode('utf-8')
    return make_base32_hash

@pytest.fixture
def xt(hash16):
    return 'urn:btih:' + hash16(b'anything')


def test_invalid_argument():
    with pytest.raises(TypeError):
        torf.Magnet(foo='bar')

def test_xt_missing():
    with pytest.raises(TypeError):
        torf.Magnet()

def test_xt_invalid():
    with pytest.raises(torf.MagnetError) as excinfo:
        torf.Magnet('asdf')
    assert str(excinfo.value) == 'asdf: Invalid exact topic ("xt")'

def test_xt_is_base16(hash16):
    xt = 'urn:btih:' + hash16(b'foo')
    m = torf.Magnet(xt)
    assert m.xt == xt
    assert m.infohash == hash16(b'foo')
    assert str(m) == f'magnet:?xt=urn:btih:{hash16(b"foo")}'
    m.infohash = hash16(b'bar')
    assert m.infohash == hash16(b'bar')
    assert str(m) == f'magnet:?xt=urn:btih:{hash16(b"bar")}'

def test_xt_is_base32(hash32):
    xt = 'urn:btih:' + hash32(b'foo')
    m = torf.Magnet(xt)
    assert m.xt == xt
    assert m.infohash == hash32(b'foo')
    assert str(m) == f'magnet:?xt=urn:btih:{hash32(b"foo")}'
    m.infohash = hash32(b'bar')
    assert m.infohash == hash32(b'bar')
    assert str(m) == f'magnet:?xt=urn:btih:{hash32(b"bar")}'


def test_xt_is_naked_infohash(hash16, hash32):
    for infohash in (hash16(b'foo'), hash32(b'foo')):
        m = torf.Magnet(infohash)
        assert m.xt == f'urn:btih:{infohash}'

def test_dn(xt):
    m = torf.Magnet(xt, dn='Héllo Wörld!')
    assert m.dn == 'Héllo Wörld!'
    assert str(m) == f'magnet:?xt={xt}&dn=H%C3%A9llo+W%C3%B6rld%21'
    m.dn = 'Göödbye World!'
    assert m.dn == 'Göödbye World!'
    assert str(m) == f'magnet:?xt={xt}&dn=G%C3%B6%C3%B6dbye+World%21'
    m.dn = (1, 2, 3)
    assert m.dn == '(1, 2, 3)'
    assert str(m) == f'magnet:?xt={xt}&dn=%281%2C+2%2C+3%29'

def test_xl(xt):
    m = torf.Magnet(xt, xl=123)
    assert m.xl == 123
    assert str(m) == f'magnet:?xt={xt}&xl=123'
    m.xl = 456
    assert str(m) == f'magnet:?xt={xt}&xl=456'
    with pytest.raises(torf.MagnetError) as excinfo:
        m.xl = 'foo'
    assert str(excinfo.value) == 'foo: Invalid exact length ("xl")'
    with pytest.raises(torf.MagnetError) as excinfo:
        m.xl = -123
    assert str(excinfo.value) == '-123: Must be 1 or larger'

def test_tr(xt):
    m = torf.Magnet(xt, tr=('http://foo.bar/baz',))
    assert m.tr == ['http://foo.bar/baz']
    assert str(m) == f'magnet:?xt={xt}&tr=http%3A%2F%2Ffoo.bar%2Fbaz'
    m.tr.append('http://blim/blam')
    assert m.tr == ['http://foo.bar/baz', 'http://blim/blam']
    assert str(m) == f'magnet:?xt={xt}&tr=http%3A%2F%2Ffoo.bar%2Fbaz&tr=http%3A%2F%2Fblim%2Fblam'

    with pytest.raises(torf.URLError):
        m.tr = 'foo'
    assert m.tr == ['http://foo.bar/baz', 'http://blim/blam']

    with pytest.raises(torf.URLError):
        m.tr.append('foo')
    assert m.tr == ['http://foo.bar/baz', 'http://blim/blam']

    m.tr = None
    assert m.tr == []

def test_xs(xt):
    m = torf.Magnet(xt, xs='http://foo.bar/baz.torrent')
    assert m.xs == 'http://foo.bar/baz.torrent'
    assert str(m) == f'magnet:?xt={xt}&xs=http%3A%2F%2Ffoo.bar%2Fbaz.torrent'
    m.xs = 'http://blim/blam.torrent'
    assert m.xs == 'http://blim/blam.torrent'
    assert str(m) == f'magnet:?xt={xt}&xs=http%3A%2F%2Fblim%2Fblam.torrent'
    with pytest.raises(torf.URLError):
        m.xs = 23

def test_as(xt):
    m = torf.Magnet(xt, as_='http://foo.bar/baz.torrent')
    assert m.as_ == 'http://foo.bar/baz.torrent'
    assert str(m) == f'magnet:?xt={xt}&as_=http%3A%2F%2Ffoo.bar%2Fbaz.torrent'
    m.as_ = 'http://blim/blam.torrent'
    assert m.as_ == 'http://blim/blam.torrent'
    assert str(m) == f'magnet:?xt={xt}&as_=http%3A%2F%2Fblim%2Fblam.torrent'
    with pytest.raises(torf.URLError):
        m.as_ = 23

def test_ws(xt):
    m = torf.Magnet(xt, ws=['http://foo.bar/baz.jpg',
                            'http://bar.foo/baz.jpg'])
    assert m.ws == ['http://foo.bar/baz.jpg',
                    'http://bar.foo/baz.jpg']
    with pytest.raises(torf.URLError):
        m.ws = ['foo']
    assert str(m) == f'magnet:?xt={xt}&ws=http%3A%2F%2Ffoo.bar%2Fbaz.jpg&ws=http%3A%2F%2Fbar.foo%2Fbaz.jpg'
    m.ws.remove('http://foo.bar/baz.jpg')
    assert str(m) == f'magnet:?xt={xt}&ws=http%3A%2F%2Fbar.foo%2Fbaz.jpg'
    m.ws = 'http://some/other/url/to/baz.jpg'
    assert m.ws == ['http://some/other/url/to/baz.jpg']
    with pytest.raises(torf.URLError):
        m.ws.replace(('adf',))
    assert m.ws == ['http://some/other/url/to/baz.jpg']

def test_kt(xt):
    m = torf.Magnet(xt, kt=('that', 'thing'))
    assert m.kt == ['that', 'thing']
    assert str(m) == f'magnet:?xt={xt}&kt=that+thing'
    m.kt = ('that', 'other', 'thing')
    assert m.kt == ['that', 'other', 'thing']
    assert str(m) == f'magnet:?xt={xt}&kt=that+other+thing'
    with pytest.raises(torf.MagnetError) as excinfo:
        m.kt = 17
    assert str(excinfo.value) == '17: Invalid keyword topic ("kt")'

def test_x(xt):
    m = torf.Magnet(xt, x_foo='asdf', x_bar=(1, 2, 3))
    assert m.x['foo'] == 'asdf'
    assert m.x['bar'] == (1, 2, 3)
    m.x['foo'] = '1234'
    assert m.x['foo'] == '1234'
    assert m.x['baz'] is None

def test_torrent(hash16, hash32):
    m = torf.Magnet(xt='urn:btih:' + hash16(b'some string'),
                    dn='foo', xl=1e6,
                    tr=('http://foo.bar/baz', 'http://asdf'),
                    ws=('http://x/y', 'http://z'))
    t = m.torrent()
    assert t.name == 'foo'
    assert t.size == 1e6
    assert t.trackers == [['http://foo.bar/baz'], ['http://asdf']]
    assert t.webseeds == ['http://x/y', 'http://z']
    assert t.infohash == hash16(b'some string')
    m = torf.Magnet(xt='urn:btih:' + hash32(b'some string'))
    assert m.torrent().infohash == hash16(b'some string')
    assert 'length' not in m.torrent().metainfo['info']

def test_from_string(hash32):
    m = torf.Magnet.from_string(f'magnet:?xt=urn:btih:{hash32(b"asdf")}'
                                '&dn=Some+Name'
                                '&xl=123456'
                                '&tr=http://tracker1.example.com/&tr=http://tracker2.example.com/'
                                '&xs=http://source.example.com/'
                                '&as=http://asource.example.com/'
                                '&ws=http://webseed1.example.com/&ws=http://webseed2.example.com/'
                                '&kt=one+two+three')
    assert m.xt == f'urn:btih:{hash32(b"asdf")}'
    assert m.dn == 'Some Name'
    assert m.xl == 123456
    assert m.tr == ['http://tracker1.example.com/', 'http://tracker2.example.com/']
    assert m.xs == 'http://source.example.com/'
    assert m.as_ == 'http://asource.example.com/'
    assert m.ws == ['http://webseed1.example.com/', 'http://webseed2.example.com/']
    assert m.kt == ['one', 'two', 'three']

def test_from_string_with_wrong_scheme(xt, hash16, hash32):
    uri = f'http:?xt=urn:btih:{hash32(b"asdf")}'
    with pytest.raises(torf.MagnetError) as excinfo:
        torf.Magnet.from_string(uri)
    assert str(excinfo.value) == f'{uri}: Not a magnet URI'

def test_from_string_with_unknown_parameter(xt, hash16, hash32):
    uri = (f'magnet:?xt=urn:btih:{hash32(b"asdf")}'
           '&dn=Some+Name'
           '&ab=foo')
    with pytest.raises(torf.MagnetError) as excinfo:
        torf.Magnet.from_string(uri)
    assert str(excinfo.value) == f'{uri}: ab: Unknown parameter'

def test_from_string_with_multiple_xt_parameters(xt, hash16, hash32):
    uri = (f'magnet:?xt=urn:btih:{hash32(b"asdf")}'
           f'&xt=urn:btih:{hash16(b"fdsa")}')
    with pytest.raises(torf.MagnetError) as excinfo:
        torf.Magnet.from_string(uri)
    assert str(excinfo.value) == f'{uri}: Multiple exact topics ("xt")'

def test_from_string_with_multiple_dn_parameters(xt, hash16, hash32):
    uri = f'magnet:?xt={xt}&dn=Foo&dn=Foo'
    with pytest.raises(torf.MagnetError) as excinfo:
        torf.Magnet.from_string(uri)
    assert str(excinfo.value) == f'{uri}: Multiple display names ("dn")'

def test_from_string_with_multiple_xl_parameters(xt, hash16, hash32):
    uri = f'magnet:?xt={xt}&xl=1234&xl=2345'
    with pytest.raises(torf.MagnetError) as excinfo:
        torf.Magnet.from_string(uri)
    assert str(excinfo.value) == f'{uri}: Multiple exact lengths ("xl")'

def test_from_string_with_multiple_xs_parameters(xt, hash16, hash32):
    uri = (f'magnet:?xt={xt}'
           '&xs=http%3A%2F%2Ffoo.bar%2Fbaz.torrent'
           '&xs=http%3A%2F%2Fbar.foo%2Fbaz.torrent')
    with pytest.raises(torf.MagnetError) as excinfo:
        torf.Magnet.from_string(uri)
    assert str(excinfo.value) == f'{uri}: Multiple exact sources ("xs")'

def test_from_string_with_multiple_as_parameters(xt, hash16, hash32):
    uri = (f'magnet:?xt={xt}'
           '&as=http%3A%2F%2Ffoo.bar%2Fbaz.torrent'
           '&as=http%3A%2F%2Fbar.foo%2Fbaz.torrent')
    with pytest.raises(torf.MagnetError) as excinfo:
        torf.Magnet.from_string(uri)
    assert str(excinfo.value) == f'{uri}: Multiple acceptable sources ("as")'

def test_from_string_with_multiple_kt_parameters(xt, hash16, hash32):
    uri = (f'magnet:?xt={xt}'
           '&kt=a+b+c'
           '&kt=1+2+5')
    with pytest.raises(torf.MagnetError) as excinfo:
        torf.Magnet.from_string(uri)
    assert str(excinfo.value) == f'{uri}: Multiple keyword topics ("kt")'


def test_from_string_with_invalid_xt_parameter():
    uri = 'magnet:?xt=foo'
    with pytest.raises(torf.MagnetError) as excinfo:
        torf.Magnet.from_string(uri)
    assert str(excinfo.value) == 'foo: Invalid exact topic ("xt")'

def test_from_string_with_invalid_xl_parameter(xt):
    uri = f'magnet:?xt={xt}&xl=nan'
    with pytest.raises(torf.MagnetError) as excinfo:
        torf.Magnet.from_string(uri)
    assert str(excinfo.value) == 'nan: Invalid exact length ("xl")'

def test_from_string_with_invalid_tr_parameter(xt):
    uri = f'magnet:?xt={xt}&tr=not+a+URL'
    with pytest.raises(torf.URLError) as excinfo:
        torf.Magnet.from_string(uri)
    assert str(excinfo.value) == 'not a URL: Invalid URL'

def test_from_string_with_invalid_xs_parameter(xt):
    uri = f'magnet:?xt={xt}&xs=not+a+URL'
    with pytest.raises(torf.URLError) as excinfo:
        torf.Magnet.from_string(uri)
    assert str(excinfo.value) == 'not a URL: Invalid URL'

def test_from_string_with_invalid_as_parameter(xt):
    uri = f'magnet:?xt={xt}&as=not+a+URL'
    with pytest.raises(torf.URLError) as excinfo:
        torf.Magnet.from_string(uri)
    assert str(excinfo.value) == 'not a URL: Invalid URL'

def test_from_string_with_invalid_ws_parameter(xt):
    uri = f'magnet:?xt={xt}&ws=not+a+URL'
    with pytest.raises(torf.URLError) as excinfo:
        torf.Magnet.from_string(uri)
    assert str(excinfo.value) == 'not a URL: Invalid URL'


def test_from_torrent(singlefile_content, multifile_content):
    for content in singlefile_content, multifile_content:
        t = torf.Torrent(content.path,
                         trackers=['http://foo', 'http://bar'],
                         webseeds=['http://qux', 'http://quux'])
        t.generate()
        assert str(t.magnet()) == (f'magnet:?xt=urn:btih:{t.infohash}'
                                   f'&dn={quote_plus(t.name)}'
                                   f'&xl={t.size}'
                                   '&tr=http%3A%2F%2Ffoo&tr=http%3A%2F%2Fbar'
                                   '&ws=http%3A%2F%2Fqux&ws=http%3A%2F%2Fquux')

def test_from_torrent_without_name(singlefile_content, multifile_content):
    for content in singlefile_content, multifile_content:
        t = torf.Torrent(content.path, trackers=['http://foo', 'http://bar'])
        t.generate()
        assert str(t.magnet(name=False)) == (f'magnet:?xt=urn:btih:{t.infohash}'
                                             f'&xl={t.size}'
                                             f'&tr=http%3A%2F%2Ffoo&tr=http%3A%2F%2Fbar')

def test_from_torrent_without_size(singlefile_content, multifile_content):
    for content in singlefile_content, multifile_content:
        t = torf.Torrent(content.path, trackers=['http://foo', 'http://bar'])
        t.generate()
        assert str(t.magnet(size=False)) == (f'magnet:?xt=urn:btih:{t.infohash}'
                                             f'&dn={quote_plus(t.name)}'
                                             f'&tr=http%3A%2F%2Ffoo&tr=http%3A%2F%2Fbar')

def test_from_torrent_with_single_tracker(singlefile_content, multifile_content):
    for content in singlefile_content, multifile_content:
        t = torf.Torrent(content.path, trackers=['http://foo'])
        t.generate()
        assert str(t.magnet()) == (f'magnet:?xt=urn:btih:{t.infohash}'
                                   f'&dn={quote_plus(t.name)}'
                                   f'&xl={t.size}'
                                   f'&tr=http%3A%2F%2Ffoo')
        assert str(t.magnet(tracker=True, trackers=False)) == (f'magnet:?xt=urn:btih:{t.infohash}'
                                                               f'&dn={quote_plus(t.name)}'
                                                               f'&xl={t.size}'
                                                               f'&tr=http%3A%2F%2Ffoo')
        assert str(t.magnet(tracker=False, trackers=False)) == (f'magnet:?xt=urn:btih:{t.infohash}'
                                                                f'&dn={quote_plus(t.name)}'
                                                                f'&xl={t.size}')
        assert str(t.magnet(tracker=True, trackers=True)) == (f'magnet:?xt=urn:btih:{t.infohash}'
                                                              f'&dn={quote_plus(t.name)}'
                                                              f'&xl={t.size}'
                                                              f'&tr=http%3A%2F%2Ffoo')
        assert str(t.magnet(tracker=False, trackers=True)) == (f'magnet:?xt=urn:btih:{t.infohash}'
                                                               f'&dn={quote_plus(t.name)}'
                                                               f'&xl={t.size}'
                                                               f'&tr=http%3A%2F%2Ffoo')

def test_from_torrent_with_multiple_trackers(singlefile_content, multifile_content):
    for content in singlefile_content, multifile_content:
        t = torf.Torrent(content.path, trackers=['http://foo', 'http://bar'])
        t.generate()
        assert str(t.magnet()) == (f'magnet:?xt=urn:btih:{t.infohash}'
                                   f'&dn={quote_plus(t.name)}'
                                   f'&xl={t.size}'
                                   f'&tr=http%3A%2F%2Ffoo&tr=http%3A%2F%2Fbar')
        assert str(t.magnet(tracker=True, trackers=False)) == (f'magnet:?xt=urn:btih:{t.infohash}'
                                                               f'&dn={quote_plus(t.name)}'
                                                               f'&xl={t.size}'
                                                               f'&tr=http%3A%2F%2Ffoo')
        assert str(t.magnet(tracker=False, trackers=False)) == (f'magnet:?xt=urn:btih:{t.infohash}'
                                                                f'&dn={quote_plus(t.name)}'
                                                                f'&xl={t.size}')
        assert str(t.magnet(tracker=True, trackers=True)) == (f'magnet:?xt=urn:btih:{t.infohash}'
                                                              f'&dn={quote_plus(t.name)}'
                                                              f'&xl={t.size}'
                                                              f'&tr=http%3A%2F%2Ffoo')
        assert str(t.magnet(tracker=False, trackers=True)) == (f'magnet:?xt=urn:btih:{t.infohash}'
                                                               f'&dn={quote_plus(t.name)}'
                                                               f'&xl={t.size}'
                                                               f'&tr=http%3A%2F%2Ffoo&tr=http%3A%2F%2Fbar')

def test_repr(xt):
    m = torf.Magnet(xt, dn='Foo', xl=123, tr=('http://tracker:123',),
                    xs='http://primary.source/url.torrent',
                    as_='http://alt.source/url.torrent',
                    ws=('http://webseed/url/file.content',),
                    kt=('keyword1', 'keyword2'),
                    x_foo='some', x_bar='junk')
    assert repr(m) == ("Magnet(xt='urn:btih:8867c88b56e0bfb82cffaf15a66bc8d107d6754a', "
                       "dn='Foo', xl=123, tr=['http://tracker:123'], "
                       "xs='http://primary.source/url.torrent', "
                       "as_='http://alt.source/url.torrent', "
                       "ws=['http://webseed/url/file.content'], "
                       "kt=['keyword1', 'keyword2'], "
                       "x_foo='some', x_bar='junk')")

def test_setting_info_with_wrong_infohash(generated_singlefile_torrent, generated_multifile_torrent):
    magnet = torf.Magnet(generated_singlefile_torrent.infohash)

    with pytest.raises(torf.MetainfoError) as excinfo:
        magnet._set_info_from_torrent(generated_multifile_torrent.dump(), validate=True)
    assert str(excinfo.value) == ('Invalid metainfo: Mismatching info hashes: '
                                  f'{generated_singlefile_torrent.infohash} != {generated_multifile_torrent.infohash}')

    magnet._set_info_from_torrent(generated_multifile_torrent.dump(), validate=False)
    assert magnet._info == generated_multifile_torrent.metainfo['info']

def test_getting_info__unsupported_protocol(generated_singlefile_torrent):
    torrent = generated_singlefile_torrent
    magnet = torf.Magnet(torrent.infohash, xs='asdf://xs.foo:123/torrent')

    cb = mock.MagicMock()
    assert magnet.get_info(callback=cb) is False
    exp_calls = [mock.call(ComparableException(torf.ConnectionError('asdf://xs.foo:123/torrent', 'Unsupported protocol')))]
    assert cb.call_args_list == exp_calls

    torrent_ = magnet.torrent()
    assert torrent_.metainfo['info'] == {}

def test_getting_info__xs_fails__as_fails(generated_singlefile_torrent):
    torrent = generated_singlefile_torrent
    magnet = torf.Magnet(torrent.infohash,
                         xs='http://xs.foo:123/torrent', as_='http://as.foo:123/torrent')

    cb = mock.MagicMock()
    assert magnet.get_info(callback=cb) is False
    exp_calls = [mock.call(ComparableException(torf.ConnectionError('http://xs.foo:123/torrent', 'Name or service not known'))),
                 mock.call(ComparableException(torf.ConnectionError('http://as.foo:123/torrent', 'Name or service not known')))]
    assert cb.call_args_list == exp_calls

    torrent_ = magnet.torrent()
    assert torrent_.metainfo['info'] == {}

def test_getting_info__xs_succeeds__as_fails(generated_singlefile_torrent, httpserver):
    torrent = generated_singlefile_torrent
    magnet = torf.Magnet(torrent.infohash,
                         xs=httpserver.url_for('/torrent'), as_='http://as.foo:123/torrent')
    httpserver.expect_request('/torrent').respond_with_data(torrent.dump())

    cb = mock.MagicMock()
    assert magnet.get_info(callback=cb) is True
    assert cb.call_args_list == []

    torrent_ = magnet.torrent()
    assert torrent_.metainfo['info'] == torrent.metainfo['info']

def test_getting_info__xs_fails__as_succeeds(generated_singlefile_torrent, httpserver, monkeypatch):
    torrent = generated_singlefile_torrent
    total_timeout = 100
    now = 0.0
    mock_time_monotonic = mock.MagicMock(return_value=now)
    monkeypatch.setattr(time, 'monotonic', mock_time_monotonic)

    def timed_out_download(url, *args, **kwargs):
        # First download() call (xs) took almost all our available time
        mock_time_monotonic.return_value = now + total_timeout - 1
        # Remove mock for second download() call (as)
        download_patch.stop()
        raise torf.ConnectionError(url, 'Nope')
    download_patch = mock.patch('torf._utils.download', timed_out_download)
    download_patch.start()

    httpserver.expect_request('/as.torrent').respond_with_data(torrent.dump())
    magnet = torf.Magnet(torrent.infohash,
                         xs='http://xs.foo:123/torrent',
                         as_=httpserver.url_for('/as.torrent'))

    cb = mock.MagicMock()
    assert magnet.get_info(callback=cb, timeout=total_timeout) is True
    exp_calls = [mock.call(ComparableException(torf.ConnectionError('http://xs.foo:123/torrent', 'Nope')))]
    assert cb.call_args_list == exp_calls

    torrent_ = magnet.torrent()
    assert torrent_.metainfo['info'] == torrent.metainfo['info']

def test_getting_info__xs_returns_invalid_bytes(generated_singlefile_torrent, httpserver):
    torrent = generated_singlefile_torrent
    magnet = torf.Magnet(torrent.infohash,
                         xs=httpserver.url_for('/torrent'), as_='http://as.foo:123/torrent')
    httpserver.expect_request('/torrent').respond_with_data(b'not bencoded bytes')

    cb = mock.MagicMock()
    assert magnet.get_info(callback=cb) is False
    exp_calls = [mock.call(ComparableException(torf.BdecodeError())),
                 mock.call(ComparableException(torf.ConnectionError('http://as.foo:123/torrent', 'Name or service not known')))]
    assert cb.call_args_list == exp_calls

    torrent_ = magnet.torrent()
    assert torrent_.metainfo['info'] == {}

def test_getting_info__as_returns_invalid_bytes(generated_singlefile_torrent, httpserver):
    torrent = generated_singlefile_torrent
    magnet = torf.Magnet(torrent.infohash,
                         xs='http://xs.foo:123/torrent', as_=httpserver.url_for('/torrent'))
    httpserver.expect_request('/torrent').respond_with_data(b'not bencoded bytes')

    cb = mock.MagicMock()
    assert magnet.get_info(callback=cb) is False
    exp_calls = [mock.call(ComparableException(torf.ConnectionError('http://xs.foo:123/torrent', 'Name or service not known'))),
                 mock.call(ComparableException(torf.BdecodeError()))]
    assert cb.call_args_list == exp_calls

    torrent_ = magnet.torrent()
    assert torrent_.metainfo['info'] == {}

def test_getting_info__xs_times_out(generated_singlefile_torrent, monkeypatch):
    torrent = generated_singlefile_torrent
    total_timeout = 100
    now = 0.0
    mock_time_monotonic = mock.MagicMock(return_value=now)
    monkeypatch.setattr(time, 'monotonic', mock_time_monotonic)

    def timed_out_download(url, *args, **kwargs):
        # First download() call (xs) took almost all our available time
        mock_time_monotonic.return_value = now + total_timeout
        # Remove mock for second download() call (as)
        download_patch.stop()
        raise torf.ConnectionError(url, 'Timed out (mocked)')
    download_patch = mock.patch('torf._utils.download', timed_out_download)
    download_patch.start()

    magnet = torf.Magnet(torrent.infohash,
                         xs='http://xs.foo:123/torrent',
                         as_='http://as.foo:123/torrent')

    cb = mock.MagicMock()
    assert magnet.get_info(callback=cb, timeout=total_timeout) is False
    exp_calls = [mock.call(ComparableException(torf.ConnectionError('http://xs.foo:123/torrent', 'Timed out (mocked)'))),
                 mock.call(ComparableException(torf.ConnectionError('http://as.foo:123/torrent', 'Timed out')))]
    assert cb.call_args_list == exp_calls

    torrent_ = magnet.torrent()
    assert torrent_.metainfo['info'] == {}

def test_getting_info_from_ws(generated_multifile_torrent, httpserver):
    torrent = generated_multifile_torrent
    magnet = torf.Magnet(torrent.infohash, ws=[httpserver.url_for('/bar//')])

    httpserver.expect_request('/bar.torrent').respond_with_data(torrent.dump())
    cb = mock.MagicMock()
    assert magnet.get_info(callback=cb) is True
    assert cb.call_args_list == []

    torrent_ = magnet.torrent()
    assert torrent_.metainfo['info'] == torrent.metainfo['info']

def test_getting_info_from_tr(generated_multifile_torrent, httpserver):
    torrent = generated_multifile_torrent
    magnet = torf.Magnet(torrent.infohash, tr=[httpserver.url_for('/announce')])

    infohash_enc = urllib.parse.quote_from_bytes(binascii.unhexlify(torrent.infohash))
    httpserver.expect_request('/file', query_string=f'info_hash={infohash_enc}').respond_with_data(torrent.dump())
    cb = mock.MagicMock()
    assert magnet.get_info(callback=cb) is True
    assert cb.call_args_list == []

    torrent_ = magnet.torrent()
    assert torrent_.metainfo['info'] == torrent.metainfo['info']
