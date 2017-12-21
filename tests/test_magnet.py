import torf

import pytest
from urllib.parse import quote_plus


def test_singlefile_without_trackers(singlefile_content):
    t = torf.Torrent(singlefile_content.path)
    t.generate()
    assert t.magnet() == f'magnet:?xt=urn:btih:{t.infohash}&dn={quote_plus(t.name)}&xl={t.size}'

def test_singlefile_with_trackers(singlefile_content):
    t = torf.Torrent(singlefile_content.path, trackers=['http://foo', 'http://bar'])
    t.generate()
    assert t.magnet() == (f'magnet:?xt=urn:btih:{t.infohash}&dn={quote_plus(t.name)}&xl={t.size}'
                          '&tr=http%3A%2F%2Ffoo&tr=http%3A%2F%2Fbar')


def test_multifile_without_trackers(multifile_content):
    t = torf.Torrent(multifile_content.path)
    t.generate()
    assert t.magnet() == f'magnet:?xt=urn:btih:{t.infohash}&dn={quote_plus(t.name)}&xl={t.size}'

def test_multifile_with_trackers(multifile_content):
    t = torf.Torrent(multifile_content.path, trackers=['http://foo', 'http://bar'])
    t.generate()
    assert t.magnet() == (f'magnet:?xt=urn:btih:{t.infohash}&dn={quote_plus(t.name)}&xl={t.size}'
                          '&tr=http%3A%2F%2Ffoo&tr=http%3A%2F%2Fbar')
