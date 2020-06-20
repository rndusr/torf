from collections import OrderedDict
from datetime import datetime

import pytest

import torf


def test_valid_metainfo():
    t = torf.Torrent(created_by=None)
    now = datetime.now()
    t.metainfo['foo'] = now
    t.metainfo['baz'] = {'one': True, 'two': 2.34,
                         'bam': ['x', 'y', ('z',False)]}

    exp = OrderedDict([(b'baz', OrderedDict([(b'bam', [b'x', b'y', [b'z', 0]]),
                                             (b'one', 1),
                                             (b'two', 2)])),
                       (b'foo', int(now.timestamp())),
                       (b'info', OrderedDict())])

    assert t.convert() == exp


def test_invalid_metainfo():
    t = torf.Torrent()

    t.metainfo['invalid'] = lambda foo: 'bar'
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.convert()
    assert excinfo.match("Invalid value: .*lambda")

    t.metainfo['invalid'] = {'arf': int}
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.convert()
    assert excinfo.match("Invalid value: <class 'int'>")

    t.metainfo['invalid'] = [3, ['a', 'b', {str: 'c'}], 4, 5]
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.convert()
    assert excinfo.match("Invalid key: <class 'str'>")

    t.metainfo['invalid'] = {'x': [3, ['a', 'b', {Exception, 'c'}], 4, 5]}
    with pytest.raises(torf.MetainfoError) as excinfo:
        t.convert()
    assert excinfo.match("Invalid value: <class 'Exception'>")


def test_metainfo_sort_order(create_torrent):
    torrent = create_torrent()
    md_conv = torrent.convert()
    exp_keys = sorted(bytes(key, encoding='utf-8', errors='replace')
                      for key in torrent.metainfo)
    assert list(md_conv) == exp_keys

    exp_info_keys = sorted(bytes(key, encoding='utf-8', errors='replace')
                           for key in torrent.metainfo['info'])
    assert list(md_conv[b'info']) == exp_info_keys
