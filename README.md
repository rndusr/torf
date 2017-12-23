# torf

torf provides a high-level, flexible `Torrent` class that holds torrent metainfo
and can export it to and import it from `.torrent` files. It can also create
[BTIH magnet links](https://en.wikipedia.org/wiki/Magnet_link#BitTorrent_info_hash_(BTIH)).

It started as a fork of [dottorrent](https://github.com/kz26/dottorrent) but
turned into a rewrite with more features like full control over the torrent's
metainfo, validation, randomization of the info hash to help with cross-seeding
and more.

Documentation
-------------

**Example usage**

```python
t = torf.Torrent(path='path/to/content',
                 trackers=['https://tracker1.example.org:1234/announce',
                           'https://tracker1.example.org:1234/announce'],
                 comment='This is a comment')
t.private = True
with open('my.torrent', 'wb') as f:
    t.write(f)
```

Everything is documented in the docstrings. Run `pydoc3 torf.Torrent` to read
it.

Installation
------------

torf is available on [PyPI](https://pypi.org/project/torf).

The latest development version is in the master branch on
[GitHub](https://github.com/rndusr/torf).

License
-------

[MIT](https://opensource.org/licenses/MIT)
