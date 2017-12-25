torf
====

torf provides a high-level, flexible ``Torrent`` class that holds torrent
metainfo and can export it to and import it from ``.torrent`` files. It can also
create `BTIH magnet links
<https://en.wikipedia.org/wiki/Magnet_link#BitTorrent_info_hash_(BTIH)>`_.

It started as a fork of `dottorrent <https://github.com/kz26/dottorrent>`_ but
turned into a rewrite with more features like full control over the torrent's
metainfo, validation, randomization of the info hash to help with cross-seeding
and more.

Example
-------

.. code:: python

    t = torf.Torrent(path='path/to/content',
                     trackers=['https://tracker1.example.org:1234/announce',
                               'https://tracker1.example.org:1234/announce'],
                     comment='This is a comment')
    t.private = True
    with open('my.torrent', 'wb') as f:
        t.write(f)

Documentation
-------------

Documentation is in the docstrings. Read it locally by running ``pydoc3
torf.Torrent`` after the installation. It's also available `online
<https://rndusr.github.io/torf/>`_.

Installation
------------

torf is available on `PyPI <https://pypi.org/project/torf>`_.

The latest development version is in the master branch on `GitHub
<https://github.com/rndusr/torf>`_.

License
-------

`MIT <https://opensource.org/licenses/MIT>`_
