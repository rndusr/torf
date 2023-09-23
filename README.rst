torf
====

torf provides a ``Torrent`` and a ``Magnet`` class.

`torf-cli <https://github.com/rndusr/torf-cli>`_ and `torf-gui
<https://github.com/SavageCore/torf-gui>`_ provide user interfaces for torf.

This project started as a fork of `dottorrent
<https://github.com/kz26/dottorrent>`_ but turned into a rewrite.

Features
--------

- Create a ``Torrent`` instance from a path to the torrent's content or by
  reading an existing ``.torrent`` file
- High-level access to standard metainfo fields via properties
- Low-level access to arbitrary metainfo fields via ``metainfo`` property
- Optional metainfo validation with helpful error messages
- Generate a `BTIH magnet URI
  <https://en.wikipedia.org/wiki/Magnet_URI_scheme>`_ from a ``.torrent`` file
  (the reverse is also possible but the resulting torrent is incomplete due to
  the lack of information in magnet URIs)
- Use multiple CPU cores to compute piece hashes
- Randomize the info hash to help with cross-seeding
- Conveniently re-use piece hashes from an existing torrent file

Example
-------

.. code:: python

    from torf import Torrent
    t = Torrent(path='path/to/content',
                trackers=['https://tracker1.example.org:1234/announce',
                          'https://tracker2.example.org:5678/announce'],
                comment='This is a comment')
    t.private = True
    t.generate()
    t.write('my.torrent')

Documentation
-------------

Everything should be explained in the docstrings. Read it with ``pydoc3
torf.Torrent`` or ``pydoc3 torf.Magnet``.

Documentation is also available at `torf.readthedocs.io
<https://torf.readthedocs.io/>`_ or `torf.readthedocs.io/en/latest
<https://torf.readthedocs.io/en/latest>`_ for the development version.

Installation
------------

torf is available on `PyPI <https://pypi.org/project/torf>`_.

The latest development version is in the master branch on `GitHub
<https://github.com/rndusr/torf>`_.

Contributing
------------

I consider this project feature complete, but feel free to request new features
or improvements. Bug reports are always welcome, of course.

License
-------

`GPLv3+ <https://www.gnu.org/licenses/gpl-3.0.en.html>`_
