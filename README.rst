torf
====

torf is a Python (>=3.6) module for creating, parsing and editing .torrent
files. It started as a fork of `dotorrent <https://github.com/kz26/dottorrent>`_
but turned into a rewrite with more features like full access to the torrent's
metainfo, magnet link generation, validation and more.

Installation
------------

torf is available on `PyPI <https://pypi.org/project/torf>`_.

The latest development version is in the master branch on
`Github <https://github.com/rndusr/torf>`_.

License
-------

`MIT <https://opensource.org/licenses/MIT>`_

Documentation
-------------

::

   Create a new torrent object:

   >>> from torf import Torrent
   >>> torrent = Torrent('path/to/My Torrent',
   ...                   trackers=['https://localhost:123/announce'],
   ...                   comment='This is my first torrent')

   Convenient access to metainfo via properties:

   >>> torrent.comment
   'This is my first torrent. Be gentle.'
   >>> torrent.private = True

   Full control over unencoded metainfo:

   >>> torrent.metainfo['info']['private']
   True
   >>> torrent.metainfo['more stuff'] = {'foo': 12,
   ...                                   'bar': ('x', 'y', 'z')}

   Start hashing and update progress once per second:

   >>> def callback(filepath, pieces_done, pieces_total):
   ...     print(f'{pieces_done/pieces_total*100:3.0f} % done')
   >>> success = torrent.generate(callback, interval=1)
     1 % done
     2 % done
     [...]
   100 % done

   Write torrent file:

   >>> with open('my_torrent.torrent', 'wb') as f:
   ...    torrent.write(f)

   Generate magnet link:

   >>> torrent.magnet()
   'magnet:?xt=urn:btih:e167b1fbb42ea72f051f4f50432703308efb8fd1&dn=My+Torrent&xl=142631&tr=https%3A%2F%2Flocalhost%3A123%2Fannounce'

   Read torrent:

   >>> with open('my_torrent.torrent', 'rb') as f:
   ...    t = Torrent.read(f)
   >>> t.comment
   'This is my first torrent. Be gentle.'

   Methods defined here:

   __init__(self, path=None, exclude=(), trackers=(), webseeds=(), httpseeds=(),
            private=False, comment=None, creation_date=None, created_by=None,
            source=None, piece_size=None, include_md5=False)
       Initialize self.

   convert(self)
       Return `metainfo` with all keys encoded to bytes and all values encoded
       to bytes, int, list or OrderedDict

       Raise MetainfoError on values that cannot be converted properly.

   dump(self, validate=True)
       Return `metainfo` as validated, bencoded byte string

       validate: Whether to run validate() first

   generate(self, callback=None, interval=0)
       Set 'pieces' in 'info' dictionary of `metainfo`

       callback: Callable with signature (filename, pieces_completed,
                 pieces_total); if `callable` returns anything that is not
                 None, hashing is canceled
       interval: Number of seconds between calls to `callback`

       Raise PathEmptyError if `path` contains no data.

       Return True if 'pieces' was successfully added to `metainfo`.
       Return False if `callback` canceled the operation.

   magnet(self, name=True, size=True, trackers=True, tracker=False, validate=True)
       BTIH Magnet URI (generate() must run first)

       name: Whether to include the name
       size: Whether to include the size
       trackers: Whether to include all trackers
       tracker: Whether to include only the first tracker of the first tier
                (overrides `trackers`)
       validate: Whether to run validate() first

   validate(self)
       Check if all mandatory keys exist in `metainfo` and are of expected types

       The necessary values are documented here:

           http://bittorrent.org/beps/bep_0003.html
           https://wiki.theory.org/index.php/BitTorrentSpecification#Metainfo_File_Structure

       Note that 'announce' is not considered mandatory because of DHT.

       Raise MetainfoError if `metainfo` would not generate a valid torrent
       file or magnet link.

   write(self, stream, validate=True)
       Write torrent metainfo to file object (generate() must run first)

       stream: A stream or file object (must be opened in 'wb' mode)
       validate: Whether to run validate() first

   ----------------------------------------------------------------------
   Class methods defined here:

   read(stream, validate=True) from builtins.type
       Read torrent metainfo from file object

       stream: A stream or file object (must be opened in 'rb' mode)
       validate: Whether to run validate() on the Torrent object

       Raise MetainfoParseError if metainfo is not a valid bencoded byte
       string.

       Return a new Torrent object.

   ----------------------------------------------------------------------
   Data descriptors defined here:

   comment
       Comment string or None

       Setting this property sets or removes 'comment' in `metainfo`.

   created_by
       Application name or None

       Setting this property sets or removes 'created by' in `metainfo`.

   creation_date
       datetime object, int (as from time.time()) or None

       Setting this property sets or removes 'creation date' in `metainfo`.

   exclude
       List of filename patterns to exclude:

           *      matches everything
           ?      matches any single character
           [seq]  matches any character in seq
           [!seq] matches any char not in seq

   filepaths
       Yield absolute paths to local files in `path`

   files
       Yield relative paths to files specified in `metainfo`

       Paths include the torrent's name.

       Note that the paths may not exist. See `filepaths` for existing files.

   httpseeds
       List of httpseed URLs or None

       http://bittorrent.org/beps/bep_0017.html

   include_md5
       Whether to include MD5 sums for each file

       This takes only effect when generate() is called.

   infohash
       SHA1 info hash (generate() must run first)

   infohash_base32
       Base32 encoded SHA1 info hash (generate() must run first)

   metainfo
       Unencoded torrent metainfo as mutable mapping

       You can put anything in here as long as keys are convertable to bytes
       and values are convertable to bytes, int, list or dict. See also
       convert() and validate().

       'info' is guaranteed to exist.

   name
       Torrent name

       Default to last item in `path` or None if `path` is None.

       Setting this property sets or removes 'name' in the 'info' dictionary of
       `metainfo`.

   path
       Path to torrent content or None

   piece_size
       Piece size/length or None to pick one automatically

       Setting this property sets 'piece length' in the 'info' dictionary in
       `metainfo`.

       Getting this property if it hasn't been set calculates 'piece length' so
       that there are approximately 1500 pieces in total. The result is stored
       in `metainfo`.

   private
       Whether torrent should use trackers exclusively for peer discovery

       Setting this property sets or removes 'private' in the 'info' dictionary
       of `metainfo`.

   randomize_infohash
       Whether to ensure that `infohash` is always different

       This allows cross-seeding without changing `piece_size` manually.

       Setting this property to True sets 'entropy' in the 'info' dictionary of
       `metainfo` to a random integer. Setting it to False removes it if
       present.

   size
       Total size of content in bytes

       If the 'info' dictionary in `metainfo` doesn't have 'length' or 'files'
       set, return None instead.

   source
       Source string or None

       Setting this property sets or removes 'source' in `metainfo`.

   trackers
       List of tiers of announce URLs or None

       A tier is either a single announce URL (string) or a list (any iterable)
       of announce URLs.

       Setting this property sets or removes 'announce' and 'announce-list' in
       `metainfo`. 'announce' is set to the first tracker of the first tier.

   webseeds
       List of webseed URLs or None

       http://bittorrent.org/beps/bep_0019.html

   ----------------------------------------------------------------------
   Data and other attributes defined here:

   MAX_PIECE_SIZE = 67108864

   MIN_PIECE_SIZE = 16384
