from setuptools import setup, find_packages

import re
version_match = re.search(r"^__version__\s*=\s*['\"]([^'\"]*)['\"]",
                          open('torf/__init__.py').read(), re.M)
if version_match:
    __version__ = version_match.group(1)
else:
    raise RuntimeError("Unable to find __version__")

try:
    long_description = open('README.rst').read()
except OSError:
    long_description = ''

setup(
    name               = 'torf',
    version            = __version__,
    license            = 'GPLv3+',
    packages           = find_packages(),
    python_requires    = '>=3.6, ==3.*',
    install_requires   = ['flatbencode==0.2.*'],

    author             = 'Random User',
    author_email       = 'rndusr@posteo.de',
    description        = 'Python 3 module for creating and parsing torrent files and magnet URIs',
    long_description   = long_description,
    keywords           = 'bittorrent torrent magnet',
    url                = 'https://github.com/rndusr/torf',

    classifiers        = [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: Software Development :: Libraries',
    ]
)
