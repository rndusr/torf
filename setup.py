from setuptools import setup, find_packages

with open('torf/_version.py') as f:
    exec(f.read())

try:
    long_description = open('README.rst').read()
except OSError:
    long_description = ''

setup(
    name               = 'torf',
    version            = __version__,
    packages           = find_packages(),
    install_requires   = ['bencoder.pyx>=1.1.1,<3.0.0'],

    author             = 'Random User',
    author_email       = 'rndusr@posteo.de',
    description        = 'High-level Python 3 module for creating and parsing torrent files',
    long_description   = long_description,
    keywords           = 'bittorrent torrent bencode magnet',
    url                = 'https://github.com/rndusr/torf',

    classifiers        = [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6'
    ]
)
