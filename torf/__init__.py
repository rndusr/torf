# This file is part of torf.
#
# torf is free software: you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# torf is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with torf.  If not, see <https://www.gnu.org/licenses/>.

# flake8: noqa

"""
Create and parse torrent files and magnet URIs
"""

__version__ = '3.1.3'

from ._errors import *
from ._magnet import Magnet
from ._torrent import Torrent
# Export File class so users can add items to Torrent.files and Filepath so it
# is documented by readthedocs.
from ._utils import File, Filepath
