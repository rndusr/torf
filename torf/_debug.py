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

import logging
import threading
_debug_lock = threading.Lock()
def enable_debugging(filepath=None):
    logging.basicConfig(level=logging.DEBUG, format='%(message)s',
                        filename=filepath)

def _pretty_bytes(b):
    if isinstance(b, (bytes, bytearray)):
        if len(b) > 8:
            return b[:8].hex() + '...' + b[-8:].hex()
        else:
            return b.hex()
    else:
        return b

def debug(msg):
    with _debug_lock:
        logging.debug(msg)
debug.pretty_bytes = _pretty_bytes
