# Peerz - P2P python library using ZeroMQ sockets and gevent
# Copyright (C) 2014-2015 Steve Henderson
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import pkg_resources

from peerz.messaging import discovery, hashtable
# Mapping of known context Id -> Messaging class
registry = {
    discovery.Discovery.id: discovery.Discovery,
    hashtable.DistributedHashtable.id: hashtable.DistributedHashtable,
}

for context in pkg_resources.iter_entry_points(group='peerz.messaging'):
    c = context.load()
    registry[c.id] = c
