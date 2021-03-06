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

import tempfile
import urllib
import urllib2
import webbrowser

from peerz.routing import generate_random, Node, RoutingZone

def render_graph(dot):
    with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
        data = urllib.urlencode({'cht': 'gv:dot', 'chl': dot})
        print data
        u = urllib2.urlopen('http://chart.apis.google.com/chart', data)
        tmp.write(u.read())
        tmp.close()
        webbrowser.open_new_tab(tmp.name)

if __name__ == '__main__':
    node = Node('a', 0, generate_random())
    nodetree = RoutingZone(node.node_id, bdepth=4, binsize=4)
    nodetree.add(node)
    for x in xrange(1, 500):
        nodetree.add(Node('b', x, generate_random()))
    render_graph(nodetree.visualise())
