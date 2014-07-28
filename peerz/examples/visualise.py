# Peerz - P2P python library using ZeroMQ sockets and gevent
# Copyright (C) 2014 Steve Henderson
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
import sys
import tempfile
import urllib
import urllib2
import webbrowser

from peerz.persistence import LocalStorage

def get_tree(root, port):
    local = LocalStorage(root, port)
    return local.fetch('nodetree')

def render_graph(dot):
    with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
        data = urllib.urlencode({'cht': 'gv:dot', 'chl': dot})
        print data
        u = urllib2.urlopen('http://chart.apis.google.com/chart', data)
        tmp.write(u.read())
        tmp.close()
        webbrowser.open_new_tab(tmp.name)

if __name__ == '__main__':
    """
    Simple tool to read the state files from running helloworld example
    and plot the routing tree for the chosen node using google charts.
    """
    root = '/tmp/helloworld'
    port = 7111
    if len(sys.argv) > 2:
        port = int(sys.argv[2])
    if len(sys.argv) > 1:
        root = sys.argv[1]
    
    dot = get_tree(root, port).visualise()
    render_graph(dot)
