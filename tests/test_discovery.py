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

from functools import wraps
import shutil
import tempfile
import time

from zmq.utils import z85

from peerz.api import Network
from peerz.routing import distance_sort, generate_random

def tmproot(f):
    """
    Test decorator to create a temp working dir and cleanup
    afterwards.
    
    Passes the following params to the wrapped function:
    @param root: Temp root path for local storage
    @param closeme: A list of Network instances, that the function
    can add to, that will be closed at the end of the test run.
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        root = tempfile.mkdtemp('peerz_test')
        closeme = []
        try:
            return f(root, closeme, *args, **kwargs)
        finally:
            for x in closeme:
                try:
                    x.leave()
                except:
                    pass
            shutil.rmtree(root, ignore_errors=True)
    return wrapper

@tmproot
def test_find_nodes(root, closeme):
    # seed
    net = Network([], root)
    net.join()
    node = net.get_local()
    seeds = ["{0}:{1}:{2}".format(node['address'], node['port'], node['node_id'])]
    closeme.append(net)
    # can find self
    assert net.find_nodes(node['node_id']) != None

    # check for consistency across nodes
    net2 = Network(seeds, root)
    net2.join()
    closeme.append(net2)
    net3 = Network(seeds, root)
    net3.join()
    closeme.append(net3)

    # some more random nodes
    for _ in range(20):
        n = Network(seeds, root)
        n.join()
        closeme.append(n)

    # give them a bit of time to register with the seed
    time.sleep(2)

    # find a random id
    target = z85.encode(generate_random())
    
    # just pull out the id's for comparison
    nodes = [ x['node_id'] for x in net2.find_nodes(target, 5) ]
    # same response from other nodes
    assert nodes == [ x['node_id'] for x in net3.find_nodes(target, 5) ]
    
    # check that the routing agrees they're in distance order
    # need to transform back to raw bytes
    nodes = [ z85.decode(x) for x in nodes ]
    nodes2 = list(nodes)
    distance_sort(nodes2, z85.decode(target))
    assert nodes == nodes2

@tmproot
def test_publish(root, closeme):
    # seed
    net = Network([], root)
    net.join()
    node = net.get_local()
    seeds = ["{0}:{1}:{2}".format(node['address'], node['port'], node['node_id'])]
    closeme.append(net)
    
    net.publish('foo', 'bar')
    assert net.fetch('foo') == 'bar'
    
    # check for consistency across nodes
    net2 = Network(seeds, root)
    net2.join()
    closeme.append(net2)
    assert net2.fetch('foo') == 'bar'
    
    net3 = Network(seeds, root)
    net3.join()
    closeme.append(net3)
    assert net3.fetch('foo') == 'bar'


    