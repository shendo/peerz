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

from functools import wraps
import shutil
import tempfile

from peerz.core import Network
from peerz.routing import generate_random

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
            shutil.rmtree(root)
            for x in closeme:
                try:
                    x.leave()
                except:
                    pass
            
    return wrapper

@tmproot
def test_find_nodes(root, closeme):
    # seed
    net = Network(7001, root)
    net.join(['localhost:7001'])
    closeme.append(net)
    node = net.get_local()
    
    # can find self
    assert net.find_nodes(node.node_id) != None

    # check for consistency across nodes
    net2 = Network(7002, root)
    net2.join(['localhost:7001'])
    closeme.append(net2)
    net3 = Network(7003, root)
    net3.join(['localhost:7001'])
    closeme.append(net3)

    # some more random nodes
    for i in range(7004, 7024):
        closeme.append(Network(i, root).join(['localhost:7001']))

    # find a random id
    target = generate_random()
    # just pull out the id's for comparison
    nodes = [ x.node_id for x in net.find_nodes(target, 5) ]
    # same response from other nodes
    assert nodes == [ x.node_id for x in net2.find_nodes(target, 5) ]
    assert nodes == [ x.node_id for x in net3.find_nodes(target, 5) ]
