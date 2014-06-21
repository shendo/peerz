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

from collections import OrderedDict
from datetime import datetime, timedelta
import logging
import socket
import uuid

import gevent

LOG = logging.Logger(__name__)
EPOCH = datetime.utcfromtimestamp(0)

# K number of nodes per bucket/bin
K = 10
# lookup accelaration by allowing B further
# splits/depth for non-node_id subtrees
B = 5
# number of simultaneous/outstanding queries 
A = 3
# bit length of node id/key values
KEY_BITS = 128

def time_in_future(seconds):
    """
    Returns time in seconds, since 1970,
    the specified number of seconds into the future.
    Precision is guaranteed to be at least to second
    resolution but may also be decimal for subsecond.
    @param seconds: How many seconds in future to return
    @retrun: Seconds since 1970 epoch
    """
    return (datetime.utcnow() - EPOCH + timedelta(seconds=seconds)) \
        .total_seconds()

def distance(node_id1, node_id2):
    """
    The XOR distance betwen two keys/nodes.
    @param node_id1: Node id/key 1 as long
    @param node_id2: Node id/key 2 as long
    @return: The distance as long
    """
    return node_id1 ^ node_id2

def bit_number(node_id, bit):
    """
    Returns value of the specified bit, counting from
    MSB = 0, for the specified node_id/key.
    The node_id is treated as a key of KEY_BITS length
    regardless of current size.
    @param node_id: Node id/key as long
    @param bit: Bit position to return
    @return: Value at bit position 'bit' or 0 if > KEY_BITS
    """
    if bit >= KEY_BITS:
        return 0
    return (node_id >> (KEY_BITS - 1 - bit)) & 1 
    
    
class Overlay(object):
    """
    Encapsulates the logic and state storage for the
    overlay network implementation.
    No network communication to be performed directly
    by this class, instead it registers callbacks
    on the connection class and _requests_ join and
    leave actions via the connection.
    """ 
    
    def __init__(self, connection):
        """
        Creates the overlay but does not start
        any interaction with peers/network.
        @param connection: Connection object for interacting
        with underlying network
        """
        self.connection = connection
        self.nodelist = RoutingBin()
        self.peers = {}
        self.connection.add_listener(self)
        self.seeds = []
        self.node = None
        
    def generate_id(self):
        """
        Create a new random key/id KEY_BITS in size.
        Large key sizes should minimises chances of collision.
        @return: New randomly generated id
        """
        return int(uuid.uuid4())
    
    def get_peers(self):
        return self.peers.values()
    
    def get_peer(self, node_id):
        return self.peers.get(node_id)
    
    def get_known_nodes(self):
        return self.nodelist.nodes.values()
    
    def route_to(self, node_id):
        return None
    
    def peer_updated(self, node):
        self.nodelist.update(node)
    
    def peer_joined(self, node):
        node.last_connected = time_in_future(0)
        if not node.first_connected:
            node.first_connected = node.last_connected
        self.peers[node.node_id] = self.nodelist.update(node)
    
    def peer_left(self, node):
        del self.peers[node.node_id]

    def peer_peerlist(self, nodes):
        for x in nodes:
            self.nodelist.update(x)
    
    def _attempt_peer(self, endpoint):
        self.connection._join(endpoint)
        
    def manage_peers(self):        
        while not self.connection.shutdown:
            if not self.peers:
                # try to bootstrap from any known node
                for x in self.seeds + [ "{0}:{1}".format(x.address, x.port)
                                       for x in self.nodelist.get_all() ]:
                    self._attempt_peer(x)
                    if self.peers:
                        break
            elif len(self.peers) < self.max_peers:
                # manage existing peers
                for x in [ "{0}:{1}".format(x.address, x.port)
                          for x in self.nodelist.get_all() if not x.node_id in self.peers ]:
                    self._attempt_peer(x)
                    if len(self.peers) >= self.max_peers:
                        break
            gevent.sleep(10)
    
class Node(object):
        
    def __init__(self, node_id, address, port):
        self.node_id = node_id
        self.address = address
        self.port = int(port)
        self.hostname = socket.getfqdn(address)
        self.first_connected = None
        self.last_connected = None
        self.last_activity = None
        self.latency_ms = 0
    
    def to_json(self):
        return {'node_id': self.str_id(),
                'address': self.address,
                'port': self.port,
                'hostname': self.hostname,
                'first_connected': self.first_connected,
                'last_connected': self.last_connected,
                'last_activity': self.last_activity,
                'latency_ms': self.latency_ms
                }
        
    def str_id(self):
        return uuid.UUID(int=self.node_id)
    
    def __str__(self):
        return '{0} - {1}:{2} ({3})'.format(self.str_id(), self.address, self.port, self.hostname)

class RoutingBin(object):

    def __init__(self):
        self.nodes = OrderedDict()
    
    def get_by_id(self, node_id):
        for x in self.nodes.values():
            if x.node_id == node_id:
                return x
        return None
    
    def get_by_addr(self, addr, port):
        return self.nodes.get((addr, port))
    
    def get_all(self):
        return self.nodes.values()
    
    def get_node_ids(self):
        return [ x.node_id for x in self.get_all() ]
    
    def get_oldest(self):
        node = None
        for x in self.nodes.values():
            if not node \
                or (not node.last_connected and x.last_connected) \
                or (x.last_connected and x.last_connected < node.last_connected):
                node = x
        return node
    
    def get_closest_to(self, target, max_nodes=1):
        distances = sorted([ distance(x, target) for x in self.get_node_ids() ])
        distances = distances[:max_nodes]
        return [ self.get_by_id(distance(target, x)) for x in distances ]
    
    def size(self):
        return len(self.nodes)
    
    def remaining(self):
        return K - self.size()
    
    def push_to_bottom(self, node):
        del self.nodes[(node.address, node.port)]
        self.nodes[(node.address, node.port)] = node
        
    def update(self, node):
        """Return a node guaranteed to be in the table
        with the values contained in supplied node.
        """
        n = self.nodes.get((node.address, node.port))
        if n:
            if node.latency_ms:
                n.latency_ms = node.latency_ms
            if node.last_connected:
                n.last_connected = node.last_connected
            if not n.first_connected:
                n.first_connected = node.first_connected
        else:
            self.nodes[(node.address, node.port)] = node
            n = node

        n.last_activity = time_in_future(0)
        return n
    
    
class RoutingZone(object):
    
    def __init__(self, root, depth, index, node_id):
        self.root = root
        self.children = [None, None]
        self.index = index
        self.depth = depth
        self.routing_bin = RoutingBin()
        self.node_id = node_id
        
    def add(self, node):
        if self.is_leaf():
            if self.can_split():
                self.split()

            # still a leaf...
            if self.is_leaf() and self.routing_bin.remaining():
                self.routing_bin.update(node)
            # otherwise we've reached limit
            # TODO replace or discard logic
        else:
            index = bit_number(distance(self.node_id, node.node_id), self.depth)
            self.children[index].add(node)
            
    def is_leaf(self):
        return self.routing_bin
    
    def can_split(self):
        return self.depth <= KEY_BITS and \
            not self.routing_bin.remaining() and \
            (self.node_id in self.routing_bin.get_node_ids() or \
             self.depth < B)
            
    def get_node_by_id(self, node_id):
        if self.is_leaf():
            return self.routing_bin.get_by_id(node_id)
        else:
            node = self.children[0].get_node_by_id(node_id)
            if not node:
                node = self.children[1].get_node_by_id(node_id)
            return node
    
    def get_node_by_addr(self, address, port):
        if self.is_leaf():
            return self.routing_bin.get_by_addr(address, port)
        else:
            node = self.children[0].get_node_by_addr(address, port)
            if not node:
                node = self.children[1].get_node_by_addr(address, port)
            return node

    def get_all_nodes(self, depth=KEY_BITS-1):
        nodes = []
        if not depth:
            return nodes
        if self.is_leaf():
            return self.routing_bin.get_all()
        else:
            nodes += self.children[0].get_all_nodes(depth-1)
            nodes += self.children[1].get_all_nodes(depth-1)
            return nodes
    
    def closest_to(self, target, distance, max_nodes):
        return []
    
    def max_depth(self):
        if self.is_leaf():
            return self.depth
        else:
            return max(self.children[0].max_depth(), 
                       self.children[1].max_depth())
    
    def consolidate(self):
        assert not self.is_leaf()
        self.routing_bin = RoutingBin()
        for x in self.children[0].routing_bin.get_all() + \
            self.children[1].routing_bin.get_all():
            self.routing_bin.update(x)
        self.children = [None, None]
    
    def split(self):
        assert self.is_leaf()
        self.children[0] = RoutingZone(self, self.depth+1, self.index, self.node_id)
        self.children[1] = RoutingZone(self, self.depth+1, self.index, self.node_id)
        # split based on matching prefix
        for x in self.routing_bin.get_all():
            index = bit_number(distance(self.node_id, x.node_id), self.depth)
            self.children[index].add(x)

        self.routing_bin = None
