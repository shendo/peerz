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

LOG = logging.Logger(__name__)
EPOCH = datetime.utcfromtimestamp(0)

# K number of nodes per bucket/bin
K = 8
# lookup acceleration by allowing B further
# splits/depth for non-node_id subtrees
B = 5
# bit length of node id/key values
KEY_BITS = 128

def time_since_epoch(future=0):
    """
    Returns time in seconds, since 1 Jan, 1970 UTC,
    the specified number of seconds into the future.
    Precision is guaranteed to be at least to second
    resolution but may also be decimal for subseconds.
    @param future: How many seconds in future to return
    @retrun: Seconds since 1970 epoch
    """
    return (datetime.utcnow() - EPOCH + timedelta(seconds=future)) \
        .total_seconds()

def generate_id():
    """
    Create a new random key/id KEY_BITS in size.
    Large key sizes should minimises chances of collision.
    @return: New randomly generated id
    """
    return int(uuid.uuid4())

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

        
class Node(object):
    """
    Represents a node in the peer to peer network
    and its related details/statistics.
    """
    def __init__(self, node_id, address, port):
        """
        Create a new node.
        @param node_id: The unique node key
        @param address: IP address as string
        @param port: Server port number as string or int
        """
        self.node_id = node_id
        self.address = address
        self.port = int(port)
        self.hostname = socket.getfqdn(address)
        self.first_contact = None
        self.last_contact = None
        self.latency_ms = 0
        self.failures = 0
    
    def update(self, latency_ms):
        """
        Update this node due to recent activity.
        @param latency_ms: Node message latency in milliseconds.
        """
        if self.latency_ms:
            # smooth by averaging over previous values
            self.latency_ms = (self.latency_ms + latency_ms) / 2
        else:
            self.latency_ms = latency_ms
            
        if not self.first_contact:
            self.first_contact = time_since_epoch()
            
        self.last_contact = time_since_epoch()
        self.failures = 0
        
    def to_json(self):
        """
        Output the current node details in json
        @return JSON style dictionary
        """
        return {'node_id': Node.id_to_str(self.node_id),
                'address': self.address,
                'port': self.port,
                'hostname': self.hostname,
                'first_contact': self.first_contact,
                'last_contact': self.last_contact,
                'latency_ms': self.latency_ms,
                'failures': self.failures
                }
    
    @staticmethod
    def id_to_str(node_id):
        """
        @return: String representation of supplied numeric key/id.
        """
        return str(uuid.UUID(int=node_id))
    
    @staticmethod
    def str_to_id(node_id):
        """
        @return: Numeric representation of supplied string key/id.
        """
        return int(uuid.UUID(node_id))
    
    def __str__(self):
        """
        @return: Brief string representation of this node.
        """
        return '{0} - {1}:{2} ({3})'.format(Node.id_to_str(self.node_id), 
                                            self.address, self.port, 
                                            self.hostname)


class RoutingBin(object):
    """
    List of active nodes up to K size.
    Designated as 'k-buckets' in Kademlia literature.
    Intended for use as leaves of RoutingZones tree it acts as
    an LRU cache for known nodes but with a preference to keep nodes
    that have been active the longest duration.
    """
    def __init__(self, maxsize=K):
        self.maxsize = maxsize
        self.nodes = OrderedDict()
        self.replacements = OrderedDict()
        
    def get_by_id(self, node_id):
        """
        Return the node corresponding to the supplied id.
        @param node_id: Id of node to lookup.
        @return Node with node_id or None if not found.
        """
        return self.nodes.get(node_id)
    
    def get_by_address(self, address, port):
        """
        Return the node corresponding to the supplied address details.
        @param address: IP Address of node to lookup.
        @param port: Port number of node to lookup.
        @return Node with specified details or None id not found.
        """
        for x in self.nodes.values():
            if x.address == address and x.port == port:
                return x
        return None
    
    def get_all(self):
        """
        Return all nodes in this routing bin.
        @return List of nodes.
        """
        return self.nodes.values()
    
    def get_node_ids(self):
        """
        Return Ids of all nodes in this routing bin.
        @return List of node ids.
        """
        return self.nodes.keys()
    
    def push(self, node):
        """
        Adds the supplied node into the routing bin.
        If the bin is full it will overflow into the
        replacement cache.
        @param node: Node to be added.
        """
        node_id = node.node_id
        if self.remaining():
            self.nodes[node_id] = node
        else:
            # add to replacement cache
            # ensure pushed to end as most recent
            if node_id in self.replacements:
                self.replacements.pop(node_id)
            self.replacements[node_id] = node
            # trim if needed
            if self.replacements > self.maxsize:
                self.replacements.popitem()
                
    def get_oldest(self):
        """
        Returns the node that hasn't had activity for the 
        longest duration.
        @return: Oldest node in the active list.
        """
        return self.nodes.values()[0]
    
    def pop(self, node_id):
        """
        Removes the specified node from the routing bin.
        The node's place may be taken by another waiting
        in the replacement cache.
        @param node_id: Node to remove.
        @return: The node that was removed from the bin.
        """
        if not node_id in self.get_node_ids():
            return None
        # promote a replacement node if available
        if self.replacements:
            repl = self.replacements.popitem(last=True)
            self.nodes[repl.node_id] = repl
        return self.nodes.pop(node_id)
    
    def get_closest_to(self, target, max_nodes=1):
        """
        Return the node/s whose distance is the closest to the 
        supplied target id.
        @param target: Target Id for distance
        @param max_nodes: Maximum number of nodes to return.
        @return: A list of closest nodes with 0 < len() <= max_nodes
        """
        distances = sorted([ distance(x, target) for x in self.get_node_ids() ])
        distances = distances[:max_nodes]
        return [ self.get_by_id(distance(target, x)) for x in distances ]
    
    def remaining(self):
        """
        @return: The remaining space for active nodes in this bin.
        """
        return self.maxsize - len(self)
    
    def update(self, node_id):
        """
        Updates the specified node as having recent activity.
        @param node_id: Id of the node to move in list.
        """
        node = self.nodes.pop(node_id)
        self.nodes[node_id] = node
        
    def __len__(self):
        """
        @return: The number of active nodes in this bin.
        """
        return len(self.nodes)

    
class RoutingZone(object):
    """
    RoutingZones make up the routing tree of known/active nodes.
    Only leaves can contain routing bins with nodes.
    The term zone is used to avoid overloading the use of
    the word node.
    """
    def __init__(self, node_id, parent=None, depth=0, prefix='',
                 bdepth=B, binsize=K):
        """
        Creates a new zone in the routing tree.
        @param node_id: The id of our node (must be constant for entire tree).
        @param parent: This zone's parent, None for root.
        @param depth: How deep this zone is in the tree, 0 for root.
        @param prefix: String representation of the common routing prefix
        this zone represents. 
        @param bdepth: Extra depth allowed to split to in non-node_id 
        subtrees.  This allows greater knowledge of the network for faster
        lookups.
        @param binsize: Max size of routing bins for each leaf.
        """
        self.node_id = node_id
        self.parent = parent
        self.depth = depth
        self.prefix = prefix
        self.bdepth = bdepth
        self.binsize = binsize
        self.routing_bin = RoutingBin(binsize)
        self.children = [None, None]
        
    def add(self, node):
        """
        Add the specified node into the tree.
        Node is assumed to not already exist and is not guaranteed
        to be added if the routing zone is full for its given prefix.
        @param node: Node to add.
        """
        # split if needed
        if self._can_split():
            self._split()
        # still a leaf
        if self.is_leaf():
            self.routing_bin.push(node)
        else:
            index = bit_number(node.node_id, self.depth)
            self.children[index].add(node)
    
    def remove(self, node):
        """
        Remove the specified node from the tree.
        The node may not actually be removed if there is still
        available space in the routing zone for its given prefix.
        @param node: Node to remove.
        """
        if self.is_leaf():
            self.routing_bin.pop(node.node_id)

            if self.parent and self.parent._can_consolidate():
                self.parent._consolidate()
        else:
            index = bit_number(node.node_id, self.depth)
            self.children[index].remove(node)
            
    def is_leaf(self):
        """
        @return: True if this zone is a leaf, otherwise False.
        """
        return self.children[0] is None
    
    def get_node_by_id(self, node_id):
        """
        Find the node with the specified Id.
        @param node_id: Id of node to find
        @return: The corresponding node or None if not found.
        """
        if self.is_leaf():
            return self.routing_bin.get_by_id(node_id)
        else:
            node = self.children[0].get_node_by_id(node_id)
            if not node:
                node = self.children[1].get_node_by_id(node_id)
            return node
    
    def get_node_by_addr(self, address, port):
        """
        Find the node with the specified address and port.
        @param address: IP address of node to find
        @param port: UDP port number of node to find
        @return: The corresponding node or None if not found.
        """
        if self.is_leaf():
            return self.routing_bin.get_by_address(address, port)
        else:
            node = self.children[0].get_node_by_addr(address, port)
            if not node:
                node = self.children[1].get_node_by_addr(address, port)
            return node

    def get_all_nodes(self):
        """
        @return List of all active nodes in tree
        """
        nodes = []
        if self.is_leaf():
            return self.routing_bin.get_all()
        else:
            nodes += self.children[0].get_all_nodes()
            nodes += self.children[1].get_all_nodes()
            return nodes
    
    def closest_to(self, target, max_nodes=K):
        """
        Find and return the specified number of nodes
        closest in XOR distance to the supplied target value.
        @param target: Target id to calculate distance
        @param max_nodes: Maximum number of nodes to return
        @return: List of nodes where len() <= max_nodes.  Fewer
        nodes will be returned if there are not max_nodes available
        in the tree.
        """
        if self.is_leaf():
            return self.routing_bin.get_closest_to(target, max_nodes)
        else:
            index = bit_number(target, self.depth)
            nodes = self.children[index].closest_to(target, max_nodes)
            # not enough nodes.. try other side
            if len(nodes) < max_nodes:
                nodes += self.children[not index].closest_to(
                                            target, max_nodes-len(nodes))
            return nodes
            
    def max_depth(self):
        """
        @return: Maximum depth level of the tree.
        """
        if self.is_leaf():
            return self.depth
        else:
            return max(self.children[0].max_depth(), 
                       self.children[1].max_depth())
            
    def _can_split(self):
        """
        @return: True if this zone is eligible to split, otherwise False.
        """
        return self.is_leaf() and self.depth < KEY_BITS and \
            not self.routing_bin.remaining() and \
            (self.node_id in self.routing_bin.get_node_ids() or \
             self.depth < self.bdepth)
            
    def _can_consolidate(self):
        """
        @return: True if this zone is eligible to consolidate, otherwise False.
        """
        return not self.is_leaf() and \
            len(self.get_all_nodes()) <= self.binsize / 2
    
    def _consolidate(self):
        """
        Causes this zone to roll-up its child zones and become
        a leaf zone itself.
        """
        assert not self.is_leaf()
        self.routing_bin = RoutingBin(self.binsize)
        for x in self.get_all_nodes():
            self.routing_bin.push(x)
        self.children = [None, None]
    
    def _split(self):
        """
        Causes this leaf node to split its node list into two
        child zones and become a branch instead.
        """
        assert self.is_leaf()
        self.children[0] = RoutingZone(self.node_id, 
                                       self, 
                                       self.depth+1, 
                                       self.prefix + '0',
                                       self.bdepth,
                                       self.routing_bin.maxsize)
        self.children[1] = RoutingZone(self.node_id, 
                                       self, 
                                       self.depth+1, 
                                       self.prefix + '1',
                                       self.bdepth,
                                       self.routing_bin.maxsize)
        # split based on matching prefix
        for x in self.routing_bin.get_all():
            index = bit_number(x.node_id, self.depth)
            self.children[index].add(x)

        self.routing_bin = None
        
    def visualise(self):
        """
        Generate a dot file representing this routing zone/tree.
        This can help in debugging/visualising the current peer state.
        Content can be rendered using graphviz, google charts api, 
        or similar tool.
        @return String buffer with dot syntax reprensenting the tree.
        """
        return 'digraph G{{graph[ranskep=0];' \
            'node[shape=record];{0}{1}}}' \
                .format(self._generate_dot_nodes(),
                        self._generate_dot_edges())
        
    def _generate_dot_nodes(self):
        """
        Generate the dot node definitions for the digraph.
        @return String in dot syntax
        """ 
        def format_node(node):
            if self.node_id == node.node_id:
                return "{{** {0} **|{1}:{2}}}" \
                    .format(Node.id_to_str(node.node_id), 
                            node.address, node.port)
            return "{{{0}|{1}:{2}}}" \
                .format(Node.id_to_str(node.node_id), 
                        node.address, node.port)
        nodes = ""             
        if self.is_leaf():
            return '{0}[label="{{prefix={0}|{1}}}"];' \
                .format(self.prefix or 'None',
                    '|'.join([ format_node(x)
                        for x in self.routing_bin.get_all() ]))
        else:
            nodes += '{0}[label="prefix={0}"];' \
                .format(self.prefix or 'None')
            nodes += self.children[0]._generate_dot_nodes()
            nodes += self.children[1]._generate_dot_nodes()
            return nodes
        
    def _generate_dot_edges(self):
        """
        Generate the dot edge definitions for the digraph.
        @return String buffer in dot syntax
        """
        edges = ""
        if not self.is_leaf():
            edges += '{0}->{1}[label=0];' \
                .format(self.prefix or 'None', 
                    self.children[0].prefix)
            edges += '{0}->{1}[label=1];' \
                .format(self.prefix or 'None', 
                    self.children[1].prefix)
            edges += self.children[0]._generate_dot_edges()
            edges += self.children[1]._generate_dot_edges()
        return edges