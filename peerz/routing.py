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

def time_in_future(seconds):
    return (datetime.utcnow() - EPOCH + timedelta(seconds=120)).total_seconds()

class Overlay(object):
    
    def __init__(self, connection):
        self.max_peers = 10
        self.connection = connection
        self.nodelist = NodeTable()
        self.peers = {}
        self.connection.add_listener(self)
        self.seeds = []
        
    def generate_id(self):
        return str(uuid.uuid4())
    
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
        return {'node_id': self.node_id,
                'address': self.address,
                'port': self.port,
                'hostname': self.hostname,
                'first_connected': self.first_connected,
                'last_connected': self.last_connected,
                'last_activity': self.last_activity,
                'latency_ms': self.latency_ms
                }
        
    def __str__(self):
        return '{0} - {1}:{2} ({3})'.format(self.node_id, self.address, self.port, self.hostname)


class NodeTable(object):    
    
    # TODO fix up this crap
    def __init__(self):
        self.nodes = OrderedDict()
    
    def get_by_id(self, node_id):
        # O(N) :(
        for x in self.nodes.values():
            if x.node_id == node_id:
                return x
        return None
    
    def get_by_addr(self, addr, port):
        return self.nodes.get((addr, port))
    
    def get_all(self):
        return self.nodes.values()
    
    def update(self, node):
        """Return a node guaranteed to be in the table
        with the values contained in node.
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