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

import logging
import socket

import gevent
from gevent.pool import Pool

from peerz.persistence import LocalStorage
from peerz.transport import Connection, ConnectionPool, ConnectionError, Server
from peerz.routing import distance, generate_id, Node, RoutingZone

LOG = logging.Logger(__name__)
# simultaneous requests
A = 3

class Network(object):
    """
    Encapsulates the interaction and state storage for a
    a nodes connection into the p2p network.
    """ 
    
    def __init__(self, port, storage, parallel_requests=A):
        """
        Creates a new (not yet connected) network object.
        @param port: Listener/server port for this node.
        @param storage: Filesystem path to root of local storage.
        """
        self.shutdown = False
        self.port = port
        # eventually cater for external IPs / UPNP / port forwarding
        self.addr = socket.gethostbyname(socket.gethostname())
        self.req_pool = Pool(parallel_requests)
        self.storage = storage
        self.server = None
        # Tree of known nodes
        self.nodetree = None
        # Node representing ourself
        self.node = None
        # persistence between runs
        self.localstore = LocalStorage(storage, port)
        self._load_state()
        # clean start
        if not self.node:
            self.reset()
        self.connection_pool = ConnectionPool(self.node.node_id, 
                                              self.node.address,
                                              self.node.port)

    def get_local(self):
        """
        @return: the node object that represents the current/local node.
        """
        return self.node
    
    def get_peers(self):
        """
        @return: List of all known active peer nodes.
        """
        nodes = self.nodetree.get_all_nodes()
        nodes.remove(self.node)
        return nodes
    
    def reset(self):
        """
        Remove any existing state and reset as a new node.
        """
        self.node = Node(generate_id(), 
                         self.addr,
                         self.port)        
        self.nodetree = RoutingZone(self.node.node_id)
        self._dump_state()
    
    def join(self, seeds):
        """
        Attempt to connect this node into the network.
        Handle case where we are the first node and do not yet have available peers.
        @param seeds: List of seeds 'addr:port' to attempt to bootstrap from.
        @raise ConnectionError: Unable to bind to server socket.
        """
        
        LOG.info("Joining network")
        self.server = Server(self.node.node_id, self.node.address,
                             self.node.port, self)
        gevent.spawn(self.server.dispatch)
        self._bootstrap(seeds)
        gevent.spawn(self._peer_maintenance)
            
    def leave(self):
        """
        Leave the network and tear-down any intermediate state.
        """
        LOG.info("Leaving network")
        self.shutdown = True
        self.server.close()
    
    def publish(self, content, context='default', redundancy=1, ttl=0):
        """
        Publish the object content into the network.
        @param content: Object to be published.
        @param context: String label for which namespace to publish the object.
        @param redundancy: Desired minimum copies of object (not guaranteed).
        @param ttl: Time-to-live in seconds for the object.
        @return: Object ID to use to retrieve object in future.
        """
        return None
    
    def unpublish(self, object_id, context='default'):
        """
        Best effort to remove the defined object from the network.
        @param object_id: Identifier of object to remove.
        @param context: Namespace to remove object from.
        """
        pass
    
    def fetch(self, object_id, context='default'):
        """
        Retrieve the given object from the network.
        @param object_id: Identifier of object to remove.
        @param context: Namespace for object to be retrieved from.
        @return Content of requested object, None if not available.
        """
        return None
    
    def route_to_node(self, node_id, message):
        """
        Route the given message to the specified node.
        @param node_id: Identifier of node.
        @param message: Python object to send.
        """
        pass
    
    def route_to_object(self, object_id, message, context='default'):
        """
        Route the given message to the closest node hosting the identified object.
        @param object_id: Identifier of object.
        @param context: Namespace context for supplied object.
        """
        pass
    
    def find_nodes(self, target_id):
        nodes = self.nodetree.closest_to(target_id)
        self.req_pool.map(self._find_nodes_request, 
                          zip(nodes, [ target_id for i in range(len(nodes))] ))
    
    def _find_nodes_request(self, arg):
        peer, target_id = arg
        try:
            conn = self.connection_pool.fetch(peer)
            nodes, rtt = conn.find_nodes(target_id)
            peer.update(rtt)
            for peer_id, peer_addr, peer_port in nodes:
                node = self.nodetree.get_node_by_id(peer_id)
                if not node:
                    node = Node(peer_id, peer_addr, peer_port)
                    self.nodetree.add(node)
                    closest =  self.nodetree.closest_to(target_id, 1)[0]
                    if distance(peer_id, target_id) < \
                        distance(closest.node_id, target_id):
                        self._find_nodes_request(node, target_id)
        except ConnectionError:
            peer.failures += 1
    
    def _peer_maintenance(self):
        while not self.shutdown:
            # keep neighbourhood fresh
            self.find_nodes(self.node.node_id)
            # randomly refresh other routing zones
            # could be smarter about selection of ranges here...
            self.find_nodes(generate_id())
            gevent.sleep(60)
            
    def handle_node_seen(self, peer_id, peer_addr, peer_port):
        """
        Callback when activity from a peer is seen.
        @param peer_id: Id (long) of peer performing activity
        @param peer_addr: Endpoint IP address of peer
        @param peer_port: Endpoint Port number of peer
        """
        node = self.nodetree.get_node_by_id(peer_id)
        if not node:
            node = Node(peer_id, peer_addr, peer_port)
            self.nodetree.add(node)

    def handle_find_nodes(self, peer_id, target):
        """
        Callback for a find_nodes query from a peer.
        @param peer_id: Id (long) of peer performing request
        @param peer_addr: Endpoint IP address of peer
        @param peer_port: Endpoint Port number of peer
        @return: List of node tuples (id, addr, port) to return to peer.
        """
        LOG.debug("Finding nodes for peer: {0} target: {1}" \
                  .format(peer_id, target))
        return [ (x.node_id, x.address, x.port) 
                for x in self.nodetree.closest_to(target) ]
        
    def _bootstrap(self, seeds):
        """
        Given a list of seeds attempt to make a connection into the
        p2p network and discover neighbours.
        @param seeds: List of "ip:port" peers to attempt ot bootstrap from
        """
        if not seeds:
            raise ValueError('Seeds list must not be empty and must contain '
                             'endpoints in "address:port" format.')
        untried = list(seeds)
        self.nodetree.add(self.node)
        while len(self.nodetree.get_all_nodes()) < 2: # ignore self
            if not untried:
                gevent.sleep(5)
                untried = list(seeds)
            self._bootstrap_from_peer(untried.pop())
        self.find_nodes(self.node.node_id)
                
    def _bootstrap_from_peer(self, endpoint):
        """
        Bootstrap from the given peer endpoint details.
        @param endpoint: Endpoint string "id:port" of peer to attempt
        """
        addr, port = endpoint.split(':')
        try:
            LOG.debug("Attempting to bootstrap from {0}".format(endpoint))
            with Connection(self.node.node_id, self.node.address, 
                            self.node.port, Node(None, addr, port)) as conn:
                nodes, rtt = conn.find_nodes(self.node.node_id)
                LOG.debug("Discovered {0} nodes from seed {1}".format(
                            len(nodes), endpoint))
                for x in nodes:
                    node = Node(*x)
                    # only add newly discovered
                    if not self.nodetree.get_node_by_id(node.node_id):
                        self.nodetree.add(node)
                seed = self.nodetree.get_node_by_addr(addr, port)
                if seed:
                    seed.update(rtt)
        except ConnectionError, ex:
            LOG.debug("Failed to bootstrap from {0} because of {1}".format(
                            endpoint, str(ex)))


    def _dump_state(self):
        """
        Dump local state for persistence between runs.
        """
        self.localstore.store('node', self.node)
        self.localstore.store('nodetree', self.nodetree)
    
    def _load_state(self):
        """
        Reload any persisted state.
        """
        self.node = self.localstore.fetch('node')
        self.nodetree = self.localstore.fetch('nodetree')
     
