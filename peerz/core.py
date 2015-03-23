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

import logging
import socket

import gevent
from gevent.pool import Pool

from peerz.persistence import LocalStorage
from peerz.transport import Connection, ConnectionPool, ConnectionError, Server
from peerz.routing import distance, generate_random
from peerz.routing import Node, RoutingZone, K

import zmq

LOG = logging.Logger(__name__)
# simultaneous requests
A = 3
# number of comms fails to node before evicting from node tree
MAX_NODE_FAILS = 2
# automatic find_nodes request poll cycles in seconds
NEIGHBOURHOOD_POLL = 120
STALE_POLL = 600

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
        self.connection_pool = ConnectionPool(self.node)

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
        keys = zmq.curve_keypair()
        self.node = Node(self.addr,
                         self.port,
                         keys[0],
                         keys[1])
        self.nodetree = RoutingZone(self.node.node_id)
        self._dump_state()

    def join(self, seeds):
        """
        Attempt to connect this node into the network and initiate node 
        discovery/maintenance.
        @param seeds: List of seeds 'addr:port' to attempt to bootstrap from.
        @raise ConnectionError: Unable to bind to server socket.
        """

        LOG.info("Joining network with node Id: {0}" \
                 .format(self.node.node_id))
        self.server = Server(self.node.address,
                             self.node.port,
                             self.node.node_id,
                             self,
                             self.node.secret_key)
        gevent.spawn(self.server.dispatch)
        self._bootstrap(seeds)
        gevent.spawn(self._refresh_neighbours)
        gevent.spawn(self._refresh_random)

    def leave(self):
        """
        Leave the network and tear-down any intermediate state.
        """
        LOG.info("Leaving network")
        self.shutdown = True
        self.server.close()

    def publish(self, key, content, context='default', redundancy=1, ttl=0):
        """
        Publish the object content into the network.
        @param key: Identifier to lookup the object
        @param content: Object to be published.
        @param context: String label for which namespace to publish the object.
        @param redundancy: Desired minimum copies of object (not guaranteed).
        @param ttl: Time-to-live in seconds for the object.
        @return: Target Id of primary storage location.
        """
        return None

    def unpublish(self, key, context='default'):
        """
        Best effort to remove the defined object from the network.
        @param key: Identifier of object to remove.
        @param context: Namespace to remove object from.
        """

    def fetch(self, key, context='default'):
        """
        Retrieve the given object from the network.
        @param key: Identifier of object to remove.
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

    def find_nodes(self, target_id, max_nodes=1):
        """
        Recursively find the nodes closest to the specified target_id.
        @param target_id: Id as long to find closest node/s to
        @param max_nodes: At most max_nodes count of nodes returned.
        @return List of nodes.
        """
        closest = self.nodetree.closest_to(target_id)
        unqueried = list(closest)
        while unqueried:
            responses = self.req_pool.map(self._find_nodes_request,
                          zip(unqueried, [target_id] * len(unqueried)))
            # flatten lists
            newnodes = [ x for sub in responses for x in sub ]
            # new nodes that are closer, are to be queried further
            unqueried = [ x for x in newnodes
                         if distance(x.node_id, target_id) < \
                            distance(closest[0].node_id, target_id) ]
            # append new nodes and resort
            closest.extend(newnodes)
            closest = sorted(closest, key=lambda x: distance(x.node_id, target_id))
            closest[:K]
        return closest[:max_nodes]

    def _find_nodes_request(self, arg):
        peer, target_id = arg
        nodes = []
        try:
            conn = self.connection_pool.fetch(peer)
            resp, rtt = conn.find_nodes(target_id)
            peer.update(rtt)
            for peer_addr, peer_port, peer_id in resp:
                node = self.nodetree.get_node_by_id(peer_id)
                if not node:
                    node = Node(peer_addr, peer_port, peer_id)
                    self.nodetree.add(node)
                    nodes.append(node)
        except ConnectionError:
            peer.failures += 1
            if peer.failures >= MAX_NODE_FAILS:
                self.nodetree.remove(peer)
        return nodes

    def _refresh_neighbours(self):
        while not self.shutdown:
            gevent.sleep(NEIGHBOURHOOD_POLL)
            # keep neighbourhood fresh
            LOG.info("Refreshing nearest neighbours")
            self.find_nodes(self.node.node_id)
            self._dump_state()

    def _refresh_random(self):
        while not self.shutdown:
            gevent.sleep(STALE_POLL)
            # randomly refresh other routing zones
            # could be smarter about selection of ranges here...
            random_id = generate_random()
            LOG.info("Refreshing random zone, centered around: {0}" \
                     .format(random_id))
            self.find_nodes(random_id)

    def handle_node_seen(self, peer_addr, peer_port, peer_id):
        """
        Callback when activity from a peer is seen.
        @param peer_addr: Endpoint IP address of peer
        @param peer_port: Endpoint Port number of peer
        @param peer_id: Public key (z85 encoded) of peer
        """
        node = self.nodetree.get_node_by_id(peer_id)
        if not node:
            node = Node(peer_addr, peer_port, peer_id)
            self.nodetree.add(node)

    def handle_find_nodes(self, peer_id, target):
        """
        Callback for a find_nodes query from a peer.
        @param peer_id: Id (z85 encoded) of peer performing request
        @param target: Key (z85 encoded) of target to locate
        @return: List of node tuples (addr, port, id) to return to peer.
        """
        LOG.debug("Finding nodes for peer: {0} target: {1}" \
                  .format(peer_id, target))
        return [ (x.address, x.port, x.node_id)
                for x in self.nodetree.closest_to(target) ]

    def _is_only_seed(self, seeds):
        """
        @return True if this node is the only known seed otherwise False.
        """
        # don't care about endpoint (could be external), just compare key
        return len(seeds) == 1 and \
            seeds[0].split(':', 2)[2] == self.node.node_id

    def _bootstrap(self, seeds):
        """
        Given a list of seeds attempt to make a connection into the
        p2p network and discover neighbours.
        @param seeds: List of "address:port:key" peers to attempt to bootstrap
        """
        if not seeds:
            raise ValueError('Seeds list must not be empty and must contain '
                             'endpoints in "address:port:key" format.')
        untried = list(seeds)
        self.nodetree.add(self.node)
        # don't bother connecting if only self
        if not self._is_only_seed(seeds):
            while len(self.nodetree.get_all_nodes()) < 2:  # ignore self
                if not untried:
                    gevent.sleep(5)
                    untried = list(seeds)
                self._bootstrap_from_peer(untried.pop())
        self.find_nodes(self.node.node_id)

    def _bootstrap_from_peer(self, endpoint):
        """
        Bootstrap from the given peer endpoint details.
        @param endpoint: Endpoint string "address:port:key" of peer to attempt
        """
        addr, port, key = endpoint.split(':', 2)
        try:
            LOG.debug("Attempting to bootstrap from {0}".format(endpoint))
            with Connection(self.node, Node(addr, port, key)) as conn:
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
        LOG.info("Storing latest network state")
        self.localstore.store('node', self.node)
        self.localstore.store('nodetree', self.nodetree)

    def _load_state(self):
        """
        Reload any persisted state.
        """
        self.node = self.localstore.fetch('node')
        self.nodetree = self.localstore.fetch('nodetree')
