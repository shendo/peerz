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

import zmq
from zmq.utils import z85

from peerz import engine, utils
from peerz.routing import Node

LOG = logging.Logger(__name__)

class Network(object):
    """
    Encapsulates the interaction and state storage for a
    a nodes connection into the p2p network.
    """

    def __init__(self, seeds):
        """
        Creates a new (not yet connected) network object.
        @param port: Listener/server port for this node.
        @param storage: Filesystem path to root of local storage.
        """
        ctx = zmq.Context()
        self.engine = utils.Actor(ctx, engine.Engine, seeds)

    def get_local(self):
        """
        @return: the node object that represents the current/local node.
        """
        self.engine.send_unicode("NODE")
        return self.unpack_node(*self.engine.recv_multipart())

    def get_peers(self):
        """
        @return: List of all known active peer nodes.
        """
        self.engine.send_unicode("PEERS")
        i = iter(self.engine.recv_multipart())
        return [ self.unpack_node(*x) for x in zip(i, i, i)]

    def reset(self, secret_key=''):
        """
        Remove any existing state and reset as a new node.
        """
        self.engine.send_unicode("RESET", zmq.SNDMORE)
        self.engine.send_unicode(secret_key)
        return self.unpack_node(*self.engine.recv_multipart())

    def join(self, secret_key=''):
        """
        Attempt to connect this node into the network and initiate node 
        discovery/maintenance.
        @param seeds: List of seeds 'addr:port' to attempt to bootstrap from.
        @raise ConnectionError: Unable to bind to server socket.
        """
        self.engine.send_unicode("START", zmq.SNDMORE)
        self.engine.send_unicode(secret_key)
        return self.unpack_node(*self.engine.recv_multipart())

    def leave(self):
        """
        Leave the network and tear-down any intermediate state.
        """
        LOG.info("Leaving network")
        self.engine.send_unicode("STOP")
        self.engine.resolve().wait()

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

    def unpack_node(self, address, port, node_id):
        return Node(address, int(port), z85.decode(node_id))

    def find_nodes(self, target_id, max_nodes=1):
        """
        Recursively find the nodes closest to the specified target_id.
        @param target_id: Id as long to find closest node/s to
        @param max_nodes: At most max_nodes count of nodes returned.
        @return List of nodes.
        """
        self.engine.send_unicode("FNOD", zmq.SNDMORE)
        self.engine.send_unicode(target_id)
        i = iter(self.engine.recv_multipart())
        return [ unpack_node(*x) for x in zip(i, i, i)]
