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

import json
import logging

import zmq

from peerz import engine, utils

LOG = logging.Logger(__name__)

class Network(object):
    """
    Client API for creating an interacting with
    a nodes connection into the p2p network.
    """

    def __init__(self, seeds, storage):
        """
        Creates a new (not yet connected) network object.

        :param seeds: Default list of seeds (in host:port:id format) to connect
        :param storage: Filesystem path to root of local storage.
        """
        ctx = zmq.Context()
        self.engine = utils.Actor(ctx, engine.Engine, seeds, storage)

    def get_local(self):
        """
        :return: the node object that represents the current/local node.
        """
        self.engine.send_unicode("NODE")
        return json.loads(self.engine.recv())

    def get_peers(self):
        """
        :return: List of all known active nodes.
        """
        self.engine.send_unicode("PEERS")
        return json.loads(self.engine.recv())

    def reset(self, node_id='', secret_key=''):
        """
        Remove any existing state and reset as a new node.
        
        :param node_id: New curve public key to reset to
        :param secret_key: New curve private key to reset to
        """
        self.engine.send_unicode("RESET", zmq.SNDMORE)
        self.engine.send_unicode(node_id, zmq.SNDMORE)
        self.engine.send_unicode(secret_key)
        return json.loads(self.engine.recv())

    def join(self, node_id='', secret_key=''):
        """
        Attempt to connect this node into the network and initiate node 
        discovery/maintenance.

        :param node_id: New curve public key to reset to
        :param secret_key: New curve private key to reset to
        """
        self.engine.send_unicode("START", zmq.SNDMORE)
        self.engine.send_unicode(node_id, zmq.SNDMORE)
        self.engine.send_unicode(secret_key)
        return json.loads(self.engine.recv())

    def leave(self):
        """
        Leave the network and tear-down any non-persistent state.
        """
        LOG.info("Leaving network")
        self.engine.send_unicode("STOP")
        self.engine.resolve().wait()

    def publish(self, key, content, context='default'):
        """
        Publish the object content into the network.

        :param key: Identifier to lookup the object
        :param content: Object to be published (max 256KB).
        :param context: (Unused) String label for which namespace
                        to publish the object.
        :return: Target Id of primary storage location.
        """
        self.engine.send_unicode("STOR", zmq.SNDMORE)
        self.engine.send_unicode(key, zmq.SNDMORE)
        self.engine.send_unicode(content, zmq.SNDMORE)
        self.engine.send_unicode(context)
        return json.loads(self.engine.recv())

    def unpublish(self, key, context='default'):
        """
        Stop publishing the given object into the network.
        The object will eventually age out of active nodes.

        :param key: Identifier of object to stop publishing.
        :param context: (unused) Namespace to remove object from.
        """
        self.engine.send_unicode("REMV", zmq.SNDMORE)
        self.engine.send_unicode(key, zmq.SNDMORE)
        self.engine.send_unicode(context)
        self.engine.resolve().wait()
    
    def get_published(self):
        """
        Get a list of objects published by the current node.
        
        :return: Dict of key = value published objects
        """
        self.engine.send_unicode("PUBL")
        return json.loads(self.engine.recv())

    def get_hashtable(self):
        """
        Get a list of all known published pbjects
        
        :return: Dict of key = value publihsed objects
        """
        self.engine.send_unicode("HASH")
        return json.loads(self.engine.recv())
                
    def fetch(self, key, context='default'):
        """
        Retrieve the given object from the network.
    
        :param key: Identifier of object to remove.
        :param context: (Unused) Namespace for object to be retrieved from.
        :return Content of requested object, None if not available.
        """
        self.engine.send_unicode("FVAL", zmq.SNDMORE)
        self.engine.send_unicode(key, zmq.SNDMORE)
        self.engine.send_unicode(context)
        return json.loads(self.engine.recv())

    def find_nodes(self, target_id, max_nodes=1):
        """
        Recursively find the nodes closest to the specified target_id.

        :param target_id: Id as bytes to find closest node/s to
        :param max_nodes: At most max_nodes count of nodes returned.
        :return: List of nodes.
        """
        self.engine.send_unicode("FNOD", zmq.SNDMORE)
        self.engine.send_unicode(target_id)
        return json.loads(self.engine.recv())
