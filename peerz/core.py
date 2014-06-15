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

from datetime import datetime
from functools import update_wrapper
import logging
import socket

import zmq.green as zmq
import gevent

from peerz.persistence import LocalStorage
from peerz.routing import Overlay, Node, NodeTable

DEFAULT_TIMEOUT = 20
LOG = logging.Logger(__name__)


class Connection(object):
    """
    Encapsulates a connection and interaction with the p2p network.
    """    
    
    def __init__(self, port, storage):
        """
        Creates a new (not yet connected) connection object.
        @param port: Listener/server port for this connection.
        @param storage: Filesystem path to root of storage.
        """
        self.shutdown = False
        self.listeners = []
        self.port = port
        self.storage = storage
        self.zctx = zmq.Context()
        self.server = None
        self.seeds = []
        self.conntrack = ConnectionTracker()
        self.localstore = LocalStorage(storage, port)
        self.overlay = Overlay(self)
        self._load_state()
        if not self.overlay.nodelist:
            self.overlay.nodelist = NodeTable()
        if not self.node:
            self.node = Node(self.overlay.generate_id(), 
                             socket.gethostbyname(socket.gethostname()), port)

    
    def add_listener(self, listener):
        self.listeners.append(listener)
        
    def remove_listener(self, listener):
        self.listeners.remove(listener)
        

    def fire_peer_joined(self, node):
        for x in self.listeners:
            x.peer_joined(node)
        
    def fire_peer_left(self, node):
        for x in self.listeners:
            x.peer_left(node)
            
    def fire_peer_updated(self, node):
        for x in self.listeners:
            x.peer_updated(node)
            
    def fire_peer_peerlist(self, peers):
        for x in self.listeners:
            x.peer_peerlist(peers)
            
    def get_local(self):
        return self.node
    
    def get_peers(self):
        return self.overlay.get_peers()
    
    def join(self, seeds):
        """
        Attempt to connect this node into the network.
        Handle case where we are the first node and do not yet have available peers.
        @param seeds: List of seeds 'addr:port' to attempt to bootstrap from.
        @raise ZMQError: Unable to bind to server socket.
        """
        self.server = Socket(self.zctx, zmq.REP)
        self.server.bind("tcp://*:{0}".format(self.port))  
        self.overlay.seeds = list(seeds)
        
        LOG.info("Joining network")
        # TODO parralelise with gevent pools and better zmq pattern 
        gevent.spawn(self._server_loop)
        gevent.spawn(self._ping_peers)
        gevent.spawn(self.overlay.manage_peers)
        
                    
    def _ping_peers(self):
        while not self.shutdown:
            for node in self.overlay.get_peers():
                sock = self.conntrack.get(node.node_id)
                try:
                    self._heartbeat(node, sock)
                    self._fetchpeers(node, sock) # not so freq?
                except (zmq.ZMQError, TimeoutError):
                    self.conntrack.remove(node.node_id)
                    self.fire_peer_left(node)
            self._dump_state()
            gevent.sleep(10)
            
    def leave(self):
        """
        Leave the network and tear-down any intermediate state.
        """
        self.shutdown = True
        for node in self.overlay.get_peers():
            sock = self.conntrack.get(node.node_id)
            try:
                sock.send_multipart(['LEAVE'])
                sock.recv_multipart()
                sock.close()
            except (zmq.ZMQError, TimeoutError):
                pass

    
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
    
    
    def _server_loop(self):
        self._dump_state()      
        while not self.shutdown:
            try:
                msg = self.server.recv_multipart()
                LOG.debug(msg)
                mtype = msg[0]
                if mtype == 'JOIN':
                    node = Node(msg[1], *msg[2].split(':'))
                    self.fire_peer_updated(node) # inbound not really peer?
                    if msg[1] != self.node.node_id:
                        self.server.send_multipart(['JOINOK', self.node.node_id, '{0}:{1}'.format(self.node.address, self.node.port)])
                    else:
                        self.server.send_multipart(['NOJOIN'])
                elif mtype == 'PING':
                    self.server.send_multipart(['PONG'])
                elif mtype == 'LEAVE':
                    self.server.send_multipart(['BYE'])
                elif mtype == 'PEERS':
                    nodes = [ "{0}:{1}:{2}".format(x.node_id, x.address, x.port) 
                             for x in self.overlay.get_known_nodes() ]
                    self.server.send_multipart(['PEERS'] + nodes)
            except TimeoutError:
                pass
            
    def _join(self, endpoint):
        try:
            sock = Socket(self.zctx, zmq.REQ)
            sock.connect("tcp://{0}".format(endpoint))
            sock.send_multipart(['JOIN', self.node.node_id, '{0}:{1}'.format(self.node.address, self.node.port)])
            msg = sock.recv_multipart()
            mtype = msg[0]
            if mtype == 'JOINOK':
                node = Node(msg[1], msg[2].split(':')[0], msg[2].split(':')[1])
                self.conntrack.add(node.node_id, sock)
                self.fire_peer_joined(node)
            elif mtype == 'NOJOIN':
                sock.close()
                # update ?
        except (zmq.ZMQError, TimeoutError):
            pass

    def _heartbeat(self, node, sock):
        start = datetime.utcnow()
        sock.send_multipart(['PING'])       
        sock.recv_multipart()
        latency = (datetime.utcnow() - start).total_seconds() * 1000
        # average with previous value to help smooth
        if node.latency_ms:
            node.latency_ms = (node.latency_ms + latency) /2
        else:
            node.latency_ms = latency
        LOG.debug("Heartbeat recvd from {0} in {1} ms".format(node.node_id, node.latency_ms))
        self.fire_peer_updated(node)
        
    def _fetchpeers(self, node, sock):
        sock.send_multipart(['PEERS'])
        msg = sock.recv_multipart()
        # TODO constrain size of list
        self.fire_peer_peerlist([ Node(*x.split(":")) for x in msg[1:] ])
            
    def _dump_state(self):
        self.localstore.store('node', self.node)
        self.localstore.store('overlay.nodelist', self.overlay.nodelist)
    
    def _load_state(self):
        self.node = self.localstore.fetch('node')
        self.overlay.nodelist = self.localstore.fetch('overlay.nodelist')
     
# http://lucumr.pocoo.org/2012/6/26/disconnects-are-good-for-you/
class Socket(zmq.Socket):

    def on_timeout(self):
        raise TimeoutError

    def _timeout_wrapper(f):
        def wrapper(self, *args, **kwargs):
            timeout = kwargs.pop('timeout', DEFAULT_TIMEOUT)
            if timeout is not None:
                timeout = int(timeout * 1000)
                poller = zmq.Poller()
                poller.register(self)
                if not poller.poll(timeout):
                    return self.on_timeout()
            return f(self, *args, **kwargs)
        return update_wrapper(wrapper, f, ('__name__', '__doc__'))

    for _meth in dir(zmq.Socket):
        if _meth.startswith(('send', 'recv')):
            locals()[_meth] = _timeout_wrapper(getattr(zmq.Socket, _meth))

    del _meth, _timeout_wrapper

class ConnectionTracker(object):
    def __init__(self):
        # node_id -> open socket
        self.sockets = {} 

    def add(self, node_id, socket):
        tmp = self.sockets.get(node_id)
        if tmp != socket:
            self._close(tmp)
        self.sockets[node_id] = socket
        
    def remove(self, node_id):
        try:
            self._close(self.sockets.get(node_id))
            del self.sockets[node_id]
        except (KeyError, AttributeError):
            pass
    
    def get(self, node_id):
        return self.sockets.get(node_id)
    
    def _close(self, socket):
        try:
            if socket:
                socket.close()
        except zmq.ZMQError:
            pass
        
class TimeoutError(Exception):
    pass
