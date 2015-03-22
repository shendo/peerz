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

from collections import OrderedDict
from functools import update_wrapper, wraps
import time

import zmq.green as zmq
from zmq.auth import CURVE_ALLOW_ANY
from zmq.auth.thread import ThreadAuthenticator
from zmq.utils import z85

from version import __protocol__ as PROTOCOL_VERSION

PROTOCOL_NAME = 'PEERZ'
DEFAULT_TIMEOUT = 10

def headers(node, msgtype):
    """
    Generate the message headers for sending on the wire.
    Any message body elements can be appended to the returned
    object before sending.
    @param node: Node details in 'addr:port:id' format.
    @param msgtype: String name of message type
    @return: List that can be sent as a multipart message.
    """
    return [PROTOCOL_NAME, PROTOCOL_VERSION, node, msgtype]

def splitmsg(msg):
    """
    Given a multipart message, split out the application important
    fields and return them.
    @param msg: Multipart message list
    @return: Tuple of peer_addr, peer_port, peer_id, msgtype, extra
    where extra is a list of msg body fields (or an empty list if none)
    @raise InvalidMessage: Any message parsing issues
    """
    if len(msg) < 4:
        raise InvalidMessage("Insufficient message parts")
    protocol, version, peer, msgtype = msg[:4]
    extra = msg[4:]
    peer_addr, peer_port, peer_id = unpack_node(peer)

    if protocol != PROTOCOL_NAME:
        raise InvalidMessage("Mismatch protocol name: {0} Expected: {1}" \
                             .format(protocol, PROTOCOL_NAME))
    return peer_addr, peer_port, peer_id, msgtype, extra

def pack_node(addr, port, node_id):
    """
    Given a tuple of basic node details, return in packed string format.
    @param addr: IP address of node
    @param port: Port number of node
    @param id: Public key (z85 encoded) of node
    @return: String representation of node.
    """
    return '{0}:{1}:{2}'.format(addr, port, node_id)

def unpack_node(node_str):
    """
    Given a string representation of a node, return a tuple of its details.
    @param node_str: Node details in 'addr:port:id' format.
    @return: Tuple of address, port, id
    """
    x = node_str.split(':', 2)
    return (x[0], x[1], x[2])

def timer(f):
    """
    Time the given function and return the duration
    in milliseconds.
    If the wrapped function does not return a value itself, 
    the duration is the return value, otherwise the function
    returns a tuple of (original value, duration).
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        start = time.time()
        ret = f(*args, **kwargs)
        duration = (time.time() - start) * 1000
        # need to be sure we don't wrap any function
        # that can return None as a valid value
        if ret is None:
            return duration
        return ret, duration
    return wrapper

def zmqerror_adapter(f):
    """
    Reraise any thrown ZMQErrors as our ConnectionError
    type instead.
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except zmq.ZMQError, ex:
            raise ConnectionError(str(ex))
    return wrapper

class ConnectionPool(object):
    """
    Simple LRU connection pool for client connections.
    """
    def __init__(self, node, ctx=None, maxsize=20):
        """
        Initialise the pool with the supplied details.
        @param node: This node (the client)
        @param ctx: ZMQ context, will be auto created if None
        @param maxsize: Maximum number of open connections
        """
        self.pool = OrderedDict()
        self.maxsize = maxsize
        if not ctx:
            self.ctx = zmq.Context()
        else:
            self.ctx = ctx
        self.node = node

    def fetch(self, peer):
        """
        Fetch a connection for the given peer.
        @param peer: Peer (Node obj) to get a connection for
        @return: A Connection to the specified peer
        @raise ConnectionError unable to obtain the connection
        """
        if peer.node_id in self.pool:
            conn = self.pool.pop(peer.node_id)
        else:
            conn = Connection(self.node, peer, self.ctx)
        self.pool[peer.node_id] = conn
        if len(self.pool) > self.maxsize:
            _, old = self.pool.popitem(last=False)
            old.close()
        return conn

class Connection(object):
    """
    Represents a client connection to a given peer.
    """
    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()

    @zmqerror_adapter
    def __init__(self, node, peer, ctx=None):
        """
        Create a new connection to the specified peer.
        @param node: Node obj of this node
        @param peer: Peer (Node obj) to create connection to
        @param ctx: ZMQ context or autocreate if None
        """
        self.node = node
        self.peer = peer
        self.node_header = pack_node(node.address, node.port, node.node_id)
        if not ctx:
            self.ctx = zmq.Context()
            self.ctx_managed = True
        else:
            self.ctx = ctx
            self.ctx_managed = False
        self.socket = Socket(self.ctx, zmq.REQ)
        self.socket.curve_publickey = z85.decode(self.node.node_id)
        self.socket.curve_secretkey = z85.decode(self.node.secret_key)
        self.socket.curve_serverkey = z85.decode(self.peer.node_id)
        self.socket.connect("tcp://{0}:{1}".format(peer.address, peer.port))

    @timer
    @zmqerror_adapter
    def ping(self):
        """
        Ping the peer.
        @return: Round trip time in ms
        """
        msg = headers(self.node_header, 'PING')
        self.socket.send_multipart(msg)
        resp = self.socket.recv_multipart()
        splitmsg(resp)

    @timer
    @zmqerror_adapter
    def find_nodes(self, target_id):
        """
        Request the peers list of closest nodes to the given target.
        @param target_id: Node id (z85 encoded) to use for distance
        @return: Tuple of list of nodes (addr, port, id) and 
        round trip time in ms
        """
        msg = headers(self.node_header, 'FNOD')
        msg.append(target_id)
        self.socket.send_multipart(msg)
        resp = self.socket.recv_multipart()
        peer_addr, peer_port, peer_id, msgtype, extra = splitmsg(resp)
        nodes = [ unpack_node(x) for x in extra ]
        return nodes

    @zmqerror_adapter
    def close(self):
        """
        Teardown this connection
        """
        self.socket.close()
        if self.ctx_managed:
            self.ctx.term()

class Server(object):
    """
    The server socket for the given node.
    """
    # static auth instance as only want a singleton
    # while there should only be a single Server per process
    # this is not necessarily true in unit tests/examples, etc.
    auth = None
    @zmqerror_adapter
    def __init__(self, addr, port, node_id, listener, secret_key):
        """
        Creates a new server (bound but not dispatching messages)
        @param addr: IP address of this node
        @param port: Port number to listen on
        @param node_id: Public key (z85 encoded) of this node
        @param listener: Callback object to receive handle_* function calls
        """
        self.ctx = zmq.Context()
        self.sock = Socket(self.ctx, zmq.REP)
        # enable curve encryption/auth
        if not Server.auth:
            Server.auth = ThreadAuthenticator()
            Server.auth.start()
            Server.auth.configure_curve(domain='*', location=CURVE_ALLOW_ANY)
        self.sock.curve_server = True
        self.sock.curve_secretkey = z85.decode(secret_key)
        self.sock.curve_publickey = z85.decode(node_id)
        self.sock.bind("tcp://*:{0}".format(port))
        self.node = pack_node(addr, port, node_id)
        self.shutdown = False
        self.listener = listener

    def dispatch(self):
        """
        Start the message dispatch loop (blocking).
        """
        while not self.shutdown:
            try:
                msg = self.sock.recv_multipart()
                self._handle_msg(msg)
            except TimeoutError:
                pass
        self.sock.close()
        self.ctx.term()

    def _handle_msg(self, msg):
        resp = headers(self.node, 'FAIL')
        try:
            peer_addr, peer_port, peer_id, msgtype, extra = splitmsg(msg)
            # notify of peer actvity
            self.listener.handle_node_seen(peer_addr, peer_port, peer_id)
            if msgtype == 'PING':
                resp = headers(self.node, 'PONG')
            elif msgtype == 'FNOD':
                target_id = extra[0]
                nodes = self.listener.handle_find_nodes(peer_id, target_id)
                resp = headers(self.node, 'FNOD')
                resp += [ pack_node(x[0], x[1], x[2]) for x in nodes ]
            else:
                raise InvalidMessage('Unknown or unsupported message type: {0}' \
                                     .format(msgtype))
        except InvalidMessage, ex:
            # can we drop a client conn with REQ/REP?
            resp.append(str(ex))
        self.sock.send_multipart(resp)

    def close(self):
        self.shutdown = True
        if Server.auth:
            Server.auth.stop()

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

class ConnectionError(Exception):
    """
    Some error occurred with the network connection/transport.
    """
    pass

class TimeoutError(ConnectionError):
    """
    A timeout occured on the given connection.
    """
    pass

class InvalidMessage(Exception):
    """
    An error occurred while trying to parse a network message.
    """
    pass
