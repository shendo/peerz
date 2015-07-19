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
import logging
import math
import struct
import time

import zmq.green as zmq
from zmq.auth import CURVE_ALLOW_ANY
from zmq.auth.thread import ThreadAuthenticator
from zmq.utils import z85

import version

PROTOCOL_NAME = 'PEERZ'
DEFAULT_TIMEOUT = 10
LOGGER = logging.getLogger(__name__)

OPTION_PLAIN = 0x01
OPTION_CURVE = 0x02

class InvalidPacket(Exception):
    pass

class Packet(object):

    def pack(self, payload,
             node_id,
             mode=OPTION_PLAIN,
             major=version.__protocol_major__,
             minor=version.__protocol_minor__):
        self.payload = payload
        self.mode = mode
        self.major = major
        self.minor = minor
        self.node_id = node_id
        self.length = len(payload)

    def unpack(self, msg):
        if msg < 36:
            raise InvalidPacket("Packet too small")
#         prot, self.major, self.minor, self.node_id, self.mode, self.length = struct.unpack('!3sBB32sBI', msg[:42])
#         self.payload = msg[42:]
        self.node_id, self.mode = struct.unpack('!32sB', msg[:33])
        self.payload = msg[33:]

#         if prot != PROTOCOL_MAGIC:
#             raise InvalidPacket("Expected protocol %s got %s" % (PROTOCOL_MAGIC, prot))
#         if self.major > version.__protocol_major__ or \
#            self.major < version.__protocol_major__:
#             raise InvalidPacket("Incompatible protocol version. Expected major: %i got %i" %
#                                 (version.__protocol_major__, self.major))
#         if len(self.payload) != self.length:
#             raise InvalidPacket("Header length %i does not match payload size %i" %
#                                 (self.length, len(self.payload)))

    @property
    def msg(self):
#         return struct.pack('!3sBB32sBI', PROTOCOL_MAGIC, self.major, self.minor, self.node_id, self.mode, self.length) + self.payload
        return struct.pack('!32sB', self.node_id, self.mode) + self.payload

class Payload(object):
    MAX_FRAGMENT = 1100

    def __init__(self):
        self.fragments = []

    def pack(self, txid, msgtype, content=b''):
        self.lastfrag = math.floor(len(content) / Payload.MAX_FRAGMENT)
        fragment = 0
        while len(content) > Payload.MAX_FRAGMENT:
            part = content[:Payload.MAX_FRAGMENT]
            content = content[Payload.MAX_FRAGMENT:]
            self.fragments.append(struct.pack('!4sBBBH', txid, msgtype, fragment, self.lastfrag, len(part)) + part)
            fragment += 1
        self.fragments.append(struct.pack('!4sBBBH', txid, msgtype, fragment, self.lastfrag, len(content)) + content)

    def unpack(self, payload):
        self.txid, self.msgtype, self.fragment, self.lastfrag, self.content_length = struct.unpack('!4sBBBH', payload[:9])
        self.content = payload[9:self.content_length + 9]  # cater for padding

class DefragMap(object):
    def __init__(self):
        self.map = {}

    # TODO add expiry

    def get_msg(self, txid, fragid, maxfrag, fragment):
        # not fragmented
        if fragid == maxfrag == 0:
            return fragment
        # list of all frags
        frags = self.map.set_default(txid, [b''] * (maxfrag + 1))
        frags[fragid] = fragment
        if all(frags):
            del self.map[txid]
            return b''.join(frags)
        return None

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
    def __init__(self, node, peer, ctx=None, secure=True):
        """
        Create a new connection to the specified peer.
        @param node: Node obj of this node
        @param peer: Peer (Node obj) to create connection to
        @param ctx: ZMQ context or autocreate if None
        """
        self.node = node
        self.peer = peer
        # abbreviated peer id for logging/debugging
        self.peer_label = peer.node_id[:6] + '..'
        self.node_header = pack_node(node.address, node.port, node.node_id)
        if not ctx:
            self.ctx = zmq.Context()
            self.ctx_managed = True
        else:
            self.ctx = ctx
            self.ctx_managed = False

        self.socket = Socket(self.ctx, zmq.DEALER)
        self.socket.identity = node.node_id
        if secure:
            self.socket.curve_publickey = z85.decode(node.node_id)
            self.socket.curve_secretkey = z85.decode(node.secret_key)
            self.socket.curve_serverkey = z85.decode(peer.node_id)
        self.socket.connect("tcp://{0}:{1}".format(peer.address, peer.port))
        LOGGER.info("C (%s) connected.", self.peer_label)

    def headers(self, msgtype):
        """
        Generate the message headers for sending on the wire.
        Any message body elements can be appended to the returned
        object before sending.
        @param msgtype: String name of message type
        @return: List that can be sent as a multipart message.
        """
        return [PROTOCOL_NAME, PROTOCOL_VERSION, self.node_header, msgtype]

    def splitmsg(self, msg):
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

        if protocol != PROTOCOL_NAME:
            raise InvalidMessage("Mismatch protocol name: {0} Expected: {1}" \
                                 .format(protocol, PROTOCOL_NAME))
        peer_addr, peer_port, peer_id = unpack_node(peer)
        return peer_addr, peer_port, peer_id, msgtype, extra

    @timer
    @zmqerror_adapter
    def ping(self):
        """
        Ping the peer.
        @return: Round trip time in ms
        """
        msg = self.headers('PING')
        LOGGER.debug("C (%s) send: %s", self.peer_label, msg)
        self.socket.send_multipart(msg)
        resp = self.socket.recv_multipart()
        LOGGER.debug("C (%s) recv: %s", self.peer_label, resp)
        self.splitmsg(resp)

    @timer
    @zmqerror_adapter
    def find_nodes(self, target_id):
        """
        Request the peers list of closest nodes to the given target.
        @param target_id: Node id (z85 encoded) to use for distance
        @return: Tuple of list of nodes (addr, port, id) and 
        round trip time in ms
        """
        msg = self.headers('FNOD')
        msg.append(target_id)
        LOGGER.debug("C (%s) send: %s", self.peer_label, msg)
        self.socket.send_multipart(msg)
        resp = self.socket.recv_multipart()
        LOGGER.debug("C (%s) recv: %s", self.peer_label, resp)
        peer_addr, peer_port, peer_id, msgtype, extra = self.splitmsg(resp)
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
        LOGGER.info("C (%s) closed.", self.peer_label)

class Server(object):
    """
    The server socket for the given node.
    """
    # static auth instance as only want a singleton
    # while there should only be a single Server per process
    # this is not necessarily true in unit tests/examples, etc.
    auth = None
    @zmqerror_adapter
    def __init__(self, addr, port, node_id, listener, secret_key, secure=True):
        """
        Creates a new server (bound but not dispatching messages)
        @param addr: IP address of this node
        @param port: Port number to listen on
        @param node_id: Public key (z85 encoded) of this node
        @param listener: Callback object to receive handle_* function calls
        """
        self.ctx = zmq.Context()
        self.node = pack_node(addr, port, node_id)
        self.shutdown = False
        self.listener = listener

        self.sock = Socket(self.ctx, zmq.ROUTER)
        self.sock.identity = node_id
        # self.sock.router_handover = 1 # allow peer reconnections with same id
        # enable curve encryption/auth
        if secure:
            if not Server.auth:
                Server.auth = ThreadAuthenticator()
                Server.auth.start()
                Server.auth.configure_curve(domain='*',
                                            location=CURVE_ALLOW_ANY)
            self.sock.curve_server = True
            self.sock.curve_secretkey = z85.decode(secret_key)
            self.sock.curve_publickey = z85.decode(node_id)
        self.sock.bind("tcp://*:{0}".format(port))
        LOGGER.info("S listening.")

    def headers(self, peer_id, msgtype):
        """
        Generate the message headers for sending on the wire.
        Any message body elements can be appended to the returned
        object before sending.
        @param peer_id: Peer id/key in z85 format.
        @param msgtype: String name of message type
        @return: List that can be sent as a multipart message.
        """
        return [peer_id, PROTOCOL_NAME, PROTOCOL_VERSION, self.node, msgtype]

    def splitmsg(self, msg):
        """
        Given a multipart message, split out the application important
        fields and return them.
        @param msg: Multipart message list
        @return: Tuple of peer_addr, peer_port, peer_id, msgtype, extra
        where extra is a list of msg body fields (or an empty list if none)
        @raise InvalidMessage: Any message parsing issues
        """
        if len(msg) < 5:
            raise InvalidMessage("Insufficient message parts")
        client, protocol, version, peer, msgtype = msg[:5]
        extra = msg[5:]

        if protocol != PROTOCOL_NAME:
            raise InvalidMessage("Mismatch protocol name: {0} Expected: {1}" \
                                 .format(protocol, PROTOCOL_NAME))
        peer_addr, peer_port, peer_id = unpack_node(peer)
        assert client == peer_id
        return peer_addr, peer_port, peer_id, msgtype, extra

    def dispatch(self):
        """
        Start the message dispatch loop (blocking).
        """
        while not self.shutdown:
            try:
                msg = self.sock.recv_multipart()
                LOGGER.debug("S (%s..) recv: %s", msg[0][:6], msg)
                self._handle_msg(msg)
            except TimeoutError:
                pass
        self.sock.close()
        self.ctx.term()

    def _handle_msg(self, msg):
        resp = None
        try:
            peer_addr, peer_port, peer_id, msgtype, extra = self.splitmsg(msg)
            resp = self.headers(peer_id, 'FAIL')
            # notify of peer actvity
            self.listener.handle_node_seen(peer_addr, peer_port, peer_id)
            if msgtype == 'PING':
                resp = self.headers(peer_id, 'PONG')
            elif msgtype == 'FNOD':
                target_id = extra[0]
                nodes = self.listener.handle_find_nodes(peer_id, target_id)
                resp = self.headers(peer_id, 'FNOD')
                resp += [ pack_node(x[0], x[1], x[2]) for x in nodes ]
            else:
                raise InvalidMessage('Unknown or unsupported message type: {0}' \
                                     .format(msgtype))
        except InvalidMessage, ex:
            LOGGER.warn('S error: %s', str(ex))
        if resp:
            LOGGER.debug('S (%s..) send: %s', resp[0][:6], resp)
            self.sock.send_multipart(resp)

    def close(self):
        self.shutdown = True
        if Server.auth:
            Server.auth.stop()
        LOGGER.info("S closed.")

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
