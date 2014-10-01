from collections import OrderedDict
from functools import update_wrapper, wraps
import time

import zmq.green as zmq

from version import __protocol__ as PROTOCOL_VERSION
from routing import Node

PROTOCOL_NAME = 'PEERZ'
DEFAULT_TIMEOUT = 10

def headers(node, msgtype):
    """
    Generate the message headers for sending on the wire.
    Any message body elements can be appended to the returned
    object before sending.
    @param node: Node details in 'id:addr:port' format.
    @param msgtype: String name of message type
    @return: List that can be sent as a multipart message.
    """
    return [PROTOCOL_NAME, PROTOCOL_VERSION, node, msgtype]

def splitmsg(msg):
    """
    Given a multipart message, split out the application important
    fields and return them.
    @param msg: Multipart message list
    @return: Tuple of peer_id, peer_addr, peer_port, msgtype, extra
    where extra is a list of msg body fields (or an empty list if none)
    @raise InvalidMessage: Any message parsing issues
    """
    if len(msg) < 4:
        raise InvalidMessage("Insufficient message parts")
    protocol, version, peer, msgtype = msg[:4]
    extra = msg[4:]
    peer_id, peer_addr, peer_port = unpack_node(peer)

    if protocol != PROTOCOL_NAME:
        raise InvalidMessage("Mismatch protocol name: {0} Expected: {1}" \
                             .format(protocol, PROTOCOL_NAME))
    return peer_id, peer_addr, peer_port, msgtype, extra

def pack_node(node_id, addr, port):
    """
    Given a tuple of basic node details, return in packed string format.
    @param node_id: Id of node as a long
    @param addr: IP address of node
    @param port: Port number of node
    @return: String representation of node.
    """
    return '{0}:{1}:{2}'.format(Node.id_to_str(node_id), addr, port)

def unpack_node(node_str):
    """
    Given a string representation of a node, return a tuple of its details.
    @param node_str: Node details in 'id:addr:port' format.
    @return: Tuple of node_id (as long), address, port
    """
    x = node_str.split(':')
    return (Node.str_to_id(x[0]), x[1], x[2])

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
    def __init__(self, node_id, addr, port, ctx=None, maxsize=20):
        """
        Initialise the pool with the supplied details.
        @param node_id: Id of this node as a long
        @param addr: IP address of this node
        @param port: Port number of this node
        @param ctx: ZMQ context, will be auto created if None
        @param maxsize: Maximum number of open connections
        """
        self.pool = OrderedDict()
        self.maxsize = maxsize
        if not ctx:
            self.ctx = zmq.Context()
        else:
            self.ctx = ctx
        self.node_id = node_id
        self.addr = addr
        self.port = port

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
            conn = Connection(self.node_id, self.addr, self.port, peer, self.ctx)
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
    def __init__(self, node_id, addr, port, peer, ctx=None):
        """
        Create a new connection to the specified peer.
        @param node_id: Node id (as long) of this node
        @param addr: IP address of this node
        @param port: Port number of this node
        @param peer: Peer (Node obj) to create connection to
        @param ctx: ZMQ context or autocreate if None
        """
        self.node = pack_node(node_id, addr, port)
        self.peer = peer
        if not ctx:
            self.ctx = zmq.Context()
            self.ctx_managed = True
        else:
            self.ctx = ctx
            self.ctx_managed = False
        self.socket = Socket(self.ctx, zmq.REQ)
        self.socket.connect("tcp://{0}:{1}".format(peer.address, peer.port))

    @timer
    @zmqerror_adapter
    def ping(self):
        """
        Ping the peer.
        @return: Round trip time in ms
        """
        msg = headers(self.node, 'PING')
        self.socket.send_multipart(msg)
        resp = self.socket.recv_multipart()
        splitmsg(resp)

    @timer
    @zmqerror_adapter
    def find_nodes(self, target_id):
        """
        Request the peers list of closest nodes to the given target.
        @param target_id: Node id (as long) to use for distance
        @return: Tuple of list of nodes (id_str, addr, port) and 
        round trip time in ms
        """
        msg = headers(self.node, 'FNOD')
        msg.append(Node.id_to_str(target_id))
        self.socket.send_multipart(msg)
        resp = self.socket.recv_multipart()
        peer_id, peer_addr, peer_port, msgtype, extra = splitmsg(resp)
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
    @zmqerror_adapter
    def __init__(self, node_id, addr, port, listener):
        """
        Creates a new server (bound but not dispatching messages)
        @param node_id: Id of this node (as long)
        @param addr: IP address of this node
        @param port: Port number to listen on
        @param listener: Callback object to receive handle_* function calls
        """
        self.ctx = zmq.Context()
        self.sock = Socket(self.ctx, zmq.REP)
        self.sock.bind("tcp://*:{0}".format(port))
        self.node = pack_node(node_id, addr, port)
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
        self.ctx.term()

    def _handle_msg(self, msg):
        resp = headers(self.node, 'FAIL')
        try:
            peer_id, peer_addr, peer_port, msgtype, extra = splitmsg(msg)
            # notify of peer actvity
            self.listener.handle_node_seen(peer_id, peer_addr, peer_port)
            if msgtype == 'PING':
                resp = headers(self.node, 'PONG')
            elif msgtype == 'FNOD':
                target_id = Node.str_to_id(extra[0])
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
