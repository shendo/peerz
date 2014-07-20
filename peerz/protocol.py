from collections import OrderedDict
from functools import update_wrapper
import time

import zmq.green as zmq

from version import __protocol__ as PROTOCOL_VERSION
from routing import Node

PROTOCOL_NAME = 'PEERZ'
DEFAULT_TIMEOUT = 20

def headers(node, msgtype):
    return [PROTOCOL_NAME, PROTOCOL_VERSION, node, msgtype]
    
def splitmsg(msg):
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
    return '{0}:{1}:{2}'.format(Node.id_to_str(node_id), addr, port)

def unpack_node(node_str):
    x = node_str.split(':')
    return (Node.str_to_id(x[0]), x[1], x[2])

class ConnectionPool(object):
    
    def __init__(self, node_id, addr, port, ctx=None, maxsize=20):
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
        if peer.node_id in self.pool:
            conn = self.pool.pop(peer.node_id)
        else:
            conn = Connection(self.node_id, self.addr, self.port, peer, self.ctx)
        self.pool[peer.node_id] = conn
        if len(self.pool) > self.maxsize:
            old = self.pool.popitem(last=False)
            old.close()
        return conn

class Connection(object):
    
    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()
        
    def __init__(self, node_id, addr, port, peer, ctx=None):
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
    
    def ping(self):
        msg = headers(self.node, 'PING')
        start = time.time()
        self.socket.send_multipart(msg)
        resp = self.socket.recv_multipart()
        rtt = time.time() - start
        splitmsg(resp)
        return rtt
        
    def find_nodes(self, target_id):
        msg = headers(self.node, 'FNOD')
        msg.append(Node.id_to_str(target_id))
        start = time.time() 
        self.socket.send_multipart(msg)
        resp = self.socket.recv_multipart()
        rtt = time.time() - start
        peer_id, peer_addr, peer_port, msgtype, extra = splitmsg(resp)
        nodes = [ unpack_node(x) for x in extra ]
        return nodes, rtt
    
    def close(self):
        self.socket.close()
        if self.ctx_managed:
            self.ctx.term()
    
class Server(object):
    def __init__(self, node_id, addr, port, listener):
        try:
            self.ctx = zmq.Context()
            self.sock = Socket(self.ctx, zmq.REP)
            self.sock.bind("tcp://*:{0}".format(port))
            self.node = pack_node(node_id, addr, port)
            self.shutdown = False
            self.listener = listener
        except zmq.ZMQError, ex:
            raise ConnectionError(str(ex))
        
    def dispatch(self):
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
                nodes = self.listener.handle_find_nodes(peer_id, Node.str_to_id(extra[0]))
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
    pass

class TimeoutError(ConnectionError):
    pass

class InvalidMessage(Exception):
    pass