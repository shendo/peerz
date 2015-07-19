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

import socket
import time

import nacl.utils
from nacl.public import PrivateKey, PublicKey, Box

import zmq
from zmq.utils import z85

from peerz.persistence import LocalStorage
from peerz.routing import Node, RoutingZone
from peerz import transport, context, transaction

# TODO config class
# simultaneous requests
A = 3
# number of comms fails to node before evicting from node tree
MAX_NODE_FAILS = 2
# automatic find_nodes request poll cycles in seconds
NEIGHBOURHOOD_POLL = 120
STALE_POLL = 600
BASE_PORT = 7111

class Engine(object):

    def __init__(self, ctx, pipe, seeds=None, *args, **kwargs):
        self.ctx = ctx
        self.pipe = pipe
        if seeds:
            self.seeds = seeds
        else:
            self.seeds = []
        # externally advertised
        self.port = BASE_PORT
        self.addr = socket.gethostbyname(socket.gethostname())
        # local may differ to external
        self.bindport = self.port
        self.bindaddr = ''
        self.udpserver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.registry = dict([(id, val(self)) for id, val in context.registry.items()])
        self.defrag = transport.DefragMap()
        self.txmap = transaction.TxMap()
        # TODO need to think about this better.. what happens if bindaddr is invalid??
        # how do we flag error back to client if terminal?
        while True:
            try:
                server_address = (self.bindaddr, self.bindport)
                self.udpserver.bind(server_address)
                break
            except socket.error:
                self.port += 1
                self.bindport += 1
        # persistence between runs
        # TODO: external responsibility?
        self.localstore = LocalStorage('/tmp/testing', self.port)
        self._load_state()
        # clean start
        if not self.node:
            self.reset()
        self.secret_key = PrivateKey(self.node.secret_key)
        self.secure = True

        self.poller = zmq.Poller()
        self.poller.register(self.pipe, zmq.POLLIN)
        self.shutdown = False
        self.run()

    def reset(self, secret_key=None):
        """
        Remove any existing state and reset as a new node.
        """
        if not secret_key:
            public_key, secret_key = zmq.curve_keypair()
            public_key = z85.decode(public_key)
            secret_key = z85.decode(secret_key)
        else:
            secret_key = z85.decode(secret_key)
            public_key = str(PrivateKey(secret_key).public_key)

        self.node = Node(self.addr, self.port, public_key, secret_key)
        self.secret_key = PrivateKey(self.node.secret_key)
        self.nodetree = RoutingZone(self.node.node_id)
        # ensure we exist in own tree
        self.nodetree.add(self.node)
        self.txmap = transaction.TxMap()
        self.defrag = transport.DefragMap()
        self._dump_state()

    def run(self):
        # Signal actor successfully initialized
        self.signal_api()

        while not self.shutdown:
            timeout = 5000
            if timeout < 0:
                timeout = 0
            items = dict(self.poller.poll(timeout * 1000))

            if self.pipe in items and items[self.pipe] == zmq.POLLIN:
                self.recv_api()
            if self.udpserver.fileno() in items and items[self.udpserver.fileno()] == zmq.POLLIN:
                self.recv_external()
            self._dump_state()
            if time.time():  # TODO: add real time check
                self.txmap.timeout(5000)

    def start(self, secret_key=None):
        if secret_key:
            self.reset(secret_key)
# TODO: how to handle starting seeds?... is list optional?
#         if not self.seeds:
#             raise ValueError('Seeds list must not be empty and must contain '
#                              'endpoints in "address:port:key" format.')
        self.poller.register(self.udpserver.fileno(), zmq.POLLIN)
        for endpoint in self.seeds:
            addr, port, id = endpoint.split(':', 2)
            self.nodetree.add(Node(addr, int(port), z85.decode(id)))
        self.txmap.create(context.core.Global.state_table['FNOD'], [z85.encode(self.node.node_id)], self)

    def send_api_node(self):
        self.send_api(self.node.address, zmq.SNDMORE)
        self.send_api(str(self.node.port), zmq.SNDMORE)
        self.send_api(z85.encode(self.node.node_id))

    def stop(self):
        self.shutdown = True

    def recv_api(self):
        request = self.pipe.recv_multipart()
        command = request.pop(0).decode('UTF-8')
        if command == 'START':
            self.start(request.pop(0).decode('UTF-8'))
            self.send_api_node()
        elif command == 'STOP':
            self.stop()
        elif command == 'RESET':
            self.reset(request.pop(0).decode('UTF-8'))
            self.send_api_node()
        elif command == 'NODE':
            self.send_api_node()
        elif command == 'PEERS':
            filtered_nodes = [ x for x in self.nodetree.get_all_nodes() if x.node_id != self.node.node_id ]
            if not filtered_nodes:
                self.send_api('')
            for i, peer in enumerate(filtered_nodes, start=1):
                self.send_api(peer.address, zmq.SNDMORE)
                self.send_api(str(peer.port), zmq.SNDMORE)
                self.send_api(z85.encode(peer.node_id), i < len(filtered_nodes) and zmq.SNDMORE or 0)
        else:
            for x in context.registry.values():
                if x.has_command(command):
                    self.txmap.create(x.state_table[command], request, self)
                    return
            self.send_api('Invalid Command')  # placeholder, what should error handling look like from client

    def send_api(self, msg, flags=0):
        self.pipe.send(msg, flags=flags)

    def signal_api(self):
        self.pipe.signal()

    def encrypt(self, peer_id, msg):
        nonce = nacl.utils.random(Box.NONCE_SIZE)
        box = Box(self.secret_key, PublicKey(peer_id))
        return box.encrypt(msg, nonce)

    def decrypt(self, peer_id, msg):
        box = Box(self.secret_key, PublicKey(peer_id))
        return box.decrypt(msg)

    def verify_peer(self, addr, port, node_id):
        # IP filter/blacklisting...
        # public key whitelisting?
        node = self.nodetree.get_node_by_addr(addr, port)
        # same node but external address ahs changed
        if node and node.node_id != node_id:
            self.nodetree.remove(node)
        elif node:  # exists already
            return node
        return Node(addr, port, node_id)

    def recv_external(self):
        try:
            data, addr = self.udpserver.recvfrom(2048)
            p = transport.Packet()
            p.unpack(data)
            if p.mode == 0x02:
                p.payload = self.decrypt(p.node_id, p.payload)
            data = transport.Payload()
            data.unpack(p.payload)
            peer = self.verify_peer(addr[0], addr[1], p.node_id)
            msg = self.defrag.get_msg(data.txid, data.fragment, data.lastfrag, data.content)
            # need context id?
            if msg != None:
                # if is peer request...
                if data.msgtype % 2 == 1:
                    context.registry[0x00](self).handle_peer(peer, data.txid, data.msgtype, data.content)
                    self.nodetree.add(peer)
                else:
                    tx = self.txmap.get(data.txid)
                    if tx:
                        tx.handle_response(peer, data.txid, data.msgtype, data.content)
                    self.nodetree.add(peer)
        except Exception, ex:
            print 'Warning: %s... ignoring' % str(ex)

    def send_external(self, node, txid, mtype, content=b''):
        payload = transport.Payload()
        payload.pack(txid, mtype, content)
        for x in payload.fragments:
            if self.secure:
                payload = self.encrypt(node.node_id, x)
            p = transport.Packet()
            p.pack(payload, self.node.node_id, self.secure and 0x02 or 0x01)
            self.udpserver.sendto(p.msg, (node.address, node.port))


    def _dump_state(self):
        """
        Dump local state for persistence between runs.
        """
        self.localstore.store('node', self.node)
        # TODO is it better to rebuild new state or discard stale after restart?
        self.localstore.store('nodetree', self.nodetree)

    def _load_state(self):
        """
        Reload any persisted state.
        """
        self.node = self.localstore.fetch('node')
        self.nodetree = self.localstore.fetch('nodetree')
