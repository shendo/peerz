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
import random
import time

from zmq.utils import z85

from peerz.messaging.base import MessageState
from peerz.routing import distance_sort, generate_random

class FindNodes(MessageState):
    states = ['initialised', 'querying', 'waiting response', 'exhausted', 'timedout']
    transitions = [
        {'trigger': 'query', 'source': 'initialised', 'dest': 'querying', 'before': '_update', 'after': '_send_query'},
        {'trigger': 'query', 'source': 'querying', 'dest': 'waiting response', 'before': '_update', 'conditions': ['has_outstanding']},
        {'trigger': 'response', 'source': ['querying', 'waiting response'], 'dest': 'exhausted', 'before': '_update', 'after': '_completed', 'conditions': ['unqueried_is_empty', 'outstanding_is_empty']},
        {'trigger': 'response', 'source': ['querying', 'waiting response'], 'dest': 'querying', 'before': '_update', 'after': '_send_query', 'conditions': ['has_capacity']},
        {'trigger': 'timeout', 'source': '*', 'dest': 'timedout', 'before': '_update', 'after': '_completed', },
    ]

    def parse_message(self, msg):
        self.target = z85.decode(msg.pop(0))
        # include peers that may be in bad states in case they have come good? will eventually be evicted
        self.closest = self.engine.nodetree.closest_to(self.target)
        self.unqueried = list(self.closest)  # shallow is fine
        self.queried = []
        self.outstanding = {} # peer_id -> query time

    def is_complete(self):
        return self.state in ['exhausted', 'timedout']
    
    def has_capacity(self):
        return len(self.outstanding) < self.max_concurrency

    def has_unqueried(self):
        return self.unqueried

    def unqueried_is_empty(self):
        return not self.unqueried

    def has_outstanding(self):
        return self.outstanding

    def outstanding_is_empty(self):
        return not self.outstanding

    def pack_request(self):
        return self.target

    @staticmethod
    def pack_response(closest):
        resp = b''
        for x in closest:
            # possibly the world's worst serialisation scheme
            resp += b'%s%s\0%i\0' % (x.node_id, x.address, x.port)
        return resp

    @staticmethod
    def unpack_response(content):
        while content:
            try:
                id = content[:32]
                addr, port, content = content[32:].split(b'\0', 2)
                yield addr, int(port), id
            except:
                break

    def handle_response(self, peer, txid, msgtype, content):
        if msgtype == 0x04:
            for x in self.unpack_response(content):
                n = self.engine.verify_peer(*x)
                self.engine.nodetree.add(n)
                # only add new nodes
                if not n.node_id in self.queried and not n.node_id in [ u.node_id for u in self.unqueried ]:
                    self.closest.append(n)
            distance_sort(self.closest, self.target, key=lambda x: x.node_id)
            self.closest = self.closest[:8]
            self.unqueried = [ x for x in self.closest if x.node_id not in self.queried ]
        # duplicate? first wins
        ts = self.outstanding.pop(peer.node_id, None)
        if ts:
            peer.add_rtt(time.time() * 1000 - ts)
            self.response()

    def _send_query(self):
        while self.has_capacity() and self.unqueried:
            peer = self.unqueried.pop(0)
            self.engine.send_external(peer, self.txid, 0x03, self.pack_request())
            self.outstanding[peer.node_id] = time.time() * 1000
            self.queried.append(peer.node_id)

    def _completed(self):
        for node_id, start in self.outstanding.items():
            # check duration?
            node = self.engine.nodetree.get_node_by_id(node_id)
            if node:
                node.timeout()

        if self.callback:
            self.callback(json.dumps([ x.to_json() for x in self.closest ]))

class Ping(MessageState):
    transitions = [
        {'trigger': 'ping', 'source': 'initialised', 'dest': 'waiting response', 'before': '_update', 'after': '_send_ping'},
        {'trigger': 'pong', 'source': 'waiting response', 'dest': 'complete', 'before': '_update', 'after': '_signal_pong'},
        {'trigger': 'timeout', 'source': '*', 'dest': 'timedout', 'before': '_update', 'after': '_timeout'},
    ]

    def parse_message(self, msg):
        self.unpack_request(msg)
        if self.peer:
            self.ping()
    
    def unpack_request(self, msg):
        peer_addr = msg.pop(0)
        peer_port = int(msg.pop(0))
        peer_id = z85.decode(msg.pop(0))
        self.peer = self.engine.verify_peer(peer_addr, peer_port, peer_id)

    def handle_response(self, peer, txid, msgtype, content):
        if msgtype == 0x02:
            peer.add_rtt(self.latency())
            self.pong()

    def _send_ping(self):
        self.engine.send_external(self.peer, self.txid, 0x01)

    def _signal_pong(self):
        if self.callback:
            self.callback()
    
    def _timeout(self):
        if self.peer:
            self.peer.timeout()

class Discovery(object):
    id = 0x00
    mtype_table = {0x01: 'PING',
                   0x02: 'PONG',
                   0x03: 'FNOD',
                   0x04: 'RNOD',
    }
    state_table = {'PING': Ping,
                   'FNOD': FindNodes,
    }

    def __init__(self, engine, 
                 neighbour_poll=120, 
                 random_poll=300,
                 verify_poll=61,
                 verify_limit=3,
                 reap_poll=62):
        self.engine = engine
        self.neighbour_poll = neighbour_poll
        self.random_poll = random_poll
        self.verify_poll = verify_poll
        self.verify_limit = verify_limit
        self.reap_poll = reap_poll
        self.next_random_poll = time.time() + random_poll
        self.next_verify_poll = time.time() + verify_poll
        self.next_reap_poll = time.time() + reap_poll
        self.next_neighbour_poll = time.time() # poll immediately as part of bootstrap/init 

    def handle_peer(self, peer, txid, msgtype, content):
        if msgtype == 0x01:
            self.engine.send_external(peer, txid, 0x02)
        elif msgtype == 0x03:
            assert len(content) == 32
            self.engine.send_external(peer, txid, 0x04,
                # do not include nodes of questionable status
                FindNodes.pack_response(
                    [ x for x in self.engine.nodetree.closest_to(content) if not x.is_failed() ]))

    def trigger_events(self):
        now = time.time()
        if self.next_neighbour_poll - now <= 0:
            self.poll_neighbours()
            self.next_neighbour_poll += self.neighbour_poll
            
        if self.next_random_poll - now <= 0:
            self.poll_random()
            self.next_random_poll += self.random_poll
            
        if self.next_verify_poll - now <= 0:
            self.verify_peers()
            self.next_verify_poll += self.verify_poll
            
        if self.next_reap_poll - now <= 0:
            self.reap_peers()
            self.next_reap_poll += self.reap_poll
            
    def poll_neighbours(self):
        self.engine.txmap.create(FindNodes,
                  [z85.encode(self.engine.node.node_id)], self.engine)
    
    def poll_random(self):
        # should really find a stale bucket not just random
        self.engine.txmap.create(FindNodes,
                  [z85.encode(generate_random())], self.engine)
    
    # needed?
    def verify_peers(self):
        queried = 0
        nodes = self.engine.nodetree.get_all_nodes()
        random.shuffle(nodes)
        for x in nodes:
            if x.is_discovered():
                self.engine.txmap.create(Ping,
                  [x.address, x.port, z85.encode(x.node_id)], self.engine)
                queried += 1
                if queried >= self.verify_limit:
                    break
    
    def reap_peers(self):
        for x in self.engine.nodetree.get_all_nodes():
            if x.is_failed():
                self.engine.nodetree.remove(x)
    
    @staticmethod
    def has_command(command):
        return command in Discovery.state_table.keys()
    
    @staticmethod
    def has_message(msgtype):
        return msgtype in Discovery.mtype_table.keys()