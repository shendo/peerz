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

# TODO: these packge/module names suck

import time

from transitions import Machine, MachineError
import zmq
from zmq.utils import z85

from peerz.routing import sort, Node

class FindNodes(object):
    states = ['initialised', 'querying', 'waiting response', 'found', 'exhausted', 'timedout']
    transitions = [
        # {'trigger': 'start', 'source': 'initialised', 'dest': 'complete', 'before': '_update', 'after': '_completed', 'conditions': ['is_found']},
        {'trigger': 'query', 'source': 'initialised', 'dest': 'querying', 'before': '_update', 'after': '_send_query'},
        {'trigger': 'query', 'source': 'querying', 'dest': 'querying', 'before': '_update', 'after': '_send_query', 'conditions': ['has_capacity', 'has_unqueried']},
        {'trigger': 'query', 'source': 'querying', 'dest': 'waiting response', 'before': '_update', 'conditions': ['has_outstanding']},
        # {'trigger': 'response', 'source': 'waiting response', 'dest': 'found', 'before': '_update', 'after': '_completed', 'conditions': ['is_found']},
        {'trigger': 'response', 'source': ['querying', 'waiting response'], 'dest': 'exhausted', 'before': '_update', 'after': '_completed', 'conditions': ['unqueried_is_empty', 'outstanding_is_empty']},
        {'trigger': 'response', 'source': ['querying', 'waiting response'], 'dest': 'querying', 'before': '_update', 'after': '_send_query', 'conditions': ['has_capacity']},
        {'trigger': 'timeout', 'source': '*', 'dest': 'timedout', 'before': '_update', 'after': '_completed', },
    ]

    def __init__(self, engine, txid, msg, max_concurrent=3):
        self.engine = engine
        self.machine = Machine(model=self,
                               states=FindNodes.states,
                               transitions=FindNodes.transitions,
                               initial='initialised')

        self.start = self.last_change = time.time() * 1000
        self.txid = txid
        self.times = {}
        self.max_concurrent = max_concurrent
        self.target = z85.decode(msg.pop(0))
        self.closest = self.engine.nodetree.closest_to(self.target)
        self.unqueried = list(self.closest)  # shallow is fine
        self.queried = []
        # TODO indiv msg timeouts and peer removal
        self.outstanding = []
        self.query()

#     def is_found(self):
#         return self.target in self.closest

    def has_capacity(self):
        return len(self.outstanding) < self.max_concurrent

    def has_unqueried(self):
        return self.unqueried

    def unqueried_is_empty(self):
        return not self.unqueried

    def has_outstanding(self):
        return self.outstanding

    def outstanding_is_empty(self):
        return not self.outstanding

    def _pack_request(self):
        return self.target

    @staticmethod
    def _pack_response(closest):
        resp = b''
        for x in closest:
            # possibly the world's worst serialisation scheme
            resp += b'%s%s\0%i\0' % (x.node_id, x.address, x.port)
        return resp

    def _unpack_response(self, content):
        while content:
            try:
                id = content[:32]
                addr, port, content = content[32:].split(b'\0', 2)
                yield addr, int(port), id
            except:
                break

    def handle_response(self, peer, txid, msgtype, content):
        if msgtype == 0x04:
            for x in self._unpack_response(content):
                n = Node(*x)
                self.engine.nodetree.add(n)
                # only add new nodes
                if not n.node_id in self.queried and not n.node_id in [ u.node_id for u in self.unqueried ]:
                    self.closest.append(n)
            sort(self.closest, self.target, key=lambda x: x.node_id)
            self.closest = self.closest[:8]
            self.unqueried = [ x for x in self.closest if x.node_id not in self.queried ]
            self.outstanding.remove(peer.node_id)
            self.response()

    def _update(self):
        now = time.time() * 1000
        self.times.setdefault(self.state, 0.0)
        self.times[self.state] += (now - self.last_change)
        self.last_change = now

    def duration(self):
        return time.time() * 1000 - self.start

    def _send_query(self):
        peer = self.unqueried.pop(0)
        self.engine.send_external(peer, self.txid, 0x03, self._pack_request())
        self.outstanding.append(peer.node_id)
        self.queried.append(peer.node_id)
        self.query()

    def _completed(self):
        for i, x in enumerate(self.closest, start=1):
            self.engine.send_api(x.address, zmq.SNDMORE)
            self.engine.send_api(str(x.port), zmq.SNDMORE)
            self.engine.send_api(z85.encode(x.node_id), i < len(self.closest) and zmq.SNDMORE or 0)

class Ping(object):
    states = ['initialised', 'waiting response', 'complete', 'timedout']
    transitions = [
        {'trigger': 'ping', 'source': 'initialised', 'dest': 'waiting response', 'before': '_update', 'after': '_send_ping'},
        {'trigger': 'pong', 'source': 'waiting response', 'dest': 'complete', 'before': '_update', 'after': '_signal_pong'},
        {'trigger': 'timeout', 'source': '*', 'dest': 'timedout', 'before': '_update'},
    ]

    def __init__(self, engine, txid, msg):
        self.engine = engine
        self.machine = Machine(model=self,
                               states=Ping.states,
                               transitions=Ping.transitions,
                               initial='initialised')

        self.start = self.last_change = time.time() * 1000
        self.txid = txid
        self.times = {}
        self._unpack_request(msg)
        if self.peer:
            self.ping()

    def _unpack_request(self, msg):
        peer_addr = msg.pop(0)
        peer_port = int(msg.pop(0))
        peer_id = z85.decode(msg.pop(0))
        self.peer = self.engine.verify_peer(peer_addr, peer_port, peer_id)

    def _update(self):
        now = time.time() * 1000
        self.times.setdefault(self.state, 0.0)
        self.times[self.state] += (now - self.last_change)
        self.last_change = now

    def handle_response(self, peer, txid, msgtype, content):
        if msgtype == 0x02:
            self.pong()

    def duration(self):
        return time.time() * 1000 - self.start

    def latency(self):
        return self.times.setdefault('waiting response', 0.0)

    def _send_ping(self):
        self.engine.send_external(self.peer, self.txid, 0x01)

    def _signal_pong(self):
        print 'latency: %0.02f ms' % self.latency()
        self.engine.signal_api()

class Global(object):
    id = 0x00
    mtype_table = {0x01: 'PING',
                   0x02: 'PONG',
                   0x03: 'FNOD',
                   0x04: 'FNODresp',
                   0x05: 'FVAL',
                   0x06: 'FVALresp',
    }
    state_table = {'PING': Ping,
                   'FNOD': FindNodes,
                   'FVAL': None,
    }

    def __init__(self, engine):
        self.engine = engine

    def handle_peer(self, peer, txid, msgtype, content):
        if msgtype == 0x01:
            self.engine.send_external(peer, txid, 0x02)
        elif msgtype == 0x03:
            assert len(content) == 32
            self.engine.send_external(peer, txid, 0x04,
                FindNodes._pack_response(self.engine.nodetree.closest_to(content)))

    @staticmethod
    def has_command(command):
        return command in Global.state_table.keys()
