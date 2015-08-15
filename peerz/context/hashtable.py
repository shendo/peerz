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

import time

from transitions import Machine, MachineError
import zmq
from zmq.utils import z85

from peerz.routing import distance_sort, Node

class FindValue(object):
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

    def __init__(self, engine, txid, msg, silent=False, max_concurrent=3):
        self.engine = engine
        self.machine = Machine(model=self,
                               states=FindValue.states,
                               transitions=FindValue.transitions,
                               initial='initialised')

        self.start = self.last_change = time.time() * 1000
        self.txid = txid
        self.silent = silent
        self.times = {}
        self.max_concurrent = max_concurrent
        self.target = z85.decode(msg.pop(0))
        self.closest = self.engine.nodetree.closest_to(self.target)
        self.unqueried = list(self.closest)  # shallow is fine
        self.queried = []
        self.outstanding = {} # peer_id -> query time
        self.query()

    def is_complete(self):
        return self.state in ['found', 'exhausted', 'timedout']
    
    def is_found(self):
        return self.target in self.closest

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
        self.outstanding[peer.node_id] = time.time() * 1000
        self.queried.append(peer.node_id)
        self.query()

    def _completed(self):
        if self.silent:
            return
        self.engine.send_api([ x.to_json() for x in self.closest() ])
        
class DistributedHashtable(object):
    id = 0x01
    mtype_table = {0x01: 'FVAL',
                   0x02: 'RVAL',
                   0x03: 'STOR',
    }
    state_table = {'FVAL': FindValue,
    }

    def __init__(self, engine, neighbour_poll=120, random_poll=300):
        self.engine = engine


    def handle_peer(self, peer, txid, msgtype, content):
        if msgtype == 0x01:
            self.engine.send_external(peer, txid, 0x02)
        elif msgtype == 0x03:
            assert len(content) == 32
            self.engine.send_external(peer, txid, 0x04,
                FindValue._pack_response(self.engine.nodetree.closest_to(content)))

    def trigger_events(self):
        now = time.time()
        
    @staticmethod
    def has_command(command):
        return command in DistributedHashtable.state_table.keys()
    