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
import time

from zmq.utils import z85

from peerz.messaging.base import MessageState
from peerz.messaging.discovery import FindNodes
from peerz.routing import distance_sort, id_for_key

class FindValue(MessageState):
    states = ['initialised', 'querying', 'waiting response', 'found', 'exhausted', 'timedout']
    transitions = [
        {'trigger': 'query', 'source': 'initialised', 'dest': 'querying', 'before': '_update', 'after': '_send_query'},
        {'trigger': 'query', 'source': 'querying', 'dest': 'querying', 'before': '_update', 'after': '_send_query', 'conditions': ['has_capacity', 'has_unqueried']},
        {'trigger': 'query', 'source': 'querying', 'dest': 'waiting response', 'before': '_update', 'conditions': ['has_outstanding']},
        {'trigger': 'response', 'source': 'waiting response', 'dest': 'found', 'before': '_update', 'after': '_completed', 'conditions': ['is_found']},
        {'trigger': 'response', 'source': ['querying', 'waiting response'], 'dest': 'exhausted', 'before': '_update', 'after': '_completed', 'conditions': ['unqueried_is_empty', 'outstanding_is_empty']},
        {'trigger': 'response', 'source': ['querying', 'waiting response'], 'dest': 'querying', 'before': '_update', 'after': '_send_query', 'conditions': ['has_capacity']},
        {'trigger': 'timeout', 'source': '*', 'dest': 'timedout', 'before': '_update', 'after': '_completed', },
    ]

    def parse_message(self, msg):
        self.key = id_for_key(msg.pop(0))
        self.context = msg.pop(0)
        self.closest = self.engine.nodetree.closest_to(self.key)
        self.unqueried = list(self.closest)  # shallow is fine
        self.queried = []
        self.outstanding = {} # peer_id -> query time
        self.value = None

    def is_complete(self):
        return self.state in ['found', 'exhausted', 'timedout']
    
    def is_found(self):
        return self.value

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
        return self.key

    @staticmethod
    def pack_node_response(closest):
        resp = b''
        for x in closest:
            # possibly the world's worst serialisation scheme
            resp += b'%s%s\0%i\0' % (x.node_id, x.address, x.port)
        return resp

    @staticmethod
    def unpack_node_response(content):
        while content:
            try:
                id = content[:32]
                addr, port, content = content[32:].split(b'\0', 2)
                yield addr, int(port), id
            except:
                break

    def handle_response(self, peer, txid, msgtype, content):
        if msgtype == 0x08:
            self.value = content
            self.engine.hashtable[self.key] = peer.node_id, time.time(), content 
        elif msgtype == 0x06:
            for x in FindNodes.unpack_response(content):
                n = self.engine.verify_peer(*x)
                self.engine.nodetree.add(n)
                # only add new nodes
                if not n.node_id in self.queried and not n.node_id in [ u.node_id for u in self.unqueried ]:
                    self.closest.append(n)
            distance_sort(self.closest, self.key, key=lambda x: x.node_id)
            self.closest = self.closest[:8]
            self.unqueried = [ x for x in self.closest if x.node_id not in self.queried ]
        # duplicate? first wins
        ts = self.outstanding.pop(peer.node_id, None)
        if ts:
            peer.add_rtt(time.time() * 1000 - ts)
            self.response()

    def _send_query(self):
        peer = self.unqueried.pop(0)
        self.engine.send_external(peer, self.txid, 0x05, self.pack_request())
        self.outstanding[peer.node_id] = time.time() * 1000
        self.queried.append(peer.node_id)
        self.query()

    def _completed(self):
        if self.callback:
            self.callback(json.dumps(self.value, ensure_ascii=False))


class StoreValue(MessageState):
    states = ['initialised', 'waiting response', 'storing', 'stored', 'timedout']
    transitions = [
        {'trigger': 'query', 'source': 'initialised', 'dest': 'waiting response', 'before': '_update', 'after': '_send_query'},
        {'trigger': 'response', 'source': 'waiting response', 'dest': 'storing', 'before': '_update', 'after': '_completed'},
        {'trigger': 'timeout', 'source': '*', 'dest': 'timedout', 'before': '_update', 'after': '_completed', },
    ]

    def parse_message(self, msg):
        self.key = msg.pop(0)
        self.content = msg.pop(0)
        self.context = msg.pop(0)
        self.engine.hashtable[id_for_key(self.key)] = (self.engine.node.node_id, time.time(), self.content)
        self.closest = [  x.to_json() for x in self.engine.nodetree.closest_to(id_for_key(self.key)) ]

    def is_complete(self):
        return self.state in ['stored', 'timedout']
    
    def pack_request(self):
        return b'%s%s' % (self.key, self.content)

    @staticmethod
    def unpack_response(content):
        return content[:32], content[32:]

    def _send_query(self):
        self.engine.txmap.create(FindNodes,
                  [z85.encode(id_for_key(self.key))], self.engine, callback=self._closest)

    def _closest(self, nodes):
        self.closest = json.loads(nodes)
        for x in self.closest:
            peer = self.engine.verify_peer(x['address'], x['port'], x['node_id'])
            self.engine.send_external(peer, self.txid, 0x09, self.pack_request())

    def _completed(self):
        if self.callback:
            self.callback(json.dumps(self.closest))

class GetPublished(MessageState):

    def parse_message(self, msg):
        if self.callback:
            d = {repr(k): repr(v) for k, v in self.engine.hashtable.items()
                 if v[0] == self.engine.node.node_id}
            self.callback(json.dumps(d, ensure_ascii=False))

    def is_complete(self):
        return True
    
    def timeout(self):
        pass

class GetHashtable(MessageState):

    def parse_message(self, msg):
        d = {repr(k): repr(v) for k, v in self.engine.hashtable.items()}
        if self.callback:
            self.callback(json.dumps(d, ensure_ascii=False))

    def is_complete(self):
        return True
    
    def timeout(self):
        pass
    
class RemoveValue(MessageState):

    def parse_message(self, msg):
        self.key = msg.pop(0)
        self.context = msg.pop(0)
        self.engine.hashtable.pop(id_for_key(self.key), None)
        if self.callback:
            self.callback()
        
    def is_complete(self):
        return True
    
    def timeout(self):
        pass
    
class DistributedHashtable(object):
    id = 0x01
    mtype_table = {0x05: 'FVAL',
                   0x06: 'NODE',
                   0x08: 'RVAL',
                   0x09: 'STOR',
                   0x0a: 'STOK',
                   0x0b: 'REMV',
                   0x0c: 'RMOK',
    }
    state_table = {'FVAL': FindValue,
                   'STOR': StoreValue,
                   'HASH': GetHashtable,
                   'PUBL': GetPublished,
    }

    def __init__(self, engine, published_refresh=600, closest_refresh=300):
        self.engine = engine
        self.published_refresh = published_refresh
        self.closest_refresh = closest_refresh
        self.next_published_refresh = time.time()
        self.next_closest_refresh = time.time() + closest_refresh


    def handle_peer(self, peer, txid, msgtype, content):
        if msgtype == 0x05:
            assert len(content) == 32
            entry = self.engine.hashtable.get(content)
            if entry:
                node_id, store_time, val = entry
                # found
                self.engine.send_external(peer, txid, 0x08, val)
            else:
                # send next closest nodes
                self.engine.send_external(peer, txid, 0x06,
                    FindNodes.pack_response(self.engine.nodetree.closest_to(content)))

        elif msgtype == 0x09:
            key = id_for_key(content)
            self.engine.hashtable[key] = (peer.node_id, time.time(), content)

    def trigger_events(self):
        now = time.time()
        if self.next_published_refresh - now <= 0:
            self.republish_own()
            self.next_published_refresh += self.published_refresh

        if self.next_closest_refresh - now <= 0:
            self.republish_closest()
            self.expire_values()
            self.next_closest_refresh += self.closest_refresh

    def republish_own(self):
        for k, v in self.engine.hashtable.items():
            node_id, last, content = v
            if node_id == self.engine.node.node_id and time.time() > last + self.published_refresh:
                self.engine.txmap.create(StoreValue,
                  [k, content, 'default'], self.engine)
    
    def republish_closest(self):
        for k, v in self.engine.hashtable.items():
            node_id, last, content = v
            if node_id != self.engine.node.node_id and time.time() > last + self.closest_refresh:
                # are we the closest node we know of
                if self.engine.nodetree.closest_to(id_for_key(k)):
                    self.engine.txmap.create(StoreValue,
                      [k, content, 'default'], self.engine)

    
    def expire_values(self):
        for k, v in self.engine.hashtable.items():
            node_id, last, content = v
            if time.time() >= last + (2.5 * self.published_refresh):
                assert node_id != self.engine.node.node_id
                self.engine.hashtable.pop(k, None)
        
    @staticmethod
    def has_command(command):
        return command in DistributedHashtable.state_table.keys()

    @staticmethod
    def has_message(msgtype):
        return msgtype in DistributedHashtable.mtype_table.keys()
    