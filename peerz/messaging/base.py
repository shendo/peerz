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

from transitions import Machine

class MessageState(object):
    states = ['initialised', 'waiting response', 'complete', 'timedout']
    transitions = [
        {'trigger': 'query', 'source': 'initialised', 'dest': 'waiting response', 'before': '_update', 'after': '_send_query'},
        {'trigger': 'response', 'source': 'waiting response', 'dest': 'complete', 'before': '_update', 'after': '_completed'},
        {'trigger': 'timeout', 'source': '*', 'dest': 'timedout', 'before': '_update', 'after': '_completed', },
    ]

    def __init__(self, engine, txid, msg, callback=None, max_duration=5000, max_concurrency=3):
        self.engine = engine
        self.callback = callback
        self.machine = Machine(model=self,
                               states=self.states,
                               transitions=self.transitions,
                               initial='initialised')

        self.start = self.last_change = time.time() * 1000
        self.max_duration = max_duration
        self.max_concurrency = max_concurrency
        self.txid = txid
        self.times = {}
        self.parse_message(msg)
        self.query()
 
    def query(self):
        pass

    def parse_message(self, msg):
        self.val = msg.pop(0)
        
    def is_complete(self):
        return self.state in ['complete', 'timedout']
    
    def pack_request(self):
        return None

    @staticmethod
    def unpack_response(content):
        return None

    @staticmethod
    def pack_response(content):
        return None

    def _update(self):
        now = time.time() * 1000
        self.times.setdefault(self.state, 0.0)
        self.times[self.state] += (now - self.last_change)
        self.last_change = now

    def duration(self):
        return time.time() * 1000 - self.start

    def latency(self):
        return self.times.setdefault('waiting response', 0.0)

    def _send_query(self):
        pass

    def _completed(self):
        pass
