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

import os

class TxMap(object):
    def __init__(self):
        self.transactions = {}

    def next_txid(self):
        txid = os.urandom(4)
        while txid in self.transactions:
            txid = os.urandom(4)
        return txid

    def has(self, txid):
        return txid in self.transactions

    def create(self, clazz, msg, engine):
        txid = self.next_txid()
        self.transactions[txid] = clazz(engine, txid, msg)
        return txid

    def get(self, txid):
        return self.transactions.get(txid)

    def timeout(self, age):
        for tx in self.transactions.values():
            if tx.duration() > age:
                tx.timeout()

    def expire(self, age):
        for key, val in self.transactions.items():
            if val.duration() > age:
                del self.transactions[key]
