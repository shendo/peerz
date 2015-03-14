# Peerz - P2P python library using ZeroMQ sockets and gevent
# Copyright (C) 2014 Steve Henderson
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

from peerz.routing import distance, bit_number
from peerz.routing import RoutingBin, RoutingZone, Node

def test_distance():
    assert distance('1b8f0f', '1b8f0f') == 0
    assert distance('1b8f10', '1b8f0f') == 0x1f
    assert distance('1b8f0f', '1b8f10') == 0x1f
    assert distance('2b8f11', '1b8f10') == 0x300001
    assert distance('300001', '2b8f11') == 0x1b8f10
    assert distance('123456789abcdef0', '1234567890abcdef') == 0xa17131f
    assert distance('123456789abcdef0', '0a') == 0x123456789abcdefa
    assert distance('ffffffffffffffffffffffffffffffff', '00') == 0xffffffffffffffffffffffffffffffff

def test_bit_number():
    assert bit_number('0123456789abcdef0123456789abcdef01234567', 0) == 0
    assert bit_number('f123456789abcdeff123456789abcdef01234500', 0) == 1
    assert bit_number('f0000000000000000123456789ab000000000000', 159) == 0
    assert bit_number('0000000000000000000000000000000000000001', 159) == 1


class TestRoutingBin(object):

    def test_get_by_id(self):
        r = RoutingBin()
        r.push(Node('123456789abcdef0', '127.0.0.1', 7781))
        r.push(Node('123456789abcdef1', '127.0.0.1', 7782))
        r.push(Node('123456789abcdef2', '127.0.0.2', 7777))

        n = r.get_by_id('123456789abcdef1')
        assert n.node_id == '123456789abcdef1'
        assert n.address == '127.0.0.1'
        assert n.port == 7782

    def test_get_by_address(self):
        r = RoutingBin()
        r.push(Node('123456789abcdef0', '127.0.0.1', 7781))
        r.push(Node('123456789abcdef1', '127.0.0.1', 7782))
        r.push(Node('123456789abcdef2', '127.0.0.2', 7777))

        n = r.get_by_address('127.0.0.1', 7781)
        assert n.node_id == '123456789abcdef0'
        assert n.address == '127.0.0.1'
        assert n.port == 7781

    def test_get_all(self):
        r = RoutingBin()
        r.push(Node('123456789abcdef0', '127.0.0.1', 7781))
        r.push(Node('123456789abcdef1', '127.0.0.1', 7782))
        r.push(Node('123456789abcdef2', '127.0.0.2', 7777))

        assert len(r.get_all()) == 3

    def test_get_node_ids(self):
        r = RoutingBin()
        r.push(Node('123456789abcdef0', '127.0.0.1', 7781))
        r.push(Node('123456789abcdef1', '127.0.0.1', 7782))
        r.push(Node('123456789abcdef2', '127.0.0.2', 7777))

        assert len(r.get_node_ids()) == 3
        assert '123456789abcdef0' in r.get_node_ids()
        assert '123456789abcdef1' in r.get_node_ids()
        assert '123456789abcdef2' in r.get_node_ids()

    def test_get_oldest(self):
        r = RoutingBin()
        n = Node('123456789abcdef0', '127.0.0.1', 7781)
        r.push(n)
        assert r.get_oldest().node_id == '123456789abcdef0'

        r.push(Node('123456789abcdef1', '127.0.0.1', 7782))
        assert r.get_oldest().node_id == '123456789abcdef0'
        # now update (push to end of list)
        r.update(n.node_id)
        assert r.get_oldest().node_id == '123456789abcdef1'

    def test_get_closest_to(self):
        r = RoutingBin()
        n1 = Node('123456789abcde00', '127.0.0.1', 7781)
        n2 = Node('123456789abcdef1', '127.0.0.1', 7782)
        n3 = Node('ffffffffffffffff', '127.0.0.2', 7777)
        r.push(n1)
        r.push(n2)
        r.push(n3)

        nodes = r.get_closest_to('123456789abcdef2')
        assert len(nodes) == 1
        assert n2 in nodes

        nodes = r.get_closest_to('123456789abcdef2', 2)
        assert len(nodes) == 2
        assert n1 in nodes
        assert n2 in nodes

        nodes = r.get_closest_to('123456789abcdef2', 10)
        assert len(nodes) == 3

    def test_len(self):
        r = RoutingBin()
        assert not len(r)

        r.push(Node('123456789abcdef0', '127.0.0.1', 7781))
        r.push(Node('123456789abcdef1', '127.0.0.1', 7782))
        r.push(Node('123456789abcdef2', '127.0.0.2', 7777))
        assert len(r) == 3

    def test_remaining(self):
        r = RoutingBin(maxsize=5)
        r.push(Node('123456789abcdef0', '127.0.0.1', 7781))
        r.push(Node('123456789abcdef1', '127.0.0.1', 7782))
        r.push(Node('123456789abcdef2', '127.0.0.2', 7777))
        assert r.remaining() == 2

class TestRoutingZone(object):
    def test_split_balanced(self):
        own_node = Node('ffffffffffffffffffffffffffffffffffffff01', '127.0.0.1', 7001)
        r = RoutingZone(own_node.node_id, binsize=10)
        r.add(own_node)
        r.add(Node('0fffffffffffffffffffffffffffffffffffff03', '1.2.3.4', 7003))
        r.add(Node('ffffffffffffffffffffffffffffffffffffff04', '127.0.0.1', 7004))
        r.add(Node('0fffffffffffffffffffffffffffffffffffff05', '1.2.3.4', 7005))
        r.add(Node('ffffffffffffffffffffffffffffffffffffff06', '127.0.0.1', 7006))
        r.add(Node('0fffffffffffffffffffffffffffffffffffff07', '1.2.3.4', 7007))
        r.add(Node('ffffffffffffffffffffffffffffffffffffff08', '127.0.0.1', 7008))
        r.add(Node('0fffffffffffffffffffffffffffffffffffff09', '1.2.3.4', 7009))
        r.add(Node('ffffffffffffffffffffffffffffffffffffff0a', '127.0.0.1', 7010))
        r.add(Node('0fffffffffffffffffffffffffffffffffffff0b', '1.2.3.4', 7011))
        r.add(Node('ffffffffffffffffffffffffffffffffffffff0c', '127.0.0.1', 7012))
        assert not r.is_leaf()
        assert r.max_depth() == 1
        assert len(r.children[0].get_all_nodes()) == 5
        assert len(r.children[1].get_all_nodes()) == 6
        assert own_node in r.children[1].get_all_nodes()

    def test_split_unbalanced(self):
        own_node = Node('ffffffffffffffffffffffffffffffffffffff01', '127.0.0.1', 7001)
        r = RoutingZone(own_node.node_id, binsize=1, bdepth=1)
        r.add(own_node)
        r.add(Node('00000fffffffffffffffffffffffffffffffff03', '127.0.0.1', 7003))
        r.add(Node('ffffffffffffffffffffffffffffffffffffff00', '127.0.0.1', 7000))
        r.add(Node('00000fffffffffffffffffffffffffffffffff05', '127.0.0.1', 7005))
        r.add(Node('00000fffffffffffffffffffffffffffffffff07', '127.0.0.1', 7007))
        assert not r.is_leaf()
        assert r.max_depth() == 160  # will split entire key length when 'ffff..00' added
        assert len(r.get_all_nodes()) == 3  # non matching will get discarded after first split

    def test_consolidate(self):
        own_node = Node('ffffffffffffffffffffffffffffffffffffff01', '127.0.0.1', 7001)
        r = RoutingZone(own_node.node_id, binsize=2, bdepth=1)
        r.add(own_node)
        r.add(Node('00000fffffffffffffffffffffffffffffffff03', '127.0.0.1', 7003))
        r.add(Node('ffffffffffffffffffffffffffffffffffffff00', '127.0.0.1', 7000))
        r.add(Node('00000fffffffffffffffffffffffffffffffff05', '127.0.0.1', 7005))
        r.add(Node('00000fffffffffffffffffffffffffffffffff07', '127.0.0.1', 7007))
        r.remove(Node('00000fffffffffffffffffffffffffffffffff03', '127.0.0.1', 7003))
        r.remove(Node('ffffffffffffffffffffffffffffffffffffff00', '127.0.0.1', 7000))
        r.remove(Node('00000fffffffffffffffffffffffffffffffff05', '127.0.0.1', 7005))
        r.remove(Node('00000fffffffffffffffffffffffffffffffff07', '127.0.0.1', 7007))
        assert r.is_leaf()
        assert r.max_depth() == 0
        assert len(r.get_all_nodes()) == 1
