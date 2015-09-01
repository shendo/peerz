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


from peerz.routing import distance, distance_sort, bit_number
from peerz.routing import RoutingBin, RoutingZone, Node

def test_distance():
    assert distance('10001', '10001') == 0
    assert distance('\x42\xf1\x00', '\x42\xf0\xff') == 0x01ff
    assert distance('\x42\xf0\xff', '\x42\xf1\x00') == 0x01ff
    assert distance('\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff', '\x00\x00\x00') == 0xffffffffffffffffffffffffffffffff

def test_bit_number():
    assert bit_number('\x00\x00\x00', 255) == 0
    assert bit_number('\x00\x00\x01', 255) == 1
    assert bit_number('\x0f\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xf1\x23\x45\x67\x89\xab\xcd\xef\x01\x23\x45\x67\x89\xab\xcd\xef\x01\x23\x45\x67', 0) == 0
    assert bit_number('\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xf1\x23\x45\x67\x89\xab\xcd\xef\xf1\x23\x45\x67\x89\xab\xcd\xef\x01\x23\x45\x00', 0) == 1
    assert bit_number('\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xf0\x00\x00\x00\x00\x00\x00\x00\x01\x23\x45\x67\x89\xab\x00\x00\x00\x00\x00\x00', 255) == 0
    assert bit_number('\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01', 255) == 1

def test_distance_sort():
    x = ['\x00\x02\x00\x00\x00', '\x00\x01\x00\x00\x00', '\x00\x04\x00\x00\x00', ]
    distance_sort(x, '\x00\x00\x00\x00\x00')
    assert x == ['\x00\x01\x00\x00\x00', '\x00\x02\x00\x00\x00', '\x00\x04\x00\x00\x00']
    distance_sort(x, '\x00\x04\x00\x00\x00')
    assert x == ['\x00\x04\x00\x00\x00', '\x00\x01\x00\x00\x00', '\x00\x02\x00\x00\x00']
    
class TestRoutingBin(object):

    def test_get_by_id(self):
        r = RoutingBin()
        r.push(Node('127.0.0.1', 7781, '12345678f0'))
        r.push(Node('127.0.0.1', 7782, '12345678f1'))
        r.push(Node('127.0.0.2', 7777, '12345678f2'))

        n = r.get_by_id('12345678f1')
        assert n.node_id == '12345678f1'
        assert n.address == '127.0.0.1'
        assert n.port == 7782

    def test_get_by_address(self):
        r = RoutingBin()
        r.push(Node('127.0.0.1', 7781, '12345678f0'))
        r.push(Node('127.0.0.1', 7782, '12345678f1'))
        r.push(Node('127.0.0.2', 7777, '12345678f2'))

        n = r.get_by_address('127.0.0.1', 7781)
        assert n.node_id == '12345678f0'
        assert n.address == '127.0.0.1'
        assert n.port == 7781

    def test_get_all(self):
        r = RoutingBin()
        r.push(Node('127.0.0.1', 7781, '12345678f0'))
        r.push(Node('127.0.0.1', 7782, '12345678f1'))
        r.push(Node('127.0.0.2', 7777, '12345678f2'))

        assert len(r.get_all()) == 3

    def test_get_node_ids(self):
        r = RoutingBin()
        r.push(Node('127.0.0.1', 7781, '12345678f0'))
        r.push(Node('127.0.0.1', 7782, '12345678f1'))
        r.push(Node('127.0.0.2', 7777, '12345678f2'))

        assert len(r.get_node_ids()) == 3
        assert '12345678f0' in r.get_node_ids()
        assert '12345678f1' in r.get_node_ids()
        assert '12345678f2' in r.get_node_ids()

    def test_get_oldest(self):
        r = RoutingBin()
        n = Node('127.0.0.1', 7781, '12345678f0')
        r.push(n)
        assert r.get_oldest().node_id == '12345678f0'

        r.push(Node('127.0.0.1', 7782, '12345678f1'))
        assert r.get_oldest().node_id == '12345678f0'
        # now update (push to end of list)
        r.update(n.node_id)
        assert r.get_oldest().node_id == '12345678f1'

    def test_get_closest_to(self):
        r = RoutingBin()
        n1 = Node('127.0.0.1', 7781, '1234567800')
        n2 = Node('127.0.0.1', 7782, '12345678f1')
        n3 = Node('127.0.0.2', 7777, 'ffffffffff')
        r.push(n1)
        r.push(n2)
        r.push(n3)

        nodes = r.get_closest_to('12345678f2')
        assert len(nodes) == 1
        assert n2 in nodes

        nodes = r.get_closest_to('12345678f2', 2)
        assert len(nodes) == 2
        assert n1 in nodes
        assert n2 in nodes

        nodes = r.get_closest_to('12345678f2', 10)
        assert len(nodes) == 3

    def test_len(self):
        r = RoutingBin()
        assert not len(r)

        r.push(Node('127.0.0.1', 7781, '12345678f0'))
        r.push(Node('127.0.0.1', 7782, '12345678f1'))
        r.push(Node('127.0.0.2', 7777, '12345678f2'))
        assert len(r) == 3

    def test_remaining(self):
        r = RoutingBin(maxsize=5)
        r.push(Node('127.0.0.1', 7781, '12345678f0'))
        r.push(Node('127.0.0.1', 7782, '12345678f1'))
        r.push(Node('127.0.0.2', 7777, '12345678f2'))
        assert r.remaining() == 2

class TestRoutingZone(object):
    def test_split_balanced(self):
        own_node = Node('127.0.0.1', 7001, '\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00')
        r = RoutingZone(own_node.node_id, binsize=10)
        r.add(own_node)
        r.add(Node('100.2.3.4', 7003, '\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x03'))
        r.add(Node('127.0.0.1', 7004, '\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x04'))
        r.add(Node('100.2.3.4', 7005, '\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x05'))
        r.add(Node('127.0.0.1', 7006, '\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x06'))
        r.add(Node('100.2.3.4', 7007, '\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x07'))
        r.add(Node('127.0.0.1', 7008, '\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x08'))
        r.add(Node('100.2.3.4', 7009, '\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x09'))
        r.add(Node('127.0.0.1', 7010, '\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x0a'))
        r.add(Node('100.2.3.4', 7011, '\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x0b'))
        r.add(Node('127.0.0.1', 7012, '\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x0c'))
        assert not r.is_leaf()
        assert r.max_depth() == 1
        assert len(r.children[0].get_all_nodes()) == 5
        assert len(r.children[1].get_all_nodes()) == 6
        assert own_node in r.children[1].get_all_nodes()

    def test_split_unbalanced(self):
        own_node = Node('127.0.0.1', 7001, '\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01')
        r = RoutingZone(own_node.node_id, binsize=1, bdepth=1)
        r.add(own_node)
        r.add(Node('127.0.0.1', 7003, '\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x03'))
        r.add(Node('127.0.0.1', 7000, '\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00'))
        r.add(Node('127.0.0.1', 7005, '\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x05'))
        r.add(Node('127.0.0.1', 7007, '\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x07'))
        assert not r.is_leaf()
        assert r.max_depth() == 256  # will split entire key length when 'ffff..00' added
        assert len(r.get_all_nodes()) == 3  # non matching will get discarded after first split

    def test_consolidate(self):
        own_node = Node('127.0.0.1', 7001, '\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01')
        r = RoutingZone(own_node.node_id, binsize=2, bdepth=1)
        r.add(own_node)
        r.add(Node('127.0.0.1', 7003, '\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x03'))
        r.add(Node('127.0.0.1', 7000, '\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00'))
        r.add(Node('127.0.0.1', 7005, '\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x05'))
        r.add(Node('127.0.0.1', 7007, '\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x07'))
        r.remove(Node('127.0.0.1', 7003, '\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x03'))
        r.remove(Node('127.0.0.1', 7000, '\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00'))
        r.remove(Node('127.0.0.1', 7005, '\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x05'))
        r.remove(Node('127.0.0.1', 7007, '\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x07'))
        assert r.is_leaf()
        assert r.max_depth() == 0
        assert len(r.get_all_nodes()) == 1
