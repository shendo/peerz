from peerz.routing import distance, bit_number
from peerz.routing import RoutingBin, RoutingZone, Node

def test_distance():
    assert distance(0x1b8f0f, 0x1b8f0f) == 0
    assert distance(0x1b8f10, 0x1b8f0f) == 0x1f
    assert distance(0x1b8f0f, 0x1b8f10) == 0x1f
    assert distance(0x2b8f11, 0x1b8f10) == 0x300001
    assert distance(0x300001, 0x2b8f11) == 0x1b8f10
    assert distance(0x123456789abcdef0, 0x1234567890abcdef) == 0xa17131f
    assert distance(0x123456789abcdef0, 0xa) == 0x123456789abcdefa
    assert distance(0xffffffffffffffffffffffffffffffff, 0) == 0xffffffffffffffffffffffffffffffff
      
def test_bit_number():
    assert bit_number(0x0123456789abcdef0123456789abcdef, 0) == 0
    assert bit_number(0xf123456789abcdeff123456789abcd00, 0) == 1
    assert bit_number(0xf0000000000000000123456789ab0000, 127) == 0
    assert bit_number(0x00000000000000000000000000000001, 127) == 1


class TestRoutingBin(object):
    
    def test_get_by_id(self):
        r = RoutingBin()
        r.push(Node(0x123456789abcdef0, '127.0.0.1', 7781))
        r.push(Node(0x123456789abcdef1, '127.0.0.1', 7782))
        r.push(Node(0x123456789abcdef2, '127.0.0.2', 7777))
        
        n = r.get_by_id(0x123456789abcdef1)
        assert n.node_id == 0x123456789abcdef1
        assert n.address == '127.0.0.1'
        assert n.port == 7782
        
    def test_get_by_address(self):
        r = RoutingBin()
        r.push(Node(0x123456789abcdef0, '127.0.0.1', 7781))
        r.push(Node(0x123456789abcdef1, '127.0.0.1', 7782))
        r.push(Node(0x123456789abcdef2, '127.0.0.2', 7777))
        
        n = r.get_by_address('127.0.0.1', 7781)
        assert n.node_id == 0x123456789abcdef0
        assert n.address == '127.0.0.1'
        assert n.port == 7781
    
    def test_get_all(self):
        r = RoutingBin()
        r.push(Node(0x123456789abcdef0, '127.0.0.1', 7781))
        r.push(Node(0x123456789abcdef1, '127.0.0.1', 7782))
        r.push(Node(0x123456789abcdef2, '127.0.0.2', 7777))
        
        assert len(r.get_all()) == 3
    
    def test_get_node_ids(self):
        r = RoutingBin()
        r.push(Node(0x123456789abcdef0, '127.0.0.1', 7781))
        r.push(Node(0x123456789abcdef1, '127.0.0.1', 7782))
        r.push(Node(0x123456789abcdef2, '127.0.0.2', 7777))
        
        assert len(r.get_node_ids()) == 3
        assert 0x123456789abcdef0 in r.get_node_ids()
        assert 0x123456789abcdef1 in r.get_node_ids()
        assert 0x123456789abcdef2 in r.get_node_ids()
        
    def test_get_oldest(self):
        r = RoutingBin()
        n = Node(0x123456789abcdef0, '127.0.0.1', 7781)
        r.push(n)
        assert r.get_oldest().node_id == 0x123456789abcdef0
        
        r.push(Node(0x123456789abcdef1, '127.0.0.1', 7782))
        assert r.get_oldest().node_id == 0x123456789abcdef0
        # now update (push to end of list)
        r.update(n.node_id)
        assert r.get_oldest().node_id == 0x123456789abcdef1
        
    def test_get_closest_to(self):
        r = RoutingBin()
        n1 = Node(0x123456789abcde00, '127.0.0.1', 7781)
        n2 = Node(0x123456789abcdef1, '127.0.0.1', 7782)
        n3 = Node(0xfffffffffffffffff, '127.0.0.2', 7777)
        r.push(n1)
        r.push(n2)
        r.push(n3)
        
        nodes = r.get_closest_to(0x123456789abcdef2)
        assert len(nodes) == 1
        assert n2 in nodes
        
        nodes = r.get_closest_to(0x123456789abcdef2, 2)
        assert len(nodes) == 2
        assert n1 in nodes
        assert n2 in nodes
        
        nodes = r.get_closest_to(0x123456789abcdef2, 10)
        assert len(nodes) == 3
        
    def test_len(self):
        r = RoutingBin()
        assert not len(r)
        
        r.push(Node(0x123456789abcdef0, '127.0.0.1', 7781))
        r.push(Node(0x123456789abcdef1, '127.0.0.1', 7782))
        r.push(Node(0x123456789abcdef2, '127.0.0.2', 7777))
        assert len(r) == 3
    
    def test_remaining(self):
        r = RoutingBin(maxsize=5)
        r.push(Node(0x123456789abcdef0, '127.0.0.1', 7781))
        r.push(Node(0x123456789abcdef1, '127.0.0.1', 7782))
        r.push(Node(0x123456789abcdef2, '127.0.0.2', 7777))
        assert r.remaining() == 2

class TestRoutingZone(object):
    def test_split_balanced(self):
        own_node = Node(0xffffffffffffffffffffffffffffff01, '127.0.0.1', 7001)
        r = RoutingZone(own_node.node_id, binsize=10)
        r.add(own_node)
        r.add(Node(0x0fffffffffffffffffffffffffffff03, '1.2.3.4', 7003))
        r.add(Node(0xffffffffffffffffffffffffffffff04, '127.0.0.1', 7004))
        r.add(Node(0x0fffffffffffffffffffffffffffff05, '1.2.3.4', 7005))
        r.add(Node(0xffffffffffffffffffffffffffffff06, '127.0.0.1', 7006))
        r.add(Node(0x0fffffffffffffffffffffffffffff07, '1.2.3.4', 7007))
        r.add(Node(0xffffffffffffffffffffffffffffff08, '127.0.0.1', 7008))
        r.add(Node(0x0fffffffffffffffffffffffffffff09, '1.2.3.4', 7009))
        r.add(Node(0xffffffffffffffffffffffffffffff0a, '127.0.0.1', 7010))
        r.add(Node(0x0fffffffffffffffffffffffffffff0b, '1.2.3.4', 7011))
        r.add(Node(0xffffffffffffffffffffffffffffff0c, '127.0.0.1', 7012))
        assert not r.is_leaf()
        assert r.max_depth() == 1
        # left (0) is matching subtree, right (1) is non-matching
        assert len(r.children[0].get_all_nodes()) == 6
        assert len(r.children[1].get_all_nodes()) == 5
        assert own_node in r.children[0].get_all_nodes()
        
    def test_split_unbalanced(self):
        own_node = Node(0xffffffffffffffffffffffffffffff01, '127.0.0.1', 7001)
        r = RoutingZone(own_node.node_id, binsize=1, bdepth=1)
        r.add(own_node)
        r.add(Node(0x00000fffffffffffffffffffffffff03, '127.0.0.1', 7003))
        r.add(Node(0xffffffffffffffffffffffffffffff00, '127.0.0.1', 7000))
        r.add(Node(0x00000fffffffffffffffffffffffff05, '127.0.0.1', 7005))
        r.add(Node(0x00000fffffffffffffffffffffffff07, '127.0.0.1', 7007))
        assert not r.is_leaf()
        assert r.max_depth() == 128 # will split entire key length when 'ffff..00' added
        assert len(r.get_all_nodes()) == 3 # non matching will get discarded after first split
        