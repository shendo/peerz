from peerz.routing import distance, bit_number
from peerz.routing import RoutingBin, RoutingZone, Node, K

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
        r.update(Node(0x123456789abcdef0, '127.0.0.1', 7781))
        r.update(Node(0x123456789abcdef1, '127.0.0.1', 7782))
        r.update(Node(0x123456789abcdef2, '127.0.0.2', 7777))
        
        n = r.get_by_id(0x123456789abcdef1)
        assert n.node_id == 0x123456789abcdef1
        assert n.address == '127.0.0.1'
        assert n.port == 7782
        
    def test_get_by_addr(self):
        r = RoutingBin()
        r.update(Node(0x123456789abcdef0, '127.0.0.1', 7781))
        r.update(Node(0x123456789abcdef1, '127.0.0.1', 7782))
        r.update(Node(0x123456789abcdef2, '127.0.0.2', 7777))
        
        n = r.get_by_addr('127.0.0.1', 7781)
        assert n.node_id == 0x123456789abcdef0
        assert n.address == '127.0.0.1'
        assert n.port == 7781
    
    def test_get_all(self):
        r = RoutingBin()
        r.update(Node(0x123456789abcdef0, '127.0.0.1', 7781))
        r.update(Node(0x123456789abcdef1, '127.0.0.1', 7782))
        r.update(Node(0x123456789abcdef2, '127.0.0.2', 7777))
        
        assert len(r.get_all()) == 3
    
    def test_get_node_ids(self):
        r = RoutingBin()
        r.update(Node(0x123456789abcdef0, '127.0.0.1', 7781))
        r.update(Node(0x123456789abcdef1, '127.0.0.1', 7782))
        r.update(Node(0x123456789abcdef2, '127.0.0.2', 7777))
        
        assert len(r.get_node_ids()) == 3
        assert 0x123456789abcdef0 in r.get_node_ids()
        assert 0x123456789abcdef1 in r.get_node_ids()
        assert 0x123456789abcdef2 in r.get_node_ids()
        
    def test_get_oldest(self):
        r = RoutingBin()
        n = r.update(Node(0x123456789abcdef0, '127.0.0.1', 7781))
        n.last_connected = 12345678
        n = r.update(Node(0x123456789abcdef1, '127.0.0.1', 7782))
        n.last_connected = 5771
        n = r.update(Node(0x123456789abcdef2, '127.0.0.2', 7777))

        assert r.get_oldest().last_connected == 5771
    
    def test_get_closest_to(self):
        r = RoutingBin()
        n1 = r.update(Node(0x123456789abcde00, '127.0.0.1', 7781))
        n2 = r.update(Node(0x123456789abcdef1, '127.0.0.1', 7782))
        r.update(Node(0xfffffffffffffffff, '127.0.0.2', 7777))
    
        nodes = r.get_closest_to(0x123456789abcdef2)
        assert len(nodes) == 1
        assert n2 in nodes
        
        nodes = r.get_closest_to(0x123456789abcdef2, 2)
        assert len(nodes) == 2
        assert n1 in nodes
        assert n2 in nodes
        
        nodes = r.get_closest_to(0x123456789abcdef2, 10)
        assert len(nodes) == 3
        
    def test_size(self):
        r = RoutingBin()
        r.update(Node(0x123456789abcdef0, '127.0.0.1', 7781))
        r.update(Node(0x123456789abcdef1, '127.0.0.1', 7782))
        r.update(Node(0x123456789abcdef2, '127.0.0.2', 7777))
        
        assert r.size() == 3
    
    def test_remaining(self):
        r = RoutingBin()
        r.update(Node(0x123456789abcdef0, '127.0.0.1', 7781))
        r.update(Node(0x123456789abcdef1, '127.0.0.1', 7782))
        r.update(Node(0x123456789abcdef2, '127.0.0.2', 7777))
        
        assert r.remaining() == K - 3
    
    def test_push_to_bottom(self):
        r = RoutingBin()
        n1 = r.update(Node(0x123456789abcdef0, '127.0.0.1', 7781))
        r.update(Node(0x123456789abcdef1, '127.0.0.1', 7782))
        r.update(Node(0x123456789abcdef2, '127.0.0.2', 7777))
        r.push_to_bottom(n1)
        assert r.get_all()[2] == n1
        
    def test_update(self):
        r = RoutingBin()
        n1 = r.update(Node(0x123456789abcdef2, '127.0.0.1', 7781))
        n2 = r.update(Node(0x123456789abcdef2, '127.0.0.1', 7781))
        assert n1 == n2

class TestRoutingZone(object):
    def test_split_balanced(self):
        r = RoutingZone(None, 0, 0, 0xffffffffffffffffffffffffffffff01)
        r.add(Node(0xffffffffffffffffffffffffffffff02, '127.0.0.1', 7001))
        r.add(Node(0x0fffffffffffffffffffffffffffff03, '127.0.0.1', 7002))
        r.add(Node(0xffffffffffffffffffffffffffffff04, '127.0.0.1', 7003))
        r.add(Node(0x0fffffffffffffffffffffffffffff05, '127.0.0.1', 7004))
        r.add(Node(0xffffffffffffffffffffffffffffff06, '127.0.0.1', 7005))
        r.add(Node(0x0fffffffffffffffffffffffffffff07, '127.0.0.1', 7007))
        r.add(Node(0xffffffffffffffffffffffffffffff08, '127.0.0.1', 7008))
        r.add(Node(0x0fffffffffffffffffffffffffffff09, '127.0.0.1', 7009))
        r.add(Node(0xffffffffffffffffffffffffffffff0a, '127.0.0.1', 7010))
        r.add(Node(0x0fffffffffffffffffffffffffffff0b, '127.0.0.1', 7011))
        r.add(Node(0xffffffffffffffffffffffffffffff0c, '127.0.0.1', 7012))
        assert not r.is_leaf()
        assert r.max_depth() == 1
        assert len(r.children[0].get_all_nodes()) == 5
        assert len(r.children[1].get_all_nodes()) == 5
        