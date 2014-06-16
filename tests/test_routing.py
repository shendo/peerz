from peerz.routing import distance, node_id, RoutingBin, Node, K

def test_distance():
    assert distance('2286f0bd-ed30-4454-aa1c-2b731bc5736c', '2286f0bd-ed30-4454-aa1c-2b731bc5736c') == 0
    assert distance('2286f0bd-ed30-4454-aa1c-2b731bc5736d', '2286f0bd-ed30-4454-aa1c-2b731bc5736c') == 1
    assert distance('2286f0bd-ed30-4454-aa1c-2b731bc5736c', '2286f0bd-ed30-4454-aa1c-2b731bc5736d') == 1
    assert distance('2286f0bd-ed30-4454-aa1c-2b731bc57370', '2286f0bd-ed30-4454-aa1c-2b731bc57360') == 0x10
    assert distance('2286f0bd-ed30-4454-aa1c-2b731bc57360', '2286f0bd-ed30-4454-aa1c-2b731bc57370') == 0x10
    assert distance('10000000-0000-0000-0000-000000000000', '20000000-0000-0000-0000-000000000000') == 63802943797675961899382738893456539648L
    assert distance('10000000-0000-0000-0000-000000000000', '01000000-0000-0000-0000-000000000000') == 22596875928343569839364720024765857792L
    
def node_id():
    assert node_id('2286f0bd-ed30-4454-aa1c-2b731bc5736c', 0) == '2286f0bd-ed30-4454-aa1c-2b731bc5736c'
    assert node_id('2286f0bd-ed30-4454-aa1c-2b731bc5736d', 1) == '2286f0bd-ed30-4454-aa1c-2b731bc5736c'
    assert node_id('2286f0bd-ed30-4454-aa1c-2b731bc5736c', 1) == '2286f0bd-ed30-4454-aa1c-2b731bc5736d'
    assert node_id('2286f0bd-ed30-4454-aa1c-2b731bc57370', 0x10) == '2286f0bd-ed30-4454-aa1c-2b731bc57360'
    assert node_id('2286f0bd-ed30-4454-aa1c-2b731bc57360', 0x10) == '2286f0bd-ed30-4454-aa1c-2b731bc57370'
    assert node_id('10000000-0000-0000-0000-000000000000', 63802943797675961899382738893456539648L) == '20000000-0000-0000-0000-000000000000'
    assert node_id('01000000-0000-0000-0000-000000000000', 22596875928343569839364720024765857792L) == '10000000-0000-0000-0000-000000000000' 
    
class TestRoutingBin(object):
    
    def test_get_by_id(self):
        r = RoutingBin()
        r.update(Node('2286f0bd-ed30-4454-aa1c-2b731bc5736c', '127.0.0.1', 7781))
        r.update(Node('00123455-ed30-4454-aa1c-2b731bc5736c', '127.0.0.1', 7782))
        r.update(Node('99999999-ed30-4454-aa1c-2b731bc5736c', '127.0.0.2', 7777))
        
        n = r.get_by_id('00123455-ed30-4454-aa1c-2b731bc5736c')
        assert n.node_id == '00123455-ed30-4454-aa1c-2b731bc5736c'
        assert n.address == '127.0.0.1'
        assert n.port == 7782
        
    def test_get_by_addr(self):
        r = RoutingBin()
        r.update(Node('2286f0bd-ed30-4454-aa1c-2b731bc5736c', '127.0.0.1', 7781))
        r.update(Node('00123455-ed30-4454-aa1c-2b731bc5736c', '127.0.0.1', 7782))
        r.update(Node('99999999-ed30-4454-aa1c-2b731bc5736c', '127.0.0.2', 7777))
        
        n = r.get_by_addr('127.0.0.1', 7781)
        assert n.node_id == '2286f0bd-ed30-4454-aa1c-2b731bc5736c'
        assert n.address == '127.0.0.1'
        assert n.port == 7781
    
    def test_get_all(self):
        r = RoutingBin()
        r.update(Node('2286f0bd-ed30-4454-aa1c-2b731bc5736c', '127.0.0.1', 7781))
        r.update(Node('00123455-ed30-4454-aa1c-2b731bc5736c', '127.0.0.1', 7782))
        r.update(Node('99999999-ed30-4454-aa1c-2b731bc5736c', '127.0.0.2', 7777))
        
        assert len(r.get_all()) == 3
    
    def test_get_node_ids(self):
        r = RoutingBin()
        r.update(Node('2286f0bd-ed30-4454-aa1c-2b731bc5736c', '127.0.0.1', 7781))
        r.update(Node('00123455-ed30-4454-aa1c-2b731bc5736c', '127.0.0.1', 7782))
        r.update(Node('99999999-ed30-4454-aa1c-2b731bc5736c', '127.0.0.2', 7777))
        
        assert len(r.get_node_ids()) == 3
        assert '2286f0bd-ed30-4454-aa1c-2b731bc5736c' in r.get_node_ids()
        assert '00123455-ed30-4454-aa1c-2b731bc5736c' in r.get_node_ids()
        assert '99999999-ed30-4454-aa1c-2b731bc5736c' in r.get_node_ids()
        
    def test_get_oldest(self):
        r = RoutingBin()
        n = r.update(Node('2286f0bd-ed30-4454-aa1c-2b731bc5736c', '127.0.0.1', 7781))
        n.last_connected = 12345678
        n = r.update(Node('00123455-ed30-4454-aa1c-2b731bc5736c', '127.0.0.1', 7782))
        n.last_connected = 5771
        n = r.update(Node('99999999-ed30-4454-aa1c-2b731bc5736c', '127.0.0.2', 7777))

        assert r.get_oldest().last_connected == 5771
    
    def test_get_closest_to(self):
        r = RoutingBin()
        n1 = r.update(Node('2286f0bd-ed30-4454-aa1c-2b731bc5736c', '127.0.0.1', 7781))
        n2 = r.update(Node('00123455-ed30-4454-aa1c-2b731bc5736c', '127.0.0.1', 7782))
        r.update(Node('99999999-9999-4454-8888-2b731bc5736c', '127.0.0.2', 7777))
    
        nodes = r.get_closest_to('00123455-ed30-4454-0000-2b731bc5736c')
        print nodes
        assert len(nodes) == 1
        assert n2 in nodes
        
        nodes = r.get_closest_to('2286f0bd-7777-4454-aa1c-2b731bc5736c', 2)
        print nodes
        assert len(nodes) == 2
        assert n1 in nodes
        assert n2 in nodes
        
        nodes = r.get_closest_to('2286f0bd-7777-4454-aa1c-2b731bc5736c', 10)
        print nodes
        assert len(nodes) == 3
        
    def test_size(self):
        r = RoutingBin()
        r.update(Node('2286f0bd-ed30-4454-aa1c-2b731bc5736c', '127.0.0.1', 7781))
        r.update(Node('00123455-ed30-4454-aa1c-2b731bc5736c', '127.0.0.1', 7782))
        r.update(Node('99999999-ed30-4454-aa1c-2b731bc5736c', '127.0.0.2', 7777))
        
        assert r.size() == 3
    
    def test_remaining(self):
        r = RoutingBin()
        r.update(Node('2286f0bd-ed30-4454-aa1c-2b731bc5736c', '127.0.0.1', 7781))
        r.update(Node('00123455-ed30-4454-aa1c-2b731bc5736c', '127.0.0.1', 7782))
        r.update(Node('99999999-ed30-4454-aa1c-2b731bc5736c', '127.0.0.2', 7777))
        
        assert r.remaining() == K - 3
    
    def test_push_to_bottom(self):
        r = RoutingBin()
        n1 = r.update(Node('2286f0bd-ed30-4454-aa1c-2b731bc5736c', '127.0.0.1', 7781))
        r.update(Node('00123455-ed30-4454-aa1c-2b731bc5736c', '127.0.0.1', 7782))
        r.update(Node('99999999-ed30-4454-aa1c-2b731bc5736c', '127.0.0.2', 7777))
        r.push_to_bottom(n1)
        assert r.get_all()[2] == n1
        
    def test_update(self):
        r = RoutingBin()
        n1 = r.update(Node('2286f0bd-ed30-4454-aa1c-2b731bc5736c', '127.0.0.1', 7781))
        n2 = r.update(Node('2286f0bd-ed30-4454-aa1c-2b731bc5736c', '127.0.0.1', 7781))
        assert n1 == n2
        