"""
Unit tests for ConsistentHashRing (virtual nodes, rack diversity, membership changes).
"""

from vault.ring import ConsistentHashRing
from vault.models import NodeInfo, NodeStatus


def test_consistent_hash_ring_preference_list():
    ring = ConsistentHashRing(vnodes_per_node=64)

    node1 = NodeInfo(node_id="node-1", host="127.0.0.1", port=9001, rack="rack-1")
    node2 = NodeInfo(node_id="node-2", host="127.0.0.1", port=9002, rack="rack-2")
    node3 = NodeInfo(node_id="node-3", host="127.0.0.1", port=9003, rack="rack-3")

    ring.add_node(node1)
    ring.add_node(node2)
    ring.add_node(node3)

    key = "documents/annual-report.pdf"
    pref = ring.get_preference_list(key, n=3)
    assert len(pref) == 3

    # All nodes must be distinct physical nodes (no duplicate vnodes)
    node_ids = [n.node_id for n in pref]
    assert len(set(node_ids)) == 3
    assert set(node_ids) == {"node-1", "node-2", "node-3"}


def test_rack_diversity():
    ring = ConsistentHashRing(vnodes_per_node=64)

    # 2 nodes on rack-1, 1 node on rack-2
    n1 = NodeInfo(node_id="node-1", host="127.0.0.1", port=9001, rack="rack-1")
    n2 = NodeInfo(node_id="node-2", host="127.0.0.1", port=9002, rack="rack-1")
    n3 = NodeInfo(node_id="node-3", host="127.0.0.1", port=9003, rack="rack-2")

    ring.add_node(n1)
    ring.add_node(n2)
    ring.add_node(n3)

    pref = ring.get_preference_list("test_key", n=2, enforce_rack_diversity=True)
    assert len(pref) == 2
    racks = [n.rack for n in pref]
    # Should pick nodes across different racks
    assert set(racks) == {"rack-1", "rack-2"}


def test_dynamic_node_removal():
    ring = ConsistentHashRing(vnodes_per_node=32)
    n1 = NodeInfo(node_id="node-1", host="127.0.0.1", port=9001)
    n2 = NodeInfo(node_id="node-2", host="127.0.0.1", port=9002)

    ring.add_node(n1)
    ring.add_node(n2)

    assert len(ring.get_all_nodes()) == 2
    ring.remove_node("node-1")
    assert len(ring.get_all_nodes()) == 1

    pref = ring.get_preference_list("any_key", n=1)
    assert pref[0].node_id == "node-2"
