"""
Consistent Hash Ring with Virtual Nodes and Rack/Zone Diversity.
"""

import bisect
import hashlib
from typing import List, Dict, Tuple, Optional, Set
from vault.models import NodeInfo, NodeStatus


def hash_key(key: str) -> int:
    """Hashes a key string into an integer position on the ring using MD5."""
    return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)


class ConsistentHashRing:
    """
    Consistent Hash Ring supporting virtual nodes, rack diversity,
    dynamic membership, and preference list generation.
    """

    def __init__(self, vnodes_per_node: int = 128):
        self.vnodes_per_node = vnodes_per_node
        # Ring array of sorted integer hash positions
        self.ring: List[int] = []
        # Mapping from ring position -> physical node_id
        self.ring_map: Dict[int, str] = {}
        # Physical nodes dictionary: node_id -> NodeInfo
        self.nodes: Dict[str, NodeInfo] = {}

    def add_node(self, node: NodeInfo) -> None:
        """Adds a physical node and registers its virtual nodes on the ring."""
        self.nodes[node.node_id] = node
        for i in range(self.vnodes_per_node):
            vnode_key = f"{node.node_id}-vnode-{i}"
            position = hash_key(vnode_key)
            # Insert maintaining sorted order
            idx = bisect.bisect_left(self.ring, position)
            if idx == len(self.ring) or self.ring[idx] != position:
                self.ring.insert(idx, position)
                self.ring_map[position] = node.node_id

    def remove_node(self, node_id: str) -> None:
        """Removes a physical node and its virtual nodes from the ring."""
        if node_id not in self.nodes:
            return
        del self.nodes[node_id]
        
        # Filter ring and ring_map
        new_ring = []
        new_map = {}
        for pos in self.ring:
            if self.ring_map[pos] != node_id:
                new_ring.append(pos)
                new_map[pos] = self.ring_map[pos]
        self.ring = new_ring
        self.ring_map = new_map

    def update_node_status(self, node_id: str, status: NodeStatus) -> None:
        """Updates the operational status of a physical node."""
        if node_id in self.nodes:
            self.nodes[node_id].status = status

    def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Retrieves NodeInfo by node_id."""
        return self.nodes.get(node_id)

    def get_all_nodes(self) -> List[NodeInfo]:
        """Returns all registered physical nodes."""
        return list(self.nodes.values())

    def get_healthy_nodes(self) -> List[NodeInfo]:
        """Returns physical nodes that are HEALTHY."""
        return [n for n in self.nodes.values() if n.status == NodeStatus.HEALTHY]

    def get_preference_list(self, key: str, n: Optional[int] = None, enforce_rack_diversity: bool = False) -> List[NodeInfo]:
        """
        Determines the ordered preference list of physical nodes responsible for a key.
        Guarantees that distinct physical nodes are selected (no duplicate vnodes).
        Optionally enforces rack diversity when multiple racks exist.
        """
        if not self.ring:
            return []

        target_count = n if n is not None else len(self.nodes)
        target_count = min(target_count, len(self.nodes))

        key_pos = hash_key(key)
        idx = bisect.bisect_left(self.ring, key_pos)

        selected_nodes: List[NodeInfo] = []
        seen_node_ids: Set[str] = set()
        seen_racks: Set[str] = set()

        total_vnodes = len(self.ring)
        # First pass: try with rack diversity if requested
        for i in range(total_vnodes):
            current_idx = (idx + i) % total_vnodes
            pos = self.ring[current_idx]
            node_id = self.ring_map[pos]
            
            if node_id in seen_node_ids:
                continue

            node = self.nodes.get(node_id)
            if not node:
                continue

            if enforce_rack_diversity and node.rack in seen_racks and len(selected_nodes) < target_count:
                # If we have other racks available, skip for now
                continue

            selected_nodes.append(node)
            seen_node_ids.add(node_id)
            seen_racks.add(node.rack)

            if len(selected_nodes) >= target_count:
                break

        # Second pass: if rack diversity constraint could not find enough nodes, fill remainder
        if len(selected_nodes) < target_count:
            for i in range(total_vnodes):
                current_idx = (idx + i) % total_vnodes
                pos = self.ring[current_idx]
                node_id = self.ring_map[pos]
                
                if node_id in seen_node_ids:
                    continue

                node = self.nodes.get(node_id)
                if not node:
                    continue

                selected_nodes.append(node)
                seen_node_ids.add(node_id)

                if len(selected_nodes) >= target_count:
                    break

        return selected_nodes

    def get_sloppy_preference_list(
        self, key: str, n: int
    ) -> Tuple[List[NodeInfo], List[NodeInfo]]:
        """
        Returns (healthy_primary_nodes, fallback_handoff_nodes).
        If some primary nodes are DEAD or ISOLATED, provides subsequent healthy nodes
        on the ring to store hinted handoffs.
        """
        all_pref = self.get_preference_list(key, n=len(self.nodes))
        primary_targets = all_pref[:n]
        overflow_candidates = all_pref[n:]

        healthy_primaries = [node for node in primary_targets if node.status == NodeStatus.HEALTHY]
        needed_fallbacks = n - len(healthy_primaries)

        fallback_nodes: List[NodeInfo] = []
        for cand in overflow_candidates:
            if cand.status == NodeStatus.HEALTHY:
                fallback_nodes.append(cand)
                if len(fallback_nodes) >= needed_fallbacks:
                    break

        return healthy_primaries, fallback_nodes
