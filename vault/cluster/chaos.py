"""
Chaos Engineering Controller for Vault.
Simulates node crashes, network partitions, disk corruptions, and network latency.
"""

from typing import Set, Dict, List, Optional
import asyncio
import logging

from vault.models import NodeStatus

logger = logging.getLogger("vault.chaos")


class ChaosController:
    """
    Manages active chaos experiments, simulated network partitions,
    and storage faults across the cluster.
    """

    def __init__(self):
        # Set of isolated node IDs that cannot communicate
        self.isolated_nodes: Set[str] = set()
        # Network partition splits: list of sets, e.g. [{node1, node2}, {node3}]
        self.partition_groups: List[Set[str]] = []
        # Node-specific artificial latency in milliseconds
        self.node_latency_ms: Dict[str, float] = {}

    def isolate_nodes(self, node_ids: List[str]) -> None:
        """Simulates network partition isolating given nodes from the rest of cluster."""
        for nid in node_ids:
            self.isolated_nodes.add(nid)
        logger.warning(f"[Chaos] Isolated nodes from network: {node_ids}")

    def create_partition(self, group_a: List[str], group_b: List[str]) -> None:
        """Simulates a split-brain / network partition dividing nodes into two disjoint sets."""
        self.partition_groups = [set(group_a), set(group_b)]
        logger.warning(f"[Chaos] Created network partition: Group A={group_a} vs Group B={group_b}")

    def heal_partitions(self) -> None:
        """Heals all network partitions and restores full network connectivity."""
        self.isolated_nodes.clear()
        self.partition_groups.clear()
        logger.info("[Chaos] All network partitions healed")

    def is_node_partitioned(self, node_id: str) -> bool:
        """Returns True if the node is in the isolated list."""
        return node_id in self.isolated_nodes

    def can_communicate(self, node_a: str, node_b: str) -> bool:
        """Checks if two nodes can communicate across partition boundaries."""
        if node_a in self.isolated_nodes or node_b in self.isolated_nodes:
            return False

        if self.partition_groups:
            # If both belong to different groups, communication is blocked
            group_a_idx = None
            group_b_idx = None
            for idx, grp in enumerate(self.partition_groups):
                if node_a in grp:
                    group_a_idx = idx
                if node_b in grp:
                    group_b_idx = idx
            if group_a_idx is not None and group_b_idx is not None and group_a_idx != group_b_idx:
                return False

        return True

    def set_latency(self, node_id: str, latency_ms: float) -> None:
        """Injects network latency for requests to a node."""
        self.node_latency_ms[node_id] = latency_ms

    async def apply_latency(self, node_id: str) -> None:
        """Applies injected latency if configured."""
        delay_ms = self.node_latency_ms.get(node_id, 0.0)
        if delay_ms > 0:
            await asyncio.sleep(delay_ms / 1000.0)
