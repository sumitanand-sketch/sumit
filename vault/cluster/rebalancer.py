"""
Dynamic Background Rebalancing for Vault.
Rebalances partitions and chunk placements when nodes join or leave.
"""

import time
import asyncio
import logging
from typing import Dict, List, Any, Optional

from vault.ring import ConsistentHashRing
from vault.models import NodeInfo, NodeStatus

logger = logging.getLogger("vault.rebalancer")


class ClusterRebalancer:
    """
    Coordinates migration of data chunks to maintain uniform distribution
    and satisfy replication policies when topology changes.
    """

    def __init__(
        self,
        ring: ConsistentHashRing,
        clients: Dict[str, Any],
        replication_factor: int = 3,
        batch_size: int = 20
    ):
        self.ring = ring
        self.clients = clients
        self.replication_factor = replication_factor
        self.batch_size = batch_size
        self.is_rebalancing = False
        self.last_rebalance_stats: Dict[str, Any] = {}

    async def rebalance_for_new_node(self, new_node_id: str) -> Dict[str, Any]:
        """
        Scans other nodes to migrate chunks that now hash to new_node_id.
        """
        start_time = time.time()
        self.is_rebalancing = True
        target_client = self.clients.get(new_node_id)
        if not target_client:
            return {"status": "FAILED", "reason": f"Node {new_node_id} client missing"}

        migrated_chunks = 0
        inspected_chunks = 0

        # Scan chunks across existing nodes
        for node in self.ring.get_all_nodes():
            if node.node_id == new_node_id or node.status != NodeStatus.HEALTHY:
                continue

            source_client = self.clients.get(node.node_id)
            if not source_client:
                continue

            try:
                tree_data = await source_client.get_merkle_tree()
                leaves = tree_data.get("leaves", {})
                for leaf in leaves.values():
                    items = leaf.get("items", {})
                    for chunk_id, sha256 in items.items():
                        inspected_chunks += 1
                        # Check preference list for this chunk on the updated ring
                        pref_nodes = self.ring.get_preference_list(chunk_id, n=self.replication_factor)
                        pref_ids = [n.node_id for n in pref_nodes]

                        if new_node_id in pref_ids:
                            # This chunk belongs on new_node!
                            try:
                                # Fetch from source and write to new node
                                data = await source_client.read_chunk(chunk_id, verify=True)
                                await target_client.write_chunk(
                                    chunk_id=chunk_id,
                                    data=data,
                                    bucket="rebalanced",
                                    key=f"rebalance/{chunk_id}",
                                    chunk_index=0,
                                    version_id="rebal_v1",
                                    expected_sha256=sha256
                                )
                                migrated_chunks += 1
                            except Exception as ex:
                                logger.error(f"[Rebalancer] Failed migrating {chunk_id}: {ex}")

            except Exception as e:
                logger.error(f"[Rebalancer] Error checking chunks on node {node.node_id}: {e}")

        self.is_rebalancing = False
        duration = round(time.time() - start_time, 4)
        stats = {
            "target_node": new_node_id,
            "inspected_chunks": inspected_chunks,
            "migrated_chunks": migrated_chunks,
            "duration_seconds": duration,
            "timestamp": time.time()
        }
        self.last_rebalance_stats = stats
        logger.info(f"[Rebalancer] Finished rebalance for {new_node_id}: {migrated_chunks} chunks migrated in {duration}s")
        return stats
