"""
Active Anti-Entropy (AAE) Manager using Merkle Trees for Replica Synchronization.
"""

import time
import asyncio
import logging
from typing import Dict, List, Any, Optional, Set, Tuple

from vault.ring import ConsistentHashRing
from vault.merkle import MerkleTree, MerkleTreeNode
from vault.models import NodeStatus

logger = logging.getLogger("vault.anti_entropy")


class AntiEntropyManager:
    """
    Orchestrates continuous background anti-entropy between replica nodes.
    Uses Merkle tree diffing to detect and heal out-of-sync replicas.
    """

    def __init__(
        self,
        ring: ConsistentHashRing,
        clients: Dict[str, Any],
        interval_seconds: float = 15.0
    ):
        self.ring = ring
        self.clients = clients
        self.interval_seconds = interval_seconds
        self.is_running = False
        self._task: Optional[asyncio.Task] = None
        self.last_sync_report: Dict[str, Any] = {}

    def _reconstruct_tree_from_payload(self, payload: Dict[str, Any]) -> MerkleTree:
        """Reconstructs a MerkleTree object from remote JSON payload."""
        depth = payload.get("depth", 4)
        tree = MerkleTree(depth=depth)
        leaves = payload.get("leaves", {})
        
        def _populate(node: MerkleTreeNode):
            if node.is_leaf:
                leaf_data = leaves.get(node.prefix)
                if leaf_data:
                    node.hash = leaf_data.get("hash")
                    node.items = leaf_data.get("items", {})
                return
            if node.left:
                _populate(node.left)
            if node.right:
                _populate(node.right)

        _populate(tree.root)
        tree.compute_hashes()
        return tree

    async def synchronize_pair(self, node_a_id: str, node_b_id: str) -> Dict[str, Any]:
        """
        Synchronizes replicas between two nodes using Merkle tree diffing.
        """
        client_a = self.clients.get(node_a_id)
        client_b = self.clients.get(node_b_id)
        if not client_a or not client_b:
            return {"status": "SKIPPED", "reason": "Client unavailable"}

        try:
            tree_a_data = await client_a.get_merkle_tree()
            tree_b_data = await client_b.get_merkle_tree()
        except Exception as e:
            logger.debug(f"[AntiEntropy] Failed to retrieve trees between {node_a_id} and {node_b_id}: {e}")
            return {"status": "FAILED", "error": str(e)}

        # Fast check: are root hashes identical?
        if tree_a_data.get("root_hash") == tree_b_data.get("root_hash"):
            return {
                "status": "IN_SYNC",
                "node_a": node_a_id,
                "node_b": node_b_id,
                "root_hash": tree_a_data.get("root_hash"),
                "repaired": 0
            }

        # Diff trees to find specific diverging chunks
        tree_a = self._reconstruct_tree_from_payload(tree_a_data)
        tree_b = self._reconstruct_tree_from_payload(tree_b_data)
        only_in_a, only_in_b, differing = MerkleTree.compare_and_diff(tree_a, tree_b)

        repaired_to_b = 0
        repaired_to_a = 0

        # Push missing from A -> B
        for chunk_id in only_in_a:
            try:
                data = await client_a.read_chunk(chunk_id, verify=True)
                # Parse metadata from chunk
                # In Vault, client reads chunk bytes; we write it to target
                # We can query metadata via head or read headers
                await client_b.write_chunk(
                    chunk_id=chunk_id,
                    data=data,
                    bucket="aae_sync",
                    key=f"synced/{chunk_id}",
                    chunk_index=0,
                    version_id="aae_v1"
                )
                repaired_to_b += 1
            except Exception as e:
                logger.error(f"[AntiEntropy] Failed to sync {chunk_id} from {node_a_id} to {node_b_id}: {e}")

        # Push missing from B -> A
        for chunk_id in only_in_b:
            try:
                data = await client_b.read_chunk(chunk_id, verify=True)
                await client_a.write_chunk(
                    chunk_id=chunk_id,
                    data=data,
                    bucket="aae_sync",
                    key=f"synced/{chunk_id}",
                    chunk_index=0,
                    version_id="aae_v1"
                )
                repaired_to_a += 1
            except Exception as e:
                logger.error(f"[AntiEntropy] Failed to sync {chunk_id} from {node_b_id} to {node_a_id}: {e}")

        # Differing chunks: resolve by comparing with newest or healthy
        for chunk_id in differing:
            # Try reading from both to see which one passes checksum
            data_a = None
            data_b = None
            try:
                data_a = await client_a.read_chunk(chunk_id, verify=True)
            except Exception:
                pass

            try:
                data_b = await client_b.read_chunk(chunk_id, verify=True)
            except Exception:
                pass

            if data_a and not data_b:
                # A is healthy, heal B
                try:
                    await client_b.write_chunk(chunk_id=chunk_id, data=data_a, bucket="aae_sync", key=f"synced/{chunk_id}", chunk_index=0, version_id="aae_v1")
                    repaired_to_b += 1
                except Exception:
                    pass
            elif data_b and not data_a:
                # B is healthy, heal A
                try:
                    await client_a.write_chunk(chunk_id=chunk_id, data=data_b, bucket="aae_sync", key=f"synced/{chunk_id}", chunk_index=0, version_id="aae_v1")
                    repaired_to_a += 1
                except Exception:
                    pass

        return {
            "status": "REPAIRED",
            "node_a": node_a_id,
            "node_b": node_b_id,
            "repaired_a_to_b": repaired_to_b,
            "repaired_b_to_a": repaired_to_a,
            "differing_count": len(differing)
        }

    async def run_anti_entropy_cycle(self) -> Dict[str, Any]:
        """Runs a complete anti-entropy round among healthy nodes in the cluster."""
        start_time = time.time()
        healthy_nodes = [n.node_id for n in self.ring.get_healthy_nodes()]
        results = []
        total_repairs = 0

        # Pairwise sync between neighbor nodes on the ring
        for i in range(len(healthy_nodes)):
            for j in range(i + 1, len(healthy_nodes)):
                node_a = healthy_nodes[i]
                node_b = healthy_nodes[j]
                res = await self.synchronize_pair(node_a, node_b)
                rep = res.get("repaired", 0) + res.get("repaired_a_to_b", 0) + res.get("repaired_b_to_a", 0)
                total_repairs += rep
                results.append(res)

        report = {
            "timestamp": time.time(),
            "duration_seconds": round(time.time() - start_time, 4),
            "healthy_nodes": healthy_nodes,
            "pairs_checked": len(results),
            "total_repairs": total_repairs,
            "pair_results": results
        }
        self.last_sync_report = report
        return report

    async def _aae_loop(self) -> None:
        while self.is_running:
            try:
                await self.run_anti_entropy_cycle()
            except Exception as e:
                logger.error(f"[AntiEntropy] Error in AAE loop: {e}")
            await asyncio.sleep(self.interval_seconds)

    def start(self) -> None:
        if not self.is_running:
            self.is_running = True
            self._task = asyncio.create_task(self._aae_loop())

    def stop(self) -> None:
        self.is_running = False
        if self._task:
            self._task.cancel()
            self._task = None
