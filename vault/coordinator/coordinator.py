"""
Quorum Read/Write Coordinator and Read-Repair Engine for Vault.
"""

import uuid
import time
import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Any

from vault.config import ReplicationPolicy, ClusterConfig
from vault.models import (
    NodeInfo,
    NodeStatus,
    ChunkMetadata,
    ObjectVersion,
    BucketMetadata,
)
from vault.ring import ConsistentHashRing
from vault.crypto import calculate_sha256, calculate_etag, calculate_crc32
from vault.coordinator.metadata import MetadataCatalog
from vault.cluster.hinted_handoff import HintedHandoffManager
from vault.erasure import ErasureCoder

logger = logging.getLogger("vault.coordinator")


class QuorumWriteFailedError(Exception):
    """Raised when write quorum (W) cannot be satisfied."""
    pass


class ObjectUnavailableError(Exception):
    """Raised when read quorum (R) cannot be satisfied."""
    pass


class VaultCoordinator:
    """
    Coordinates distributed chunk placement, quorum writes, quorum reads,
    and automatic read-repair across the storage cluster.
    """

    def __init__(
        self,
        ring: ConsistentHashRing,
        clients: Dict[str, Any],
        metadata_catalog: MetadataCatalog,
        handoff_manager: HintedHandoffManager,
        config: Optional[ClusterConfig] = None
    ):
        self.ring = ring
        self.clients = clients
        self.catalog = metadata_catalog
        self.handoff_manager = handoff_manager
        self.config = config or ClusterConfig()
        self.read_repairs_count = 0
        self.total_reads = 0
        self.total_writes = 0

    async def put_object(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        user_metadata: Optional[Dict[str, str]] = None,
        policy: Optional[ReplicationPolicy] = None
    ) -> ObjectVersion:
        """
        Stores an object with quorum replication or erasure coding.
        Chunks data, writes to replica nodes in parallel, and checks quorum.
        """
        self.total_writes += 1
        bucket_meta = await self.catalog.get_bucket(bucket)
        effective_policy = policy or (bucket_meta.replication_policy if bucket_meta else self.config.default_policy)
        effective_policy.validate_quorum()

        version_id = f"v-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"
        chunk_size = self.config.chunk_size_bytes
        total_len = len(data)

        # Split data into chunks
        chunks_data: List[bytes] = []
        if total_len == 0:
            chunks_data = [b""]
        else:
            for offset in range(0, total_len, chunk_size):
                chunks_data.append(data[offset:offset + chunk_size])

        chunk_ids: List[str] = []

        # Write each chunk with quorum replication
        safe_key = key.replace("/", "_").replace(":", "_")
        for idx, chunk_bytes in enumerate(chunks_data):
            chunk_hash = calculate_sha256(chunk_bytes)
            chunk_id = f"{bucket}_{safe_key}_{version_id}_{idx}_{chunk_hash[:12]}"
            chunk_ids.append(chunk_id)

            await self._write_chunk_quorum(
                chunk_id=chunk_id,
                chunk_bytes=chunk_bytes,
                bucket=bucket,
                key=key,
                chunk_index=idx,
                version_id=version_id,
                chunk_hash=chunk_hash,
                policy=effective_policy
            )

        etag = calculate_etag(data)
        object_version = await self.catalog.commit_object_version(
            bucket=bucket,
            key=key,
            version_id=version_id,
            size_bytes=total_len,
            etag=etag,
            content_type=content_type,
            chunk_ids=chunk_ids,
            user_metadata=user_metadata
        )

        return object_version

    async def _write_chunk_quorum(
        self,
        chunk_id: str,
        chunk_bytes: bytes,
        bucket: str,
        key: str,
        chunk_index: int,
        version_id: str,
        chunk_hash: str,
        policy: ReplicationPolicy
    ) -> None:
        """Writes a single chunk to N nodes, requiring W successful acks."""
        n = policy.replication_factor
        w = policy.write_quorum

        # Get preference list
        preference_nodes = self.ring.get_preference_list(chunk_id, n=n)
        if len(preference_nodes) < n and len(self.ring.get_all_nodes()) >= n:
            preference_nodes = self.ring.get_preference_list(chunk_id, n=n)

        # Identify healthy vs unavailable primaries
        healthy_primaries = [node for node in preference_nodes if node.status == NodeStatus.HEALTHY]
        offline_primaries = [node for node in preference_nodes if node.status != NodeStatus.HEALTHY]

        target_nodes = list(healthy_primaries)
        handoff_targets = []

        # If sloppy quorum is permitted, select fallback nodes
        if policy.allow_sloppy_quorum and offline_primaries:
            all_healthy = self.ring.get_healthy_nodes()
            candidates = [n for n in all_healthy if n not in healthy_primaries]
            for candidate in candidates[:len(offline_primaries)]:
                target_nodes.append(candidate)
                handoff_targets.append(candidate)

        async def _write_node(node: NodeInfo) -> Tuple[str, bool, Optional[str]]:
            client = self.clients.get(node.node_id)
            if not client:
                return node.node_id, False, "Client not found"
            try:
                await client.write_chunk(
                    chunk_id=chunk_id,
                    data=chunk_bytes,
                    bucket=bucket,
                    key=key,
                    chunk_index=chunk_index,
                    version_id=version_id,
                    expected_sha256=chunk_hash
                )
                return node.node_id, True, None
            except Exception as e:
                logger.error(f"[Coordinator] Write to node {node.node_id} failed with error: {type(e).__name__}: {e}")
                return node.node_id, False, str(e)

        tasks = [_write_node(node) for node in target_nodes]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        successful_nodes = [node_id for node_id, ok, _ in results if ok]
        failed_nodes = [node_id for node_id, ok, err in results if not ok]

        # Record hints for offline primary nodes if written to fallbacks or if primary failed
        for offline_node in offline_primaries:
            await self.handoff_manager.store_hint(
                target_node_id=offline_node.node_id,
                chunk_id=chunk_id,
                data=chunk_bytes,
                bucket=bucket,
                key=key,
                chunk_index=chunk_index,
                version_id=version_id,
                sha256=chunk_hash
            )

        # Check write quorum W
        if len(successful_nodes) < w:
            logger.error(
                f"[Coordinator] Quorum write failed for {chunk_id}: "
                f"Required {w}, achieved {len(successful_nodes)}. Failed: {failed_nodes}"
            )
            raise QuorumWriteFailedError(
                f"Quorum write failed for chunk {chunk_id}: "
                f"Required {w} acks, only received {len(successful_nodes)} from {successful_nodes}"
            )

    async def get_object(
        self,
        bucket: str,
        key: str,
        version_id: Optional[str] = None,
        byte_range: Optional[Tuple[int, int]] = None
    ) -> Tuple[bytes, ObjectVersion, Dict[str, Any]]:
        """
        Retrieves object with quorum read and automatic read repair.
        Returns (data_bytes, object_version, telemetry).
        """
        self.total_reads += 1
        meta = await self.catalog.get_object_metadata(bucket, key, version_id=version_id)
        if not meta:
            raise FileNotFoundError(f"Object {bucket}/{key} not found")

        bucket_meta = await self.catalog.get_bucket(bucket)
        policy = bucket_meta.replication_policy if bucket_meta else self.config.default_policy

        chunks_data: List[bytes] = []
        repair_events = []

        for chunk_id in meta.chunk_ids:
            chunk_bytes, repairs = await self._read_chunk_with_repair(chunk_id, policy)
            chunks_data.append(chunk_bytes)
            if repairs:
                repair_events.extend(repairs)

        full_data = b"".join(chunks_data)

        # Apply byte range if requested (e.g. range=0-499)
        if byte_range:
            start, end = byte_range
            end = min(end, len(full_data) - 1)
            full_data = full_data[start:end + 1]

        telemetry = {
            "version_id": meta.version_id,
            "size_bytes": len(full_data),
            "chunks_count": len(meta.chunk_ids),
            "read_repairs": repair_events,
            "etag": meta.etag
        }
        return full_data, meta, telemetry

    async def _read_chunk_with_repair(
        self, chunk_id: str, policy: ReplicationPolicy
    ) -> Tuple[bytes, List[str]]:
        """
        Reads chunk from replica nodes with quorum R.
        If a replica returns corrupt or missing chunk, automatically triggers read-repair.
        """
        n = policy.replication_factor
        r = policy.read_quorum
        pref_nodes = self.ring.get_preference_list(chunk_id, n=n)

        async def _read_from_node(node: NodeInfo) -> Tuple[str, Optional[bytes], Optional[str]]:
            client = self.clients.get(node.node_id)
            if not client:
                return node.node_id, None, "Client unavailable"
            try:
                data = await client.read_chunk(chunk_id, verify=True)
                return node.node_id, data, None
            except Exception as e:
                return node.node_id, None, str(e)

        tasks = [_read_from_node(node) for node in pref_nodes]
        responses = await asyncio.gather(*tasks, return_exceptions=False)

        valid_data: Optional[bytes] = None
        healthy_nodes: List[str] = []
        corrupted_or_missing_nodes: List[str] = []

        for node_id, data, err in responses:
            if data is not None:
                healthy_nodes.append(node_id)
                if valid_data is None:
                    valid_data = data
            else:
                corrupted_or_missing_nodes.append(node_id)

        # Verify read quorum R
        if len(healthy_nodes) < r or valid_data is None:
            raise ObjectUnavailableError(
                f"Read quorum failed for chunk {chunk_id}: "
                f"Required {r}, achieved {len(healthy_nodes)} healthy replicas."
            )

        # Perform Automatic Read Repair on damaged/missing replicas
        repairs_performed = []
        if corrupted_or_missing_nodes and valid_data is not None:
            for damaged_node_id in corrupted_or_missing_nodes:
                client = self.clients.get(damaged_node_id)
                if client:
                    try:
                        # Write the verified healthy chunk to the damaged replica
                        await client.write_chunk(
                            chunk_id=chunk_id,
                            data=valid_data,
                            bucket="read_repair",
                            key=f"repair/{chunk_id}",
                            chunk_index=0,
                            version_id="repair_v1"
                        )
                        self.read_repairs_count += 1
                        repairs_performed.append(damaged_node_id)
                        logger.info(f"[ReadRepair] Successfully repaired chunk {chunk_id} on node {damaged_node_id}")
                    except Exception as rep_err:
                        logger.warning(f"[ReadRepair] Repair failed for {damaged_node_id}: {rep_err}")

        return valid_data, repairs_performed

    async def delete_object(self, bucket: str, key: str) -> ObjectVersion:
        """Deletes object by placing a delete marker."""
        version_id = f"del-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"
        del_version = await self.catalog.commit_object_version(
            bucket=bucket,
            key=key,
            version_id=version_id,
            size_bytes=0,
            etag='""',
            content_type="application/octet-stream",
            chunk_ids=[],
            is_delete_marker=True
        )
        return del_version

    async def repair_chunk(self, chunk_id: str, bucket: str, key: str) -> bool:
        """Explicit repair helper for scrubbers."""
        pref_nodes = self.ring.get_preference_list(chunk_id, n=self.config.default_policy.replication_factor)
        # Find healthy data
        healthy_data = None
        for node in pref_nodes:
            client = self.clients.get(node.node_id)
            if client:
                try:
                    healthy_data = await client.read_chunk(chunk_id, verify=True)
                    break
                except Exception:
                    continue

        if not healthy_data:
            return False

        # Re-write to all replicas in preference list
        for node in pref_nodes:
            client = self.clients.get(node.node_id)
            if client:
                try:
                    await client.write_chunk(
                        chunk_id=chunk_id,
                        data=healthy_data,
                        bucket=bucket,
                        key=key,
                        chunk_index=0,
                        version_id="scrub_repair"
                    )
                except Exception:
                    pass
        return True
