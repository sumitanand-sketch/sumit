"""
Storage Node RPC Client and Network Simulation Wrapper.
Supports network partitions, drop rules, and latency injection.
"""

import httpx
import asyncio
from typing import Optional, Dict, Any
from vault.models import NodeInfo, NodeStatus, ChunkMetadata


class StorageNodeClient:
    """
    Asynchronous client for interacting with a Storage Node server.
    Integrates with the Chaos simulator to enforce simulated network partitions.
    """

    def __init__(self, node_info: NodeInfo, chaos_controller=None, timeout: float = 3.0):
        self.node_info = node_info
        self.chaos_controller = chaos_controller
        self.timeout = timeout
        self.base_url = node_info.endpoint_url
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout)
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    def _check_network_allowed(self) -> None:
        """Checks if this node is reachable or isolated by a simulated partition."""
        if self.node_info.status in (NodeStatus.DEAD, NodeStatus.ISOLATED):
            raise httpx.ConnectError(f"Node {self.node_info.node_id} is unreachable (status: {self.node_info.status.value})")

        if self.chaos_controller and self.chaos_controller.is_node_partitioned(self.node_info.node_id):
            raise httpx.ConnectError(f"Simulated network partition: Node {self.node_info.node_id} is isolated")

    async def ping(self) -> bool:
        """Checks if the storage node responds to healthcheck."""
        try:
            self._check_network_allowed()
            client = await self._get_client()
            resp = await client.get("/health")
            return resp.status_code == 200
        except Exception:
            return False

    async def write_chunk(
        self,
        chunk_id: str,
        data: bytes,
        bucket: str,
        key: str,
        chunk_index: int,
        version_id: str,
        expected_sha256: Optional[str] = None
    ) -> ChunkMetadata:
        """Uploads a chunk to the storage node."""
        self._check_network_allowed()
        client = await self._get_client()
        headers = {
            "x-bucket": bucket,
            "x-key": key,
            "x-chunk-index": str(chunk_index),
            "x-version-id": version_id,
        }
        if expected_sha256:
            headers["x-sha256"] = expected_sha256

        resp = await client.post(f"/chunks/{chunk_id}", content=data, headers=headers)
        if resp.status_code != 200:
            raise RuntimeError(f"Write failed on node {self.node_info.node_id}: {resp.status_code} {resp.text}")
        return ChunkMetadata(**resp.json())

    async def read_chunk(self, chunk_id: str, verify: bool = True) -> bytes:
        """Retrieves chunk bytes from the storage node with integrity checking."""
        self._check_network_allowed()
        client = await self._get_client()
        resp = await client.get(f"/chunks/{chunk_id}", params={"verify": str(verify).lower()})
        if resp.status_code == 200:
            return resp.content
        elif "BIT_ROT_DETECTED" in resp.text:
            raise ValueError(f"CorruptedChunkError: Bit rot detected on {self.node_info.node_id} for chunk {chunk_id}")
        elif resp.status_code == 404:
            raise FileNotFoundError(f"Chunk {chunk_id} not found on node {self.node_info.node_id}")
        else:
            raise RuntimeError(f"Read failed on node {self.node_info.node_id}: {resp.status_code} {resp.text}")

    async def delete_chunk(self, chunk_id: str) -> bool:
        """Deletes chunk from storage node."""
        self._check_network_allowed()
        client = await self._get_client()
        resp = await client.delete(f"/chunks/{chunk_id}")
        return resp.status_code == 200

    async def get_merkle_tree(self) -> Dict[str, Any]:
        """Fetches Merkle tree from node."""
        self._check_network_allowed()
        client = await self._get_client()
        resp = await client.post("/merkle")
        if resp.status_code == 200:
            return resp.json()
        raise RuntimeError(f"Merkle fetch failed: {resp.text}")

    async def corrupt_chunk(self, chunk_id: str) -> bool:
        """Instructs node to corrupt chunk on disk (chaos hook)."""
        self._check_network_allowed()
        client = await self._get_client()
        resp = await client.post(f"/chaos/corrupt/{chunk_id}")
        return resp.status_code == 200

    async def trigger_scrub(self) -> Dict[str, Any]:
        """Triggers immediate disk scrub on node."""
        self._check_network_allowed()
        client = await self._get_client()
        resp = await client.post("/admin/scrub")
        return resp.json()
