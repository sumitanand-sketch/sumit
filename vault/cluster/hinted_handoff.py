"""
Hinted Handoff Manager for Vault.
Buffers writes destined for offline or partitioned nodes and replays them upon node recovery.
"""

import time
import asyncio
import logging
from typing import Dict, List, Any, Optional
from vault.models import HintedHandoffRecord

logger = logging.getLogger("vault.handoff")


class HintedHandoffManager:
    """
    Stores and replays hinted handoffs for temporarily unavailable nodes.
    """

    def __init__(self, clients: Dict[str, Any], retry_interval_seconds: float = 3.0):
        self.clients = clients
        self.retry_interval_seconds = retry_interval_seconds
        # target_node_id -> List[HintedHandoffRecord]
        self.hints: Dict[str, List[HintedHandoffRecord]] = {}
        self.is_running = False
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    async def store_hint(
        self,
        target_node_id: str,
        chunk_id: str,
        data: bytes,
        bucket: str,
        key: str,
        chunk_index: int,
        version_id: str,
        sha256: str
    ) -> None:
        """Stores a write hint destined for target_node_id."""
        record = HintedHandoffRecord(
            target_node_id=target_node_id,
            chunk_id=chunk_id,
            bucket=bucket,
            key=key,
            chunk_index=chunk_index,
            version_id=version_id,
            sha256=sha256,
            data_hex=data.hex(),
            created_at=time.time(),
            attempts=0
        )
        async with self._lock:
            if target_node_id not in self.hints:
                self.hints[target_node_id] = []
            self.hints[target_node_id].append(record)

        logger.info(f"[HintedHandoff] Stored hint for target node {target_node_id} (chunk: {chunk_id})")

    async def replay_hints_for_node(self, node_id: str) -> int:
        """Attempts to flush buffered hints to recovered node."""
        async with self._lock:
            pending = self.hints.get(node_id, [])
            if not pending:
                return 0

        client = self.clients.get(node_id)
        if not client:
            return 0

        logger.info(f"[HintedHandoff] Replaying {len(pending)} hints for node {node_id}")
        replayed_count = 0
        remaining: List[HintedHandoffRecord] = []

        for record in pending:
            try:
                data = bytes.fromhex(record.data_hex)
                await client.write_chunk(
                    chunk_id=record.chunk_id,
                    data=data,
                    bucket=record.bucket,
                    key=record.key,
                    chunk_index=record.chunk_index,
                    version_id=record.version_id,
                    expected_sha256=record.sha256
                )
                replayed_count += 1
                logger.info(f"[HintedHandoff] Successfully delivered hint {record.chunk_id} to {node_id}")
            except Exception as e:
                logger.warning(f"[HintedHandoff] Delivery of {record.chunk_id} to {node_id} failed: {e}")
                record.attempts += 1
                remaining.append(record)

        async with self._lock:
            if remaining:
                self.hints[node_id] = remaining
            else:
                self.hints.pop(node_id, None)

        return replayed_count

    async def replay_all_pending(self) -> int:
        """Sweeps and replays all hints whose targets are currently reachable."""
        async with self._lock:
            target_ids = list(self.hints.keys())

        total_replayed = 0
        for node_id in target_ids:
            client = self.clients.get(node_id)
            if client and await client.ping():
                replayed = await self.replay_hints_for_node(node_id)
                total_replayed += replayed
        return total_replayed

    async def _replay_loop(self) -> None:
        while self.is_running:
            try:
                await self.replay_all_pending()
            except Exception as e:
                logger.error(f"[HintedHandoff] Error in replay loop: {e}")
            await asyncio.sleep(self.retry_interval_seconds)

    def start(self) -> None:
        if not self.is_running:
            self.is_running = True
            self._task = asyncio.create_task(self._replay_loop())

    def stop(self) -> None:
        self.is_running = False
        if self._task:
            self._task.cancel()
            self._task = None

    def get_pending_count(self) -> int:
        return sum(len(records) for records in self.hints.values())
