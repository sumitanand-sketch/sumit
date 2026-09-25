"""
Background Disk Scrubber for Silent Data Corruption (Bit-Rot) Detection and Repair.
"""

import time
import logging
import asyncio
from typing import Dict, Any, Optional, Callable, Awaitable

from vault.storage.engine import StorageEngine, CorruptedChunkError
from vault.crypto import calculate_sha256

logger = logging.getLogger("vault.scrubber")


class DiskScrubber:
    """
    Background worker that continuously or periodically audits stored chunks on disk
    to detect bit-rot and silent disk degradation.
    """

    def __init__(
        self,
        engine: StorageEngine,
        interval_seconds: float = 30.0,
        repair_callback: Optional[Callable[[str, str, str], Awaitable[bool]]] = None
    ):
        self.engine = engine
        self.interval_seconds = interval_seconds
        # repair_callback(chunk_id, bucket, key) -> bool
        self.repair_callback = repair_callback
        self.is_running = False
        self._task: Optional[asyncio.Task] = None
        self.last_report: Dict[str, Any] = {}

    async def run_scrub_cycle(self) -> Dict[str, Any]:
        """
        Executes a complete disk scrub pass across all local chunks.
        """
        start_time = time.time()
        chunks = self.engine.list_chunks()
        scanned = 0
        corrupted = 0
        repaired = 0

        for chunk_meta in chunks:
            chunk_id = chunk_meta.chunk_id
            scanned += 1
            try:
                # Read and verify checksum
                await self.engine.read_chunk(chunk_id, verify_checksum=True)
            except CorruptedChunkError as e:
                corrupted += 1
                logger.warning(f"[Scrubber] Node {self.engine.node_id}: {str(e)}")
                # Attempt proactive healing via peer nodes if callback registered
                if self.repair_callback:
                    try:
                        success = await self.repair_callback(chunk_id, chunk_meta.bucket, chunk_meta.key)
                        if success:
                            repaired += 1
                            logger.info(f"[Scrubber] Node {self.engine.node_id}: Successfully repaired chunk {chunk_id}")
                    except Exception as rep_err:
                        logger.error(f"[Scrubber] Node {self.engine.node_id}: Repair failed for {chunk_id}: {rep_err}")
            except Exception as ex:
                logger.error(f"[Scrubber] Unexpected error scrubbing {chunk_id}: {ex}")

        duration = time.time() - start_time
        report = {
            "node_id": self.engine.node_id,
            "timestamp": time.time(),
            "duration_seconds": round(duration, 4),
            "chunks_scanned": scanned,
            "corrupted_detected": corrupted,
            "repaired_count": repaired
        }
        self.last_report = report
        return report

    async def _scrub_loop(self) -> None:
        while self.is_running:
            try:
                await self.run_scrub_cycle()
            except Exception as e:
                logger.error(f"[Scrubber] Error during scrub cycle: {e}")
            await asyncio.sleep(self.interval_seconds)

    def start(self) -> None:
        if not self.is_running:
            self.is_running = True
            self._task = asyncio.create_task(self._scrub_loop())

    def stop(self) -> None:
        self.is_running = False
        if self._task:
            self._task.cancel()
            self._task = None
