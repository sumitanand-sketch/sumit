"""
Phi-Accrual Failure Detector and Heartbeat Manager for Vault Cluster.
"""

import time
import math
import asyncio
import logging
from typing import Dict, List, Optional, Callable, Awaitable
from collections import deque

from vault.ring import ConsistentHashRing
from vault.models import NodeStatus, NodeInfo

logger = logging.getLogger("vault.detector")


class PhiAccrualFailureDetector:
    """
    Implementation of the Phi-Accrual Failure Detector (Hayashibara et al.).
    Computes continuous suspicion level phi based on sliding window of heartbeat intervals.
    """

    def __init__(self, window_size: int = 50, min_std_dev: float = 0.1):
        self.window_size = window_size
        self.min_std_dev = min_std_dev
        self.intervals: deque = deque(maxlen=window_size)
        self.last_heartbeat_time: Optional[float] = None

    def record_heartbeat(self, timestamp: Optional[float] = None) -> None:
        now = timestamp or time.time()
        if self.last_heartbeat_time is not None:
            interval = now - self.last_heartbeat_time
            if interval > 0:
                self.intervals.append(interval)
        self.last_heartbeat_time = now

    def phi(self, timestamp: Optional[float] = None) -> float:
        """
        Computes suspicion level phi.
        phi = -log10(P(t > interval))
        Higher phi indicates higher probability that the node has failed.
        """
        if self.last_heartbeat_time is None:
            return 0.0

        now = timestamp or time.time()
        time_diff = now - self.last_heartbeat_time
        if time_diff < 0:
            return 0.0

        if len(self.intervals) < 2:
            # Baseline estimation if insufficient history
            expected_interval = 1.0
            return max(0.0, (time_diff - expected_interval) * 3.0)

        mean = sum(self.intervals) / len(self.intervals)
        variance = sum((x - mean) ** 2 for x in self.intervals) / len(self.intervals)
        std_dev = max(math.sqrt(variance), self.min_std_dev)

        # Cumulative distribution function using error function approximation
        y = (time_diff - mean) / std_dev
        # Approximation of upper tail of normal distribution
        try:
            # P(X >= t)
            p_later = 0.5 * math.erfc(y / math.sqrt(2))
            if p_later <= 1e-15:
                return 16.0
            return -math.log10(p_later)
        except Exception:
            return 16.0


class HeartbeatManager:
    """
    Cluster-wide heartbeat monitor and failure state machine.
    """

    def __init__(
        self,
        ring: ConsistentHashRing,
        clients: Dict[str, Any],
        heartbeat_interval: float = 1.0,
        suspect_phi: float = 6.0,
        dead_phi: float = 10.0,
        on_node_recovered: Optional[Callable[[str], Awaitable[None]]] = None,
        on_node_failed: Optional[Callable[[str], Awaitable[None]]] = None
    ):
        self.ring = ring
        self.clients = clients
        self.heartbeat_interval = heartbeat_interval
        self.suspect_phi = suspect_phi
        self.dead_phi = dead_phi
        self.on_node_recovered = on_node_recovered
        self.on_node_failed = on_node_failed

        self.detectors: Dict[str, PhiAccrualFailureDetector] = {}
        self.is_running = False
        self._task: Optional[asyncio.Task] = None

    def _get_detector(self, node_id: str) -> PhiAccrualFailureDetector:
        if node_id not in self.detectors:
            self.detectors[node_id] = PhiAccrualFailureDetector()
        return self.detectors[node_id]

    async def check_nodes_once(self) -> None:
        """Pings each node, updates phi detector, and transitions statuses."""
        now = time.time()
        for node in self.ring.get_all_nodes():
            if node.is_decommissioned:
                continue

            node_id = node.node_id
            client = self.clients.get(node_id)
            detector = self._get_detector(node_id)

            is_up = False
            if client:
                is_up = await client.ping()

            old_status = node.status

            if is_up:
                detector.record_heartbeat(now)
                node.last_heartbeat = now
                if old_status in (NodeStatus.SUSPECT, NodeStatus.DEAD, NodeStatus.ISOLATED):
                    node.status = NodeStatus.HEALTHY
                    logger.info(f"[FailureDetector] Node {node_id} recovered to HEALTHY")
                    if self.on_node_recovered:
                        await self.on_node_recovered(node_id)
            else:
                # Node did not respond
                phi = detector.phi(now)
                if phi >= self.dead_phi:
                    if old_status != NodeStatus.DEAD:
                        node.status = NodeStatus.DEAD
                        logger.warning(f"[FailureDetector] Node {node_id} marked DEAD (phi={phi:.2f})")
                        if self.on_node_failed:
                            await self.on_node_failed(node_id)
                elif phi >= self.suspect_phi:
                    if old_status == NodeStatus.HEALTHY:
                        node.status = NodeStatus.SUSPECT
                        logger.warning(f"[FailureDetector] Node {node_id} marked SUSPECT (phi={phi:.2f})")

    async def _heartbeat_loop(self) -> None:
        while self.is_running:
            try:
                await self.check_nodes_once()
            except Exception as e:
                logger.error(f"[FailureDetector] Error in heartbeat loop: {e}")
            await asyncio.sleep(self.heartbeat_interval)

    def start(self) -> None:
        if not self.is_running:
            self.is_running = True
            self._task = asyncio.create_task(self._heartbeat_loop())

    def stop(self) -> None:
        self.is_running = False
        if self._task:
            self._task.cancel()
            self._task = None
