"""
Vault Cluster Builder and Test Harness.
Spawns multi-node clusters with isolated storage engines and coordinator.
Supports both in-memory ASGI transport (zero-port overhead) and standard HTTP transport.
"""

import os
import shutil
import tempfile
import httpx
from typing import List, Dict, Optional, Any

from vault.models import NodeInfo, NodeStatus
from vault.ring import ConsistentHashRing
from vault.storage.engine import StorageEngine
from vault.storage.scrubber import DiskScrubber
from vault.storage.server import create_storage_node_app
from vault.cluster.node import StorageNodeClient
from vault.cluster.chaos import ChaosController
from vault.cluster.hinted_handoff import HintedHandoffManager
from vault.cluster.anti_entropy import AntiEntropyManager
from vault.cluster.rebalancer import ClusterRebalancer
from vault.cluster.failure_detector import HeartbeatManager
from vault.coordinator.metadata import MetadataCatalog
from vault.coordinator.coordinator import VaultCoordinator
from vault.gateway.app import create_gateway_app
from vault.config import ClusterConfig, ReplicationPolicy


class VaultCluster:
    """
    Complete distributed Vault cluster instance.
    Runs multiple storage nodes and coordinator gateway.
    """

    def __init__(
        self,
        node_count: int = 3,
        base_dir: Optional[str] = None,
        config: Optional[ClusterConfig] = None,
        use_in_memory_asgi: bool = True
    ):
        self.node_count = node_count
        self.config = config or ClusterConfig()
        self.use_in_memory_asgi = use_in_memory_asgi

        self._temp_dir = None
        if base_dir:
            self.base_dir = base_dir
            os.makedirs(self.base_dir, exist_ok=True)
        else:
            self._temp_dir = tempfile.mkdtemp(prefix="vault_cluster_")
            self.base_dir = self._temp_dir

        self.ring = ConsistentHashRing(vnodes_per_node=self.config.vnodes_per_node)
        self.chaos = ChaosController()
        self.metadata_catalog = MetadataCatalog()

        self.engines: Dict[str, StorageEngine] = {}
        self.scrubbers: Dict[str, DiskScrubber] = {}
        self.clients: Dict[str, StorageNodeClient] = {}
        self.node_apps: Dict[str, Any] = {}

        self.handoff_manager: Optional[HintedHandoffManager] = None
        self.anti_entropy: Optional[AntiEntropyManager] = None
        self.rebalancer: Optional[ClusterRebalancer] = None
        self.heartbeat_mgr: Optional[HeartbeatManager] = None
        self.coordinator: Optional[VaultCoordinator] = None
        self.gateway_app = None

    async def initialize(self) -> None:
        """Initializes all nodes, background managers, and coordinator."""
        racks = ["rack-1", "rack-2", "rack-3"]
        zones = ["zone-a", "zone-b"]

        for i in range(self.node_count):
            node_id = f"node-{i+1}"
            port = 9001 + i
            rack = racks[i % len(racks)]
            zone = zones[i % len(zones)]

            node_dir = os.path.join(self.base_dir, node_id)
            engine = StorageEngine(data_dir=node_dir, node_id=node_id)
            scrubber = DiskScrubber(engine=engine, interval_seconds=self.config.scrub_interval_seconds)

            node_app = create_storage_node_app(engine=engine, scrubber=scrubber)

            node_info = NodeInfo(
                node_id=node_id,
                host="127.0.0.1",
                port=port,
                rack=rack,
                zone=zone,
                status=NodeStatus.HEALTHY
            )

            client = StorageNodeClient(node_info=node_info, chaos_controller=self.chaos)
            if self.use_in_memory_asgi:
                # Direct in-process ASGI client without socket binding overhead
                transport = httpx.ASGITransport(app=node_app)
                client._client = httpx.AsyncClient(transport=transport, base_url=node_info.endpoint_url)

            self.ring.add_node(node_info)
            self.engines[node_id] = engine
            self.scrubbers[node_id] = scrubber
            self.clients[node_id] = client
            self.node_apps[node_id] = node_app

        self.handoff_manager = HintedHandoffManager(
            clients=self.clients,
            retry_interval_seconds=self.config.hinted_handoff_retry_interval_seconds
        )

        self.anti_entropy = AntiEntropyManager(
            ring=self.ring,
            clients=self.clients,
            interval_seconds=self.config.anti_entropy_interval_seconds
        )

        self.rebalancer = ClusterRebalancer(
            ring=self.ring,
            clients=self.clients,
            replication_factor=self.config.default_policy.replication_factor
        )

        self.coordinator = VaultCoordinator(
            ring=self.ring,
            clients=self.clients,
            metadata_catalog=self.metadata_catalog,
            handoff_manager=self.handoff_manager,
            config=self.config
        )

        # Wire scrubber repair callback to coordinator
        for node_id, scrubber in self.scrubbers.items():
            scrubber.repair_callback = self.coordinator.repair_chunk

        self.gateway_app = create_gateway_app(
            coordinator=self.coordinator,
            ring=self.ring,
            catalog=self.metadata_catalog,
            chaos=self.chaos,
            handoff_manager=self.handoff_manager,
            anti_entropy=self.anti_entropy,
            rebalancer=self.rebalancer,
            heartbeat_mgr=None
        )

    async def add_new_node(self, node_id: Optional[str] = None) -> NodeInfo:
        """Dynamically adds a new storage node to the running cluster."""
        idx = len(self.engines) + 1
        nid = node_id or f"node-{idx}"
        port = 9000 + idx
        node_dir = os.path.join(self.base_dir, nid)

        engine = StorageEngine(data_dir=node_dir, node_id=nid)
        scrubber = DiskScrubber(engine=engine)
        node_app = create_storage_node_app(engine=engine, scrubber=scrubber)

        node_info = NodeInfo(
            node_id=nid,
            host="127.0.0.1",
            port=port,
            rack=f"rack-{idx % 3 + 1}",
            zone="zone-dynamic",
            status=NodeStatus.HEALTHY
        )

        client = StorageNodeClient(node_info=node_info, chaos_controller=self.chaos)
        if self.use_in_memory_asgi:
            transport = httpx.ASGITransport(app=node_app)
            client._client = httpx.AsyncClient(transport=transport, base_url=node_info.endpoint_url)

        self.ring.add_node(node_info)
        self.engines[nid] = engine
        self.scrubbers[nid] = scrubber
        self.clients[nid] = client
        self.node_apps[nid] = node_app

        return node_info

    async def shutdown(self) -> None:
        """Stops background workers and cleans up temp storage."""
        if self.handoff_manager:
            self.handoff_manager.stop()
        if self.anti_entropy:
            self.anti_entropy.stop()
        for scrubber in self.scrubbers.values():
            scrubber.stop()
        for client in self.clients.values():
            await client.close()

        if self._temp_dir and os.path.exists(self._temp_dir):
            try:
                shutil.rmtree(self._temp_dir, ignore_errors=True)
            except Exception:
                pass
