"""
Vault: Fault-Tolerant Distributed Object Storage System.
"""

from vault.config import ReplicationPolicy, ClusterConfig
from vault.models import NodeInfo, NodeStatus, ChunkMetadata, ObjectVersion, BucketMetadata
from vault.ring import ConsistentHashRing
from vault.storage.engine import StorageEngine
from vault.coordinator.coordinator import VaultCoordinator
from vault.cluster.builder import VaultCluster

__version__ = "1.0.0"
__all__ = [
    "ReplicationPolicy",
    "ClusterConfig",
    "NodeInfo",
    "NodeStatus",
    "ChunkMetadata",
    "ObjectVersion",
    "BucketMetadata",
    "ConsistentHashRing",
    "StorageEngine",
    "VaultCoordinator",
    "VaultCluster",
]
