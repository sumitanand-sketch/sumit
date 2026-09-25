"""
Vault Configuration and Policies.
"""

from typing import Dict, Any, Optional
from pydantic import BaseModel, Field


class ReplicationPolicy(BaseModel):
    """Replication and consistency policy for a bucket or object."""
    replication_factor: int = Field(default=3, ge=1, description="Number of replicas (N)")
    write_quorum: int = Field(default=2, ge=1, description="Minimum write acknowledgments required (W)")
    read_quorum: int = Field(default=2, ge=1, description="Minimum read acknowledgments required (R)")
    allow_sloppy_quorum: bool = Field(default=True, description="Enable hinted handoff when primary nodes are down")
    enable_erasure_coding: bool = Field(default=False, description="Use erasure coding instead of full replication")
    ec_data_shards: int = Field(default=4, description="K data shards for erasure coding")
    ec_parity_shards: int = Field(default=2, description="M parity shards for erasure coding")

    def validate_quorum(self) -> None:
        if self.write_quorum > self.replication_factor:
            raise ValueError(f"write_quorum ({self.write_quorum}) cannot exceed replication_factor ({self.replication_factor})")
        if self.read_quorum > self.replication_factor:
            raise ValueError(f"read_quorum ({self.read_quorum}) cannot exceed replication_factor ({self.replication_factor})")


class ClusterConfig(BaseModel):
    """Cluster-wide operational configuration."""
    cluster_id: str = "vault-cluster-01"
    vnodes_per_node: int = 128
    chunk_size_bytes: int = 1024 * 1024  # 1 MB chunking for streaming objects
    heartbeat_interval_seconds: float = 1.0
    failure_detector_threshold: float = 3.0  # Suspect if missed heartbeats exceed this window
    dead_node_threshold_seconds: float = 10.0  # Consider dead after 10 seconds of no heartbeats
    scrub_interval_seconds: float = 30.0  # Periodic disk bit-rot scrub interval
    anti_entropy_interval_seconds: float = 15.0  # Background Merkle anti-entropy sync
    hinted_handoff_retry_interval_seconds: float = 3.0
    rebalance_chunk_batch_size: int = 20
    default_policy: ReplicationPolicy = Field(default_factory=ReplicationPolicy)
