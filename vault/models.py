"""
Data models and schemas for Vault distributed object storage.
"""

from enum import Enum
import time
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field

from vault.config import ReplicationPolicy


class NodeStatus(str, Enum):
    HEALTHY = "HEALTHY"
    SUSPECT = "SUSPECT"
    DEAD = "DEAD"
    RECOVERING = "RECOVERING"
    DRAINING = "DRAINING"
    ISOLATED = "ISOLATED"  # Simulating network partition


class NodeInfo(BaseModel):
    """Storage node physical and network metadata."""
    node_id: str
    host: str = "127.0.0.1"
    port: int
    rack: str = "rack-1"
    zone: str = "zone-a"
    status: NodeStatus = NodeStatus.HEALTHY
    disk_total_bytes: int = 100 * 1024 * 1024 * 1024  # 100 GB default
    disk_used_bytes: int = 0
    stored_chunks_count: int = 0
    last_heartbeat: float = Field(default_factory=time.time)
    is_decommissioned: bool = False

    @property
    def endpoint_url(self) -> str:
        return f"http://{self.host}:{self.port}"


class ChunkMetadata(BaseModel):
    """Metadata describing a single immutable data block/chunk."""
    chunk_id: str
    bucket: str
    key: str
    chunk_index: int
    size_bytes: int
    sha256: str
    crc32: int
    version_id: str
    created_at: float = Field(default_factory=time.time)
    is_corrupt: bool = False


class ObjectVersion(BaseModel):
    """A specific immutable version of an object."""
    version_id: str
    bucket: str
    key: str
    size_bytes: int
    etag: str
    content_type: str = "application/octet-stream"
    chunk_ids: List[str] = Field(default_factory=list)
    user_metadata: Dict[str, str] = Field(default_factory=dict)
    is_delete_marker: bool = False
    created_at: float = Field(default_factory=time.time)


class ObjectMetadata(BaseModel):
    """Top-level object descriptor tracking current and historical versions."""
    bucket: str
    key: str
    current_version_id: Optional[str] = None
    versions: List[ObjectVersion] = Field(default_factory=list)
    is_deleted: bool = False
    created_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)


class BucketMetadata(BaseModel):
    """Bucket descriptor with replication and versioning policies."""
    name: str
    created_at: float = Field(default_factory=time.time)
    replication_policy: ReplicationPolicy = Field(default_factory=ReplicationPolicy)
    versioning_enabled: bool = True


class HintedHandoffRecord(BaseModel):
    """Record holding a write intended for a temporarily offline node."""
    target_node_id: str
    chunk_id: str
    bucket: str
    key: str
    chunk_index: int
    version_id: str
    sha256: str
    data_hex: str
    created_at: float = Field(default_factory=time.time)
    attempts: int = 0


class MultipartUploadInfo(BaseModel):
    """Metadata for an in-progress multipart upload."""
    upload_id: str
    bucket: str
    key: str
    created_at: float = Field(default_factory=time.time)
    parts: Dict[int, ChunkMetadata] = Field(default_factory=dict)
    user_metadata: Dict[str, str] = Field(default_factory=dict)
    content_type: str = "application/octet-stream"
