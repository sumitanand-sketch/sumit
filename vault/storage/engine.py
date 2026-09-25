"""
Storage Node Local Disk Engine.
Provides atomic writes, bit-rot detection, manifest tracking, and disk metrics.
"""

import os
import json
import shutil
import time
import asyncio
from typing import Dict, List, Optional, Set
from pathlib import Path

from vault.models import ChunkMetadata
from vault.crypto import calculate_sha256, calculate_crc32, verify_block_integrity


class CorruptedChunkError(Exception):
    """Raised when a stored block fails checksum verification (bit-rot)."""
    pass


class StorageEngine:
    """
    Manages local persistent chunk storage, manifests, and integrity checks for a single node.
    """

    def __init__(self, data_dir: str, node_id: str):
        self.node_id = node_id
        self.data_dir = Path(data_dir)
        self.chunks_dir = self.data_dir / "chunks"
        self.tmp_dir = self.data_dir / "tmp"
        self.manifest_file = self.data_dir / "manifest.json"

        # In-memory manifest of chunk_id -> ChunkMetadata
        self.chunks: Dict[str, ChunkMetadata] = {}
        # Track simulated disk errors / bit-rot occurrences
        self.corrupt_chunk_ids: Set[str] = set()

        self._lock = asyncio.Lock()
        self._init_directories()
        self._load_manifest()

    def _init_directories(self) -> None:
        self.chunks_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

    def _load_manifest(self) -> None:
        if self.manifest_file.exists():
            try:
                with open(self.manifest_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    for cid, meta_dict in data.items():
                        self.chunks[cid] = ChunkMetadata(**meta_dict)
            except Exception as e:
                # If corrupt or empty, start fresh
                self.chunks = {}

    def _save_manifest(self) -> None:
        tmp_manifest = self.data_dir / "manifest.json.tmp"
        manifest_data = {cid: meta.model_dump() for cid, meta in self.chunks.items()}
        with open(tmp_manifest, "w", encoding="utf-8") as f:
            json.dump(manifest_data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_manifest, self.manifest_file)

    def _sanitize_filename(self, name: str) -> str:
        for ch in [':', '/', '\\', '?', '*', '<', '>', '"', '|']:
            name = name.replace(ch, '_')
        return name

    def _get_chunk_path(self, chunk_id: str) -> Path:
        safe_name = self._sanitize_filename(chunk_id)
        return self.chunks_dir / f"{safe_name}.blob"

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
        """
        Atomically writes chunk data to disk using temporary staging and fsync.
        Verifies SHA-256 before committing to manifest.
        """
        computed_sha256 = calculate_sha256(data)
        if expected_sha256 and computed_sha256 != expected_sha256:
            raise ValueError(f"Checksum mismatch: expected {expected_sha256}, got {computed_sha256}")

        computed_crc32 = calculate_crc32(data)
        size_bytes = len(data)

        # Stage to temporary file
        safe_id = self._sanitize_filename(chunk_id)
        tmp_path = self.tmp_dir / f"stage_{safe_id}_{time.time_ns()}.tmp"
        with open(tmp_path, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())

        # Atomic rename to final path
        final_path = self._get_chunk_path(chunk_id)
        os.replace(tmp_path, final_path)

        metadata = ChunkMetadata(
            chunk_id=chunk_id,
            bucket=bucket,
            key=key,
            chunk_index=chunk_index,
            size_bytes=size_bytes,
            sha256=computed_sha256,
            crc32=computed_crc32,
            version_id=version_id,
            created_at=time.time(),
            is_corrupt=False
        )

        async with self._lock:
            self.chunks[chunk_id] = metadata
            if chunk_id in self.corrupt_chunk_ids:
                self.corrupt_chunk_ids.remove(chunk_id)
            self._save_manifest()

        return metadata

    async def read_chunk(self, chunk_id: str, verify_checksum: bool = True) -> bytes:
        """
        Reads chunk data from disk and verifies integrity.
        Raises CorruptedChunkError on bit-rot.
        """
        chunk_path = self._get_chunk_path(chunk_id)
        if not chunk_path.exists() or chunk_id not in self.chunks:
            raise FileNotFoundError(f"Chunk {chunk_id} not found on node {self.node_id}")

        meta = self.chunks[chunk_id]

        with open(chunk_path, "rb") as f:
            data = f.read()

        if verify_checksum:
            current_sha256 = calculate_sha256(data)
            if current_sha256 != meta.sha256:
                # Mark as corrupt
                async with self._lock:
                    meta.is_corrupt = True
                    self.corrupt_chunk_ids.add(chunk_id)
                    self._save_manifest()
                raise CorruptedChunkError(
                    f"Bit-rot detected on node {self.node_id} for chunk {chunk_id}: "
                    f"expected {meta.sha256}, actual {current_sha256}"
                )

        return data

    async def delete_chunk(self, chunk_id: str) -> bool:
        """Removes a chunk and its metadata."""
        async with self._lock:
            chunk_path = self._get_chunk_path(chunk_id)
            if chunk_path.exists():
                chunk_path.unlink()
            if chunk_id in self.chunks:
                del self.chunks[chunk_id]
            if chunk_id in self.corrupt_chunk_ids:
                self.corrupt_chunk_ids.remove(chunk_id)
            self._save_manifest()
            return True

    async def corrupt_chunk_on_disk(self, chunk_id: str) -> bool:
        """
        Fault injection: Intentionally mutates/scrambles bytes of a chunk on disk
        to simulate silent data corruption or bit-rot for testing.
        """
        chunk_path = self._get_chunk_path(chunk_id)
        if not chunk_path.exists():
            return False

        with open(chunk_path, "r+b") as f:
            content = f.read()
            f.seek(0)
            if len(content) > 0:
                # Flip the first byte
                corrupted = bytes([(content[0] ^ 0xFF)]) + content[1:]
                f.write(corrupted)
            else:
                f.write(b"CORRUPTED_BYTES")
            f.flush()
            os.fsync(f.fileno())

        self.corrupt_chunk_ids.add(chunk_id)
        return True

    def get_metadata(self, chunk_id: str) -> Optional[ChunkMetadata]:
        return self.chunks.get(chunk_id)

    def list_chunks(self) -> List[ChunkMetadata]:
        return list(self.chunks.values())

    def get_stats(self) -> Dict[str, any]:
        total_used = sum(c.size_bytes for c in self.chunks.values())
        return {
            "node_id": self.node_id,
            "chunk_count": len(self.chunks),
            "corrupt_count": len(self.corrupt_chunk_ids),
            "bytes_used": total_used
        }
