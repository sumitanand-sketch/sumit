"""
Unit tests for local StorageEngine (atomic writes, bit-rot detection, manifests).
"""

import pytest
import os
import tempfile
import shutil

from vault.storage.engine import StorageEngine, CorruptedChunkError
from vault.crypto import calculate_sha256


@pytest.fixture
def storage_engine():
    temp_dir = tempfile.mkdtemp(prefix="vault_engine_test_")
    engine = StorageEngine(data_dir=temp_dir, node_id="test-node-1")
    yield engine
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_atomic_write_and_read(storage_engine):
    data = b"Distributed resilient object chunk bytes"
    meta = await storage_engine.write_chunk(
        chunk_id="chunk-001",
        data=data,
        bucket="b1",
        key="k1",
        chunk_index=0,
        version_id="v1"
    )

    assert meta.chunk_id == "chunk-001"
    assert meta.size_bytes == len(data)
    assert meta.sha256 == calculate_sha256(data)

    read_data = await storage_engine.read_chunk("chunk-001")
    assert read_data == data


@pytest.mark.asyncio
async def test_bitrot_corruption_detection(storage_engine):
    data = b"Critical data block that must not get corrupted"
    await storage_engine.write_chunk(
        chunk_id="chunk-002",
        data=data,
        bucket="b1",
        key="k2",
        chunk_index=0,
        version_id="v1"
    )

    # Corrupt chunk bytes on disk
    corrupted = await storage_engine.corrupt_chunk_on_disk("chunk-002")
    assert corrupted is True

    # Reading should detect bit-rot and raise CorruptedChunkError
    with pytest.raises(CorruptedChunkError):
        await storage_engine.read_chunk("chunk-002", verify_checksum=True)


@pytest.mark.asyncio
async def test_delete_chunk(storage_engine):
    data = b"Temporary block"
    await storage_engine.write_chunk(
        chunk_id="chunk-003",
        data=data,
        bucket="b1",
        key="k3",
        chunk_index=0,
        version_id="v1"
    )

    assert storage_engine.get_metadata("chunk-003") is not None
    deleted = await storage_engine.delete_chunk("chunk-003")
    assert deleted is True
    assert storage_engine.get_metadata("chunk-003") is None

    with pytest.raises(FileNotFoundError):
        await storage_engine.read_chunk("chunk-003")
