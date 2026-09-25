"""
Integration tests for quorum writes, quorum reads, chunking, and versioning.
"""

import pytest
from vault.coordinator.coordinator import QuorumWriteFailedError


@pytest.mark.asyncio
async def test_quorum_write_and_read(cluster):
    bucket = "test-quorum"
    key = "file.txt"
    payload = b"Hello Quorum Storage World!"

    version = await cluster.coordinator.put_object(bucket=bucket, key=key, data=payload)
    assert version.bucket == bucket
    assert version.key == key
    assert version.size_bytes == len(payload)

    data, read_ver, telemetry = await cluster.coordinator.get_object(bucket=bucket, key=key)
    assert data == payload
    assert read_ver.version_id == version.version_id
    assert telemetry["size_bytes"] == len(payload)


@pytest.mark.asyncio
async def test_multi_chunk_large_object(cluster):
    bucket = "large-objects"
    key = "big-payload.bin"
    # Create 2.5 MB data (splits into 3 chunks at 1MB chunk size)
    payload = b"A" * (1024 * 1024) + b"B" * (1024 * 1024) + b"C" * (512 * 1024)

    version = await cluster.coordinator.put_object(bucket=bucket, key=key, data=payload)
    assert len(version.chunk_ids) == 3

    # Retrieve and verify complete byte identity
    data, _, _ = await cluster.coordinator.get_object(bucket=bucket, key=key)
    assert len(data) == len(payload)
    assert data == payload

    # Range read test: bytes 1048570 to 1048585 (straddles chunk boundary)
    range_data, _, _ = await cluster.coordinator.get_object(
        bucket=bucket, key=key, byte_range=(1048570, 1048585)
    )
    assert range_data == payload[1048570:1048586]


@pytest.mark.asyncio
async def test_object_versioning(cluster):
    bucket = "versioned-bucket"
    key = "profile.json"

    v1_data = b'{"name": "Alice", "role": "admin"}'
    v2_data = b'{"name": "Alice", "role": "superadmin"}'

    ver1 = await cluster.coordinator.put_object(bucket=bucket, key=key, data=v1_data)
    ver2 = await cluster.coordinator.put_object(bucket=bucket, key=key, data=v2_data)

    assert ver1.version_id != ver2.version_id

    # Current read returns v2
    curr_data, curr_ver, _ = await cluster.coordinator.get_object(bucket=bucket, key=key)
    assert curr_ver.version_id == ver2.version_id
    assert curr_data == v2_data

    # Historical read using version_id returns v1
    hist_data, hist_ver, _ = await cluster.coordinator.get_object(bucket=bucket, key=key, version_id=ver1.version_id)
    assert hist_ver.version_id == ver1.version_id
    assert hist_data == v1_data
