"""
Integration test for Silent Data Corruption detection and Automatic Read-Repair.
"""

import pytest


@pytest.mark.asyncio
async def test_automatic_read_repair_on_bitrot(cluster):
    bucket = "repair-bucket"
    key = "critical-data.bin"
    payload = b"Immutable financial record bytes that must remain intact 1234567890"

    version = await cluster.coordinator.put_object(bucket=bucket, key=key, data=payload)
    chunk_id = version.chunk_ids[0]

    # Preference nodes for this chunk
    pref = cluster.ring.get_preference_list(chunk_id, n=3)
    target_node = pref[0]

    # Inject bit-rot corruption on the first node's disk
    engine = cluster.engines[target_node.node_id]
    corrupted = await engine.corrupt_chunk_on_disk(chunk_id)
    assert corrupted is True

    # Initial repair counter
    initial_repairs = cluster.coordinator.read_repairs_count

    # Execute read on object: Coordinator will detect the corrupt replica and perform automatic read repair
    data, _, telemetry = await cluster.coordinator.get_object(bucket=bucket, key=key)
    assert data == payload

    # Verify read repair was triggered
    assert cluster.coordinator.read_repairs_count == initial_repairs + 1
    assert target_node.node_id in telemetry["read_repairs"]

    # Now verify the corrupted node has been fully healed on disk!
    healed_data = await engine.read_chunk(chunk_id, verify_checksum=True)
    assert healed_data == payload
    assert engine.get_metadata(chunk_id).is_corrupt is False
