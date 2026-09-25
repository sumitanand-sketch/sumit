"""
Tests for background disk scrubbing and silent bit-rot self-healing.
"""

import pytest


@pytest.mark.asyncio
async def test_scrubber_detection_and_repair(cluster):
    bucket = "scrub-test"
    key = "critical-asset.dat"
    data = b"Precious asset data subject to background scrubbing."

    version = await cluster.coordinator.put_object(bucket=bucket, key=key, data=data)
    chunk_id = version.chunk_ids[0]

    pref = cluster.ring.get_preference_list(chunk_id, n=3)
    target_node_id = pref[0].node_id
    engine = cluster.engines[target_node_id]
    scrubber = cluster.scrubbers[target_node_id]

    # Inject bit rot
    await engine.corrupt_chunk_on_disk(chunk_id)

    # Run scrub cycle
    report = await scrubber.run_scrub_cycle()

    assert report["chunks_scanned"] >= 1
    assert report["corrupted_detected"] >= 1
    assert report["repaired_count"] >= 1

    # Verify healed data on disk
    restored = await engine.read_chunk(chunk_id, verify_checksum=True)
    assert restored == data

    # Second scrub cycle should be completely clean
    report_clean = await scrubber.run_scrub_cycle()
    assert report_clean["corrupted_detected"] == 0
