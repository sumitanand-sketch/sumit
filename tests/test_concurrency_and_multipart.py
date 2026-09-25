"""
Tests for concurrent reads/writes, multipart uploads, and failure detector.
"""

import pytest
import asyncio
from vault.cluster.failure_detector import PhiAccrualFailureDetector


@pytest.mark.asyncio
async def test_concurrent_writes_and_reads(cluster):
    bucket = "concurrent-bucket"
    num_concurrent = 20

    async def _write_and_verify(idx: int):
        key = f"concurrent_key_{idx}.txt"
        payload = f"Payload content for concurrent worker #{idx}".encode("utf-8")
        ver = await cluster.coordinator.put_object(bucket=bucket, key=key, data=payload)
        read_bytes, _, _ = await cluster.coordinator.get_object(bucket=bucket, key=key)
        assert read_bytes == payload
        assert ver.size_bytes == len(payload)

    # Launch 20 concurrent write and read operations
    tasks = [_write_and_verify(i) for i in range(num_concurrent)]
    await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_phi_accrual_detector():
    detector = PhiAccrualFailureDetector(window_size=10)

    # Regular heartbeats every 1.0 second
    base_time = 1000.0
    for i in range(10):
        detector.record_heartbeat(base_time + i * 1.0)

    # Suspicion immediately after heartbeat should be very low
    phi_low = detector.phi(base_time + 9.5)
    assert phi_low < 2.0

    # Suspicion after 10 seconds of silence should be very high
    phi_high = detector.phi(base_time + 20.0)
    assert phi_high > 8.0
