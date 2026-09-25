"""
Tests for dynamic cluster membership and background rebalancing.
"""

import pytest


@pytest.mark.asyncio
async def test_dynamic_rebalance_on_node_join(cluster):
    # Store multiple objects
    for i in range(10):
        await cluster.coordinator.put_object(
            bucket="rebal-bucket",
            key=f"item-{i}.bin",
            data=f"Data payload for item {i}".encode("utf-8")
        )

    # Add node-4 dynamically
    node4 = await cluster.add_new_node("node-4")
    assert cluster.ring.get_node("node-4") is not None
    assert len(cluster.ring.get_all_nodes()) == 4

    # Run rebalance for node-4
    stats = await cluster.rebalancer.rebalance_for_new_node("node-4")
    assert stats["inspected_chunks"] > 0

    # Verify node-4 now stores chunks
    engine4 = cluster.engines["node-4"]
    assert len(engine4.list_chunks()) > 0

    # Ensure all original items remain fully readable
    for i in range(10):
        data, _, _ = await cluster.coordinator.get_object(bucket="rebal-bucket", key=f"item-{i}.bin")
        assert data == f"Data payload for item {i}".encode("utf-8")
