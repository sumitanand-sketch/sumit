"""
Integration test for Sloppy Quorum and Hinted Handoff.
"""

import pytest
from vault.models import NodeStatus


@pytest.mark.asyncio
async def test_hinted_handoff_lifecycle(cluster):
    # Determine which nodes will be responsible for a chunk
    test_chunk_id = "handoff-bucket:doc.txt:v1:0:hash123"
    pref = cluster.ring.get_preference_list(test_chunk_id, n=3)
    primary_target = pref[0]

    # Mark the primary target node as DEAD
    cluster.ring.update_node_status(primary_target.node_id, NodeStatus.DEAD)

    bucket = "handoff-bucket"
    key = "offline-test.txt"
    payload = b"Payload written while one node is offline"

    # Write object with sloppy quorum enabled (W=2, N=3)
    version = await cluster.coordinator.put_object(bucket=bucket, key=key, data=payload)
    chunk_id = version.chunk_ids[0]

    # Verify that a hint was recorded for primary_target
    assert cluster.handoff_manager.get_pending_count() >= 1
    assert primary_target.node_id in cluster.handoff_manager.hints

    # Verify primary target does not have the chunk yet
    target_engine = cluster.engines[primary_target.node_id]
    assert target_engine.get_metadata(chunk_id) is None

    # Bring primary target back online
    cluster.ring.update_node_status(primary_target.node_id, NodeStatus.HEALTHY)

    # Replay hints
    replayed = await cluster.handoff_manager.replay_hints_for_node(primary_target.node_id)
    assert replayed >= 1

    # Verify target now has the chunk stored and verified
    stored_data = await target_engine.read_chunk(chunk_id)
    assert stored_data == payload

    # Verify hint queue for this node is empty
    assert primary_target.node_id not in cluster.handoff_manager.hints
