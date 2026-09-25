"""
Fault tolerance and chaos engineering tests: node failure, partitions, split-brain avoidance.
"""

import pytest
from vault.models import NodeStatus
from vault.coordinator.coordinator import QuorumWriteFailedError


@pytest.mark.asyncio
async def test_majority_partition_continues_minority_fails(cluster):
    bucket = "chaos-bucket"
    key = "critical-config.xml"
    data = b"<config><resilient>true</resilient></config>"

    # Isolate node-3 (simulating 2 vs 1 network partition)
    cluster.chaos.isolate_nodes(["node-3"])
    cluster.ring.update_node_status("node-3", NodeStatus.ISOLATED)

    # In majority partition (nodes 1 & 2 healthy), write should succeed with W=2
    v1 = await cluster.coordinator.put_object(bucket=bucket, key=key, data=data)
    assert v1.size_bytes == len(data)

    # Read should succeed with R=2
    read_data, _, _ = await cluster.coordinator.get_object(bucket=bucket, key=key)
    assert read_data == data

    # Now isolate node-2 as well (only node-1 remaining)
    cluster.chaos.isolate_nodes(["node-2"])
    cluster.ring.update_node_status("node-2", NodeStatus.ISOLATED)

    # Quorum write must FAIL to prevent split-brain / inconsistent writes (W=2 cannot be satisfied)
    with pytest.raises(QuorumWriteFailedError):
        await cluster.coordinator.put_object(
            bucket=bucket, key="should-fail.txt", data=b"Will fail quorum"
        )

    # Heal network partitions
    cluster.chaos.heal_partitions()
    cluster.ring.update_node_status("node-2", NodeStatus.HEALTHY)
    cluster.ring.update_node_status("node-3", NodeStatus.HEALTHY)

    # Once healed, writes succeed again
    v2 = await cluster.coordinator.put_object(
        bucket=bucket, key="should-succeed.txt", data=b"Restored quorum!"
    )
    assert v2.size_bytes == len(b"Restored quorum!")
