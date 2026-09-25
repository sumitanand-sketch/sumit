"""
Unit and integration tests for Merkle Trees and Active Anti-Entropy.
"""

import pytest
from vault.merkle import MerkleTree


def test_merkle_tree_identical():
    tree1 = MerkleTree(depth=4)
    tree2 = MerkleTree(depth=4)

    items = {
        "k1": "hash_1111",
        "k2": "hash_2222",
        "k3": "hash_3333"
    }

    for k, v in items.items():
        tree1.insert(k, v)
        tree2.insert(k, v)

    root1 = tree1.compute_hashes()
    root2 = tree2.compute_hashes()
    assert root1 == root2

    only_a, only_b, differing = MerkleTree.compare_and_diff(tree1, tree2)
    assert len(only_a) == 0
    assert len(only_b) == 0
    assert len(differing) == 0


def test_merkle_tree_diffing():
    tree1 = MerkleTree(depth=4)
    tree2 = MerkleTree(depth=4)

    tree1.insert("k1", "hash_common")
    tree2.insert("k1", "hash_common")

    tree1.insert("k2", "hash_A")  # differing value
    tree2.insert("k2", "hash_B")

    tree1.insert("k3_only_in_1", "hash_3")  # only in tree1
    tree2.insert("k4_only_in_2", "hash_4")  # only in tree2

    only_a, only_b, differing = MerkleTree.compare_and_diff(tree1, tree2)

    assert "k3_only_in_1" in only_a
    assert "k4_only_in_2" in only_b
    assert "k2" in differing


@pytest.mark.asyncio
async def test_active_anti_entropy_healing(cluster):
    # Write an object to the cluster
    bucket = "aae-bucket"
    key = "sync-doc.txt"
    data = b"Anti-Entropy Synchronization Test Data"

    version = await cluster.coordinator.put_object(bucket=bucket, key=key, data=data)
    chunk_id = version.chunk_ids[0]

    # Artificially delete the chunk from node-3
    engine_3 = cluster.engines["node-3"]
    await engine_3.delete_chunk(chunk_id)

    # Verify node-3 is missing the chunk
    assert engine_3.get_metadata(chunk_id) is None

    # Run Anti-Entropy cycle
    report = await cluster.anti_entropy.run_anti_entropy_cycle()
    assert report["total_repairs"] >= 1

    # Verify chunk was healed on node-3
    assert engine_3.get_metadata(chunk_id) is not None
    restored_data = await engine_3.read_chunk(chunk_id)
    assert restored_data == data
