"""
Pytest fixtures for Vault distributed storage tests.
"""

import pytest
import shutil
import tempfile
import os
from vault.cluster.builder import VaultCluster
from vault.config import ClusterConfig, ReplicationPolicy


@pytest.fixture
async def cluster():
    """Provides a fresh 3-node in-memory ASGI cluster for tests."""
    temp_dir = tempfile.mkdtemp(prefix="vault_test_cluster_")
    config = ClusterConfig()
    config.default_policy = ReplicationPolicy(
        replication_factor=3,
        write_quorum=2,
        read_quorum=2,
        allow_sloppy_quorum=True
    )
    cl = VaultCluster(node_count=3, base_dir=temp_dir, config=config, use_in_memory_asgi=True)
    await cl.initialize()
    yield cl
    await cl.shutdown()
    shutil.rmtree(temp_dir, ignore_errors=True)
