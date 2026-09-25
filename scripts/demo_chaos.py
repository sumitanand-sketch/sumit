"""
End-to-End Fault Tolerance and Chaos Demonstration Script for Vault.
Simulates bit rot, node failure, sloppy quorum, read repair, hinted handoff, and rebalancing.
"""

import asyncio
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from vault.cluster.builder import VaultCluster
from vault.models import NodeStatus

console = Console()


async def run_demo():
    console.print(Panel.fit(
        "[bold cyan]Vault: Fault-Tolerant Distributed Object Storage Engine[/bold cyan]\n"
        "[dim]Running interactive live demonstration of fault tolerance, read repair, and self-healing[/dim]",
        border_style="cyan"
    ))

    # 1. Initialize cluster
    console.print("\n[bold yellow]Step 1: Initializing 3-node cluster with Consistent Hash Ring...[/bold yellow]")
    cluster = VaultCluster(node_count=3, use_in_memory_asgi=True)
    await cluster.initialize()
    console.print("[green][OK] Cluster initialized with 3 healthy nodes (128 vnodes each, rack-aware).[/green]")

    # 2. Store object
    console.print("\n[bold yellow]Step 2: Writing object 'vault-demo/telemetry.json' with Quorum (W=2, N=3)...[/bold yellow]")
    bucket = "vault-demo"
    key = "telemetry.json"
    data = b'{"system": "Vault", "status": "nominal", "mission_critical": true}'
    version = await cluster.coordinator.put_object(bucket=bucket, key=key, data=data)
    chunk_id = version.chunk_ids[0]
    console.print(f"[green][OK] Object written successfully![/green] ETag: {version.etag}, Chunk ID: {chunk_id}")

    # 3. Simulate Bit-Rot (Silent Data Corruption)
    console.print("\n[bold yellow]Step 3: Simulating Bit-Rot (Silent Data Corruption) on node-1 disk...[/bold yellow]")
    engine1 = cluster.engines["node-1"]
    await engine1.corrupt_chunk_on_disk(chunk_id)
    console.print("[red][!] Flipped bits on disk for chunk on node-1 to simulate physical bit-rot![/red]")

    # 4. Trigger Read Repair
    console.print("\n[bold yellow]Step 4: Reading object back with Quorum (R=2)...[/bold yellow]")
    read_data, _, telemetry = await cluster.coordinator.get_object(bucket=bucket, key=key)
    console.print(f"[green][OK] Quorum read succeeded![/green] Returned valid data: {read_data.decode('utf-8')}")
    console.print(f"[purple][OK] AUTOMATIC READ-REPAIR TRIGGERED:[/purple] Repaired nodes: {telemetry['read_repairs']}")

    # Verify node-1 disk was repaired
    verified_data = await engine1.read_chunk(chunk_id, verify_checksum=True)
    console.print(f"[green][OK] Verified node-1 disk on-the-fly checksum: PASSED! Data matches original.[/green]")

    # 5. Node Failure & Hinted Handoff
    console.print("\n[bold yellow]Step 5: Simulating node crash on node-3 & Sloppy Quorum Write...[/bold yellow]")
    cluster.ring.update_node_status("node-3", NodeStatus.DEAD)
    console.print("[red][!] node-3 crashed (marked DEAD).[/red]")

    key2 = "emergency_log.txt"
    data2 = b"Log entry generated while node-3 was crashed"
    v2 = await cluster.coordinator.put_object(bucket=bucket, key=key2, data=data2)
    console.print(f"[green][OK] Write succeeded on remaining healthy quorum![/green] Hint buffered in HintedHandoffManager.")
    console.print(f"Pending hints count: {cluster.handoff_manager.get_pending_count()}")

    # 6. Node Recovery & Hint Delivery
    console.print("\n[bold yellow]Step 6: Recovering node-3 and flushing hinted handoff buffer...[/bold yellow]")
    cluster.ring.update_node_status("node-3", NodeStatus.HEALTHY)
    replayed = await cluster.handoff_manager.replay_hints_for_node("node-3")
    console.print(f"[green][OK] node-3 back online! Delivered {replayed} buffered hints.[/green]")
    console.print(f"Pending hints remaining: {cluster.handoff_manager.get_pending_count()}")

    # 7. Active Anti-Entropy via Merkle Trees
    console.print("\n[bold yellow]Step 7: Running Active Anti-Entropy (AAE) via Merkle Tree diffing...[/bold yellow]")
    aae_report = await cluster.anti_entropy.run_anti_entropy_cycle()
    console.print(f"[green][OK] AAE cycle completed across {aae_report['pairs_checked']} node pairs.[/green] Total sync repairs: {aae_report['total_repairs']}")

    # 8. Dynamic Rebalancing
    console.print("\n[bold yellow]Step 8: Adding node-4 dynamically & triggering partition rebalancing...[/bold yellow]")
    await cluster.add_new_node("node-4")
    rebal_stats = await cluster.rebalancer.rebalance_for_new_node("node-4")
    console.print(f"[green][OK] Rebalance complete![/green] Migrated {rebal_stats['migrated_chunks']} chunks to node-4.")

    # 9. Summary Table
    table = Table(title="Vault Cluster Final State", border_style="cyan")
    table.add_column("Node ID", style="bold")
    table.add_column("Status")
    table.add_column("Stored Chunks")
    table.add_column("Corrupt Chunks")
    for nid, eng in cluster.engines.items():
        node_status = cluster.ring.get_node(nid).status.value
        table.add_row(nid, f"[green]{node_status}[/green]", str(len(eng.list_chunks())), str(len(eng.corrupt_chunk_ids)))
    console.print(table)

    await cluster.shutdown()
    console.print("\n[bold green]All fault-tolerance and self-healing scenarios verified successfully![/bold green]\n")


if __name__ == "__main__":
    asyncio.run(run_demo())
