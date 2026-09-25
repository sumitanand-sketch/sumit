"""
Vault Command-Line Interface (CLI).
Provides rich terminal management for cluster nodes, S3 object operations, and chaos injection.
"""

import sys
import argparse
import asyncio
import httpx
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

console = Console()
DEFAULT_ENDPOINT = "http://127.0.0.1:8000"


def format_bytes(num_bytes: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if num_bytes < 1024.0:
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.2f} PB"


async def cmd_cluster_status(endpoint: str):
    async with httpx.AsyncClient(base_url=endpoint) as client:
        try:
            resp = await client.get("/api/cluster/status")
            if resp.status_code != 200:
                console.print(f"[bold red]Failed to get cluster status:[/bold red] {resp.text}")
                return
            data = resp.json()

            table = Table(title=f"Vault Cluster Overview: {data['cluster_id']}", border_style="cyan")
            table.add_column("Property", style="bold white")
            table.add_column("Value", style="cyan")

            table.add_row("Total Storage Nodes", str(data["total_nodes"]))
            table.add_row("Healthy Nodes", f"[green]{data['healthy_nodes']}[/green]")
            table.add_row("Total Buckets", str(data["total_buckets"]))
            table.add_row("Total Stored Chunks", str(data["total_chunks_stored"]))
            table.add_row("Total Volume", format_bytes(data["total_bytes_stored"]))
            table.add_row("Total Reads / Writes", f"{data['total_reads']} / {data['total_writes']}")
            table.add_row("Automatic Read Repairs", f"[purple]{data['read_repairs_count']}[/purple]")
            table.add_row("Pending Hinted Handoffs", f"[yellow]{data['pending_hints']}[/yellow]")
            console.print(table)

            nodes_table = Table(title="Storage Node Topology", border_style="blue")
            nodes_table.add_column("Node ID", style="bold")
            nodes_table.add_column("Status")
            nodes_table.add_column("Endpoint")
            nodes_table.add_column("Topology (Rack/Zone)")
            nodes_table.add_column("Chunks (Corrupt)")
            nodes_table.add_column("Disk Usage")

            for node in data["nodes"]:
                status = node["status"]
                status_color = "green" if status == "HEALTHY" else ("red" if status == "DEAD" else "yellow")
                stats = node.get("stats", {})
                chunks_info = f"{stats.get('chunk_count', 0)} ({stats.get('corrupt_count', 0)})"
                usage_info = format_bytes(stats.get("bytes_used", 0))

                nodes_table.add_row(
                    node["node_id"],
                    f"[{status_color}]{status}[/{status_color}]",
                    f"{node['host']}:{node['port']}",
                    f"{node['rack']} / {node['zone']}",
                    chunks_info,
                    usage_info
                )
            console.print(nodes_table)

        except Exception as e:
            console.print(f"[bold red]Connection error:[/bold red] {e}")


async def cmd_put(endpoint: str, bucket: str, key: str, payload_str: str):
    async with httpx.AsyncClient(base_url=endpoint) as client:
        try:
            resp = await client.put(f"/{bucket}/{key}", content=payload_str.encode("utf-8"))
            if resp.status_code == 200:
                etag = resp.headers.get("ETag")
                v_id = resp.headers.get("x-amz-version-id")
                console.print(f"[bold green]SUCCESS:[/bold green] Put object [cyan]{bucket}/{key}[/cyan]")
                console.print(f"ETag: {etag} | Version ID: {v_id}")
            else:
                console.print(f"[bold red]PUT failed ({resp.status_code}):[/bold red] {resp.text}")
        except Exception as e:
            console.print(f"[bold red]Error:[/bold red] {e}")


async def cmd_get(endpoint: str, bucket: str, key: str):
    async with httpx.AsyncClient(base_url=endpoint) as client:
        try:
            resp = await client.get(f"/{bucket}/{key}")
            if resp.status_code == 200:
                etag = resp.headers.get("ETag")
                repairs = resp.headers.get("x-vault-repairs-performed", "0")
                console.print(f"[bold green]SUCCESS:[/bold green] Object [cyan]{bucket}/{key}[/cyan]")
                console.print(f"Size: {len(resp.content)} bytes | ETag: {etag} | Repairs: {repairs}")
                console.print("[dim]---------------- Content ----------------[/dim]")
                try:
                    console.print(resp.text)
                except Exception:
                    console.print(f"<binary data {len(resp.content)} bytes>")
            else:
                console.print(f"[bold red]GET failed ({resp.status_code}):[/bold red] {resp.text}")
        except Exception as e:
            console.print(f"[bold red]Error:[/bold red] {e}")


async def cmd_chaos(endpoint: str, action: str, node_id: str):
    async with httpx.AsyncClient(base_url=endpoint) as client:
        try:
            if action == "fail":
                resp = await client.post(f"/api/chaos/node/fail?node_id={node_id}")
            elif action == "recover":
                resp = await client.post(f"/api/chaos/node/recover?node_id={node_id}")
            elif action == "partition":
                resp = await client.post(f"/api/chaos/partition?node_ids={node_id}")
            elif action == "heal":
                resp = await client.post("/api/chaos/heal")
            else:
                console.print(f"[red]Unknown action:[/red] {action}")
                return
            console.print(Panel(str(resp.json()), title=f"Chaos: {action} {node_id or ''}"))
        except Exception as e:
            console.print(f"[bold red]Chaos command failed:[/bold red] {e}")


def main():
    parser = argparse.ArgumentParser(description="Vault Distributed Object Storage CLI")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="Gateway HTTP endpoint")

    subparsers = parser.add_subparsers(dest="command")

    # status
    subparsers.add_parser("status", help="Get cluster health and storage node stats")

    # put
    put_parser = subparsers.add_parser("put", help="Upload an object")
    put_parser.add_argument("bucket")
    put_parser.add_argument("key")
    put_parser.add_argument("payload", help="String content or payload")

    # get
    get_parser = subparsers.add_parser("get", help="Retrieve an object")
    get_parser.add_argument("bucket")
    get_parser.add_argument("key")

    # chaos
    chaos_parser = subparsers.add_parser("chaos", help="Trigger chaos fault injection")
    chaos_parser.add_argument("action", choices=["fail", "recover", "partition", "heal"])
    chaos_parser.add_argument("--node-id", default="", help="Target node ID")

    # serve
    serve_parser = subparsers.add_parser("serve", help="Run local multi-node Vault cluster & dashboard")
    serve_parser.add_argument("--port", type=int, default=8000)
    serve_parser.add_argument("--nodes", type=int, default=3)

    args = parser.parse_args()

    if args.command == "status":
        asyncio.run(cmd_cluster_status(args.endpoint))
    elif args.command == "put":
        asyncio.run(cmd_put(args.endpoint, args.bucket, args.key, args.payload))
    elif args.command == "get":
        asyncio.run(cmd_get(args.endpoint, args.bucket, args.key))
    elif args.command == "chaos":
        asyncio.run(cmd_chaos(args.endpoint, args.action, args.node_id))
    elif args.command == "serve":
        run_standalone_server(port=args.port, node_count=args.nodes)
    else:
        parser.print_help()


def run_standalone_server(port: int = 8000, node_count: int = 3):
    import uvicorn
    from vault.cluster.builder import VaultCluster

    async def _start():
        cluster = VaultCluster(node_count=node_count, use_in_memory_asgi=True)
        await cluster.initialize()
        console.print(Panel(
            f"[bold green]Vault Distributed Object Storage Cluster Started![/bold green]\n"
            f"Storage Nodes: [cyan]{node_count}[/cyan]\n"
            f"Gateway API & Web Dashboard: [bold cyan]http://127.0.0.1:{port}/dashboard[/bold cyan]\n"
            f"S3 REST API Endpoint: [bold cyan]http://127.0.0.1:{port}[/bold cyan]",
            title="Vault Server Ready"
        ))
        config = uvicorn.Config(cluster.gateway_app, host="127.0.0.1", port=port, log_level="info")
        server = uvicorn.Server(config)
        await server.serve()

    try:
        asyncio.run(_start())
    except KeyboardInterrupt:
        console.print("[yellow]Server stopped by user.[/yellow]")


if __name__ == "__main__":
    main()
