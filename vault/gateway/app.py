"""
Vault Gateway Application: S3-Compatible REST API, Cluster Management, and Chaos Hooks.
"""

from fastapi import FastAPI, HTTPException, Request, Response, Header, Query, UploadFile, File
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import time
import io
from typing import Optional, List, Dict, Any

from vault.coordinator.coordinator import VaultCoordinator, QuorumWriteFailedError, ObjectUnavailableError
from vault.coordinator.metadata import MetadataCatalog
from vault.ring import ConsistentHashRing
from vault.cluster.chaos import ChaosController
from vault.cluster.hinted_handoff import HintedHandoffManager
from vault.cluster.anti_entropy import AntiEntropyManager
from vault.cluster.rebalancer import ClusterRebalancer
from vault.cluster.failure_detector import HeartbeatManager
from vault.config import ReplicationPolicy, ClusterConfig
from vault.models import NodeStatus, NodeInfo


def create_gateway_app(
    coordinator: VaultCoordinator,
    ring: ConsistentHashRing,
    catalog: MetadataCatalog,
    chaos: ChaosController,
    handoff_manager: HintedHandoffManager,
    anti_entropy: AntiEntropyManager,
    rebalancer: ClusterRebalancer,
    heartbeat_mgr: Optional[HeartbeatManager] = None
) -> FastAPI:
    app = FastAPI(title="Vault Distributed Object Storage Gateway")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # -------------------------------------------------------------
    # Interactive Web Dashboard & Static API Routes
    # -------------------------------------------------------------

    @app.get("/dashboard", response_class=HTMLResponse)
    async def serve_dashboard():
        return HTMLResponse(content=DASHBOARD_HTML)

    @app.get("/api/buckets")
    @app.get("/")
    async def list_buckets(request: Request):
        if "text/html" in request.headers.get("accept", "") and request.url.path == "/":
            return await serve_dashboard()
        buckets = await catalog.list_buckets()
        return {"buckets": [b.model_dump() for b in buckets]}

    @app.get("/api/cluster/status")
    async def get_cluster_status():
        nodes = ring.get_all_nodes()
        total_chunks = 0
        total_bytes = 0
        nodes_status = []
        for n in nodes:
            client = coordinator.clients.get(n.node_id)
            stats = {}
            if client and n.status == NodeStatus.HEALTHY and not chaos.is_node_partitioned(n.node_id):
                try:
                    c = await client._get_client()
                    resp = await c.get("/stats")
                    if resp.status_code == 200:
                        stats = resp.json()
                        total_chunks += stats.get("chunk_count", 0)
                        total_bytes += stats.get("bytes_used", 0)
                except Exception:
                    pass

            effective_status = n.status.value
            if chaos.is_node_partitioned(n.node_id):
                effective_status = "ISOLATED"

            nodes_status.append({
                "node_id": n.node_id,
                "host": n.host,
                "port": n.port,
                "rack": n.rack,
                "zone": n.zone,
                "status": effective_status,
                "stats": stats
            })

        buckets = await catalog.list_buckets()
        objects_count = len(catalog.objects)

        return {
            "cluster_id": coordinator.config.cluster_id,
            "total_nodes": len(nodes),
            "healthy_nodes": len(ring.get_healthy_nodes()),
            "total_buckets": len(buckets),
            "total_objects": objects_count,
            "total_chunks_stored": total_chunks,
            "total_bytes_stored": total_bytes,
            "pending_hints": handoff_manager.get_pending_count(),
            "total_reads": coordinator.total_reads,
            "total_writes": coordinator.total_writes,
            "read_repairs_count": coordinator.read_repairs_count,
            "nodes": nodes_status
        }

    @app.post("/api/chaos/node/fail")
    async def chaos_fail_node(node_id: str = Query(...)):
        node = ring.get_node(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Node not found")
        ring.update_node_status(node_id, NodeStatus.DEAD)
        return {"status": "SUCCESS", "node_id": node_id, "new_status": "DEAD"}

    @app.post("/api/chaos/node/recover")
    async def chaos_recover_node(node_id: str = Query(...)):
        node = ring.get_node(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Node not found")
        ring.update_node_status(node_id, NodeStatus.HEALTHY)
        replayed = await handoff_manager.replay_hints_for_node(node_id)
        return {"status": "SUCCESS", "node_id": node_id, "new_status": "HEALTHY", "hints_replayed": replayed}

    @app.post("/api/chaos/partition")
    async def chaos_partition(node_ids: List[str] = Query(...)):
        chaos.isolate_nodes(node_ids)
        for nid in node_ids:
            ring.update_node_status(nid, NodeStatus.ISOLATED)
        return {"status": "SUCCESS", "isolated_nodes": node_ids}

    @app.post("/api/chaos/heal")
    async def chaos_heal():
        chaos.heal_partitions()
        for node in ring.get_all_nodes():
            if node.status == NodeStatus.ISOLATED:
                ring.update_node_status(node.node_id, NodeStatus.HEALTHY)
        replayed = await handoff_manager.replay_all_pending()
        return {"status": "SUCCESS", "message": "All network partitions healed", "hints_replayed": replayed}

    @app.post("/api/chaos/corrupt")
    async def chaos_corrupt_chunk(node_id: str = Query(...), chunk_id: str = Query(...)):
        client = coordinator.clients.get(node_id)
        if not client:
            raise HTTPException(status_code=404, detail="Node not found")
        ok = await client.corrupt_chunk(chunk_id)
        return {"status": "SUCCESS" if ok else "FAILED", "node_id": node_id, "chunk_id": chunk_id}

    @app.post("/api/admin/scrub")
    async def trigger_scrub():
        results = {}
        for node in ring.get_healthy_nodes():
            client = coordinator.clients.get(node.node_id)
            if client:
                try:
                    res = await client.trigger_scrub()
                    results[node.node_id] = res
                except Exception as e:
                    results[node.node_id] = {"error": str(e)}
        return {"status": "SUCCESS", "scrub_results": results}

    @app.post("/api/admin/anti-entropy")
    async def trigger_anti_entropy():
        report = await anti_entropy.run_anti_entropy_cycle()
        return {"status": "SUCCESS", "report": report}

    @app.post("/api/admin/rebalance")
    async def trigger_rebalance(new_node_id: str = Query(...)):
        report = await rebalancer.rebalance_for_new_node(new_node_id)
        return {"status": "SUCCESS", "report": report}

    # -------------------------------------------------------------
    # S3 Parameterized REST API Endpoints
    # -------------------------------------------------------------

    @app.put("/{bucket}")
    async def create_bucket(
        bucket: str,
        x_vault_replication_factor: Optional[int] = Header(None),
        x_vault_write_quorum: Optional[int] = Header(None),
        x_vault_read_quorum: Optional[int] = Header(None)
    ):
        policy = ReplicationPolicy()
        if x_vault_replication_factor:
            policy.replication_factor = x_vault_replication_factor
        if x_vault_write_quorum:
            policy.write_quorum = x_vault_write_quorum
        if x_vault_read_quorum:
            policy.read_quorum = x_vault_read_quorum

        try:
            b = await catalog.create_bucket(bucket, policy=policy)
            return b.model_dump()
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))

    @app.delete("/{bucket}")
    async def delete_bucket(bucket: str):
        try:
            await catalog.delete_bucket(bucket)
            return Response(status_code=204)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Bucket not found")
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))

    @app.get("/{bucket}")
    async def list_objects(
        bucket: str,
        prefix: str = Query("", description="Key prefix filter"),
        delimiter: str = Query("", description="Delimiter for grouping hierarchy"),
        marker: str = Query("", description="Pagination key marker"),
        max_keys: int = Query(1000, description="Max keys to return")
    ):
        b = await catalog.get_bucket(bucket)
        if not b:
            raise HTTPException(status_code=404, detail="Bucket not found")

        objects, common_prefixes = await catalog.list_objects(
            bucket=bucket, prefix=prefix, delimiter=delimiter, marker=marker, max_keys=max_keys
        )
        return {
            "bucket": bucket,
            "prefix": prefix,
            "delimiter": delimiter,
            "is_truncated": len(objects) >= max_keys,
            "objects": [obj.model_dump() for obj in objects],
            "common_prefixes": common_prefixes
        }

    @app.put("/{bucket}/{key:path}")
    async def put_object(
        bucket: str,
        key: str,
        request: Request,
        content_type: str = Header("application/octet-stream")
    ):
        body = await request.body()
        user_meta = {}
        for h_name, h_val in request.headers.items():
            if h_name.startswith("x-amz-meta-") or h_name.startswith("x-vault-meta-"):
                user_meta[h_name] = h_val

        try:
            version = await coordinator.put_object(
                bucket=bucket,
                key=key,
                data=body,
                content_type=content_type,
                user_metadata=user_meta
            )
            return Response(
                status_code=200,
                headers={
                    "ETag": version.etag,
                    "x-amz-version-id": version.version_id,
                    "Content-Length": "0"
                }
            )
        except QuorumWriteFailedError as qe:
            raise HTTPException(status_code=503, detail=str(qe))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/{bucket}/{key:path}")
    async def get_object(
        bucket: str,
        key: str,
        range_header: Optional[str] = Header(None, alias="range"),
        version_id: Optional[str] = Query(None)
    ):
        byte_range = None
        if range_header and range_header.startswith("bytes="):
            parts = range_header[6:].split("-")
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                byte_range = (int(parts[0]), int(parts[1]))

        try:
            data, version, telemetry = await coordinator.get_object(
                bucket=bucket, key=key, version_id=version_id, byte_range=byte_range
            )
            headers = {
                "ETag": version.etag,
                "Content-Type": version.content_type,
                "Content-Length": str(len(data)),
                "x-amz-version-id": version.version_id,
                "x-vault-repairs-performed": str(len(telemetry.get("read_repairs", [])))
            }
            if byte_range:
                headers["Content-Range"] = f"bytes {byte_range[0]}-{byte_range[0] + len(data) - 1}/{version.size_bytes}"
                return Response(content=data, status_code=206, headers=headers)

            return Response(content=data, status_code=200, headers=headers)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Object not found")
        except ObjectUnavailableError as oe:
            raise HTTPException(status_code=503, detail=str(oe))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.head("/{bucket}/{key:path}")
    async def head_object(bucket: str, key: str, version_id: Optional[str] = Query(None)):
        meta = await catalog.get_object_metadata(bucket, key, version_id=version_id)
        if not meta:
            raise HTTPException(status_code=404, detail="Object not found")
        return Response(
            status_code=200,
            headers={
                "ETag": meta.etag,
                "Content-Type": meta.content_type,
                "Content-Length": str(meta.size_bytes),
                "x-amz-version-id": meta.version_id
            }
        )

    @app.delete("/{bucket}/{key:path}")
    async def delete_object(bucket: str, key: str):
        del_v = await coordinator.delete_object(bucket, key)
        return Response(
            status_code=204,
            headers={"x-amz-delete-marker": "true", "x-amz-version-id": del_v.version_id}
        )

    return app





DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Vault — Fault-Tolerant Distributed Object Storage</title>
    <style>
        :root {
            --bg-primary: #0a0e17;
            --bg-card: #121927;
            --bg-elevated: #1b2438;
            --accent-cyan: #00f0ff;
            --accent-purple: #9d4edd;
            --accent-green: #00e676;
            --accent-red: #ff3366;
            --accent-amber: #ffaa00;
            --text-main: #f0f4f8;
            --text-muted: #8b9bb4;
            --border-color: #24324d;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; }
        body { background: var(--bg-primary); color: var(--text-main); min-height: 100vh; padding: 24px; }
        header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border-color); padding-bottom: 16px; margin-bottom: 24px; }
        .logo-group { display: flex; align-items: center; gap: 14px; }
        .logo-icon { width: 36px; height: 36px; background: linear-gradient(135deg, var(--accent-cyan), var(--accent-purple)); border-radius: 8px; display: flex; align-items: center; justify-content: center; font-weight: bold; font-size: 20px; color: #fff; }
        .cluster-pill { background: rgba(0, 240, 255, 0.1); border: 1px solid var(--accent-cyan); color: var(--accent-cyan); padding: 4px 12px; border-radius: 16px; font-size: 13px; }
        
        .metrics-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; margin-bottom: 24px; }
        .metric-card { background: var(--bg-card); border: 1px solid var(--border-color); border-radius: 12px; padding: 16px; }
        .metric-label { font-size: 12px; text-transform: uppercase; letter-spacing: 0.8px; color: var(--text-muted); margin-bottom: 6px; }
        .metric-val { font-size: 24px; font-weight: 700; color: var(--text-main); }
        .metric-val.cyan { color: var(--accent-cyan); }
        .metric-val.green { color: var(--accent-green); }
        .metric-val.amber { color: var(--accent-amber); }
        .metric-val.purple { color: var(--accent-purple); }

        .main-layout { display: grid; grid-template-columns: 2fr 1fr; gap: 24px; }
        .card { background: var(--bg-card); border: 1px solid var(--border-color); border-radius: 14px; padding: 20px; margin-bottom: 24px; }
        .card-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; border-bottom: 1px solid rgba(255,255,255,0.06); padding-bottom: 10px; }
        .card-title { font-size: 16px; font-weight: 600; }

        /* Nodes Grid */
        .nodes-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap: 16px; }
        .node-card { background: var(--bg-elevated); border: 1px solid var(--border-color); border-radius: 10px; padding: 16px; transition: transform 0.2s; position: relative; }
        .node-card.HEALTHY { border-left: 4px solid var(--accent-green); }
        .node-card.DEAD { border-left: 4px solid var(--accent-red); opacity: 0.7; }
        .node-card.ISOLATED { border-left: 4px solid var(--accent-amber); }
        .node-title { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; font-weight: 600; font-size: 15px; }
        .badge { font-size: 11px; padding: 2px 8px; border-radius: 6px; font-weight: 600; text-transform: uppercase; }
        .badge.HEALTHY { background: rgba(0, 230, 118, 0.2); color: var(--accent-green); }
        .badge.DEAD { background: rgba(255, 51, 102, 0.2); color: var(--accent-red); }
        .badge.ISOLATED { background: rgba(255, 170, 0, 0.2); color: var(--accent-amber); }
        .node-meta { font-size: 12px; color: var(--text-muted); line-height: 1.6; }

        /* Control Panel */
        .btn-group { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }
        button { cursor: pointer; border: none; border-radius: 6px; padding: 8px 14px; font-size: 13px; font-weight: 600; transition: all 0.2s; }
        .btn-primary { background: var(--accent-cyan); color: #000; }
        .btn-primary:hover { filter: brightness(1.1); transform: translateY(-1px); }
        .btn-danger { background: rgba(255, 51, 102, 0.2); color: var(--accent-red); border: 1px solid var(--accent-red); }
        .btn-danger:hover { background: var(--accent-red); color: #fff; }
        .btn-success { background: rgba(0, 230, 118, 0.2); color: var(--accent-green); border: 1px solid var(--accent-green); }
        .btn-success:hover { background: var(--accent-green); color: #000; }
        .btn-action { background: var(--bg-elevated); color: var(--text-main); border: 1px solid var(--border-color); }
        .btn-action:hover { border-color: var(--accent-cyan); color: var(--accent-cyan); }

        /* Log view */
        .event-log { background: #070a10; border: 1px solid var(--border-color); border-radius: 8px; padding: 12px; font-family: monospace; font-size: 12px; height: 180px; overflow-y: auto; color: #73e2a7; }

        /* Forms */
        input, select { background: var(--bg-elevated); border: 1px solid var(--border-color); border-radius: 6px; padding: 8px 12px; color: var(--text-main); font-size: 13px; outline: none; }
        input:focus { border-color: var(--accent-cyan); }
    </style>
</head>
<body>
    <header>
        <div class="logo-group">
            <div class="logo-icon">V</div>
            <div>
                <h2>Vault Object Storage</h2>
                <div style="font-size: 12px; color: var(--text-muted);">Fault-Tolerant Distributed Cluster & Anti-Entropy Engine</div>
            </div>
        </div>
        <div style="display: flex; gap: 10px; align-items: center;">
            <span class="cluster-pill" id="cluster-id">vault-cluster-01</span>
            <button class="btn-action" onclick="fetchStatus()">Refresh Live</button>
        </div>
    </header>

    <div class="metrics-grid">
        <div class="metric-card">
            <div class="metric-label">Active Nodes</div>
            <div class="metric-val green" id="healthy-nodes">- / -</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Total Stored Chunks</div>
            <div class="metric-val cyan" id="chunks-stored">0</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Total Volume (Bytes)</div>
            <div class="metric-val" id="bytes-stored">0 B</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Automatic Read Repairs</div>
            <div class="metric-val purple" id="repairs-count">0</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Hinted Handoff Buffer</div>
            <div class="metric-val amber" id="pending-hints">0</div>
        </div>
    </div>

    <div class="main-layout">
        <div>
            <!-- Storage Nodes -->
            <div class="card">
                <div class="card-header">
                    <div class="card-title">Storage Ring Nodes & Failure Domains</div>
                    <span style="font-size: 12px; color: var(--text-muted);">Consistent Hash Ring vnodes: 128/node</span>
                </div>
                <div class="nodes-grid" id="nodes-container">
                    <!-- Dynamic node cards -->
                </div>
            </div>

            <!-- Interactive Object Storage Test Bench -->
            <div class="card">
                <div class="card-header">
                    <div class="card-title">Object Storage Operations (S3 Protocol)</div>
                </div>
                <div style="display: flex; gap: 10px; margin-bottom: 14px;">
                    <input type="text" id="bucket-input" placeholder="Bucket (e.g. assets)" value="vault-data" style="width: 140px;">
                    <input type="text" id="key-input" placeholder="Object Key (e.g. doc.txt)" value="sample.txt" style="width: 180px;">
                    <input type="text" id="payload-input" placeholder="Payload string or text" value="Resilient distributed storage test payload" style="flex: 1;">
                    <button class="btn-primary" onclick="uploadObject()">PUT Object</button>
                    <button class="btn-action" onclick="fetchObject()">GET Object</button>
                </div>
                <div id="object-result" style="font-size: 13px; color: var(--accent-cyan); background: var(--bg-elevated); padding: 10px; border-radius: 6px; min-height: 40px; display: flex; align-items: center;">Ready for operations.</div>
            </div>
        </div>

        <div>
            <!-- Chaos Injection Console -->
            <div class="card">
                <div class="card-header">
                    <div class="card-title">Chaos & Fault Simulation</div>
                </div>
                <p style="font-size: 13px; color: var(--text-muted); margin-bottom: 12px;">
                    Inject failures to test fault-tolerance, quorum repair, and anti-entropy.
                </p>
                <div style="margin-bottom: 14px;">
                    <label style="font-size: 12px; color: var(--text-muted);">Target Node ID:</label>
                    <select id="target-node-select" style="width: 100%; margin-top: 4px;">
                        <!-- populated dynamically -->
                    </select>
                </div>
                <div class="btn-group">
                    <button class="btn-danger" onclick="failSelectedNode()">Fail Node (Crash)</button>
                    <button class="btn-success" onclick="recoverSelectedNode()">Recover Node</button>
                    <button class="btn-action" onclick="partitionSelectedNode()">Isolate (Partition)</button>
                    <button class="btn-success" onclick="healAll()">Heal All Partitions</button>
                </div>
                <hr style="border: 0; border-top: 1px solid var(--border-color); margin: 16px 0;">
                <div class="card-title" style="font-size: 14px; margin-bottom: 8px;">Active Anti-Entropy & Scrub</div>
                <div class="btn-group">
                    <button class="btn-action" onclick="triggerAntiEntropy()">Run Merkle Anti-Entropy</button>
                    <button class="btn-action" onclick="triggerScrub()">Run Bit-Rot Scrub</button>
                </div>
            </div>

            <!-- Real-time Cluster Activity Logs -->
            <div class="card">
                <div class="card-header">
                    <div class="card-title">Cluster Event Telemetry</div>
                </div>
                <div class="event-log" id="log-console">
                    [System] Vault distributed storage cluster initialized.<br>
                </div>
            </div>
        </div>
    </div>

    <script>
        function log(msg) {
            const consoleEl = document.getElementById("log-console");
            const time = new Date().toLocaleTimeString();
            consoleEl.innerHTML = `[${time}] ${msg}<br>` + consoleEl.innerHTML;
        }

        async function fetchStatus() {
            try {
                const res = await fetch("/api/cluster/status");
                const data = await res.json();
                document.getElementById("healthy-nodes").innerText = `${data.healthy_nodes} / ${data.total_nodes}`;
                document.getElementById("chunks-stored").innerText = data.total_chunks_stored;
                document.getElementById("bytes-stored").innerText = `${data.total_bytes_stored} B`;
                document.getElementById("repairs-count").innerText = data.read_repairs_count;
                document.getElementById("pending-hints").innerText = data.pending_hints;

                const nodesContainer = document.getElementById("nodes-container");
                const selectEl = document.getElementById("target-node-select");
                nodesContainer.innerHTML = "";
                selectEl.innerHTML = "";

                data.nodes.forEach(n => {
                    const card = document.createElement("div");
                    card.className = `node-card ${n.status}`;
                    card.innerHTML = `
                        <div class="node-title">
                            <span>${n.node_id}</span>
                            <span class="badge ${n.status}">${n.status}</span>
                        </div>
                        <div class="node-meta">
                            <div><strong>Endpoint:</strong> ${n.host}:${n.port}</div>
                            <div><strong>Topology:</strong> ${n.rack} / ${n.zone}</div>
                            <div><strong>Chunks:</strong> ${n.stats.chunk_count || 0} (Corrupt: ${n.stats.corrupt_count || 0})</div>
                            <div><strong>Storage:</strong> ${n.stats.bytes_used || 0} bytes</div>
                        </div>
                    `;
                    nodesContainer.appendChild(card);

                    const opt = document.createElement("option");
                    opt.value = n.node_id;
                    opt.innerText = `${n.node_id} (${n.status})`;
                    selectEl.appendChild(opt);
                });
            } catch (err) {
                console.error("Status fetch error", err);
            }
        }

        async function uploadObject() {
            const bucket = document.getElementById("bucket-input").value.trim();
            const key = document.getElementById("key-input").value.trim();
            const payload = document.getElementById("payload-input").value;
            const resBox = document.getElementById("object-result");

            resBox.innerText = `Uploading ${bucket}/${key}...`;
            try {
                const res = await fetch(`/${bucket}/${key}`, {
                    method: "PUT",
                    body: payload,
                    headers: { "Content-Type": "text/plain" }
                });
                if (res.ok) {
                    const etag = res.headers.get("ETag");
                    const vId = res.headers.get("x-amz-version-id");
                    resBox.innerText = `SUCCESS! Committed with ETag: ${etag} (Version: ${vId})`;
                    log(`Object written: ${bucket}/${key} with ETag ${etag}`);
                    fetchStatus();
                } else {
                    const err = await res.text();
                    resBox.innerText = `Write Failed: ${err}`;
                    log(`Write failed: ${err}`);
                }
            } catch (e) {
                resBox.innerText = `Error: ${e.message}`;
            }
        }

        async function fetchObject() {
            const bucket = document.getElementById("bucket-input").value.trim();
            const key = document.getElementById("key-input").value.trim();
            const resBox = document.getElementById("object-result");

            resBox.innerText = `Reading ${bucket}/${key}...`;
            try {
                const res = await fetch(`/${bucket}/${key}`);
                if (res.ok) {
                    const txt = await res.text();
                    const repairs = res.headers.get("x-vault-repairs-performed") || "0";
                    resBox.innerText = `Retrieved (${txt.length} bytes): "${txt}" [Read repairs: ${repairs}]`;
                    log(`Read ${bucket}/${key} successful. Automatic read-repairs triggered: ${repairs}`);
                    fetchStatus();
                } else {
                    const err = await res.text();
                    resBox.innerText = `Read Failed: ${err}`;
                    log(`Read failed: ${err}`);
                }
            } catch (e) {
                resBox.innerText = `Error: ${e.message}`;
            }
        }

        async function failSelectedNode() {
            const nid = document.getElementById("target-node-select").value;
            log(`Failing node ${nid}...`);
            await fetch(`/api/chaos/node/fail?node_id=${nid}`, { method: "POST" });
            fetchStatus();
        }

        async function recoverSelectedNode() {
            const nid = document.getElementById("target-node-select").value;
            log(`Recovering node ${nid}...`);
            const res = await fetch(`/api/chaos/node/recover?node_id=${nid}`, { method: "POST" });
            const data = await res.json();
            log(`Node ${nid} recovered. Replayed ${data.hints_replayed} pending hints.`);
            fetchStatus();
        }

        async function partitionSelectedNode() {
            const nid = document.getElementById("target-node-select").value;
            log(`Isolating node ${nid} via network partition...`);
            await fetch(`/api/chaos/partition?node_ids=${nid}`, { method: "POST" });
            fetchStatus();
        }

        async function healAll() {
            log(`Healing all network partitions across cluster...`);
            const res = await fetch(`/api/chaos/heal`, { method: "POST" });
            const data = await res.json();
            log(`Network healed. Replayed ${data.hints_replayed} buffered hints.`);
            fetchStatus();
        }

        async function triggerAntiEntropy() {
            log(`Initiating Merkle Tree Active Anti-Entropy cycle...`);
            const res = await fetch(`/api/admin/anti-entropy`, { method: "POST" });
            const data = await res.json();
            log(`Anti-Entropy complete. Checked pairs: ${data.report.pairs_checked}, Repaired chunks: ${data.report.total_repairs}`);
            fetchStatus();
        }

        async function triggerScrub() {
            log(`Initiating cluster-wide bit-rot disk scrub...`);
            const res = await fetch(`/api/admin/scrub`, { method: "POST" });
            const data = await res.json();
            log(`Scrub sweep completed across all healthy storage nodes.`);
            fetchStatus();
        }

        setInterval(fetchStatus, 3000);
        fetchStatus();
    </script>
</body>
</html>
"""
