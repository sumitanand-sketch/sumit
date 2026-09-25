"""
Storage Node HTTP REST Server.
Exposes chunk read/write RPC endpoints, Merkle tree generation, and chaos testing hooks.
"""

from fastapi import FastAPI, HTTPException, Request, Response, Header
from fastapi.responses import PlainTextResponse
import base64
from typing import Optional

from vault.storage.engine import StorageEngine, CorruptedChunkError
from vault.storage.scrubber import DiskScrubber
from vault.merkle import MerkleTree


def create_storage_node_app(engine: StorageEngine, scrubber: Optional[DiskScrubber] = None) -> FastAPI:
    app = FastAPI(title=f"Vault Storage Node {engine.node_id}")

    @app.get("/health")
    async def health():
        return {"status": "HEALTHY", "node_id": engine.node_id}

    @app.get("/stats")
    async def stats():
        return engine.get_stats()

    @app.post("/chunks/{chunk_id}")
    async def write_chunk(
        chunk_id: str,
        request: Request,
        x_bucket: str = Header(...),
        x_key: str = Header(...),
        x_chunk_index: int = Header(0),
        x_version_id: str = Header("v1"),
        x_sha256: Optional[str] = Header(None)
    ):
        body = await request.body()
        try:
            metadata = await engine.write_chunk(
                chunk_id=chunk_id,
                data=body,
                bucket=x_bucket,
                key=x_key,
                chunk_index=x_chunk_index,
                version_id=x_version_id,
                expected_sha256=x_sha256
            )
            return metadata.model_dump()
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=str(ve))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/chunks/{chunk_id}")
    async def read_chunk(chunk_id: str, verify: bool = True):
        try:
            data = await engine.read_chunk(chunk_id, verify_checksum=verify)
            meta = engine.get_metadata(chunk_id)
            headers = {
                "Content-Type": "application/octet-stream",
                "Content-Length": str(len(data))
            }
            if meta:
                headers["X-Sha256"] = meta.sha256
                headers["X-Crc32"] = str(meta.crc32)
                headers["X-Bucket"] = meta.bucket
                headers["X-Key"] = meta.key
                headers["X-Version-Id"] = meta.version_id
            return Response(content=data, media_type="application/octet-stream", headers=headers)
        except CorruptedChunkError as ce:
            raise HTTPException(status_code=500, detail=f"BIT_ROT_DETECTED: {str(ce)}")
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Chunk not found")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.head("/chunks/{chunk_id}")
    async def head_chunk(chunk_id: str):
        meta = engine.get_metadata(chunk_id)
        if not meta:
            raise HTTPException(status_code=404, detail="Chunk not found")
        return Response(status_code=200, headers={
            "Content-Length": str(meta.size_bytes),
            "X-Sha256": meta.sha256,
            "X-Version-Id": meta.version_id,
            "X-Corrupt": str(meta.is_corrupt)
        })

    @app.delete("/chunks/{chunk_id}")
    async def delete_chunk(chunk_id: str):
        deleted = await engine.delete_chunk(chunk_id)
        return {"deleted": deleted, "chunk_id": chunk_id}

    @app.post("/merkle")
    async def get_merkle_tree():
        """Constructs and returns the node's local Merkle tree for active anti-entropy."""
        tree = MerkleTree(depth=4)
        for chunk in engine.list_chunks():
            if not chunk.is_corrupt:
                tree.insert(chunk.chunk_id, chunk.sha256)
        root_hash = tree.compute_hashes()

        # Collect leaves for easy transmission
        leaves = {}
        def _collect(node):
            if node.is_leaf:
                leaves[node.prefix] = {"hash": node.hash, "items": node.items}
            if node.left:
                _collect(node.left)
            if node.right:
                _collect(node.right)

        _collect(tree.root)
        return {"root_hash": root_hash, "depth": tree.depth, "leaves": leaves}

    @app.post("/chaos/corrupt/{chunk_id}")
    async def chaos_corrupt(chunk_id: str):
        """Simulates bit rot by modifying the raw file on disk."""
        ok = await engine.corrupt_chunk_on_disk(chunk_id)
        if not ok:
            raise HTTPException(status_code=404, detail="Chunk not found")
        return {"corrupted": True, "chunk_id": chunk_id, "node_id": engine.node_id}

    @app.post("/admin/scrub")
    async def trigger_scrub():
        if scrubber:
            report = await scrubber.run_scrub_cycle()
            return report
        return {"error": "Scrubber not configured"}

    return app
