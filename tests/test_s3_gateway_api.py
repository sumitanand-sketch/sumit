"""
Integration tests for Gateway S3 HTTP REST API and Admin Endpoints.
"""

import pytest
import httpx


@pytest.mark.asyncio
async def test_s3_rest_api_lifecycle(cluster):
    transport = httpx.ASGITransport(app=cluster.gateway_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        # 1. Create bucket
        resp = await client.put("/media-bucket")
        assert resp.status_code == 200
        assert resp.json()["name"] == "media-bucket"

        # 2. Put object
        content = b"Image or video byte stream"
        resp = await client.put(
            "/media-bucket/images/logo.png",
            content=content,
            headers={"Content-Type": "image/png", "x-amz-meta-author": "vault"}
        )
        assert resp.status_code == 200
        assert "ETag" in resp.headers
        assert "x-amz-version-id" in resp.headers
        etag = resp.headers["ETag"]

        # 3. Head object
        resp = await client.head("/media-bucket/images/logo.png")
        assert resp.status_code == 200
        assert resp.headers["ETag"] == etag
        assert int(resp.headers["Content-Length"]) == len(content)

        # 4. Get object
        resp = await client.get("/media-bucket/images/logo.png")
        assert resp.status_code == 200
        assert resp.content == content
        assert resp.headers["Content-Type"] == "image/png"

        # 5. List objects
        resp = await client.get("/media-bucket", params={"prefix": "images/"})
        assert resp.status_code == 200
        listing = resp.json()
        assert len(listing["objects"]) == 1
        assert listing["objects"][0]["key"] == "images/logo.png"

        # 6. Delete object
        resp = await client.delete("/media-bucket/images/logo.png")
        assert resp.status_code == 204

        # 7. Verify object is no longer returned
        resp = await client.get("/media-bucket/images/logo.png")
        assert resp.status_code == 404

        # 8. Check cluster telemetry
        resp = await client.get("/api/cluster/status")
        assert resp.status_code == 200
        status_data = resp.json()
        assert status_data["healthy_nodes"] == 3
        assert status_data["total_nodes"] == 3
