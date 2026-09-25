"""
Metadata Catalog for Buckets, Objects, Versions, and Multipart Uploads.
"""

import time
import asyncio
from typing import Dict, List, Optional, Tuple
from vault.models import (
    BucketMetadata,
    ObjectMetadata,
    ObjectVersion,
    MultipartUploadInfo,
    ChunkMetadata,
)
from vault.config import ReplicationPolicy


class MetadataCatalog:
    """
    Manages bucket configuration, object versions, prefixes, and multipart uploads.
    Thread-safe and async-safe with fine-grained locking.
    """

    def __init__(self):
        # bucket_name -> BucketMetadata
        self.buckets: Dict[str, BucketMetadata] = {}
        # (bucket_name, object_key) -> ObjectMetadata
        self.objects: Dict[Tuple[str, str], ObjectMetadata] = {}
        # upload_id -> MultipartUploadInfo
        self.multipart_uploads: Dict[str, MultipartUploadInfo] = {}
        self._lock = asyncio.Lock()

    async def create_bucket(self, name: str, policy: Optional[ReplicationPolicy] = None) -> BucketMetadata:
        async with self._lock:
            if name in self.buckets:
                raise ValueError(f"Bucket '{name}' already exists")
            bucket = BucketMetadata(
                name=name,
                created_at=time.time(),
                replication_policy=policy or ReplicationPolicy(),
                versioning_enabled=True
            )
            self.buckets[name] = bucket
            return bucket

    async def get_bucket(self, name: str) -> Optional[BucketMetadata]:
        async with self._lock:
            return self.buckets.get(name)

    async def list_buckets(self) -> List[BucketMetadata]:
        async with self._lock:
            return list(self.buckets.values())

    async def delete_bucket(self, name: str) -> bool:
        async with self._lock:
            if name not in self.buckets:
                raise FileNotFoundError(f"Bucket '{name}' not found")
            # Check if bucket contains non-deleted objects
            has_objects = any(
                b == name and not obj.is_deleted
                for (b, _), obj in self.objects.items()
            )
            if has_objects:
                raise ValueError(f"Bucket '{name}' is not empty")
            del self.buckets[name]
            return True

    async def commit_object_version(
        self,
        bucket: str,
        key: str,
        version_id: str,
        size_bytes: int,
        etag: str,
        content_type: str,
        chunk_ids: List[str],
        user_metadata: Optional[Dict[str, str]] = None,
        is_delete_marker: bool = False
    ) -> ObjectVersion:
        """Commits a new object version or delete marker."""
        now = time.time()
        version = ObjectVersion(
            version_id=version_id,
            bucket=bucket,
            key=key,
            size_bytes=size_bytes,
            etag=etag,
            content_type=content_type,
            chunk_ids=chunk_ids,
            user_metadata=user_metadata or {},
            is_delete_marker=is_delete_marker,
            created_at=now
        )

        async with self._lock:
            if bucket not in self.buckets:
                # Auto-create bucket with default policy if not present
                self.buckets[bucket] = BucketMetadata(name=bucket)

            obj_key = (bucket, key)
            if obj_key not in self.objects:
                self.objects[obj_key] = ObjectMetadata(
                    bucket=bucket,
                    key=key,
                    created_at=now,
                    updated_at=now
                )

            obj_meta = self.objects[obj_key]
            obj_meta.versions.insert(0, version)  # newest first
            obj_meta.current_version_id = version_id
            obj_meta.is_deleted = is_delete_marker
            obj_meta.updated_at = now

        return version

    async def get_object_metadata(
        self, bucket: str, key: str, version_id: Optional[str] = None
    ) -> Optional[ObjectVersion]:
        async with self._lock:
            obj_key = (bucket, key)
            obj_meta = self.objects.get(obj_key)
            if not obj_meta:
                return None

            if version_id:
                for v in obj_meta.versions:
                    if v.version_id == version_id:
                        return v
                return None

            # Get current active version
            if obj_meta.is_deleted or not obj_meta.versions:
                return None

            curr = obj_meta.versions[0]
            if curr.is_delete_marker:
                return None
            return curr

    async def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        marker: str = "",
        max_keys: int = 1000
    ) -> Tuple[List[ObjectVersion], List[str]]:
        """
        Lists active objects matching prefix and delimiter for S3 compatibility.
        Returns (object_versions, common_prefixes).
        """
        async with self._lock:
            results: List[ObjectVersion] = []
            common_prefixes: Set[str] = set()

            all_keys = sorted(k for (b, k) in self.objects.keys() if b == bucket)

            for key in all_keys:
                if marker and key <= marker:
                    continue
                if prefix and not key.startswith(prefix):
                    continue

                obj = self.objects[(bucket, key)]
                if obj.is_deleted or not obj.versions:
                    continue

                current_v = obj.versions[0]
                if current_v.is_delete_marker:
                    continue

                # Check delimiter
                if delimiter:
                    rest = key[len(prefix):]
                    if delimiter in rest:
                        prefix_part = prefix + rest.split(delimiter)[0] + delimiter
                        common_prefixes.add(prefix_part)
                        continue

                results.append(current_v)
                if len(results) >= max_keys:
                    break

            return results, sorted(list(common_prefixes))

    async def initiate_multipart_upload(
        self, bucket: str, key: str, upload_id: str, content_type: str, user_metadata: Dict[str, str]
    ) -> MultipartUploadInfo:
        async with self._lock:
            info = MultipartUploadInfo(
                upload_id=upload_id,
                bucket=bucket,
                key=key,
                content_type=content_type,
                user_metadata=user_metadata
            )
            self.multipart_uploads[upload_id] = info
            return info

    async def record_uploaded_part(
        self, upload_id: str, part_number: int, chunk_meta: ChunkMetadata
    ) -> None:
        async with self._lock:
            if upload_id not in self.multipart_uploads:
                raise ValueError(f"Invalid upload_id: {upload_id}")
            self.multipart_uploads[upload_id].parts[part_number] = chunk_meta

    async def get_multipart_upload(self, upload_id: str) -> Optional[MultipartUploadInfo]:
        async with self._lock:
            return self.multipart_uploads.get(upload_id)

    async def abort_multipart_upload(self, upload_id: str) -> bool:
        async with self._lock:
            return self.multipart_uploads.pop(upload_id, None) is not None
