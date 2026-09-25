"""
Cryptographic integrity, hashing, and checksum verification for Vault.
"""

import hashlib
import zlib
from typing import Union


def calculate_sha256(data: bytes) -> str:
    """Calculates SHA-256 hex digest for arbitrary byte payload."""
    hasher = hashlib.sha256()
    hasher.update(data)
    return hasher.hexdigest()


def calculate_etag(data: bytes) -> str:
    """Calculates MD5 hex digest formatted as standard S3 ETag."""
    hasher = hashlib.md5()
    hasher.update(data)
    return f'"{hasher.hexdigest()}"'


def calculate_crc32(data: bytes) -> int:
    """Calculates 32-bit CRC checksum for high-speed block integrity checks."""
    return zlib.crc32(data) & 0xffffffff


def verify_block_integrity(data: bytes, expected_sha256: str) -> bool:
    """Verifies that the block data matches its expected SHA-256 hash."""
    return calculate_sha256(data) == expected_sha256
