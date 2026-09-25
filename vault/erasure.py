"""
Erasure Coding engine for Vault (Reed-Solomon style over GF(2^8)).
Provides K data shards + M parity shards for high durability with minimal storage overhead.
"""

from typing import List, Optional, Tuple


class GaloisField:
    """Galois Field GF(2^8) arithmetic with primitive polynomial 0x11d."""

    def __init__(self):
        self.exp = [0] * 512
        self.log = [0] * 256
        x = 1
        for i in range(255):
            self.exp[i] = x
            self.log[x] = i
            x <<= 1
            if x & 0x100:
                x ^= 0x11d
        for i in range(255, 512):
            self.exp[i] = self.exp[i - 255]
        self.log[0] = 0

    def add(self, a: int, b: int) -> int:
        return a ^ b

    def sub(self, a: int, b: int) -> int:
        return a ^ b

    def mul(self, a: int, b: int) -> int:
        if a == 0 or b == 0:
            return 0
        return self.exp[self.log[a] + self.log[b]]

    def div(self, a: int, b: int) -> int:
        if b == 0:
            raise ZeroDivisionError("GF(2^8) division by zero")
        if a == 0:
            return 0
        return self.exp[(self.log[a] - self.log[b] + 255) % 255]

    def inv(self, a: int) -> int:
        if a == 0:
            raise ZeroDivisionError("GF(2^8) inverse of zero")
        return self.exp[255 - self.log[a]]


gf = GaloisField()


class Matrix:
    """Matrix operations over GF(2^8)."""

    def __init__(self, rows: int, cols: int, data: Optional[List[List[int]]] = None):
        self.rows = rows
        self.cols = cols
        self.data = data if data is not None else [[0] * cols for _ in range(rows)]

    @classmethod
    def identity(cls, n: int) -> 'Matrix':
        m = cls(n, n)
        for i in range(n):
            m.data[i][i] = 1
        return m

    def multiply(self, other: 'Matrix') -> 'Matrix':
        assert self.cols == other.rows
        res = Matrix(self.rows, other.cols)
        for i in range(self.rows):
            for j in range(other.cols):
                val = 0
                for k in range(self.cols):
                    val = gf.add(val, gf.mul(self.data[i][k], other.data[k][j]))
                res.data[i][j] = val
        return res

    def invert(self) -> 'Matrix':
        """Inverts a square matrix using Gaussian elimination over GF(2^8)."""
        assert self.rows == self.cols
        n = self.rows
        # Augmented matrix [A | I]
        aug = [row[:] + [1 if i == j else 0 for j in range(n)] for i, row in enumerate(self.data)]

        for col in range(n):
            # Pivot
            pivot = -1
            for row in range(col, n):
                if aug[row][col] != 0:
                    pivot = row
                    break
            if pivot == -1:
                raise ValueError("Matrix is singular and cannot be inverted")

            aug[col], aug[pivot] = aug[pivot], aug[col]
            scale = gf.inv(aug[col][col])
            aug[col] = [gf.mul(val, scale) for val in aug[col]]

            for r in range(n):
                if r != col and aug[r][col] != 0:
                    factor = aug[r][col]
                    aug[r] = [gf.sub(aug[r][c], gf.mul(aug[col][c], factor)) for c in range(2 * n)]

        inv_data = [row[n:] for row in aug]
        return Matrix(n, n, inv_data)


class ErasureCoder:
    """
    Reed-Solomon Erasure Coder.
    Encodes data into K data shards + M parity shards.
    Can reconstruct original data from any K shards out of K+M!
    """

    def __init__(self, k: int, m: int):
        self.k = k
        self.m = m
        self.total = k + m
        self.matrix = self._build_vandermonde_matrix()

    def _build_vandermonde_matrix(self) -> Matrix:
        """Constructs an encoding matrix where top K rows form Identity matrix."""
        mat = Matrix(self.total, self.k)
        for i in range(self.k):
            mat.data[i][i] = 1
        for i in range(self.m):
            row_idx = self.k + i
            for j in range(self.k):
                # Cauchy / Vandermonde distribution
                mat.data[row_idx][j] = gf.exp[(i * j) % 255] if (i * j) > 0 else 1
        return mat

    def encode(self, data: bytes) -> List[bytes]:
        """
        Splits data into K shards, pads with zeros if necessary,
        and computes M parity shards.
        Returns list of K+M shards (all equal length).
        """
        orig_len = len(data)
        shard_len = (orig_len + self.k - 1) // self.k
        if shard_len == 0:
            shard_len = 1

        # Pad data
        padded_len = shard_len * self.k
        padded_data = data + b'\x00' * (padded_len - orig_len)

        shards: List[bytearray] = [
            bytearray(padded_data[i * shard_len:(i + 1) * shard_len])
            for i in range(self.k)
        ]

        # Allocate parity shards
        parity_shards = [bytearray(shard_len) for _ in range(self.m)]

        for byte_idx in range(shard_len):
            for p in range(self.m):
                row_idx = self.k + p
                val = 0
                for d in range(self.k):
                    coeff = self.matrix.data[row_idx][d]
                    val = gf.add(val, gf.mul(shards[d][byte_idx], coeff))
                parity_shards[p][byte_idx] = val

        result = [bytes(s) for s in shards] + [bytes(p) for p in parity_shards]
        return result

    def decode(self, shards: List[Optional[bytes]], orig_length: int) -> bytes:
        """
        Reconstructs original data given any K non-None shards.
        """
        present_indices = [i for i, s in enumerate(shards) if s is not None]
        if len(present_indices) < self.k:
            raise ValueError(f"Need at least {self.k} shards to decode, but only {len(present_indices)} available")

        # Pick first K present shards
        sub_indices = present_indices[:self.k]
        shard_len = len(shards[sub_indices[0]])

        # Build submatrix of corresponding rows
        submatrix_data = [self.matrix.data[idx][:] for idx in sub_indices]
        submatrix = Matrix(self.k, self.k, submatrix_data)
        inverted = submatrix.invert()

        # Reconstruct data shards
        reconstructed_data = bytearray(self.k * shard_len)
        for byte_idx in range(shard_len):
            for d in range(self.k):
                val = 0
                for sub_i, orig_idx in enumerate(sub_indices):
                    shard_byte = shards[orig_idx][byte_idx]
                    coeff = inverted.data[d][sub_i]
                    val = gf.add(val, gf.mul(shard_byte, coeff))
                reconstructed_data[d * shard_len + byte_idx] = val

        return bytes(reconstructed_data[:orig_length])
