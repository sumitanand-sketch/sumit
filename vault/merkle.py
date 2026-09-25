"""
Merkle Tree implementation for Active Anti-Entropy (AAE) and replica synchronization.
"""

import hashlib
from typing import Dict, List, Optional, Tuple, Set


class MerkleTreeNode:
    def __init__(self, prefix: str = ""):
        self.prefix = prefix
        self.hash: Optional[str] = None
        self.left: Optional['MerkleTreeNode'] = None
        self.right: Optional['MerkleTreeNode'] = None
        # Map of key -> chunk/version hash for leaf nodes
        self.items: Dict[str, str] = {}

    @property
    def is_leaf(self) -> bool:
        return self.left is None and self.right is None


class MerkleTree:
    """
    Fixed-depth binary prefix Merkle Tree.
    Divides the key space using binary prefix bit-paths from hex hashes.
    """

    def __init__(self, depth: int = 4):
        """
        Depth determines number of partitions: 2^depth leaves.
        E.g. depth=4 yields 16 leaves, depth=6 yields 64 leaves.
        """
        self.depth = depth
        self.root = self._build_skeleton("", depth)

    def _build_skeleton(self, prefix: str, current_depth: int) -> MerkleTreeNode:
        node = MerkleTreeNode(prefix=prefix)
        if current_depth > 0:
            node.left = self._build_skeleton(prefix + "0", current_depth - 1)
            node.right = self._build_skeleton(prefix + "1", current_depth - 1)
        return node

    def _get_path_for_key(self, key: str) -> str:
        """Derives a deterministic binary prefix path for a key."""
        hex_digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        # Convert hex to binary string
        binary = bin(int(hex_digest, 16))[2:].zfill(256)
        return binary[:self.depth]

    def insert(self, key: str, value_hash: str) -> None:
        """Inserts or updates a key and its version/content hash into the tree."""
        path = self._get_path_for_key(key)
        curr = self.root
        for bit in path:
            if bit == "0":
                curr = curr.left
            else:
                curr = curr.right
        curr.items[key] = value_hash

    def remove(self, key: str) -> None:
        """Removes a key from the tree."""
        path = self._get_path_for_key(key)
        curr = self.root
        for bit in path:
            if bit == "0":
                curr = curr.left
            else:
                curr = curr.right
        if key in curr.items:
            del curr.items[key]

    def compute_hashes(self) -> str:
        """Recursively recalculates all intermediate node hashes and returns the root hash."""
        return self._compute_node_hash(self.root)

    def _compute_node_hash(self, node: MerkleTreeNode) -> str:
        if node.is_leaf:
            if not node.items:
                node.hash = hashlib.sha256(b"EMPTY").hexdigest()
            else:
                # Deterministic sorted order of key:hash pairs
                hasher = hashlib.sha256()
                for k in sorted(node.items.keys()):
                    hasher.update(k.encode("utf-8"))
                    hasher.update(b":")
                    hasher.update(node.items[k].encode("utf-8"))
                    hasher.update(b";")
                node.hash = hasher.hexdigest()
            return node.hash

        left_hash = self._compute_node_hash(node.left) if node.left else "0"
        right_hash = self._compute_node_hash(node.right) if node.right else "0"

        combined = hashlib.sha256((left_hash + right_hash).encode("utf-8")).hexdigest()
        node.hash = combined
        return node.hash

    def get_root_hash(self) -> str:
        """Gets the root hash, computing it if not yet generated."""
        if self.root.hash is None:
            return self.compute_hashes()
        return self.root.hash

    @classmethod
    def compare_and_diff(cls, tree_a: 'MerkleTree', tree_b: 'MerkleTree') -> Tuple[Set[str], Set[str], Set[str]]:
        """
        Fast hierarchical diff between two trees.
        Returns:
            (only_in_a, only_in_b, differing_keys)
        """
        tree_a.compute_hashes()
        tree_b.compute_hashes()

        only_in_a: Set[str] = set()
        only_in_b: Set[str] = set()
        differing: Set[str] = set()

        def _traverse(node_a: Optional[MerkleTreeNode], node_b: Optional[MerkleTreeNode]):
            if node_a is None and node_b is None:
                return

            if node_a and not node_b:
                _collect_keys(node_a, only_in_a)
                return

            if node_b and not node_a:
                _collect_keys(node_b, only_in_b)
                return

            # Both nodes exist
            if node_a.hash == node_b.hash:
                # Subtrees match completely - prune search branch!
                return

            if node_a.is_leaf and node_b.is_leaf:
                keys_a = set(node_a.items.keys())
                keys_b = set(node_b.items.keys())
                only_in_a.update(keys_a - keys_b)
                only_in_b.update(keys_b - keys_a)
                common = keys_a & keys_b
                for k in common:
                    if node_a.items[k] != node_b.items[k]:
                        differing.add(k)
                return

            # Recurse children
            _traverse(node_a.left, node_b.left)
            _traverse(node_a.right, node_b.right)

        def _collect_keys(node: MerkleTreeNode, target_set: Set[str]):
            if node.is_leaf:
                target_set.update(node.items.keys())
                return
            if node.left:
                _collect_keys(node.left, target_set)
            if node.right:
                _collect_keys(node.right, target_set)

        _traverse(tree_a.root, tree_b.root)
        return only_in_a, only_in_b, differing
