"""On-disk chunk storage for a worker.

Layout (inside the worker's persistent volume)::

    <data_dir>/chunks/<chunk_id>.chunk       raw chunk bytes
    <data_dir>/chunks/<chunk_id>.meta.json   {"chunk_id", "size", "checksum", "stored_at", ...}
    <data_dir>/tmp/                          staging area for atomic writes

Writes are atomic: data and metadata are written to ``tmp/``, fsync'ed and
then renamed into place, so a crash never leaves a half-written chunk that
looks valid. Every read can re-hash the data and compare it with the stored
SHA-256 to detect on-disk corruption (bit rot, tampering, truncation).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import threading
import time
from pathlib import Path

CHUNK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
DATA_SUFFIX = ".chunk"
META_SUFFIX = ".meta.json"


class InvalidChunkId(ValueError):
    pass


class ChunkNotFound(KeyError):
    pass


class ChunkCorrupted(Exception):
    def __init__(self, chunk_id: str, expected: str | None, actual: str | None):
        super().__init__(f"chunk {chunk_id} is corrupted (expected {expected}, got {actual})")
        self.chunk_id = chunk_id
        self.expected = expected
        self.actual = actual


class ChecksumMismatch(ValueError):
    """The data received for a PUT does not match the declared checksum."""


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def validate_chunk_id(chunk_id: str) -> str:
    if not CHUNK_ID_RE.match(chunk_id) or ".." in chunk_id or chunk_id.endswith(META_SUFFIX):
        raise InvalidChunkId(f"invalid chunk id: {chunk_id!r}")
    return chunk_id


class ChunkStore:
    def __init__(self, data_dir: str):
        self.root = Path(data_dir)
        self.chunk_dir = self.root / "chunks"
        self.tmp_dir = self.root / "tmp"
        self.chunk_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        for leftover in self.tmp_dir.iterdir():  # half-finished writes from a crash
            leftover.unlink(missing_ok=True)
        # Chunks found corrupted (by reads or the scrubber), reported to the
        # controller on every heartbeat until they are rewritten or deleted.
        self._corrupt: set[str] = set()
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ paths

    def _data_path(self, chunk_id: str) -> Path:
        return self.chunk_dir / f"{validate_chunk_id(chunk_id)}{DATA_SUFFIX}"

    def _meta_path(self, chunk_id: str) -> Path:
        return self.chunk_dir / f"{validate_chunk_id(chunk_id)}{META_SUFFIX}"

    def _atomic_write(self, target: Path, data: bytes) -> None:
        tmp = self.tmp_dir / f"{target.name}.{os.getpid()}.{threading.get_ident()}.{time.time_ns()}"
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, target)

    # ------------------------------------------------------------------ corruption tracking

    def _flag_corrupt(self, chunk_id: str) -> None:
        with self._lock:
            self._corrupt.add(chunk_id)

    def _clear_corrupt(self, chunk_id: str) -> None:
        with self._lock:
            self._corrupt.discard(chunk_id)

    def corrupt_chunks(self) -> list[str]:
        with self._lock:
            return sorted(self._corrupt)

    # ------------------------------------------------------------------ operations

    def put(self, chunk_id: str, data: bytes, expected_checksum: str | None = None,
            extra: dict | None = None) -> tuple[dict, bool]:
        """Store a chunk (overwriting any existing copy). Returns (meta, created)."""
        checksum = sha256_hex(data)
        if expected_checksum and expected_checksum.lower() != checksum:
            raise ChecksumMismatch(f"declared checksum {expected_checksum} but received data hashes to {checksum}")
        data_path, meta_path = self._data_path(chunk_id), self._meta_path(chunk_id)
        created = not data_path.exists()
        meta = {"chunk_id": chunk_id, "size": len(data), "checksum": checksum, "stored_at": time.time(),
                **(extra or {})}
        self._atomic_write(data_path, data)
        self._atomic_write(meta_path, json.dumps(meta).encode())
        self._clear_corrupt(chunk_id)
        return meta, created

    def read_meta(self, chunk_id: str) -> dict | None:
        try:
            return json.loads(self._meta_path(chunk_id).read_bytes())
        except FileNotFoundError:
            return None
        except (ValueError, OSError):
            return {}  # unreadable metadata: treat the chunk as corrupt

    def get(self, chunk_id: str, verify: bool = True) -> tuple[bytes, dict]:
        data_path = self._data_path(chunk_id)
        try:
            data = data_path.read_bytes()
        except FileNotFoundError:
            raise ChunkNotFound(chunk_id) from None
        meta = self.read_meta(chunk_id) or {}
        if verify:
            actual = sha256_hex(data)
            if actual != meta.get("checksum"):
                self._flag_corrupt(chunk_id)
                raise ChunkCorrupted(chunk_id, meta.get("checksum"), actual)
        return data, meta

    def delete(self, chunk_id: str) -> bool:
        data_path, meta_path = self._data_path(chunk_id), self._meta_path(chunk_id)
        existed = data_path.exists()
        data_path.unlink(missing_ok=True)
        meta_path.unlink(missing_ok=True)
        self._clear_corrupt(chunk_id)
        return existed

    def verify(self, chunk_id: str) -> bool:
        try:
            self.get(chunk_id, verify=True)
        except ChunkCorrupted:
            return False
        self._clear_corrupt(chunk_id)
        return True

    def list(self, verify: bool = False) -> list[dict]:
        """Inventory of stored chunks; with ``verify`` every chunk is re-hashed."""
        result = []
        for entry in sorted(os.scandir(self.chunk_dir), key=lambda e: e.name):
            if not entry.name.endswith(DATA_SUFFIX):
                continue
            chunk_id = entry.name[: -len(DATA_SUFFIX)]
            try:
                validate_chunk_id(chunk_id)
            except InvalidChunkId:
                continue
            meta = self.read_meta(chunk_id) or {}
            item = {
                "chunk_id": chunk_id,
                "size": entry.stat().st_size,
                "checksum": meta.get("checksum"),
                "stored_at": meta.get("stored_at"),
                "object_id": meta.get("object_id"),
                "index": meta.get("index"),
            }
            if verify:
                try:
                    item["valid"] = self.verify(chunk_id)
                except ChunkNotFound:  # deleted while scanning
                    continue
            result.append(item)
        return result

    def stats(self) -> dict:
        count = used = 0
        for entry in os.scandir(self.chunk_dir):
            if entry.name.endswith(DATA_SUFFIX):
                count += 1
                try:
                    used += entry.stat().st_size
                except FileNotFoundError:
                    pass
        disk = shutil.disk_usage(self.root)
        return {"chunk_count": count, "used_bytes": used, "free_bytes": disk.free, "capacity_bytes": disk.total}
