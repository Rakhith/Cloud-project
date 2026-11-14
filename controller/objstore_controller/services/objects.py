"""Client-facing object operations: upload, download, metadata, delete, list.

Write path (upload)
-------------------
1. Require at least ``replication_factor`` UP workers (else 503).
2. Create the object row in state ``pending`` (invisible to clients).
3. Read the upload 1 MB at a time. For chunk *i*:
   - compute its SHA-256 and record the chunk row (before any worker write,
     so reconciliation never mistakes an in-flight chunk for an orphan);
   - choose ``rf`` distinct workers by rotating a load-ordered worker list by
     *i* (consecutive chunks land on different worker pairs);
   - write all copies in parallel; if a worker fails, fall back to the next
     candidate; record each acknowledged copy.
4. Store the whole-object SHA-256, size and chunk count and flip the object
   to ``active``. Any failure removes everything written so far.

Read path (download)
--------------------
Chunks are streamed to the client in order with a small read-ahead window.
Every chunk is verified against its recorded checksum (see
``ReplicationManager.read_chunk``), so corrupted copies are never returned
while a valid copy exists. The first chunk is fetched before the response
starts so that "no copy available" can still be reported as a proper JSON
error.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
import uuid
from collections import deque
from typing import AsyncIterator

from fastapi import UploadFile

from ..config import Settings
from ..errors import Conflict, InvalidRequest, ObjectNotFound, PayloadTooLarge, ServiceUnavailable
from ..store import (
    OBJECT_DELETING,
    REPLICA_HEALTHY,
    WORKER_DOWN,
    WORKER_UP,
    MetadataStore,
    VersionConflict,
)
from ..utils import MAX_NAME_LENGTH, iso, validate_custom_metadata
from ..worker_client import WorkerClient
from .replication import ReplicationManager

log = logging.getLogger(__name__)


async def _read_exact(upload: UploadFile, size: int) -> bytes:
    """Read up to ``size`` bytes, looping over short reads."""
    parts: list[bytes] = []
    remaining = size
    while remaining > 0:
        data = await upload.read(remaining)
        if not data:
            break
        parts.append(data)
        remaining -= len(data)
    return b"".join(parts)


class ObjectService:
    def __init__(self, store: MetadataStore, client: WorkerClient, replication: ReplicationManager,
                 settings: Settings):
        self.store = store
        self.client = client
        self.replication = replication
        self.settings = settings
        self.rf = settings.replication_factor

    # ------------------------------------------------------------------ upload

    async def upload(self, upload: UploadFile, name: str, content_type: str, custom_metadata: dict) -> dict:
        if not name or len(name) > MAX_NAME_LENGTH:
            raise InvalidRequest(f"object name must be 1..{MAX_NAME_LENGTH} characters")
        live = self.store.list_workers(status=WORKER_UP)
        if len(live) < self.rf:
            raise ServiceUnavailable(
                f"{len(live)} storage worker(s) available, {self.rf} required to store an object",
                {"workers_up": len(live), "replication_factor": self.rf},
            )
        load = self.store.replica_load()
        ordered = sorted(live, key=lambda w: (load.get(w["worker_id"], 0), w["worker_id"]))

        object_id = str(uuid.uuid4())
        self.store.create_object(object_id, name, content_type, self.settings.chunk_size,
                                 custom_metadata, time.time())
        hasher = hashlib.sha256()
        size = 0
        index = 0
        failed_workers: set[str] = set()
        try:
            while True:
                data = await _read_exact(upload, self.settings.chunk_size)
                if not data:
                    break
                size += len(data)
                if self.settings.max_object_size and size > self.settings.max_object_size:
                    raise PayloadTooLarge(f"object exceeds the maximum size of {self.settings.max_object_size} bytes")
                hasher.update(data)
                chunk_id = f"{object_id}_{index:06d}"
                checksum = hashlib.sha256(data).hexdigest()
                self.store.add_chunk(chunk_id, object_id, index, len(data), checksum)
                await self._store_chunk(object_id, chunk_id, index, data, checksum, ordered, failed_workers)
                index += 1
            self.store.finalize_object(object_id, size, hasher.hexdigest(), index, time.time())
        except BaseException:
            await self._abort_upload(object_id)
            raise
        self.replication.event("info", "object_uploaded", f"Stored object {object_id} ({name}, {size} bytes, "
                               f"{index} chunks x {self.rf} copies)", object_id=object_id, size=size, chunks=index)
        return self.get_metadata(object_id)

    async def _store_chunk(self, object_id: str, chunk_id: str, index: int, data: bytes, checksum: str,
                           ordered: list[dict], failed_workers: set[str]) -> None:
        n = len(ordered)
        candidates = [ordered[(index + k) % n] for k in range(n)]
        candidates = [w for w in candidates if w["worker_id"] not in failed_workers]
        attempt, spare = candidates[: self.rf], candidates[self.rf:]
        stored = 0
        while attempt:
            results = await asyncio.gather(
                *(self.client.put_chunk(w["url"], chunk_id, data, checksum, object_id, index) for w in attempt),
                return_exceptions=True,
            )
            retry = []
            for worker, result in zip(attempt, results):
                if isinstance(result, BaseException):
                    log.warning("writing %s to %s failed: %s", chunk_id, worker["worker_id"], result)
                    failed_workers.add(worker["worker_id"])
                    if spare:
                        retry.append(spare.pop(0))
                else:
                    self.store.add_replica(chunk_id, worker["worker_id"], REPLICA_HEALTHY, time.time())
                    stored += 1
            attempt = retry
        if stored < self.rf:
            raise ServiceUnavailable(
                f"could only store {stored} of {self.rf} copies of chunk {index}; not enough healthy workers",
                {"chunk_index": index, "failed_workers": sorted(failed_workers)},
            )

    async def _abort_upload(self, object_id: str) -> None:
        try:
            pairs = self.store.object_replica_pairs(object_id)
            self.store.purge_object(object_id)
            await asyncio.shield(self.replication.delete_copies(pairs))
        except BaseException:  # leftovers are collected by reconciliation / GC
            log.exception("cleanup of aborted upload %s incomplete", object_id)

    # ------------------------------------------------------------------ download

    async def open_download(self, object_id: str) -> tuple[dict, AsyncIterator[bytes]]:
        obj = self._require(object_id)
        chunks = self.store.get_object_chunks(object_id)
        for chunk in chunks:
            if not any(r["worker_status"] != WORKER_DOWN for r in chunk["replicas"]):
                raise ServiceUnavailable(
                    f"chunk {chunk['idx']} of the object has no copy on an available worker",
                    {"chunk_id": chunk["chunk_id"],
                     "copies": [{"worker_id": r["worker_id"], "worker_status": r["worker_status"]}
                                for r in chunk["replicas"]]},
                )
        streamer = _ChunkStreamer(self.replication, chunks, self.settings.read_ahead_chunks)
        await streamer.prime()
        return obj, streamer.iterate(object_id)

    # ------------------------------------------------------------------ metadata

    def _require(self, object_id: str) -> dict:
        obj = self.store.get_object(object_id)
        if obj is None:
            raise ObjectNotFound(f"object '{object_id}' does not exist")
        return obj

    def get_metadata(self, object_id: str, include_chunks: bool = True) -> dict:
        obj = self._require(object_id)
        chunks = self.store.get_object_chunks(object_id)
        chunk_views = []
        min_available = None
        for chunk in chunks:
            replicas = [
                {
                    "worker_id": r["worker_id"],
                    "status": r["status"],
                    "worker_status": r["worker_status"],
                    "available": r["status"] == REPLICA_HEALTHY and r["worker_status"] == WORKER_UP,
                }
                for r in chunk["replicas"]
            ]
            healthy = sum(1 for r in replicas if r["available"])
            min_available = healthy if min_available is None else min(min_available, healthy)
            chunk_views.append({
                "chunk_id": chunk["chunk_id"],
                "index": chunk["idx"],
                "size": chunk["size"],
                "checksum": chunk["checksum"],
                "healthy_replicas": healthy,
                "replicas": replicas,
            })
        if min_available is None or min_available >= self.rf:
            replication_status = "healthy"
        elif min_available == 0:
            replication_status = "unavailable"
        else:
            replication_status = "degraded"
        view = self._summary(obj)
        view.update({
            "checksum": obj["checksum"],
            "checksum_algorithm": "sha256",
            "chunk_size": obj["chunk_size"],
            "replication_factor": self.rf,
            "replication_status": replication_status,
            "metadata_version": obj["metadata_version"],
            "custom_metadata": obj["custom_metadata"],
            "chunks": chunk_views if include_chunks else None,
        })
        return view

    def update_metadata(self, object_id: str, updates: dict, replace: bool = False,
                        expected_version: int | None = None) -> dict:
        validate_custom_metadata(updates, allow_null=True)
        try:
            obj = self.store.update_custom_metadata(object_id, updates, replace, expected_version, time.time())
        except VersionConflict as exc:
            raise Conflict(
                f"metadata was modified concurrently (current version {exc.current_version}, "
                f"If-Match was {expected_version})",
                {"current_version": exc.current_version},
            ) from exc
        if obj is None:
            raise ObjectNotFound(f"object '{object_id}' does not exist")
        return self.get_metadata(object_id)

    # ------------------------------------------------------------------ delete & list

    async def delete(self, object_id: str) -> None:
        if not self.store.begin_delete(object_id, time.time()):
            if self.store.get_object(object_id, statuses=(OBJECT_DELETING,)) is not None:
                raise Conflict(f"object '{object_id}' is already being deleted")
            raise ObjectNotFound(f"object '{object_id}' does not exist")
        pairs = self.store.object_replica_pairs(object_id)
        deferred = await self.replication.delete_copies(pairs)
        self.store.purge_object(object_id)
        self.replication.event("info", "object_deleted", f"Deleted object {object_id} ({len(pairs)} chunk copies"
                               + (f", {deferred} deferred until their worker returns)" if deferred else ")"),
                               object_id=object_id, copies=len(pairs), deferred=deferred)

    def list_objects(self, limit: int, offset: int, prefix: str | None) -> dict:
        rows, total = self.store.list_objects(limit, offset, prefix)
        next_offset = offset + len(rows)
        return {
            "objects": [self._summary(r) for r in rows],
            "total": total,
            "limit": limit,
            "offset": offset,
            "next_offset": next_offset if next_offset < total else None,
        }

    @staticmethod
    def _summary(obj: dict) -> dict:
        return {
            "object_id": obj["object_id"],
            "name": obj["name"],
            "size": obj["size"],
            "content_type": obj["content_type"],
            "created_at": iso(obj["created_at"]),
            "updated_at": iso(obj["updated_at"]),
            "num_chunks": obj["num_chunks"],
        }


class _ChunkStreamer:
    """Streams chunks in order while prefetching up to ``read_ahead`` more."""

    def __init__(self, replication: ReplicationManager, chunks: list[dict], read_ahead: int):
        self._replication = replication
        self._chunks = chunks
        self._window = max(0, read_ahead) + 1
        self._next = 0
        self._tasks: deque[asyncio.Task] = deque()
        self._first: bytes | None = None

    def _fill(self) -> None:
        while len(self._tasks) < self._window and self._next < len(self._chunks):
            task = asyncio.ensure_future(self._replication.read_chunk(self._chunks[self._next]))
            # Avoid "exception was never retrieved" warnings for cancelled prefetches.
            task.add_done_callback(lambda t: t.cancelled() or t.exception())
            self._tasks.append(task)
            self._next += 1

    def _close(self) -> None:
        while self._tasks:
            self._tasks.popleft().cancel()

    async def prime(self) -> None:
        """Fetch the first chunk eagerly so failures surface as HTTP errors."""
        self._fill()
        if self._tasks:
            try:
                self._first = await self._tasks.popleft()
            except BaseException:
                self._close()
                raise

    async def iterate(self, object_id: str) -> AsyncIterator[bytes]:
        try:
            if self._first is not None:
                first, self._first = self._first, None
                self._fill()
                yield first
            while self._tasks:
                data = await self._tasks.popleft()
                self._fill()
                yield data
        except Exception as exc:
            # Headers are already sent; the connection is aborted so the client
            # sees a truncated body rather than silently wrong data.
            self._replication.event("error", "download_failed", f"Download of {object_id} aborted: {exc}",
                                    object_id=object_id)
            raise
        finally:
            self._close()
