"""Replication, repair and data-integrity management.

The ``ReplicationManager`` owns everything that keeps the *physical* state of
the workers in line with the controller's metadata:

* **Verified reads** (``read_chunk``): fetch a chunk from any copy, check its
  SHA-256 against the controller's record, and never return corrupted data
  when another valid copy exists. Bad copies are flagged for repair.
* **Repair** (``_repair_chunk``): for each chunk that does not have exactly
  ``replication_factor`` healthy copies on UP workers:
    - corrupt copies on live workers are overwritten in place from a good copy;
    - missing copies (worker DOWN beyond the grace period) are re-created on
      the least-loaded worker that does not already hold the chunk;
    - surplus copies (e.g. after a failed worker comes back) are trimmed.
* **Reconciliation** (``reconcile_worker``): when a worker (re)joins, compare
  its actual inventory with the metadata: confirm/verify copies, drop records
  of lost copies, adopt valid unrecorded copies and delete orphans.
* **Scrubbing** (``scrub``): on-demand re-verification of every copy.
* **Garbage collection**: retry deletes that failed while a worker was down and
  clean up uploads/deletions interrupted by a controller crash.

All mutating maintenance runs under one asyncio lock so that repair, reconcile
and scrub never race each other.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import time
from typing import Iterable

from ..config import Settings
from ..errors import DataIntegrityError, ServiceUnavailable
from ..store import (
    OBJECT_ACTIVE,
    OBJECT_PENDING,
    REPLICA_CORRUPT,
    REPLICA_HEALTHY,
    WORKER_DOWN,
    WORKER_SYNCING,
    WORKER_UP,
    MetadataStore,
)
from ..worker_client import (
    ChunkCorruptedOnWorker,
    ChunkNotFoundOnWorker,
    WorkerClient,
    WorkerError,
)

log = logging.getLogger(__name__)

_WORKER_READ_PREFERENCE = {WORKER_UP: 0, WORKER_SYNCING: 1, WORKER_DOWN: 2}


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ReplicationManager:
    def __init__(self, store: MetadataStore, client: WorkerClient, settings: Settings):
        self.store = store
        self.client = client
        self.settings = settings
        self.rf = settings.replication_factor
        self._lock = asyncio.Lock()
        self._wake_event = asyncio.Event()
        # De-duplicate noisy "cannot repair" events between maintenance rounds.
        self._reported_unrecoverable: set[str] = set()
        self._reported_no_target: set[str] = set()

    # ------------------------------------------------------------------ events / wakeups

    def event(self, level: str, type_: str, message: str, **details) -> None:
        {"error": log.error, "warning": log.warning}.get(level, log.info)("[%s] %s", type_, message)
        self.store.add_event(level, type_, message, details)

    def wake(self) -> None:
        """Ask the maintenance loop to run as soon as possible."""
        self._wake_event.set()

    async def wait_for_wake(self, timeout: float) -> None:
        try:
            await asyncio.wait_for(self._wake_event.wait(), timeout)
        except asyncio.TimeoutError:
            pass
        finally:
            self._wake_event.clear()

    # ------------------------------------------------------------------ replica state changes

    def mark_corrupt(self, chunk_id: str, worker_id: str, source: str) -> bool:
        if self.store.set_replica_status(chunk_id, worker_id, REPLICA_CORRUPT, time.time(), only_if=REPLICA_HEALTHY):
            self.event(
                "error", "corruption_detected",
                f"Corrupted copy of chunk {chunk_id} on {worker_id} (detected by {source})",
                chunk_id=chunk_id, worker_id=worker_id, source=source,
            )
            self.wake()
            return True
        return False

    def mark_missing(self, chunk_id: str, worker_id: str, source: str) -> bool:
        if self.store.remove_replica(chunk_id, worker_id):
            self.event(
                "warning", "replica_missing",
                f"Copy of chunk {chunk_id} is missing on {worker_id} (detected by {source})",
                chunk_id=chunk_id, worker_id=worker_id, source=source,
            )
            self.wake()
            return True
        return False

    # ------------------------------------------------------------------ verified reads

    async def read_chunk(self, chunk: dict) -> bytes:
        """Return the data of ``chunk`` from any valid copy.

        Copies are tried healthy-first, then by worker liveness, with random
        tie-breaking to spread read load. Every copy is verified against the
        controller's checksum. Raises ``DataIntegrityError`` if every reachable
        copy is corrupt and ``ServiceUnavailable`` if no copy is reachable.
        """
        tried: set[str] = set()
        corrupt_seen = 0
        for attempt in range(2):
            candidates = [r for r in chunk["replicas"] if r["worker_id"] not in tried]
            candidates.sort(key=lambda r: (
                r["status"] != REPLICA_HEALTHY,
                _WORKER_READ_PREFERENCE.get(r["worker_status"], 3),
                random.random(),
            ))
            for replica in candidates:
                tried.add(replica["worker_id"])
                if not replica.get("worker_url"):
                    continue
                result = await self._read_copy(chunk, replica, source="read")
                if isinstance(result, bytes):
                    return result
                if result == "corrupt":
                    corrupt_seen += 1
            # Placement may have changed while we were reading (repair/trim):
            # reload once and try copies we have not seen yet.
            if attempt == 0:
                fresh = self.store.get_chunk(chunk["chunk_id"])
                if fresh is None:
                    break
                chunk = fresh
        if corrupt_seen and corrupt_seen == len(tried):
            raise DataIntegrityError(
                f"all {corrupt_seen} copies of chunk {chunk['idx']} are corrupted",
                {"chunk_id": chunk["chunk_id"]},
            )
        raise ServiceUnavailable(
            f"no valid copy of chunk {chunk['idx']} is currently reachable",
            {"chunk_id": chunk["chunk_id"], "copies_tried": sorted(tried)},
        )

    async def _read_copy(self, chunk: dict, replica: dict, source: str) -> bytes | str:
        """Read one copy. Returns the data, or "corrupt" / "missing" / "unavailable"."""
        chunk_id, worker_id = chunk["chunk_id"], replica["worker_id"]
        try:
            data = await self.client.get_chunk(replica["worker_url"], chunk_id)
        except ChunkCorruptedOnWorker:
            self.mark_corrupt(chunk_id, worker_id, f"{source} (worker checksum)")
            return "corrupt"
        except ChunkNotFoundOnWorker:
            self.mark_missing(chunk_id, worker_id, source)
            return "missing"
        except WorkerError as exc:
            log.info("copy of %s on %s unavailable: %s", chunk_id, worker_id, exc)
            return "unavailable"
        if len(data) != chunk["size"] or sha256_hex(data) != chunk["checksum"]:
            self.mark_corrupt(chunk_id, worker_id, f"{source} (controller checksum)")
            return "corrupt"
        return data

    # ------------------------------------------------------------------ deleting copies

    async def delete_copies(self, pairs: Iterable[tuple[str, str]]) -> int:
        """Delete chunk copies from workers; failures are queued for retry.

        Returns the number of copies that could not be deleted right now.
        """
        workers = {w["worker_id"]: w for w in self.store.list_workers()}
        sem = asyncio.Semaphore(16)
        deferred: list[tuple[str, str]] = []

        async def delete_one(chunk_id: str, worker_id: str) -> None:
            worker = workers.get(worker_id)
            if worker is None:
                return  # decommissioned worker: nothing to do
            if worker["status"] == WORKER_DOWN:
                deferred.append((chunk_id, worker_id))
                return
            async with sem:
                try:
                    await self.client.delete_chunk(worker["url"], chunk_id)
                except WorkerError:
                    deferred.append((chunk_id, worker_id))

        await asyncio.gather(*(delete_one(c, w) for c, w in pairs))
        if deferred:
            self.store.add_pending_deletes(deferred, time.time())
        return len(deferred)

    # ------------------------------------------------------------------ maintenance entry points

    async def run_maintenance_once(self) -> dict:
        """One full maintenance round: reconcile, repair/trim, GC."""
        async with self._lock:
            summary = {
                "reconciled_workers": [],
                "restored_in_place": 0,
                "re_replicated": 0,
                "trimmed": 0,
                "dropped_corrupt": 0,
                "repair_failures": 0,
                "unrecoverable_chunks": 0,
                "pending_deletes_done": 0,
                "incomplete_objects_cleaned": 0,
            }
            for worker in self.store.list_workers(status=WORKER_SYNCING):
                result = await self._reconcile_locked(worker["worker_id"], verify=self.settings.reconcile_verify)
                if result is not None:
                    summary["reconciled_workers"].append(result)
            await self._repair_all(summary)
            summary["pending_deletes_done"] = await self._process_pending_deletes()
            summary["incomplete_objects_cleaned"] = await self._cleanup_incomplete(
                time.time() - self.settings.incomplete_upload_ttl
            )
            return summary

    async def reconcile_worker(self, worker_id: str, verify: bool = True) -> dict | None:
        async with self._lock:
            return await self._reconcile_locked(worker_id, verify)

    async def scrub(self) -> dict:
        """Re-verify every copy on every reachable worker, then repair."""
        async with self._lock:
            results = []
            for worker in self.store.list_workers():
                if worker["status"] == WORKER_DOWN:
                    results.append({"worker_id": worker["worker_id"], "skipped": "worker is DOWN"})
                    continue
                result = await self._reconcile_locked(worker["worker_id"], verify=True)
                results.append(result or {"worker_id": worker["worker_id"], "skipped": "unreachable"})
            self.event("info", "scrub_completed", "Scrub of all reachable workers completed",
                       corrupt=sum(r.get("corrupt", 0) for r in results),
                       missing=sum(r.get("missing", 0) for r in results))
        repair = await self.run_maintenance_once()
        return {"workers": results, "repair": repair}

    async def cleanup_incomplete_objects(self, cutoff: float) -> int:
        async with self._lock:
            return await self._cleanup_incomplete(cutoff)

    # ------------------------------------------------------------------ reconciliation

    async def _reconcile_locked(self, worker_id: str, verify: bool) -> dict | None:
        worker = self.store.get_worker(worker_id)
        if worker is None:
            return None
        # Read our records *before* the inventory: a record is only written
        # after the worker acknowledged the chunk, so every recorded copy must
        # appear in an inventory taken afterwards.
        records = {r["chunk_id"]: r for r in self.store.replicas_on_worker(worker_id)}
        try:
            inventory = await self.client.list_chunks(worker["url"], worker_id, verify=verify)
        except WorkerError as exc:
            log.warning("cannot reconcile %s: %s", worker_id, exc)
            return None

        now = time.time()
        stats = {"worker_id": worker_id, "checked": len(inventory), "verified": 0, "corrupt": 0,
                 "missing": 0, "adopted": 0, "orphans_deleted": 0}
        on_disk = {c["chunk_id"]: c for c in inventory}

        for chunk_id, rec in records.items():
            item = on_disk.get(chunk_id)
            if item is None:
                if self.mark_missing(chunk_id, worker_id, "reconciliation"):
                    stats["missing"] += 1
                continue
            valid = (item.get("valid", True) and item.get("checksum") == rec["checksum"]
                     and item.get("size") == rec["size"])
            if valid:
                stats["verified"] += 1
                if rec["status"] != REPLICA_HEALTHY:
                    self.store.set_replica_status(chunk_id, worker_id, REPLICA_HEALTHY, now)
            else:
                stats["corrupt"] += 1
                self.mark_corrupt(chunk_id, worker_id, "reconciliation")

        unrecorded = [cid for cid in on_disk if cid not in records]
        known = self.store.chunk_info(unrecorded)
        orphans: list[str] = []
        for chunk_id in unrecorded:
            item, info = on_disk[chunk_id], known.get(chunk_id)
            if (info is not None and info["object_status"] in (OBJECT_ACTIVE, OBJECT_PENDING)
                    and item.get("valid", True) and item.get("checksum") == info["checksum"]):
                # A valid copy we did not know about (e.g. the controller
                # crashed between the write and recording it): keep it.
                if self.store.add_replica(chunk_id, worker_id, REPLICA_HEALTHY, now):
                    stats["adopted"] += 1
            else:
                # Unknown chunk, deleted object, or an invalid unrecorded copy.
                orphans.append(chunk_id)
        for chunk_id in orphans:
            try:
                await self.client.delete_chunk(worker["url"], chunk_id)
                stats["orphans_deleted"] += 1
            except WorkerError:
                pass

        if worker["status"] == WORKER_SYNCING and self.store.set_worker_status(
            worker_id, WORKER_UP, time.time(), expected=WORKER_SYNCING
        ):
            self.event("info", "worker_synced", f"Worker {worker_id} reconciled and is UP", **stats)
        elif stats["corrupt"] or stats["missing"] or stats["orphans_deleted"] or stats["adopted"]:
            self.event("info", "worker_reconciled", f"Worker {worker_id} reconciled", **stats)
        if stats["corrupt"] or stats["missing"]:
            self.wake()
        return stats

    # ------------------------------------------------------------------ repair

    async def _repair_all(self, summary: dict) -> None:
        chunk_ids = self.store.chunks_needing_attention(self.rf)
        if not chunk_ids:
            self._reported_unrecoverable.clear()
            self._reported_no_target.clear()
            return
        sem = asyncio.Semaphore(max(1, self.settings.repair_concurrency))

        async def run(chunk_id: str) -> None:
            async with sem:
                try:
                    await self._repair_chunk(chunk_id, summary)
                except Exception:  # never let one chunk stop the round
                    log.exception("repair of chunk %s failed", chunk_id)
                    summary["repair_failures"] += 1

        await asyncio.gather(*(run(cid) for cid in chunk_ids))

    async def _repair_chunk(self, chunk_id: str, summary: dict) -> None:
        chunk = self.store.get_chunk(chunk_id)
        if chunk is None or chunk["object_status"] != OBJECT_ACTIVE:
            return
        now = time.time()
        replicas = chunk["replicas"]
        good = [r for r in replicas if r["status"] == REPLICA_HEALTHY and r["worker_status"] == WORKER_UP]
        corrupt_live = [r for r in replicas if r["status"] == REPLICA_CORRUPT and r["worker_status"] == WORKER_UP]

        if len(good) >= self.rf:
            await self._trim(chunk, good, corrupt_live, summary)
            return

        if not good:
            if chunk_id not in self._reported_unrecoverable:
                self._reported_unrecoverable.add(chunk_id)
                self.event(
                    "error", "chunk_unavailable",
                    f"Chunk {chunk['idx']} of object {chunk['object_id']} has no healthy copy on an UP worker",
                    chunk_id=chunk_id, object_id=chunk["object_id"],
                    copies=[f"{r['worker_id']}:{r['status']}:{r['worker_status']}" for r in replicas],
                )
            summary["unrecoverable_chunks"] += 1
            return
        self._reported_unrecoverable.discard(chunk_id)

        # Copies that may come back soon do not need to be re-created yet.
        grace = self.settings.repair_grace_period
        waiting = [
            r for r in replicas
            if r["status"] == REPLICA_HEALTHY and (
                r["worker_status"] == WORKER_SYNCING
                or (r["worker_status"] == WORKER_DOWN and now - (r["worker_status_changed_at"] or 0) < grace)
            )
        ]
        deficit = self.rf - len(good) - len(waiting)

        # 1) always restore corrupt copies on live workers in place;
        targets: list[tuple[str, str, bool]] = [(r["worker_id"], r["worker_url"], True) for r in corrupt_live]
        # 2) then create new copies on other workers for whatever is still missing.
        if deficit > len(targets):
            holders = {r["worker_id"] for r in replicas}
            load = self.store.replica_load()
            candidates = sorted(
                (w for w in self.store.list_workers(status=WORKER_UP) if w["worker_id"] not in holders),
                key=lambda w: (load.get(w["worker_id"], 0), w["worker_id"]),
            )
            needed = deficit - len(targets)
            targets += [(w["worker_id"], w["url"], False) for w in candidates[:needed]]
            if len(candidates) < needed and chunk_id not in self._reported_no_target:
                self._reported_no_target.add(chunk_id)
                self.event(
                    "warning", "replication_degraded",
                    f"Chunk {chunk['idx']} of object {chunk['object_id']} has {len(good)}/{self.rf} copies "
                    f"and no spare UP worker to copy it to",
                    chunk_id=chunk_id, object_id=chunk["object_id"],
                )
        if not targets:
            return

        data = None
        for replica in sorted(good, key=lambda _: random.random()):
            result = await self._read_copy(chunk, replica, source="repair")
            if isinstance(result, bytes):
                data = result
                break
        if data is None:
            summary["repair_failures"] += 1
            return

        for worker_id, url, in_place in targets:
            try:
                await self.client.put_chunk(url, chunk_id, data, chunk["checksum"], chunk["object_id"], chunk["idx"])
            except WorkerError as exc:
                log.warning("repair write of %s to %s failed: %s", chunk_id, worker_id, exc)
                summary["repair_failures"] += 1
                continue
            if not self.store.add_replica(chunk_id, worker_id, REPLICA_HEALTHY, time.time()):
                # The object was deleted while we were copying.
                await self.delete_copies([(chunk_id, worker_id)])
                return
            if in_place:
                summary["restored_in_place"] += 1
                self.event("info", "replica_restored",
                           f"Restored corrupted copy of chunk {chunk_id} on {worker_id}",
                           chunk_id=chunk_id, worker_id=worker_id)
            else:
                summary["re_replicated"] += 1
                self.event("info", "replica_created",
                           f"Re-replicated chunk {chunk_id} to {worker_id}",
                           chunk_id=chunk_id, worker_id=worker_id)
        self._reported_no_target.discard(chunk_id)

    async def _trim(self, chunk: dict, good: list[dict], corrupt_live: list[dict], summary: dict) -> None:
        """Chunk has enough healthy copies: drop corrupt and surplus copies."""
        chunk_id = chunk["chunk_id"]
        for replica in corrupt_live:
            if await self._remove_copy(chunk_id, replica):
                summary["dropped_corrupt"] += 1
                self.event("info", "corrupt_replica_dropped",
                           f"Dropped corrupted copy of chunk {chunk_id} on {replica['worker_id']} "
                           f"(enough healthy copies elsewhere)",
                           chunk_id=chunk_id, worker_id=replica["worker_id"])
        surplus = len(good) - self.rf
        if surplus <= 0:
            return
        load = self.store.replica_load()
        victims = sorted(good, key=lambda r: (-load.get(r["worker_id"], 0), r["worker_id"]))[:surplus]
        for replica in victims:
            if await self._remove_copy(chunk_id, replica):
                summary["trimmed"] += 1
                self.event("info", "replica_trimmed",
                           f"Removed surplus copy of chunk {chunk_id} from {replica['worker_id']}",
                           chunk_id=chunk_id, worker_id=replica["worker_id"])

    async def _remove_copy(self, chunk_id: str, replica: dict) -> bool:
        try:
            await self.client.delete_chunk(replica["worker_url"], chunk_id)
        except WorkerError as exc:
            log.warning("could not delete %s from %s: %s", chunk_id, replica["worker_id"], exc)
            return False
        self.store.remove_replica(chunk_id, replica["worker_id"])
        return True

    # ------------------------------------------------------------------ garbage collection

    async def _process_pending_deletes(self) -> int:
        done = 0
        for item in self.store.list_pending_deletes():
            if item["worker_url"] is None:  # worker was decommissioned
                self.store.remove_pending_delete(item["chunk_id"], item["worker_id"])
                continue
            if item["worker_status"] == WORKER_DOWN:
                continue
            try:
                await self.client.delete_chunk(item["worker_url"], item["chunk_id"])
            except WorkerError:
                self.store.bump_pending_delete(item["chunk_id"], item["worker_id"])
                continue
            self.store.remove_pending_delete(item["chunk_id"], item["worker_id"])
            done += 1
        return done

    async def _cleanup_incomplete(self, cutoff: float) -> int:
        object_ids = self.store.incomplete_objects(cutoff)
        for object_id in object_ids:
            pairs = self.store.object_replica_pairs(object_id)
            await self.delete_copies(pairs)
            self.store.purge_object(object_id)
            self.event("warning", "incomplete_object_cleaned",
                       f"Cleaned up interrupted upload/delete of object {object_id}",
                       object_id=object_id, copies=len(pairs))
        return len(object_ids)
