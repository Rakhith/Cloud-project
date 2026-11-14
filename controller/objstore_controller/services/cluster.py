"""Worker membership and failure detection.

Lifecycle of a worker as seen by the controller::

    register ──► SYNCING ──(inventory reconciled)──► UP
                    ▲                                 │
                    │ heartbeat resumes               │ no heartbeat for
                    │                                 ▼ heartbeat_timeout
                    └──────────────────────────────  DOWN

* ``register`` is called by a worker on start-up (and whenever the controller
  answers a heartbeat with 404, e.g. after the controller lost its database).
* ``heartbeat`` refreshes liveness; a heartbeat from a DOWN worker moves it
  back to SYNCING so its data is reconciled before it is trusted again.
* ``check_health`` (run periodically) marks silent workers DOWN.
"""

from __future__ import annotations

import time

from ..config import Settings
from ..models import HeartbeatRequest, RegisterRequest
from ..store import WORKER_DOWN, WORKER_SYNCING, WORKER_UP, MetadataStore
from ..utils import iso
from .replication import ReplicationManager


class ClusterService:
    def __init__(self, store: MetadataStore, settings: Settings, replication: ReplicationManager):
        self.store = store
        self.settings = settings
        self.replication = replication

    # ------------------------------------------------------------------ membership

    def register(self, req: RegisterRequest) -> dict:
        now = time.time()
        prev = self.store.upsert_worker(req.worker_id, req.url.rstrip("/"), WORKER_SYNCING, now, req.model_dump())
        if prev is None:
            self.replication.event("info", "worker_registered", f"Worker {req.worker_id} registered at {req.url}",
                                   worker_id=req.worker_id, url=req.url)
        else:
            self.replication.event("info", "worker_rejoined",
                                   f"Worker {req.worker_id} re-registered (was {prev['status']})",
                                   worker_id=req.worker_id, previous_status=prev["status"])
        self.replication.wake()  # reconcile it right away
        return {
            "worker_id": req.worker_id,
            "status": WORKER_SYNCING,
            "heartbeat_interval": max(1.0, self.settings.heartbeat_timeout / 3),
            "replication_factor": self.settings.replication_factor,
            "chunk_size": self.settings.chunk_size,
        }

    def heartbeat(self, req: HeartbeatRequest) -> dict | None:
        """Returns None for unknown workers (they must register again)."""
        now = time.time()
        prev = self.store.touch_worker(req.worker_id, now, req.model_dump())
        if prev is None:
            return None
        status = prev["status"]
        if status == WORKER_DOWN and self.store.set_worker_status(req.worker_id, WORKER_SYNCING, now, expected=WORKER_DOWN):
            status = WORKER_SYNCING
            self.replication.event("info", "worker_recovered",
                                   f"Worker {req.worker_id} is heartbeating again; reconciling its data",
                                   worker_id=req.worker_id, down_for_seconds=round(now - prev["status_changed_at"], 1))
            self.replication.wake()
        for chunk_id in req.corrupt_chunks:
            self.replication.mark_corrupt(chunk_id, req.worker_id, "worker scrubber")
        return {"worker_id": req.worker_id, "status": status}

    def check_health(self, now: float | None = None) -> list[str]:
        """Mark workers whose heartbeats stopped as DOWN. Returns their ids."""
        now = now if now is not None else time.time()
        newly_down = []
        for worker in self.store.stale_workers(now - self.settings.heartbeat_timeout):
            if self.store.set_worker_status(worker["worker_id"], WORKER_DOWN, now, expected=worker["status"]):
                newly_down.append(worker["worker_id"])
                affected = len(self.store.replicas_on_worker(worker["worker_id"]))
                self.replication.event(
                    "warning", "worker_down",
                    f"Worker {worker['worker_id']} missed heartbeats for {round(now - worker['last_heartbeat'], 1)}s "
                    f"and is DOWN; {affected} chunk copies are unavailable",
                    worker_id=worker["worker_id"], affected_copies=affected,
                )
        if newly_down:
            self.replication.wake()
        return newly_down

    def decommission(self, worker_id: str) -> int | None:
        if self.store.get_worker(worker_id) is None:
            return None
        removed = self.store.remove_worker(worker_id)
        self.replication.event("warning", "worker_decommissioned",
                               f"Worker {worker_id} removed from the cluster ({removed} copies forgotten)",
                               worker_id=worker_id, copies=removed)
        self.replication.wake()
        return removed

    # ------------------------------------------------------------------ views

    def worker_view(self, worker: dict, load: dict[str, int], now: float) -> dict:
        return {
            "worker_id": worker["worker_id"],
            "url": worker["url"],
            "status": worker["status"],
            "last_heartbeat": iso(worker["last_heartbeat"]),
            "seconds_since_heartbeat": round(now - worker["last_heartbeat"], 1),
            "status_changed_at": iso(worker["status_changed_at"]),
            "registered_at": iso(worker["registered_at"]),
            "chunk_count": worker["chunk_count"],
            "replicas_recorded": load.get(worker["worker_id"], 0),
            "used_bytes": worker["used_bytes"],
            "free_bytes": worker["free_bytes"],
            "capacity_bytes": worker["capacity_bytes"],
        }

    def list_workers(self) -> list[dict]:
        now = time.time()
        load = self.store.replica_load()
        return [self.worker_view(w, load, now) for w in self.store.list_workers()]

    def status(self) -> dict:
        workers = self.list_workers()
        counts = {s: sum(1 for w in workers if w["status"] == s) for s in (WORKER_UP, WORKER_SYNCING, WORKER_DOWN)}
        stats = self.store.cluster_stats(self.settings.replication_factor)
        return {
            "replication_factor": self.settings.replication_factor,
            "chunk_size": self.settings.chunk_size,
            "workers_total": len(workers),
            "workers_up": counts[WORKER_UP],
            "workers_syncing": counts[WORKER_SYNCING],
            "workers_down": counts[WORKER_DOWN],
            "writable": counts[WORKER_UP] >= self.settings.replication_factor,
            **stats,
            "replication_healthy": stats["chunks"] == stats["fully_replicated"] and stats["over_replicated"] == 0
            and stats["corrupt_replicas"] == 0,
            "workers": workers,
        }
