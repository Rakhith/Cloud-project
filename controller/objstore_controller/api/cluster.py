"""Cluster observability and administration API (beyond the required API).

Used by the demo scripts, the CLI and the dashboard to show worker health,
chunk placement, replication status and the event log.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query

from ..errors import ObjectNotFound
from ..models import ClusterStatus, Event, WorkerInfo
from ..services import Services
from ..utils import iso
from .deps import get_services

router = APIRouter(prefix="/api/v1", tags=["cluster"])


@router.get("/health", summary="Controller liveness")
async def health(svc: Services = Depends(get_services)):
    return {"status": "ok", "role": "controller"}


@router.get("/cluster", response_model=ClusterStatus, summary="Cluster and replication status")
async def cluster_status(svc: Services = Depends(get_services)):
    return svc.cluster.status()


@router.get("/workers", response_model=list[WorkerInfo], summary="List workers")
async def list_workers(svc: Services = Depends(get_services)):
    return svc.cluster.list_workers()


@router.get("/workers/{worker_id}/chunks", summary="Chunk copies the controller has recorded on a worker")
async def worker_chunks(worker_id: str, svc: Services = Depends(get_services)):
    if svc.store.get_worker(worker_id) is None:
        raise ObjectNotFound(f"worker '{worker_id}' is not registered")
    rows = svc.store.replicas_on_worker(worker_id)
    return {
        "worker_id": worker_id,
        "count": len(rows),
        "chunks": [
            {"chunk_id": r["chunk_id"], "object_id": r["object_id"], "index": r["idx"], "size": r["size"],
             "checksum": r["checksum"], "status": r["status"]}
            for r in rows
        ],
    }


@router.get("/events", response_model=list[Event], summary="Recent cluster events (newest first)")
async def events(
    limit: int = Query(50, ge=1, le=1000),
    after_id: int = Query(0, ge=0, description="Only events with a larger id"),
    level: Optional[str] = Query(None, pattern="^(info|warning|error)$"),
    svc: Services = Depends(get_services),
):
    return [
        {"id": e["id"], "time": iso(e["ts"]), "level": e["level"], "type": e["type"],
         "message": e["message"], "details": e["details"]}
        for e in svc.store.list_events(limit, after_id, level)
    ]


@router.post("/admin/scrub", tags=["admin"], summary="Verify every stored copy now, then repair")
async def scrub(svc: Services = Depends(get_services)):
    """Asks every reachable worker to re-hash all of its chunks, compares the
    result with the controller's checksums and repairs what is broken."""
    return await svc.replication.scrub()


@router.post("/admin/repair", tags=["admin"], summary="Run one repair/maintenance round now")
async def repair(svc: Services = Depends(get_services)):
    return await svc.replication.run_maintenance_once()


@router.delete("/admin/workers/{worker_id}", tags=["admin"], summary="Decommission a worker permanently")
async def decommission(worker_id: str, svc: Services = Depends(get_services)):
    """Forget a worker that will never return; its chunks are re-replicated
    from the remaining copies."""
    removed = svc.cluster.decommission(worker_id)
    if removed is None:
        raise ObjectNotFound(f"worker '{worker_id}' is not registered")
    return {"worker_id": worker_id, "copies_forgotten": removed}
