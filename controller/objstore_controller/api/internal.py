"""Internal controller endpoints called by workers (section 6)."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from ..errors import ObjectNotFound
from ..models import HeartbeatRequest, HeartbeatResponse, RegisterRequest, RegisterResponse
from ..services import Services
from .deps import get_services, require_internal_token

router = APIRouter(prefix="/internal", tags=["internal"], dependencies=[Depends(require_internal_token)])


@router.post("/workers/register", response_model=RegisterResponse, summary="Register a worker")
async def register(req: RegisterRequest, svc: Services = Depends(get_services)):
    """Called by a worker on start-up. The worker enters SYNCING; the
    controller reconciles its inventory and then marks it UP."""
    return svc.cluster.register(req)


@router.post("/workers/heartbeat", response_model=HeartbeatResponse, summary="Report worker availability")
async def heartbeat(req: HeartbeatRequest, svc: Services = Depends(get_services)):
    """Periodic liveness report, including chunks the worker's own scrubber
    found corrupted. Unknown workers get 404 and must register again."""
    result = svc.cluster.heartbeat(req)
    if result is None:
        raise ObjectNotFound(f"worker '{req.worker_id}' is not registered")
    return result


@router.get("/workers/{worker_id}/chunks", summary="Chunks the controller expects on a worker")
async def expected_chunks(worker_id: str, svc: Services = Depends(get_services)):
    """Controller-side view; the worker serves its *actual* inventory on the
    same path."""
    if svc.store.get_worker(worker_id) is None:
        raise ObjectNotFound(f"worker '{worker_id}' is not registered")
    rows = svc.store.replicas_on_worker(worker_id)
    return {
        "worker_id": worker_id,
        "count": len(rows),
        "chunks": [{"chunk_id": r["chunk_id"], "size": r["size"], "checksum": r["checksum"], "status": r["status"]}
                   for r in rows],
    }
