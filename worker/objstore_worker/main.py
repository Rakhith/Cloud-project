"""Storage worker application.

Run with::

    uvicorn --factory objstore_worker.main:create_app --host 0.0.0.0 --port 8000

Internal API (section 6 of the specification):

====== ===================================== ====================================
PUT    /internal/chunks/{chunk_id}           store a chunk (raw bytes body)
GET    /internal/chunks/{chunk_id}           read a chunk (verified, 409 if corrupt)
DELETE /internal/chunks/{chunk_id}           delete a chunk
GET    /internal/workers/{worker_id}/chunks  inventory (``?verify=true`` re-hashes)
POST   /internal/chunks/{chunk_id}/verify    re-hash a single chunk
GET    /health                               liveness + storage stats
====== ===================================== ====================================
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import Depends, FastAPI, Header, Query, Request
from fastapi.responses import JSONResponse, Response
from starlette.concurrency import run_in_threadpool

from . import __version__
from .config import WorkerSettings
from .controller_link import ControllerLink, scrub_loop
from .storage import ChecksumMismatch, ChunkCorrupted, ChunkNotFound, ChunkStore, InvalidChunkId

log = logging.getLogger("objstore_worker")


def _error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse({"error": {"status": status, "code": code, "message": message}}, status_code=status)


def create_app(settings: WorkerSettings | None = None, *,
               controller_transport: httpx.AsyncBaseTransport | None = None,
               run_background: bool = True) -> FastAPI:
    settings = settings or WorkerSettings.from_env()
    logging.basicConfig(level=settings.log_level.upper(),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    store = ChunkStore(settings.data_dir)
    link = ControllerLink(settings, store, transport=controller_transport)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        tasks = []
        if run_background:
            tasks = [asyncio.create_task(link.run()), asyncio.create_task(scrub_loop(store, settings.scrub_interval))]
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await link.close()

    app = FastAPI(title=f"Object Store Worker ({settings.worker_id})", version=__version__, lifespan=lifespan)
    app.state.store = store
    app.state.settings = settings
    app.state.link = link

    def require_token(x_internal_token: str | None = Header(None)) -> None:
        if settings.internal_token and x_internal_token != settings.internal_token:
            raise PermissionError("missing or invalid X-Internal-Token")

    @app.exception_handler(InvalidChunkId)
    async def _invalid_id(request: Request, exc: InvalidChunkId):
        return _error(400, "invalid_request", str(exc))

    @app.exception_handler(PermissionError)
    async def _forbidden(request: Request, exc: PermissionError):
        return _error(401, "unauthorized", str(exc))

    internal = [Depends(require_token)]

    @app.get("/health")
    async def health():
        stats = await run_in_threadpool(store.stats)
        return {"status": "ok", "role": "worker", "worker_id": settings.worker_id,
                "registered": link.registered, "corrupt_chunks": store.corrupt_chunks(), **stats}

    @app.put("/internal/chunks/{chunk_id}", dependencies=internal, status_code=201)
    async def put_chunk(
        chunk_id: str,
        request: Request,
        x_chunk_checksum: str | None = Header(None, description="Expected SHA-256 of the body"),
        x_object_id: str | None = Header(None),
        x_chunk_index: int | None = Header(None),
    ):
        declared = request.headers.get("content-length")
        if declared and int(declared) > settings.max_chunk_bytes:
            return _error(413, "payload_too_large", f"chunks are limited to {settings.max_chunk_bytes} bytes")
        data = await request.body()
        if len(data) > settings.max_chunk_bytes:
            return _error(413, "payload_too_large", f"chunks are limited to {settings.max_chunk_bytes} bytes")
        try:
            meta, created = await run_in_threadpool(
                store.put, chunk_id, data, x_chunk_checksum, {"object_id": x_object_id, "index": x_chunk_index}
            )
        except ChecksumMismatch as exc:
            return _error(400, "checksum_mismatch", str(exc))
        return JSONResponse({"chunk_id": chunk_id, "size": meta["size"], "checksum": meta["checksum"],
                             "created": created}, status_code=201 if created else 200)

    @app.get("/internal/chunks/{chunk_id}", dependencies=internal)
    async def get_chunk(chunk_id: str, verify: bool = Query(True, description="Re-hash before returning")):
        try:
            data, meta = await run_in_threadpool(store.get, chunk_id, verify)
        except ChunkNotFound:
            return _error(404, "not_found", f"chunk {chunk_id} not found")
        except ChunkCorrupted as exc:
            log.error("refusing to serve corrupted chunk %s", chunk_id)
            return _error(409, "chunk_corrupted", str(exc))
        return Response(content=data, media_type="application/octet-stream",
                        headers={"X-Chunk-Checksum": meta.get("checksum") or "", "X-Chunk-Size": str(len(data))})

    @app.delete("/internal/chunks/{chunk_id}", dependencies=internal, status_code=204)
    async def delete_chunk(chunk_id: str):
        existed = await run_in_threadpool(store.delete, chunk_id)
        if not existed:
            return _error(404, "not_found", f"chunk {chunk_id} not found")
        return Response(status_code=204)

    @app.post("/internal/chunks/{chunk_id}/verify", dependencies=internal)
    async def verify_chunk(chunk_id: str):
        try:
            valid = await run_in_threadpool(store.verify, chunk_id)
        except ChunkNotFound:
            return _error(404, "not_found", f"chunk {chunk_id} not found")
        return {"chunk_id": chunk_id, "valid": valid}

    @app.get("/internal/workers/{worker_id}/chunks", dependencies=internal)
    async def list_chunks(worker_id: str, verify: bool = Query(False, description="Re-hash every chunk")):
        if worker_id != settings.worker_id:
            return _error(404, "not_found", f"this is worker '{settings.worker_id}', not '{worker_id}'")
        chunks = await run_in_threadpool(store.list, verify)
        return {"worker_id": settings.worker_id, "count": len(chunks),
                "used_bytes": sum(c["size"] for c in chunks), "chunks": chunks}

    return app
