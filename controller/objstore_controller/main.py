"""Controller application factory.

Run with::

    uvicorn --factory objstore_controller.main:create_app --host 0.0.0.0 --port 8000

``create_app`` accepts an optional ``worker_transport`` and
``run_background=False`` so the test-suite can wire the controller to
in-process workers and drive the background loops by hand.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import __version__
from .api import cluster as cluster_api
from .api import internal as internal_api
from .api import objects as objects_api
from .config import Settings
from .errors import ObjectStoreError, error_body
from .services import Services
from .services.cluster import ClusterService
from .services.objects import ObjectService
from .services.replication import ReplicationManager
from .store import MetadataStore
from .worker_client import WorkerClient

log = logging.getLogger("objstore_controller")

STATIC_DIR = Path(__file__).parent / "static"

DESCRIPTION = """
A mini distributed object store: objects are split into **1 MB chunks**, each
chunk is stored on **2 different workers**, and the controller keeps the
metadata, detects worker failures and corruption, and restores replication.

* `objects` — the client API required by the specification
* `cluster` / `admin` — observability and maintenance helpers
* `internal` — controller endpoints used by workers
"""


async def monitor_loop(services: Services) -> None:
    """Failure detector: marks workers DOWN when heartbeats stop."""
    while True:
        try:
            services.cluster.check_health()
        except Exception:
            log.exception("health check failed")
        await asyncio.sleep(services.settings.monitor_interval)


async def maintenance_loop(services: Services) -> None:
    """Reconcile / repair / trim / GC, periodically or when woken up."""
    while True:
        try:
            summary = await services.replication.run_maintenance_once()
            if any(v for k, v in summary.items()):
                log.info("maintenance: %s", {k: v for k, v in summary.items() if v})
        except Exception:
            log.exception("maintenance round failed")
        await services.replication.wait_for_wake(services.settings.maintenance_interval)


def _build_services(settings: Settings, worker_transport: httpx.AsyncBaseTransport | None) -> Services:
    os.makedirs(settings.data_dir, exist_ok=True)
    store = MetadataStore(settings.db_path)
    client = WorkerClient(settings.worker_request_timeout, settings.internal_token, transport=worker_transport)
    replication = ReplicationManager(store, client, settings)
    return Services(
        settings=settings,
        store=store,
        client=client,
        replication=replication,
        cluster=ClusterService(store, settings, replication),
        objects=ObjectService(store, client, replication, settings),
    )


def _install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ObjectStoreError)
    async def _domain_error(request: Request, exc: ObjectStoreError):
        return JSONResponse(error_body(exc.status_code, exc.code, exc.message, exc.details),
                            status_code=exc.status_code)

    @app.exception_handler(RequestValidationError)
    async def _validation_error(request: Request, exc: RequestValidationError):
        problems = [
            {"location": ".".join(str(p) for p in err.get("loc", ())), "message": err.get("msg")}
            for err in exc.errors()
        ]
        return JSONResponse(error_body(400, "invalid_request", "request validation failed", {"problems": problems}),
                            status_code=400)

    @app.exception_handler(StarletteHTTPException)
    async def _http_error(request: Request, exc: StarletteHTTPException):
        code = {404: "not_found", 405: "method_not_allowed"}.get(exc.status_code, "http_error")
        return JSONResponse(error_body(exc.status_code, code, str(exc.detail)), status_code=exc.status_code)

    @app.exception_handler(Exception)
    async def _unexpected(request: Request, exc: Exception):
        log.exception("unhandled error on %s %s", request.method, request.url.path)
        return JSONResponse(error_body(500, "internal_error", "internal server error"), status_code=500)


def create_app(settings: Settings | None = None, *, worker_transport: httpx.AsyncBaseTransport | None = None,
               run_background: bool = True) -> FastAPI:
    settings = settings or Settings.from_env()
    logging.basicConfig(level=settings.log_level.upper(),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    services = _build_services(settings, worker_transport)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Liveness of workers is unknown after a restart, and uploads/deletes
        # that were in flight when the controller stopped can never finish.
        services.store.prepare_for_startup(time.time())
        await services.replication.cleanup_incomplete_objects(cutoff=time.time())
        services.replication.event("info", "controller_started", f"Controller {__version__} started",
                                   replication_factor=settings.replication_factor, chunk_size=settings.chunk_size)
        tasks = []
        if run_background:
            tasks = [asyncio.create_task(monitor_loop(services)), asyncio.create_task(maintenance_loop(services))]
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await services.client.close()
            services.store.close()

    app = FastAPI(title="Mini Distributed Cloud Object Store — Controller", version=__version__,
                  description=DESCRIPTION, lifespan=lifespan)
    app.state.services = services
    _install_error_handlers(app)
    app.include_router(objects_api.router)
    app.include_router(cluster_api.router)
    app.include_router(internal_api.router)

    @app.get("/", include_in_schema=False)
    async def dashboard():
        return FileResponse(STATIC_DIR / "index.html")

    return app
