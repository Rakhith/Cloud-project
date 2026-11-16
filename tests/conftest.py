"""Test fixtures: a complete in-process cluster (1 controller + 4 workers).

All HTTP traffic between the nodes goes through ``Router``, an httpx
transport that dispatches requests to the right ASGI app by host name. A
"stopped container" is simulated by making a host unreachable, so the real
controller/worker code paths (including connection errors) are exercised
without Docker. Background loops are disabled; tests drive failure detection
and maintenance explicitly, which keeps them fast and deterministic.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
for sub in ("controller", "worker", "client"):
    sys.path.insert(0, str(ROOT / sub))

from objstore_controller.config import Settings  # noqa: E402
from objstore_controller.main import create_app as create_controller  # noqa: E402
from objstore_worker.config import WorkerSettings  # noqa: E402
from objstore_worker.main import create_app as create_worker  # noqa: E402

WORKER_IDS = ["worker1", "worker2", "worker3", "worker4"]
MB = 1024 * 1024


class Router(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.routes: dict[str, httpx.ASGITransport] = {}
        self.down: set[str] = set()

    def add(self, host: str, app) -> None:
        self.routes[host] = httpx.ASGITransport(app=app)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        host = request.url.host
        if host in self.down or host not in self.routes:
            raise httpx.ConnectError(f"{host} is unreachable", request=request)
        return await self.routes[host].handle_async_request(request)


class Cluster:
    def __init__(self, tmp_path: Path, **overrides) -> None:
        self.router = Router()
        token = overrides.get("internal_token")
        self.settings = Settings(
            data_dir=str(tmp_path / "controller"),
            repair_grace_period=overrides.pop("repair_grace_period", 0.0),
            **overrides,
        )
        self.app = create_controller(self.settings, worker_transport=self.router, run_background=False)
        self.services = self.app.state.services
        self.router.add("controller", self.app)
        self.workers = {}
        for wid in WORKER_IDS:
            ws = WorkerSettings(worker_id=wid, advertise_url=f"http://{wid}:8000",
                                controller_url="http://controller:8000",
                                data_dir=str(tmp_path / wid), internal_token=token)
            app = create_worker(ws, controller_transport=self.router, run_background=False)
            self.router.add(wid, app)
            self.workers[wid] = app
        self.client = httpx.AsyncClient(transport=self.router, base_url="http://controller:8000", timeout=30)

    # ------------------------------------------------------------------ helpers

    @property
    def store(self):
        return self.services.store

    def disk(self, worker_id: str):
        """The ChunkStore (on-disk storage) of a worker."""
        return self.workers[worker_id].state.store

    async def register(self, worker_id: str) -> None:
        await self.workers[worker_id].state.link.register()

    async def heartbeat(self, worker_id: str) -> None:
        await self.workers[worker_id].state.link.heartbeat()

    async def maintenance(self) -> dict:
        return await self.services.replication.run_maintenance_once()

    async def start(self) -> None:
        for wid in WORKER_IDS:
            await self.register(wid)
        await self.maintenance()
        assert all(w["status"] == "UP" for w in self.store.list_workers())

    async def stop_worker(self, worker_id: str) -> None:
        """Equivalent of `docker stop`: unreachable + heartbeats stop."""
        self.router.down.add(worker_id)
        self.store._exec("UPDATE workers SET last_heartbeat = ? WHERE worker_id = ?",
                         (time.time() - self.settings.heartbeat_timeout - 1, worker_id))
        self.services.cluster.check_health()
        assert self.store.get_worker(worker_id)["status"] == "DOWN"

    async def start_worker(self, worker_id: str) -> None:
        """Equivalent of `docker start`: the worker re-registers on boot."""
        self.router.down.discard(worker_id)
        await self.register(worker_id)

    async def upload(self, data: bytes, name: str = "file.bin", metadata: dict | None = None,
                     content_type: str = "application/octet-stream") -> httpx.Response:
        import json

        form = {"metadata": json.dumps(metadata)} if metadata is not None else {}
        return await self.client.post("/api/v1/objects", files={"file": (name, data, content_type)}, data=form)

    async def metadata(self, object_id: str) -> dict:
        resp = await self.client.get(f"/api/v1/objects/{object_id}/metadata")
        assert resp.status_code == 200, resp.text
        return resp.json()

    def files_on(self, worker_id: str) -> set[str]:
        return {c["chunk_id"] for c in self.disk(worker_id).list()}

    def chunk_path(self, worker_id: str, chunk_id: str) -> Path:
        return self.disk(worker_id).chunk_dir / f"{chunk_id}.chunk"

    async def close(self) -> None:
        await self.client.aclose()
        await self.services.client.close()
        for app in self.workers.values():
            await app.state.link.close()
        self.store.close()


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def cluster(tmp_path):
    c = Cluster(tmp_path)
    await c.start()
    try:
        yield c
    finally:
        await c.close()


@pytest.fixture
def make_cluster(tmp_path):
    """Factory for clusters with custom controller settings."""
    created: list[Cluster] = []

    async def factory(**overrides) -> Cluster:
        c = Cluster(tmp_path, **overrides)
        await c.start()
        created.append(c)
        return c

    yield factory
    # closing is best-effort; tmp_path is discarded anyway
    for c in created:
        c.store.close()
