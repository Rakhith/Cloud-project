"""Registration, heartbeats and background scrubbing.

``ControllerLink.run`` keeps the worker known to the controller:

* register on start-up (retrying with back-off until the controller is up);
* send a heartbeat every ``heartbeat_interval`` seconds with storage stats
  and the ids of chunks found corrupted;
* re-register if the controller answers a heartbeat with 404 (it lost its
  state or the worker was decommissioned).

``scrub_loop`` periodically re-hashes every stored chunk so corruption is
detected even for data nobody reads.
"""

from __future__ import annotations

import asyncio
import logging

import httpx
from starlette.concurrency import run_in_threadpool

from .config import WorkerSettings
from .storage import ChunkStore

log = logging.getLogger(__name__)


class ControllerLink:
    def __init__(self, settings: WorkerSettings, store: ChunkStore,
                 transport: httpx.AsyncBaseTransport | None = None):
        self.settings = settings
        self.store = store
        headers = {"X-Internal-Token": settings.internal_token} if settings.internal_token else {}
        self._client = httpx.AsyncClient(base_url=settings.controller_url, timeout=5.0, headers=headers,
                                         transport=transport)
        self.registered = False

    async def close(self) -> None:
        await self._client.aclose()

    async def _payload(self) -> dict:
        stats = await run_in_threadpool(self.store.stats)
        return {"worker_id": self.settings.worker_id, **stats}

    async def register(self) -> None:
        payload = await self._payload()
        payload["url"] = self.settings.advertise_url
        resp = await self._client.post("/internal/workers/register", json=payload)
        resp.raise_for_status()
        self.registered = True
        log.info("registered with controller %s as %s (%s)", self.settings.controller_url,
                 self.settings.worker_id, resp.json().get("status"))

    async def heartbeat(self) -> None:
        payload = await self._payload()
        payload["corrupt_chunks"] = self.store.corrupt_chunks()
        resp = await self._client.post("/internal/workers/heartbeat", json=payload)
        if resp.status_code == 404:
            log.warning("controller does not know this worker; re-registering")
            self.registered = False
            return
        resp.raise_for_status()

    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                if not self.registered:
                    await self.register()
                else:
                    await self.heartbeat()
                # After a 404 the next iteration re-registers immediately.
                delay = self.settings.heartbeat_interval if self.registered else 0.0
                backoff = 1.0
            except (httpx.HTTPError, ValueError) as exc:
                log.warning("controller unreachable (%s); retrying in %.0fs", exc.__class__.__name__, backoff)
                delay = backoff
                backoff = min(backoff * 2, 10.0)
            await asyncio.sleep(delay)


async def scrub_loop(store: ChunkStore, interval: float) -> None:
    if interval <= 0:
        return
    while True:
        await asyncio.sleep(interval)
        try:
            inventory = await run_in_threadpool(store.list, True)
            bad = [c["chunk_id"] for c in inventory if not c.get("valid", True)]
            if bad:
                log.error("scrubber found %d corrupted chunk(s): %s", len(bad), ", ".join(bad[:10]))
        except Exception:
            log.exception("scrub failed")
