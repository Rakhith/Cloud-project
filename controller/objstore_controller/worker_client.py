"""HTTP client used by the controller to talk to storage workers.

Wraps the internal worker API (section 6 of the specification) and turns
transport/HTTP failures into a small exception hierarchy so the services can
reason about *why* a copy could not be used:

* ``WorkerUnavailable``       worker unreachable, timed out or returned 5xx
* ``ChunkNotFoundOnWorker``   worker is up but does not have the chunk
* ``ChunkCorruptedOnWorker``  worker detected a checksum mismatch on read
"""

from __future__ import annotations

import httpx


class WorkerError(Exception):
    pass


class WorkerUnavailable(WorkerError):
    pass


class ChunkNotFoundOnWorker(WorkerError):
    pass


class ChunkCorruptedOnWorker(WorkerError):
    pass


class WorkerClient:
    def __init__(self, timeout: float, internal_token: str | None = None,
                 transport: httpx.AsyncBaseTransport | None = None):
        self._token = internal_token
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout, connect=min(timeout, 3.0)),
            transport=transport,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def _request(self, method: str, url: str, headers: dict | None = None, **kwargs) -> httpx.Response:
        all_headers = dict(headers or {})
        if self._token:
            all_headers["X-Internal-Token"] = self._token
        try:
            resp = await self._client.request(method, url, headers=all_headers, **kwargs)
        except httpx.HTTPError as exc:
            raise WorkerUnavailable(f"{method} {url} failed: {exc.__class__.__name__}: {exc}") from exc
        if resp.status_code >= 500:
            raise WorkerUnavailable(f"{method} {url} returned {resp.status_code}: {resp.text[:200]}")
        return resp

    async def put_chunk(self, base_url: str, chunk_id: str, data: bytes, checksum: str,
                        object_id: str, index: int) -> None:
        resp = await self._request(
            "PUT",
            f"{base_url}/internal/chunks/{chunk_id}",
            content=data,
            headers={
                "Content-Type": "application/octet-stream",
                "X-Chunk-Checksum": checksum,
                "X-Object-Id": object_id,
                "X-Chunk-Index": str(index),
            },
        )
        if resp.status_code not in (200, 201):
            raise WorkerError(f"PUT chunk {chunk_id} on {base_url} returned {resp.status_code}: {resp.text[:200]}")

    async def get_chunk(self, base_url: str, chunk_id: str) -> bytes:
        resp = await self._request("GET", f"{base_url}/internal/chunks/{chunk_id}")
        if resp.status_code == 404:
            raise ChunkNotFoundOnWorker(f"chunk {chunk_id} not found on {base_url}")
        if resp.status_code == 409:
            raise ChunkCorruptedOnWorker(f"chunk {chunk_id} is corrupted on {base_url}")
        if resp.status_code != 200:
            raise WorkerError(f"GET chunk {chunk_id} on {base_url} returned {resp.status_code}")
        return resp.content

    async def delete_chunk(self, base_url: str, chunk_id: str) -> None:
        """Delete a chunk copy. A 404 counts as success (already gone)."""
        resp = await self._request("DELETE", f"{base_url}/internal/chunks/{chunk_id}")
        if resp.status_code not in (200, 204, 404):
            raise WorkerError(f"DELETE chunk {chunk_id} on {base_url} returned {resp.status_code}")

    async def list_chunks(self, base_url: str, worker_id: str, verify: bool = False) -> list[dict]:
        resp = await self._request(
            "GET",
            f"{base_url}/internal/workers/{worker_id}/chunks",
            params={"verify": "true" if verify else "false"},
        )
        if resp.status_code != 200:
            raise WorkerError(f"listing chunks on {base_url} returned {resp.status_code}")
        return resp.json()["chunks"]
