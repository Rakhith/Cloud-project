"""Unit tests for the worker's on-disk chunk store and HTTP API."""

from __future__ import annotations

import hashlib

import httpx
import pytest

from objstore_worker.config import WorkerSettings
from objstore_worker.main import create_app
from objstore_worker.storage import ChecksumMismatch, ChunkCorrupted, ChunkNotFound, ChunkStore, InvalidChunkId


def test_put_get_delete(tmp_path):
    store = ChunkStore(str(tmp_path))
    meta, created = store.put("c1", b"hello", hashlib.sha256(b"hello").hexdigest())
    assert created and meta["size"] == 5
    data, stored = store.get("c1")
    assert data == b"hello" and stored["checksum"] == meta["checksum"]
    _, created_again = store.put("c1", b"hello")
    assert not created_again
    assert store.delete("c1") and not store.delete("c1")
    with pytest.raises(ChunkNotFound):
        store.get("c1")


def test_checksum_mismatch_on_put_is_rejected(tmp_path):
    store = ChunkStore(str(tmp_path))
    with pytest.raises(ChecksumMismatch):
        store.put("c1", b"hello", "0" * 64)
    assert store.list() == []


def test_corruption_is_detected_and_tracked(tmp_path):
    store = ChunkStore(str(tmp_path))
    store.put("c1", b"important data")
    (store.chunk_dir / "c1.chunk").write_bytes(b"important dat4")
    with pytest.raises(ChunkCorrupted):
        store.get("c1")
    assert store.corrupt_chunks() == ["c1"]
    assert store.list(verify=True)[0]["valid"] is False
    store.put("c1", b"important data")  # repair overwrites the copy
    assert store.corrupt_chunks() == [] and store.verify("c1")


def test_missing_metadata_counts_as_corrupt(tmp_path):
    store = ChunkStore(str(tmp_path))
    store.put("c1", b"x")
    (store.chunk_dir / "c1.meta.json").unlink()
    assert not store.verify("c1")


@pytest.mark.parametrize("bad", ["../etc/passwd", "a/b", "", ".hidden", "x" * 200, "a..b"])
def test_path_traversal_is_rejected(tmp_path, bad):
    store = ChunkStore(str(tmp_path))
    with pytest.raises(InvalidChunkId):
        store.put(bad, b"x")


def test_inventory_and_stats(tmp_path):
    store = ChunkStore(str(tmp_path))
    store.put("a", b"1" * 10, extra={"object_id": "o", "index": 0})
    store.put("b", b"2" * 20)
    inventory = store.list()
    assert [c["chunk_id"] for c in inventory] == ["a", "b"]
    assert inventory[0]["object_id"] == "o"
    stats = store.stats()
    assert stats["chunk_count"] == 2 and stats["used_bytes"] == 30


@pytest.mark.anyio
async def test_worker_http_api(tmp_path):
    settings = WorkerSettings(worker_id="w1", advertise_url="http://w1:8000", data_dir=str(tmp_path))
    app = create_app(settings, run_background=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://w1") as c:
        body = b"chunk-bytes"
        digest = hashlib.sha256(body).hexdigest()
        put = await c.put("/internal/chunks/obj_000000", content=body, headers={"X-Chunk-Checksum": digest})
        assert put.status_code == 201
        bad = await c.put("/internal/chunks/obj_000001", content=body, headers={"X-Chunk-Checksum": "0" * 64})
        assert bad.status_code == 400
        got = await c.get("/internal/chunks/obj_000000")
        assert got.content == body and got.headers["x-chunk-checksum"] == digest
        inv = (await c.get("/internal/workers/w1/chunks?verify=true")).json()
        assert inv["count"] == 1 and inv["chunks"][0]["valid"] is True
        assert (await c.get("/internal/workers/other/chunks")).status_code == 404
        (tmp_path / "chunks" / "obj_000000.chunk").write_bytes(b"chunk-bytez")
        assert (await c.get("/internal/chunks/obj_000000")).status_code == 409
        assert (await c.post("/internal/chunks/obj_000000/verify")).json()["valid"] is False
        assert (await c.delete("/internal/chunks/obj_000000")).status_code == 204
        assert (await c.get("/internal/chunks/obj_000000")).status_code == 404
        assert (await c.get("/internal/chunks/..%2Fsecret")).status_code in (400, 404)
        health = (await c.get("/health")).json()
        assert health["worker_id"] == "w1" and health["chunk_count"] == 0


@pytest.fixture
def anyio_backend():
    return "asyncio"
