"""Data integrity: corruption detection, safe reads, and restoration."""

from __future__ import annotations

import os

import pytest

from conftest import MB

pytestmark = pytest.mark.anyio


def flip_bytes(path, offset: int = 100, count: int = 16) -> None:
    with open(path, "r+b") as fh:
        fh.seek(offset)
        original = fh.read(count)
        fh.seek(offset)
        fh.write(bytes(b ^ 0xFF for b in original))


async def test_corrupted_copy_is_detected_skipped_and_restored(cluster):
    data = os.urandom(3 * MB)
    meta = (await cluster.upload(data)).json()
    chunk = meta["chunks"][0]
    victim, other = [r["worker_id"] for r in chunk["replicas"]]
    flip_bytes(cluster.chunk_path(victim, chunk["chunk_id"]))
    # Corrupt copy + the only other copy unreachable: refuse rather than
    # return bad bytes.
    cluster.router.down.add(other)
    assert (await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")).status_code == 503
    cluster.router.down.discard(other)

    # With the healthy copy reachable the client gets correct data.
    got = await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")
    assert got.status_code == 200 and got.content == data

    after = await cluster.metadata(meta["object_id"])
    states = {r["worker_id"]: r["status"] for r in after["chunks"][0]["replicas"]}
    assert states == {victim: "corrupt", other: "healthy"}
    assert after["replication_status"] == "degraded"
    events = (await cluster.client.get("/api/v1/events?level=error")).json()
    assert any(e["type"] == "corruption_detected" and e["details"]["worker_id"] == victim for e in events)

    summary = await cluster.maintenance()
    assert summary["restored_in_place"] == 1
    assert cluster.disk(victim).verify(chunk["chunk_id"])  # file rewritten with valid data
    healed = await cluster.metadata(meta["object_id"])
    assert healed["replication_status"] == "healthy"
    assert {r["worker_id"] for r in healed["chunks"][0]["replicas"]} == {victim, other}


async def test_scrub_finds_corruption_nobody_has_read(cluster):
    meta = (await cluster.upload(os.urandom(2 * MB))).json()
    chunk = meta["chunks"][0]
    victim = chunk["replicas"][0]["worker_id"]
    flip_bytes(cluster.chunk_path(victim, chunk["chunk_id"]), offset=0)
    resp = await cluster.client.post("/api/v1/admin/scrub")
    assert resp.status_code == 200
    report = resp.json()
    assert sum(w.get("corrupt", 0) for w in report["workers"]) == 1
    assert report["repair"]["restored_in_place"] == 1
    assert cluster.disk(victim).verify(chunk["chunk_id"])
    assert (await cluster.client.get("/api/v1/cluster")).json()["replication_healthy"]


async def test_worker_scrubber_reports_corruption_via_heartbeat(cluster):
    meta = (await cluster.upload(os.urandom(MB))).json()
    chunk = meta["chunks"][0]
    victim = chunk["replicas"][1]["worker_id"]
    flip_bytes(cluster.chunk_path(victim, chunk["chunk_id"]))
    cluster.disk(victim).list(verify=True)  # what the worker's scrub loop does
    assert cluster.disk(victim).corrupt_chunks() == [chunk["chunk_id"]]
    await cluster.heartbeat(victim)
    states = {r["worker_id"]: r["status"] for r in (await cluster.metadata(meta["object_id"]))["chunks"][0]["replicas"]}
    assert states[victim] == "corrupt"
    await cluster.maintenance()
    assert cluster.disk(victim).corrupt_chunks() == []
    assert (await cluster.metadata(meta["object_id"]))["replication_status"] == "healthy"


async def test_truncated_or_deleted_chunk_files(cluster):
    data = os.urandom(2 * MB + 5)
    meta = (await cluster.upload(data)).json()
    c0, c1 = meta["chunks"][0], meta["chunks"][1]
    with open(cluster.chunk_path(c0["replicas"][0]["worker_id"], c0["chunk_id"]), "r+b") as fh:
        fh.truncate(1000)
    cluster.chunk_path(c1["replicas"][0]["worker_id"], c1["chunk_id"]).unlink()
    for _ in range(3):  # whichever copy is read first, data is always correct
        assert (await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")).content == data
    await cluster.client.post("/api/v1/admin/scrub")
    healed = await cluster.metadata(meta["object_id"])
    assert healed["replication_status"] == "healthy"
    assert all(len(ch["replicas"]) == 2 for ch in healed["chunks"])


async def test_all_copies_corrupted_returns_error_not_bad_data(cluster):
    meta = (await cluster.upload(os.urandom(5000))).json()
    chunk = meta["chunks"][0]
    for r in chunk["replicas"]:
        flip_bytes(cluster.chunk_path(r["worker_id"], chunk["chunk_id"]))
    resp = await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")
    assert resp.status_code == 500
    assert resp.json()["error"]["code"] == "data_integrity_error"
    summary = await cluster.maintenance()
    assert summary["unrecoverable_chunks"] == 1
    assert (await cluster.metadata(meta["object_id"]))["replication_status"] == "unavailable"
