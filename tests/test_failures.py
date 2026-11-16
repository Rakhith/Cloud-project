"""Worker failure handling: detection, degraded reads, re-replication,
rejoin reconciliation and trimming back to the replication factor."""

from __future__ import annotations

import os

import pytest

from conftest import MB, WORKER_IDS

pytestmark = pytest.mark.anyio


def copies(meta: dict) -> dict[int, list[str]]:
    return {c["index"]: sorted(r["worker_id"] for r in c["replicas"]) for c in meta["chunks"]}


async def test_worker_failure_full_cycle(cluster):
    """The `docker stop worker2` / `docker start worker2` demonstration."""
    data = os.urandom(5 * MB + 123)
    oid = (await cluster.upload(data)).json()["object_id"]
    before = await cluster.metadata(oid)
    on_w2 = [i for i, ws in copies(before).items() if "worker2" in ws]
    assert on_w2, "worker2 should hold some chunks"

    # --- during the failure -------------------------------------------------
    await cluster.stop_worker("worker2")
    during = await cluster.metadata(oid)
    assert during["replication_status"] == "degraded"
    for chunk in during["chunks"]:
        expected = 1 if chunk["index"] in on_w2 else 2
        assert chunk["healthy_replicas"] == expected
    status = (await cluster.client.get("/api/v1/cluster")).json()
    assert status["workers_down"] == 1 and status["under_replicated"] == len(on_w2)
    assert not status["replication_healthy"]

    # reads still work from the surviving copies
    got = await cluster.client.get(f"/api/v1/objects/{oid}")
    assert got.status_code == 200 and got.content == data

    # --- repair: re-replicate onto the remaining workers ----------------------
    summary = await cluster.maintenance()
    assert summary["re_replicated"] == len(on_w2)
    repaired = await cluster.metadata(oid)
    assert repaired["replication_status"] == "healthy"
    for chunk in repaired["chunks"]:
        live = [r["worker_id"] for r in chunk["replicas"] if r["available"]]
        assert len(live) == 2 and "worker2" not in live
    assert (await cluster.client.get("/api/v1/cluster")).json()["under_replicated"] == 0

    # --- recovery: worker2 restarts with its (persistent) data ---------------
    await cluster.start_worker("worker2")
    assert cluster.store.get_worker("worker2")["status"] == "SYNCING"
    summary = await cluster.maintenance()
    assert cluster.store.get_worker("worker2")["status"] == "UP"
    assert summary["trimmed"] == len(on_w2)  # surplus third copies removed
    after = await cluster.metadata(oid)
    assert after["replication_status"] == "healthy"
    for chunk in after["chunks"]:
        assert len(chunk["replicas"]) == 2 and chunk["healthy_replicas"] == 2
    # physical state matches the metadata exactly
    total = sum(len(cluster.files_on(w)) for w in WORKER_IDS)
    assert total == 2 * after["num_chunks"]
    status = (await cluster.client.get("/api/v1/cluster")).json()
    assert status["replication_healthy"] and status["over_replicated"] == 0
    assert (await cluster.client.get(f"/api/v1/objects/{oid}")).content == data


async def test_short_outage_within_grace_period_does_not_copy(make_cluster):
    c = await make_cluster(repair_grace_period=3600)
    oid = (await c.upload(os.urandom(3 * MB))).json()["object_id"]
    await c.stop_worker("worker3")
    summary = await c.maintenance()
    assert summary["re_replicated"] == 0
    assert (await c.metadata(oid))["replication_status"] == "degraded"
    await c.start_worker("worker3")
    await c.maintenance()
    meta = await c.metadata(oid)
    assert meta["replication_status"] == "healthy"
    assert all(len(ch["replicas"]) == 2 for ch in meta["chunks"])


async def test_uploads_continue_while_a_worker_is_down(cluster):
    await cluster.stop_worker("worker1")
    data = os.urandom(4 * MB)
    meta = (await cluster.upload(data)).json()
    for chunk in meta["chunks"]:
        workers = [r["worker_id"] for r in chunk["replicas"]]
        assert len(set(workers)) == 2 and "worker1" not in workers
    assert (await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")).content == data


async def test_write_fails_over_when_worker_dies_unnoticed(cluster):
    # worker4 is unreachable but the controller still believes it is UP
    cluster.router.down.add("worker4")
    data = os.urandom(4 * MB)
    resp = await cluster.upload(data)
    assert resp.status_code == 201, resp.text
    for chunk in resp.json()["chunks"]:
        workers = [r["worker_id"] for r in chunk["replicas"]]
        assert len(set(workers)) == 2 and "worker4" not in workers


async def test_object_unavailable_when_all_copies_are_down(cluster):
    meta = (await cluster.upload(os.urandom(1000))).json()
    for wid in [r["worker_id"] for r in meta["chunks"][0]["replicas"]]:
        await cluster.stop_worker(wid)
    resp = await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")
    assert resp.status_code == 503
    assert (await cluster.metadata(meta["object_id"]))["replication_status"] == "unavailable"


async def test_worker_returning_with_empty_disk_is_refilled(cluster):
    data = os.urandom(4 * MB)
    oid = (await cluster.upload(data)).json()["object_id"]
    lost = cluster.files_on("worker2")
    assert lost
    await cluster.stop_worker("worker2")
    for chunk_id in lost:  # the volume was wiped while the container was down
        cluster.disk("worker2").delete(chunk_id)
    await cluster.start_worker("worker2")
    summary = await cluster.maintenance()
    reconciled = summary["reconciled_workers"][0]
    assert reconciled["worker_id"] == "worker2" and reconciled["missing"] == len(lost)
    assert summary["re_replicated"] == len(lost)
    meta = await cluster.metadata(oid)
    assert meta["replication_status"] == "healthy"
    assert all(len(ch["replicas"]) == 2 for ch in meta["chunks"])


async def test_delete_while_worker_down_is_completed_later(cluster):
    meta = (await cluster.upload(os.urandom(3 * MB))).json()
    held = cluster.files_on("worker2")
    assert held
    await cluster.stop_worker("worker2")
    assert (await cluster.client.delete(f"/api/v1/objects/{meta['object_id']}")).status_code == 204
    assert (await cluster.client.get("/api/v1/cluster")).json()["pending_deletes"] == len(held)
    assert cluster.files_on("worker2") == held  # still there while it is down
    await cluster.start_worker("worker2")
    await cluster.maintenance()
    assert cluster.files_on("worker2") == set()
    assert (await cluster.client.get("/api/v1/cluster")).json()["pending_deletes"] == 0


async def test_orphan_chunks_are_removed_on_reconcile(cluster):
    cluster.disk("worker1").put("orphan-chunk", b"left over from a crash")
    await cluster.start_worker("worker1")  # re-register -> SYNCING -> reconcile
    summary = await cluster.maintenance()
    assert summary["reconciled_workers"][0]["orphans_deleted"] == 1
    assert "orphan-chunk" not in cluster.files_on("worker1")


async def test_decommissioned_worker_data_is_re_replicated(cluster):
    meta = (await cluster.upload(os.urandom(4 * MB))).json()
    await cluster.stop_worker("worker4")
    resp = await cluster.client.delete("/api/v1/admin/workers/worker4")
    assert resp.status_code == 200
    await cluster.maintenance()
    after = await cluster.metadata(meta["object_id"])
    assert after["replication_status"] == "healthy"
    assert all("worker4" not in [r["worker_id"] for r in ch["replicas"]] for ch in after["chunks"])


async def test_heartbeat_from_down_worker_triggers_resync(cluster):
    await cluster.stop_worker("worker3")
    cluster.router.down.discard("worker3")
    await cluster.heartbeat("worker3")  # e.g. a network partition healed
    assert cluster.store.get_worker("worker3")["status"] == "SYNCING"
    await cluster.maintenance()
    assert cluster.store.get_worker("worker3")["status"] == "UP"
