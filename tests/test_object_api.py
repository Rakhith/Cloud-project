"""Client API behaviour: storage, chunking, replication, metadata, errors."""

from __future__ import annotations

import hashlib
import os

import pytest

from conftest import MB, WORKER_IDS

pytestmark = pytest.mark.anyio


async def test_upload_and_download_roundtrip(cluster):
    data = os.urandom(3 * MB + 512 * 1024)
    resp = await cluster.upload(data, name="video.mp4", content_type="video/mp4")
    assert resp.status_code == 201, resp.text
    meta = resp.json()
    assert resp.headers["location"] == f"/api/v1/objects/{meta['object_id']}"
    assert meta["size"] == len(data)
    assert meta["checksum"] == hashlib.sha256(data).hexdigest()
    assert meta["content_type"] == "video/mp4"

    got = await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")
    assert got.status_code == 200
    assert got.content == data
    assert got.headers["content-type"] == "video/mp4"
    assert got.headers["x-checksum-sha256"] == meta["checksum"]
    assert 'filename="video.mp4"' in got.headers["content-disposition"]


async def test_chunking_order_and_sizes(cluster):
    data = os.urandom(3 * MB + 1000)
    meta = (await cluster.upload(data)).json()
    assert meta["num_chunks"] == 4
    assert [c["index"] for c in meta["chunks"]] == [0, 1, 2, 3]
    assert [c["size"] for c in meta["chunks"]] == [MB, MB, MB, 1000]
    assert len({c["chunk_id"] for c in meta["chunks"]}) == 4
    for i, chunk in enumerate(meta["chunks"]):
        assert chunk["checksum"] == hashlib.sha256(data[i * MB:(i + 1) * MB]).hexdigest()


async def test_every_chunk_has_two_copies_on_different_workers(cluster):
    data = os.urandom(6 * MB)
    meta = (await cluster.upload(data)).json()
    assert meta["replication_status"] == "healthy"
    used = set()
    for chunk in meta["chunks"]:
        workers = [r["worker_id"] for r in chunk["replicas"]]
        assert len(workers) == 2 and len(set(workers)) == 2
        assert chunk["healthy_replicas"] == 2
        for wid in workers:
            # the copy really is on that worker's disk, and only there
            assert chunk["chunk_id"] in cluster.files_on(wid)
        used.update(workers)
    assert used == set(WORKER_IDS), "chunks should be spread over all workers"
    total_files = sum(len(cluster.files_on(w)) for w in WORKER_IDS)
    assert total_files == 2 * meta["num_chunks"]


async def test_load_balancing_across_uploads(cluster):
    for _ in range(4):
        await cluster.upload(os.urandom(1000))  # single-chunk objects
    counts = [len(cluster.files_on(w)) for w in WORKER_IDS]
    assert counts == [2, 2, 2, 2]


async def test_empty_object(cluster):
    meta = (await cluster.upload(b"", name="empty.txt")).json()
    assert meta["size"] == 0 and meta["num_chunks"] == 0
    got = await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")
    assert got.status_code == 200 and got.content == b""


async def test_content_type_is_guessed_from_name(cluster):
    meta = (await cluster.upload(b"\x89PNG....", name="photo.png")).json()
    assert meta["content_type"] == "image/png"


async def test_custom_metadata_lifecycle(cluster):
    data = os.urandom(2 * MB + 10)
    meta = (await cluster.upload(data, metadata={"owner": "alice", "project": "cloud"})).json()
    oid = meta["object_id"]
    assert meta["custom_metadata"] == {"owner": "alice", "project": "cloud"}
    assert meta["metadata_version"] == 1
    stored_at = {c["chunk_id"]: c["stored_at"] for c in cluster.disk("worker1").list()}

    resp = await cluster.client.patch(f"/api/v1/objects/{oid}/metadata",
                                      json={"owner": "bob", "reviewed": True, "project": None})
    assert resp.status_code == 200, resp.text
    updated = resp.json()
    assert updated["custom_metadata"] == {"owner": "bob", "reviewed": True}
    assert updated["metadata_version"] == 2
    assert updated["updated_at"] >= meta["updated_at"]
    # system metadata and data are untouched (no re-upload happened)
    assert updated["checksum"] == meta["checksum"] and updated["chunks"] == meta["chunks"]
    assert {c["chunk_id"]: c["stored_at"] for c in cluster.disk("worker1").list()} == stored_at

    fetched = await cluster.metadata(oid)
    assert fetched["custom_metadata"] == {"owner": "bob", "reviewed": True}
    assert (await cluster.client.get(f"/api/v1/objects/{oid}")).content == data


async def test_metadata_optimistic_concurrency(cluster):
    oid = (await cluster.upload(b"abc", metadata={"a": 1})).json()["object_id"]
    ok = await cluster.client.patch(f"/api/v1/objects/{oid}/metadata", json={"a": 2}, headers={"If-Match": "1"})
    assert ok.status_code == 200
    stale = await cluster.client.patch(f"/api/v1/objects/{oid}/metadata", json={"a": 3}, headers={"If-Match": "1"})
    assert stale.status_code == 409
    assert stale.json()["error"]["details"]["current_version"] == 2


async def test_metadata_replace_mode(cluster):
    oid = (await cluster.upload(b"abc", metadata={"a": 1, "b": 2})).json()["object_id"]
    resp = await cluster.client.patch(f"/api/v1/objects/{oid}/metadata?replace=true", json={"c": "x"})
    assert resp.json()["custom_metadata"] == {"c": "x"}


async def test_delete_object_removes_all_copies(cluster):
    meta = (await cluster.upload(os.urandom(2 * MB))).json()
    oid = meta["object_id"]
    assert (await cluster.client.delete(f"/api/v1/objects/{oid}")).status_code == 204
    assert all(not cluster.files_on(w) for w in WORKER_IDS)
    assert (await cluster.client.get(f"/api/v1/objects/{oid}")).status_code == 404
    assert (await cluster.client.get(f"/api/v1/objects/{oid}/metadata")).status_code == 404
    assert (await cluster.client.delete(f"/api/v1/objects/{oid}")).status_code == 404


async def test_list_objects_with_pagination_and_prefix(cluster):
    for name in ("a/1.txt", "a/2.txt", "b/3.txt"):
        await cluster.upload(b"x", name=name)
    page = (await cluster.client.get("/api/v1/objects?limit=2")).json()
    assert page["total"] == 3 and len(page["objects"]) == 2 and page["next_offset"] == 2
    entry = page["objects"][0]
    assert {"object_id", "name", "size", "content_type"} <= entry.keys()
    last = (await cluster.client.get("/api/v1/objects?limit=2&offset=2")).json()
    assert len(last["objects"]) == 1 and last["next_offset"] is None
    pref = (await cluster.client.get("/api/v1/objects?prefix=a/")).json()
    assert sorted(o["name"] for o in pref["objects"]) == ["a/1.txt", "a/2.txt"]


@pytest.mark.parametrize("method,path,body", [
    ("GET", "/api/v1/objects/nope", None),
    ("GET", "/api/v1/objects/nope/metadata", None),
    ("PATCH", "/api/v1/objects/nope/metadata", {"a": 1}),
    ("DELETE", "/api/v1/objects/nope", None),
])
async def test_missing_object_returns_404_json(cluster, method, path, body):
    resp = await cluster.client.request(method, path, json=body)
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


async def test_invalid_requests_return_400(cluster):
    no_file = await cluster.client.post("/api/v1/objects", data={"metadata": "{}"})
    assert no_file.status_code == 400
    bad_json = await cluster.client.post("/api/v1/objects", files={"file": ("a", b"x")}, data={"metadata": "{oops"})
    assert bad_json.status_code == 400 and "JSON" in bad_json.json()["error"]["message"]
    nested = await cluster.upload(b"x", metadata={"a": {"nested": 1}})
    assert nested.status_code == 400
    oid = (await cluster.upload(b"x")).json()["object_id"]
    not_obj = await cluster.client.patch(f"/api/v1/objects/{oid}/metadata", json=[1, 2])
    assert not_obj.status_code == 400
    bad_limit = await cluster.client.get("/api/v1/objects?limit=0")
    assert bad_limit.status_code == 400
    # failed uploads leave nothing behind
    assert (await cluster.client.get("/api/v1/objects")).json()["total"] == 1


async def test_upload_needs_replication_factor_workers(cluster):
    for wid in ("worker2", "worker3", "worker4"):
        await cluster.stop_worker(wid)
    resp = await cluster.upload(b"hello")
    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "service_unavailable"
    assert (await cluster.client.get("/api/v1/objects")).json()["total"] == 0


async def test_cluster_status_endpoint(cluster):
    await cluster.upload(os.urandom(2 * MB))
    status = (await cluster.client.get("/api/v1/cluster")).json()
    assert status["workers_up"] == 4 and status["writable"]
    assert status["chunks"] == 2 and status["replicas"] == 4
    assert status["replication_healthy"] is True
    events = (await cluster.client.get("/api/v1/events")).json()
    assert any(e["type"] == "object_uploaded" for e in events)


async def test_internal_endpoints_require_token_when_configured(make_cluster):
    c = await make_cluster(internal_token="s3cret")  # workers registered using the token
    assert (await c.client.get("/internal/workers/worker1/chunks")).status_code == 401
    ok = await c.client.get("/internal/workers/worker1/chunks", headers={"X-Internal-Token": "s3cret"})
    assert ok.status_code == 200 and ok.json()["count"] == 0
    resp = await c.upload(b"data")  # controller -> worker calls carry the token too
    assert resp.status_code == 201


async def test_unknown_worker_heartbeat_gets_404(cluster):
    resp = await cluster.client.post("/internal/workers/heartbeat", json={"worker_id": "ghost"})
    assert resp.status_code == 404
