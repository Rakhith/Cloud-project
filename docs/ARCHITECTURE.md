# Architecture

This document explains how the object store is built and why. For usage instructions see the [README](../README.md).

## 1. Components

| Component | Responsibility | State |
|---|---|---|
| **Controller** (1) | Client REST API, object and chunk metadata, chunk placement, worker membership, failure detection, repair, integrity checking, garbage collection | SQLite database `metadata.db` on volume `controller-data` |
| **Worker** (4) | Stores and serves chunk files, verifies checksums, scrubs its own disk, registers with and heartbeats to the controller | Chunk files on its own volume `workerN-data` |
| **Client** (1+) | CLI (`client/objstore_cli.py`), the `client` container, the browser dashboard, or any HTTP client | none |

Clients only ever talk to the controller and never need to know where chunks are stored (specification §5.2). All controller↔worker traffic uses the internal API (specification §6).

## 2. Data model

```
objects (object_id PK, name, size, content_type, checksum, chunk_size, num_chunks,
         status pending|active|deleting, custom_metadata JSON, metadata_version,
         created_at, updated_at)
chunks  (chunk_id PK = "<object_id>_<index:06d>", object_id FK, idx, size, checksum)
replicas(chunk_id FK, worker_id, status healthy|corrupt, created_at, updated_at)
workers (worker_id PK, url, status UP|SYNCING|DOWN, last_heartbeat, status_changed_at,
         used/free/capacity bytes, chunk_count)
pending_deletes(chunk_id, worker_id, attempts)      -- deletes to retry later
events  (id, ts, level, type, message, details)     -- audit log shown in demos
```

* Each chunk's SHA-256 is stored on the controller (the authoritative value) and also in the chunk's sidecar file on the worker (`<chunk>.meta.json`), so a worker can check its own data without asking the controller.
* The whole-object SHA-256 is returned as `ETag` / `X-Checksum-SHA256`, so clients can verify a download end-to-end.
* An object is visible to clients only in the `active` state. A crash in the middle of an upload or delete therefore never exposes a half-written object. Leftover `pending`/`deleting` objects are cleaned up when the controller starts and periodically afterwards.

On a worker's disk:

```
/data/chunks/<chunk_id>.chunk        raw bytes
/data/chunks/<chunk_id>.meta.json    {"chunk_id","size","checksum","stored_at","object_id","index"}
/data/tmp/                           staging area; write → fsync → rename (atomic)
```

## 3. Write path (upload)

1. `POST /api/v1/objects` (multipart). The controller checks that at least `RF = 2` workers are `UP`; otherwise it returns **503**.
2. It creates the object row with status `pending` and orders the UP workers by how many copies they already hold (least-loaded first).
3. It reads the upload 1 MB at a time. For chunk *i*:
   * computes the chunk's SHA-256 and inserts the chunk row *before* writing to any worker, so a concurrent reconciliation never mistakes an in-flight chunk for an orphan;
   * picks candidate workers `ordered[(i + k) mod n]`. Consecutive chunks rotate across worker pairs (w1+w2, w2+w3, w3+w4, w4+w1, …), which spreads both data and load;
   * writes the 2 copies **in parallel**. Each worker re-hashes the body against `X-Chunk-Checksum`, which detects corruption in transit;
   * if a worker fails, it is excluded for the rest of the upload and the next candidate is tried. Each acknowledged copy is recorded in `replicas`;
   * if 2 copies cannot be written, the upload fails with **503** and everything written so far is deleted.
4. It records the object's size, SHA-256 and chunk count, sets the status to `active`, and returns **201** with the full metadata, including chunk locations.

## 4. Read path (download)

1. Returns **404** if the object does not exist, and **503** if some chunk has no copy on a worker that is not DOWN.
2. Chunks are streamed in index order. A prefetch window (`READ_AHEAD_CHUNKS`) fetches upcoming chunks concurrently.
3. For each chunk, copies are tried in this order: healthy before corrupt, then UP before SYNCING before DOWN, with random tie-breaking to spread load. Every copy received is **re-hashed and compared with the controller's checksum**:
   * a mismatch (or a 409 from the worker's own check) marks that copy `corrupt` and wakes the repair loop, and the next copy is tried;
   * a 404 means the copy is lost: its record is removed and repair is woken;
   * connection errors move on to the next copy.
   If every copy fails, the controller reloads the placement once (it may have changed during repair) and tries again.
4. The first chunk is fetched *before* the response headers are sent. If it fails, the client gets a proper JSON error: **500 `data_integrity_error`** when all copies are corrupt, **503** when none is reachable. A failure on a later chunk aborts the connection, so the client sees a truncated response rather than wrong data.

## 5. Failure detection and worker lifecycle

```
 register ──► SYNCING ──(inventory reconciled)──► UP
                 ▲                                 │ no heartbeat for HEARTBEAT_TIMEOUT (10 s)
                 └──── heartbeat resumes ──── DOWN ◄┘
```

* Workers send a heartbeat every 3 s with stats and the IDs of chunks they found corrupted.
* The controller's monitor loop (every 2 s) marks silent workers **DOWN** and logs how many copies became unavailable.
* A worker that registers (for example after `docker start worker2`), or that heartbeats again after being DOWN, enters **SYNCING**. Its copies are not counted as healthy until reconciliation finishes. Then it is **UP**.
* After the controller itself restarts, every known worker is set to SYNCING and must prove it is alive again.

## 6. Replication maintenance

A maintenance round runs every `MAINTENANCE_INTERVAL` seconds, and immediately whenever something is woken (a worker goes down or comes back, corruption is detected, and so on). All maintenance runs under a single lock. A round does the following:

1. **Reconcile** every SYNCING worker. The controller lists the worker's actual inventory (`GET /internal/workers/{id}/chunks?verify=true`, which re-hashes every chunk) and compares it with its records:
   * recorded and valid → confirmed healthy;
   * recorded but missing → record removed (`replica_missing`);
   * recorded but bad checksum → marked `corrupt`;
   * not recorded, belongs to a live object and valid → *adopted*;
   * not recorded and unknown, deleted or invalid → *orphan*, deleted from the worker.
2. **Repair** every chunk whose count of healthy copies on UP workers differs from RF, or that has a corrupt copy on an UP worker:
   * **Too few copies.** Corrupt copies on live workers are rewritten **in place** from a verified healthy copy. If copies are still missing, new ones are created on the least-loaded UP worker that does not already hold the chunk. Copies on workers that have been DOWN for less than `REPAIR_GRACE_PERIOD`, or that are SYNCING, count as "coming back", so a quick restart does not trigger a mass copy.
   * **Enough copies.** Any corrupt copy is dropped. If there are more than RF copies (typically after a failed worker returns with its old data), the copies on the most-loaded workers are **trimmed**.
   * **No healthy copy at all.** The chunk is reported once as `chunk_unavailable`. It becomes recoverable again if a worker holding a copy comes back.
3. **Retry pending deletes** for workers that are reachable again.
4. **Clean up** uploads or deletes that have been stuck for longer than `INCOMPLETE_UPLOAD_TTL`.

### Worker failure walkthrough (`docker stop worker2` → `docker start worker2`)

| Phase | What happens | Visible in |
|---|---|---|
| Before | Every chunk has 2/2 copies | `locate`, `cluster` → `replication HEALTHY` |
| ≤10 s after stop | worker2 is marked DOWN. Chunks with a copy on worker2 show 1/2 | `worker_down` event, `under-replicated=N` |
| During | Downloads succeed from the surviving copies. Uploads use the 3 UP workers | `verify` → MATCH |
| After the 20 s grace period | Missing copies are re-created on other workers. Replication is HEALTHY again with 3 workers | `replica_created` events |
| `docker start` | worker2 registers, enters SYNCING, is reconciled with its persistent data intact, then becomes UP | `worker_rejoined`, `worker_synced` |
| Afterwards | Chunks now have 3 healthy copies, so the surplus copies are trimmed and every chunk is back to exactly 2 | `replica_trimmed` events, `over-replicated=0` |

If worker2 comes back *within* the grace period, nothing is copied: its copies are verified and simply count again.

## 7. Data integrity

Corruption can come from bit rot, a failing disk, tampering, truncation or a deleted file. The system checks for it at four points:

| Where | Mechanism |
|---|---|
| On write | The worker rejects a body whose SHA-256 differs from `X-Chunk-Checksum`, with **400** |
| On every read | The worker re-hashes the chunk (409 if bad) **and** the controller re-hashes it against its own record |
| In the background | Each worker's scrubber re-hashes all of its chunks every `SCRUB_INTERVAL` seconds and reports bad ones in its heartbeats |
| On demand / on rejoin | `POST /api/v1/admin/scrub` and worker reconciliation re-hash every copy on every reachable worker |

A corrupt copy is never served while a valid copy exists. It is marked `corrupt` and then restored in place from a good copy. If *every* copy is corrupt, the controller returns an error instead of bad data.

## 8. Consistency and concurrency notes

* The controller holds the single source of truth, and SQLite transactions make metadata changes atomic.
* A copy's record is written only *after* the worker acknowledges the write. Reconciliation reads the records *before* the inventory, so a recorded copy is never wrongly declared missing because of a concurrent upload.
* Chunk IDs are immutable: an object's data never changes, so a copy is either valid or invalid with no versioning needed. Repair can overwrite a copy in place safely.
* Metadata updates are merges with a monotonically increasing `metadata_version`. Clients can use `If-Match` for optimistic concurrency (**409** on conflict).
* Deleting an object while a worker is DOWN deletes the remaining copies immediately and queues the rest. Reconciliation would also delete them as orphans when the worker returns.

## 9. Testing strategy

`tests/` starts the real controller and 4 real worker applications in one process. Their HTTP clients go through a router transport that can make any host unreachable, which simulates `docker stop`. Tests run maintenance and failure detection explicitly, so they are deterministic and fast. They cover:

* the API contract: status codes, JSON error bodies, pagination, metadata semantics, and the internal token;
* chunking, ordering, placement on distinct workers, and load balancing;
* the full stop → degraded → repair → rejoin → trim cycle, the grace period, write fail-over, a worker returning with a wiped disk, delayed deletes, orphans and decommissioning;
* corruption that is flipped, truncated or deleted: detection on read, by scrub, and by the worker scrubber, restoration in place, and the all-copies-corrupt case.
