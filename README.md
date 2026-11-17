# Mini Distributed Cloud Object Store

A distributed object storage system that stores arbitrary binary objects (documents, images, video, audio, archives and so on) across **4 storage workers**, coordinated by **1 controller**. Objects are split into **1 MB chunks**, and every chunk is stored on **2 different workers**. The system detects worker failures and data corruption and restores replication automatically.

```
                       ┌───────────────────────────────────────────┐
  clients ── REST ───► │ controller  (FastAPI + SQLite on volume)  │
  (CLI, curl,          │  • object API  • placement  • metadata    │
   dashboard)          │  • failure detector  • repair / scrub     │
                       └──────┬──────────┬──────────┬──────────┬───┘
                internal API  │          │          │          │   ▲ register /
                (chunks)      ▼          ▼          ▼          ▼   │ heartbeat
                         ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐
                         │worker1 │ │worker2 │ │worker3 │ │worker4 │
                         │ volume │ │ volume │ │ volume │ │ volume │
                         └────────┘ └────────┘ └────────┘ └────────┘
```

| Parameter            | Value                                   |
|----------------------|-----------------------------------------|
| Controller nodes     | 1 (`controller`)                        |
| Storage/worker nodes | 4 (`worker1` … `worker4`)               |
| Clients              | 1 or more (host CLI, `client` container, browser dashboard, curl) |
| Chunk size           | 1 MB (1,048,576 bytes; the last chunk may be smaller) |
| Replication factor   | 2, with the two copies always on different workers |
| Deployment           | Docker Compose (no Kubernetes)          |
| Worker storage       | One named Docker volume per worker      |

More detail is in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) (design) and [docs/DEMO.md](docs/DEMO.md) (step-by-step demonstration script).

---

## Quick start

Prerequisites: Docker with the Compose plugin, and Python 3.9+ on the host if you want to use the CLI from the host.

```bash
# 1. Build and start the controller and the 4 workers
docker compose up -d --build

# 2. Check that all 4 workers have registered and are UP
python3 client/objstore_cli.py cluster

# 3. Run every required demonstration (≈ 2 minutes)
scripts/demo.sh            # or: make demo
```

* REST API: <http://localhost:8080/api/v1/objects>
* Interactive API docs (OpenAPI/Swagger): <http://localhost:8080/docs>
* Live dashboard with workers, replication health, chunk placement, events and uploads: <http://localhost:8080/>

If port 8080 is already in use, start with `CONTROLLER_PORT=18080 docker compose up -d` and `export OBJSTORE_URL=http://localhost:18080`.

Stop the cluster with `docker compose down`. Data is kept in volumes; `docker compose down -v` deletes it.

---

## Using the system

### CLI (host or container)

`client/objstore_cli.py` uses only the Python standard library, so it needs no `pip install`.

```bash
alias objstore="python3 client/objstore_cli.py"        # talks to $OBJSTORE_URL (default http://localhost:8080)

objstore upload movie.mp4 -m owner=alice -m project=demo   # 201: prints metadata + chunk placement
objstore list
objstore download <id> -o copy.mp4                         # checks the SHA-256 after download
objstore verify <id> movie.mp4                             # download and compare with the original
objstore info <id>                                         # system + custom metadata
objstore locate <id>                                       # where each chunk's 2 copies are stored
objstore set-meta <id> owner=bob reviewed=true --remove project
objstore delete <id>
objstore cluster | workers | events                        # health, replication and audit log
objstore scrub | repair                                    # force an integrity scan / repair round
```

The same CLI runs as a containerised client inside the cluster network. Files in `./demo-data` are visible at `/work`:

```bash
docker compose run --rm client upload /work/somefile.bin
docker compose run --rm client cluster
```

### REST API with curl

```bash
# Upload (multipart/form-data: required "file", optional "metadata" JSON)
curl -F file=@photo.jpg -F 'metadata={"owner":"alice","camera":"x100"}' localhost:8080/api/v1/objects

curl -o photo.jpg        localhost:8080/api/v1/objects/<id>             # download
curl                     localhost:8080/api/v1/objects/<id>/metadata    # metadata only
curl -X PATCH -H 'Content-Type: application/json' \
     -d '{"owner":"bob","camera":null}' localhost:8080/api/v1/objects/<id>/metadata   # null removes a key
curl -X DELETE           localhost:8080/api/v1/objects/<id>             # 204
curl 'localhost:8080/api/v1/objects?limit=20&offset=0&prefix=photos/'   # list
```

---

## API reference

### Client API (specification §5)

| Method | Endpoint | Purpose | Success | Errors |
|---|---|---|---|---|
| POST | `/api/v1/objects` | Upload an object (`file`, optional `metadata` JSON, optional `name`, `content_type`) | 201 + `Location` | 400, 413, 503 |
| GET | `/api/v1/objects/{id}` | Download the original bytes with the right `Content-Type` | 200 | 404, 500, 503 |
| GET | `/api/v1/objects/{id}/metadata` | System + custom metadata, chunk list and locations (`?include_chunks=false` to omit chunks) | 200 | 404 |
| PATCH | `/api/v1/objects/{id}/metadata` | Merge custom metadata (`null` deletes a key; `?replace=true`; optional `If-Match: <version>`) | 200 | 400, 404, 409 |
| DELETE | `/api/v1/objects/{id}` | Delete the object and all of its chunk copies | 204 | 404, 409 |
| GET | `/api/v1/objects` | List objects (`limit`, `offset`, `prefix`; returns `total` and `next_offset`) | 200 | 400 |

Download responses also carry `Content-Length`, `Content-Disposition`, `ETag`, `Last-Modified` and `X-Checksum-SHA256`.

**Metadata returned for every object:** `object_id`, `name`, `size`, `content_type`, `created_at`, `updated_at`, `checksum` (SHA-256), `num_chunks`, `chunk_size`, `replication_factor`, `replication_status` (`healthy` / `degraded` / `unavailable`), `metadata_version`, `custom_metadata`, and `chunks[]`. Each chunk entry has `chunk_id`, `index`, `size`, `checksum`, `healthy_replicas`, and `replicas[]` (`worker_id`, `status`, `worker_status`, `available`).

**Errors** are always JSON:

```json
{"error": {"status": 404, "code": "not_found", "message": "object '…' does not exist"}}
```

| Status | When |
|---|---|
| 400 | Malformed request: missing `file`, invalid metadata JSON, nested values, bad query parameters |
| 404 | Object (or worker) not found |
| 409 | `If-Match` version conflict on a metadata update, or a delete that is already in progress |
| 413 | Object larger than `MAX_OBJECT_SIZE` |
| 500 | Every copy of a chunk is corrupted (`data_integrity_error`); unexpected errors |
| 503 | Fewer than 2 workers UP for an upload, or no reachable copy of a chunk for a download |

### Internal worker API (specification §6)

| Method | Endpoint | Served by | Purpose |
|---|---|---|---|
| POST | `/internal/workers/register` | controller | Worker registers on start-up (and again if the controller forgets it) |
| POST | `/internal/workers/heartbeat` | controller | Liveness every 3 s, with storage stats and any chunks the worker found corrupted |
| PUT | `/internal/chunks/{chunk_id}` | worker | Store a chunk. The body is raw bytes; the `X-Chunk-Checksum` header is checked on arrival |
| GET | `/internal/chunks/{chunk_id}` | worker | Return a chunk after re-hashing it. Returns **409** if the stored data is corrupt |
| DELETE | `/internal/chunks/{chunk_id}` | worker | Delete a chunk |
| GET | `/internal/workers/{worker_id}/chunks` | worker (actual inventory, `?verify=true` re-hashes everything) and controller (expected inventory) | Chunk inventory |

If `INTERNAL_TOKEN` is set, every internal call must send a matching `X-Internal-Token` header.

### Observability and admin API (extra)

`GET /api/v1/cluster`, `GET /api/v1/workers`, `GET /api/v1/workers/{id}/chunks`, `GET /api/v1/events`, `POST /api/v1/admin/scrub`, `POST /api/v1/admin/repair`, `DELETE /api/v1/admin/workers/{id}` (permanently decommission a worker).

---

## How the requirements are met

| Requirement | Implementation |
|---|---|
| Arbitrary binary objects | Data is streamed as opaque bytes. `Content-Type` comes from the upload, or is guessed from the file name |
| Unique Object ID | UUID4 assigned by the controller |
| Required metadata | `objects` / `chunks` / `replicas` tables in SQLite on the controller's persistent volume ([store.py](controller/objstore_controller/store.py)) |
| Custom key/value metadata | `metadata` form field on upload. `PATCH` merges keys, `null` removes a key, and the object data is never touched |
| 1 MB chunks, ordering, unique IDs | Chunk ID is `<object_id>_<index>`. The index is stored and chunks are reassembled in index order |
| 2 copies on different workers | Placement rotates a load-ordered worker list by chunk index, so consecutive chunks land on different worker pairs. If a write fails, the next worker is tried |
| Detect and restore replication factor | Heartbeat failure detector, then a repair loop that copies chunks from a healthy copy to another worker after a grace period. When the failed worker returns it is reconciled and surplus copies are trimmed |
| Keep serving during failures | Reads try every copy (healthy and UP first). Uploads use any 2 UP workers |
| Corruption detection | SHA-256 per chunk (stored on both controller and worker) plus SHA-256 per object. Checked on every read, by the worker's background scrubber, on worker rejoin, and by `POST /admin/scrub` |
| Never return corrupt data | The controller verifies every chunk before sending it. A bad copy is skipped and flagged, and the next copy is used. If no valid copy exists, the client gets an error instead of corrupt bytes |
| Restore corrupted data | Corrupt copies are rewritten in place from a healthy copy. Missing copies are re-created |
| Persistent worker storage | One named volume per worker. Writes are atomic (temp file + fsync + rename) |
| Docker, no Kubernetes | `docker-compose.yml`. Container names `worker1`…`worker4`, so `docker stop worker2` works as the specification describes |

---

## Configuration

Controller settings (set via environment variables in `docker-compose.yml`):

| Variable | Default | Meaning |
|---|---|---|
| `CHUNK_SIZE` | 1048576 | Chunk size in bytes |
| `REPLICATION_FACTOR` | 2 | Number of copies per chunk |
| `HEARTBEAT_TIMEOUT` | 10 | Seconds without a heartbeat before a worker is marked DOWN |
| `REPAIR_GRACE_PERIOD` | 20 | Seconds a worker may stay DOWN before its chunks are re-replicated elsewhere |
| `MAINTENANCE_INTERVAL` | 5 | Seconds between idle repair / garbage-collection rounds |
| `RECONCILE_VERIFY` | true | Re-hash a worker's chunks when it (re)joins |
| `READ_AHEAD_CHUNKS` | 2 | Download prefetch window |
| `MAX_OBJECT_SIZE` | 10 GiB | Upload limit (0 = unlimited) |
| `INTERNAL_TOKEN` | – | Shared secret for `/internal/*` |

Worker settings: `WORKER_ID`, `WORKER_URL`, `CONTROLLER_URL`, `HEARTBEAT_INTERVAL` (3), `SCRUB_INTERVAL` (60; 0 disables), `INTERNAL_TOKEN`.

---

## Project layout

```
controller/                     controller service (Dockerfile, requirements)
  objstore_controller/
    main.py                     app factory, error handlers, background loops
    config.py                   settings from environment variables
    store.py                    SQLite metadata store (objects, chunks, replicas, workers, events)
    worker_client.py            HTTP client for the internal worker API
    services/objects.py         upload / download / metadata / delete / list
    services/replication.py     verified reads, repair, trim, reconcile, scrub, GC
    services/cluster.py         registration, heartbeats, failure detection
    api/objects.py              /api/v1/objects routes
    api/cluster.py              observability + admin routes
    api/internal.py             /internal routes used by workers
    static/index.html           dashboard
worker/
  objstore_worker/
    main.py                     internal chunk API
    storage.py                  atomic on-disk chunk store with SHA-256 verification
    controller_link.py          register / heartbeat loop, background scrubber
client/objstore_cli.py          standard-library-only CLI (also built as the `client` image)
scripts/demo.sh                 all required demonstrations
scripts/corrupt_chunk.sh        intentionally damage a stored chunk (flip / truncate / delete)
scripts/local_cluster.sh        run the cluster as local processes (no Docker)
tests/                          in-process cluster tests (API, failures, integrity, storage)
docs/                           architecture and demo guides
```

---

## Development and tests

The test suite starts a full cluster (controller + 4 workers) **in-process**. The nodes talk over a simulated network that can make a worker unreachable, so failure and corruption scenarios run in seconds without Docker:

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
pytest                                   # 50 tests
```

To run the real services without Docker (ports 8080 and 9001–9004; set `CONTROLLER_PORT` to change the controller port):

```bash
pip install uvicorn
scripts/local_cluster.sh up
OBJSTORE_MODE=local scripts/demo.sh      # stop/start/corrupt act on local processes
scripts/local_cluster.sh clean
```

## Limitations

These are deliberate simplifications for a mini system:

* The controller is a single point of coordination. Its metadata is persistent, but it is not replicated.
* Chunk data flows through the controller on upload, download and repair. Workers do not copy chunks to each other directly.
* If a chunk fails *after* a download has started streaming, the connection is aborted (the client sees a short body) because the 200 status has already been sent. A failure on the first chunk is reported as a normal JSON error.
* Objects are immutable: updating the data means uploading a new object. Metadata can be updated.
