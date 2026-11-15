#!/usr/bin/env python3
"""Command-line client for the Mini Distributed Cloud Object Store.

Uses only the Python standard library, so it runs on any machine with
Python 3.9+ (or inside the ``client`` container).

    export OBJSTORE_URL=http://localhost:8080     # default

    objstore_cli.py upload FILE [-m key=value ...] [--metadata-json JSON]
    objstore_cli.py download OBJECT_ID [-o PATH]
    objstore_cli.py verify OBJECT_ID FILE          download and compare with a local file
    objstore_cli.py info OBJECT_ID                 metadata (system + custom)
    objstore_cli.py locate OBJECT_ID               chunk -> worker placement
    objstore_cli.py set-meta OBJECT_ID k=v ... [--remove KEY] [--replace] [--if-match N]
    objstore_cli.py delete OBJECT_ID
    objstore_cli.py list [--limit N] [--offset N] [--prefix P]
    objstore_cli.py cluster | workers | events     cluster observability
    objstore_cli.py scrub | repair                 trigger integrity scan / repair
    objstore_cli.py wait-worker WORKER STATUS      block until a worker reaches a status
    objstore_cli.py wait-healthy [OBJECT_ID]       block until replication is healthy

Add ``--json`` (before the command) for raw JSON output.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any, Iterator

DEFAULT_URL = os.environ.get("OBJSTORE_URL", "http://localhost:8080")
IO_CHUNK = 1024 * 1024


def obj_path(object_id: str, suffix: str = "") -> str:
    return f"/api/v1/objects/{urllib.parse.quote(object_id, safe='')}{suffix}"


class ApiError(Exception):
    def __init__(self, status: int, body: Any):
        self.status = status
        self.body = body
        message = body.get("error", {}).get("message") if isinstance(body, dict) else str(body)
        super().__init__(f"HTTP {status}: {message}")


# ---------------------------------------------------------------------- HTTP helpers


class Client:
    def __init__(self, base_url: str, timeout: float = 300.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _open(self, method: str, path: str, data=None, headers: dict | None = None, params: dict | None = None):
        url = self.base_url + path
        if params:
            url += "?" + urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
        try:
            return urllib.request.urlopen(req, timeout=self.timeout)
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                body = json.loads(raw)
            except ValueError:
                body = raw.decode(errors="replace")
            raise ApiError(exc.code, body) from None
        except urllib.error.URLError as exc:
            raise SystemExit(f"error: cannot reach {self.base_url}: {exc.reason}") from None

    def json(self, method: str, path: str, body: Any = None, headers: dict | None = None,
             params: dict | None = None) -> Any:
        data = None
        hdrs = dict(headers or {})
        if body is not None:
            data = json.dumps(body).encode()
            hdrs["Content-Type"] = "application/json"
        with self._open(method, path, data, hdrs, params) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None

    def upload(self, path: str, name: str | None, content_type: str | None, metadata: dict) -> dict:
        """Stream a multipart/form-data upload without loading the file in memory."""
        boundary = "----objstore" + uuid.uuid4().hex
        filename = name or os.path.basename(path)
        ctype = content_type or mimetypes.guess_type(filename)[0] or "application/octet-stream"

        def field(key: str, value: str) -> bytes:
            return (f"--{boundary}\r\nContent-Disposition: form-data; name=\"{key}\"\r\n\r\n"
                    f"{value}\r\n").encode()

        head = b""
        if metadata:
            head += field("metadata", json.dumps(metadata))
        if name:
            head += field("name", name)
        safe_name = filename.replace('"', "%22").replace("\r", "").replace("\n", "")
        head += (f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{safe_name}\"\r\n"
                 f"Content-Type: {ctype}\r\n\r\n").encode()
        tail = f"\r\n--{boundary}--\r\n".encode()
        length = len(head) + os.path.getsize(path) + len(tail)

        def body() -> Iterator[bytes]:
            yield head
            with open(path, "rb") as fh:
                while block := fh.read(IO_CHUNK):
                    yield block
            yield tail

        headers = {"Content-Type": f"multipart/form-data; boundary={boundary}", "Content-Length": str(length)}
        with self._open("POST", "/api/v1/objects", body(), headers) as resp:
            return json.loads(resp.read())

    def download(self, object_id: str, out) -> tuple[int, str, dict]:
        """Stream an object into ``out``; returns (bytes, sha256, headers)."""
        hasher = hashlib.sha256()
        total = 0
        with self._open("GET", obj_path(object_id)) as resp:
            while block := resp.read(IO_CHUNK):
                hasher.update(block)
                out.write(block)
                total += len(block)
            return total, hasher.hexdigest(), dict(resp.headers)


# ---------------------------------------------------------------------- formatting


def human(n: int | None) -> str:
    if n is None:
        return "-"
    size = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.2f} {unit}"
        size /= 1024
    return str(n)


def table(rows: list[list[Any]], headers: list[str]) -> str:
    cells = [headers] + [[str(c) for c in r] for r in rows]
    widths = [max(len(r[i]) for r in cells) for i in range(len(headers))]
    lines = ["  ".join(c.ljust(w) for c, w in zip(r, widths)).rstrip() for r in cells]
    lines.insert(1, "  ".join("-" * w for w in widths))
    return "\n".join(lines)


def parse_kv(pairs: list[str]) -> dict[str, Any]:
    """Parse key=value pairs; values that look like JSON numbers/bools are typed."""
    result: dict[str, Any] = {}
    for pair in pairs:
        if "=" not in pair:
            raise SystemExit(f"error: expected key=value, got {pair!r}")
        key, value = pair.split("=", 1)
        try:
            parsed = json.loads(value)
            result[key] = parsed if isinstance(parsed, (int, float, bool)) else value
        except ValueError:
            result[key] = value
    return result


def sha256_file(path: str) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as fh:
        while block := fh.read(IO_CHUNK):
            hasher.update(block)
    return hasher.hexdigest()


def print_metadata(meta: dict) -> None:
    print(f"Object ID      : {meta['object_id']}")
    print(f"Name           : {meta['name']}")
    print(f"Size           : {meta['size']} bytes ({human(meta['size'])})")
    print(f"Content type   : {meta['content_type']}")
    print(f"Created        : {meta['created_at']}")
    print(f"Last modified  : {meta['updated_at']}")
    print(f"Checksum       : {meta['checksum_algorithm']}:{meta['checksum']}")
    print(f"Chunks         : {meta['num_chunks']} x {human(meta['chunk_size'])} (last chunk may be smaller)")
    print(f"Replication    : {meta['replication_status']} (factor {meta['replication_factor']})")
    print(f"Metadata ver.  : {meta['metadata_version']}")
    print("Custom metadata:")
    if meta["custom_metadata"]:
        for key, value in sorted(meta["custom_metadata"].items()):
            print(f"  {key} = {json.dumps(value) if not isinstance(value, str) else value}")
    else:
        print("  (none)")


def print_placement(meta: dict) -> None:
    rows = []
    per_worker: dict[str, int] = {}
    for chunk in meta.get("chunks") or []:
        copies = []
        for r in chunk["replicas"]:
            flag = "" if r["available"] else f" [{r['status']}/{r['worker_status']}]"
            copies.append(r["worker_id"] + flag)
            per_worker[r["worker_id"]] = per_worker.get(r["worker_id"], 0) + 1
        rows.append([chunk["index"], chunk["chunk_id"], human(chunk["size"]), chunk["checksum"][:12],
                     f"{chunk['healthy_replicas']}/{meta['replication_factor']}", ", ".join(copies)])
    print(f"Object {meta['object_id']}  ({meta['name']}, {human(meta['size'])}, "
          f"{meta['num_chunks']} chunks, replication {meta['replication_status']})")
    print(table(rows, ["IDX", "CHUNK ID", "SIZE", "SHA256", "COPIES", "WORKERS"]))
    if per_worker:
        print("Copies per worker: " + ", ".join(f"{w}={n}" for w, n in sorted(per_worker.items())))


def print_cluster(status: dict) -> None:
    ok = "HEALTHY" if status["replication_healthy"] else "NOT HEALTHY"
    print(f"Replication factor {status['replication_factor']}, chunk size {human(status['chunk_size'])}, "
          f"writable={status['writable']}")
    print(f"Workers: {status['workers_up']} UP, {status['workers_syncing']} SYNCING, {status['workers_down']} DOWN")
    print(f"Objects: {status['objects']} ({human(status['stored_bytes'])}), chunks: {status['chunks']}, "
          f"copies: {status['replicas']}")
    print(f"Replication {ok}: fully replicated={status['fully_replicated']}, "
          f"under-replicated={status['under_replicated']}, unavailable={status['unavailable']}, "
          f"over-replicated={status['over_replicated']}, corrupt copies={status['corrupt_replicas']}, "
          f"pending deletes={status['pending_deletes']}")
    print()
    print_workers(status["workers"])


def print_workers(workers: list[dict]) -> None:
    rows = [[w["worker_id"], w["status"], f"{w['seconds_since_heartbeat']}s", w["replicas_recorded"],
             w["chunk_count"], human(w["used_bytes"]), w["url"]] for w in workers]
    print(table(rows, ["WORKER", "STATUS", "LAST HB", "COPIES (CTRL)", "FILES (WORKER)", "USED", "URL"]))


# ---------------------------------------------------------------------- commands


def cmd_upload(c: Client, a) -> Any:
    metadata = json.loads(a.metadata_json) if a.metadata_json else {}
    metadata.update(parse_kv(a.meta or []))
    started = time.time()
    meta = c.upload(a.file, a.name, a.content_type, metadata)
    if a.json:
        return meta
    if a.quiet:
        print(meta["object_id"])
        return None
    print(f"Uploaded {a.file} in {time.time() - started:.2f}s")
    print_metadata(meta)
    print()
    print_placement(meta)
    return None


def cmd_download(c: Client, a) -> Any:
    out_path = a.output
    if out_path is None:
        meta = c.json("GET", obj_path(a.object_id, "/metadata"), params={"include_chunks": "false"})
        out_path = os.path.basename(meta["name"]) or a.object_id
    tmp = out_path + ".part"
    with open(tmp, "wb") as fh:
        size, digest, headers = c.download(a.object_id, fh)
    os.replace(tmp, out_path)
    expected = headers.get("X-Checksum-SHA256")
    status = "OK" if expected == digest else f"MISMATCH (server says {expected})"
    print(f"Downloaded {size} bytes to {out_path} [{headers.get('Content-Type')}] sha256={digest} {status}")
    return None


def cmd_verify(c: Client, a) -> Any:
    local = sha256_file(a.file)

    class _Null:
        def write(self, _):
            pass

    size, digest, _ = c.download(a.object_id, _Null())
    match = digest == local
    print(f"local  sha256={local} ({os.path.getsize(a.file)} bytes)")
    print(f"remote sha256={digest} ({size} bytes)")
    print("MATCH: retrieved object is identical to the original" if match else "DIFFERENT!")
    if not match:
        raise SystemExit(1)
    return None


def cmd_info(c: Client, a) -> Any:
    meta = c.json("GET", obj_path(a.object_id, "/metadata"), params={"include_chunks": "true"})
    if a.json:
        return meta
    print_metadata(meta)
    return None


def cmd_locate(c: Client, a) -> Any:
    meta = c.json("GET", obj_path(a.object_id, "/metadata"))
    if a.json:
        return meta["chunks"]
    if a.chunk is not None:
        chunk = next((ch for ch in meta["chunks"] if ch["index"] == a.chunk), None)
        if chunk is None:
            raise SystemExit(f"error: object has no chunk {a.chunk}")
        print(chunk["chunk_id"], *[r["worker_id"] for r in chunk["replicas"] if r["available"]])
        return None
    print_placement(meta)
    return None


def cmd_set_meta(c: Client, a) -> Any:
    updates: dict[str, Any] = parse_kv(a.pairs or [])
    for key in a.remove or []:
        updates[key] = None
    headers = {"If-Match": str(a.if_match)} if a.if_match is not None else {}
    meta = c.json("PATCH", obj_path(a.object_id, "/metadata"), updates, headers=headers,
                  params={"replace": "true" if a.replace else None, "include_chunks": "false"})
    if a.json:
        return meta
    print_metadata(meta)
    return None


def cmd_delete(c: Client, a) -> Any:
    c.json("DELETE", obj_path(a.object_id))
    print(f"Deleted {a.object_id}")
    return None


def cmd_list(c: Client, a) -> Any:
    result = c.json("GET", "/api/v1/objects", params={"limit": a.limit, "offset": a.offset, "prefix": a.prefix})
    if a.json:
        return result
    rows = [[o["object_id"], o["name"], human(o["size"]), o["content_type"], o["num_chunks"], o["created_at"]]
            for o in result["objects"]]
    print(table(rows, ["OBJECT ID", "NAME", "SIZE", "CONTENT TYPE", "CHUNKS", "CREATED"]))
    print(f"{len(rows)} of {result['total']} objects"
          + (f" (next page: --offset {result['next_offset']})" if result["next_offset"] is not None else ""))
    return None


def cmd_cluster(c: Client, a) -> Any:
    status = c.json("GET", "/api/v1/cluster")
    if a.json:
        return status
    print_cluster(status)
    return None


def cmd_workers(c: Client, a) -> Any:
    workers = c.json("GET", "/api/v1/workers")
    if a.json:
        return workers
    print_workers(workers)
    return None


def cmd_events(c: Client, a) -> Any:
    events = c.json("GET", "/api/v1/events", params={"limit": a.limit, "level": a.level})
    if a.json:
        return events
    for e in reversed(events):
        print(f"{e['time']}  {e['level'].upper():7}  {e['type']:26} {e['message']}")
    return None


def cmd_scrub(c: Client, a) -> Any:
    return c.json("POST", "/api/v1/admin/scrub")


def cmd_repair(c: Client, a) -> Any:
    return c.json("POST", "/api/v1/admin/repair")


def cmd_wait_worker(c: Client, a) -> Any:
    deadline = time.time() + a.timeout
    want = a.status.upper()
    while True:
        workers = {w["worker_id"]: w for w in c.json("GET", "/api/v1/workers")}
        current = workers.get(a.worker, {}).get("status", "UNKNOWN")
        if current == want:
            print(f"{a.worker} is {want}")
            return None
        if time.time() > deadline:
            raise SystemExit(f"timeout: {a.worker} is {current}, expected {want}")
        time.sleep(1)


def cmd_wait_healthy(c: Client, a) -> Any:
    deadline = time.time() + a.timeout
    while True:
        if a.object_id:
            meta = c.json("GET", obj_path(a.object_id, "/metadata"))
            healthy = meta["replication_status"] == "healthy" and all(
                len(ch["replicas"]) == meta["replication_factor"] for ch in meta["chunks"])
        else:
            healthy = c.json("GET", "/api/v1/cluster")["replication_healthy"]
        if healthy:
            print("replication is healthy")
            return None
        if time.time() > deadline:
            raise SystemExit("timeout waiting for healthy replication")
        time.sleep(1)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="objstore", description=__doc__.split("\n\n")[0])
    p.add_argument("--url", default=DEFAULT_URL, help=f"controller URL (default {DEFAULT_URL})")
    p.add_argument("--json", action="store_true", help="print raw JSON responses")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("upload", help="upload a file")
    s.add_argument("file")
    s.add_argument("--name", help="object name (default: file name)")
    s.add_argument("--content-type")
    s.add_argument("-m", "--meta", action="append", metavar="KEY=VALUE", help="custom metadata (repeatable)")
    s.add_argument("--metadata-json", help="custom metadata as a JSON object")
    s.add_argument("-q", "--quiet", action="store_true", help="only print the object id")
    s.set_defaults(func=cmd_upload)

    s = sub.add_parser("download", help="download an object")
    s.add_argument("object_id")
    s.add_argument("-o", "--output", help="output path (default: object name)")
    s.set_defaults(func=cmd_download)

    s = sub.add_parser("verify", help="download an object and compare it with a local file")
    s.add_argument("object_id")
    s.add_argument("file")
    s.set_defaults(func=cmd_verify)

    s = sub.add_parser("info", help="show object metadata")
    s.add_argument("object_id")
    s.set_defaults(func=cmd_info)

    s = sub.add_parser("locate", help="show where every chunk copy is stored")
    s.add_argument("object_id")
    s.add_argument("--chunk", type=int, help="print '<chunk_id> <worker> ...' for one chunk index")
    s.set_defaults(func=cmd_locate)

    s = sub.add_parser("set-meta", help="add/update/remove custom metadata")
    s.add_argument("object_id")
    s.add_argument("pairs", nargs="*", metavar="KEY=VALUE")
    s.add_argument("--remove", action="append", metavar="KEY")
    s.add_argument("--replace", action="store_true", help="replace all custom metadata")
    s.add_argument("--if-match", type=int, metavar="VERSION", help="fail with 409 if the version changed")
    s.set_defaults(func=cmd_set_meta)

    s = sub.add_parser("delete", help="delete an object")
    s.add_argument("object_id")
    s.set_defaults(func=cmd_delete)

    s = sub.add_parser("list", help="list objects")
    s.add_argument("--limit", type=int, default=100)
    s.add_argument("--offset", type=int, default=0)
    s.add_argument("--prefix")
    s.set_defaults(func=cmd_list)

    sub.add_parser("cluster", help="cluster and replication status").set_defaults(func=cmd_cluster)
    sub.add_parser("workers", help="worker status").set_defaults(func=cmd_workers)

    s = sub.add_parser("events", help="recent cluster events")
    s.add_argument("--limit", type=int, default=30)
    s.add_argument("--level", choices=["info", "warning", "error"])
    s.set_defaults(func=cmd_events)

    sub.add_parser("scrub", help="verify every stored copy now and repair").set_defaults(func=cmd_scrub)
    sub.add_parser("repair", help="run a repair round now").set_defaults(func=cmd_repair)

    s = sub.add_parser("wait-worker", help="wait until a worker has a given status")
    s.add_argument("worker")
    s.add_argument("status", choices=["UP", "DOWN", "SYNCING", "up", "down", "syncing"])
    s.add_argument("--timeout", type=float, default=120)
    s.set_defaults(func=cmd_wait_worker)

    s = sub.add_parser("wait-healthy", help="wait until replication is healthy")
    s.add_argument("object_id", nargs="?")
    s.add_argument("--timeout", type=float, default=180)
    s.set_defaults(func=cmd_wait_healthy)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    client = Client(args.url)
    try:
        result = args.func(client, args)
    except ApiError as exc:
        print(f"error: {exc}", file=sys.stderr)
        if isinstance(exc.body, dict) and exc.body.get("error", {}).get("details"):
            print(json.dumps(exc.body["error"]["details"], indent=2), file=sys.stderr)
        return 1
    if result is not None:
        print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
