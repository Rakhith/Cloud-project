"""SQLite-backed metadata store.

The controller is the single source of truth for *what* is stored *where*.
Everything lives in one SQLite database on the controller's persistent
volume, so the controller can be restarted without losing the namespace.

Tables
------
workers          registered storage workers and their liveness state
objects          one row per object (system + custom metadata)
chunks           one row per chunk (ordered by ``idx`` inside an object)
replicas         one row per (chunk, worker) copy, with a health status
pending_deletes  chunk copies that still have to be removed from a worker
events           an append-only audit log used for demos / debugging

Worker status values
--------------------
UP       heartbeating and reconciled; eligible for reads, writes, repair
SYNCING  (re)joined; its inventory is being reconciled before it counts
DOWN     missed heartbeats for longer than ``heartbeat_timeout``

Replica status values
---------------------
healthy  believed to be a valid copy
corrupt  checksum mismatch detected; must not be served, will be repaired

Object status values
--------------------
pending   upload in progress (invisible to clients)
active    fully stored and readable
deleting  deletion in progress (invisible to clients)

All methods are synchronous and guarded by a re-entrant lock; every
statement is fast, so calling them from the asyncio event loop is fine for a
system of this scale.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterable, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS workers (
    worker_id         TEXT PRIMARY KEY,
    url               TEXT NOT NULL,
    status            TEXT NOT NULL,
    registered_at     REAL NOT NULL,
    last_heartbeat    REAL NOT NULL,
    status_changed_at REAL NOT NULL,
    capacity_bytes    INTEGER,
    used_bytes        INTEGER NOT NULL DEFAULT 0,
    free_bytes        INTEGER,
    chunk_count       INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS objects (
    object_id        TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    size             INTEGER NOT NULL DEFAULT 0,
    content_type     TEXT NOT NULL,
    checksum         TEXT,
    chunk_size       INTEGER NOT NULL,
    num_chunks       INTEGER NOT NULL DEFAULT 0,
    status           TEXT NOT NULL,
    custom_metadata  TEXT NOT NULL DEFAULT '{}',
    metadata_version INTEGER NOT NULL DEFAULT 1,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_objects_status_created ON objects(status, created_at);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id  TEXT PRIMARY KEY,
    object_id TEXT NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
    idx       INTEGER NOT NULL,
    size      INTEGER NOT NULL,
    checksum  TEXT NOT NULL,
    UNIQUE (object_id, idx)
);

CREATE TABLE IF NOT EXISTS replicas (
    chunk_id   TEXT NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
    worker_id  TEXT NOT NULL,
    status     TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (chunk_id, worker_id)
);
CREATE INDEX IF NOT EXISTS idx_replicas_worker ON replicas(worker_id);

CREATE TABLE IF NOT EXISTS pending_deletes (
    chunk_id   TEXT NOT NULL,
    worker_id  TEXT NOT NULL,
    created_at REAL NOT NULL,
    attempts   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (chunk_id, worker_id)
);

CREATE TABLE IF NOT EXISTS events (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts      REAL NOT NULL,
    level   TEXT NOT NULL,
    type    TEXT NOT NULL,
    message TEXT NOT NULL,
    details TEXT NOT NULL DEFAULT '{}'
);
"""

WORKER_UP = "UP"
WORKER_SYNCING = "SYNCING"
WORKER_DOWN = "DOWN"

REPLICA_HEALTHY = "healthy"
REPLICA_CORRUPT = "corrupt"

OBJECT_PENDING = "pending"
OBJECT_ACTIVE = "active"
OBJECT_DELETING = "deleting"

MAX_EVENTS = 10_000
_BATCH = 500


class VersionConflict(Exception):
    def __init__(self, current_version: int):
        super().__init__(f"metadata version is {current_version}")
        self.current_version = current_version


def _chunks(items: list, size: int = _BATCH) -> Iterator[list]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


class MetadataStore:
    def __init__(self, db_path: str):
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA)
        self._event_inserts = 0

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ helpers

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    def _all(self, sql: str, params: Iterable[Any] = ()) -> list[dict]:
        with self._lock:
            return [dict(r) for r in self._conn.execute(sql, tuple(params)).fetchall()]

    def _one(self, sql: str, params: Iterable[Any] = ()) -> dict | None:
        with self._lock:
            row = self._conn.execute(sql, tuple(params)).fetchone()
            return dict(row) if row else None

    def _exec(self, sql: str, params: Iterable[Any] = ()) -> int:
        with self._lock:
            return self._conn.execute(sql, tuple(params)).rowcount

    # ------------------------------------------------------------------ workers

    def upsert_worker(self, worker_id: str, url: str, status: str, now: float, stats: dict) -> dict | None:
        """Insert or refresh a worker on registration. Returns the previous row."""
        with self._tx() as c:
            prev = c.execute("SELECT * FROM workers WHERE worker_id = ?", (worker_id,)).fetchone()
            if prev is None:
                c.execute(
                    """INSERT INTO workers (worker_id, url, status, registered_at, last_heartbeat,
                           status_changed_at, capacity_bytes, used_bytes, free_bytes, chunk_count)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (worker_id, url, status, now, now, now, stats.get("capacity_bytes"),
                     stats.get("used_bytes", 0), stats.get("free_bytes"), stats.get("chunk_count", 0)),
                )
            else:
                c.execute(
                    """UPDATE workers SET url = ?, status = ?, registered_at = ?, last_heartbeat = ?,
                           status_changed_at = CASE WHEN status = ? THEN status_changed_at ELSE ? END,
                           capacity_bytes = ?, used_bytes = ?, free_bytes = ?, chunk_count = ?
                       WHERE worker_id = ?""",
                    (url, status, now, now, status, now, stats.get("capacity_bytes"),
                     stats.get("used_bytes", 0), stats.get("free_bytes"), stats.get("chunk_count", 0),
                     worker_id),
                )
        return dict(prev) if prev else None

    def touch_worker(self, worker_id: str, now: float, stats: dict) -> dict | None:
        """Record a heartbeat. Returns the previous row, or None if unknown."""
        with self._tx() as c:
            prev = c.execute("SELECT * FROM workers WHERE worker_id = ?", (worker_id,)).fetchone()
            if prev is None:
                return None
            c.execute(
                """UPDATE workers SET last_heartbeat = ?, capacity_bytes = ?, used_bytes = ?,
                       free_bytes = ?, chunk_count = ?
                   WHERE worker_id = ?""",
                (now, stats.get("capacity_bytes"), stats.get("used_bytes", 0),
                 stats.get("free_bytes"), stats.get("chunk_count", 0), worker_id),
            )
        return dict(prev)

    def set_worker_status(self, worker_id: str, status: str, now: float, expected: str | None = None) -> bool:
        """Change a worker's status (optionally only if it currently is ``expected``)."""
        sql = "UPDATE workers SET status = ?, status_changed_at = ? WHERE worker_id = ? AND status != ?"
        params: list[Any] = [status, now, worker_id, status]
        if expected is not None:
            sql += " AND status = ?"
            params.append(expected)
        return self._exec(sql, params) > 0

    def get_worker(self, worker_id: str) -> dict | None:
        return self._one("SELECT * FROM workers WHERE worker_id = ?", (worker_id,))

    def list_workers(self, status: str | None = None) -> list[dict]:
        if status is None:
            return self._all("SELECT * FROM workers ORDER BY worker_id")
        return self._all("SELECT * FROM workers WHERE status = ? ORDER BY worker_id", (status,))

    def stale_workers(self, cutoff: float) -> list[dict]:
        return self._all(
            "SELECT * FROM workers WHERE status != ? AND last_heartbeat < ?", (WORKER_DOWN, cutoff)
        )

    def remove_worker(self, worker_id: str) -> int:
        """Forget a worker entirely (decommission). Returns removed replica count."""
        with self._tx() as c:
            removed = c.execute("DELETE FROM replicas WHERE worker_id = ?", (worker_id,)).rowcount
            c.execute("DELETE FROM pending_deletes WHERE worker_id = ?", (worker_id,))
            c.execute("DELETE FROM workers WHERE worker_id = ?", (worker_id,))
        return removed

    def prepare_for_startup(self, now: float) -> None:
        """After a controller restart, liveness is unknown: every worker that was
        not DOWN must re-prove itself (heartbeat + reconcile) before it counts."""
        self._exec(
            "UPDATE workers SET status = ?, last_heartbeat = ?, status_changed_at = ? WHERE status != ?",
            (WORKER_SYNCING, now, now, WORKER_DOWN),
        )

    # ------------------------------------------------------------------ objects

    @staticmethod
    def _decode_object(row: dict | None) -> dict | None:
        if row is not None:
            row["custom_metadata"] = json.loads(row["custom_metadata"] or "{}")
        return row

    def create_object(self, object_id: str, name: str, content_type: str, chunk_size: int,
                      custom_metadata: dict, now: float) -> None:
        self._exec(
            """INSERT INTO objects (object_id, name, content_type, chunk_size, status,
                   custom_metadata, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (object_id, name, content_type, chunk_size, OBJECT_PENDING,
             json.dumps(custom_metadata), now, now),
        )

    def finalize_object(self, object_id: str, size: int, checksum: str, num_chunks: int, now: float) -> None:
        self._exec(
            """UPDATE objects SET size = ?, checksum = ?, num_chunks = ?, status = ?, updated_at = ?
               WHERE object_id = ?""",
            (size, checksum, num_chunks, OBJECT_ACTIVE, now, object_id),
        )

    def get_object(self, object_id: str, statuses: tuple[str, ...] = (OBJECT_ACTIVE,)) -> dict | None:
        marks = ",".join("?" * len(statuses))
        row = self._one(
            f"SELECT * FROM objects WHERE object_id = ? AND status IN ({marks})", (object_id, *statuses)
        )
        return self._decode_object(row)

    def list_objects(self, limit: int, offset: int, prefix: str | None = None) -> tuple[list[dict], int]:
        where = "status = ?"
        params: list[Any] = [OBJECT_ACTIVE]
        if prefix:
            where += " AND name LIKE ? ESCAPE '\\'"
            escaped = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            params.append(escaped + "%")
        total = self._one(f"SELECT COUNT(*) AS n FROM objects WHERE {where}", params)["n"]
        rows = self._all(
            f"SELECT * FROM objects WHERE {where} ORDER BY created_at, object_id LIMIT ? OFFSET ?",
            [*params, limit, offset],
        )
        return [self._decode_object(r) for r in rows], total

    def update_custom_metadata(self, object_id: str, updates: dict, replace: bool,
                               expected_version: int | None, now: float) -> dict | None:
        """Merge (or replace) custom metadata. ``None`` values delete keys.

        Returns the updated object, ``None`` if it does not exist, and raises
        ``VersionConflict`` if ``expected_version`` does not match.
        """
        with self._tx() as c:
            row = c.execute(
                "SELECT custom_metadata, metadata_version FROM objects WHERE object_id = ? AND status = ?",
                (object_id, OBJECT_ACTIVE),
            ).fetchone()
            if row is None:
                return None
            if expected_version is not None and row["metadata_version"] != expected_version:
                raise VersionConflict(row["metadata_version"])
            current = {} if replace else json.loads(row["custom_metadata"] or "{}")
            for key, value in updates.items():
                if value is None:
                    current.pop(key, None)
                else:
                    current[key] = value
            c.execute(
                """UPDATE objects SET custom_metadata = ?, metadata_version = metadata_version + 1,
                       updated_at = ? WHERE object_id = ?""",
                (json.dumps(current), now, object_id),
            )
        return self.get_object(object_id)

    def begin_delete(self, object_id: str, now: float) -> bool:
        return self._exec(
            "UPDATE objects SET status = ?, updated_at = ? WHERE object_id = ? AND status = ?",
            (OBJECT_DELETING, now, object_id, OBJECT_ACTIVE),
        ) > 0

    def purge_object(self, object_id: str) -> list[tuple[str, str]]:
        """Delete an object's rows. Returns the (chunk_id, worker_id) copies it had."""
        with self._tx() as c:
            pairs = [
                (r["chunk_id"], r["worker_id"])
                for r in c.execute(
                    """SELECT r.chunk_id, r.worker_id FROM replicas r
                       JOIN chunks ch ON ch.chunk_id = r.chunk_id WHERE ch.object_id = ?""",
                    (object_id,),
                )
            ]
            c.execute("DELETE FROM objects WHERE object_id = ?", (object_id,))
        return pairs

    def object_replica_pairs(self, object_id: str) -> list[tuple[str, str]]:
        return [
            (r["chunk_id"], r["worker_id"])
            for r in self._all(
                """SELECT r.chunk_id, r.worker_id FROM replicas r
                   JOIN chunks ch ON ch.chunk_id = r.chunk_id WHERE ch.object_id = ?""",
                (object_id,),
            )
        ]

    def incomplete_objects(self, cutoff: float) -> list[str]:
        rows = self._all(
            "SELECT object_id FROM objects WHERE status IN (?, ?) AND updated_at < ?",
            (OBJECT_PENDING, OBJECT_DELETING, cutoff),
        )
        return [r["object_id"] for r in rows]

    # ------------------------------------------------------------------ chunks & replicas

    def add_chunk(self, chunk_id: str, object_id: str, idx: int, size: int, checksum: str) -> None:
        self._exec(
            "INSERT INTO chunks (chunk_id, object_id, idx, size, checksum) VALUES (?, ?, ?, ?, ?)",
            (chunk_id, object_id, idx, size, checksum),
        )

    def add_replica(self, chunk_id: str, worker_id: str, status: str, now: float) -> bool:
        """Record a copy of a chunk. Returns False if the chunk no longer exists."""
        with self._tx() as c:
            if c.execute("SELECT 1 FROM chunks WHERE chunk_id = ?", (chunk_id,)).fetchone() is None:
                return False
            c.execute(
                """INSERT INTO replicas (chunk_id, worker_id, status, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT (chunk_id, worker_id)
                   DO UPDATE SET status = excluded.status, updated_at = excluded.updated_at""",
                (chunk_id, worker_id, status, now, now),
            )
        return True

    def set_replica_status(self, chunk_id: str, worker_id: str, status: str, now: float,
                           only_if: str | None = None) -> bool:
        sql = "UPDATE replicas SET status = ?, updated_at = ? WHERE chunk_id = ? AND worker_id = ? AND status != ?"
        params: list[Any] = [status, now, chunk_id, worker_id, status]
        if only_if is not None:
            sql += " AND status = ?"
            params.append(only_if)
        return self._exec(sql, params) > 0

    def remove_replica(self, chunk_id: str, worker_id: str) -> bool:
        return self._exec(
            "DELETE FROM replicas WHERE chunk_id = ? AND worker_id = ?", (chunk_id, worker_id)
        ) > 0

    _REPLICA_COLUMNS = """r.chunk_id, r.worker_id, r.status, r.created_at, r.updated_at,
        w.url AS worker_url, COALESCE(w.status, 'UNKNOWN') AS worker_status,
        w.status_changed_at AS worker_status_changed_at"""

    def _attach_replicas(self, chunks: list[dict]) -> list[dict]:
        by_id = {c["chunk_id"]: c for c in chunks}
        for c in chunks:
            c["replicas"] = []
        ids = list(by_id)
        for batch in _chunks(ids):
            marks = ",".join("?" * len(batch))
            for r in self._all(
                f"""SELECT {self._REPLICA_COLUMNS} FROM replicas r
                    LEFT JOIN workers w ON w.worker_id = r.worker_id
                    WHERE r.chunk_id IN ({marks}) ORDER BY r.created_at, r.worker_id""",
                batch,
            ):
                by_id[r["chunk_id"]]["replicas"].append(r)
        return chunks

    def get_object_chunks(self, object_id: str) -> list[dict]:
        chunks = self._all(
            "SELECT * FROM chunks WHERE object_id = ? ORDER BY idx", (object_id,)
        )
        return self._attach_replicas(chunks)

    def get_chunk(self, chunk_id: str) -> dict | None:
        chunk = self._one(
            """SELECT ch.*, o.status AS object_status FROM chunks ch
               JOIN objects o ON o.object_id = ch.object_id WHERE ch.chunk_id = ?""",
            (chunk_id,),
        )
        if chunk is None:
            return None
        return self._attach_replicas([chunk])[0]

    def chunk_info(self, chunk_ids: Iterable[str]) -> dict[str, dict]:
        """Look up chunk checksum/size/object status for many chunk ids."""
        result: dict[str, dict] = {}
        for batch in _chunks(list(chunk_ids)):
            marks = ",".join("?" * len(batch))
            for r in self._all(
                f"""SELECT ch.chunk_id, ch.size, ch.checksum, o.status AS object_status
                    FROM chunks ch JOIN objects o ON o.object_id = ch.object_id
                    WHERE ch.chunk_id IN ({marks})""",
                batch,
            ):
                result[r["chunk_id"]] = r
        return result

    def replicas_on_worker(self, worker_id: str) -> list[dict]:
        return self._all(
            """SELECT r.chunk_id, r.status, r.created_at, r.updated_at,
                      ch.object_id, ch.idx, ch.size, ch.checksum
               FROM replicas r JOIN chunks ch ON ch.chunk_id = r.chunk_id
               WHERE r.worker_id = ? ORDER BY ch.object_id, ch.idx""",
            (worker_id,),
        )

    def replica_load(self) -> dict[str, int]:
        rows = self._all("SELECT worker_id, COUNT(*) AS n FROM replicas GROUP BY worker_id")
        return {r["worker_id"]: r["n"] for r in rows}

    _CHUNK_HEALTH_SQL = """
        SELECT c.chunk_id, c.object_id, c.idx,
            SUM(CASE WHEN r.status = 'healthy' AND w.status = 'UP' THEN 1 ELSE 0 END) AS live_healthy,
            SUM(CASE WHEN r.status = 'corrupt' AND w.status = 'UP' THEN 1 ELSE 0 END) AS live_corrupt,
            SUM(CASE WHEN r.status = 'corrupt' THEN 1 ELSE 0 END) AS corrupt,
            COUNT(r.worker_id) AS total
        FROM chunks c
        JOIN objects o ON o.object_id = c.object_id AND o.status = 'active'
        LEFT JOIN replicas r ON r.chunk_id = c.chunk_id
        LEFT JOIN workers w ON w.worker_id = r.worker_id
        GROUP BY c.chunk_id
    """

    def chunks_needing_attention(self, rf: int) -> list[str]:
        """Chunks that are under/over-replicated or have a corrupt copy on a live worker."""
        rows = self._all(
            f"SELECT chunk_id FROM ({self._CHUNK_HEALTH_SQL}) WHERE live_healthy != ? OR live_corrupt > 0",
            (rf,),
        )
        return [r["chunk_id"] for r in rows]

    # ------------------------------------------------------------------ pending deletes

    def add_pending_deletes(self, pairs: Iterable[tuple[str, str]], now: float) -> None:
        with self._tx() as c:
            c.executemany(
                "INSERT OR IGNORE INTO pending_deletes (chunk_id, worker_id, created_at) VALUES (?, ?, ?)",
                [(chunk_id, worker_id, now) for chunk_id, worker_id in pairs],
            )

    def list_pending_deletes(self, limit: int = 500) -> list[dict]:
        return self._all(
            """SELECT p.*, w.url AS worker_url, w.status AS worker_status FROM pending_deletes p
               LEFT JOIN workers w ON w.worker_id = p.worker_id
               ORDER BY p.created_at LIMIT ?""",
            (limit,),
        )

    def remove_pending_delete(self, chunk_id: str, worker_id: str) -> None:
        self._exec("DELETE FROM pending_deletes WHERE chunk_id = ? AND worker_id = ?", (chunk_id, worker_id))

    def bump_pending_delete(self, chunk_id: str, worker_id: str) -> None:
        self._exec(
            "UPDATE pending_deletes SET attempts = attempts + 1 WHERE chunk_id = ? AND worker_id = ?",
            (chunk_id, worker_id),
        )

    # ------------------------------------------------------------------ stats & events

    def cluster_stats(self, rf: int) -> dict:
        objects = self._one(
            "SELECT COUNT(*) AS n, COALESCE(SUM(size), 0) AS bytes FROM objects WHERE status = ?",
            (OBJECT_ACTIVE,),
        )
        health = self._one(
            f"""SELECT COUNT(*) AS chunks,
                    COALESCE(SUM(total), 0) AS replicas,
                    COALESCE(SUM(CASE WHEN live_healthy >= ? THEN 1 ELSE 0 END), 0) AS fully_replicated,
                    COALESCE(SUM(CASE WHEN live_healthy < ? AND live_healthy > 0 THEN 1 ELSE 0 END), 0) AS under_replicated,
                    COALESCE(SUM(CASE WHEN live_healthy = 0 THEN 1 ELSE 0 END), 0) AS unavailable,
                    COALESCE(SUM(CASE WHEN live_healthy > ? THEN 1 ELSE 0 END), 0) AS over_replicated,
                    COALESCE(SUM(corrupt), 0) AS corrupt_replicas
                FROM ({self._CHUNK_HEALTH_SQL})""",
            (rf, rf, rf),
        )
        pending = self._one("SELECT COUNT(*) AS n FROM pending_deletes")["n"]
        return {
            "objects": objects["n"],
            "stored_bytes": objects["bytes"],
            "pending_deletes": pending,
            **health,
        }

    def add_event(self, level: str, type_: str, message: str, details: dict | None = None,
                  now: float | None = None) -> None:
        with self._lock:
            self._exec(
                "INSERT INTO events (ts, level, type, message, details) VALUES (?, ?, ?, ?, ?)",
                (now or time.time(), level, type_, message, json.dumps(details or {})),
            )
            self._event_inserts += 1
            if self._event_inserts % 500 == 0:
                self._exec(
                    "DELETE FROM events WHERE id <= (SELECT MAX(id) FROM events) - ?", (MAX_EVENTS,)
                )

    def list_events(self, limit: int = 100, after_id: int = 0, level: str | None = None) -> list[dict]:
        sql = "SELECT * FROM events WHERE id > ?"
        params: list[Any] = [after_id]
        if level:
            sql += " AND level = ?"
            params.append(level)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        rows = self._all(sql, params)
        for r in rows:
            r["details"] = json.loads(r["details"] or "{}")
        return rows
