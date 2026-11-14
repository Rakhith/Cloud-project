"""Controller configuration.

All settings can be overridden through environment variables (see
``Settings.from_env``). Defaults match the project specification:
1 MB chunks and a replication factor of 2.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_str(name: str, default: str | None) -> str | None:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    return int(value) if value not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    return float(value) if value not in (None, "") else default


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value in (None, ""):
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Settings:
    # Persistent directory holding the SQLite metadata database.
    data_dir: str = "/data"
    # Size of every chunk except (possibly) the last one of an object.
    chunk_size: int = 1024 * 1024
    # Number of copies of every chunk, each on a different worker.
    replication_factor: int = 2
    # A worker that has not sent a heartbeat for this long is marked DOWN.
    heartbeat_timeout: float = 10.0
    # How often the failure detector checks heartbeats.
    monitor_interval: float = 2.0
    # How often the maintenance loop (repair / trim / GC) runs when idle.
    maintenance_interval: float = 5.0
    # How long to wait after a worker goes DOWN before re-replicating its
    # chunks elsewhere. Avoids needless copying on short restarts.
    repair_grace_period: float = 20.0
    # Max number of chunks repaired concurrently.
    repair_concurrency: int = 4
    # Re-hash every chunk on a worker when it (re)joins the cluster.
    reconcile_verify: bool = True
    # Timeout for each controller -> worker HTTP request.
    worker_request_timeout: float = 15.0
    # Number of chunks fetched ahead of the client during downloads.
    read_ahead_chunks: int = 2
    # Upper bound for a single object (0 disables the limit).
    max_object_size: int = 10 * 1024 ** 3
    # Incomplete uploads older than this are garbage collected.
    incomplete_upload_ttl: float = 3600.0
    # Optional shared secret required on all /internal endpoints.
    internal_token: str | None = None
    log_level: str = "INFO"

    @property
    def db_path(self) -> str:
        return str(Path(self.data_dir) / "metadata.db")

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            data_dir=_env_str("DATA_DIR", cls.data_dir),
            chunk_size=_env_int("CHUNK_SIZE", cls.chunk_size),
            replication_factor=_env_int("REPLICATION_FACTOR", cls.replication_factor),
            heartbeat_timeout=_env_float("HEARTBEAT_TIMEOUT", cls.heartbeat_timeout),
            monitor_interval=_env_float("MONITOR_INTERVAL", cls.monitor_interval),
            maintenance_interval=_env_float("MAINTENANCE_INTERVAL", cls.maintenance_interval),
            repair_grace_period=_env_float("REPAIR_GRACE_PERIOD", cls.repair_grace_period),
            repair_concurrency=_env_int("REPAIR_CONCURRENCY", cls.repair_concurrency),
            reconcile_verify=_env_bool("RECONCILE_VERIFY", cls.reconcile_verify),
            worker_request_timeout=_env_float("WORKER_REQUEST_TIMEOUT", cls.worker_request_timeout),
            read_ahead_chunks=_env_int("READ_AHEAD_CHUNKS", cls.read_ahead_chunks),
            max_object_size=_env_int("MAX_OBJECT_SIZE", cls.max_object_size),
            incomplete_upload_ttl=_env_float("INCOMPLETE_UPLOAD_TTL", cls.incomplete_upload_ttl),
            internal_token=_env_str("INTERNAL_TOKEN", None),
            log_level=_env_str("LOG_LEVEL", cls.log_level),
        )
