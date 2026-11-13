"""Worker configuration (environment variables)."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass


@dataclass(frozen=True)
class WorkerSettings:
    # Unique id of this worker (e.g. worker1).
    worker_id: str
    # URL the controller uses to reach this worker.
    advertise_url: str
    # Base URL of the controller.
    controller_url: str = "http://controller:8000"
    # Persistent directory holding the chunk files.
    data_dir: str = "/data"
    # Seconds between heartbeats.
    heartbeat_interval: float = 3.0
    # Seconds between background integrity scans (0 disables).
    scrub_interval: float = 60.0
    # Largest chunk accepted by PUT.
    max_chunk_bytes: int = 16 * 1024 * 1024
    # Optional shared secret for /internal endpoints.
    internal_token: str | None = None
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "WorkerSettings":
        worker_id = os.environ.get("WORKER_ID") or socket.gethostname()
        port = os.environ.get("PORT", "8000")
        return cls(
            worker_id=worker_id,
            advertise_url=os.environ.get("WORKER_URL") or f"http://{socket.gethostname()}:{port}",
            controller_url=os.environ.get("CONTROLLER_URL", cls.controller_url).rstrip("/"),
            data_dir=os.environ.get("DATA_DIR", cls.data_dir),
            heartbeat_interval=float(os.environ.get("HEARTBEAT_INTERVAL", cls.heartbeat_interval)),
            scrub_interval=float(os.environ.get("SCRUB_INTERVAL", cls.scrub_interval)),
            max_chunk_bytes=int(os.environ.get("MAX_CHUNK_BYTES", cls.max_chunk_bytes)),
            internal_token=os.environ.get("INTERNAL_TOKEN") or None,
            log_level=os.environ.get("LOG_LEVEL", cls.log_level),
        )
