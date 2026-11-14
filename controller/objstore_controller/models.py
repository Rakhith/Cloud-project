"""Pydantic models for request/response bodies.

These double as the OpenAPI documentation served at ``/docs``.
"""

from __future__ import annotations

from typing import Any, Literal, Optional, Union

from pydantic import BaseModel, Field

MetadataValue = Union[str, int, float, bool]


# --------------------------------------------------------------------- objects


class ReplicaInfo(BaseModel):
    worker_id: str
    status: Literal["healthy", "corrupt"] = Field(description="Health of this copy as known to the controller")
    worker_status: str = Field(description="UP / SYNCING / DOWN / UNKNOWN")
    available: bool = Field(description="True if this copy is healthy and its worker is UP")


class ChunkInfo(BaseModel):
    chunk_id: str
    index: int = Field(description="Position of the chunk inside the object (0-based)")
    size: int
    checksum: str = Field(description="SHA-256 of the chunk data (hex)")
    healthy_replicas: int = Field(description="Copies that are healthy and on an UP worker")
    replicas: list[ReplicaInfo]


class ObjectSummary(BaseModel):
    object_id: str
    name: str
    size: int
    content_type: str
    created_at: str
    updated_at: str
    num_chunks: int


class ObjectMetadata(ObjectSummary):
    checksum: str = Field(description="SHA-256 of the full object (hex)")
    checksum_algorithm: str = "sha256"
    chunk_size: int
    replication_factor: int
    replication_status: Literal["healthy", "degraded", "unavailable"] = Field(
        description="healthy: every chunk has the required number of available copies; "
        "degraded: some chunk has fewer; unavailable: some chunk has no available copy"
    )
    metadata_version: int = Field(description="Incremented on every metadata update (use with If-Match)")
    custom_metadata: dict[str, Any]
    chunks: Optional[list[ChunkInfo]] = None


class ObjectList(BaseModel):
    objects: list[ObjectSummary]
    total: int
    limit: int
    offset: int
    next_offset: Optional[int] = Field(None, description="Offset of the next page, null on the last page")


# --------------------------------------------------------------------- cluster


class WorkerInfo(BaseModel):
    worker_id: str
    url: str
    status: str
    last_heartbeat: str
    seconds_since_heartbeat: float
    status_changed_at: str
    registered_at: str
    chunk_count: int = Field(description="Chunk files reported by the worker")
    replicas_recorded: int = Field(description="Copies the controller believes are on this worker")
    used_bytes: int
    free_bytes: Optional[int] = None
    capacity_bytes: Optional[int] = None


class ClusterStatus(BaseModel):
    replication_factor: int
    chunk_size: int
    workers_total: int
    workers_up: int
    workers_syncing: int
    workers_down: int
    writable: bool = Field(description="True if enough workers are UP to accept uploads")
    objects: int
    stored_bytes: int
    chunks: int
    replicas: int
    fully_replicated: int
    under_replicated: int
    unavailable: int
    over_replicated: int
    corrupt_replicas: int
    pending_deletes: int
    replication_healthy: bool
    workers: list[WorkerInfo]


class Event(BaseModel):
    id: int
    time: str
    level: str
    type: str
    message: str
    details: dict[str, Any]


# --------------------------------------------------------------------- internal


class WorkerStats(BaseModel):
    chunk_count: int = 0
    used_bytes: int = 0
    free_bytes: Optional[int] = None
    capacity_bytes: Optional[int] = None


class RegisterRequest(WorkerStats):
    worker_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9._-]+$")
    url: str = Field(min_length=1, description="Base URL the controller should use to reach the worker")


class HeartbeatRequest(WorkerStats):
    worker_id: str
    corrupt_chunks: list[str] = Field(default_factory=list, description="Chunks the worker found corrupted")


class RegisterResponse(BaseModel):
    worker_id: str
    status: str
    heartbeat_interval: float
    replication_factor: int
    chunk_size: int


class HeartbeatResponse(BaseModel):
    worker_id: str
    status: str
