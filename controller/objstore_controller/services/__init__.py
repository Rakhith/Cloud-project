"""Business logic of the controller (independent of the HTTP layer)."""

from dataclasses import dataclass

from ..config import Settings
from ..store import MetadataStore
from ..worker_client import WorkerClient
from .cluster import ClusterService
from .objects import ObjectService
from .replication import ReplicationManager


@dataclass
class Services:
    settings: Settings
    store: MetadataStore
    client: WorkerClient
    replication: ReplicationManager
    cluster: ClusterService
    objects: ObjectService
