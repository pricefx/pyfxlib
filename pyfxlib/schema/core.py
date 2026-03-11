"""Core Pricefx domain objects: backend, users, notifications, jobs."""

from enum import Enum, unique


@unique
class JobStatus(Enum):
    """Possible status of a job for the Pricefx REST API."""

    PROCESSING = "PROCESSING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"
