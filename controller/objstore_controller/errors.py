"""Domain errors raised by controller services.

Each error carries the HTTP status it maps to; ``main.py`` installs an
exception handler that renders every error as a uniform JSON body::

    {"error": {"status": 404, "code": "not_found", "message": "...", "details": {...}}}
"""

from __future__ import annotations

from typing import Any


class ObjectStoreError(Exception):
    status_code = 500
    code = "internal_error"

    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class InvalidRequest(ObjectStoreError):
    status_code = 400
    code = "invalid_request"


class Unauthorized(ObjectStoreError):
    status_code = 401
    code = "unauthorized"


class ObjectNotFound(ObjectStoreError):
    status_code = 404
    code = "not_found"


class Conflict(ObjectStoreError):
    status_code = 409
    code = "conflict"


class PayloadTooLarge(ObjectStoreError):
    status_code = 413
    code = "payload_too_large"


class DataIntegrityError(ObjectStoreError):
    """No uncorrupted copy of some chunk is available."""

    status_code = 500
    code = "data_integrity_error"


class ServiceUnavailable(ObjectStoreError):
    """Not enough healthy workers to satisfy the request."""

    status_code = 503
    code = "service_unavailable"


def error_body(status: int, code: str, message: str, details: dict[str, Any] | None = None) -> dict:
    body: dict[str, Any] = {"status": status, "code": code, "message": message}
    if details:
        body["details"] = details
    return {"error": body}
