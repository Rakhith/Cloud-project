"""Small helpers shared by the controller modules."""

from __future__ import annotations

import json
import mimetypes
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

from .errors import InvalidRequest

MAX_METADATA_KEYS = 100
MAX_METADATA_KEY_LENGTH = 128
MAX_METADATA_VALUE_LENGTH = 2048
MAX_NAME_LENGTH = 1024


def iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def validate_custom_metadata(data: Any, allow_null: bool = False) -> dict[str, Any]:
    """Validate user-defined metadata: a flat JSON object of scalar values.

    ``allow_null`` is used for PATCH, where ``null`` means "remove this key".
    """
    if not isinstance(data, dict):
        raise InvalidRequest("custom metadata must be a JSON object of key/value pairs")
    if len(data) > MAX_METADATA_KEYS:
        raise InvalidRequest(f"at most {MAX_METADATA_KEYS} metadata keys are allowed")
    for key, value in data.items():
        if not isinstance(key, str) or not key.strip():
            raise InvalidRequest("metadata keys must be non-empty strings")
        if len(key) > MAX_METADATA_KEY_LENGTH:
            raise InvalidRequest(f"metadata key '{key[:32]}...' exceeds {MAX_METADATA_KEY_LENGTH} characters")
        if value is None:
            if not allow_null:
                raise InvalidRequest(f"metadata value for '{key}' must not be null")
            continue
        if not isinstance(value, (str, int, float, bool)):
            raise InvalidRequest(
                f"metadata value for '{key}' must be a string, number or boolean (nested values are not supported)"
            )
        if isinstance(value, str) and len(value) > MAX_METADATA_VALUE_LENGTH:
            raise InvalidRequest(f"metadata value for '{key}' exceeds {MAX_METADATA_VALUE_LENGTH} characters")
    return data


def parse_metadata_field(raw: str | None) -> dict[str, Any]:
    """Parse the optional ``metadata`` multipart field of an upload."""
    if raw is None or raw.strip() == "":
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise InvalidRequest(f"'metadata' field is not valid JSON: {exc.msg}") from exc
    return validate_custom_metadata(data)


def resolve_content_type(declared: str | None, filename: str) -> str:
    """Prefer an explicit content type; fall back to guessing from the name."""
    if declared and declared.lower() != "application/octet-stream":
        return declared
    guessed, _ = mimetypes.guess_type(filename)
    return guessed or declared or "application/octet-stream"


def content_disposition(filename: str, disposition: str = "attachment") -> str:
    ascii_name = filename.encode("ascii", "replace").decode("ascii").replace('"', "'").replace("?", "_")
    return f"{disposition}; filename=\"{ascii_name}\"; filename*=UTF-8''{quote(filename, safe='')}"
