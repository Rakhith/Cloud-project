"""Client REST API: /api/v1/objects (section 5 of the specification)."""

from __future__ import annotations

from email.utils import formatdate
from typing import Any, Optional

from fastapi import APIRouter, Body, Depends, File, Form, Header, Query, Response, UploadFile
from fastapi.responses import StreamingResponse

from ..errors import InvalidRequest
from ..models import ObjectList, ObjectMetadata
from ..services import Services
from ..utils import content_disposition, parse_metadata_field, resolve_content_type
from .deps import get_services

router = APIRouter(prefix="/api/v1/objects", tags=["objects"])

ERRORS = {
    400: {"description": "Invalid request"},
    404: {"description": "Object not found"},
    409: {"description": "Conflict"},
    500: {"description": "Internal error / no uncorrupted copy available"},
    503: {"description": "Not enough storage workers available"},
}


@router.post("", status_code=201, response_model=ObjectMetadata, responses=ERRORS,
             summary="Upload an object")
async def upload_object(
    response: Response,
    file: UploadFile = File(..., description="Binary object data"),
    metadata: Optional[str] = Form(None, description='Custom metadata as a JSON object, e.g. {"owner": "alice"}'),
    name: Optional[str] = Form(None, description="Object name (defaults to the uploaded file name)"),
    content_type: Optional[str] = Form(None, description="Override the content type of the object"),
    svc: Services = Depends(get_services),
):
    """Split the object into 1 MB chunks, store every chunk on two different
    workers and return the new object's metadata (including chunk placement)."""
    custom = parse_metadata_field(metadata)
    object_name = name or file.filename or "unnamed"
    ctype = resolve_content_type(content_type or file.content_type, object_name)
    result = await svc.objects.upload(file, object_name, ctype, custom)
    response.headers["Location"] = f"/api/v1/objects/{result['object_id']}"
    return result


@router.get("", response_model=ObjectList, summary="List stored objects")
async def list_objects(
    limit: int = Query(100, ge=1, le=1000, description="Page size"),
    offset: int = Query(0, ge=0, description="Number of objects to skip"),
    prefix: Optional[str] = Query(None, description="Only objects whose name starts with this prefix"),
    svc: Services = Depends(get_services),
):
    return svc.objects.list_objects(limit, offset, prefix)


@router.get(
    "/{object_id}",
    response_class=StreamingResponse,
    responses={200: {"content": {"application/octet-stream": {}}, "description": "The object data"}, **ERRORS},
    summary="Download an object",
)
async def download_object(
    object_id: str,
    disposition: str = Query("attachment", pattern="^(attachment|inline)$",
                             description="Content-Disposition type (inline lets browsers display media)"),
    svc: Services = Depends(get_services),
):
    """Reassemble the object from its chunks. The client never needs to know
    where chunks live; every chunk is checksum-verified before it is sent."""
    obj, body = await svc.objects.open_download(object_id)
    headers = {
        "Content-Length": str(obj["size"]),
        "Content-Disposition": content_disposition(obj["name"], disposition),
        "ETag": f'"{obj["checksum"]}"',
        "Last-Modified": formatdate(obj["updated_at"], usegmt=True),
        "X-Object-Id": obj["object_id"],
        "X-Checksum-SHA256": obj["checksum"],
    }
    return StreamingResponse(body, media_type=obj["content_type"], headers=headers)


@router.get("/{object_id}/metadata", response_model=ObjectMetadata, responses=ERRORS,
            summary="Get object metadata")
async def get_metadata(
    object_id: str,
    include_chunks: bool = Query(True, description="Include per-chunk placement information"),
    svc: Services = Depends(get_services),
):
    """System metadata (name, size, type, times, checksum, chunks and their
    locations) plus custom metadata, without downloading the data."""
    return svc.objects.get_metadata(object_id, include_chunks)


@router.patch("/{object_id}/metadata", response_model=ObjectMetadata, responses=ERRORS,
              summary="Add / update custom metadata")
async def update_metadata(
    object_id: str,
    updates: dict[str, Any] = Body(..., examples=[{"owner": "alice", "project": "demo", "obsolete-key": None}]),
    replace: bool = Query(False, description="Replace all custom metadata instead of merging"),
    if_match: Optional[str] = Header(None, description="Expected metadata_version; 409 if it changed"),
    svc: Services = Depends(get_services),
):
    """Merge the given keys into the custom metadata (a ``null`` value removes
    a key). The object data is not touched."""
    expected = None
    if if_match is not None:
        raw = if_match.strip().removeprefix("W/").strip('"')
        if not raw.isdigit():
            raise InvalidRequest("If-Match must be the numeric metadata_version")
        expected = int(raw)
    return svc.objects.update_metadata(object_id, updates, replace, expected)


@router.delete("/{object_id}", status_code=204, responses=ERRORS, summary="Delete an object",
               response_class=Response)
async def delete_object(object_id: str, svc: Services = Depends(get_services)):
    """Delete the object and all copies of its chunks. Copies on workers that
    are currently DOWN are removed once those workers return."""
    await svc.objects.delete(object_id)
    return Response(status_code=204)
