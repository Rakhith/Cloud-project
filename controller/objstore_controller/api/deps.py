"""FastAPI dependencies."""

from fastapi import Header, Request

from ..errors import Unauthorized
from ..services import Services


def get_services(request: Request) -> Services:
    return request.app.state.services


def require_internal_token(request: Request, x_internal_token: str | None = Header(None)) -> None:
    """If INTERNAL_TOKEN is configured, every /internal call must present it."""
    expected = request.app.state.services.settings.internal_token
    if expected and x_internal_token != expected:
        raise Unauthorized("missing or invalid X-Internal-Token")
