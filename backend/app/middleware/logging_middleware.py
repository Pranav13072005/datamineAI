from __future__ import annotations

import contextvars
import logging
import time
import uuid
from typing import Any

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response


_request_id_ctx: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "request_id",
    default=None,
)


def get_request_id() -> str | None:
    return _request_id_ctx.get()


class RequestIdFilter(logging.Filter):
    """Inject request_id from context into log records."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        if not getattr(record, "request_id", None):
            rid = get_request_id()
            if rid:
                setattr(record, "request_id", rid)
        return True


class LoggingMiddleware(BaseHTTPMiddleware):
    """Request logging middleware.

    - Generates a UUID request_id for every request.
    - Stores it in a context var so downstream logs can include it.
    - Adds X-Request-ID header.
    - Logs a JSON payload for every response.
    """

    def __init__(self, app: Any) -> None:
        super().__init__(app)
        self._logger = logging.getLogger("app.request")

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = str(uuid.uuid4())
        token = _request_id_ctx.set(request_id)
        start = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
        except Exception as exc:
            duration_ms = int((time.perf_counter() - start) * 1000)
            # Log even on exception, then re-raise so FastAPI can render the error response.
            self._logger.info(
                "request",
                extra={
                    "request_id": request_id,
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": status_code,
                    "duration_ms": duration_ms,
                    "error": str(exc),
                },
            )
            raise
        finally:
            _request_id_ctx.reset(token)

        response.headers["X-Request-ID"] = request_id
        duration_ms = int((time.perf_counter() - start) * 1000)

        self._logger.info(
            "request",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": status_code,
                "duration_ms": duration_ms,
            },
        )
        return response
