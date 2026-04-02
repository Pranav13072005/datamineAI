from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from app.config import settings


_RESERVED_ATTRS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "message",
    "asctime",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Include request_id if available (record extra or context var filter).
        request_id = getattr(record, "request_id", None)
        if isinstance(request_id, str) and request_id:
            payload["request_id"] = request_id

        # Copy across user-provided `extra` fields.
        for key, value in record.__dict__.items():
            if key in _RESERVED_ATTRS:
                continue
            if key == "request_id":
                continue
            payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str, ensure_ascii=False)


def configure_logging() -> None:
    """Configure app-wide logging with JSON formatting.

    No third-party deps; safe to call multiple times.
    """

    level_name = (settings.LOG_LEVEL or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    root = logging.getLogger()
    root.setLevel(level)

    # Avoid duplicating handlers if called multiple times.
    for handler in root.handlers:
        if isinstance(getattr(handler, "formatter", None), JsonFormatter):
            return

    # Replace existing handlers with JSON handler.
    for handler in list(root.handlers):
        root.removeHandler(handler)

    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(JsonFormatter())

    # Inject request_id from context into all logs.
    try:
        from app.middleware.logging_middleware import RequestIdFilter

        handler.addFilter(RequestIdFilter())
    except Exception:
        # Middleware module not available yet (e.g., early import); continue without filter.
        pass

    root.addHandler(handler)

