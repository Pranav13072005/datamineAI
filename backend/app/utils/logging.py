from __future__ import annotations

import logging

from app.utils.config import settings


def configure_logging() -> None:
    """Configure app-wide logging.

    Keep this intentionally minimal (stdlib logging) and safe to call multiple times.
    """

    level_name = (settings.LOG_LEVEL or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
