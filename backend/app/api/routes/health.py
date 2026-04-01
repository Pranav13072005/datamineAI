from __future__ import annotations

import logging

from fastapi import APIRouter
from sqlalchemy import text

from app.utils.config import settings
from app.utils.database import get_engine


logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/health")
def health_check() -> dict:
    """Health check endpoint.

    Returns API status and (if configured) database connectivity.
    """

    database = {"configured": False}

    database_url = None
    try:
        # settings is accessed inside get_engine; if missing, we treat as unconfigured
        engine = get_engine()
        database_url = str(engine.url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        database = {"configured": True, "ok": True}
    except Exception:
        logger.warning("Health check DB probe failed", exc_info=True)
        if database_url is None:
            database = {"configured": False}
        else:
            database = {"configured": True, "ok": False}

    return {
        "status": "ok",
        "database": database,
        "llm": {
            "provider": "groq",
            "configured": bool(settings.GROQ_API_KEY),
            "model": settings.GROQ_MODEL,
        },
    }
