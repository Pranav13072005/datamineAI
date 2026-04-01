from __future__ import annotations

from functools import lru_cache
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.utils.config import settings


@lru_cache
def get_engine() -> Engine:
    database_url = settings.SQLALCHEMY_DATABASE_URI
    if not database_url:
        raise RuntimeError(
            "Database is not configured. Set DATABASE_URL (preferred) or URL_SUPABASE in your environment/.env"
        )

    return create_engine(
        database_url,
        pool_pre_ping=True,
        echo=False,
    )


@lru_cache
def _get_sessionmaker() -> sessionmaker[Session]:
    return sessionmaker(autocommit=False, autoflush=False, bind=get_engine())


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency that yields a DB session per request."""

    SessionLocal = _get_sessionmaker()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
