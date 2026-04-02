"""
database.py — SQLAlchemy engine, session factory, and ORM Base.

All models import Base from here. All routes/services get a DB session
via the get_db() dependency.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Generator

from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

from app.config import settings

# Declarative base that all ORM models extend
Base = declarative_base()


# When True, forces the app to use local SQLite regardless of configured DB URL.
# This is a dev-safety valve for cases like broken DNS / network restrictions.
_force_sqlite_fallback: bool = False


def enable_sqlite_fallback() -> None:
    """Force SQLite fallback for the current process.

    Clears cached engine/sessionmaker so subsequent calls pick up the fallback.
    """

    global _force_sqlite_fallback
    _force_sqlite_fallback = True

    # Clear caches so we rebuild engine+sessionmaker using SQLite.
    get_engine.cache_clear()
    _get_sessionmaker.cache_clear()


@lru_cache
def get_engine() -> Engine:
    database_url = settings.SQLALCHEMY_DATABASE_URI

    if _force_sqlite_fallback:
        database_url = "sqlite:///./local.db"
    elif not database_url:
        if settings.ENVIRONMENT.lower() == "production":
            raise RuntimeError(
                "Database is not configured. Set DATABASE_URL (preferred) or URL_SUPABASE in your backend/.env"
            )
        database_url = "sqlite:///./local.db"

    engine_kwargs: dict = {"echo": False}

    # SQLite needs this for FastAPI's threaded request model.
    # For in-memory SQLite we also need StaticPool so all sessions share the same DB.
    if database_url.startswith("sqlite"):
        engine_kwargs["connect_args"] = {"check_same_thread": False}

        if ":memory:" in database_url or "mode=memory" in database_url:
            engine_kwargs["poolclass"] = StaticPool
    else:
        engine_kwargs["pool_pre_ping"] = True

    return create_engine(database_url, **engine_kwargs)


@lru_cache
def _get_sessionmaker() -> sessionmaker[Session]:
    return sessionmaker(autocommit=False, autoflush=False, bind=get_engine())


def get_db() -> Generator[Session, None, None]:
    """
    FastAPI dependency that yields a database session per request,
    then closes it afterwards. Use with Depends(get_db).
    """

    try:
        SessionLocal = _get_sessionmaker()
    except RuntimeError as exc:
        # Use 503 rather than a generic 500 when the DB isn't configured.
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
