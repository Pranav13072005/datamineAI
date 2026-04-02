from __future__ import annotations

import asyncio
from logging.config import fileConfig
import os
from typing import Any

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import async_engine_from_config

# Alembic Config object (reads values from alembic.ini if present)
config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)


def _get_database_url() -> str:
    # Prefer env var for CLI usage, falling back to backend/.env via app.config.
    url = os.getenv("DATABASE_URL") or os.getenv("DB_URL") or os.getenv("URL_SUPABASE")
    if url:
        return url

    from app.config import settings

    return settings.SQLALCHEMY_DATABASE_URI or "sqlite:///./local.db"


def _as_async_url(url: str) -> str:
    """Coerce SQLAlchemy URL into an async-driver URL when possible.

    Alembic can run in async mode even if the application uses sync SQLAlchemy.
    For Postgres we prefer asyncpg.
    """

    try:
        parsed = make_url(url)
    except Exception:
        return url

    drivername = parsed.drivername

    # Normalize postgres scheme aliases
    if drivername == "postgres":
        drivername = "postgresql"
        parsed = parsed.set(drivername=drivername)

    if drivername.startswith("postgresql"):
        if "+asyncpg" in drivername:
            return str(parsed)
        return str(parsed.set(drivername="postgresql+asyncpg"))

    return str(parsed)


# Import models so metadata is populated
from app.utils.database import Base  # noqa: E402
from app.models import Dataset, QueryHistory  # noqa: F401,E402

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    url = _as_async_url(_get_database_url())
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()

def _do_run_migrations(connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    configuration: dict[str, Any] = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = _as_async_url(_get_database_url())

    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        future=True,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(_do_run_migrations)

    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
