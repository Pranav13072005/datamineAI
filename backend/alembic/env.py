from __future__ import annotations

import asyncio
from logging.config import fileConfig
import os
from typing import Any

from alembic import context
from sqlalchemy import engine_from_config, pool
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

    url = settings.SQLALCHEMY_DATABASE_URI
    if url:
        return url

    # Important: do NOT silently fall back to SQLite for Alembic.
    # This can trick you into thinking migrations ran on Postgres when they
    # actually created a local SQLite file.
    if os.getenv("ALEMBIC_ALLOW_SQLITE") == "1":
        return "sqlite:///./local.db"

    raise RuntimeError(
        "DATABASE_URL is not set (and Settings.SQLALCHEMY_DATABASE_URI is empty). "
        "Set DATABASE_URL (or DB_URL/URL_SUPABASE) to your Postgres connection string before running Alembic."
    )


def _normalize_url(url: str) -> str:
    """Normalize URL without forcing an async driver.

    On Windows, asyncpg connectivity can be less reliable for some hosted
    Postgres setups. We therefore only use async migrations when the URL
    explicitly selects an async driver (e.g. postgresql+asyncpg://...).
    """

    try:
        parsed = make_url(url)
    except Exception:
        return url

    # Normalize postgres scheme aliases
    if parsed.drivername == "postgres":
        parsed = parsed.set(drivername="postgresql")

    # IMPORTANT: In SQLAlchemy 2.x, `str(URL)` renders with the password hidden
    # (replaced by "***"). Using that string for actual connections breaks
    # authentication. Always render with hide_password=False for real use.
    return parsed.render_as_string(hide_password=False)


def _safe_url_for_logs(url: str) -> str:
    """Return a version of the DB URL safe to print (no password)."""

    try:
        parsed = make_url(url)
        return str(parsed.set(password="***"))
    except Exception:
        return "<unparseable url>"


def _is_async_driver(url: str) -> bool:
    try:
        drivername = make_url(url).drivername
    except Exception:
        return False

    return drivername.endswith("+asyncpg") or drivername.endswith("+aiosqlite")


# Import models so metadata is populated
from app.utils.database import Base  # noqa: E402
from app.models import Dataset, QueryHistory  # noqa: F401,E402

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    url = _normalize_url(_get_database_url())
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
    url = _normalize_url(_get_database_url())
    configuration["sqlalchemy.url"] = url

    if os.getenv("ALEMBIC_SHOW_URL") == "1":
        print(f"[alembic] sqlalchemy.url={_safe_url_for_logs(url)}")

    # Prefer sync migrations unless an async driver is explicitly selected.
    if not _is_async_driver(url):
        connectable = engine_from_config(
            configuration,
            prefix="sqlalchemy.",
            poolclass=pool.NullPool,
            future=True,
        )

        with connectable.connect() as connection:
            _do_run_migrations(connection)
        return

    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        future=True,
        # Hosted Postgres commonly requires TLS; asyncpg expects an explicit ssl flag.
        connect_args={"ssl": True} if url.startswith("postgresql+asyncpg") else {},
    )

    async with connectable.connect() as connection:
        await connection.run_sync(_do_run_migrations)

    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
