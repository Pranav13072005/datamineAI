from __future__ import annotations

from pathlib import Path
import sys

import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy import engine_from_config
from sqlalchemy import pool
from sqlalchemy.engine.url import make_url

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.config import settings  # noqa: E402


def _safe_url(url: str) -> str:
    try:
        u = sa.engine.make_url(url)
        return str(u.set(password="***"))
    except Exception:
        return "<unparseable url>"


def main() -> int:
    url = settings.SQLALCHEMY_DATABASE_URI
    print("db_target=", _safe_url(url))

    try:
        parsed0 = make_url(url)
        print("parsed_user=", parsed0.username)
        print("parsed_has_password=", parsed0.password is not None)
        if parsed0.password is not None:
            print("parsed_password_len=", len(parsed0.password))
    except Exception:
        pass

    # Demonstrate the SQLAlchemy 2.x footgun: `str(URL)` hides the password as "***".
    # Never use the result for real connections.
    masked_url = url
    try:
        masked_url = str(make_url(url))
        if masked_url != url:
            parsed_masked = make_url(masked_url)
            print("masked_url_has_password=", parsed_masked.password is not None)
            if parsed_masked.password is not None:
                print("masked_url_password_len=", len(parsed_masked.password))
    except Exception:
        pass

    # A safe equivalent to Alembic's fixed normalization.
    connect_url = url
    try:
        connect_url = make_url(url).render_as_string(hide_password=False)
    except Exception:
        connect_url = url

    if not url:
        print("ERROR: settings.SQLALCHEMY_DATABASE_URI is empty")
        return 2

    try:
        engine = sa.create_engine(connect_url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("select 1"))
            dialect = engine.dialect.name
            print("dialect=", dialect)
            if dialect == "postgresql":
                print("current_database=", conn.execute(text("select current_database()")).scalar())
                print("current_schema=", conn.execute(text("select current_schema()")).scalar())
                print(
                    "datasets=",
                    conn.execute(text("select to_regclass('public.datasets')")).scalar(),
                )
                print(
                    "alembic_version=",
                    conn.execute(text("select to_regclass('public.alembic_version')")).scalar(),
                )

                try:
                    print(
                        "pgvector_extension_installed=",
                        conn.execute(text("select count(*) from pg_extension where extname='vector'"))
                        .scalar(),
                    )
                except Exception as exc:
                    print("pgvector_extension_installed_failed=", type(exc).__name__, str(exc))

                try:
                    print(
                        "column_registry=",
                        conn.execute(text("select to_regclass('public.column_registry')")).scalar(),
                    )
                    print(
                        "column_registry_rows=",
                        conn.execute(text("select count(*) from column_registry")).scalar(),
                    )
                    print(
                        "column_registry_rows_with_embedding=",
                        conn.execute(text("select count(*) from column_registry where embedding is not null")).scalar(),
                    )
                except Exception as exc:
                    print("column_registry_failed=", type(exc).__name__, str(exc))

        # Replicate Alembic's engine creation path.
        cfg = {"sqlalchemy.url": connect_url}
        engine2 = engine_from_config(cfg, prefix="sqlalchemy.", poolclass=pool.NullPool, future=True)
        with engine2.connect() as conn2:
            conn2.execute(text("select 1"))
        print("engine_from_config= OK")
    except Exception as exc:
        print("ERROR:", repr(exc))
        return 1

    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
