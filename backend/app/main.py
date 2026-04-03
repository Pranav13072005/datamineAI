"""FastAPI application entry point.

Bootstraps:
- FastAPI app + CORS
- Logging middleware
- Database connectivity check (dev-friendly)
- Routers: /health, /datasets, /query, /export/pdf
"""

from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.engine import make_url

from app.config import settings
from app.middleware.logging_middleware import LoggingMiddleware
from app.routers.datasets import router as datasets_router
from app.routers.export import router as export_router
from app.routers.health import router as health_router
from app.routers.query import router as query_router
from app.utils.database import Base, enable_sqlite_fallback, get_engine
from app.utils.logging import configure_logging


logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    configure_logging()

    logger.info("Starting API", extra={"environment": settings.ENVIRONMENT})

    def _db_target_for_logs() -> str:
        try:
            u = make_url(settings.SQLALCHEMY_DATABASE_URI)
            safe = u.set(password="***")
            return str(safe)
        except Exception:
            return "<unparseable DATABASE_URL>"

    # Verify live DB connection early.
    # - In production: fail fast if DB is misconfigured/unreachable.
    # - In development: fall back to SQLite only when Postgres is unreachable.
    #   If Postgres is reachable but schema is incompatible, fail with a clear message
    #   so pgvector/migrations don't silently appear "broken".
    if settings.SQLALCHEMY_DATABASE_URI:
        try:
            engine = get_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))

                if engine.dialect.name == "postgresql":
                    # If Postgres is reachable but tables aren't there yet, fail fast.
                    # This avoids confusing 500s like "relation datasets does not exist".
                    datasets_exists = conn.execute(
                        text("select to_regclass('public.datasets')")
                    ).scalar()
                    if datasets_exists is None:
                        raise RuntimeError(
                            "Postgres schema is not initialized (public.datasets is missing). "
                            "Run `alembic upgrade head` against this DATABASE_URL, then restart the API."
                        )

                    # Detect the common "old schema" situation early: datasets.id is TEXT/VARCHAR
                    # but the current app/migrations expect UUID.
                    row = conn.execute(
                        text(
                            """
                            SELECT data_type, udt_name
                            FROM information_schema.columns
                            WHERE table_name = 'datasets' AND column_name = 'id'
                            LIMIT 1
                            """
                        )
                    ).fetchone()
                    if row is not None:
                        data_type, udt_name = row
                        if str(udt_name).lower() != "uuid":
                            raise RuntimeError(
                                "Postgres schema mismatch: datasets.id is not UUID "
                                f"(data_type={data_type}, udt_name={udt_name}). "
                                "This project now expects UUID ids. "
                                "Fix by running Alembic on a clean schema (or dropping old tables) and then "
                                "`alembic upgrade head`."
                            )
            logger.info(
                "Database connection OK",
                extra={"db_target": _db_target_for_logs(), "dialect": engine.dialect.name},
            )

            # SQLite dev mode: create tables automatically.
            # Postgres: use Alembic migrations (do NOT create_all; it doesn't migrate).
            if engine.dialect.name == "sqlite":
                Base.metadata.create_all(bind=engine)
        except Exception as exc:
            # If Postgres credentials are wrong, do NOT fall back to SQLite.
            # Falling back hides the real issue and makes features like pgvector appear "broken".
            if "password authentication failed" in str(exc).lower():
                logger.error(
                    "Postgres authentication failed. Fix DATABASE_URL/DB_URL/URL_SUPABASE (wrong password/user).",
                    extra={"db_target": _db_target_for_logs()},
                )
                raise

            if isinstance(exc, RuntimeError) and "Postgres schema mismatch" in str(exc):
                logger.error(str(exc))
                raise

            if settings.ENVIRONMENT.lower() == "production":
                logger.exception("Database connection failed during startup")
                raise
            logger.warning(
                "Database connection failed during startup; falling back to SQLite for local dev. error=%s",
                exc,
            )

            enable_sqlite_fallback()
            engine = get_engine()
            # NOTE: create_all does not migrate existing schemas.
            # If a previous local SQLite DB exists with an older schema,
            # queries may fail (e.g. missing columns). In dev fallback mode
            # we prefer a working API over preserving old local data.
            if engine.dialect.name == "sqlite":
                logger.info("Recreating SQLite schema for dev fallback")
                Base.metadata.drop_all(bind=engine)
            Base.metadata.create_all(bind=engine)
    else:
        logger.info("Database not configured; skipping startup DB probe")

    yield

    logger.info("Shutting down API")


app = FastAPI(
    title=settings.PROJECT_NAME,
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_origin_regex=settings.CORS_ORIGIN_REGEX,
    allow_credentials=settings.CORS_ALLOW_CREDENTIALS,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Logs JSON on every response and adds X-Request-ID.
app.add_middleware(LoggingMiddleware)

app.include_router(health_router)
app.include_router(datasets_router)
app.include_router(query_router)
app.include_router(export_router)