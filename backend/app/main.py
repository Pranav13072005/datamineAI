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

    # Verify live DB connection early.
    # - In production: fail fast if DB is misconfigured/unreachable.
    # - In development: log a warning and continue (so you can still hit /docs, etc.).
    if settings.SQLALCHEMY_DATABASE_URI:
        try:
            engine = get_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection OK")

            # Ensure tables exist (dev-friendly). In production, consider migrations.
            Base.metadata.create_all(bind=engine)
        except Exception as exc:
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