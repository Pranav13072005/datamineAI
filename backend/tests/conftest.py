from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from typing import AsyncGenerator

import httpx
import pytest
import pytest_asyncio
from fastapi.testclient import TestClient


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


def _configure_test_environment() -> None:
    """Force a fast, isolated DB for tests and clear cached settings/engine."""

    os.environ.setdefault("ENVIRONMENT", "test")
    # Requirement: override DB_URL env var
    os.environ["DB_URL"] = "sqlite+pysqlite:///:memory:"
    # Ensure no other DB setting overrides DB_URL (Settings prefers DATABASE_URL).
    os.environ["DATABASE_URL"] = ""
    os.environ["URL_SUPABASE"] = ""

    # Refresh Settings so it sees env overrides
    import app.config as config

    config.get_settings.cache_clear()
    config.settings = config.get_settings()

    # Refresh engine/session caches so they see the new DB URL
    import app.utils.database as database

    database.get_engine.cache_clear()
    database._get_sessionmaker.cache_clear()


@pytest.fixture()
def client() -> TestClient:
    _configure_test_environment()

    from app.main import app

    return TestClient(app)


@pytest_asyncio.fixture()
async def async_client() -> AsyncGenerator[httpx.AsyncClient, None]:
    _configure_test_environment()

    from app.main import app

    # Run FastAPI lifespan (startup/shutdown) so DB tables exist.
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac


@pytest.fixture()
def sample_csv_file() -> Path:
    # Requirement: create a small 20-row CSV in /tmp.
    # On Windows, "/tmp" typically maps to "C:\\tmp". If not creatable, fall back.
    tmp_root = Path("/tmp")
    try:
        tmp_root.mkdir(parents=True, exist_ok=True)
    except Exception:
        tmp_root = Path(tempfile.gettempdir())

    path = tmp_root / "sample_20_rows.csv"
    rows = ["id,value"]
    for i in range(1, 21):
        rows.append(f"{i},{i * 10}")
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path
