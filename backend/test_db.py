"""Manual database connectivity check.

This file is intentionally *not* a unit test. Pytest may still try to collect
it due to its name, so we skip it unless it is run directly.
"""

from __future__ import annotations

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine


def main() -> None:
    load_dotenv()

    database_url = os.getenv("DATABASE_URL") or os.getenv("URL_SUPABASE")
    if not database_url:
        raise SystemExit("Set DATABASE_URL (or URL_SUPABASE) in your environment/.env")

    engine = create_engine(database_url)

    try:
        with engine.connect():
            print("✅ Connected to PostgreSQL!")
    except Exception as e:
        print("❌ Connection failed:", e)
        raise


if __name__ == "__main__":
    main()
else:
    # If pytest tries to collect this module, skip it.
    try:
        import pytest  # type: ignore

        pytest.skip("Manual DB connectivity script", allow_module_level=True)
    except Exception:
        pass