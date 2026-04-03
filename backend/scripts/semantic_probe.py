"""Dev-only smoke test for pgvector-backed semantic search.

Runs a few DB checks via the app's SQLAlchemy engine:
- Confirms dialect is Postgres.
- Confirms `column_registry` exists.
- Prints counts for datasets + embeddings.
- Executes a sample vector search using the same embedding model.

Usage (Windows):
  C:/Users/prana/anaconda3/Scripts/conda.exe run -n dmai python backend/scripts/semantic_probe.py --query "revenue" --top-k 5
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
import os


def _ensure_backend_on_path() -> None:
    backend_dir = Path(__file__).resolve().parents[1]
    if str(backend_dir) not in sys.path:
        sys.path.insert(0, str(backend_dir))

    # Ensure Settings(env_file=".env") resolves to backend/.env
    os.chdir(str(backend_dir))


def main() -> int:
    _ensure_backend_on_path()

    parser = argparse.ArgumentParser(description="Probe pgvector semantic search pipeline")
    parser.add_argument("--query", default="revenue", help="Semantic search query string")
    parser.add_argument("--top-k", type=int, default=5, help="Number of results to return")
    args = parser.parse_args()

    from app.utils.database import get_engine

    engine = get_engine()
    print(f"dialect={engine.dialect.name}")

    if engine.dialect.name != "postgresql":
        print("SKIP: Not running on Postgres, so pgvector search is disabled.")
        return 2

    import sqlalchemy as sa

    with engine.connect() as conn:
        datasets_count = conn.execute(sa.text("select count(*) from datasets")).scalar_one()
        print(f"datasets={datasets_count}")

        # Confirm table exists (safe on Postgres).
        exists = conn.execute(sa.text("select to_regclass('public.column_registry') is not null")).scalar_one()
        print(f"column_registry_exists={bool(exists)}")
        if not exists:
            print("FAIL: column_registry table missing. Run Alembic migrations.")
            return 3

        cr_count = conn.execute(sa.text("select count(*) from column_registry")).scalar_one()
        cr_emb = conn.execute(sa.text("select count(*) from column_registry where embedding is not null")).scalar_one()
        print(f"column_registry={cr_count}")
        print(f"column_registry_with_embedding={cr_emb}")

    if cr_count == 0:
        print("NOTE: column_registry is empty. Upload a dataset and wait for background processing.")
        return 4

    if cr_emb == 0:
        print(
            "NOTE: column_registry rows exist but embeddings are NULL. "
            "That usually means the embedding step failed (model download, missing deps, or exception in background task)."
        )
        return 5

    # Run a real vector search using the same embedding service.
    from app.services.embedding_service import get_embedding_service

    query_vec = get_embedding_service().embed([args.query])[0]

    from pgvector.sqlalchemy import Vector

    stmt = (
        sa.text(
            """
            SELECT
                cr.dataset_id::text AS dataset_id,
                d.name AS dataset_name,
                cr.column_name AS column_name,
                cr.description AS description,
                1 - (cr.embedding <=> :query_vec) AS similarity_score
            FROM column_registry cr
            JOIN datasets d ON d.id = cr.dataset_id
            WHERE cr.embedding IS NOT NULL
            ORDER BY cr.embedding <=> :query_vec
            LIMIT :top_k
            """
        )
        .bindparams(
            sa.bindparam("query_vec", value=query_vec, type_=Vector(384)),
            sa.bindparam("top_k", value=int(args.top_k)),
        )
    )

    with engine.connect() as conn:
        rows = conn.execute(stmt).mappings().all()

    print("\nTop matches:")
    if not rows:
        print("(no results)")
        return 6

    for r in rows:
        score = float(r["similarity_score"]) if r["similarity_score"] is not None else None
        print(
            f"- score={score:.3f} dataset={r['dataset_name']} ({r['dataset_id']}) "
            f"column={r['column_name']} desc={r['description']}"
        )

    print("\nOK: pgvector semantic search is producing results.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
