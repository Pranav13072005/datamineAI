from __future__ import annotations

import uuid


def test_get_dataset_history_returns_recent(async_client):
    # Ensure DB tables exist via lifespan (async_client fixture).
    from sqlalchemy.orm import sessionmaker

    from app.models import Dataset, QueryHistory
    from app.services.history_service import get_dataset_history
    from app.utils.database import get_engine

    engine = get_engine()
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    dataset_id = uuid.uuid4()

    with SessionLocal() as db:
        db.add(
            Dataset(
                id=dataset_id,
                name="test",
                file_path="/tmp/test.csv",
                row_count=1,
                col_count=1,
            )
        )
        db.add(
            QueryHistory(
                dataset_id=dataset_id,
                question="What is the mean?",
                question_embedding=None,
                answer_summary="Mean was 10.",
                response_json={"answer": "Mean was 10."},
                query_type="analytical",
            )
        )
        db.commit()

    rows = get_dataset_history(str(dataset_id), limit=20)
    assert len(rows) == 1
    assert rows[0]["question"] == "What is the mean?"
    assert rows[0]["answer_summary"] == "Mean was 10."


def test_search_history_safe_on_sqlite(async_client):
    # On SQLite/non-Postgres, semantic search should just return empty.
    from app.services.history_service import search_history

    results = search_history("test question", str(uuid.uuid4()), top_k=3)
    assert results == []


def test_search_history_fallback_text_similarity_on_sqlite(async_client):
    from sqlalchemy.orm import sessionmaker

    from app.models import Dataset, QueryHistory
    from app.services.history_service import search_history
    from app.utils.database import get_engine

    engine = get_engine()
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    dataset_id = uuid.uuid4()
    with SessionLocal() as db:
        db.add(
            Dataset(
                id=dataset_id,
                name="test",
                file_path="/tmp/test.csv",
                row_count=1,
                col_count=1,
            )
        )
        db.add(
            QueryHistory(
                dataset_id=dataset_id,
                question="Approval rate by Credit_Score bucket",
                question_embedding=None,
                answer_summary="Approval rate varied by credit score bucket.",
                response_json={"answer": "ok"},
                query_type="analytical",
            )
        )
        db.commit()

    hits = search_history("Approval rate by Credit_Score bucket", str(dataset_id), top_k=3)
    assert isinstance(hits, list)
    assert len(hits) >= 1
    assert hits[0]["question"] == "Approval rate by Credit_Score bucket"
    assert float(hits[0]["score"]) >= 0.99
