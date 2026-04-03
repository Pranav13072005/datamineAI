from __future__ import annotations

import datetime
import difflib
import logging
import uuid
from typing import Any

from sqlalchemy import bindparam, text
from sqlalchemy.orm import sessionmaker

from app.models import QueryHistory
from app.utils.database import get_engine

logger = logging.getLogger(__name__)

try:
    from pgvector.sqlalchemy import Vector as _Vector  # type: ignore
except Exception:  # pragma: no cover
    _Vector = None


def _to_iso(dt: Any) -> str | None:
    if dt is None:
        return None
    if isinstance(dt, datetime.datetime):
        try:
            if dt.tzinfo is None:
                return dt.replace(tzinfo=datetime.UTC).isoformat()
            return dt.isoformat()
        except Exception:
            return str(dt)
    return str(dt)


def search_history(question: str, dataset_id: str, top_k: int = 3) -> list[dict]:
    """Semantic search over query history for a dataset.

    Uses pgvector cosine distance operator (<=>) on Postgres.
    Returns only matches with score > 0.75.
    """

    question = (question or "").strip()
    if not question:
        return []

    try:
        dataset_uuid = uuid.UUID(str(dataset_id))
    except Exception:
        return []

    try:
        top_k_int = max(1, int(top_k))
    except Exception:
        top_k_int = 3

    def _fallback_text_similarity() -> list[dict[str, Any]]:
        """Cross-DB fallback when embeddings/pgvector aren't available.

        This keeps the UX working in local/dev SQLite setups and before embeddings
        have been backfilled.
        """

        try:
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=get_engine())
            db = SessionLocal()
        except Exception:
            return []

        try:
            # Pull a small recent window and compute string similarity.
            window = min(200, top_k_int * 25)
            entries = (
                db.query(QueryHistory)
                .filter(QueryHistory.dataset_id == dataset_uuid)
                .order_by(QueryHistory.created_at.desc())
                .limit(window)
                .all()
            )

            q_norm = question.casefold()
            scored: list[dict[str, Any]] = []
            for e in entries:
                qh = str(getattr(e, "question", "") or "").strip()
                ah = str(getattr(e, "answer_summary", "") or "").strip()
                if not qh or not ah:
                    continue
                score = difflib.SequenceMatcher(None, q_norm, qh.casefold()).ratio()
                if score <= 0.75:
                    continue
                scored.append(
                    {
                        "id": str(getattr(e, "id", "") or ""),
                        "question": qh,
                        "answer_summary": ah,
                        "query_type": str(getattr(e, "query_type", "") or ""),
                        "score": float(score),
                        "created_at": _to_iso(getattr(e, "created_at", None)),
                    }
                )

            scored.sort(key=lambda r: float(r.get("score") or 0.0), reverse=True)
            return scored[:top_k_int]
        except Exception:
            logger.exception("history_search_fallback_failed", extra={"dataset_id": str(dataset_id)})
            return []
        finally:
            try:
                db.close()
            except Exception:
                pass

    # Embed the question.
    try:
        from app.services.embedding_service import get_embedding_service

        query_vec = get_embedding_service().embed([question])[0]
    except Exception:
        logger.info("history_search_embedding_unavailable")
        return _fallback_text_similarity()

    engine = get_engine()
    if engine.dialect.name != "postgresql":
        return _fallback_text_similarity()

    if _Vector is None:
        logger.info("history_search_pgvector_unavailable")
        return _fallback_text_similarity()

    stmt = (
        text(
            """
            SELECT
                id::text AS id,
                question,
                answer_summary,
                query_type,
                created_at,
                1 - (question_embedding <=> :query_vec) AS score
            FROM query_history
            WHERE dataset_id = :dataset_id
              AND question_embedding IS NOT NULL
            ORDER BY question_embedding <=> :query_vec
            LIMIT :top_k
            """
        )
        .bindparams(
            bindparam("dataset_id", value=dataset_uuid),
            bindparam("query_vec", value=query_vec, type_=_Vector(384)),
            bindparam("top_k", value=top_k_int),
        )
    )

    try:
        with engine.connect() as conn:
            rows = conn.execute(stmt).mappings().all()
    except Exception:
        logger.exception("history_search_query_failed", extra={"dataset_id": str(dataset_id)})
        return []

    results: list[dict[str, Any]] = []
    for row in rows:
        try:
            score = float(row.get("score") or 0.0)
        except Exception:
            score = 0.0

        if score <= 0.75:
            continue

        results.append(
            {
                "id": str(row.get("id") or ""),
                "question": str(row.get("question") or ""),
                "answer_summary": str(row.get("answer_summary") or ""),
                "query_type": str(row.get("query_type") or ""),
                "score": score,
                "created_at": _to_iso(row.get("created_at")),
            }
        )

    if results:
        return results

    # If we have no vector hits (e.g., embeddings not yet populated), try fallback.
    return _fallback_text_similarity()


def get_dataset_history(dataset_id: str, limit: int = 20) -> list[dict]:
    """Return recent query history for a dataset.

    Intended for the history panel (newest first).
    """

    try:
        dataset_uuid = uuid.UUID(str(dataset_id))
    except Exception:
        return []

    try:
        limit_int = max(1, int(limit))
    except Exception:
        limit_int = 20

    try:
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=get_engine())
        db = SessionLocal()
    except Exception:
        logger.exception("history_session_init_failed")
        return []

    try:
        entries = (
            db.query(QueryHistory)
            .filter(QueryHistory.dataset_id == dataset_uuid)
            .order_by(QueryHistory.created_at.desc())
            .limit(limit_int)
            .all()
        )

        result: list[dict[str, Any]] = []
        for entry in entries:
            result.append(
                {
                    "id": str(entry.id),
                    "question": str(entry.question or ""),
                    "answer_summary": str(getattr(entry, "answer_summary", "") or ""),
                    "response_json": getattr(entry, "response_json", None),
                    "query_type": str(getattr(entry, "query_type", "") or ""),
                    "created_at": _to_iso(getattr(entry, "created_at", None)),
                }
            )
        return result
    except Exception:
        logger.exception("get_dataset_history_failed", extra={"dataset_id": str(dataset_id)})
        return []
    finally:
        try:
            db.close()
        except Exception:
            pass
