from __future__ import annotations

import logging
import time
from typing import Any
import uuid
import datetime

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session

from app.config import settings
from app.models import Dataset, QueryHistory
from app.schemas.query import QueryResponse
from app.services.analytical_handler import handle_analytical
from app.services.dataset_service import get_schema, load_dataset
from app.services.descriptive_handler import handle_descriptive
from app.services.correlation_handler import handle_correlation_query
from app.services.ml_handler import handle_ml_query
from app.services.query_classifier import classify_query
from app.services.agent_planner import execute_plan, plan_query, synthesise_results
from app.middleware.logging_middleware import get_request_id
from app.utils.database import get_db, get_engine


router = APIRouter(tags=["Query"])
logger = logging.getLogger(__name__)


class QueryRequest(BaseModel):
    dataset_id: str
    question: str


@router.post("/query", response_model=QueryResponse, summary="Ask a question about a dataset")
async def query_dataset(
    request: QueryRequest,
    background_tasks: BackgroundTasks,
    mode: str | None = None,
    db: Session = Depends(get_db),
):
    start = time.perf_counter()
    request_id = get_request_id()
    question = (request.question or "").strip()
    if not question:
        raise HTTPException(status_code=422, detail="Question cannot be empty.")

    try:
        dataset_uuid = uuid.UUID(request.dataset_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="dataset_id must be a valid UUID")

    dataset = db.query(Dataset).filter(Dataset.id == dataset_uuid).first()
    if not dataset:
        raise HTTPException(
            status_code=404,
            detail=f"Dataset '{request.dataset_id}' not found. Upload it first via POST /datasets/upload.",
        )

    try:
        df = load_dataset(str(dataset.id))
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"Dataset file for '{request.dataset_id}' is missing from disk.",
        )

    schema = get_schema(df)
    query_type = classify_query(question, schema)
    logger.info("query_classified", extra={"query_type": query_type})

    plan_steps: list[dict[str, Any]] = []
    plan_tools_used: list[str] = []

    response: QueryResponse
    if query_type == "smalltalk":
        response = QueryResponse(
            answer=(
                "Hi! I’m ready. Ask me something about this dataset — for example: "
                "‘Show the top 5 rows’, ‘What columns exist?’, or ‘Which values are highest/lowest?’."
            ),
            query_type="smalltalk",
        )
    elif query_type == "descriptive":
        response = handle_descriptive(df, question)
    elif query_type == "correlation":
        response = handle_correlation_query(question, getattr(dataset, "fact_cache", None))
    elif query_type in {"anomaly", "clustering", "forecast"}:
        response = handle_ml_query(query_type, question, getattr(dataset, "fact_cache", None))
    else:
        is_fast = (mode or "").strip().lower() == "fast"
        if is_fast:
            response = handle_analytical(df, question, _groq_callable)
        else:
            fact_cache = getattr(dataset, "fact_cache", None)
            request_fact_cache: dict[str, Any]
            if isinstance(fact_cache, dict):
                request_fact_cache = dict(fact_cache)
            else:
                request_fact_cache = {}

            plan_steps = plan_query(question, schema, request_fact_cache, dataset_id=str(dataset.id))
            plan_tools_used = []
            seen: set[str] = set()
            for step in plan_steps:
                if not isinstance(step, dict):
                    continue
                tool = step.get("tool")
                if isinstance(tool, str) and tool and tool not in seen:
                    seen.add(tool)
                    plan_tools_used.append(tool)

            related_history = request_fact_cache.get("related_history")
            if not isinstance(related_history, list):
                related_history = []

            related_history_sanitized: list[dict[str, Any]] = []
            for item in related_history:
                if not isinstance(item, dict):
                    continue
                qh = str(item.get("question") or "").strip()
                ah = str(item.get("answer_summary") or "").strip()
                try:
                    sc = float(item.get("score") or 0.0)
                except Exception:
                    sc = 0.0
                if not qh or not ah:
                    continue
                related_history_sanitized.append({"question": qh, "answer_summary": ah, "score": sc})

            results = execute_plan(plan_steps, df, request_fact_cache)
            response = synthesise_results(question, results, schema)

            try:
                response = response.model_copy(update={"related_history": related_history_sanitized})
            except Exception:
                # Non-fatal: response still returns without related history.
                pass

    # Persist query history in the background (do not block response).
    response_json = response.model_dump(mode="json")
    answer_summary = (str(getattr(response, "answer", "") or "")[:200]).strip()
    background_tasks.add_task(
        _persist_query_history_background,
        dataset_id=dataset.id,
        question=question,
        query_type=response.query_type,
        response_json=response_json,
        answer_summary=answer_summary,
        request_id=request_id,
    )

    total_duration_ms = int((time.perf_counter() - start) * 1000)
    logger.info(
        "query_completed",
        extra={
            "request_id": request_id,
            "question": question,
            "plan_steps": plan_steps,
            "plan_tools_used": plan_tools_used,
            "total_duration_ms": total_duration_ms,
        },
    )

    return response


def _persist_query_history_background(
    *,
    dataset_id: uuid.UUID,
    question: str,
    query_type: str,
    response_json: dict[str, Any],
    answer_summary: str,
    request_id: str | None,
) -> None:
    """Best-effort persistence of query history.

    Must never raise.
    """

    try:
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=get_engine())
        db = SessionLocal()
    except Exception:
        logger.exception("query_history_session_init_failed", extra={"request_id": request_id})
        return

    try:
        question_embedding: list[float] | None = None
        try:
            # Local import so the router can load even when embeddings deps are missing.
            from app.services.embedding_service import get_embedding_service

            question_embedding = get_embedding_service().embed([question])[0]
        except Exception:
            # Embedding is optional; keep the record without it.
            logger.info("query_history_embedding_unavailable", extra={"request_id": request_id})

        entry = QueryHistory(
            dataset_id=dataset_id,
            question=question,
            question_embedding=question_embedding,
            answer_summary=answer_summary,
            response_json=response_json,
            query_type=query_type,
            created_at=datetime.datetime.now(datetime.UTC),
        )

        db.add(entry)
        db.commit()
    except Exception:
        logger.exception(
            "failed_persist_query_history",
            extra={"dataset_id": str(dataset_id), "request_id": request_id},
        )
    finally:
        try:
            db.close()
        except Exception:
            pass


def _groq_callable(*, system: str, user: str) -> str:
    if not settings.GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY is not configured")

    headers = {
        "Authorization": f"Bearer {settings.GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    payload: dict[str, Any] = {
        "model": settings.GROQ_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": 900,
        "response_format": {"type": "json_object"},
    }

    r = httpx.post(
        "https://api.groq.com/openai/v1/chat/completions",
        json=payload,
        headers=headers,
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    return data["choices"][0]["message"]["content"]


class HistoryEntry(BaseModel):
    id: str
    question: str
    answer: str
    created_at: Any


@router.get("/history/{dataset_id}", response_model=list[HistoryEntry], summary="Retrieve query history for a dataset")
def get_history(dataset_id: str, db: Session = Depends(get_db)):
    try:
        dataset_uuid = uuid.UUID(dataset_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="dataset_id must be a valid UUID")

    dataset = db.query(Dataset).filter(Dataset.id == dataset_uuid).first()
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found.")

    entries = (
        db.query(QueryHistory)
        .filter(QueryHistory.dataset_id == dataset_uuid)
        .order_by(QueryHistory.created_at.desc())
        .all()
    )

    response: list[HistoryEntry] = []
    for entry in entries:
        response_json = getattr(entry, "response_json", None) or {}
        answer = response_json.get("answer")
        response.append(
            HistoryEntry(
                id=str(entry.id),
                question=entry.question,
                answer=str(answer or ""),
                created_at=entry.created_at,
            )
        )

    return response
