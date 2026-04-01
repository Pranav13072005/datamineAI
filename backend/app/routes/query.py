"""query.py — POST /query route.

Accepts a dataset_id and a natural-language question.

This endpoint intentionally returns a consistent response shape for *all* queries
to keep the frontend stable.

Flow:
    1) Validate dataset exists.
    2) Load DataFrame from disk.
    3) Extract schema.
    4) Classify the question (smalltalk/descriptive/analytical).
    5) Route to deterministic handlers where possible.
    6) Persist history.
    7) Return a QueryResponse (always JSON-serializable).
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.models import Dataset, History
from app.schemas.query import QueryResponse
from app.services.analytical_handler import handle_analytical
from app.services.data_service import get_schema, load_dataset
from app.services.descriptive_handler import handle_descriptive
from app.services.query_classifier import classify_query
from app.utils.config import settings
from app.utils.database import get_db

router = APIRouter()

logger = logging.getLogger(__name__)


# ─── Request / Response schemas ───────────────────────────────────────────────

class QueryRequest(BaseModel):
    dataset_id: str
    question: str


# ─── Route ────────────────────────────────────────────────────────────────────

@router.post("/query", response_model=QueryResponse, summary="Ask a question about a dataset")
async def query_dataset(
    request: QueryRequest,
    db: Session = Depends(get_db),
):
    """
    Full analysis pipeline:
    dataset_id + question → AI-generated code → executed result → saved to history.
    """

    question = (request.question or "").strip()
    if not question:
        raise HTTPException(status_code=422, detail="Question cannot be empty.")

    # ── 1. Validate dataset exists in DB ─────────────────────────────────────
    dataset = db.query(Dataset).filter(Dataset.id == request.dataset_id).first()
    if not dataset:
        raise HTTPException(
            status_code=404,
            detail=f"Dataset '{request.dataset_id}' not found. Upload it first via POST /upload.",
        )

    # ── 2. Load DataFrame from disk ───────────────────────────────────────────
    try:
        df = load_dataset(request.dataset_id)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"Dataset file for '{request.dataset_id}' is missing from disk.",
        )

    # ── 3. Extract schema ─────────────────────────────────────────────────────
    schema = get_schema(df)

    # ── 4. Classify + route ──────────────────────────────────────────────────
    query_type = classify_query(question, schema)
    logger.info("query_classified", extra={"query_type": query_type})

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
    else:
        # Analytical: use strict JSON plan + sandboxed execution.
        response = handle_analytical(df, question, _groq_callable)

    # ── 5. Persist history ───────────────────────────────────────────────────
    # Store the human-readable answer for now.
    try:
        history_entry = History(
            dataset_id=request.dataset_id,
            question=question,
            answer=response.answer,
        )
        db.add(history_entry)
        db.commit()
        db.refresh(history_entry)
    except Exception:
        # History write should never break the response.
        response.warnings.append("failed to persist query history")

    return response


def _groq_callable(*, system: str, user: str) -> str:
    """Small Groq caller used by the analytical handler.

    The analytical handler supports either a Groq client object or a callable.
    We keep this as a callable to avoid adding a new SDK dependency.
    """

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
        # Ask for JSON when supported by Groq.
        "response_format": {"type": "json_object"},
    }

    r = httpx.post(
        "https://api.groq.com/openai/v1/chat/completions",
        json=payload,
        headers=headers,
        timeout=30,
    )

    # Raise a concise error (analytical handler will catch and return warnings).
    r.raise_for_status()
    data = r.json()
    return data["choices"][0]["message"]["content"]


# ─── GET /history/<dataset_id> ────────────────────────────────────────────────

class HistoryEntry(BaseModel):
    id: str
    question: str
    answer: str
    created_at: Any


@router.get(
    "/history/{dataset_id}",
    response_model=list[HistoryEntry],
    summary="Retrieve query history for a dataset",
)
def get_history(dataset_id: str, db: Session = Depends(get_db)):
    """Return all past questions and answers for the given dataset_id."""
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found.")

    entries = (
        db.query(History)
        .filter(History.dataset_id == dataset_id)
        .order_by(History.created_at.desc())
        .all()
    )
    return entries
