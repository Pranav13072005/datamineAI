from __future__ import annotations

import logging
from typing import Any
import uuid

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
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
from app.utils.database import get_db


router = APIRouter(tags=["Query"])
logger = logging.getLogger(__name__)


class QueryRequest(BaseModel):
    dataset_id: str
    question: str


@router.post("/query", response_model=QueryResponse, summary="Ask a question about a dataset")
async def query_dataset(request: QueryRequest, db: Session = Depends(get_db)):
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
        response = handle_analytical(df, question, _groq_callable)

    try:
        history_entry = QueryHistory(
            dataset_id=dataset.id,
            question=question,
            response_json=response.model_dump(mode="json"),
            query_type=response.query_type,
        )
        db.add(history_entry)
        db.commit()
        db.refresh(history_entry)
    except Exception:
        logger.exception("failed_persist_query_history", extra={"dataset_id": str(dataset.id)})
        response.warnings.append("failed to persist query history")

    return response


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
