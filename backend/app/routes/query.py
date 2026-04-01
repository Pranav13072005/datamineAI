"""
query.py — POST /query route.

Accepts a dataset_id and a natural-language question.
Pipeline:
  1. Validate dataset_id exists in the database.
  2. Load the DataFrame from disk.
  3. Extract schema (columns, dtypes, sample rows).
  4. Send schema + question to the AI service → get Python code back.
  5. Safely execute the code on the DataFrame.
  6. Save the question + answer to the `history` table.
  7. Return the result to the client.
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Any

from app.models import Dataset, History
from app.services.ai_service import generate_response
from app.services.data_service import execute_query_code, get_schema, load_dataset
from app.utils.database import get_db

router = APIRouter()


# ─── Request / Response schemas ───────────────────────────────────────────────

class QueryRequest(BaseModel):
    dataset_id: str
    question: str


class QueryResponse(BaseModel):
    dataset_id: str
    question: str
    result: Any          # Can be scalar, list, dict, etc.
    history_id: str


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

    # ── 4. Generate code via AI service ──────────────────────────────────────
    generated_code = generate_response(question=request.question, schema=schema)

    # ── 5. Safely execute the generated code ─────────────────────────────────
    try:
        result = execute_query_code(generated_code, df)
    except ValueError as e:
        # Security filter blocked the code
        raise HTTPException(status_code=422, detail=f"Unsafe code blocked: {e}")
    except RuntimeError as e:
        # Execution error
        raise HTTPException(status_code=500, detail=f"Code execution error: {e}")

    # Convert result to a JSON-serializable string for storage
    result_str = str(result)

    # ── 6. Persist query + result to history ─────────────────────────────────
    history_entry = History(
        dataset_id=request.dataset_id,
        question=request.question,
        answer=result_str,
    )
    db.add(history_entry)
    db.commit()
    db.refresh(history_entry)

    # ── 7. Return response ────────────────────────────────────────────────────
    return QueryResponse(
        dataset_id=request.dataset_id,
        question=request.question,
        result=result,
        history_id=history_entry.id,
    )


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
