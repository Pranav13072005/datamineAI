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
import re

from app.models import Dataset, History
from app.services.ai_service import LLMError, generate_response
from app.services.data_service import build_dataset_overview, execute_query_code, get_schema, load_dataset
from app.utils.database import get_db

router = APIRouter()


# ─── Request / Response schemas ───────────────────────────────────────────────

class QueryRequest(BaseModel):
    dataset_id: str
    question: str


class QueryResponse(BaseModel):
    dataset_id: str
    question: str
    # Preferred structured UI fields (optional)
    response: str | None = None
    insights: list[str] | None = None
    table_data: list[dict[str, Any]] | None = None
    table_title: str | None = None

    # Backwards-compatible payload field
    result: Any | None = None          # Can be scalar, list, dict, etc.
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

    # Small talk / non-analytic questions: answer directly without LLM/code execution.
    # This avoids failures when the model returns plain text or when the user is just testing.
    if _is_smalltalk(question):
        result = (
            "Hi! I'm ready. Ask me something about this dataset — for example: "
            "'Show the top 5 rows', 'How many rows are positive sentiment?', or "
            "'What is the distribution of sentiment?'."
        )

        history_entry = History(
            dataset_id=request.dataset_id,
            question=question,
            answer=str(result),
        )
        db.add(history_entry)
        db.commit()
        db.refresh(history_entry)

        return QueryResponse(
            dataset_id=request.dataset_id,
            question=question,
            result=result,
            history_id=history_entry.id,
        )

    # ── 2. Load DataFrame from disk ───────────────────────────────────────────
    try:
        df = load_dataset(request.dataset_id)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"Dataset file for '{request.dataset_id}' is missing from disk.",
        )

    # Dataset overview / explanation questions: answer deterministically with pandas
    # (no LLM needed), so users get an actual analysis rather than suggested code.
    if _is_overview_question(question):
        overview = build_dataset_overview(df)

        history_entry = History(
            dataset_id=request.dataset_id,
            question=question,
            answer=str(overview.get("response") or overview),
        )
        db.add(history_entry)
        db.commit()
        db.refresh(history_entry)

        return QueryResponse(
            dataset_id=request.dataset_id,
            question=question,
            response=overview.get("response"),
            insights=overview.get("insights"),
            table_data=overview.get("table_data"),
            table_title=overview.get("table_title"),
            result=overview.get("overview"),
            history_id=history_entry.id,
        )

    # ── 3. Extract schema ─────────────────────────────────────────────────────
    schema = get_schema(df)

    # ── 4. Generate code via AI service ──────────────────────────────────────
    try:
        generated_code = generate_response(question=question, schema=schema)
    except LLMError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e

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
        question=question,
        answer=result_str,
    )
    db.add(history_entry)
    db.commit()
    db.refresh(history_entry)

    # ── 7. Return response ────────────────────────────────────────────────────
    return QueryResponse(
        dataset_id=request.dataset_id,
        question=question,
        result=result,
        history_id=history_entry.id,
    )


def _is_smalltalk(question: str) -> bool:
    q = question.strip().lower()
    if len(q) <= 4 and q in {"hi", "hey", "yo", "sup"}:
        return True
    if q in {"hello", "good morning", "good afternoon", "good evening"}:
        return True
    if re.fullmatch(r"hi+", q) or re.fullmatch(r"hey+", q):
        return True
    return False


def _is_overview_question(question: str) -> bool:
    q = question.strip().lower()

    # Direct phrase matches
    keywords = [
        "describe the dataset",
        "explain the dataset",
        "dataset explanation",
        "dataset overview",
        "give an overview",
        "summarize the dataset",
        "summary of the dataset",
        "what is in this dataset",
        "tell me about the dataset",
        "columns and rows",
        "show me the schema",
    ]
    if any(k in q for k in keywords):
        return True

    # Heuristic: if the user is asking for a dataset-level summary (columns/rows/missing/duplicates),
    # use the deterministic pandas overview instead of LLM-generated code.
    intent_words = {"summarize", "summary", "describe", "overview", "explain", "schema", "profile"}
    dataset_words = {"dataset", "data", "columns", "column", "rows", "row", "missing", "null", "na", "duplicate", "duplicates"}
    return any(w in q for w in intent_words) and any(w in q for w in dataset_words)


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
