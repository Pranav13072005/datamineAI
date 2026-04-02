from __future__ import annotations

from functools import lru_cache
import io
import logging
import os
import time
import uuid
from typing import Any

import anyio
import pandas as pd
from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, UploadFile
from pydantic import BaseModel
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import settings
from app.middleware.logging_middleware import get_request_id
from app.models import Dataset
from app.schemas.dataset import DatasetInsights, DatasetMeta, DatasetSchema
from app.services.dataset_service import delete_dataset_file, get_schema, load_dataset, save_dataset
from app.services.insight_extractor import extract_insights
from app.utils.database import get_db, get_engine


router = APIRouter(prefix="/datasets", tags=["Datasets"])


class UploadResponse(BaseModel):
    dataset_id: str
    filename: str
    row_count: int
    columns: list[str]
    message: str


@router.post("/upload", response_model=UploadResponse, summary="Upload a CSV dataset")
async def upload_dataset(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename.")

    filename_lower = file.filename.lower()
    is_csv = filename_lower.endswith(".csv")
    is_excel = filename_lower.endswith(".xlsx") or filename_lower.endswith(".xls")
    if not (is_csv or is_excel):
        raise HTTPException(status_code=400, detail="Only .csv, .xlsx, and .xls files are accepted.")

    max_bytes = settings.MAX_UPLOAD_BYTES
    chunk_size = 1024 * 1024  # 1MB
    buf = bytearray()

    while True:
        chunk = await file.read(chunk_size)
        if not chunk:
            break
        buf.extend(chunk)
        if len(buf) > max_bytes:
            raise HTTPException(
                status_code=413,
                detail=f"File too large. Max size is {settings.MAX_UPLOAD_MB}MB.",
            )

    content = bytes(buf)
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    dataset_uuid = uuid.uuid4()
    dataset_id = str(dataset_uuid)
    try:
        if is_excel:
            df = pd.read_excel(io.BytesIO(content))
            csv_bytes = df.to_csv(index=False).encode("utf-8")
            save_dataset(csv_bytes, dataset_id)
            schema = get_schema(df)
        else:
            save_dataset(content, dataset_id)
            df = load_dataset(dataset_id)
            schema = get_schema(df)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception:
        raise HTTPException(status_code=422, detail="Uploaded file is not a valid CSV/Excel dataset.")

    file_path = os.path.join(settings.UPLOAD_DIR, f"{dataset_id}.csv")
    row_count = int(schema.get("row_count", 0) or 0)
    col_count = len(schema.get("columns", []) or [])

    dataset_record = Dataset(
        id=dataset_uuid,
        name=file.filename,
        file_path=file_path,
        row_count=row_count,
        col_count=col_count,
        schema_json=schema,
        fact_cache=None,
    )
    db.add(dataset_record)
    db.commit()
    db.refresh(dataset_record)

    # Capture request_id before middleware clears its context var.
    request_id = get_request_id()
    background_tasks.add_task(
        run_insight_extraction,
        dataset_id=dataset_id,
        file_path=file_path,
        request_id=request_id,
    )

    return UploadResponse(
        dataset_id=dataset_id,
        filename=file.filename,
        row_count=schema["row_count"],
        columns=schema["columns"],
        message="Dataset uploaded successfully.",
    )


logger = logging.getLogger("app.insights")


def _coerce_async_database_url(url: str) -> str:
    """Convert a sync DB URL into an async-driver URL when applicable."""

    if not url:
        return url

    if url.startswith("postgresql"):
        if "+asyncpg" in url:
            return url
        # Replace any explicit sync driver with asyncpg.
        if "+psycopg2" in url:
            return url.replace("postgresql+psycopg2", "postgresql+asyncpg")
        return url.replace("postgresql", "postgresql+asyncpg", 1)

    if url.startswith("sqlite"):
        if "+aiosqlite" in url:
            return url
        return url.replace("sqlite", "sqlite+aiosqlite", 1)

    return url


@lru_cache
def _get_async_engine(sync_database_url: str) -> AsyncEngine:
    url = _coerce_async_database_url(sync_database_url)
    # For Postgres we want pool_pre_ping; for SQLite it is ignored.
    return create_async_engine(url, pool_pre_ping=True)


@lru_cache
def _get_async_sessionmaker(sync_database_url: str) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(_get_async_engine(sync_database_url), expire_on_commit=False)


def _insight_count(insights: dict[str, Any]) -> int:
    # A small, stable metric: count of list items across the main list-shaped outputs.
    count = 0
    for key in ("missing", "correlations", "distribution_flags", "numeric_summary", "sample_rows"):
        val = insights.get(key)
        if isinstance(val, list):
            count += len(val)
    return count


def _load_dataframe_from_path(file_path: str) -> pd.DataFrame:
    lower = file_path.lower()
    if lower.endswith(".xlsx") or lower.endswith(".xls"):
        return pd.read_excel(file_path)
    return pd.read_csv(file_path)


def _persist_fact_cache_sync(dataset_uuid: uuid.UUID, payload: dict[str, Any]) -> None:
    """Persist fact_cache using the existing sync engine/session.

    This is used for SQLite (including tests) so we don't require async SQLite drivers.
    """

    SessionLocal = sessionmaker(bind=get_engine(), autoflush=False, autocommit=False)
    db = SessionLocal()
    try:
        db.execute(
            sa.update(Dataset).where(Dataset.id == dataset_uuid).values(fact_cache=payload)
        )
        db.commit()
    finally:
        db.close()


async def _persist_fact_cache_async(dataset_uuid: uuid.UUID, payload: dict[str, Any]) -> None:
    """Persist fact_cache using an async SQLAlchemy session (Postgres/asyncpg)."""

    # Use the *effective* sync engine URL (psycopg2) and coerce to asyncpg.
    # This matters when the app falls back to SQLite but settings still points to Postgres.
    effective_sync_url = str(get_engine().url)
    SessionAsync = _get_async_sessionmaker(effective_sync_url)
    async with SessionAsync() as session:
        await session.execute(
            sa.update(Dataset).where(Dataset.id == dataset_uuid).values(fact_cache=payload)
        )
        await session.commit()


def _effective_db_dialect_name() -> str:
    """Return the dialect name of the engine the API is *actually* using."""

    try:
        return get_engine().dialect.name
    except Exception:
        # Fall back to settings-based heuristic.
        url = settings.SQLALCHEMY_DATABASE_URI
        return "postgresql" if url.startswith("postgresql") else "sqlite"


async def run_insight_extraction(dataset_id: str, file_path: str, request_id: str | None = None) -> None:
    """Background task: compute insights and persist to datasets.fact_cache.

    - Never raises (errors are logged and stored in fact_cache as {"error": "..."}).
    """

    start = time.perf_counter()
    try:
        dataset_uuid = uuid.UUID(dataset_id)

        # Pandas work is CPU-bound and should not block the event loop.
        def _compute() -> dict[str, Any]:
            df = _load_dataframe_from_path(file_path)
            return extract_insights(df)

        insights: dict[str, Any] = await anyio.to_thread.run_sync(_compute)

        # IMPORTANT: determine DB based on the effective engine.
        # When Postgres is unreachable, app startup switches to SQLite, but settings may still
        # contain the Postgres URL. If we follow settings here, persistence will fail and
        # /insights will stay stuck in "processing".
        if _effective_db_dialect_name() == "postgresql":
            await _persist_fact_cache_async(dataset_uuid, insights)
        else:
            await anyio.to_thread.run_sync(_persist_fact_cache_sync, dataset_uuid, insights)

        duration_ms = int((time.perf_counter() - start) * 1000)
        logger.info(
            "insights_extracted",
            extra={
                "request_id": request_id,
                "dataset_id": dataset_id,
                "duration_ms": duration_ms,
                "insight_count": _insight_count(insights),
            },
        )
    except Exception as exc:
        duration_ms = int((time.perf_counter() - start) * 1000)
        logger.exception(
            "insights_extraction_failed",
            extra={
                "request_id": request_id,
                "dataset_id": dataset_id,
                "duration_ms": duration_ms,
                "error": str(exc),
            },
        )

        # Best-effort error persistence.
        try:
            dataset_uuid = uuid.UUID(dataset_id)
            payload = {"error": str(exc)}

            if _effective_db_dialect_name() == "postgresql":
                await _persist_fact_cache_async(dataset_uuid, payload)
            else:
                await anyio.to_thread.run_sync(_persist_fact_cache_sync, dataset_uuid, payload)
        except Exception:
            # Swallow everything; this must never crash.
            logger.exception(
                "insights_error_persist_failed",
                extra={"request_id": request_id, "dataset_id": dataset_id},
            )


@router.get("", response_model=list[DatasetMeta], summary="List datasets")
def list_datasets(db: Session = Depends(get_db)) -> list[DatasetMeta]:
    datasets = db.query(Dataset).order_by(Dataset.created_at.desc()).all()
    response: list[DatasetMeta] = []

    for dataset in datasets:
        rows = int(getattr(dataset, "row_count", 0) or 0)
        cols = int(getattr(dataset, "col_count", 0) or 0)

        # Backfill counts for older rows if needed.
        if rows == 0 or cols == 0:
            try:
                df = load_dataset(str(dataset.id))
                schema = get_schema(df)
                rows = int(schema.get("row_count", 0) or 0)
                cols = len(schema.get("columns", []) or [])
            except FileNotFoundError:
                rows = 0
                cols = 0

        response.append(
            DatasetMeta(
                id=str(dataset.id),
                name=dataset.name,
                rows=rows,
                columns=cols,
                uploaded_at=dataset.created_at,
            )
        )

    return response


@router.get("/{dataset_id}", response_model=DatasetMeta, summary="Get dataset")
def get_dataset(dataset_id: str, db: Session = Depends(get_db)) -> DatasetMeta:
    try:
        dataset_uuid = uuid.UUID(dataset_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="dataset_id must be a valid UUID")

    dataset = db.query(Dataset).filter(Dataset.id == dataset_uuid).first()
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    rows = int(getattr(dataset, "row_count", 0) or 0)
    cols = int(getattr(dataset, "col_count", 0) or 0)

    if rows == 0 or cols == 0:
        try:
            df = load_dataset(str(dataset.id))
            schema = get_schema(df)
            rows = int(schema.get("row_count", 0) or 0)
            cols = len(schema.get("columns", []) or [])
        except FileNotFoundError:
            rows = 0
            cols = 0

    return DatasetMeta(
        id=str(dataset.id),
        name=dataset.name,
        rows=rows,
        columns=cols,
        uploaded_at=dataset.created_at,
    )


@router.get("/{dataset_id}/schema", response_model=DatasetSchema, summary="Get dataset schema")
def get_dataset_schema(dataset_id: str, db: Session = Depends(get_db)) -> DatasetSchema:
    try:
        dataset_uuid = uuid.UUID(dataset_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="dataset_id must be a valid UUID")

    dataset = db.query(Dataset).filter(Dataset.id == dataset_uuid).first()
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    try:
        df = load_dataset(str(dataset.id))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Dataset file for '{dataset_id}' is missing")

    schema = get_schema(df)
    return DatasetSchema(**schema)


@router.get("/{dataset_id}/insights", response_model=DatasetInsights, summary="Get dataset insights")
def get_dataset_insights(dataset_id: str, db: Session = Depends(get_db)) -> DatasetInsights:
    try:
        dataset_uuid = uuid.UUID(dataset_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="dataset_id must be a valid UUID")

    dataset = db.query(Dataset).filter(Dataset.id == dataset_uuid).first()
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    fact_cache = getattr(dataset, "fact_cache", None)

    if fact_cache is None:
        return DatasetInsights(
            dataset_id=str(dataset.id),
            status="processing",
            insights=None,
            generated_at=None,
        )

    if isinstance(fact_cache, dict) and "error" in fact_cache:
        return DatasetInsights(
            dataset_id=str(dataset.id),
            status="error",
            insights=fact_cache,
            generated_at=getattr(dataset, "created_at", None),
        )

    return DatasetInsights(
        dataset_id=str(dataset.id),
        status="ready",
        insights=fact_cache if isinstance(fact_cache, dict) else {"value": fact_cache},
        generated_at=getattr(dataset, "created_at", None),
    )


@router.delete("/{dataset_id}", summary="Delete dataset")
def delete_dataset(dataset_id: str, db: Session = Depends(get_db)) -> dict:
    try:
        dataset_uuid = uuid.UUID(dataset_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="dataset_id must be a valid UUID")

    dataset = db.query(Dataset).filter(Dataset.id == dataset_uuid).first()
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    try:
        delete_dataset_file(str(dataset.id))
    except Exception:
        pass

    db.delete(dataset)
    db.commit()

    return {"status": "ok", "deleted": dataset_id}
