from __future__ import annotations

import io
import logging
import os
import time
import uuid
from typing import Any

import anyio
import pandas as pd
from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, Query, UploadFile
from pydantic import BaseModel
import sqlalchemy as sa
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session, sessionmaker

from app.config import settings
from app.middleware.logging_middleware import get_request_id
from app.models import Dataset
from app.schemas.dataset import (
    ColumnSearchResult,
    DatasetInsights,
    DatasetMeta,
    DatasetSchema,
    SearchResponse,
)
from app.services.dataset_service import delete_dataset_file, get_schema, load_dataset, save_dataset
from app.services.embedding_service import get_embedding_service
from app.services.insight_extractor import compute_ml_insights, extract_insights
from app.utils.database import get_db, get_engine


router = APIRouter(prefix="/datasets", tags=["Datasets"])


def _raise_if_schema_missing(exc: Exception) -> None:
    msg = str(exc).lower()
    if "does not exist" in msg and "relation \"datasets\"" in msg:
        raise HTTPException(
            status_code=503,
            detail="Database schema not initialized (datasets table is missing). Run `alembic upgrade head` for your DATABASE_URL.",
        )


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
    try:
        db.commit()
        db.refresh(dataset_record)
    except ProgrammingError as exc:
        _raise_if_schema_missing(exc)
        raise

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


def _insight_count(insights: dict[str, Any]) -> int:
    # A small, stable metric: count of list items across the main list-shaped outputs.
    count = 0
    for key in ("missing", "correlations", "distribution_flags", "numeric_summary"):
        val = insights.get(key)
        if isinstance(val, list):
            count += len(val)
    return count


def _load_dataframe_from_path(file_path: str) -> pd.DataFrame:
    lower = file_path.lower()
    if lower.endswith(".xlsx") or lower.endswith(".xls"):
        return pd.read_excel(file_path)
    return pd.read_csv(file_path)


def _build_column_descriptions(df: pd.DataFrame, max_columns: int = 200) -> list[dict[str, Any]]:
    """Build column metadata + a human-readable description per column.

    Returns a list of dicts with keys: column_name, column_dtype, description.
    """

    cols = list(df.columns)[:max_columns]

    # Precompute numeric min/max in two passes to avoid per-column scans.
    numeric_cols = [c for c in cols if pd.api.types.is_numeric_dtype(df[c].dtype)]
    numeric_min: dict[Any, Any] = {}
    numeric_max: dict[Any, Any] = {}
    if numeric_cols:
        mins = df[numeric_cols].min(skipna=True)
        maxs = df[numeric_cols].max(skipna=True)
        numeric_min = mins.to_dict()
        numeric_max = maxs.to_dict()

    out: list[dict[str, Any]] = []
    for col in cols:
        col_name = str(col)
        dtype_str = str(df[col].dtype)

        s = df[col]
        sample_vals = s.dropna()
        if not sample_vals.empty:
            sample_list = [str(v) for v in sample_vals.head(3).tolist()]
        else:
            sample_list = []

        desc = f"{col_name} ({dtype_str}): sample values: {', '.join(sample_list)}"

        if col in numeric_min and col in numeric_max:
            mn = numeric_min.get(col)
            mx = numeric_max.get(col)
            if pd.notna(mn) and pd.notna(mx):
                desc = f"{desc}; range {mn}–{mx}"

        out.append({"column_name": col_name, "column_dtype": dtype_str, "description": desc})

    return out


def _persist_fact_cache_sync(dataset_uuid: uuid.UUID, payload: dict[str, Any]) -> None:
    """Persist fact_cache using the existing sync engine/session."""

    SessionLocal = sessionmaker(bind=get_engine(), autoflush=False, autocommit=False)
    db = SessionLocal()
    try:
        db.execute(
            sa.update(Dataset).where(Dataset.id == dataset_uuid).values(fact_cache=payload)
        )
        db.commit()
    finally:
        db.close()

def _postgres_table_exists(table: str) -> bool:
    engine = get_engine()
    if engine.dialect.name != "postgresql":
        return False

    with engine.connect() as conn:
        reg = conn.execute(sa.text("select to_regclass(:name)"), {"name": f"public.{table}"}).scalar()
        return reg is not None


def _upsert_column_registry_sync(
    dataset_uuid: uuid.UUID,
    rows: list[dict[str, Any]],
) -> None:
    """Upsert rows into column_registry using the sync SQLAlchemy engine.

    Requires Postgres + the pgvector extension.
    """

    if not rows:
        return

    engine = get_engine()
    if engine.dialect.name != "postgresql":
        return

    from pgvector.sqlalchemy import Vector
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.dialects.postgresql import insert

    metadata = sa.MetaData()
    column_registry = sa.Table(
        "column_registry",
        metadata,
        sa.Column("dataset_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("column_name", sa.Text(), nullable=False),
        sa.Column("column_dtype", sa.Text(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("embedding", Vector(384), nullable=True),
    )

    values = [
        {
            "dataset_id": dataset_uuid,
            "column_name": r["column_name"],
            "column_dtype": r.get("column_dtype"),
            "description": r.get("description"),
            "embedding": r.get("embedding"),
        }
        for r in rows
    ]

    stmt = insert(column_registry).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["dataset_id", "column_name"],
        set_={
            "column_dtype": stmt.excluded.column_dtype,
            "description": stmt.excluded.description,
            "embedding": stmt.excluded.embedding,
        },
    )

    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    db = SessionLocal()
    try:
        db.execute(stmt)
        db.commit()
    finally:
        db.close()


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
        def _compute() -> tuple[dict[str, Any], pd.DataFrame]:
            df = _load_dataframe_from_path(file_path)
            # Quick glance only (fast). Heavy ML is computed after we persist.
            return extract_insights(df), df

        insights, df = await anyio.to_thread.run_sync(_compute)

        # Persist quick insights first (fast), using the same sync engine the API uses.
        await anyio.to_thread.run_sync(_persist_fact_cache_sync, dataset_uuid, insights)

        # Compute ML outputs after quick insights are already persisted.
        try:
            ml = await anyio.to_thread.run_sync(lambda: compute_ml_insights(df))
            if isinstance(ml, dict):
                insights["ml"] = ml

                await anyio.to_thread.run_sync(_persist_fact_cache_sync, dataset_uuid, insights)
        except Exception as exc:
            # ML is best-effort and should never block quick insights.
            logger.exception(
                "ml_insights_failed",
                extra={"request_id": request_id, "dataset_id": dataset_id, "error": str(exc)},
            )

        # Populate column_registry (Postgres only). Best-effort; must never block fact_cache.
        if _effective_db_dialect_name() == "postgresql" and not _postgres_table_exists("column_registry"):
            logger.warning(
                "column_registry_missing",
                extra={"request_id": request_id, "dataset_id": dataset_id},
            )

        if _effective_db_dialect_name() == "postgresql" and _postgres_table_exists("column_registry"):
            try:
                from app.services.embedding_service import get_embedding_service
                from app.services.dataset_service import find_related_datasets

                col_meta = await anyio.to_thread.run_sync(_build_column_descriptions, df)
                descriptions = [m["description"] for m in col_meta]

                # Batch embed all column descriptions in one model call.
                embeddings = await anyio.to_thread.run_sync(
                    lambda: get_embedding_service().embed(descriptions)
                )

                # Attach embeddings.
                for m, emb in zip(col_meta, embeddings, strict=False):
                    m["embedding"] = emb

                await anyio.to_thread.run_sync(_upsert_column_registry_sync, dataset_uuid, col_meta)

                # Compute related datasets based on newly inserted embeddings.
                # Best-effort and should not impact the main insights payload.
                try:
                    related = await anyio.to_thread.run_sync(
                        lambda: find_related_datasets(dataset_id, threshold=0.85)
                    )
                    insights["related_datasets"] = related
                    await anyio.to_thread.run_sync(_persist_fact_cache_sync, dataset_uuid, insights)
                except Exception as exc:
                    logger.exception(
                        "related_datasets_failed",
                        extra={
                            "request_id": request_id,
                            "dataset_id": dataset_id,
                            "error": str(exc),
                        },
                    )
            except Exception as exc:
                logger.exception(
                    "column_registry_population_failed",
                    extra={"request_id": request_id, "dataset_id": dataset_id, "error": str(exc)},
                )

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

            await anyio.to_thread.run_sync(_persist_fact_cache_sync, dataset_uuid, payload)
        except Exception:
            # Swallow everything; this must never crash.
            logger.exception(
                "insights_error_persist_failed",
                extra={"request_id": request_id, "dataset_id": dataset_id},
            )


@router.get("", response_model=list[DatasetMeta], summary="List datasets")
def list_datasets(db: Session = Depends(get_db)) -> list[DatasetMeta]:
    try:
        datasets = db.query(Dataset).order_by(Dataset.created_at.desc()).all()
    except ProgrammingError as exc:
        _raise_if_schema_missing(exc)
        raise
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


@router.get("/search", response_model=SearchResponse, summary="Semantic search over dataset columns")
async def search_dataset_columns(
    q: str = Query(..., min_length=1),
    top_k: int = Query(10, ge=1, le=50),
) -> SearchResponse:
    """Search columns across all datasets using pgvector embeddings.

    If the API isn't using Postgres, returns an empty list.
    """

    engine = get_engine()
    if engine.dialect.name != "postgresql":
        return SearchResponse(query=q, results=[])

    from pgvector.sqlalchemy import Vector

    # Compute embedding without blocking the event loop.
    query_vec = await anyio.to_thread.run_sync(lambda: get_embedding_service().embed([q])[0])

    def _run_query() -> list[dict[str, Any]]:
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
                sa.bindparam("top_k", value=int(top_k)),
            )
        )

        with engine.connect() as conn:
            return conn.execute(stmt).mappings().all()

    rows = await anyio.to_thread.run_sync(_run_query)

    results: list[ColumnSearchResult] = []
    for row in rows:
        results.append(
            ColumnSearchResult(
                dataset_id=str(row.get("dataset_id") or ""),
                dataset_name=str(row.get("dataset_name") or ""),
                column_name=str(row.get("column_name") or ""),
                description=str(row.get("description") or ""),
                similarity_score=float(row.get("similarity_score") or 0.0),
            )
        )

    # SQL already returns closest-first (highest similarity), but keep it explicit.
    results.sort(key=lambda r: r.similarity_score, reverse=True)

    return SearchResponse(query=q, results=results)


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
