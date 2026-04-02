from __future__ import annotations

import io
import os
import uuid
from typing import Any

import pandas as pd
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.models import Dataset
from app.schemas.dataset import DatasetMeta, DatasetSchema
from app.services.dataset_service import delete_dataset_file, get_schema, load_dataset, save_dataset
from app.utils.database import get_db
from app.config import settings


router = APIRouter(prefix="/datasets", tags=["Datasets"])


class UploadResponse(BaseModel):
    dataset_id: str
    filename: str
    row_count: int
    columns: list[str]
    message: str


@router.post("/upload", response_model=UploadResponse, summary="Upload a CSV dataset")
async def upload_dataset(
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

    return UploadResponse(
        dataset_id=dataset_id,
        filename=file.filename,
        row_count=schema["row_count"],
        columns=schema["columns"],
        message="Dataset uploaded successfully.",
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
