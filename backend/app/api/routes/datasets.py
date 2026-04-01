from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.models import Dataset
from app.services.data_service import get_schema, load_dataset
from app.utils.database import get_db


router = APIRouter()


class DatasetOut(BaseModel):
    id: str
    name: str
    rows: int
    columns: int
    uploaded_at: datetime | None = None


class DatasetSchemaOut(BaseModel):
    columns: list[str]
    dtypes: dict[str, str]
    sample_rows: list[dict[str, Any]]
    row_count: int


@router.get("", response_model=list[DatasetOut], summary="List datasets")
def list_datasets(db: Session = Depends(get_db)) -> list[DatasetOut]:
    datasets = db.query(Dataset).order_by(Dataset.created_at.desc()).all()
    response: list[DatasetOut] = []

    for dataset in datasets:
        rows = 0
        cols = 0
        try:
            df = load_dataset(dataset.id)
            schema = get_schema(df)
            rows = int(schema.get("row_count", 0) or 0)
            cols = len(schema.get("columns", []) or [])
        except FileNotFoundError:
            # Dataset exists in DB but file is missing; keep it listable.
            rows = 0
            cols = 0

        response.append(
            DatasetOut(
                id=dataset.id,
                name=dataset.name,
                rows=rows,
                columns=cols,
                uploaded_at=dataset.created_at,
            )
        )

    return response


@router.get("/{dataset_id}", response_model=DatasetOut, summary="Get dataset")
def get_dataset(dataset_id: str, db: Session = Depends(get_db)) -> DatasetOut:
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    try:
        df = load_dataset(dataset.id)
        schema = get_schema(df)
        rows = int(schema.get("row_count", 0) or 0)
        cols = len(schema.get("columns", []) or [])
    except FileNotFoundError:
        rows = 0
        cols = 0

    return DatasetOut(
        id=dataset.id,
        name=dataset.name,
        rows=rows,
        columns=cols,
        uploaded_at=dataset.created_at,
    )


@router.get(
    "/{dataset_id}/schema",
    response_model=DatasetSchemaOut,
    summary="Get dataset schema",
)
def get_dataset_schema(dataset_id: str, db: Session = Depends(get_db)) -> DatasetSchemaOut:
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    try:
        df = load_dataset(dataset_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Dataset file for '{dataset_id}' is missing")

    schema = get_schema(df)
    return DatasetSchemaOut(**schema)


@router.delete("/{dataset_id}", summary="Delete dataset")
def delete_dataset(dataset_id: str, db: Session = Depends(get_db)) -> dict:
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    # Best-effort delete the file from disk.
    try:
        from app.services.data_service import delete_dataset_file

        delete_dataset_file(dataset_id)
    except Exception:
        # Don't block DB deletion on file cleanup.
        pass

    db.delete(dataset)
    db.commit()

    return {"status": "ok", "deleted": dataset_id}
