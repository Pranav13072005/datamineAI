"""
upload.py — POST /upload route.

Accepts a CSV file, saves it to disk, and records metadata in the database.
Returns a dataset_id that the client uses for all subsequent /query calls.
"""

import uuid

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.models import Dataset
from app.services.data_service import get_schema, load_dataset, save_dataset
from app.utils.database import get_db

router = APIRouter()


# ─── Response schema ──────────────────────────────────────────────────────────

class UploadResponse(BaseModel):
    dataset_id: str
    filename: str
    row_count: int
    columns: list[str]
    message: str


# ─── Route ────────────────────────────────────────────────────────────────────

@router.post("/upload", response_model=UploadResponse, summary="Upload a CSV dataset")
async def upload_csv(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    """
    1. Validate the file is a CSV.
    2. Generate a UUID as the dataset_id.
    3. Save the CSV to disk (uploaded_datasets/<dataset_id>.csv).
    4. Insert a row into the `datasets` table.
    5. Return metadata + dataset_id to the client.
    """

    # ── 1. Validate file type ────────────────────────────────────────────────
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are accepted.")

    # ── 2. Read bytes ────────────────────────────────────────────────────────
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    # ── 3. Generate dataset_id & save to disk ────────────────────────────────
    dataset_id = str(uuid.uuid4())
    try:
        save_dataset(content, dataset_id)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    # ── 4. Extract schema for the response ───────────────────────────────────
    df = load_dataset(dataset_id)
    schema = get_schema(df)

    # ── 5. Persist metadata to database ─────────────────────────────────────
    dataset_record = Dataset(id=dataset_id, name=file.filename)
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
