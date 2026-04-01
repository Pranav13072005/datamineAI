"""
upload.py — POST /upload route.

Accepts a CSV file, saves it to disk, and records metadata in the database.
Returns a dataset_id that the client uses for all subsequent /query calls.
"""

import io
import uuid

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from pydantic import BaseModel
from sqlalchemy.orm import Session

import pandas as pd

from app.models import Dataset
from app.services.data_service import get_schema, load_dataset, save_dataset
from app.utils.database import get_db
from app.utils.config import settings

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
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename.")

    filename_lower = file.filename.lower()
    is_csv = filename_lower.endswith(".csv")
    is_excel = filename_lower.endswith(".xlsx") or filename_lower.endswith(".xls")
    if not (is_csv or is_excel):
        raise HTTPException(status_code=400, detail="Only .csv, .xlsx, and .xls files are accepted.")

    # ── 2. Read bytes (bounded) ─────────────────────────────────────────────
    # Read in chunks so we can enforce limits without huge memory spikes.
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

    # ── 3. Generate dataset_id & save to disk ────────────────────────────────
    dataset_id = str(uuid.uuid4())
    try:
        if is_excel:
            # Convert Excel to a normalized CSV for storage.
            df = pd.read_excel(io.BytesIO(content))
            csv_bytes = df.to_csv(index=False).encode("utf-8")
            save_dataset(csv_bytes, dataset_id)
            schema = get_schema(df)
        else:
            save_dataset(content, dataset_id)
            df = load_dataset(dataset_id)
            schema = get_schema(df)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception:
        raise HTTPException(status_code=422, detail="Uploaded file is not a valid CSV/Excel dataset.")

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
