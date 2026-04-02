from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


class DatasetMeta(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    name: str
    rows: int
    columns: int
    uploaded_at: datetime | None = None


class DatasetSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    columns: list[str]
    dtypes: dict[str, str]
    sample_rows: list[dict[str, Any]]
    row_count: int


class DatasetInsights(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_id: str
    status: str  # "ready", "processing", "error"
    insights: dict[str, Any] | None = None
    generated_at: datetime | None = None
