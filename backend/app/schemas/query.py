from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _to_jsonable(value: Any) -> Any:
    """Convert common numpy/pandas scalars/containers into JSON-serializable Python.

    This is defensive: it prevents leaking numpy/pandas types into API responses.
    """

    if value is None:
        return None

    if isinstance(value, (str, int, float, bool)):
        return value

    # bytes -> decode best-effort
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8")
        except Exception:
            return value.hex()

    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(v) for v in value]

    # numpy / pandas scalars
    try:
        import numpy as np  # type: ignore

        if isinstance(value, np.generic):
            return _to_jsonable(value.item())
        if isinstance(value, np.dtype):
            return str(value)
    except Exception:
        pass

    try:
        import pandas as pd  # type: ignore

        if value is pd.NA:
            return None
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if isinstance(value, pd.Timedelta):
            return str(value)
        if isinstance(value, pd.Series):
            return _to_jsonable(value.to_list())
        if isinstance(value, pd.DataFrame):
            # preserve table shape as list[dict]
            return _to_jsonable(value.to_dict(orient="records"))
    except Exception:
        pass

    # last resort: string representation
    return str(value)


class TableResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    columns: list[str] = Field(default_factory=list)
    rows: list[list[Any]] = Field(default_factory=list)

    @field_validator("rows", mode="before")
    @classmethod
    def _rows_jsonable(cls, v: Any) -> Any:
        return _to_jsonable(v)


class ChartSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # Plotly trace/layout spec. `type` is used only as a hint; frontend renders
    # based on `data` (e.g., heatmap traces).
    type: str
    data: dict[str, Any] = Field(default_factory=dict)

    @field_validator("data", mode="before")
    @classmethod
    def _data_jsonable(cls, v: Any) -> Any:
        return _to_jsonable(v)


class RelatedHistoryItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question: str
    answer_summary: str
    score: float


class QueryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    answer: str
    table: Optional[TableResult] = None
    chart: Optional[ChartSpec] = None
    insights: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    related_history: list[RelatedHistoryItem] = Field(default_factory=list)
    query_type: Literal[
        "analytical",
        "descriptive",
        "smalltalk",
        "correlation",
        "anomaly",
        "clustering",
        "forecast",
        "error",
    ] = "analytical"

    @field_validator("insights", "warnings", mode="before")
    @classmethod
    def _lists_jsonable(cls, v: Any) -> Any:
        return _to_jsonable(v)


