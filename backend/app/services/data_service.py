"""
data_service.py — Dataset management and query execution service.

Responsibilities:
  - Save uploaded CSV files to the local filesystem.
  - Load a dataset from disk by its dataset_id.
  - Extract schema (columns, dtypes, sample rows) for the LLM prompt.
  - Safely execute AI-generated Python/pandas code against a DataFrame.
"""

import io
import contextlib
import math
import os
import re
import traceback
from typing import Any

from io import StringIO

import pandas as pd
import numpy as np

from app.utils.config import settings

# ─── Storage directory ────────────────────────────────────────────────────────

# Ensure the upload directory exists on startup
os.makedirs(settings.UPLOAD_DIR, exist_ok=True)


def _dataset_path(dataset_id: str) -> str:
    """Return the file path for a stored dataset CSV."""
    return os.path.join(settings.UPLOAD_DIR, f"{dataset_id}.csv")


# ─── Save & Load ──────────────────────────────────────────────────────────────

def save_dataset(file_bytes: bytes, dataset_id: str) -> None:
    """
    Persist the uploaded CSV bytes to disk under the dataset_id filename.
    Raises ValueError if the bytes don't represent a valid CSV.
    """
    # Validate it's readable as a CSV before saving
    try:
        pd.read_csv(io.BytesIO(file_bytes), nrows=5)
    except Exception:
        raise ValueError("Uploaded file is not a valid CSV.")

    path = _dataset_path(dataset_id)
    with open(path, "wb") as f:
        f.write(file_bytes)


def load_dataset(dataset_id: str) -> pd.DataFrame:
    """
    Load and return the DataFrame for the given dataset_id.
    Raises FileNotFoundError if the dataset doesn't exist on disk.
    """
    path = _dataset_path(dataset_id)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Dataset '{dataset_id}' not found on disk.")
    return pd.read_csv(path)


def delete_dataset_file(dataset_id: str) -> None:
    """Delete a stored dataset file from disk.

    This is a best-effort cleanup helper; callers can choose whether to
    treat missing files as errors.
    """

    path = _dataset_path(dataset_id)
    if os.path.exists(path):
        os.remove(path)


# ─── Schema Extraction ────────────────────────────────────────────────────────

def _truncate_value(value: Any, *, max_chars: int) -> Any:
    if value is None:
        return None
    if isinstance(value, str) and len(value) > max_chars:
        return value[: max_chars - 1] + "…"
    return value


def get_schema(df: pd.DataFrame, sample_rows: int = 3, *, max_cell_chars: int = 200) -> dict:
    """
    Build a schema dict describing the DataFrame:
      - columns     : list of column names
      - dtypes      : column → dtype string
      - sample_rows : first N rows as list of dicts (JSON-serializable)
      - row_count   : total number of rows
    """
    sample = df.head(sample_rows).to_dict(orient="records")
    sanitized_sample = [
        {k: _truncate_value(v, max_chars=max_cell_chars) for k, v in row.items()}
        for row in sample
    ]

    return {
        "columns": df.columns.tolist(),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "sample_rows": sanitized_sample,
        "row_count": len(df),
    }


# ─── Safe Code Execution ──────────────────────────────────────────────────────

# Patterns that are never allowed in executed code
_DANGEROUS_PATTERNS = [
    r"\bos\b", r"\bsubprocess\b", r"\bopen\b", r"\beval\b",
    r"\bexec\b", r"\b__import__\b", r"\bshutil\b", r"\bsys\b",
    r"\brm\b", r"\bdel\b", r"import\s+os", r"import\s+sys",
    r"import\s+subprocess", r"import\s+shutil",
]


def _is_safe_code(code: str) -> bool:
    """
    Basic security filter — reject code containing dangerous patterns.
    This is a best-effort guard; do NOT rely on it as the sole security measure.
    """
    for pattern in _DANGEROUS_PATTERNS:
        if re.search(pattern, code):
            return False
    return True


def execute_query_code(code: str, df: pd.DataFrame) -> Any:
    """
    Safely execute AI-generated pandas code against the provided DataFrame.

    Expects the code to assign its final result to a variable named `result`.
    Returns the value of `result` after execution.

    Raises:
      - ValueError  : if the code contains dangerous patterns
      - RuntimeError: if execution fails or `result` is not defined
    """
    if not _is_safe_code(code):
        raise ValueError(
            "The generated code contains unsafe operations and was blocked."
        )

    # Restricted execution namespace — only pandas and the DataFrame are exposed
    exec_globals = {"pd": pd, "df": df.copy()}

    stdout_buffer = StringIO()
    stderr_buffer = StringIO()

    try:
        with contextlib.redirect_stdout(stdout_buffer), contextlib.redirect_stderr(stderr_buffer):
            exec(code, exec_globals)  # noqa: S102
    except Exception:
        raise RuntimeError(
            f"Code execution failed:\n{traceback.format_exc()}"
        )

    # If the LLM forgot to assign `result`, fall back to captured output.
    # This makes responses like `print(df.info())` still useful.
    if "result" not in exec_globals:
        captured = (stdout_buffer.getvalue() or "").strip()
        if captured:
            return captured[:50000]
        captured_err = (stderr_buffer.getvalue() or "").strip()
        if captured_err:
            return captured_err[:50000]
        raise RuntimeError(
            "The generated code did not define a `result` variable."
        )

    def _to_jsonable(value: Any) -> Any:
        """Best-effort conversion of objects to JSON-serializable Python types.

        This is needed because LLM-generated code can return numpy/pandas objects
        (e.g. `df.dtypes`) that contain `numpy.dtype` values which Pydantic can't
        serialize.
        """

        if value is None:
            return None

        # Pandas missing values
        if value is pd.NA:
            return None

        # Fast path for common primitives
        if isinstance(value, (str, int, bool)):
            return value
        if isinstance(value, float):
            return None if math.isnan(value) else value

        # numpy scalars + numpy dtypes
        if isinstance(value, np.dtype):
            return str(value)
        if isinstance(value, np.generic):
            try:
                return _to_jsonable(value.item())
            except Exception:
                return str(value)

        # pandas timestamp-like
        if isinstance(value, pd.Timestamp):
            return value.isoformat()

        # pandas structures
        if isinstance(value, pd.DataFrame):
            return _to_jsonable(value.to_dict(orient="records"))
        if isinstance(value, pd.Series):
            return _to_jsonable(value.to_dict())

        # collections
        if isinstance(value, dict):
            return {str(k): _to_jsonable(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [_to_jsonable(v) for v in value]

        # fallback
        return str(value)

    # Convert pandas objects to plain Python and ensure JSON serialisation
    result = exec_globals["result"]
    jsonable = _to_jsonable(result)

    # If result is empty-ish but there is useful printed output, include it.
    # (Common when the model uses print() for summaries.)
    captured = (stdout_buffer.getvalue() or "").strip()
    if (jsonable is None or jsonable == "") and captured:
        return captured[:50000]

    return jsonable


def build_dataset_overview(
    df: pd.DataFrame,
    *,
    max_sample_rows: int = 10,
    max_value_counts_cols: int = 6,
    max_value_counts: int = 8,
) -> dict:
    """Create a bounded, JSON-friendly dataset overview using pandas.

    This is used for questions like "describe the dataset" or "give an overview".
    It avoids relying on the LLM for basic exploratory analysis.
    """

    row_count = int(len(df))
    col_count = int(df.shape[1])

    missing = df.isnull().sum()
    missing_by_column = missing.to_dict()
    missing_top = (
        missing[missing > 0]
        .sort_values(ascending=False)
        .head(20)
        .to_dict()
    )

    duplicate_rows = int(df.duplicated().sum())

    dtypes = {col: str(dtype) for col, dtype in df.dtypes.items()}

    # Per-column summary (bounded, JSON-friendly)
    non_null = df.notnull().sum().to_dict()
    columns_summary: list[dict[str, Any]] = []
    for col in df.columns.tolist():
        dtype_str = dtypes.get(col, "")
        null_count = int(missing_by_column.get(col, 0))
        non_null_count = int(non_null.get(col, 0))

        col_summary: dict[str, Any] = {
            "name": col,
            "dtype": dtype_str,
            "non_null": non_null_count,
            "nulls": null_count,
        }

        # Unique/top for non-numeric columns
        if col in df.select_dtypes(exclude=["number"]).columns:
            try:
                series = df[col]
                col_summary["unique"] = int(series.nunique(dropna=True))
                vc = series.astype(str).value_counts(dropna=False)
                if not vc.empty:
                    top_value = str(vc.index[0])
                    col_summary["top"] = _truncate_value(top_value, max_chars=200)
                    col_summary["top_count"] = int(vc.iloc[0])
            except Exception:
                pass
        else:
            # Numeric columns: basic stats (small)
            try:
                s = pd.to_numeric(df[col], errors="coerce")
                if s.notna().any():
                    col_summary["min"] = float(s.min())
                    col_summary["max"] = float(s.max())
                    col_summary["mean"] = float(s.mean())
            except Exception:
                pass

        columns_summary.append(col_summary)

    # Numeric summary
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    numeric_summary = None
    if numeric_cols:
        desc = df[numeric_cols].describe().transpose()
        # Round to keep payload compact
        numeric_summary = desc.round(4).to_dict(orient="index")

    # Categorical top values (bounded)
    cat_cols = [
        c for c in df.columns
        if c not in numeric_cols
    ]
    value_counts: dict[str, list[dict[str, Any]]] = {}
    for col in cat_cols[:max_value_counts_cols]:
        try:
            vc = (
                df[col]
                .astype(str)
                .value_counts(dropna=False)
                .head(max_value_counts)
            )
            value_counts[col] = [
                {"value": str(idx), "count": int(cnt)} for idx, cnt in vc.items()
            ]
        except Exception:
            # Some columns might be unhashable (lists/dicts); skip.
            continue

    sample_rows = df.head(max_sample_rows).to_dict(orient="records")
    # Reuse existing truncation to keep payload safe
    sample_rows = [
        {k: _truncate_value(v, max_chars=200) for k, v in row.items()}
        for row in sample_rows
    ]

    # A human-readable response + insights for the UI
    insights: list[str] = []
    insights.append(f"Rows: {row_count:,} | Columns: {col_count:,}.")
    if missing_top:
        worst_col, worst_missing = next(iter(missing_top.items()))
        insights.append(f"Missing values present. Most missing: '{worst_col}' ({int(worst_missing):,}).")
    else:
        insights.append("No missing values detected.")
    if duplicate_rows:
        insights.append(f"Duplicate rows: {duplicate_rows:,}.")

    response_lines = [
        f"Dataset overview:",
        f"- Rows: {row_count:,}",
        f"- Columns: {col_count:,}",
        f"- Duplicates: {duplicate_rows:,}",
    ]
    if missing_top:
        response_lines.append(f"- Columns with missing values: {len(missing_top)} (showing up to 20)")
    else:
        response_lines.append("- Missing values: none")
    if numeric_cols:
        response_lines.append(f"- Numeric columns: {len(numeric_cols)}")
    if value_counts:
        response_lines.append(f"- Top values computed for {len(value_counts)} categorical/text columns")

    return {
        "response": "\n".join(response_lines),
        "insights": insights,
        "table_data": sample_rows,
        "table_title": "Sample rows",
        "overview": {
            "row_count": row_count,
            "column_count": col_count,
            "dtypes": dtypes,
            "columns": columns_summary,
            "missing_by_column": missing_by_column,
            "missing_top": missing_top,
            "duplicate_rows": duplicate_rows,
            "numeric_summary": numeric_summary,
            "value_counts": value_counts,
        },
    }
