"""
data_service.py — Dataset management and query execution service.

Responsibilities:
  - Save uploaded CSV files to the local filesystem.
  - Load a dataset from disk by its dataset_id.
  - Extract schema (columns, dtypes, sample rows) for the LLM prompt.
  - Safely execute AI-generated Python/pandas code against a DataFrame.
"""

import io
import os
import re
import traceback
from typing import Any

import pandas as pd

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


# ─── Schema Extraction ────────────────────────────────────────────────────────

def get_schema(df: pd.DataFrame, sample_rows: int = 3) -> dict:
    """
    Build a schema dict describing the DataFrame:
      - columns     : list of column names
      - dtypes      : column → dtype string
      - sample_rows : first N rows as list of dicts (JSON-serializable)
      - row_count   : total number of rows
    """
    return {
        "columns": df.columns.tolist(),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "sample_rows": df.head(sample_rows).to_dict(orient="records"),
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

    try:
        exec(code, exec_globals)  # noqa: S102
    except Exception:
        raise RuntimeError(
            f"Code execution failed:\n{traceback.format_exc()}"
        )

    if "result" not in exec_globals:
        raise RuntimeError(
            "The generated code did not define a `result` variable."
        )

    # Convert pandas objects to plain Python for JSON serialisation
    result = exec_globals["result"]
    if isinstance(result, pd.DataFrame):
        return result.to_dict(orient="records")
    if isinstance(result, pd.Series):
        return result.to_dict()
    return result
