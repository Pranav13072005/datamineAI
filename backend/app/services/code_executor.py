from __future__ import annotations

import math
import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pandas as pd


_FORBIDDEN_SUBSTRINGS: tuple[str, ...] = (
    "import os",
    "import sys",
    "import subprocess",
    "open(",
    "exec(",
    "eval(",
    "__import__",
    "requests",
    "http",
    "socket",
)


_IMPORT_STMT_RE = re.compile(r"(^|\n)\s*(import|from)\s+", flags=re.IGNORECASE)


def execute_pandas_code(code: str, df: pd.DataFrame, timeout_seconds: int = 10) -> dict:
    """Execute pandas code against a dataframe in a restricted namespace.

    Contract:
      - Rejects code containing forbidden substrings.
      - Executes using exec() with only `df`, `pd`, and safe builtins exposed.
      - Enforces a best-effort timeout using ThreadPoolExecutor.
      - Expects the code to set a variable named `result`.
      - Always returns: {"result": <jsonable-or-null>, "error": <str-or-null>}.

    Note: Thread timeouts cannot forcibly stop a running exec(). This is a
    best-effort guard (as requested) and should not be relied upon alone for
    hard isolation.
    """

    if not isinstance(code, str) or not code.strip():
        return {"result": None, "error": "code must be a non-empty string"}

    if not isinstance(timeout_seconds, int) or timeout_seconds <= 0:
        return {"result": None, "error": "timeout_seconds must be a positive integer"}

    # Normalize whitespace to catch cases like "import\n os".
    normalized = re.sub(r"\s+", " ", code.lower())

    # Reject any import statements. Even harmless imports require __import__,
    # which we intentionally do not expose in safe builtins.
    if _IMPORT_STMT_RE.search(code):
        return {"result": None, "error": "blocked unsafe code pattern: import statement"}

    for needle in _FORBIDDEN_SUBSTRINGS:
        if needle in normalized:
            return {"result": None, "error": f"blocked unsafe code pattern: {needle}"}

    def _run_exec() -> dict:
        safe_builtins: dict[str, Any] = {
            "abs": abs,
            "all": all,
            "any": any,
            "bool": bool,
            "dict": dict,
            "enumerate": enumerate,
            "float": float,
            "int": int,
            "isinstance": isinstance,
            "len": len,
            "list": list,
            "map": map,
            "max": max,
            "min": min,
            "print": print,
            "range": range,
            "repr": repr,
            "reversed": reversed,
            "round": round,
            "set": set,
            "sorted": sorted,
            "str": str,
            "sum": sum,
            "tuple": tuple,
            "zip": zip,
        }

        # Restricted namespace (no open/import).
        env: dict[str, Any] = {
            "__builtins__": safe_builtins,
            "pd": pd,
            # Shallow copy to reduce risk of mutating shared dataframe state.
            "df": df.copy(deep=False),
        }

        try:
            exec(code, env, env)  # noqa: S102
        except Exception as e:
            return {"result": None, "error": f"execution failed: {e.__class__.__name__}: {e}"}

        if "result" not in env:
            return {"result": None, "error": "code did not set variable `result`"}

        try:
            return {"result": _to_jsonable(env["result"]), "error": None}
        except Exception as e:
            return {"result": None, "error": f"failed to serialize result: {e.__class__.__name__}: {e}"}

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="pandas-exec") as ex:
        fut = ex.submit(_run_exec)
        try:
            return fut.result(timeout=timeout_seconds)
        except TimeoutError:
            fut.cancel()
            return {"result": None, "error": f"execution timed out after {timeout_seconds}s"}


def _to_jsonable(value: Any) -> Any:
    """Convert pandas/numpy/scalar values to JSON-safe Python types."""

    # Fast-path primitives.
    if value is None or isinstance(value, (str, bool, int)):
        return value

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return float(value)

    # Pandas containers.
    if isinstance(value, pd.DataFrame):
        columns = [str(c) for c in value.columns.tolist()]
        rows = value.to_numpy().tolist()
        return {
            "columns": columns,
            "rows": [[_to_jsonable(cell) for cell in row] for row in rows],
        }

    if isinstance(value, pd.Series):
        return [_to_jsonable(v) for v in value.to_list()]

    # Pandas NA/NaT and numpy scalars/arrays if numpy is installed.
    try:
        import numpy as np  # type: ignore

        if isinstance(value, np.dtype):
            return str(value)

        if isinstance(value, np.generic):
            return _to_jsonable(value.item())

        if isinstance(value, np.ndarray):
            return _to_jsonable(value.tolist())
    except Exception:
        pass

    # Generic containers.
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(v) for v in value]

    # Best-effort NaN/NA handling for scalar-like objects.
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    # Last resort: stringify.
    return str(value)
