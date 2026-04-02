from __future__ import annotations

from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd


def extract_insights(df: pd.DataFrame) -> dict[str, Any]:
    """Compute fast, JSON-serialisable dataset insights.

    Notes:
    - Uses vectorized pandas operations only (no row-wise loops).
    - Designed to run quickly on ~100k rows; most work is O(n_rows * n_cols)
      and correlations are O(n_numeric_cols^2).
    """

    def _json_safe(obj: Any) -> Any:
        """Recursively convert objects to JSON-safe Python primitives."""
        if obj is None:
            return None

        # Pandas missing scalars (NaT/NA) and numpy NaN
        if obj is pd.NA:
            return None
        if isinstance(obj, float) and np.isnan(obj):
            return None
        if isinstance(obj, (np.floating, np.integer, np.bool_)):
            return obj.item()
        if isinstance(obj, (np.ndarray,)):
            return [_json_safe(x) for x in obj.tolist()]
        if isinstance(obj, (pd.Timestamp, datetime)):
            # ISO 8601
            return obj.isoformat()
        if isinstance(obj, (date,)):
            return obj.isoformat()
        if isinstance(obj, (bytes, bytearray)):
            return obj.decode("utf-8", errors="replace")

        if isinstance(obj, dict):
            return {str(k): _json_safe(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [_json_safe(v) for v in obj]

        # Python built-in primitives
        if isinstance(obj, (str, int, bool)):
            return obj
        if isinstance(obj, float):
            # Already handled NaN above
            return float(obj)

        # Fallback: stringify unknown types to preserve serialisability
        return str(obj)

    nrows, ncols = df.shape

    # Missing values
    if nrows == 0:
        missing_counts = df.isna().sum()
        missing_pct = pd.Series(0.0, index=df.columns)
    else:
        missing_counts = df.isna().sum()
        missing_pct = (missing_counts / float(nrows)) * 100.0

    missing_df = (
        pd.DataFrame({"column": df.columns, "count": missing_counts.values, "pct": missing_pct.values})
        .query("count > 0")
        .sort_values("pct", ascending=False)
    )

    missing: list[dict[str, Any]] = missing_df.to_dict(orient="records")
    high_missing: list[str] = missing_df.loc[missing_df["pct"] > 30.0, "column"].astype(str).tolist()

    # Duplicates (row-wise)
    if nrows == 0:
        dup_count = 0
        dup_pct = 0.0
    else:
        dup_count = int(df.duplicated(keep="first").sum())
        dup_pct = (dup_count / float(nrows)) * 100.0

    # Correlations (numeric columns only)
    numeric_df = df.select_dtypes(include=["number"])
    correlations: list[dict[str, Any]] = []
    if numeric_df.shape[1] >= 2 and nrows >= 2:
        corr = numeric_df.corr(method="pearson")
        # Upper triangle without diagonal
        mask = np.triu(np.ones(corr.shape, dtype=bool), k=1)
        corr_pairs = corr.where(mask).stack()
        corr_pairs = corr_pairs[corr_pairs.abs() > 0.5]
        if not corr_pairs.empty:
            top_abs = corr_pairs.abs().nlargest(5)
            top = corr_pairs.loc[top_abs.index]
            correlations = [
                {"col_a": str(a), "col_b": str(b), "r": float(r)}
                for (a, b), r in top.items()
            ]

    # Distribution flags
    distribution_flags: list[dict[str, Any]] = []

    nunique_all = df.nunique(dropna=False)

    # constant / possible_id (all dtypes)
    if nrows > 0:
        constant_cols = nunique_all[nunique_all == 1].index.astype(str).tolist()
        for col in constant_cols:
            distribution_flags.append(
                {"column": col, "flag": "constant", "detail": {"nunique": 1}}
            )

        possible_id_cols = nunique_all[nunique_all == nrows].index.astype(str).tolist()
        for col in possible_id_cols:
            distribution_flags.append(
                {"column": col, "flag": "possible_id", "detail": {"nunique": int(nrows)}}
            )

    # low_cardinality (object dtype only)
    if nrows > 0 and ncols > 0:
        object_cols = [c for c in df.columns if df[c].dtype == object]
        if object_cols:
            nunique_obj = df[object_cols].nunique(dropna=False)
            low_card = nunique_obj[nunique_obj < 10].sort_values().index.astype(str).tolist()
            for col in low_card:
                distribution_flags.append(
                    {
                        "column": col,
                        "flag": "low_cardinality",
                        "detail": {"nunique": int(nunique_obj[col])},
                    }
                )

    # high_skew (numeric)
    if numeric_df.shape[1] > 0 and nrows >= 3:
        skew = numeric_df.skew(numeric_only=True)
        skew = skew.replace([np.inf, -np.inf], np.nan).dropna()
        skew_cols = skew[skew.abs() > 2.0].sort_values(key=lambda s: s.abs(), ascending=False)
        for col, value in skew_cols.items():
            distribution_flags.append(
                {
                    "column": str(col),
                    "flag": "high_skew",
                    "detail": {"skew": float(value)},
                }
            )

    # Numeric summary
    numeric_summary: list[dict[str, Any]] = []
    if numeric_df.shape[1] > 0:
        summary = numeric_df.agg(["mean", "std", "min", "max", "median"]).T
        summary = summary.replace([np.inf, -np.inf], np.nan)
        # Iterate over columns (cheap; no row-wise loop over data)
        for col in summary.index:
            row = summary.loc[col]
            numeric_summary.append(
                {
                    "col": str(col),
                    "mean": None if pd.isna(row["mean"]) else float(row["mean"]),
                    "std": None if pd.isna(row["std"]) else float(row["std"]),
                    "min": None if pd.isna(row["min"]) else float(row["min"]),
                    "max": None if pd.isna(row["max"]) else float(row["max"]),
                    "median": None if pd.isna(row["median"]) else float(row["median"]),
                }
            )

    # Sample rows (first 5)
    sample_rows = df.head(5).where(df.head(5).notna(), None).to_dict(orient="records")

    result: dict[str, Any] = {
        "shape": {"rows": int(nrows), "cols": int(ncols)},
        "missing": missing,
        "high_missing": high_missing,
        "duplicates": {"count": int(dup_count), "pct": float(dup_pct)},
        "correlations": correlations,
        "distribution_flags": distribution_flags,
        "numeric_summary": numeric_summary,
        "sample_rows": sample_rows,
    }

    return _json_safe(result)
