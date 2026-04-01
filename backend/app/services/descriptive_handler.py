from __future__ import annotations

from typing import Any

import pandas as pd

from app.schemas.query import QueryResponse, TableResult


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def handle_descriptive(df: pd.DataFrame, question: str) -> QueryResponse:
    """Return a deterministic descriptive summary for a dataset.

    - No LLM calls
    - Fast and always succeeds
    - Uses df.describe(include='all') for summary
    - Counts missing values per column
    - Detects duplicate rows
    """

    warnings: list[str] = []
    insights: list[str] = []

    try:
        row_count = _safe_int(len(df))
        col_count = _safe_int(df.shape[1] if hasattr(df, "shape") else 0)

        if row_count == 0 or col_count == 0:
            return QueryResponse(
                answer="This dataset appears to be empty (no rows or columns), so there’s nothing to summarize.",
                table=None,
                insights=[],
                warnings=[],
                query_type="descriptive",
            )

        # Bound work for very large datasets while remaining deterministic.
        max_rows_for_desc = 50_000
        df_for_desc = df
        if row_count > max_rows_for_desc:
            df_for_desc = df.sample(n=max_rows_for_desc, random_state=0)
            warnings.append(f"sampled {max_rows_for_desc} rows for summary statistics")

        # Summary statistics (used for insights; not shown as the primary table)
        numeric_desc: dict[str, dict[str, Any]] = {}
        try:
            desc = df_for_desc.describe(include="all")
            # For numeric columns, pull a few stable stats.
            for col in desc.columns:
                try:
                    stats = desc[col].to_dict()
                    numeric_desc[str(col)] = stats
                except Exception:
                    continue
        except Exception:
            warnings.append("summary statistics unavailable (describe failed)")

        try:
            missing_by_col = df.isna().sum().to_dict()
        except Exception:
            missing_by_col = {}
            warnings.append("missing-value check failed")

        try:
            duplicate_rows = _safe_int(df.duplicated().sum())
        except Exception:
            duplicate_rows = 0
            warnings.append("duplicate-row check failed")

        total_cells = max(1, row_count * col_count)
        missing_total = _safe_int(sum(_safe_int(v) for v in missing_by_col.values()))
        missing_pct_total = (missing_total / total_cells) * 100.0

        if missing_by_col:
            for col, miss in sorted(missing_by_col.items(), key=lambda kv: _safe_int(kv[1]), reverse=True):
                miss_i = _safe_int(miss)
                if miss_i <= 0:
                    continue
                pct = (miss_i / max(1, row_count)) * 100.0
                if pct >= 1.0:
                    insights.append(
                        f"Column '{col}' has {_safe_float(pct):.1f}% missing values ({miss_i:,} rows)."
                    )
                if len(insights) >= 8:
                    break

        if duplicate_rows > 0:
            insights.append(f"Found {duplicate_rows:,} duplicate rows.")

        # Add a couple of numeric summary insights (bounded)
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        for col in numeric_cols[:5]:
            stats = numeric_desc.get(str(col)) or {}
            mean = stats.get("mean")
            min_v = stats.get("min")
            max_v = stats.get("max")
            if mean is not None and min_v is not None and max_v is not None:
                insights.append(f"'{col}': mean {_safe_float(mean):.3f}, min {_safe_float(min_v):.3f}, max {_safe_float(max_v):.3f}.")

        # Primary table: sample rows (what users expect to see in the UI)
        sample_n = 10
        try:
            sample_df = df.head(sample_n)
            sample_df = sample_df.where(pd.notna(sample_df), None)
            table = TableResult(
                columns=[str(c) for c in sample_df.columns.tolist()],
                rows=sample_df.values.tolist(),
            )
        except Exception:
            table = None
            warnings.append("sample rows unavailable")

        missing_sentence = (
            "No missing values were detected."
            if missing_total == 0
            else f"Missing values account for about {missing_pct_total:.2f}% of all cells ({missing_total:,} total)."
        )
        duplicates_sentence = (
            "No duplicate rows were detected."
            if duplicate_rows == 0
            else f"There are {duplicate_rows:,} duplicate rows that may need cleanup."
        )

        # 2–3 sentence narrative, professional tone.
        answer = (
            f"This dataset contains {row_count:,} rows and {col_count:,} columns. "
            f"I checked basic data quality signals (missing values and duplicates) and summarized the columns. "
            f"{missing_sentence} {duplicates_sentence}"
        )

        return QueryResponse(
            answer=answer,
            table=table,
            chart=None,
            insights=insights,
            warnings=warnings,
            query_type="descriptive",
        )

    except Exception as exc:
        return QueryResponse(
            answer=(
                "I couldn’t generate a full dataset summary due to an unexpected error, "
                "but the service is still running."
            ),
            table=None,
            insights=[],
            warnings=[f"descriptive summary failed: {exc.__class__.__name__}"],
            query_type="descriptive",
        )
