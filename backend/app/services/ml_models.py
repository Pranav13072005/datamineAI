from __future__ import annotations

from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd


def detect_anomalies(df: pd.DataFrame) -> dict[str, Any]:
    """Detect anomalous rows using IsolationForest over numeric columns.

    Requirements:
    - Numeric columns only
    - Drop numeric cols with >50% missing
    - Impute missing with column median
    - Fit IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    - Return JSON-safe output
    """

    def _json_safe(obj: Any) -> Any:
        if obj is None:
            return None
        if obj is pd.NA:
            return None
        if isinstance(obj, float) and np.isnan(obj):
            return None
        if isinstance(obj, (np.floating, np.integer, np.bool_)):
            return obj.item()
        if isinstance(obj, (np.ndarray,)):
            return [_json_safe(x) for x in obj.tolist()]
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        if isinstance(obj, (date,)):
            return obj.isoformat()
        if isinstance(obj, (bytes, bytearray)):
            return obj.decode("utf-8", errors="replace")
        if isinstance(obj, dict):
            return {str(k): _json_safe(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [_json_safe(v) for v in obj]
        if isinstance(obj, (str, int, bool)):
            return obj
        if isinstance(obj, float):
            return float(obj)
        return str(obj)

    nrows = int(df.shape[0])
    if nrows < 10:
        return _json_safe({"anomaly_count": 0, "skipped": True, "reason": "Fewer than 10 rows"})

    numeric_df = df.select_dtypes(include=["number"])
    if numeric_df.shape[1] == 0:
        return _json_safe({"anomaly_count": 0, "skipped": True, "reason": "No numeric columns"})

    # Drop columns with >50% missing
    missing_frac = numeric_df.isna().mean(axis=0)
    keep_cols = missing_frac[missing_frac <= 0.5].index
    if len(keep_cols) == 0:
        return _json_safe(
            {"anomaly_count": 0, "skipped": True, "reason": "All numeric columns have >50% missing"}
        )

    numeric_df = numeric_df.loc[:, keep_cols]

    # Impute with median (vectorized)
    medians = numeric_df.median(axis=0, skipna=True)
    X_df = numeric_df.fillna(medians)

    # IsolationForest operates on numpy arrays
    X = X_df.to_numpy(dtype=np.float32, copy=False)

    try:
        from sklearn.ensemble import IsolationForest
    except Exception as exc:  # pragma: no cover
        return _json_safe({"anomaly_count": 0, "skipped": True, "reason": f"scikit-learn unavailable: {exc}"})

    iso = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    iso.fit(X)

    preds = iso.predict(X)  # 1 normal, -1 anomaly
    anomaly_mask = preds == -1
    anomaly_count = int(np.sum(anomaly_mask))
    anomaly_pct = float((anomaly_count / float(nrows)) * 100.0) if nrows else 0.0

    columns_used = [str(c) for c in X_df.columns]
    if anomaly_count == 0:
        return _json_safe(
            {
                "anomaly_count": anomaly_count,
                "anomaly_pct": anomaly_pct,
                "anomaly_rows": [],
                "columns_used": columns_used,
            }
        )

    # Use score_samples: lower = more anomalous
    raw_scores = iso.score_samples(X)
    # Select top 20 most anomalous among predicted anomalies
    anomaly_indices = np.flatnonzero(anomaly_mask)
    anomaly_raw = raw_scores[anomaly_indices]
    k = int(min(20, anomaly_indices.shape[0]))
    # argsort on just anomalies (k <= anomaly_count)
    topk_order = np.argsort(anomaly_raw)[:k]
    top_rows = anomaly_indices[topk_order]

    # Compute z-scores for the selected rows only
    mean = np.mean(X, axis=0, dtype=np.float64)
    std = np.std(X, axis=0, dtype=np.float64)
    std_safe = np.where(std == 0.0, 1.0, std)

    X_top = X[top_rows].astype(np.float64, copy=False)
    z = (X_top - mean) / std_safe

    abs_z = np.abs(z)

    if abs_z.shape[1] >= 2:
        # Top 2 columns by absolute deviation for each row
        top2_idx = np.argpartition(abs_z, -2, axis=1)[:, -2:]
        # sort the two indices by descending abs_z
        row_arange = np.arange(top2_idx.shape[0])[:, None]
        top2_sorted = top2_idx[
            row_arange,
            np.argsort(abs_z[row_arange, top2_idx], axis=1)[:, ::-1],
        ]
    else:
        top2_sorted = np.zeros((abs_z.shape[0], 2), dtype=int)

    anomaly_rows: list[dict[str, Any]] = []
    cols = np.asarray(columns_used, dtype=object)
    for i in range(k):
        idx0 = int(top2_sorted[i, 0])
        idx1 = int(top2_sorted[i, 1]) if abs_z.shape[1] > 1 else idx0
        col_a = str(cols[idx0])
        col_b = str(cols[idx1])
        reason = f"Most extreme: {col_a}, {col_b}" if col_a != col_b else f"Most extreme: {col_a}"  # noqa: E501

        scores_dict = {str(cols[j]): float(z[i, j]) for j in range(z.shape[1])}
        anomaly_rows.append({"row_index": int(top_rows[i]), "scores": scores_dict, "reason": reason})

    result: dict[str, Any] = {
        "anomaly_count": anomaly_count,
        "anomaly_pct": anomaly_pct,
        "anomaly_rows": anomaly_rows,
        "columns_used": columns_used,
    }
    return _json_safe(result)


def cluster_dataset(df: pd.DataFrame, n_clusters: int = 4) -> dict[str, Any]:
    """Cluster a dataset using KMeans over numeric columns.

    Requirements:
    1. Select numeric cols, impute median, standard-scale
    2. If fewer than 3 numeric cols or fewer than 50 rows: return {skipped: true, reason: str}
    3. Auto-select k using elbow method (try k=2..6, pick lowest k where inertia drop < 20%)
    4. Fit KMeans
    5. For each cluster, compute mean of each original column and produce a label
    6. Return JSON-safe output
    """

    def _json_safe(obj: Any) -> Any:
        if obj is None:
            return None
        if obj is pd.NA:
            return None
        if isinstance(obj, float) and np.isnan(obj):
            return None
        if isinstance(obj, (np.floating, np.integer, np.bool_)):
            return obj.item()
        if isinstance(obj, (np.ndarray,)):
            return [_json_safe(x) for x in obj.tolist()]
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        if isinstance(obj, (date,)):
            return obj.isoformat()
        if isinstance(obj, (bytes, bytearray)):
            return obj.decode("utf-8", errors="replace")
        if isinstance(obj, dict):
            return {str(k): _json_safe(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [_json_safe(v) for v in obj]
        if isinstance(obj, (str, int, bool)):
            return obj
        if isinstance(obj, float):
            return float(obj)
        return str(obj)

    nrows = int(df.shape[0])
    if nrows < 50:
        return _json_safe({"skipped": True, "reason": "Fewer than 50 rows"})

    numeric_df = df.select_dtypes(include=["number"])
    if numeric_df.shape[1] < 3:
        return _json_safe({"skipped": True, "reason": "Fewer than 3 numeric columns"})

    # Median impute (vectorized)
    medians = numeric_df.median(axis=0, skipna=True)
    X_df = numeric_df.fillna(medians)

    columns_used = [str(c) for c in X_df.columns]

    X = X_df.to_numpy(dtype=np.float32, copy=False)

    # Standard scale using numpy (avoids extra sklearn pass over data)
    mean = np.mean(X, axis=0, dtype=np.float64)
    std = np.std(X, axis=0, dtype=np.float64)
    std_safe = np.where(std == 0.0, 1.0, std).astype(np.float64, copy=False)
    X_scaled = ((X.astype(np.float64, copy=False) - mean) / std_safe).astype(np.float32, copy=False)

    try:
        from sklearn.cluster import KMeans
    except Exception as exc:  # pragma: no cover
        return _json_safe({"skipped": True, "reason": f"scikit-learn unavailable: {exc}"})

    # Elbow selection on a sample for efficiency on large datasets.
    # We still compute final labels for all rows.
    rng = np.random.RandomState(42)
    sample_size = int(min(100_000, nrows))
    if sample_size < nrows:
        sample_idx = rng.choice(nrows, size=sample_size, replace=False)
        X_for_fit = X_scaled[sample_idx]
    else:
        X_for_fit = X_scaled

    max_k = int(min(6, sample_size - 1))
    if max_k < 2:
        return _json_safe({"skipped": True, "reason": "Not enough rows to fit k=2"})

    k_min = 2
    k_max = max_k
    inertias: list[float] = []
    ks = list(range(k_min, k_max + 1))
    for k in ks:
        km = KMeans(n_clusters=k, random_state=42, n_init="auto")
        km.fit(X_for_fit)
        inertias.append(float(km.inertia_))

    selected_k: int | None = None
    for i in range(1, len(inertias)):
        prev = inertias[i - 1]
        curr = inertias[i]
        if prev <= 0:
            continue
        drop_frac = (prev - curr) / prev
        # Pick the lowest k where improvement becomes small.
        # (k corresponds to ks[i], comparing inertia(k-1) -> inertia(k))
        if drop_frac < 0.20:
            selected_k = ks[i]
            break

    if selected_k is None:
        # If the elbow threshold never triggers, fall back to the requested k if valid,
        # otherwise use the maximum tried.
        requested = int(n_clusters)
        if 2 <= requested <= k_max:
            selected_k = requested
        else:
            selected_k = k_max

    # Fit final model (on the same fit set for speed), then predict for all rows.
    # For large N, this is dramatically faster while still producing consistent clusters.
    km_final = KMeans(n_clusters=selected_k, random_state=42, n_init="auto")
    km_final.fit(X_for_fit)
    labels = km_final.predict(X_scaled)
    labels = labels.astype(np.int32, copy=False)

    # Cluster sizes
    sizes = np.bincount(labels, minlength=selected_k).astype(np.int64, copy=False)
    cluster_sizes: dict[str, int] = {str(i): int(sizes[i]) for i in range(selected_k)}

    # Cluster profiles: mean of original (imputed) numeric columns
    sums = np.zeros((selected_k, X.shape[1]), dtype=np.float64)
    np.add.at(sums, labels, X.astype(np.float64, copy=False))
    counts = sizes.astype(np.float64, copy=False)
    counts_safe = np.where(counts == 0.0, 1.0, counts)
    means_by_cluster = (sums / counts_safe[:, None]).astype(np.float64, copy=False)

    overall_mean = np.mean(X.astype(np.float64, copy=False), axis=0, dtype=np.float64)
    denom = np.maximum(np.abs(overall_mean), 1e-9)
    rel_dev = (means_by_cluster - overall_mean[None, :]) / denom[None, :]

    def _human(col: str) -> str:
        return col.replace("_", " ").replace("-", " ").strip().lower()

    cluster_labels: dict[str, str] = {}
    cluster_profiles: dict[str, dict[str, float]] = {}
    for cid in range(selected_k):
        profile = {columns_used[j]: float(means_by_cluster[cid, j]) for j in range(len(columns_used))}
        cluster_profiles[str(cid)] = profile

        dev = rel_dev[cid]
        pos_idx = int(np.argmax(dev))
        neg_idx = int(np.argmin(dev))

        pos_col = _human(columns_used[pos_idx])
        neg_col = _human(columns_used[neg_idx])

        parts: list[str] = []
        if pos_idx != neg_idx:
            parts.append(f"High {pos_col}")
            parts.append(f"low {neg_col}")
        else:
            parts.append(f"Distinct {pos_col}")
        cluster_labels[str(cid)] = ", ".join(parts)

    return _json_safe(
        {
            "n_clusters": int(selected_k),
            "cluster_sizes": cluster_sizes,
            "cluster_labels": cluster_labels,
            "cluster_profiles": cluster_profiles,
            "columns_used": columns_used,
        }
    )


def forecast_series(df: pd.DataFrame) -> dict[str, Any]:
    """Forecast a time series using ARIMA(1,1,1).

    Logic:
    1. Detect a datetime column (first column that can be parsed by pd.to_datetime).
    2. Pick a numeric target (highest variance, then lowest null count).
    3. Sort by datetime, set index, fill missing by linear interpolation.
    4. Fit statsmodels ARIMA(1,1,1), forecast 10 periods ahead using detected frequency.

    Returns JSON-safe output; returns {skipped: true, reason: str} on failure.
    """

    def _json_safe(obj: Any) -> Any:
        if obj is None:
            return None
        if obj is pd.NA:
            return None
        if isinstance(obj, float) and np.isnan(obj):
            return None
        if isinstance(obj, (np.floating, np.integer, np.bool_)):
            return obj.item()
        if isinstance(obj, (np.ndarray,)):
            return [_json_safe(x) for x in obj.tolist()]
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        if isinstance(obj, (date,)):
            return obj.isoformat()
        if isinstance(obj, (bytes, bytearray)):
            return obj.decode("utf-8", errors="replace")
        if isinstance(obj, dict):
            return {str(k): _json_safe(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [_json_safe(v) for v in obj]
        if isinstance(obj, (str, int, bool)):
            return obj
        if isinstance(obj, float):
            return float(obj)
        return str(obj)

    nrows = int(df.shape[0])
    if nrows < 5:
        return _json_safe({"skipped": True, "reason": "Not enough rows"})

    # 1) Detect datetime column
    date_col: str | None = None
    parsed_dates: pd.Series | None = None
    for col in df.columns:
        s = df[col]
        if pd.api.types.is_datetime64_any_dtype(s):
            parsed = pd.to_datetime(s, errors="coerce", utc=False)
        else:
            # Avoid treating plain numeric columns as timestamps (pandas will interpret ints as ns since epoch).
            if not (pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s)):
                continue
            parsed = pd.to_datetime(s, errors="coerce", utc=False)

        non_na = int(parsed.notna().sum())
        if non_na < max(10, int(0.5 * nrows)):
            continue
        # Require some variability so constants like "2020" don't pass too easily.
        if int(parsed.dropna().nunique()) < 3:
            continue
        date_col = str(col)
        parsed_dates = parsed
        break

    if not date_col or parsed_dates is None:
        return _json_safe({"skipped": True, "reason": "No datetime column detected"})

    # 2) Find best numeric target: highest variance, then lowest null count
    numeric_df = df.select_dtypes(include=["number"])
    if numeric_df.shape[1] == 0:
        return _json_safe({"skipped": True, "reason": "No numeric columns found"})

    variances = numeric_df.var(axis=0, skipna=True)
    null_counts = numeric_df.isna().sum(axis=0)
    candidates = pd.DataFrame({"var": variances, "nulls": null_counts})
    candidates = candidates.replace([np.inf, -np.inf], np.nan).dropna(subset=["var"])
    if candidates.empty:
        return _json_safe({"skipped": True, "reason": "No numeric target with finite variance"})

    candidates = candidates.sort_values(by=["var", "nulls"], ascending=[False, True])
    target_col = str(candidates.index[0])

    # 3-4) Build series, sort, index, aggregate duplicates, infer frequency
    try:
        work = pd.DataFrame({"__date": parsed_dates, "__y": numeric_df[target_col]})
        work = work.dropna(subset=["__date"])
        if work.shape[0] < 10:
            return _json_safe({"skipped": True, "reason": "Too few valid datetime rows"})

        work = work.sort_values("__date")
        # Aggregate duplicate timestamps (mean of target)
        series = work.groupby("__date", sort=True)["__y"].mean()
        series = series.sort_index()
        if series.shape[0] < 10:
            return _json_safe({"skipped": True, "reason": "Too few unique datetime points"})

        inferred = None
        try:
            inferred = pd.infer_freq(series.index)
        except Exception:
            inferred = None

        def _freq_label(freq_str: str | None, median_delta: pd.Timedelta | None) -> str:
            if freq_str:
                f = freq_str.upper()
                if f.endswith("H") or f == "H":
                    return "hourly"
                if f.endswith("D") or f == "D":
                    return "daily"
                if f.endswith("W") or f.startswith("W-"):
                    return "weekly"
                if "M" in f and (f == "M" or f.endswith("MS") or f.endswith("ME") or f.endswith("M")):
                    return "monthly"
                if "Q" in f:
                    return "quarterly"
                if "A" in f or "Y" in f:
                    return "yearly"
            if median_delta is None:
                return "unknown"
            seconds = float(median_delta.total_seconds())
            day = 24.0 * 3600.0
            if abs(seconds - 3600.0) / 3600.0 < 0.25:
                return "hourly"
            if abs(seconds - day) / day < 0.25:
                return "daily"
            if abs(seconds - 7 * day) / (7 * day) < 0.25:
                return "weekly"
            if abs(seconds - 30 * day) / (30 * day) < 0.35:
                return "monthly"
            if abs(seconds - 365 * day) / (365 * day) < 0.35:
                return "yearly"
            return "unknown"

        median_delta: pd.Timedelta | None = None
        if inferred is None and series.shape[0] >= 3:
            diffs = series.index.to_series().diff().dropna()
            if not diffs.empty:
                median_delta = diffs.median()

        # Regularize index if we have a usable frequency
        if inferred is not None:
            full_index = pd.date_range(start=series.index[0], end=series.index[-1], freq=inferred)
            series = series.reindex(full_index)
        # Fill missing with linear interpolation (time-aware where possible)
        series = series.astype(float)
        if series.isna().any():
            try:
                series = series.interpolate(method="time")
            except Exception:
                series = series.interpolate(method="linear")
            series = series.ffill().bfill()

        # For very large series, cap training window for practicality.
        if series.shape[0] > 50_000:
            series_fit = series.iloc[-50_000:]
        else:
            series_fit = series

        try:
            from statsmodels.tsa.arima.model import ARIMA
        except Exception as exc:  # pragma: no cover
            return _json_safe({"skipped": True, "reason": f"statsmodels unavailable: {exc}"})

        model = ARIMA(series_fit, order=(1, 1, 1))
        res = model.fit()

        steps = 10
        fc = res.get_forecast(steps=steps)
        pred = fc.predicted_mean
        ci = fc.conf_int(alpha=0.05)

        # Build future dates
        last_date = series.index[-1]
        if inferred is not None:
            future_index = pd.date_range(start=last_date, periods=steps + 1, freq=inferred)[1:]
        else:
            if median_delta is None:
                median_delta = pd.Timedelta(days=1)
            future_index = pd.DatetimeIndex([last_date + median_delta * (i + 1) for i in range(steps)])

        # Historical last 30 points
        hist_tail = series.tail(30)
        historical = [{"date": d.isoformat(), "value": float(v)} for d, v in hist_tail.items()]

        lower_col = ci.columns[0] if hasattr(ci, "columns") else 0
        upper_col = ci.columns[1] if hasattr(ci, "columns") else 1

        forecast: list[dict[str, Any]] = []
        for i in range(steps):
            v = float(pred.iloc[i])
            lower = float(ci.iloc[i][lower_col])
            upper = float(ci.iloc[i][upper_col])
            forecast.append(
                {
                    "date": future_index[i].isoformat(),
                    "value": v,
                    "lower": lower,
                    "upper": upper,
                }
            )

        frequency = _freq_label(inferred, median_delta)

        return _json_safe(
            {
                "date_column": date_col,
                "target_column": target_col,
                "historical": historical,
                "forecast": forecast,
                "model": "ARIMA(1,1,1)",
                "frequency": frequency,
            }
        )
    except Exception as exc:
        return _json_safe({"skipped": True, "reason": f"Forecasting failed: {exc}"})
