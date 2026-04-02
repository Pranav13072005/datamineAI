from __future__ import annotations

from typing import Any, Literal

from app.schemas.query import ChartSpec, QueryResponse, TableResult


MLQueryType = Literal["anomaly", "clustering", "forecast"]


def handle_ml_query(query_type: str, question: str, fact_cache: dict | None) -> QueryResponse:
    """Serve pre-computed ML results from fact_cache.

    This path is fully deterministic and never calls the LLM.

    Expected cache layout:
      fact_cache["ml"]["anomalies"|"clusters"|"forecast"]
    """

    if query_type not in {"anomaly", "clustering", "forecast"}:
        return QueryResponse(
            answer="I can only answer anomaly, clustering, or forecast questions in this mode.",
            query_type="error",
            warnings=[f"unsupported ml query_type: {query_type}"],
        )

    cache: dict[str, Any] = fact_cache or {}
    ml = cache.get("ml") if isinstance(cache, dict) else None
    if not isinstance(ml, dict):
        return QueryResponse(
            answer=(
                "I’m still computing the dataset insights in the background. "
                "Please try this question again in a few seconds."
            ),
            query_type=query_type,  # type: ignore[arg-type]
            warnings=["fact_cache.ml missing (still processing)"],
        )

    key = {"anomaly": "anomalies", "clustering": "clusters", "forecast": "forecast"}[query_type]
    result = ml.get(key)
    if not isinstance(result, dict):
        return QueryResponse(
            answer=(
                "I don’t have the precomputed ML result for this dataset yet. "
                "Please wait a few seconds and try again."
            ),
            query_type=query_type,  # type: ignore[arg-type]
            warnings=[f"fact_cache.ml.{key} missing"],
        )

    if result.get("skipped") is True:
        reason = str(result.get("reason") or "This analysis was skipped for this dataset.")
        return QueryResponse(
            answer=(
                "I couldn’t run that analysis on this dataset. "
                f"Reason: {reason}"
            ),
            query_type=query_type,  # type: ignore[arg-type]
            insights=[reason],
            warnings=["ml analysis skipped"],
        )

    try:
        if query_type == "anomaly":
            return _format_anomalies(result)
        if query_type == "clustering":
            return _format_clusters(result)
        return _format_forecast(result)
    except Exception as exc:
        return QueryResponse(
            answer=(
                "I had trouble formatting the precomputed ML results for display. "
                f"Error: {exc}"
            ),
            query_type="error",
            warnings=[f"ml formatting failed: {exc.__class__.__name__}: {exc}"],
        )


def _format_anomalies(result: dict[str, Any]) -> QueryResponse:
    count = int(result.get("anomaly_count") or 0)
    pct = float(result.get("anomaly_pct") or 0.0)
    rows = result.get("anomaly_rows") or []

    if not rows:
        answer = (
            f"I scanned the numeric columns for outliers and didn’t find any strong anomalies. "
            f"(Detected anomalies: {count}, {pct:.2f}%.)"
        )
        return QueryResponse(answer=answer, query_type="anomaly", insights=["No anomalies detected."])

    # Build compact table: one row per anomaly
    table_rows: list[list[Any]] = []
    xs: list[int] = []
    ys: list[float] = []
    hover: list[str] = []

    for r in rows:
        try:
            row_index = int(r.get("row_index"))
        except Exception:
            continue
        scores = r.get("scores") or {}
        if not isinstance(scores, dict):
            scores = {}

        # Severity = max abs z-score across columns
        abs_vals = [abs(float(v)) for v in scores.values() if _is_number(v)]
        severity = float(max(abs_vals) if abs_vals else 0.0)

        # Top 2 extreme columns
        pairs = [(str(k), float(v)) for k, v in scores.items() if _is_number(v)]
        pairs.sort(key=lambda kv: abs(kv[1]), reverse=True)
        top1 = pairs[0] if len(pairs) > 0 else ("", 0.0)
        top2 = pairs[1] if len(pairs) > 1 else ("", 0.0)

        reason = str(r.get("reason") or "")

        table_rows.append(
            [
                row_index,
                float(severity),
                top1[0],
                float(top1[1]),
                top2[0],
                float(top2[1]),
                reason,
            ]
        )
        xs.append(row_index)
        ys.append(severity)
        hover.append(reason)

    table = TableResult(
        columns=["row_index", "severity", "col_1", "z_1", "col_2", "z_2", "reason"],
        rows=table_rows,
    )

    chart = ChartSpec(
        type="scatter",
        data={
            "data": [
                {
                    "type": "scatter",
                    "mode": "markers",
                    "x": xs,
                    "y": ys,
                    "text": hover,
                    "hovertemplate": "row=%{x}<br>severity=%{y:.2f}<br>%{text}<extra></extra>",
                    "name": "Anomalies",
                }
            ],
            "layout": {
                "title": "Most anomalous rows (higher = more extreme)",
                "xaxis": {"title": "Row index"},
                "yaxis": {"title": "Severity (max |z|)"},
            },
        },
    )

    answer = (
        f"I found {count} potentially unusual rows ({pct:.2f}%) based on numeric patterns. "
        "The chart highlights the most extreme rows by deviation from the overall distribution."
    )

    insights = [
        f"Anomalies detected: {count} ({pct:.2f}%).",
    ]
    if table_rows:
        insights.append("Most extreme rows are listed in the table (top 20).")

    return QueryResponse(
        answer=answer,
        table=table,
        chart=chart,
        insights=insights,
        query_type="anomaly",
    )


def _format_clusters(result: dict[str, Any]) -> QueryResponse:
    k = int(result.get("n_clusters") or 0)
    sizes = result.get("cluster_sizes") or {}
    labels = result.get("cluster_labels") or {}
    profiles = result.get("cluster_profiles") or {}

    # Table: cluster summary
    rows: list[list[Any]] = []
    x: list[str] = []
    y: list[int] = []

    for cid_str, size in sorted(sizes.items(), key=lambda kv: int(kv[0])):
        label = str(labels.get(cid_str) or f"Cluster {cid_str}")
        rows.append([cid_str, int(size), label])
        x.append(str(cid_str))
        y.append(int(size))

    table = TableResult(columns=["cluster_id", "count", "label"], rows=rows)

    chart = ChartSpec(
        type="bar",
        data={
            "data": [
                {
                    "type": "bar",
                    "x": x,
                    "y": y,
                    "name": "Cluster size",
                }
            ],
            "layout": {"title": "Cluster sizes"},
        },
    )

    answer = (
        f"I clustered the dataset into {k} groups based on numeric similarity. "
        "Each cluster label summarizes how its averages differ from the overall dataset."
    )

    insights: list[str] = [f"Clusters found: {k}."]
    # Add up to 3 labels
    for cid_str, _ in rows[:3]:
        insights.append(f"Cluster {cid_str}: {labels.get(cid_str, '')}".strip())

    # Optional: include a lightweight profile snippet for the largest cluster
    try:
        if isinstance(profiles, dict) and rows:
            largest = max(rows, key=lambda r: int(r[1]))[0]
            prof = profiles.get(largest)
            if isinstance(prof, dict):
                # pick 3 columns to mention
                items = list(prof.items())[:3]
                if items:
                    insights.append(
                        "Largest cluster example means: "
                        + ", ".join([f"{k}={_fmt_num(v)}" for k, v in items])
                        + "."
                    )
    except Exception:
        pass

    return QueryResponse(
        answer=answer,
        table=table,
        chart=chart,
        insights=insights,
        query_type="clustering",
    )


def _format_forecast(result: dict[str, Any]) -> QueryResponse:
    date_col = str(result.get("date_column") or "")
    target_col = str(result.get("target_column") or "")
    freq = str(result.get("frequency") or "")
    hist = result.get("historical") or []
    fc = result.get("forecast") or []

    # Table: forecast points
    table_rows: list[list[Any]] = []
    x_fore: list[str] = []
    y_fore: list[float] = []
    lo: list[float] = []
    hi: list[float] = []

    for p in fc:
        if not isinstance(p, dict):
            continue
        d = str(p.get("date") or "")
        v = float(p.get("value") or 0.0)
        l = float(p.get("lower") or v)
        u = float(p.get("upper") or v)
        table_rows.append([d, v, l, u])
        x_fore.append(d)
        y_fore.append(v)
        lo.append(l)
        hi.append(u)

    table = TableResult(columns=["date", "value", "lower", "upper"], rows=table_rows)

    # Chart: historical + forecast + confidence band
    x_hist = [str(p.get("date")) for p in hist if isinstance(p, dict) and p.get("date") is not None]
    y_hist = [float(p.get("value")) for p in hist if isinstance(p, dict) and _is_number(p.get("value"))]

    chart = ChartSpec(
        type="line",
        data={
            "data": [
                {
                    "type": "scatter",
                    "mode": "lines",
                    "name": "Historical",
                    "x": x_hist,
                    "y": y_hist,
                },
                {
                    "type": "scatter",
                    "mode": "lines",
                    "name": "Forecast",
                    "x": x_fore,
                    "y": y_fore,
                },
                {
                    "type": "scatter",
                    "mode": "lines",
                    "name": "Upper",
                    "x": x_fore,
                    "y": hi,
                    "line": {"width": 0},
                    "showlegend": False,
                },
                {
                    "type": "scatter",
                    "mode": "lines",
                    "name": "Lower",
                    "x": x_fore,
                    "y": lo,
                    "fill": "tonexty",
                    "fillcolor": "rgba(100, 149, 237, 0.15)",
                    "line": {"width": 0},
                    "showlegend": False,
                },
            ],
            "layout": {
                "title": f"Forecast for {target_col} ({freq})",
                "xaxis": {"title": date_col or "date"},
                "yaxis": {"title": target_col or "value"},
            },
        },
    )

    answer = (
        f"I detected a time series using '{date_col}' and forecasted '{target_col}' 10 steps ahead using ARIMA(1,1,1). "
        "The forecast line includes an uncertainty band (lower/upper)."
    )

    insights = [
        f"Model: ARIMA(1,1,1).",
        f"Frequency: {freq or 'unknown'}.",
    ]

    return QueryResponse(
        answer=answer,
        table=table,
        chart=chart,
        insights=insights,
        query_type="forecast",
    )


def _is_number(v: Any) -> bool:
    try:
        float(v)
        return True
    except Exception:
        return False


def _fmt_num(v: Any) -> str:
    try:
        x = float(v)
        if abs(x) >= 1000:
            return f"{x:,.2f}"
        return f"{x:.3f}" if abs(x) < 10 else f"{x:.2f}"
    except Exception:
        return str(v)
