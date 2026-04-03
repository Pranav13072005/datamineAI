from __future__ import annotations

from typing import Any

from app.schemas.query import ChartSpec, QueryResponse, TableResult


def _is_number(v: Any) -> bool:
    try:
        float(v)
        return True
    except Exception:
        return False


def handle_correlation_query(question: str, fact_cache: dict | None) -> QueryResponse:
    """Serve pre-computed correlation results from fact_cache.

    This handler is deterministic and never calls the LLM.

    Expected cache layout (from extract_insights):
      fact_cache["correlations"] = list[{col_a, col_b, r}]
    """

    cache: dict[str, Any] = fact_cache or {}
    if not isinstance(cache, dict):
        cache = {}

    correlations = cache.get("correlations")
    if not isinstance(correlations, list):
        return QueryResponse(
            answer=(
                "I’m still computing the dataset insights in the background. "
                "Please try again in a few seconds."
            ),
            query_type="correlation",
            warnings=["fact_cache.correlations missing (still processing)"],
        )

    pairs: list[dict[str, Any]] = []
    for item in correlations:
        if not isinstance(item, dict):
            continue
        r = item.get("r")
        if not _is_number(r):
            continue
        pairs.append({"col_a": str(item.get("col_a") or ""), "col_b": str(item.get("col_b") or ""), "r": float(r)})

    if not pairs:
        return QueryResponse(
            answer=(
                "I didn’t find any strong linear relationships between numeric columns (|r| > 0.5). "
                "Try asking about trends, distributions, or specific columns instead."
            ),
            query_type="correlation",
            insights=["No strong correlations found."],
        )

    pairs.sort(key=lambda d: abs(float(d["r"])), reverse=True)
    top = pairs[0]
    col_a = top["col_a"]
    col_b = top["col_b"]
    r_top = float(top["r"])

    direction = "positive" if r_top >= 0 else "negative"
    meaning = (
        "they tend to increase together" if r_top >= 0 else "when one increases, the other tends to decrease"
    )

    answer = (
        f"The strongest relationship I found is between '{col_a}' and '{col_b}' (r={r_top:.2f}). "
        f"This is a {direction} correlation, meaning {meaning}. "
        "See the table for the strongest pairs I detected."
    )

    top5 = pairs[:5]
    table_rows: list[list[Any]] = []
    x: list[str] = []
    y: list[float] = []
    text: list[str] = []
    colors: list[str] = []

    for p in top5:
        r = float(p["r"])
        strength = abs(r)
        table_rows.append([p["col_a"], p["col_b"], r, strength])
        x.append(f"{p['col_a']} vs {p['col_b']}")
        y.append(strength)
        text.append(f"r={r:.2f}")
        colors.append("#22c55e" if r >= 0 else "#ef4444")

    table = TableResult(columns=["col_a", "col_b", "r", "abs_r"], rows=table_rows)

    chart = ChartSpec(
        type="bar",
        data={
            "data": [
                {
                    "type": "bar",
                    "x": x,
                    "y": y,
                    "text": text,
                    "textposition": "auto",
                    "marker": {"color": colors},
                    "hovertemplate": "%{x}<br>%{text}<br>|r|=%{y:.2f}<extra></extra>",
                    "name": "Correlation strength",
                }
            ],
            "layout": {
                "title": "Top correlated column pairs (by |r|)",
                "yaxis": {"title": "|r|"},
                "xaxis": {"title": "Column pair", "tickangle": -20},
                "margin": {"l": 50, "r": 20, "t": 50, "b": 120},
            },
        },
    )

    insights = [
        f"Strongest: {col_a} vs {col_b} (r={r_top:.2f}).",
        "Correlation is linear association; it doesn’t imply causation.",
    ]

    return QueryResponse(
        answer=answer,
        table=table,
        chart=chart,
        insights=insights,
        query_type="correlation",
    )
