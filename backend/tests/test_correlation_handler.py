from __future__ import annotations

from app.services.correlation_handler import handle_correlation_query


def test_handle_correlation_query_formats_top_pair() -> None:
    fact_cache = {
        "correlations": [
            {"col_a": "A", "col_b": "B", "r": 0.91},
            {"col_a": "C", "col_b": "D", "r": -0.72},
        ]
    }

    resp = handle_correlation_query("which two columns are related", fact_cache)

    assert resp.query_type == "correlation"
    assert "A" in resp.answer and "B" in resp.answer
    assert resp.table is not None
    assert resp.chart is not None
    assert resp.table.columns[:3] == ["col_a", "col_b", "r"]


def test_handle_correlation_query_processing_when_missing_cache() -> None:
    resp = handle_correlation_query("which two columns are related", None)
    assert resp.query_type == "correlation"
    assert "still computing" in resp.answer.lower()
