from __future__ import annotations

import json
import sys
import types
from typing import Any

import pandas as pd
import pytest

from app.config import settings
from app.services.agent_planner import execute_plan, plan_query, synthesise_results


class _DummyResponse:
    def __init__(self, *, payload: dict[str, Any], status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> dict[str, Any]:
        return self._payload


def test_plan_query_parses_json_array(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "GROQ_API_KEY", "test")
    monkeypatch.setattr(settings, "GROQ_MODEL", "test-model")

    calls: dict[str, int] = {"count": 0}

    def fake_post(*args: Any, **kwargs: Any) -> _DummyResponse:
        calls["count"] += 1
        return _DummyResponse(
            payload={
                "choices": [
                    {
                        "message": {
                            "content": '[{"tool":"describe","args":{},"purpose":"overview"}]'
                        }
                    }
                ]
            }
        )

    import app.services.agent_planner as planner

    monkeypatch.setattr(planner.httpx, "post", fake_post)

    out = plan_query("What columns are in the dataset?", {"columns": ["a"]}, {})
    assert out == [{"tool": "describe", "args": {}, "purpose": "overview"}]
    assert calls["count"] == 1


def test_plan_query_invalid_json_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "GROQ_API_KEY", "test")
    monkeypatch.setattr(settings, "GROQ_MODEL", "test-model")

    def fake_post(*args: Any, **kwargs: Any) -> _DummyResponse:
        return _DummyResponse(
            payload={"choices": [{"message": {"content": "not json"}}]}
        )

    import app.services.agent_planner as planner

    monkeypatch.setattr(planner.httpx, "post", fake_post)

    out = plan_query("Anything", {}, {})
    assert out == [{"tool": "describe", "args": {}, "purpose": "fallback"}]


def test_plan_query_injects_related_history_into_system_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "GROQ_API_KEY", "test")
    monkeypatch.setattr(settings, "GROQ_MODEL", "test-model")

    import app.services.agent_planner as planner
    import app.services.history_service as history_service

    def fake_search_history(question: str, dataset_id: str, top_k: int = 3) -> list[dict[str, Any]]:
        return [
            {
                "id": "1",
                "question": "Past Q",
                "answer_summary": "Past A",
                "query_type": "analytical",
                "score": 0.81234,
                "created_at": "2026-01-01T00:00:00Z",
            }
        ]

    monkeypatch.setattr(history_service, "search_history", fake_search_history)

    captured: dict[str, str] = {"system": ""}

    def fake_post(*args: Any, **kwargs: Any) -> _DummyResponse:
        payload = kwargs.get("json") or {}
        msgs = payload.get("messages") or []
        captured["system"] = msgs[0]["content"]
        return _DummyResponse(
            payload={
                "choices": [
                    {
                        "message": {
                            "content": '[{"tool":"describe","args":{},"purpose":"overview"}]'
                        }
                    }
                ]
            }
        )

    monkeypatch.setattr(planner.httpx, "post", fake_post)

    fc: dict[str, Any] = {}
    out = plan_query("Q", {"columns": ["a"]}, fc, dataset_id="ds1")
    assert out == [{"tool": "describe", "args": {}, "purpose": "overview"}]
    assert "Dataset schema:" in captured["system"]
    assert "Relevant findings from previous analyses of this dataset:" in captured["system"]
    assert "- Past Q: Past A (similarity: 0.812" in captured["system"]
    assert isinstance(fc.get("related_history"), list)
    assert fc["related_history"][0]["question"] == "Past Q"


def test_plan_query_missing_api_key_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "GROQ_API_KEY", "")
    out = plan_query("Anything", {}, {})
    assert out == [{"tool": "describe", "args": {}, "purpose": "fallback"}]


def test_execute_plan_dispatch_and_outputs(monkeypatch: pytest.MonkeyPatch) -> None:
    df = pd.DataFrame({"a": [1, 2, None], "b": ["x", "y", "z"]})

    # Avoid actually exec'ing pandas code in unit tests.
    import app.services.agent_planner as planner

    calls: dict[str, Any] = {"pandas": 0, "search": 0}

    def fake_execute(code: str, df_in: pd.DataFrame, timeout_seconds: int = 10) -> dict:
        assert "result" in code
        assert list(df_in.columns) == ["a", "b"]
        calls["pandas"] += 1
        return {"result": 123, "error": None}

    monkeypatch.setattr(planner.code_executor, "execute_pandas_code", fake_execute)

    class _Emb:
        def search_columns(self, query: str, top_k: int = 10) -> list[dict[str, Any]]:
            calls["search"] += 1
            return [{"dataset_id": "d1", "column_name": "a", "description": None, "score": 0.9}]

    # execute_plan lazy-imports app.services.embedding_service
    emb_mod = types.ModuleType("app.services.embedding_service")
    emb_mod.get_embedding_service = lambda: _Emb()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "app.services.embedding_service", emb_mod)

    plan = [
        {"tool": "run_pandas", "args": {"code": "result = 1"}, "purpose": "quick"},
        {"tool": "get_ml_result", "args": {"type": "anomalies"}, "purpose": "ml"},
        {"tool": "get_column_stats", "args": {"column": "a"}, "purpose": "stats"},
        {"tool": "search_datasets", "args": {"query": "similar to a"}, "purpose": "search"},
        {"tool": "describe", "args": {}, "purpose": "overview"},
    ]

    fact_cache = {"ml": {"anomalies": {"ok": True}}}
    out = execute_plan(plan, df, fact_cache)

    assert [s["tool"] for s in out] == ["run_pandas", "get_ml_result", "get_column_stats", "search_datasets", "describe"]
    assert out[0]["result"] == 123 and out[0]["error"] is None
    assert out[1]["result"] == {"ok": True} and out[1]["error"] is None

    stats = out[2]["result"]
    assert stats["nulls"] == 1
    assert stats["min"] == 1.0 and stats["max"] == 2.0
    assert isinstance(stats["sample"], list)
    assert out[2]["error"] is None

    assert out[3]["result"][0]["column_name"] == "a" and out[3]["error"] is None
    assert isinstance(out[4]["result"], dict) and out[4]["error"] is None
    assert calls["pandas"] == 1
    assert calls["search"] == 1


def test_execute_plan_catches_errors_per_step() -> None:
    df = pd.DataFrame({"a": [1]})
    plan = [
        {"tool": "get_column_stats", "args": {"column": "missing"}, "purpose": "oops"},
        {"tool": "unknown", "args": {}, "purpose": "nope"},
    ]

    out = execute_plan(plan, df, {})
    assert out[0]["error"]
    assert out[1]["error"]


def test_synthesise_results_valid_json_builds_table_and_chart(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "GROQ_API_KEY", "test")
    monkeypatch.setattr(settings, "GROQ_MODEL", "test-model")

    calls: dict[str, int] = {"count": 0}

    def fake_post(*args: Any, **kwargs: Any) -> _DummyResponse:
        calls["count"] += 1
        return _DummyResponse(
            payload={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "answer": "I reviewed the dataset results and summarized the key findings.",
                                    "insights": ["A", "B", "C"],
                                    "chart_type": "bar",
                                    "chart_columns": ["x", "y"],
                                    "table_from_tool": "run_pandas",
                                }
                            )
                        }
                    }
                ]
            }
        )

    import app.services.agent_planner as planner

    monkeypatch.setattr(planner.httpx, "post", fake_post)

    plan_results = [
        {
            "tool": "run_pandas",
            "purpose": "compute",
            "result": [{"x": "A", "y": 1}, {"x": "B", "y": 2}],
            "error": None,
        }
    ]

    resp = synthesise_results("What happened?", plan_results, {"columns": ["x", "y"]})
    assert resp.answer
    assert resp.insights == ["A", "B", "C"]
    assert resp.table is not None
    assert resp.table.columns == ["x", "y"]
    assert resp.table.rows == [["A", 1], ["B", 2]]
    assert resp.chart is not None
    assert resp.chart.type == "bar"
    assert resp.chart.data["data"][0]["type"] == "bar"
    assert resp.chart.data["data"][0]["x"] == ["A", "B"]
    assert resp.chart.data["data"][0]["y"] == [1, 2]
    assert calls["count"] == 1


def test_synthesise_results_invalid_json_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "GROQ_API_KEY", "test")
    monkeypatch.setattr(settings, "GROQ_MODEL", "test-model")

    def fake_post(*args: Any, **kwargs: Any) -> _DummyResponse:
        return _DummyResponse(payload={"choices": [{"message": {"content": "not json"}}]})

    import app.services.agent_planner as planner

    monkeypatch.setattr(planner.httpx, "post", fake_post)

    plan_results = [
        {
            "tool": "run_pandas",
            "purpose": "compute",
            "result": [{"x": "A", "y": 1}],
            "error": None,
        }
    ]

    resp = synthesise_results("Any?", plan_results, {})
    assert resp.answer
    assert resp.chart is None
    assert "synthesis: invalid json" in resp.warnings
