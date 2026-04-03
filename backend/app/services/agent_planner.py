from __future__ import annotations

import json
import re
from typing import Any

import httpx
import pandas as pd

from app.config import settings
from app.schemas.query import ChartSpec, QueryResponse, TableResult
from app.services import code_executor, descriptive_handler


_FALLBACK: list[dict[str, Any]] = [{"tool": "describe", "args": {}, "purpose": "fallback"}]


_SYSTEM_PROMPT = """You are a senior data analyst planning how to answer a question about a dataset.
You must respond with ONLY a JSON array of steps, no other text.
Each step has: {\"tool\": str, \"args\": dict, \"purpose\": str}

Available tools:
  run_pandas: {\"code\": str} — run pandas code, result stored in `result` variable
  get_ml_result: {\"type\": \"anomalies\"|\"clusters\"|\"forecast\"} — get pre-computed ML result
  get_column_stats: {\"column\": str} — get stats for a specific column
  search_datasets: {\"query\": str} — find similar columns across all datasets
  describe: {} — get the dataset overview

Rules:
  - Use 1 step for simple questions, up to 4 steps for complex ones
  - Prefer get_ml_result over run_pandas when the question is about anomalies/clusters/forecast
  - Keep run_pandas code under 10 lines, vectorised only
  - If the question cannot be answered with these tools, use [{\"tool\": \"describe\", \"args\": {}, \"purpose\": \"explain what data is available\"}]"""


_SYNTHESIS_SYSTEM_PROMPT = """You are a senior data analyst writing a clear, direct answer.
Respond ONLY with valid JSON matching this schema:
    {
        answer: str,          # 2–4 sentence narrative, past tense, professional tone
        insights: [str],      # 3–5 bullet findings from the data
        chart_type: str|null, # bar|line|scatter|pie|histogram|null
        chart_columns: [str]|null,  # which columns to plot
        table_from_tool: str|null   # which tool result to use as the table (tool name)
    }
"""


def plan_query(question: str, dataset_schema: dict, fact_cache: dict) -> list[dict]:
    """Generate a tool plan for answering a dataset question.

    Makes exactly one Groq LLM call and expects a JSON array response.

    Args:
        question: The user's natural-language question.
        dataset_schema: Dict describing the dataset schema.
        fact_cache: Dict of precomputed facts/ML results.

    Returns:
        List of steps. On invalid JSON (or missing Groq config), returns a
        single describe fallback step.
    """

    q = (question or "").strip()
    if not q:
        return _FALLBACK

    if not settings.GROQ_API_KEY:
        return _FALLBACK

    user_payload = {
        "question": q,
        "dataset_schema": dataset_schema or {},
        "fact_cache": fact_cache or {},
    }

    try:
        user_content = json.dumps(user_payload, ensure_ascii=False, default=str, separators=(",", ":"))
    except Exception:
        # If schema/cache contains unserializable values despite default=str,
        # don't crash; fall back.
        return _FALLBACK

    try:
        raw_text = _call_groq(system=_SYSTEM_PROMPT, user=user_content)
    except Exception:
        return _FALLBACK

    plan = _parse_plan_json_array(raw_text)
    return plan if plan is not None else _FALLBACK


def execute_plan(plan: list[dict], df: pd.DataFrame, fact_cache: dict) -> list[dict]:
    """Execute a tool plan sequentially against a dataframe.

    Never raises: errors are captured per-step.

    Returns a list of:
      {"tool": str, "purpose": str, "result": Any, "error": str|None}
    """

    steps = plan if isinstance(plan, list) else []
    cache: dict[str, Any] = fact_cache if isinstance(fact_cache, dict) else {}

    outputs: list[dict[str, Any]] = []
    for step in steps:
        tool = _step_get_str(step, "tool")
        purpose = _step_get_str(step, "purpose")
        args = _step_get_dict(step, "args")

        out: dict[str, Any] = {"tool": tool or "", "purpose": purpose or "", "result": None, "error": None}

        try:
            if tool == "run_pandas":
                code = _step_get_str(args, "code")
                r = code_executor.execute_pandas_code(code or "", df)
                if isinstance(r, dict):
                    out["result"] = r.get("result")
                    out["error"] = r.get("error")
                else:
                    out["error"] = "execute_pandas_code returned non-dict"

            elif tool == "get_ml_result":
                ml_type = _step_get_str(args, "type")
                ml = cache.get("ml") if isinstance(cache, dict) else None
                if not isinstance(ml, dict):
                    out["error"] = "fact_cache.ml missing"
                else:
                    if not ml_type:
                        out["error"] = "args.type missing"
                    else:
                        out["result"] = ml.get(ml_type)
                        if out["result"] is None:
                            out["error"] = f"fact_cache.ml.{ml_type} missing"

            elif tool == "get_column_stats":
                col = _step_get_str(args, "column")
                if not col:
                    out["error"] = "args.column missing"
                elif not hasattr(df, "columns") or col not in df.columns:
                    out["error"] = f"column not found: {col}"
                else:
                    s = df[col]
                    nulls = int(s.isna().sum())
                    # Compute numeric stats when possible; otherwise return None.
                    mean_v: Any = None
                    std_v: Any = None
                    min_v: Any = None
                    max_v: Any = None

                    try:
                        if pd.api.types.is_numeric_dtype(s):
                            mean_v = float(s.mean(skipna=True)) if s.dropna().shape[0] else None
                            std_v = float(s.std(skipna=True)) if s.dropna().shape[0] else None
                            min_v = float(s.min(skipna=True)) if s.dropna().shape[0] else None
                            max_v = float(s.max(skipna=True)) if s.dropna().shape[0] else None
                        else:
                            non_null = s.dropna()
                            if non_null.shape[0]:
                                min_v = _json_safe(non_null.min())
                                max_v = _json_safe(non_null.max())
                    except Exception:
                        # Keep defaults; stats should be best-effort.
                        pass

                    sample = [_json_safe(v) for v in s.dropna().head(5).tolist()]
                    out["result"] = {
                        "mean": mean_v,
                        "std": std_v,
                        "min": min_v,
                        "max": max_v,
                        "nulls": nulls,
                        "sample": sample,
                    }

            elif tool == "search_datasets":
                query = _step_get_str(args, "query")
                # Lazy import so planner works even when embedding deps aren't installed.
                from app.services.embedding_service import get_embedding_service

                out["result"] = get_embedding_service().search_columns(query or "")

            elif tool == "describe":
                resp = descriptive_handler.handle_descriptive(df, "describe")
                # QueryResponse is a pydantic model; keep output JSON-safe.
                try:
                    out["result"] = resp.model_dump()  # type: ignore[attr-defined]
                except Exception:
                    out["result"] = _json_safe(resp)

            else:
                out["error"] = f"unknown tool: {tool}" if tool else "missing tool"

        except Exception as exc:
            out["error"] = f"{exc.__class__.__name__}: {exc}"

        outputs.append(out)

    return outputs


def synthesise_results(question: str, plan_results: list[dict], schema: dict) -> QueryResponse:
    """Synthesize executed plan results into a final QueryResponse.

    Makes exactly one Groq LLM call to generate a structured narrative response.
    If the response is invalid JSON, falls back to a basic answer.

    After parsing:
      - Build chart spec deterministically from chart_type + chart_columns
      - Select the table from the specified tool result
    """

    q = (question or "").strip()
    results = plan_results if isinstance(plan_results, list) else []
    schema_obj = schema if isinstance(schema, dict) else {}

    if not settings.GROQ_API_KEY:
        return _basic_fallback_response(q, results)

    user_payload = {
        "question": q,
        "schema": schema_obj,
        "plan_results": results,
    }

    try:
        user_content = json.dumps(user_payload, ensure_ascii=False, default=str, separators=(",", ":"))
    except Exception:
        return _basic_fallback_response(q, results)

    try:
        raw_text = _call_groq(system=_SYNTHESIS_SYSTEM_PROMPT, user=user_content)
    except Exception:
        return _basic_fallback_response(q, results)

    parsed = _parse_synthesis_json(raw_text)
    if parsed is None:
        resp = _basic_fallback_response(q, results)
        resp.warnings.append("synthesis: invalid json")
        return resp

    answer = parsed["answer"]
    insights = parsed["insights"]
    chart_type = parsed["chart_type"]
    chart_columns = parsed["chart_columns"]
    table_from_tool = parsed["table_from_tool"]

    warnings: list[str] = []
    table, table_warnings = _select_table_from_results(results, table_from_tool)
    warnings.extend(table_warnings)

    chart: ChartSpec | None = None
    if chart_type is not None:
        if table is None:
            warnings.append("chart requested but no table available")
        else:
            chart, chart_warnings = _build_chart_from_table(chart_type, chart_columns, table)
            warnings.extend(chart_warnings)

    return QueryResponse(
        answer=answer,
        insights=insights,
        table=table,
        chart=chart,
        warnings=warnings,
        query_type="analytical",
    )


def _call_groq(*, system: str, user: str) -> str:
    headers = {
        "Authorization": f"Bearer {settings.GROQ_API_KEY}",
        "Content-Type": "application/json",
    }

    payload: dict[str, Any] = {
        "model": settings.GROQ_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": 600,
    }

    r = httpx.post(
        "https://api.groq.com/openai/v1/chat/completions",
        json=payload,
        headers=headers,
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    return str(data["choices"][0]["message"]["content"])


def _strip_markdown_fences(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        return ""
    fence = re.search(r"```(?:json)?\s*(.*?)\s*```", raw, flags=re.DOTALL | re.IGNORECASE)
    if fence:
        return fence.group(1).strip()
    return raw


def _parse_synthesis_json(text: str) -> dict[str, Any] | None:
    raw = _strip_markdown_fences(text)
    if not raw:
        return None

    try:
        data = json.loads(raw)
    except Exception:
        return None

    if not isinstance(data, dict):
        return None

    answer = data.get("answer")
    insights = data.get("insights")
    chart_type = data.get("chart_type")
    chart_columns = data.get("chart_columns")
    table_from_tool = data.get("table_from_tool")

    if not isinstance(answer, str) or not answer.strip():
        return None

    if not isinstance(insights, list) or not insights or not all(isinstance(x, str) and x.strip() for x in insights):
        return None

    # Normalize insights length without failing hard.
    insights_norm = [str(x).strip() for x in insights if str(x).strip()]

    allowed_chart_types = {"bar", "line", "scatter", "pie", "histogram"}
    if chart_type is None:
        chart_type_norm: str | None = None
    elif isinstance(chart_type, str) and chart_type in allowed_chart_types:
        chart_type_norm = chart_type
    else:
        return None

    if chart_columns is None:
        chart_cols_norm: list[str] | None = None
    elif isinstance(chart_columns, list) and all(isinstance(c, str) and c.strip() for c in chart_columns):
        chart_cols_norm = [c.strip() for c in chart_columns]
    else:
        return None

    if table_from_tool is None:
        table_from_tool_norm: str | None = None
    elif isinstance(table_from_tool, str) and table_from_tool.strip():
        table_from_tool_norm = table_from_tool.strip()
    else:
        return None

    return {
        "answer": answer.strip(),
        "insights": insights_norm[:5],
        "chart_type": chart_type_norm,
        "chart_columns": chart_cols_norm,
        "table_from_tool": table_from_tool_norm,
    }


def _basic_fallback_response(question: str, plan_results: list[dict]) -> QueryResponse:
    q = (question or "").strip()
    base = (
        "I reviewed the available analysis outputs for your question. "
        "I couldn’t generate a structured narrative summary from the tool results, so I returned the raw results for review."
    )
    if q:
        base = (
            f"I reviewed the available analysis outputs for the question: {q} "
            "I couldn’t generate a structured narrative summary from the tool results, so I returned the raw results for review."
        )

    table, warnings = _select_table_from_results(plan_results if isinstance(plan_results, list) else [], None)
    return QueryResponse(answer=base, insights=[], table=table, chart=None, warnings=warnings, query_type="analytical")


def _select_table_from_results(
    plan_results: list[dict], preferred_tool: str | None
) -> tuple[TableResult | None, list[str]]:
    warnings: list[str] = []
    results = plan_results if isinstance(plan_results, list) else []

    def iter_candidates() -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for r in results:
            if not isinstance(r, dict):
                continue
            tool = r.get("tool")
            if not isinstance(tool, str):
                continue
            err = r.get("error")
            if err:
                continue
            out.append(r)  # type: ignore[arg-type]
        return out

    candidates = iter_candidates()
    if preferred_tool:
        for r in candidates:
            if r.get("tool") == preferred_tool:
                table = _result_to_table(r.get("result"))
                if table is not None:
                    return table, warnings
                warnings.append(f"table_from_tool result not tabular: {preferred_tool}")
                break
        else:
            warnings.append(f"table_from_tool not found: {preferred_tool}")

    for r in candidates:
        table = _result_to_table(r.get("result"))
        if table is not None:
            if preferred_tool:
                warnings.append("used first available tabular result")
            return table, warnings

    warnings.append("no tabular tool result available")
    return None, warnings


def _result_to_table(result: Any) -> TableResult | None:
    if result is None:
        return None

    # Already in TableResult-like shape
    if isinstance(result, dict) and isinstance(result.get("columns"), list) and isinstance(result.get("rows"), list):
        cols = [str(c) for c in result.get("columns") if c is not None]
        rows_raw = result.get("rows")
        if isinstance(rows_raw, list) and all(isinstance(r, list) for r in rows_raw):
            return TableResult(columns=cols, rows=rows_raw)  # type: ignore[arg-type]

    # list[dict] -> table
    if isinstance(result, list) and result and all(isinstance(x, dict) for x in result):
        cols = list({k for row in result for k in row.keys()})
        cols.sort()
        rows = [[(row.get(c) if isinstance(row, dict) else None) for c in cols] for row in result]
        return TableResult(columns=[str(c) for c in cols], rows=rows)

    # dict -> key/value table (avoid huge nested objects)
    if isinstance(result, dict):
        cols = ["key", "value"]
        rows = [[str(k), _json_safe(v)] for k, v in result.items()]
        return TableResult(columns=cols, rows=rows)

    return None


def _build_chart_from_table(
    chart_type: str, chart_columns: list[str] | None, table: TableResult
) -> tuple[ChartSpec | None, list[str]]:
    warnings: list[str] = []

    if not chart_columns:
        warnings.append("chart_columns missing")
        return None, warnings

    col_to_idx = {c: i for i, c in enumerate(table.columns)}
    missing = [c for c in chart_columns if c not in col_to_idx]
    if missing:
        warnings.append(f"chart_columns not in table: {', '.join(missing)}")
        return None, warnings

    max_points = 200
    rows = table.rows[:max_points]
    if len(table.rows) > max_points:
        warnings.append(f"chart sampled first {max_points} rows")

    def col_values(col: str) -> list[Any]:
        idx = col_to_idx[col]
        out: list[Any] = []
        for r in rows:
            if not isinstance(r, list) or idx >= len(r):
                out.append(None)
            else:
                out.append(r[idx])
        return out

    if chart_type == "histogram":
        x_col = chart_columns[0]
        x = col_values(x_col)
        trace = {"type": "histogram", "x": x, "name": x_col}
        data = {"data": [trace], "layout": {"title": f"Distribution of {x_col}"}}
        return ChartSpec(type="histogram", data=data), warnings

    if len(chart_columns) < 2:
        warnings.append("not enough chart_columns for chart type")
        return None, warnings

    x_col = chart_columns[0]
    y_col = chart_columns[1]
    x = col_values(x_col)
    y = col_values(y_col)

    if chart_type == "pie":
        trace = {"type": "pie", "labels": x, "values": y, "name": f"{y_col} by {x_col}"}
        data = {"data": [trace], "layout": {"title": f"{y_col} by {x_col}"}}
        return ChartSpec(type="pie", data=data), warnings

    if chart_type == "line":
        trace = {"type": "scatter", "mode": "lines", "x": x, "y": y, "name": y_col}
        data = {"data": [trace], "layout": {"title": f"{y_col} vs {x_col}", "xaxis": {"title": x_col}, "yaxis": {"title": y_col}}}
        return ChartSpec(type="line", data=data), warnings

    if chart_type == "scatter":
        trace = {"type": "scatter", "mode": "markers", "x": x, "y": y, "name": y_col}
        data = {"data": [trace], "layout": {"title": f"{y_col} vs {x_col}", "xaxis": {"title": x_col}, "yaxis": {"title": y_col}}}
        return ChartSpec(type="scatter", data=data), warnings

    # bar
    trace = {"type": "bar", "x": x, "y": y, "name": y_col}
    data = {"data": [trace], "layout": {"title": f"{y_col} by {x_col}", "xaxis": {"title": x_col}, "yaxis": {"title": y_col}}}
    return ChartSpec(type="bar", data=data), warnings


def _parse_plan_json_array(text: str) -> list[dict[str, Any]] | None:
    raw = (text or "").strip()
    if not raw:
        return None

    # Strip accidental markdown fences
    fence = re.search(r"```(?:json)?\s*(.*?)\s*```", raw, flags=re.DOTALL | re.IGNORECASE)
    if fence:
        raw = fence.group(1).strip()

    try:
        data = json.loads(raw)
    except Exception:
        return None

    if not isinstance(data, list):
        return None

    steps: list[dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            return None
        tool = item.get("tool")
        args = item.get("args")
        purpose = item.get("purpose")
        if not isinstance(tool, str) or not tool.strip():
            return None
        if not isinstance(args, dict):
            return None
        if not isinstance(purpose, str) or not purpose.strip():
            return None
        steps.append({"tool": tool, "args": args, "purpose": purpose})

    return steps


def _step_get_str(obj: Any, key: str) -> str | None:
    if obj is None:
        return None
    try:
        if isinstance(obj, dict):
            v = obj.get(key)
        else:
            v = getattr(obj, key)
    except Exception:
        return None
    return v if isinstance(v, str) else None


def _step_get_dict(obj: Any, key: str) -> dict[str, Any]:
    if obj is None:
        return {}
    try:
        if isinstance(obj, dict):
            v = obj.get(key)
        else:
            v = getattr(obj, key)
    except Exception:
        return {}
    return v if isinstance(v, dict) else {}


def _json_safe(value: Any) -> Any:
    """Best-effort JSON-safe conversion for planner outputs."""

    if value is None or isinstance(value, (str, int, bool, float)):
        return value

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]

    try:
        if hasattr(value, "model_dump"):
            return value.model_dump()  # type: ignore[no-any-return]
    except Exception:
        pass

    return str(value)
