from __future__ import annotations

import json
import math
import re
import traceback
from dataclasses import dataclass
from typing import Any, Literal

import pandas as pd

from app.schemas.query import ChartSpec, QueryResponse, TableResult


ChartType = Literal["bar", "line", "scatter", "pie"]


_IMPORT_STMT_RE = re.compile(r"(^|\n)\s*(import|from)\s+", flags=re.IGNORECASE)


@dataclass(frozen=True)
class _LLMPlan:
    pandas_code: str | None
    answer_template: str
    chart_type: ChartType | None
    insights: list[str]


def handle_analytical(df: pd.DataFrame, question: str, groq_client: Any) -> QueryResponse:
    """Handle analytical questions using an LLM-generated pandas plan.

    Flow:
      1) Build a prompt containing schema + 3 sample rows.
      2) Ask the LLM to return ONLY a JSON object with keys:
         pandas_code, answer_template, chart_type, insights.
      3) Parse strictly. If parsing fails, return query_type="error".
      4) Execute pandas_code in a restricted sandbox with a hard 10s timeout.
      5) Never raise; always return a QueryResponse with warnings.
    """

    q = (question or "").strip()
    if not q:
        return QueryResponse(
            answer="Please ask a non-empty analytical question about the dataset.",
            query_type="error",
            warnings=["empty question"],
        )

    # Deterministic chart path: for visualization questions, don't rely on
    # LLM codegen/execution. Return an inline Plotly spec reliably.
    if _looks_like_chart_request(q):
        return _handle_chart_request(df, q)

    # Deterministic "feature importance" path: this phrasing is common but
    # underspecified. Provide a grounded answer based on numeric correlations.
    if _looks_like_feature_importance_request(q):
        return _handle_feature_importance_request(df, q)

    schema_payload = _build_schema_payload(df)

    # One automatic repair attempt for common LLM failure modes:
    # - invalid JSON
    # - forbidden code (imports)
    # This keeps behavior consistent without requiring users to debug phrasing.
    llm_warnings: list[str] = []
    plan: _LLMPlan | None = None
    last_llm_text: str | None = None

    for attempt in range(2):
        attempt_question = q
        if attempt == 1 and llm_warnings:
            attempt_question = (
                q
                + "\n\nIMPORTANT REPAIR NOTE: Your previous response was invalid. "
                + "Fix it by returning ONLY valid JSON and ensuring pandas_code has NO import/from statements and assigns result. "
                + "Error details: "
                + "; ".join(llm_warnings)
            )

        try:
            last_llm_text = _call_groq_for_json_plan(
                question=attempt_question,
                schema_payload=schema_payload,
                row_count=int(len(df)),
                col_count=int(df.shape[1]),
                columns=[str(c) for c in df.columns.tolist()],
                groq_client=groq_client,
            )
        except Exception as e:
            return QueryResponse(
                answer="I couldn’t generate an analysis plan right now.",
                query_type="error",
                warnings=[f"llm call failed: {e.__class__.__name__}: {e}"],
            )

        try:
            plan = _parse_llm_plan(last_llm_text)
        except Exception as e:
            llm_warnings = [f"llm json parse failed: {e.__class__.__name__}: {e}"]
            continue

        if plan.pandas_code is not None and _IMPORT_STMT_RE.search(plan.pandas_code):
            llm_warnings = ["model included an import/from statement (forbidden)"]
            plan = None
            continue

        # Valid plan.
        break

    if plan is None:
        return QueryResponse(
            answer=(
                "I couldn’t reliably produce a safe analysis plan for that request. "
                "Try asking a simpler question (e.g., ‘average X’, ‘top 10 by Y’, ‘count rows where ...’)."
            ),
            query_type="error",
            warnings=llm_warnings or ["llm plan generation failed"],
        )

    if plan.pandas_code is None:
        return QueryResponse(
            answer=_render_answer(plan.answer_template, df=df, result=None),
            query_type="error",
            insights=[],
            warnings=[
                "model indicated the question cannot be answered from the data (pandas_code is null)",
            ],
        )

    # Hard guardrail: imports are not allowed in our exec environments.
    # If the model violates this, fail with a clean error (no traceback).
    if _IMPORT_STMT_RE.search(plan.pandas_code):
        return QueryResponse(
            answer=(
                "I couldn’t run that analysis because the generated code attempted to import modules, "
                "which is disabled for safety. Try asking the question without requesting external libraries."
            ),
            query_type="error",
            insights=[],
            warnings=["model produced an import statement; execution blocked"],
        )

    # Execute safely (prefer RestrictedPython sandbox; fall back to in-process safe exec)
    exec_result = _execute_pandas_code_sandboxed(plan.pandas_code, df, timeout_seconds=10)
    warnings: list[str] = []
    warnings.extend(exec_result.warnings)

    if not exec_result.ok:
        return QueryResponse(
            answer=(
                "I couldn’t compute that result safely. "
                "Try asking a simpler question (e.g., ‘count’, ‘average’, ‘top N’)."
            ),
            query_type="error",
            warnings=warnings or ["execution failed"],
            insights=[],
        )

    result = exec_result.result

    table, table_warnings = _result_to_table(result)
    warnings.extend(table_warnings)

    chart = None
    if plan.chart_type and table:
        chart, chart_warnings = _build_chart(plan.chart_type, table)
        warnings.extend(chart_warnings)

    answer = _render_answer(plan.answer_template, df=df, result=result)

    return QueryResponse(
        answer=answer,
        table=table,
        chart=chart,
        insights=plan.insights,
        warnings=warnings,
        query_type="analytical",
    )


def _build_schema_payload(df: pd.DataFrame) -> dict[str, Any]:
    schema: dict[str, Any] = {}
    for col in df.columns:
        series = df[col]
        sample_values = _jsonify(series.head(3).tolist())
        null_count = int(series.isna().sum())
        schema[str(col)] = {
            "dtype": str(series.dtype),
            "sample_values": sample_values,
            "null_count": null_count,
        }
    return schema


def _looks_like_chart_request(question: str) -> bool:
    q = (question or "").lower()
    return any(
        k in q
        for k in (
            "chart",
            "charts",
            "graph",
            "graphs",
            "plot",
            "plots",
            "hist",
            "histogram",
            "distribution",
            "dist",
            "density",
            "boxplot",
            "box plot",
            "violin",
            "bar",
            "line",
            "scatter",
            "pie",
            "heatmap",
            "correlation",
            "corr",
            "matrix",
            "visualize",
            "visualise",
            "visualization",
            "visualisation",
        )
    )


def _looks_like_feature_importance_request(question: str) -> bool:
    q = (question or "").lower()
    return any(
        k in q
        for k in (
            "most important feature",
            "important feature",
            "feature importance",
            "most important column",
            "key feature",
            "key driver",
            "most influential",
        )
    )


def _handle_feature_importance_request(df: pd.DataFrame, question: str) -> QueryResponse:
    """Deterministic, grounded 'feature importance' response.

    Without a target variable and a predictive model, 'importance' is ambiguous.
    We use a transparent proxy: mean absolute correlation of each numeric column
    with the other numeric columns (sampled for speed).
    """

    warnings: list[str] = [
        "'feature importance' is approximated using mean absolute correlation across numeric columns (not a predictive model)",
    ]

    # Sample for speed.
    max_rows = 5000
    sample_df = df
    if len(df) > max_rows:
        sample_df = df.head(max_rows)
        warnings.append(f"sampled first {max_rows} rows for correlation-based importance")

    numeric_cols = sample_df.select_dtypes(include="number").columns.tolist()
    if len(numeric_cols) < 2:
        return QueryResponse(
            answer="I need at least two numeric columns to estimate feature importance via correlations.",
            query_type="error",
            warnings=warnings + ["not enough numeric columns"],
        )

    cols = [str(c) for c in numeric_cols]
    corr = sample_df[cols].corr(numeric_only=True).abs()
    # ignore self-correlation
    for c in cols:
        if c in corr.columns:
            corr.loc[c, c] = 0.0

    # mean absolute correlation per feature
    mean_abs = corr.mean(axis=1).sort_values(ascending=False)
    top = mean_abs.head(10)

    table = TableResult(
        columns=["feature", "mean_abs_correlation"],
        rows=[[str(idx), float(val)] for idx, val in top.items()],
    )

    chart = ChartSpec(
        type="bar",
        data={
            "data": [
                {
                    "type": "bar",
                    "x": [str(i) for i in top.index.tolist()],
                    "y": [float(v) for v in top.values.tolist()],
                    "name": "Mean |corr|",
                }
            ],
            "layout": {"title": "Feature importance (mean absolute correlation)"},
        },
    )

    best_feature = str(top.index[0])
    best_score = float(top.values[0])

    answer = (
        f"Using a correlation-based proxy (mean absolute correlation across numeric columns), '{best_feature}' appears most influential "
        f"in this dataset (score ≈ {best_score:.3f}). "
        "This does not prove causation; it indicates which variables move most consistently with others."
    )

    insights = [
        f"Top feature by mean |corr|: {best_feature} (≈ {best_score:.3f}).",
    ]
    if len(top) >= 3:
        insights.append(
            "Other strong features: "
            + ", ".join([f"{str(top.index[i])} (≈ {float(top.values[i]):.3f})" for i in range(1, min(4, len(top)))])
            + "."
        )

    return QueryResponse(
        answer=answer,
        table=table,
        chart=chart,
        insights=insights,
        warnings=warnings,
        query_type="analytical",
    )


def _handle_chart_request(df: pd.DataFrame, question: str) -> QueryResponse:
    """Generate a deterministic chart response (no LLM).

    Goal: Always return a usable chart spec for the frontend to render inline.
    """

    warnings: list[str] = []
    insights: list[str] = []

    # Sample for speed.
    max_rows = 1000
    sample_df = df
    if len(df) > max_rows:
        sample_df = df.head(max_rows)
        warnings.append(f"sampled first {max_rows} rows for chart")

    q = (question or "").lower()
    numeric_cols = sample_df.select_dtypes(include="number").columns.tolist()
    all_cols = [str(c) for c in df.columns.tolist()]

    # Default chart choice.
    chart: ChartSpec | None = None
    table: TableResult | None = None

    # Correlation heatmap request
    if ("heatmap" in q) or ("correlation" in q) or re.search(r"\bcorr\b", q):
        if len(numeric_cols) < 2:
            return QueryResponse(
                answer="I need at least two numeric columns to compute a correlation heatmap.",
                query_type="error",
                warnings=warnings + ["not enough numeric columns for correlation"],
            )

        # Limit to avoid unreadable heatmaps.
        max_cols = 12
        cols = [str(c) for c in numeric_cols[:max_cols]]
        if len(numeric_cols) > max_cols:
            warnings.append(f"using first {max_cols} numeric columns for correlation heatmap")

        corr_df = sample_df[cols].corr(numeric_only=True)
        labels = [str(c) for c in corr_df.columns.tolist()]
        z = _jsonify(corr_df.to_numpy().round(3).tolist())

        chart = ChartSpec(
            type="heatmap",
            data={
                "data": [
                    {
                        "type": "heatmap",
                        "x": labels,
                        "y": labels,
                        "z": z,
                        "colorscale": "RdBu",
                        "zmin": -1,
                        "zmax": 1,
                    }
                ],
                "layout": {
                    "title": "Correlation heatmap",
                    "xaxis": {"tickangle": -45},
                    "yaxis": {"automargin": True},
                },
            },
        )

        # Also provide top correlation pairs as a small table.
        try:
            pairs: list[list[Any]] = []
            for i in range(len(labels)):
                for j in range(i + 1, len(labels)):
                    v = float(corr_df.iloc[i, j])
                    if not math.isnan(v):
                        pairs.append([labels[i], labels[j], v])
            pairs.sort(key=lambda r: abs(r[2]), reverse=True)
            top_pairs = pairs[:10]
            table = TableResult(columns=["feature_a", "feature_b", "correlation"], rows=_jsonify(top_pairs))

            if top_pairs:
                insights.append(
                    f"Strongest correlation in sample: {top_pairs[0][0]} vs {top_pairs[0][1]} = {top_pairs[0][2]:.3f}."
                )
        except Exception:
            warnings.append("failed to compute top correlation pairs")

        answer = (
            "Here’s a correlation heatmap for the numeric columns (sampled for speed). "
            "Correlations close to 1 or -1 indicate strong linear relationships."
        )
        return QueryResponse(
            answer=answer,
            table=table,
            chart=chart,
            insights=insights,
            warnings=warnings,
            query_type="analytical",
        )

    _short_token_stopwords = {"at", "in", "on", "to", "of", "for", "and", "or", "vs", "x", "y"}
    _q_tokens = re.findall(r"[a-z0-9]+", (question or "").lower())

    # Histogram / distribution request
    if ("histogram" in q) or re.search(r"\bhist\b", q) or ("distribution" in q) or re.search(r"\bdist\b", q):
        if not numeric_cols:
            return QueryResponse(
                answer="I couldn’t find numeric columns to plot a histogram for this dataset.",
                query_type="error",
                warnings=warnings + ["no numeric columns for histogram"],
            )

        # Choose columns: prefer explicitly mentioned numeric columns, else first 1–2 numeric columns.
        mentioned: list[tuple[int, str]] = []
        for col in numeric_cols:
            col_str = str(col)
            if len(col_str) <= 3:
                token = col_str.lower()
                if token in _q_tokens and token not in _short_token_stopwords:
                    # approximate location for ordering
                    m = re.search(rf"\b{re.escape(token)}\b", (question or "").lower())
                else:
                    m = None
            else:
                pat = re.compile(rf"\b{re.escape(col_str)}\b", flags=re.IGNORECASE)
                m = pat.search(question)
            if m:
                mentioned.append((m.start(), col_str))
        mentioned.sort(key=lambda t: t[0])
        mentioned_numeric = [c for _, c in mentioned]

        selected_cols = mentioned_numeric[:3] if mentioned_numeric else [str(numeric_cols[0])]
        if not mentioned_numeric and len(numeric_cols) >= 2:
            selected_cols = [str(numeric_cols[0]), str(numeric_cols[1])]

        traces: list[dict[str, Any]] = []
        stats_rows: list[list[Any]] = []

        for col_name in selected_cols:
            series = sample_df[col_name].dropna()
            traces.append(
                {
                    "type": "histogram",
                    "x": _jsonify(series.tolist()),
                    "name": col_name,
                    "opacity": 0.65,
                }
            )
            # basic descriptive stats
            try:
                stats_rows.append(
                    [
                        col_name,
                        int(series.shape[0]),
                        float(series.mean()),
                        float(series.median()),
                        float(series.min()),
                        float(series.max()),
                    ]
                )
            except Exception:
                stats_rows.append([col_name, int(series.shape[0]), None, None, None, None])

        chart = ChartSpec(
            type="histogram",
            data={
                "data": traces,
                "layout": {
                    "title": "Histogram" if len(selected_cols) == 1 else "Histograms (overlay)",
                    "barmode": "overlay" if len(selected_cols) > 1 else "relative",
                },
            },
        )

        table = TableResult(
            columns=["column", "n", "mean", "median", "min", "max"],
            rows=_jsonify(stats_rows),
        )

        insights.append(
            "Histogram shows the distribution of values; skew or heavy tails may indicate outliers or non-normality."
        )
        insights.append(f"Plotted histogram for: {', '.join(selected_cols)}.")
        answer = (
            "Here’s a histogram-based view of the requested numeric columns (sampled for speed). "
            "If you want, tell me which single column to focus on or ask for a comparison between two columns."
        )

        return QueryResponse(
            answer=answer,
            table=table,
            chart=chart,
            insights=insights,
            warnings=warnings,
            query_type="analytical",
        )

    def _explicit_numeric_mentions() -> list[str]:
        # Prefer explicit mentions of actual column names.
        # For very short column names (e.g., "AT"), use token matching but avoid
        # common stopwords (so "at" doesn't accidentally match AT).
        mentioned: list[tuple[int, str]] = []
        for col in numeric_cols:
            col_str = str(col)
            if len(col_str) <= 3:
                token = col_str.lower()
                if token in _q_tokens and token not in _short_token_stopwords:
                    m = re.search(rf"\b{re.escape(token)}\b", (question or "").lower())
                else:
                    m = None
            else:
                pat = re.compile(rf"\b{re.escape(col_str)}\b", flags=re.IGNORECASE)
                m = pat.search(question)
            if m:
                mentioned.append((m.start(), col_str))
        mentioned.sort(key=lambda t: t[0])
        return [c for _, c in mentioned]

    def _strongest_corr_pair(cols: list[str]) -> tuple[str, str, float] | None:
        if len(cols) < 2:
            return None
        try:
            corr = sample_df[cols].corr(numeric_only=True).abs()
            # mask diagonal
            for c in cols:
                if c in corr.columns:
                    corr.loc[c, c] = 0.0
            best_val = float(corr.max().max())
            if math.isnan(best_val) or best_val <= 0:
                return None
            # locate first best
            best_idx = corr.stack().idxmax()
            x_name = str(best_idx[0])
            y_name = str(best_idx[1])
            return (x_name, y_name, best_val)
        except Exception:
            return None

    def _best_partner_for(col_name: str, cols: list[str]) -> tuple[str, float] | None:
        others = [c for c in cols if str(c) != str(col_name)]
        if not others:
            return None
        try:
            corr = sample_df[[str(col_name)] + [str(c) for c in others]].corr(numeric_only=True).abs()
            series = corr[str(col_name)].drop(labels=[str(col_name)], errors="ignore")
            best_val = float(series.max())
            if math.isnan(best_val) or best_val <= 0:
                return None
            best_col = str(series.idxmax())
            return (best_col, best_val)
        except Exception:
            return None

    if len(numeric_cols) >= 2:
        cols = [str(c) for c in numeric_cols]

        mentioned = _explicit_numeric_mentions()
        selected_corr: float | None = None

        if len(mentioned) >= 2:
            x_col, y_col = mentioned[0], mentioned[1]
        elif len(mentioned) == 1:
            x_col = mentioned[0]
            partner = _best_partner_for(x_col, cols)
            if partner:
                y_col, selected_corr = partner[0], partner[1]
            else:
                x_col, y_col = cols[0], cols[1]
        else:
            best = _strongest_corr_pair(cols)
            if best:
                x_col, y_col, selected_corr = best
            else:
                x_col, y_col = cols[0], cols[1]

        x = _jsonify(sample_df[x_col].tolist())
        y = _jsonify(sample_df[y_col].tolist())

        chart = ChartSpec(
            type="scatter",
            data={
                "data": [
                    {
                        "type": "scatter",
                        "mode": "markers",
                        "x": x,
                        "y": y,
                        "name": f"{y_col} vs {x_col}",
                    }
                ],
                "layout": {"title": f"Scatter: {y_col} vs {x_col}"},
            },
        )

        # Provide a small sample table too.
        sample_rows = sample_df.head(10)
        table = TableResult(
            columns=[str(c) for c in sample_rows.columns.tolist()],
            rows=_jsonify(sample_rows.to_numpy().tolist()),
        )

        insights.append(f"Plotted '{y_col}' against '{x_col}' (scatter).")
        try:
            corr = float(sample_df[[x_col, y_col]].corr().iloc[0, 1])
            if not math.isnan(corr):
                insights.append(f"Sample correlation between {x_col} and {y_col} is approximately {corr:.3f}.")
        except Exception:
            pass

        if selected_corr is not None:
            insights.append(
                f"Selected these columns because they appear strongly related in the sample (|corr| ≈ {selected_corr:.3f})."
            )

    elif len(numeric_cols) == 1:
        col = str(numeric_cols[0])
        series = sample_df[col].dropna()
        # Histogram via bar chart (bins).
        try:
            cut = pd.cut(series, bins=20)
            counts = cut.value_counts().sort_index()
            x = [str(i) for i in counts.index.tolist()]
            y = [int(v) for v in counts.tolist()]
            chart = ChartSpec(
                type="bar",
                data={
                    "data": [{"type": "bar", "x": x, "y": y, "name": f"Histogram of {col}"}],
                    "layout": {"title": f"Histogram (binned): {col}"},
                },
            )
            insights.append(f"Generated a binned histogram for '{col}'.")
        except Exception:
            chart = None
            warnings.append("could not generate histogram chart")

        sample_rows = sample_df.head(10)
        table = TableResult(
            columns=[str(c) for c in sample_rows.columns.tolist()],
            rows=_jsonify(sample_rows.to_numpy().tolist()),
        )
    else:
        # No numeric columns: try pie chart on first column's top values.
        if all_cols:
            col = all_cols[0]
            vc = sample_df[col].astype(str).value_counts().head(10)
            chart = ChartSpec(
                type="pie",
                data={
                    "data": [
                        {
                            "type": "pie",
                            "labels": vc.index.tolist(),
                            "values": [int(v) for v in vc.tolist()],
                        }
                    ],
                    "layout": {"title": f"Top values: {col}"},
                },
            )
            insights.append(f"No numeric columns detected; showing distribution of top values for '{col}'.")

    answer = (
        "Here are a few quick charts based on the dataset columns. "
        "If you tell me which column you want on the x/y axes (or which metric to compare), I can tailor the chart."
    )
    if warnings:
        answer = answer + " (Note: " + "; ".join(warnings) + ")"

    return QueryResponse(
        answer=answer,
        table=table,
        chart=chart,
        insights=insights,
        warnings=warnings,
        query_type="analytical",
    )


def _call_groq_for_json_plan(
    *,
    question: str,
    schema_payload: dict[str, Any],
    row_count: int,
    col_count: int,
    columns: list[str],
    groq_client: Any,
) -> str:
    """Call Groq/OpenAI-compatible client and return the raw assistant text."""

    system = (
        "You are a careful data analysis assistant.\n"
        "You MUST respond with ONLY valid JSON. No markdown. No code fences. No extra text.\n"
        "Return a single JSON object with exactly these keys and no others:\n"
        "- pandas_code: string | null\n"
        "- answer_template: string\n"
        "- chart_type: 'bar'|'line'|'scatter'|'pie'|null\n"
        "- insights: array of strings\n\n"
        "You will be given a DATAFRAME_SCHEMA JSON object mapping:\n"
        "{column_name: {dtype: string, sample_values: [3 values], null_count: number}}\n\n"
        "Rules for pandas_code:\n"
        "- DataFrame is available as `df` (pandas). Do NOT import anything.\n"
        "- No file I/O. No network.\n"
        "- Keep code under 20 lines.\n"
        "- Avoid explicit Python loops over rows for large datasets. Prefer vectorized pandas ops (groupby, value_counts, agg).\n"
        "- The final output MUST be stored in variable `result`. `result` may be a DataFrame, a scalar, or a dict.\n"
        "- If the question cannot be answered from the data, set pandas_code to null and explain why in answer_template.\n\n"
        "answer_template MUST be plain human-readable narrative text.\n"
        "Do NOT use placeholders like {result...} because they will not be post-processed.\n"
        "The narrative should directly reflect the computed result.\n\n"
        "Worked example (follow the format exactly):\n"
        "Question: Which category appears most often?\n"
        "JSON: {"
        "\"pandas_code\": \"vc = df['category'].value_counts().reset_index()\\nvc.columns = ['category','count']\\nresult = vc.head(10)\","
        "\"answer_template\": \"The most common category is the first row in the table; the table shows the top 10 category counts.\","
        "\"chart_type\": \"bar\","
        "\"insights\": [\"If one category dominates, consider stratified sampling or balancing for modeling.\"]"
        "}\n"
    )

    schema_str = json.dumps(schema_payload, ensure_ascii=False)
    user = (
        f"DATAFRAME_SCHEMA (JSON): {schema_str}\n"
        f"row_count: {int(row_count)}\n"
        f"col_count: {int(col_count)}\n"
        f"columns: {json.dumps(columns, ensure_ascii=False)}\n\n"
        f"Question: {question}"
    )

    # Support multiple client shapes.
    # 1) groq python client style: client.chat.completions.create(...)
    if hasattr(groq_client, "chat"):
        create = getattr(getattr(groq_client.chat, "completions", None), "create", None)
        if callable(create):
            resp = create(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                # let caller configure model; if missing, client may use default
                response_format={"type": "json_object"},
                temperature=0,
                max_tokens=800,
            )
            # Groq client returns objects; best-effort extract
            try:
                return resp.choices[0].message.content
            except Exception:
                return str(resp)

    # 2) If caller passed a callable, call it with the prompt
    if callable(groq_client):
        return str(groq_client(system=system, user=user))

    # 3) Unknown client
    raise RuntimeError("Unsupported groq_client; expected Groq client with chat.completions.create or a callable")


def _parse_llm_plan(text: str) -> _LLMPlan:
    raw = (text or "").strip()
    if not raw:
        raise ValueError("empty LLM response")

    # Strip accidental markdown fences
    fence = re.search(r"```(?:json)?\s*(.*?)\s*```", raw, flags=re.DOTALL | re.IGNORECASE)
    if fence:
        raw = fence.group(1).strip()

    data = json.loads(raw)
    if not isinstance(data, dict):
        raise TypeError("LLM response is not a JSON object")

    pandas_code = data.get("pandas_code")
    answer_template = data.get("answer_template")
    chart_type = data.get("chart_type")
    insights = data.get("insights")

    pandas_code_value: str | None
    if pandas_code is None:
        pandas_code_value = None
    elif isinstance(pandas_code, str) and pandas_code.strip():
        pandas_code_value = pandas_code.strip()
    else:
        raise ValueError("pandas_code must be a non-empty string or null")

    if not isinstance(answer_template, str) or not answer_template.strip():
        raise ValueError("answer_template must be a non-empty string")

    if chart_type is None:
        chart: ChartType | None = None
    elif chart_type in {"bar", "line", "scatter", "pie"}:
        chart = chart_type
    elif chart_type in {"null", "none", ""}:
        chart = None
    else:
        raise ValueError("chart_type must be bar|line|scatter|pie|null")

    if insights is None:
        insight_list: list[str] = []
    elif isinstance(insights, list) and all(isinstance(x, str) for x in insights):
        insight_list = [x.strip() for x in insights if x.strip()]
    else:
        raise ValueError("insights must be a list of strings")

    # Basic guardrails: require `result` to be assigned when code is provided.
    if pandas_code_value is not None and not re.search(r"^\s*result\s*=", pandas_code_value, flags=re.MULTILINE):
        raise ValueError("pandas_code must assign to variable 'result'")

    return _LLMPlan(
        pandas_code=pandas_code_value,
        answer_template=answer_template.strip(),
        chart_type=chart,
        insights=insight_list,
    )


@dataclass(frozen=True)
class _ExecOutcome:
    ok: bool
    result: Any | None
    warnings: list[str]


def _execute_pandas_code_sandboxed(code: str, df: pd.DataFrame, *, timeout_seconds: int) -> _ExecOutcome:
    """Execute code in a separate process with RestrictedPython and a hard timeout."""

    # If RestrictedPython isn't available, fall back to our in-process executor.
    # This keeps the system usable without extra installs while still applying basic safety checks.
    try:
        import RestrictedPython  # type: ignore  # noqa: F401
    except Exception:
        try:
            from app.services.code_executor import execute_pandas_code

            payload = execute_pandas_code(code=code, df=df, timeout_seconds=timeout_seconds)
            err = payload.get("error")
            if err:
                return _ExecOutcome(ok=False, result=None, warnings=[str(err)])
            return _ExecOutcome(ok=True, result=payload.get("result"), warnings=[])
        except Exception as e:
            return _ExecOutcome(
                ok=False,
                result=None,
                warnings=[f"fallback executor failed: {e.__class__.__name__}: {e}"],
            )

    try:
        import multiprocessing as mp

        ctx = mp.get_context("spawn")
        q: Any = ctx.Queue(maxsize=1)
        p = ctx.Process(target=_sandbox_worker, args=(code, df, q))
        p.daemon = True
        p.start()
        p.join(timeout_seconds)

        if p.is_alive():
            p.terminate()
            p.join(2)
            return _ExecOutcome(ok=False, result=None, warnings=[f"execution timed out after {timeout_seconds}s"])

        if not q.empty():
            payload = q.get_nowait()
            if isinstance(payload, dict):
                ok = bool(payload.get("ok"))
                result = payload.get("result")
                warnings = payload.get("warnings")
                if not isinstance(warnings, list):
                    warnings = []
                return _ExecOutcome(ok=ok, result=result, warnings=[str(w) for w in warnings])
        return _ExecOutcome(ok=False, result=None, warnings=["execution failed with no result"])
    except Exception as e:
        # Last resort fallback: in-process best-effort execution with basic checks.
        return _ExecOutcome(
            ok=False,
            result=None,
            warnings=[f"sandbox unavailable: {e.__class__.__name__}: {e}"],
        )


def _sandbox_worker(code: str, df: pd.DataFrame, out_q: Any) -> None:
    """Child process worker: block network, run RestrictedPython, return JSONable result."""

    warnings: list[str] = []
    try:
        # Best-effort network blocking for this process.
        import socket

        def _blocked(*_: Any, **__: Any) -> Any:
            raise RuntimeError("network access blocked")

        socket.socket = _blocked  # type: ignore[assignment]
        socket.create_connection = _blocked  # type: ignore[assignment]
    except Exception:
        warnings.append("failed to patch socket; network may not be fully blocked")

    try:
        from RestrictedPython import compile_restricted  # type: ignore
        from RestrictedPython.Guards import guarded_getattr, guarded_getitem, safe_builtins  # type: ignore
        from RestrictedPython.Eval import default_guarded_getiter  # type: ignore
    except Exception as e:
        out_q.put({"ok": False, "result": None, "warnings": [f"RestrictedPython missing: {e}"]})
        return

    # Minimal safe builtins. Note: safe_builtins intentionally excludes open/import.
    builtins: dict[str, Any] = dict(safe_builtins)
    # Allow harmless helpers (some may already be present).
    builtins.update({"len": len, "min": min, "max": max, "sum": sum, "sorted": sorted, "abs": abs, "round": round})

    # Compile restricted code.
    try:
        byte_code = compile_restricted(code, filename="<llm>", mode="exec")
    except Exception as e:
        out_q.put({"ok": False, "result": None, "warnings": [f"compile failed: {e}"]})
        return

    # Execution environment: df/pandas only.
    import pandas as pd
    import numpy as np

    glb: dict[str, Any] = {
        "__builtins__": builtins,
        # RestrictedPython guards needed for common code patterns.
        "_getattr_": guarded_getattr,
        "_getitem_": guarded_getitem,
        "_getiter_": default_guarded_getiter,
        "pd": pd,
        "np": np,
        "df": df.copy(),
    }

    try:
        exec(byte_code, glb, glb)  # noqa: S102
        if "result" not in glb:
            out_q.put({"ok": False, "result": None, "warnings": ["code did not set `result`"]})
            return
        out_q.put({"ok": True, "result": _jsonify(glb["result"]), "warnings": warnings})
    except Exception:
        tb = traceback.format_exc()[-2000:]
        out_q.put({"ok": False, "result": None, "warnings": warnings + [f"execution error: {tb}"]})


def _jsonify(value: Any) -> Any:
    """Convert pandas/numpy values to JSON-serializable Python types."""

    if value is None:
        return None
    if isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return None if math.isnan(value) else value

    # numpy
    try:
        import numpy as np  # type: ignore

        if isinstance(value, np.dtype):
            return str(value)
        if isinstance(value, np.generic):
            try:
                return _jsonify(value.item())
            except Exception:
                return str(value)
    except Exception:
        pass

    # pandas
    try:
        import pandas as pd  # type: ignore

        if value is pd.NA:
            return None
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if isinstance(value, pd.Timedelta):
            return str(value)
        if isinstance(value, pd.Series):
            return _jsonify(value.to_dict())
        if isinstance(value, pd.DataFrame):
            return _jsonify(value.to_dict(orient="records"))
    except Exception:
        pass

    if isinstance(value, dict):
        return {str(k): _jsonify(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonify(v) for v in value]

    return str(value)


def _result_to_table(result: Any) -> tuple[TableResult | None, list[str]]:
    warnings: list[str] = []

    # list[dict] -> table
    if isinstance(result, list) and result and all(isinstance(x, dict) for x in result):
        cols = list({k for row in result for k in row.keys()})
        cols.sort()
        rows = [[row.get(c) for c in cols] for row in result]
        return TableResult(columns=cols, rows=rows), warnings

    # dict of scalar/list -> try to make 2-column key/value
    if isinstance(result, dict):
        cols = ["key", "value"]
        rows = [[k, v] for k, v in result.items()]
        return TableResult(columns=cols, rows=rows), warnings

    # simple scalar -> no table
    return None, warnings


def _build_chart(chart_type: ChartType, table: TableResult) -> tuple[ChartSpec | None, list[str]]:
    warnings: list[str] = []
    if not table.columns or not table.rows:
        return None, warnings

    if len(table.columns) < 2:
        warnings.append("not enough columns to build a chart")
        return None, warnings

    max_points = 200
    rows = table.rows[:max_points]
    if len(table.rows) > max_points:
        warnings.append(f"chart sampled first {max_points} rows")

    x = [r[0] for r in rows]
    y = [r[1] for r in rows]

    if chart_type == "pie":
        data = {
            "data": [
                {
                    "type": "pie",
                    "labels": x,
                    "values": y,
                }
            ],
            "layout": {},
        }
        return ChartSpec(type="pie", data=data), warnings

    trace = {"type": chart_type, "x": x, "y": y}
    data = {"data": [trace], "layout": {}}
    return ChartSpec(type=chart_type, data=data), warnings


def _render_answer(template: str, *, df: pd.DataFrame, result: Any) -> str:
    ctx: dict[str, Any] = {
        "row_count": int(len(df)),
        "col_count": int(df.shape[1]),
        "columns": [str(c) for c in df.columns.tolist()],
        "result": result,
    }

    # Keep this safe and predictable: we only support formatting for dataset metadata.
    # LLM is instructed to NOT use result-derived placeholders.
    try:
        return template.format(
            row_count=ctx["row_count"],
            col_count=ctx["col_count"],
            columns=ctx["columns"],
        )
    except Exception:
        return template
