from __future__ import annotations

import re
from typing import Any


_WORD_RE = re.compile(r"[a-z0-9']+")


def _tokens(text: str) -> set[str]:
    return set(_WORD_RE.findall(text.lower()))


def _normalize(text: str) -> str:
    return " ".join(_WORD_RE.findall(text.lower()))


def classify_query(question: str, schema: dict) -> str:
    """Classify a natural language question.

    Returns one of:
    - "descriptive": summarize/describe/overview/schema-style questions
    - "smalltalk": greetings/thanks/identity questions
    - "analytical": anything requiring computation/comparison (default)

    Notes:
    - Uses simple keyword matching only (no LLM).
    - `schema` is accepted for future improvements (e.g., detecting column references)
      but is not required for the baseline heuristics.
    """

    q = (question or "").strip()
    if not q:
        return "analytical"

    q_norm = _normalize(q)
    toks = _tokens(q)

    # Smalltalk / meta
    smalltalk_tokens = {"hi", "hello", "hey", "thanks", "thx"}
    if toks.intersection(smalltalk_tokens):
        return "smalltalk"

    smalltalk_phrases = (
        "thank you",
        "who are you",
        "what are you",
        "what can you do",
    )
    if any(p in q_norm for p in smalltalk_phrases):
        return "smalltalk"

    # Descriptive / dataset overview
    descriptive_phrases = (
        "describe",
        "summary",
        "summarize",
        "overview",
        "tell me about",
        "about the data",
        "about this data",
        "about the dataset",
        "about this dataset",
        "what columns",
        "which columns",
        "list columns",
        "show columns",
        "schema",
        "data dictionary",
        "data types",
        "dtypes",
        "missing values",
        "null values",
        "nulls",
        "duplicates",
        "duplicate rows",
        "how many rows",
        "how many columns",
    )
    if any(p in q_norm for p in descriptive_phrases):
        return "descriptive"

    # Single-token descriptive signals
    if "columns" in toks or "schema" in toks or "dtype" in toks:
        return "descriptive"

    # Analytical signals (not strictly necessary, but helps when mixed intent)
    analytical_phrases = (
        "top",
        "bottom",
        "most",
        "least",
        "highest",
        "lowest",
        "average",
        "mean",
        "median",
        "sum",
        "total",
        "count",
        "compare",
        "correlation",
        "trend",
        "increase",
        "decrease",
        "group by",
        "distribution",
        "percentage",
        "ratio",
    )
    if any(p in q_norm for p in analytical_phrases):
        return "analytical"

    # Light use of schema: if question names a column, it's likely analytical.
    try:
        columns: list[str] = []
        if isinstance(schema, dict):
            raw_cols: Any = schema.get("columns") or schema.get("fields")
            if isinstance(raw_cols, list):
                columns = [str(c).lower() for c in raw_cols]
            elif isinstance(raw_cols, dict):
                columns = [str(k).lower() for k in raw_cols.keys()]

        if columns:
            for col in columns:
                if col and col in q_norm:
                    return "analytical"
    except Exception:
        # Never let classification fail the request.
        pass

    return "analytical"
