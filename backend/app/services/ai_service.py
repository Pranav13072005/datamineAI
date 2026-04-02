"""
ai_service.py — AI / LLM integration service.

Currently uses a mock response. Replace generate_response() body with
real Groq (or any OpenAI-compatible) API calls when ready.

The function receives:
  - question : the user's natural-language query
  - schema   : dict produced by data_service.get_schema()

It must return a string of valid Python/pandas code that stores its
final answer in a variable named `result`.
"""

import json
import re
from typing import Any

import httpx

from app.config import settings


class LLMError(RuntimeError):
    """Raised when the configured LLM provider returns an error."""

    def __init__(self, message: str, *, status_code: int | None = None, payload: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


def _extract_groq_error_detail(response: httpx.Response) -> str:
    """Best-effort extraction of Groq/OpenAI-compatible error message."""

    try:
        data: Any = response.json()
        if isinstance(data, dict):
            err = data.get("error")
            if isinstance(err, dict):
                message = err.get("message")
                if isinstance(message, str) and message.strip():
                    return message.strip()
        # Fall back to compact JSON string when possible.
        return json.dumps(data)[:2000]
    except Exception:
        text = (response.text or "").strip()
        return text[:2000] if text else f"HTTP {response.status_code} from Groq"


def generate_response(question: str, schema: dict) -> str:
    """
    Send a structured prompt to the LLM and return executable Python code
    (or a plain-text answer) as a string.

    Args:
        question : Natural-language question from the user.
        schema   : Dataset schema dict (columns, dtypes, sample_rows, row_count).

    Returns:
        A string. If the LLM returns Python/pandas code it should end with
        `result = <some expression>`. If it's a plain answer, wrap it:
        `result = "<answer text>"`.

    ──────────────────────────────────────────
    HOW TO WIRE UP GROQ (example):
    ──────────────────────────────────────────
    import httpx

    headers = {
        "Authorization": f"Bearer {settings.GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "llama3-70b-8192",
        "messages": [
            {"role": "system", "content": _build_system_prompt(schema)},
            {"role": "user",   "content": question},
        ],
        "temperature": 0,
    }
    r = httpx.post("https://api.groq.com/openai/v1/chat/completions",
                   json=payload, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]
    ──────────────────────────────────────────
    """

    if not settings.GROQ_API_KEY:
        columns = schema.get("columns", [])
        row_count = schema.get("row_count", 0)
        return (
            f"result = f\"Dataset has {row_count} rows with columns: {columns}. "
            f"(No Groq API key configured)\""
        )

    headers = {
        "Authorization": f"Bearer {settings.GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": settings.GROQ_MODEL,
        "messages": [
            {"role": "system", "content": _build_system_prompt(schema)},
            {"role": "user", "content": question},
        ],
        "temperature": 0,
        # Keep responses bounded and predictable.
        "max_tokens": 600,
    }
    
    try:
        r = httpx.post(
            "https://api.groq.com/openai/v1/chat/completions",
            json=payload,
            headers=headers,
            timeout=30,
        )
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            detail = _extract_groq_error_detail(e.response)
            raise LLMError(
                f"Groq request rejected ({e.response.status_code}): {detail}",
                status_code=e.response.status_code,
                payload={"model": settings.GROQ_MODEL},
            ) from e

        data = r.json()
        content = data["choices"][0]["message"]["content"]
        return _normalize_llm_output_to_code(content)
    except httpx.RequestError as e:
        raise LLMError(f"Groq request failed: {e.__class__.__name__}: {e}") from e


def _normalize_llm_output_to_code(content: str) -> str:
    """Normalize model output into executable Python that defines `result`.

    The UI expects `/query` to succeed even for non-data questions. Groq models
    sometimes return plain text or wrap code in markdown fences. This helper:
    - strips ``` fences if present
    - if no `result = ...` assignment exists, wraps the text as `result = "..."`
    """

    text = (content or "").strip()
    if not text:
        return 'result = ""'

    # Remove Markdown code fences.
    fence_match = re.search(r"```(?:python)?\s*(.*?)\s*```", text, flags=re.DOTALL | re.IGNORECASE)
    if fence_match:
        text = fence_match.group(1).strip()

    # If the model produced plain text (no `result =` anywhere), wrap it.
    # But if it looks like python/pandas code, return it as-is so the executor
    # can still run it and capture printed output.
    if not re.search(r"^\s*result\s*=", text, flags=re.MULTILINE):
        if _looks_like_python(text):
            return text
        return f"result = {json.dumps(text)}"

    return text


def _looks_like_python(text: str) -> bool:
    t = text.strip()
    if not t:
        return False
    # Heuristics: pandas dataframe usage, print(), assignments, def, etc.
    if re.search(r"\bdf\.", t):
        return True
    if re.search(r"\bpd\.", t):
        return True
    if re.search(r"\bprint\s*\(", t):
        return True
    if re.search(r"^\s*(for|while|if|def|class)\b", t, flags=re.MULTILINE):
        return True
    if re.search(r"=", t) and "result" not in t:
        return True
    return False


# ─── Prompt Builder (used when LLM is wired up) ───────────────────────────────

def _build_system_prompt(schema: dict) -> str:
    """
    Construct a clear system prompt that tells the LLM about the dataset
    and instructs it to return only executable Python/pandas code.
    """
    # Keep prompt stable and bounded; callers should already sanitize schema.
    schema_text = json.dumps(schema, indent=2, default=str)
    return f"""You are an expert data analyst. The user has uploaded a CSV dataset.
Your job is to answer their question by writing Python code using pandas.

Dataset schema:
{schema_text}

Rules:
1. The DataFrame is already loaded and available as the variable `df`.
2. Write Python code only — no explanations, no markdown fences.
3. Store the final answer in a variable called `result`.
4. `result` can be a scalar, list, dict, pd.Series, or pd.DataFrame.
5. Do NOT import os, sys, subprocess, or any file I/O libraries.
6. Do NOT use eval() or exec() inside your code.
7. Keep the code concise and correct.
"""
