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
import httpx

from app.utils.config import settings


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
        "model": "llama3-70b-8192",
        "messages": [
            {"role": "system", "content": _build_system_prompt(schema)},
            {"role": "user", "content": question},
        ],
        "temperature": 0,
    }
    
    try:
        r = httpx.post(
            "https://api.groq.com/openai/v1/chat/completions",
            json=payload,
            headers=headers,
            timeout=30,
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]
    except httpx.HTTPError as e:
        raise Exception(f"Groq API error: {e}")


# ─── Prompt Builder (used when LLM is wired up) ───────────────────────────────

def _build_system_prompt(schema: dict) -> str:
    """
    Construct a clear system prompt that tells the LLM about the dataset
    and instructs it to return only executable Python/pandas code.
    """
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
