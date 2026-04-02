from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.asyncio
async def test_query_persists_history(async_client, sample_csv_file: Path) -> None:
    # Upload dataset
    with sample_csv_file.open("rb") as f:
        files = {"file": ("sample.csv", f, "text/csv")}
        upload = await async_client.post("/datasets/upload", files=files)

    assert upload.status_code == 200
    dataset_id = upload.json()["dataset_id"]

    # Run a descriptive query (no Groq call required)
    query_payload = {"dataset_id": dataset_id, "question": "summarize the dataset"}
    query = await async_client.post("/query", json=query_payload)

    assert query.status_code == 200
    query_json = query.json()

    warnings = query_json.get("warnings") or []
    assert "failed to persist query history" not in warnings

    # Verify history contains the entry
    history = await async_client.get(f"/history/{dataset_id}")

    assert history.status_code == 200
    items = history.json()
    assert isinstance(items, list)
    assert len(items) >= 1
    assert items[0]["question"] == "summarize the dataset"
    assert isinstance(items[0].get("answer"), str)
