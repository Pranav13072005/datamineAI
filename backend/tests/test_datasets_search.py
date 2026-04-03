from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_datasets_search_returns_structure(async_client) -> None:
    r = await async_client.get("/datasets/search", params={"q": "revenue", "top_k": 10})
    assert r.status_code == 200

    payload = r.json()
    assert payload["query"] == "revenue"
    assert isinstance(payload["results"], list)

    # Under the SQLite test DB (no pgvector), results should be empty.
    assert payload["results"] == []
