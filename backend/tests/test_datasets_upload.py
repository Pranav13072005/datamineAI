from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.asyncio
async def test_upload_dataset_returns_dataset_id(async_client, sample_csv_file: Path) -> None:
    with sample_csv_file.open("rb") as f:
        files = {"file": ("sample.csv", f, "text/csv")}
        r = await async_client.post("/datasets/upload", files=files)

    assert r.status_code == 200
    payload = r.json()

    assert "dataset_id" in payload
    assert isinstance(payload["dataset_id"], str)
    assert payload["dataset_id"]
