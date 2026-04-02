from __future__ import annotations

import pandas as pd

from app.services.descriptive_handler import handle_descriptive


def test_handle_descriptive_returns_stable_response() -> None:
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    resp = handle_descriptive(df, "summarize the dataset")

    assert resp.query_type == "descriptive"
    assert isinstance(resp.answer, str)
    assert "rows" in resp.answer.lower()
