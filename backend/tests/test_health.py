from __future__ import annotations


def test_health_endpoint_ok(client) -> None:
    r = client.get("/health")
    assert r.status_code == 200

    payload = r.json()
    assert payload["status"] == "ok"
    assert "database" in payload
    assert "llm" in payload
