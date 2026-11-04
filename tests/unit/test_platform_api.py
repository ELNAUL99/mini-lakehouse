"""Tests for the Platform API. Skipped if FastAPI/Starlette's TestClient is absent."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from mini_lakehouse.platform_api.app import DatasetRegistry, create_app

pytestmark = pytest.mark.unit


def _client() -> TestClient:
    return TestClient(create_app(DatasetRegistry()))


def _dataset_body(**kw):
    body = {
        "name": "telemetry.events",
        "owner": "platform",
        "key": "player_id",
        "retention_days": 7,
        "schema": {
            "version": 1,
            "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "player_id", "type": "string"},
            ],
        },
    }
    body.update(kw)
    return body


def test_healthz():
    assert _client().get("/healthz").json() == {"status": "ok"}


def test_register_and_get_dataset():
    client = _client()
    assert client.post("/datasets", json=_dataset_body()).status_code == 201
    view = client.get("/datasets/telemetry.events").json()
    assert view["owner"] == "platform"
    assert view["schema_version"] == 1


def test_get_unknown_dataset_404():
    assert _client().get("/datasets/nope").status_code == 404


def test_register_rejects_key_not_in_schema():
    resp = _client().post("/datasets", json=_dataset_body(key="missing"))
    assert resp.status_code == 422


def test_propose_compatible_schema_accepted():
    client = _client()
    client.post("/datasets", json=_dataset_body())
    new_schema = {
        "schema": {
            "version": 2,
            "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "player_id", "type": "string"},
                {"name": "platform", "type": "string", "required": False},
            ],
        }
    }
    assert client.post("/schemas/telemetry.events/versions", json=new_schema).status_code == 201


def test_propose_breaking_schema_rejected():
    client = _client()
    client.post("/datasets", json=_dataset_body())
    breaking = {
        "schema": {
            "version": 2,
            "fields": [{"name": "event_id", "type": "long"}],  # retype + removed player_id
        }
    }
    resp = client.post("/schemas/telemetry.events/versions", json=breaking)
    assert resp.status_code == 409
    assert resp.json()["detail"]["error"] == "incompatible_schema_change"
