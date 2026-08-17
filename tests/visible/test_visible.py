"""Visible tests for Point-in-Time WAL Restore Engine."""

from __future__ import annotations

import os
import sys

import pytest
from fastapi.testclient import TestClient

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


@pytest.fixture()
def client(tmp_path):
    from app import create_app
    from app.scheduling.clock import FakeClock

    db = tmp_path / "visible.db"
    try:
        app = create_app(db_path=str(db), clock=FakeClock())
    except TypeError:
        app = create_app()
    return TestClient(app)


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json().get("status") == "ok"


def test_put_get_record(client):
    r = client.put("/records/sku_a", json={"value": {"qty": 3}})
    if r.status_code == 404:
        pytest.skip("records API not implemented")
    assert r.status_code == 201
    body = r.json()
    assert body["key"] == "sku_a"
    assert body["version"] == 1
    g = client.get("/records/sku_a")
    assert g.status_code == 200
    assert g.json()["value"]["qty"] == 3


def test_wal_appended(client):
    r = client.put("/records/sku_b", json={"value": {"qty": 1}})
    if r.status_code not in (200, 201):
        pytest.skip("not implemented")
    wal = client.get("/wal")
    if wal.status_code == 404:
        pytest.skip("wal not implemented")
    assert wal.status_code == 200
    rows = wal.json()
    assert len(rows) >= 1
    assert rows[0]["lsn"] == 1
    assert rows[0]["op"] == "PUT"


def test_basic_checkpoint(client):
    p = client.put("/records/sku_c", json={"value": {"qty": 9}})
    if p.status_code not in (200, 201):
        pytest.skip("not implemented")
    c = client.post("/checkpoints")
    if c.status_code == 404:
        pytest.skip("checkpoints not implemented")
    assert c.status_code == 201
    cid = c.json()["checkpoint_id"]
    run = client.post(f"/checkpoints/{cid}/run", json={"worker_id": "w1", "lease_seconds": 30})
    assert run.status_code == 200
    assert run.json()["status"] == "READY"
    assert run.json()["upto_lsn"] >= 1
