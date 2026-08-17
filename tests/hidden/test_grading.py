"""Held-out grading for point-in-time WAL restore engine."""

from __future__ import annotations

import os
import sys
import threading
import uuid

import pytest
from fastapi.testclient import TestClient

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


@pytest.fixture()
def ctx(tmp_path):
    from app import create_app
    from app.scheduling.clock import FakeClock

    db = tmp_path / f"{uuid.uuid4().hex}.db"
    clock = FakeClock()
    app = create_app(db_path=str(db), clock=clock)
    return {"client": TestClient(app), "clock": clock, "db": str(db), "app": app}


def _put(c, key, value, ver=None):
    body = {"value": value}
    if ver is not None:
        body["expected_version"] = ver
    return c.put(f"/records/{key}", json=body)


def _ckpt(c):
    cid = c.post("/checkpoints").json()["checkpoint_id"]
    r = c.post(f"/checkpoints/{cid}/run", json={"worker_id": "w1", "lease_seconds": 30})
    assert r.status_code == 200
    return r.json()


def _restore_lsn(c, lsn, worker="w1"):
    job = c.post("/restore", json={"as_of_lsn": lsn})
    assert job.status_code == 201
    rid = job.json()["restore_id"]
    run = c.post(f"/restore/{rid}/run", json={"worker_id": worker, "lease_seconds": 30})
    assert run.status_code == 200
    return run.json()


def test_h_monotonic_lsn_and_checksum(ctx):
    c = ctx["client"]
    assert _put(c, "a", {"n": 1}).status_code == 201
    assert _put(c, "b", {"n": 2}).status_code == 201
    assert _put(c, "a", {"n": 3}, ver=1).status_code == 200
    wal = c.get("/wal").json()
    lsns = [r["lsn"] for r in wal]
    assert lsns == [1, 2, 3]
    assert all(r["checksum"] for r in wal)
    assert c.get("/metrics").json()["max_lsn"] == 3
    assert c.get("/records/a").json()["value"]["n"] == 3


def test_h_checkpoint_prefix_then_pitr_lsn(ctx):
    c = ctx["client"]
    _put(c, "k1", {"v": 1})
    _put(c, "k2", {"v": 2})
    ck = _ckpt(c)
    assert ck["upto_lsn"] == 2
    assert "k1" in ck["snapshot"] and "k2" in ck["snapshot"]
    _put(c, "k3", {"v": 3})
    _put(c, "k1", {"v": 9}, ver=1)
    job = _restore_lsn(c, 2)
    assert job["status"] == "SUCCEEDED"
    keys = {r["key"] for r in c.get("/records").json()}
    assert keys == {"k1", "k2"}
    assert c.get("/records/k1").json()["value"]["v"] == 1
    assert c.get("/records/k3").status_code == 404


def test_h_pitr_by_time_and_delete(ctx):
    c = ctx["client"]
    t0 = ctx["clock"].now()
    _put(c, "x", {"v": 1})
    c.post("/_test/clock/advance", json={"seconds": 10})
    _put(c, "y", {"v": 2})
    c.post("/_test/clock/advance", json={"seconds": 10})
    d = c.delete("/records/x")
    assert d.status_code == 200
    assert c.get("/records/x").status_code == 404
    # restore to just after first write
    job = c.post("/restore", json={"as_of_ms": t0 + 1})
    assert job.status_code == 201
    rid = job.json()["restore_id"]
    run = c.post(f"/restore/{rid}/run", json={"worker_id": "w", "lease_seconds": 30})
    assert run.status_code == 200
    keys = {r["key"] for r in c.get("/records").json()}
    assert keys == {"x"}
    assert c.get("/records/x").json()["value"]["v"] == 1


def test_h_restart_same_sqlite(ctx):
    from app import create_app
    from app.scheduling.clock import FakeClock

    c = ctx["client"]
    _put(c, "r1", {"v": 5})
    _ckpt(c)
    app2 = create_app(db_path=ctx["db"], clock=FakeClock(1_700_000_100.0))
    c2 = TestClient(app2)
    assert c2.get("/records/r1").json()["value"]["v"] == 5
    assert c2.get("/wal").json()[0]["lsn"] == 1
    assert c2.get("/metrics").json()["checkpoints_ready"] == 1
    app2.state.store.conn.close()


def test_h_concurrent_unique_lsn(ctx):
    from app.domain.service import PitrService
    from app.persistence.db import connect
    from app.scheduling.clock import FakeClock

    db = ctx["db"]
    results = []
    errors = []

    def worker(i):
        store = PitrService(connect(db), clock=FakeClock())
        try:
            rec, code = store.put_record(f"k{i}", {"i": i})
            results.append((rec["lsn"], rec["key"], code))
        except Exception as exc:
            errors.append(str(exc))
        finally:
            store.conn.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    lsns = sorted(r[0] for r in results)
    assert lsns == list(range(1, 13))
    store = PitrService(connect(db), clock=FakeClock())
    assert store.metrics()["wal_records"] == 12
    assert store.metrics()["max_lsn"] == 12
    store.conn.close()


def test_h_checkpoint_interrupt_atomic(ctx):
    c = ctx["client"]
    _put(c, "a", {"v": 1})
    cid = c.post("/checkpoints").json()["checkpoint_id"]
    c.post("/_test/failures/arm", json={"process_interrupt": True})
    bad = c.post(f"/checkpoints/{cid}/run", json={"worker_id": "w", "lease_seconds": 30})
    assert bad.status_code in (400, 503)
    ck = c.get(f"/checkpoints/{cid}").json()
    assert ck["status"] == "PENDING"
    assert ck["snapshot"] is None
    ok = c.post(f"/checkpoints/{cid}/run", json={"worker_id": "w", "lease_seconds": 30})
    assert ok.status_code == 200
    assert ok.json()["status"] == "READY"


def test_h_restore_interrupt_atomic(ctx):
    c = ctx["client"]
    _put(c, "keep", {"v": 1})
    _put(c, "later", {"v": 2})
    rid = c.post("/restore", json={"as_of_lsn": 1}).json()["restore_id"]
    c.post("/_test/failures/arm", json={"process_interrupt": True})
    bad = c.post(f"/restore/{rid}/run", json={"worker_id": "w", "lease_seconds": 30})
    assert bad.status_code in (400, 503)
    # live state unchanged
    keys = {r["key"] for r in c.get("/records").json()}
    assert keys == {"keep", "later"}
    ok = c.post(f"/restore/{rid}/run", json={"worker_id": "w", "lease_seconds": 30})
    assert ok.json()["status"] == "SUCCEEDED"
    keys2 = {r["key"] for r in c.get("/records").json()}
    assert keys2 == {"keep"}


def test_h_optimistic_concurrency(ctx):
    c = ctx["client"]
    _put(c, "opt", {"v": 1})
    ver = c.get("/records/opt").json()["version"]
    ok = c.post("/records/opt/optimistic", json={"expected_version": ver})
    assert ok.status_code == 200
    stale = c.post("/records/opt/optimistic", json={"expected_version": ver})
    assert stale.status_code == 409
    stale_put = _put(c, "opt", {"v": 2}, ver=ver)
    assert stale_put.status_code == 409


def test_h_metrics_and_audit(ctx):
    c = ctx["client"]
    _put(c, "m", {"v": 1})
    _ckpt(c)
    _restore_lsn(c, 1)
    m = c.get("/metrics").json()
    assert m["records_live"] == 1
    assert m["wal_records"] >= 1
    assert m["checkpoints_ready"] >= 1
    assert m["restores_succeeded"] >= 1
    assert m["audit_events"] >= 1
    actions = {a["action"] for a in c.get("/audit").json()}
    assert "RECORD_PUT" in actions
    assert "CHECKPOINT_READY" in actions
    assert "RESTORE_SUCCEEDED" in actions
