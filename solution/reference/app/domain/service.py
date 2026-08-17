from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Any

from app.domain.models import (
    CheckpointStatus,
    ConflictError,
    DomainError,
    NotFoundError,
    RestoreStatus,
    WalOp,
)
from app.persistence.db import init_db
from app.scheduling.clock import Clock, SystemClock
from app.scheduling.failures import FailureInjector
from app.wal.checksum import wal_checksum


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


class PitrService:
    def __init__(
        self,
        conn: sqlite3.Connection,
        clock: Clock | None = None,
        injector: FailureInjector | None = None,
    ):
        self.conn = conn
        self.clock = clock or SystemClock()
        self.injector = injector or FailureInjector()
        init_db(self.conn)

    def health(self) -> dict[str, Any]:
        self.conn.execute("SELECT 1")
        return {"status": "ok", "clock_now": self.clock.now()}

    # ── records ──────────────────────────────────────────────────
    def put_record(
        self, key: str, value: Any, expected_version: int | None = None
    ) -> tuple[dict[str, Any], int]:
        if not key:
            raise DomainError("invalid_key", "key required")
        now = self.clock.now()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.injector.maybe_fail_db("put")
            row = self.conn.execute("SELECT * FROM records WHERE key=?", (key,)).fetchone()
            if row is None:
                if expected_version not in (None, 0):
                    raise ConflictError("stale_version", "record does not exist")
                version = 1
                status_code = 201
            else:
                if expected_version is not None and int(row["version"]) != int(expected_version):
                    raise ConflictError("stale_version", "record version conflict")
                version = int(row["version"]) + 1
                status_code = 200
            lsn = self._next_lsn()
            checksum = wal_checksum(lsn=lsn, op=WalOp.PUT.value, key=key, value=value, tx_time=now)
            self.conn.execute(
                """
                INSERT INTO wal(lsn, op, key, value_json, tx_time, checksum, created_at)
                VALUES (?, 'PUT', ?, ?, ?, ?, ?)
                """,
                (lsn, key, json.dumps(value, sort_keys=True), now, checksum, now),
            )
            self.conn.execute(
                """
                INSERT INTO records(key, value_json, version, deleted, lsn, updated_at)
                VALUES (?, ?, ?, 0, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                  value_json=excluded.value_json,
                  version=excluded.version,
                  deleted=0,
                  lsn=excluded.lsn,
                  updated_at=excluded.updated_at
                """,
                (key, json.dumps(value, sort_keys=True), version, lsn, now),
            )
            self._audit("RECORD_PUT", entity_id=key, detail={"lsn": lsn, "version": version}, now=now)
            if self.injector.consume_process_interrupt():
                raise DomainError("process_interrupted", "injected interrupt", status_code=503)
            self.conn.commit()
        except DomainError:
            self.conn.rollback()
            raise
        except Exception:
            self.conn.rollback()
            raise
        return self.get_record(key), status_code

    def delete_record(self, key: str, expected_version: int | None = None) -> dict[str, Any]:
        now = self.clock.now()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute("SELECT * FROM records WHERE key=?", (key,)).fetchone()
            if row is None or int(row["deleted"]) == 1:
                raise NotFoundError("record_not_found", f"unknown key: {key}")
            if expected_version is not None and int(row["version"]) != int(expected_version):
                raise ConflictError("stale_version", "record version conflict")
            lsn = self._next_lsn()
            checksum = wal_checksum(lsn=lsn, op=WalOp.DELETE.value, key=key, value=None, tx_time=now)
            self.conn.execute(
                """
                INSERT INTO wal(lsn, op, key, value_json, tx_time, checksum, created_at)
                VALUES (?, 'DELETE', ?, NULL, ?, ?, ?)
                """,
                (lsn, key, now, checksum, now),
            )
            self.conn.execute(
                """
                UPDATE records SET deleted=1, version=version+1, lsn=?, updated_at=?
                WHERE key=?
                """,
                (lsn, now, key),
            )
            self._audit("RECORD_DELETE", entity_id=key, detail={"lsn": lsn}, now=now)
            self.conn.commit()
        except DomainError:
            self.conn.rollback()
            raise
        except Exception:
            self.conn.rollback()
            raise
        return {"key": key, "deleted": True, "lsn": lsn}

    def get_record(self, key: str) -> dict[str, Any]:
        row = self.conn.execute("SELECT * FROM records WHERE key=?", (key,)).fetchone()
        if row is None or int(row["deleted"]) == 1:
            raise NotFoundError("record_not_found", f"unknown key: {key}")
        return self._record_dict(row)

    def list_records(self, include_deleted: bool = False) -> list[dict[str, Any]]:
        if include_deleted:
            rows = self.conn.execute("SELECT * FROM records ORDER BY key").fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM records WHERE deleted=0 ORDER BY key"
            ).fetchall()
        return [self._record_dict(r) for r in rows]

    def _record_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        value = None if row["value_json"] is None else json.loads(row["value_json"])
        return {
            "key": row["key"],
            "value": value,
            "version": row["version"],
            "deleted": bool(row["deleted"]),
            "lsn": row["lsn"],
            "updated_at": row["updated_at"],
        }

    def _next_lsn(self) -> int:
        row = self.conn.execute("SELECT COALESCE(MAX(lsn), 0) AS m FROM wal").fetchone()
        return int(row["m"]) + 1

    # ── wal ──────────────────────────────────────────────────────
    def list_wal(self, from_lsn: int = 0) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM wal WHERE lsn >= ? ORDER BY lsn", (int(from_lsn),)
        ).fetchall()
        out = []
        for r in rows:
            expected = wal_checksum(
                lsn=int(r["lsn"]),
                op=r["op"],
                key=r["key"],
                value=None if r["value_json"] is None else json.loads(r["value_json"]),
                tx_time=float(r["tx_time"]),
            )
            if expected != r["checksum"]:
                raise DomainError("corrupt_wal", f"checksum mismatch at lsn {r['lsn']}", 500)
            d = dict(r)
            d["value"] = None if r["value_json"] is None else json.loads(r["value_json"])
            del d["value_json"]
            out.append(d)
        return out

    # ── checkpoints ──────────────────────────────────────────────
    def create_checkpoint(self) -> dict[str, Any]:
        now = self.clock.now()
        cid = _id("ckpt")
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute(
                """
                INSERT INTO checkpoints(
                  checkpoint_id, upto_lsn, tx_time, snapshot_json, status,
                  lease_owner, lease_expires_at, created_at, updated_at
                ) VALUES (?, 0, NULL, NULL, 'PENDING', NULL, NULL, ?, ?)
                """,
                (cid, now, now),
            )
            self._audit("CHECKPOINT_CREATED", entity_id=cid, detail={}, now=now)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return self.get_checkpoint(cid)

    def get_checkpoint(self, checkpoint_id: str) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT * FROM checkpoints WHERE checkpoint_id=?", (checkpoint_id,)
        ).fetchone()
        if not row:
            raise NotFoundError("checkpoint_not_found", f"unknown checkpoint: {checkpoint_id}")
        return self._ckpt_dict(row)

    def list_checkpoints(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM checkpoints ORDER BY created_at, checkpoint_id"
        ).fetchall()
        return [self._ckpt_dict(r) for r in rows]

    def _ckpt_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        snap = None if row["snapshot_json"] is None else json.loads(row["snapshot_json"])
        return {
            "checkpoint_id": row["checkpoint_id"],
            "upto_lsn": row["upto_lsn"],
            "tx_time": row["tx_time"],
            "snapshot": snap,
            "status": row["status"],
            "lease_owner": row["lease_owner"],
            "lease_expires_at": row["lease_expires_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def run_checkpoint(
        self, checkpoint_id: str, worker_id: str, lease_seconds: float = 30.0
    ) -> dict[str, Any]:
        if not worker_id:
            raise DomainError("invalid_worker", "worker_id required")
        now = self.clock.now()
        self.injector.maybe_fail_db("checkpoint")
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute(
                "SELECT * FROM checkpoints WHERE checkpoint_id=?", (checkpoint_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("checkpoint_not_found", f"unknown checkpoint: {checkpoint_id}")
            if row["status"] == CheckpointStatus.READY.value:
                self.conn.commit()
                return self.get_checkpoint(checkpoint_id)
            held = (
                row["lease_owner"]
                and row["lease_expires_at"] is not None
                and float(row["lease_expires_at"]) > now
                and row["lease_owner"] != worker_id
            )
            if held:
                raise ConflictError("lease_held", f"checkpoint held by {row['lease_owner']}")
            claimed = self.conn.execute(
                """
                UPDATE checkpoints
                SET lease_owner=?, lease_expires_at=?, updated_at=?
                WHERE checkpoint_id=? AND status='PENDING'
                """,
                (worker_id, now + float(lease_seconds), now, checkpoint_id),
            )
            if claimed.rowcount != 1 and row["status"] != CheckpointStatus.PENDING.value:
                raise ConflictError("checkpoint_not_runnable", "checkpoint not pending")

            max_lsn = self.conn.execute("SELECT COALESCE(MAX(lsn), 0) AS m FROM wal").fetchone()["m"]
            live = self.conn.execute(
                "SELECT * FROM records WHERE deleted=0 ORDER BY key"
            ).fetchall()
            snapshot = {
                r["key"]: {
                    "value": json.loads(r["value_json"]) if r["value_json"] else None,
                    "version": r["version"],
                    "lsn": r["lsn"],
                }
                for r in live
            }
            if self.injector.consume_process_interrupt():
                raise DomainError("process_interrupted", "injected interrupt", status_code=503)
            self.conn.execute(
                """
                UPDATE checkpoints
                SET upto_lsn=?, tx_time=?, snapshot_json=?, status='READY',
                    lease_owner=NULL, lease_expires_at=NULL, updated_at=?
                WHERE checkpoint_id=?
                """,
                (
                    int(max_lsn),
                    now,
                    json.dumps(snapshot, sort_keys=True),
                    now,
                    checkpoint_id,
                ),
            )
            self._audit(
                "CHECKPOINT_READY",
                entity_id=checkpoint_id,
                detail={"upto_lsn": int(max_lsn)},
                now=now,
            )
            self.conn.commit()
        except DomainError:
            self.conn.rollback()
            raise
        except Exception:
            self.conn.rollback()
            raise
        return self.get_checkpoint(checkpoint_id)

    # ── restore ──────────────────────────────────────────────────
    def create_restore(
        self, as_of_ms: float | None = None, as_of_lsn: int | None = None
    ) -> dict[str, Any]:
        if (as_of_ms is None) == (as_of_lsn is None):
            raise DomainError("invalid_restore", "exactly one of as_of_ms or as_of_lsn required")
        now = self.clock.now()
        rid = _id("rst")
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute(
                """
                INSERT INTO restore_jobs(
                  restore_id, as_of_ms, as_of_lsn, status, applied_lsn,
                  lease_owner, lease_expires_at, last_error, created_at, updated_at
                ) VALUES (?, ?, ?, 'PENDING', 0, NULL, NULL, NULL, ?, ?)
                """,
                (rid, as_of_ms, as_of_lsn, now, now),
            )
            self._audit(
                "RESTORE_CREATED",
                entity_id=rid,
                detail={"as_of_ms": as_of_ms, "as_of_lsn": as_of_lsn},
                now=now,
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return self.get_restore(rid)

    def get_restore(self, restore_id: str) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT * FROM restore_jobs WHERE restore_id=?", (restore_id,)
        ).fetchone()
        if not row:
            raise NotFoundError("restore_not_found", f"unknown restore: {restore_id}")
        return dict(row)

    def run_restore(
        self, restore_id: str, worker_id: str, lease_seconds: float = 30.0
    ) -> dict[str, Any]:
        if not worker_id:
            raise DomainError("invalid_worker", "worker_id required")
        now = self.clock.now()
        self.injector.maybe_fail_db("restore")
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            job = self.conn.execute(
                "SELECT * FROM restore_jobs WHERE restore_id=?", (restore_id,)
            ).fetchone()
            if not job:
                raise NotFoundError("restore_not_found", f"unknown restore: {restore_id}")
            if job["status"] == RestoreStatus.SUCCEEDED.value:
                self.conn.commit()
                return self.get_restore(restore_id)
            held = (
                job["lease_owner"]
                and job["lease_expires_at"] is not None
                and float(job["lease_expires_at"]) > now
                and job["lease_owner"] != worker_id
            )
            if held:
                raise ConflictError("lease_held", f"restore held by {job['lease_owner']}")
            self.conn.execute(
                """
                UPDATE restore_jobs
                SET status='RUNNING', lease_owner=?, lease_expires_at=?, updated_at=?
                WHERE restore_id=?
                """,
                (worker_id, now + float(lease_seconds), now, restore_id),
            )

            target_lsn = job["as_of_lsn"]
            target_ms = job["as_of_ms"]

            ckpt = None
            if target_lsn is not None:
                ckpt = self.conn.execute(
                    """
                    SELECT * FROM checkpoints
                    WHERE status='READY' AND upto_lsn <= ?
                    ORDER BY upto_lsn DESC, created_at DESC
                    LIMIT 1
                    """,
                    (int(target_lsn),),
                ).fetchone()
            else:
                ckpt = self.conn.execute(
                    """
                    SELECT * FROM checkpoints
                    WHERE status='READY' AND tx_time <= ?
                    ORDER BY upto_lsn DESC, created_at DESC
                    LIMIT 1
                    """,
                    (float(target_ms),),
                ).fetchone()

            snapshot: dict[str, Any] = {}
            from_lsn = 0
            if ckpt is not None:
                snapshot = json.loads(ckpt["snapshot_json"] or "{}")
                from_lsn = int(ckpt["upto_lsn"])

            wal_rows = self.conn.execute(
                "SELECT * FROM wal WHERE lsn > ? ORDER BY lsn", (from_lsn,)
            ).fetchall()
            applied = from_lsn
            for rec in wal_rows:
                expected = wal_checksum(
                    lsn=int(rec["lsn"]),
                    op=rec["op"],
                    key=rec["key"],
                    value=None if rec["value_json"] is None else json.loads(rec["value_json"]),
                    tx_time=float(rec["tx_time"]),
                )
                if expected != rec["checksum"]:
                    raise DomainError("corrupt_wal", f"checksum mismatch at lsn {rec['lsn']}", 500)
                if target_lsn is not None and int(rec["lsn"]) > int(target_lsn):
                    break
                if target_ms is not None and float(rec["tx_time"]) > float(target_ms):
                    break
                if rec["op"] == WalOp.PUT.value:
                    snapshot[rec["key"]] = {
                        "value": json.loads(rec["value_json"]),
                        "version": snapshot.get(rec["key"], {}).get("version", 0) + 1,
                        "lsn": rec["lsn"],
                    }
                else:
                    snapshot.pop(rec["key"], None)
                applied = int(rec["lsn"])

            if self.injector.consume_process_interrupt():
                raise DomainError("process_interrupted", "injected interrupt", status_code=503)

            self.conn.execute("DELETE FROM records")
            for key, item in snapshot.items():
                self.conn.execute(
                    """
                    INSERT INTO records(key, value_json, version, deleted, lsn, updated_at)
                    VALUES (?, ?, ?, 0, ?, ?)
                    """,
                    (
                        key,
                        json.dumps(item["value"], sort_keys=True),
                        int(item["version"]),
                        int(item["lsn"]),
                        now,
                    ),
                )
            self.conn.execute(
                """
                UPDATE restore_jobs
                SET status='SUCCEEDED', applied_lsn=?, lease_owner=NULL, lease_expires_at=NULL,
                    last_error=NULL, updated_at=?
                WHERE restore_id=?
                """,
                (applied, now, restore_id),
            )
            self._audit(
                "RESTORE_SUCCEEDED",
                entity_id=restore_id,
                detail={"applied_lsn": applied},
                now=now,
            )
            self.conn.commit()
        except DomainError:
            self.conn.rollback()
            raise
        except Exception:
            self.conn.rollback()
            raise
        return self.get_restore(restore_id)

    def force_version(self, key: str, expected_version: int) -> dict[str, Any]:
        now = self.clock.now()
        cur = self.conn.execute(
            "UPDATE records SET version=version+1, updated_at=? WHERE key=? AND version=?",
            (now, key, expected_version),
        )
        if cur.rowcount != 1:
            raise ConflictError("stale_version", "record version conflict")
        self.conn.commit()
        return self.get_record(key)

    def list_audit(self, limit: int = 100) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 1000))
        rows = self.conn.execute(
            "SELECT * FROM audit_events ORDER BY created_at, event_id LIMIT ?", (limit,)
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["detail"] = json.loads(d["detail"])
            out.append(d)
        return out

    def metrics(self) -> dict[str, Any]:
        live = self.conn.execute(
            "SELECT COUNT(*) AS c FROM records WHERE deleted=0"
        ).fetchone()["c"]
        deleted = self.conn.execute(
            "SELECT COUNT(*) AS c FROM records WHERE deleted=1"
        ).fetchone()["c"]
        wal_n = self.conn.execute("SELECT COUNT(*) AS c FROM wal").fetchone()["c"]
        max_lsn = self.conn.execute("SELECT COALESCE(MAX(lsn), 0) AS m FROM wal").fetchone()["m"]
        ckpt = self.conn.execute(
            "SELECT COUNT(*) AS c FROM checkpoints WHERE status='READY'"
        ).fetchone()["c"]
        rst = self.conn.execute(
            "SELECT COUNT(*) AS c FROM restore_jobs WHERE status='SUCCEEDED'"
        ).fetchone()["c"]
        audit = self.conn.execute("SELECT COUNT(*) AS c FROM audit_events").fetchone()["c"]
        return {
            "records_live": live,
            "records_deleted": deleted,
            "wal_records": wal_n,
            "max_lsn": max_lsn,
            "checkpoints_ready": ckpt,
            "restores_succeeded": rst,
            "audit_events": audit,
            "clock_now": self.clock.now(),
        }

    def _audit(
        self, action: str, *, entity_id: str | None, detail: dict[str, Any], now: float | None = None
    ) -> None:
        ts = self.clock.now() if now is None else now
        self.conn.execute(
            """
            INSERT INTO audit_events(event_id, action, entity_id, detail, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (_id("aud"), action, entity_id, json.dumps(detail or {}, sort_keys=True), ts),
        )
