# Point-in-Time WAL Restore Engine

## Business scenario

You are building the local persistence engine behind an operations console.

Operators mutate a durable key-value catalog. The system must survive process crashes, take **atomic checkpoints**, and restore the catalog to any earlier **point in time** by replaying an append-only **write-ahead log (WAL)**.

Two writers must never receive the same **LSN**. A checkpoint crash must not leave a half-written snapshot. Restore must not invent or drop committed mutations.

This is not a generic CRUD app. The hard part is **LSN monotonicity + atomic checkpoints + PITR replay + concurrent writers + crash atomicity**.

## Goals

Implement FastAPI + SQLite supporting:

- durable PUT / GET / DELETE of records
- append-only WAL with monotonic LSN
- checksummed WAL records
- atomic checkpoints (latest committed LSN + snapshot)
- point-in-time restore by FakeClock timestamp **or** LSN
- lease-based checkpoint/restore workers
- optimistic concurrency on record `version`
- restart recovery from the same SQLite file
- metrics + append-only audit
- FakeClock and failure injection (no long sleeps)

## Domain model

### Record

- `key` (unique)
- `value` JSON
- `version` (optimistic lock, starts at 1)
- `deleted` (bool)
- `updated_at`
- `lsn` (LSN of last mutation)

### WAL record

- `lsn` (monotonic integer, primary key)
- `op`: `PUT` | `DELETE`
- `key`
- `value` JSON or null
- `tx_time` (FakeClock now)
- `checksum` (stable hash of canonical payload)
- `created_at`

LSNs start at 1 and increase by 1 with **no gaps** for committed mutations.

### Checkpoint

- `checkpoint_id`
- `upto_lsn`
- `tx_time`
- `snapshot` JSON (map of live key → {value, version, lsn})
- `status`: `PENDING` | `READY` | `FAILED`
- `lease_owner` / `lease_expires_at`
- `created_at`

A READY checkpoint is a consistent cut: it includes every committed mutation with `lsn <= upto_lsn` and no later mutation.

### Restore job

- `restore_id`
- `as_of_ms` (optional)
- `as_of_lsn` (optional)
- `status`: `PENDING` | `RUNNING` | `SUCCEEDED` | `FAILED`
- `applied_lsn`
- `lease_owner` / `lease_expires_at`

Exactly one of `as_of_ms` or `as_of_lsn` is required.

## Required API

- `PUT /records/{key}` → 200/201
- `GET /records/{key}`
- `DELETE /records/{key}`
- `GET /records`
- `GET /wal?from_lsn=0`
- `POST /checkpoints`
- `GET /checkpoints`
- `GET /checkpoints/{checkpoint_id}`
- `POST /checkpoints/{checkpoint_id}/run`
- `POST /restore`
- `GET /restore/{restore_id}`
- `POST /restore/{restore_id}/run`
- `GET /metrics`
- `GET /audit`
- `GET /health`
- `POST /_test/clock/advance`
- `POST /_test/failures/arm`

Factory:

```python
from app import create_app
app = create_app(db_path="pitr.db", clock=FakeClock(...))
```

### Put record

```json
{ "value": {"sku": "A1", "qty": 3}, "expected_version": null }
```

- First put → 201, version 1
- Later put with matching `expected_version` → 200, version+1
- Stale `expected_version` → 409
- Always appends a WAL `PUT` with a new LSN in the same transaction

### Delete

Deletes are logical: record stays with `deleted=true` and a WAL `DELETE` is appended. GET of a deleted key returns 404. Restore may resurrect a key if the as-of cut is before the delete.

### Checkpoint

`POST /checkpoints` creates a PENDING checkpoint and returns it.

`POST /checkpoints/{id}/run` with `{ "worker_id": "w1", "lease_seconds": 30 }` claims a lease and materializes the snapshot at the current max committed LSN.

Checkpoint creation must be atomic: interrupt/rollback leaves no READY checkpoint and does not mutate live records.

### Restore

```json
{ "as_of_ms": 1700000100 }
```

or

```json
{ "as_of_lsn": 4 }
```

`POST /restore/{id}/run` loads the latest READY checkpoint with `upto_lsn` ≤ target (or empty snapshot if none), then replays WAL records in LSN order whose LSN is greater than the checkpoint and whose `lsn`/`tx_time` is ≤ target.

Replay rules:

- `PUT` upserts key
- `DELETE` marks deleted
- After restore SUCCEEDED, `GET /records` matches history at that cut
- Later live writes after the restore are discarded (restore replaces live state in one transaction)

## Concurrency

Concurrent PUTs must receive **distinct consecutive LSNs**. No two committed WAL rows share an LSN.

Checkpoint/restore workers use durable leases. A live lease cannot be stolen. Expired leases can be recovered by another worker.

Record updates use optimistic `version`.

## Crash / interrupt

Failure injection may abort mid-checkpoint or mid-restore. Aborted work must leave no partial READY checkpoint and no partial restore (transaction rollback).

## Checksums

WAL checksum is a stable SHA-256 hex of a canonical JSON object:

```json
{"lsn": 1, "op": "PUT", "key": "k", "value": {...}, "tx_time": 1.0}
```

`value` is omitted for DELETE. Keys sorted. Restore/checkpoint must ignore nothing silently: if checksum mismatches, fail the operation (4xx/5xx DomainError) rather than apply a corrupt record.

## Metrics (live)

- `records_live`
- `records_deleted`
- `wal_records`
- `max_lsn`
- `checkpoints_ready`
- `restores_succeeded`
- `audit_events`

## Definition of done

1. Mutations persist with unique monotonic LSNs.
2. Checkpoints capture a consistent prefix of the WAL.
3. PITR by time and by LSN reconstructs exact historical state.
4. Concurrent writers never duplicate LSNs.
5. Checkpoint/restore interrupts do not commit partial state.
6. Restart reopens the same SQLite file with identical data.
7. `tests/test.sh` writes `/logs/verifier/reward.txt`; reference reaches full reward.

## Constraints

- Offline only
- No external databases/object stores
- No long sleeps — use FakeClock
- Do not weaken verifier files
