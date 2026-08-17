# Point-in-Time WAL Restore Engine — Odyssey draft paste fields

## Title
Point-in-Time WAL Restore Engine

## Working slug
point-in-time-wal-restore-engine

## Collection family
Product clone

## Task family
systems_integration

## Verifier family
programmatic

## Objective
Build a crash-safe key-value persistence engine on FastAPI + SQLite with an append-only write-ahead log, monotonic LSNs, checksummed records, atomic checkpoints, and point-in-time restore by timestamp or LSN. Concurrent writers must never share an LSN. Checkpoint and restore workers use durable leases. Injected interrupts must roll back partial snapshots. Restart must reopen the same SQLite file unchanged. Done means tests/test.sh writes Harbor reward 1.0 to /logs/verifier/reward.txt for the reference solution.

## Motivation
Operators need to rewind catalog state after a bad mutation without restoring an entire machine image. Naive dumps race with writers and leave half-written snapshots. This task grades real PITR mechanics—WAL prefixes, consistent checkpoints, replay, and crash atomicity—offline without external databases.

## Environment summary
Python 3.12 slim image with FastAPI, Uvicorn, Pydantic, HTTPX, and Pytest installed at image-build time. The image bakes only the incomplete starter under /app/app; tests/ and solution/ are not present in the agent image. Runtime is fully offline (network_mode = none). Persistence uses local SQLite with FakeClock and failure injection. No cloud SDKs or object stores.

## Difficulty explanation
Difficulty is composing monotonic gap-free LSN assignment under concurrent writers, atomic checkpoint snapshots of a WAL prefix, restore that replays PUT/DELETE to an exact historical cut by time or LSN, checksum fail-closed integrity, lease exclusion on checkpoint/restore jobs, optimistic record versions, and transaction rollback on injected interrupts. A CRUD table with a timestamp column will pass some visible checks and fail held-out PITR/concurrency channels.

## Expert time estimate (hours)
11

## Oracle strategy
solution/solve.sh installs solution/reference/app into /app/app (Harbor) or ./app (local). The reference uses SQLite BEGIN IMMEDIATE for LSN allocation, SHA-256 WAL checksums, atomic checkpoint snapshots, lease-claimed restore replay, FakeClock, and failure injection. After install, tests/test.sh yields reward 1.0.

## Verification strategy
Canonical entrypoint: tests/test.sh.

Visible split (tests/visible/): health, put/get, WAL append, basic checkpoint.

Hidden/decisive split (tests/hidden/): monotonic checksummed LSNs, checkpoint prefix + restore-by-LSN, restore-by-time including deletes, restart same SQLite file, concurrent unique LSNs, checkpoint interrupt atomicity, restore interrupt atomicity, optimistic version conflicts, metrics/audit consistency.

Writes /logs/verifier/reward.txt (float [0,1]), reward.json, and a weighted channel report.

## Binary success condition
/logs/verifier/reward.txt contains 1 (or 1.0) after tests/test.sh, meaning every weighted channel passed—including concurrent LSN uniqueness and PITR replay—not merely the visible happy path.

## Partial score strategy
Weighted independent channels (visible suite, monotonic LSN, checkpoint+PITR LSN, time+delete PITR, restart, concurrent LSN, checkpoint interrupt, restore interrupt, optimistic locking, metrics/audit). Reward equals the sum of passed channel weights (max 1.0). Untouched stubs score near 0; only a crash-safe WAL/PITR design reaches 1.0.

## Anticipated exploits
1. In-memory map only → defeated by restart reopen of the same SQLite file.
2. Timestamp overwrite without WAL → defeated by restore-to-LSN prefix tests.
3. Python locks without BEGIN IMMEDIATE → defeated by multi-connection concurrent LSN tests.
4. Non-atomic checkpoint file writes → defeated by interrupt rollback (PENDING, snapshot null).
5. Restore that mutates live rows incrementally → defeated by interrupt leaving original keys.
6. Fake metrics/audit → defeated by live assertions after real mutations.
7. Reading held-out tests from the image → Dockerfile bakes only app/.
8. Hard-coded reward/output → defeated by random keys/IDs and independent state checks.

## Resources (ZIP task.toml; draft may be higher)
CPU (millis): 2000
Memory (MB): 4096
Storage (MB): 5120
GPU count: 0
Agent timeout (s): 7200
Verifier timeout (s): 600

## Network
Mode: none
Justification: Fully offline after image build. Local SQLite WAL/PITR tests need no egress.
