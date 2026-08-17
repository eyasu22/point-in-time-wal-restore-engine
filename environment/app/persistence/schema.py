SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS records (
    key TEXT PRIMARY KEY,
    value_json TEXT,
    version INTEGER NOT NULL,
    deleted INTEGER NOT NULL DEFAULT 0,
    lsn INTEGER NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS wal (
    lsn INTEGER PRIMARY KEY,
    op TEXT NOT NULL,
    key TEXT NOT NULL,
    value_json TEXT,
    tx_time REAL NOT NULL,
    checksum TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS checkpoints (
    checkpoint_id TEXT PRIMARY KEY,
    upto_lsn INTEGER NOT NULL DEFAULT 0,
    tx_time REAL,
    snapshot_json TEXT,
    status TEXT NOT NULL,
    lease_owner TEXT,
    lease_expires_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS restore_jobs (
    restore_id TEXT PRIMARY KEY,
    as_of_ms REAL,
    as_of_lsn INTEGER,
    status TEXT NOT NULL,
    applied_lsn INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_expires_at REAL,
    last_error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_events (
    event_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    entity_id TEXT,
    detail TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wal_time ON wal(tx_time, lsn);
CREATE INDEX IF NOT EXISTS idx_records_deleted ON records(deleted);
"""
