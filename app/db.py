"""SQLite persistence for immutable upload batches and pinned report jobs."""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS batches (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    kind         TEXT NOT NULL CHECK (kind IN ('transactions','samples')),
    filename     TEXT NOT NULL,
    byte_size    INTEGER NOT NULL,
    sha256       TEXT NOT NULL,
    submitted_rows INTEGER NOT NULL,
    stored_rows  INTEGER NOT NULL,
    stored_path  TEXT NOT NULL,
    uploaded_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    INTEGER NOT NULL REFERENCES batches(id),
    event_id    TEXT NOT NULL,
    account_id  TEXT NOT NULL,
    occurred_at INTEGER NOT NULL,
    ingested_at INTEGER NOT NULL,
    amount_minor INTEGER NOT NULL CHECK (amount_minor >= 0),
    amount_text TEXT NOT NULL,
    action      TEXT NOT NULL CHECK (action IN ('revise','cancel')),
    raw_hash    TEXT NOT NULL,
    row_number  INTEGER NOT NULL
);

-- One semantic revision per (event, ingestion instant). The service rejects a
-- batch outright if the same pair arrives with different content, so this
-- index is the durable enforcement of that rule.
CREATE UNIQUE INDEX IF NOT EXISTS ux_txn_event_ingest
    ON transactions(event_id, ingested_at);
CREATE INDEX IF NOT EXISTS ix_txn_account_visible
    ON transactions(account_id, ingested_at);
CREATE INDEX IF NOT EXISTS ix_txn_event_ingested
    ON transactions(event_id, ingested_at);
CREATE INDEX IF NOT EXISTS ix_txn_account_occurred
    ON transactions(account_id, occurred_at);
CREATE INDEX IF NOT EXISTS ix_txn_batch ON transactions(batch_id);

CREATE TABLE IF NOT EXISTS samples (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    INTEGER NOT NULL REFERENCES batches(id),
    sample_id   TEXT NOT NULL,
    account_id  TEXT NOT NULL,
    audit_at    INTEGER NOT NULL,
    label       INTEGER NOT NULL CHECK (label IN (0,1)),
    row_number  INTEGER NOT NULL,
    UNIQUE(sample_id)
);

CREATE TABLE IF NOT EXISTS reports (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    public_id     TEXT NOT NULL UNIQUE,
    status        TEXT NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending','running','ready','failed')),
    txn_batch_hi  INTEGER NOT NULL,
    sample_batch_hi INTEGER NOT NULL,
    created_at    INTEGER NOT NULL,
    started_at    INTEGER,
    completed_at  INTEGER,
    sample_count  INTEGER,
    output_path   TEXT,
    output_sha256 TEXT,
    output_bytes  INTEGER,
    manifest_json TEXT NOT NULL,
    error         TEXT
);
CREATE INDEX IF NOT EXISTS ix_reports_status ON reports(status);
"""


class Database:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def init(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)

    @contextmanager
    def write_transaction(self):
        """Serialised IMMEDIATE transaction; all-or-nothing batch commit."""
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=30000")
            with self._lock:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    yield conn
                except BaseException:
                    conn.execute("ROLLBACK")
                    raise
                else:
                    conn.execute("COMMIT")
        finally:
            conn.close()

    @contextmanager
    def read(self):
        conn = self.connect()
        try:
            yield conn
        finally:
            conn.close()
