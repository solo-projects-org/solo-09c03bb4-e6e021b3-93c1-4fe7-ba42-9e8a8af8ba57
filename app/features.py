"""As-of feature construction.

For every sample we take the audit instant ``T`` and:

1. restrict source to transaction batches pinned in the report manifest
   (``batch_id <= txn_batch_hi``);
2. for each event, retain exactly one revision: the latest one with
   ``ingested_at <= T`` (a revision that only becomes visible after ``T`` is
   never used);
3. count that retained revision only if it is a revision (cancels are dropped)
   and its **own** ``occurred_at`` falls in ``[T - 7d, T)``.

Because step 3 uses the retained revision's occurrence time, an event revised
to a time outside the window does not inherit its previous in-window value.
"""

from __future__ import annotations

import hashlib
import io
import sqlite3
from dataclasses import dataclass
from decimal import Decimal

from .csvparse import WINDOW_SECONDS

OUTPUT_HEADERS = [
    "sample_id", "account_id", "audit_at", "window_start_at", "window_end_at",
    "txn_count_7d", "txn_amount_7d", "label",
]

_PINNED_SQL = """
WITH sample_set AS (
    SELECT s.id, s.batch_id, s.sample_id, s.account_id, s.audit_at, s.label
    FROM samples s
    WHERE s.batch_id <= :sample_hi
),
joined AS (
    SELECT
        ss.id AS sample_row_id, ss.batch_id, ss.sample_id, ss.account_id,
        ss.audit_at, ss.label, t.id AS txn_id, t.event_id, t.occurred_at,
        t.ingested_at, t.amount_minor, t.action,
        ROW_NUMBER() OVER (
            PARTITION BY ss.sample_id, t.event_id
            ORDER BY t.ingested_at DESC, t.id DESC
        ) AS rn
    FROM sample_set ss
    JOIN transactions t
      ON t.account_id = ss.account_id
     AND t.ingested_at <= ss.audit_at
     AND t.batch_id <= :txn_hi
),
latest AS (
    SELECT * FROM joined WHERE rn = 1
),
agg AS (
    SELECT sample_row_id,
           COUNT(*) AS txn_count,
           COALESCE(SUM(amount_minor), 0) AS amount_minor
    FROM latest
    WHERE action = 'revise'
      AND occurred_at >= audit_at - :window
      AND occurred_at < audit_at
    GROUP BY sample_row_id
)
SELECT ss.id AS sample_row_id, ss.batch_id, ss.sample_id, ss.account_id,
       ss.audit_at, ss.label,
       COALESCE(a.txn_count, 0) AS txn_count,
       COALESCE(a.amount_minor, 0) AS amount_minor
FROM sample_set ss
LEFT JOIN agg a ON a.sample_row_id = ss.id
ORDER BY ss.batch_id, ss.id
"""


@dataclass(frozen=True)
class ReportRow:
    sample_id: str
    account_id: str
    audit_at: int
    count: int
    amount_minor: int
    label: int

    @property
    def amount_text(self) -> str:
        return str((Decimal(self.amount_minor) / 100).quantize(Decimal("0.01")))


def compute_rows(conn: sqlite3.Connection, txn_batch_hi: int,
                 sample_batch_hi: int) -> list[ReportRow]:
    cur = conn.execute(_PINNED_SQL, {
        "txn_hi": txn_batch_hi,
        "sample_hi": sample_batch_hi,
        "window": WINDOW_SECONDS,
    })
    rows = []
    for r in cur:
        rows.append(ReportRow(
            sample_id=r["sample_id"], account_id=r["account_id"],
            audit_at=r["audit_at"], count=r["txn_count"],
            amount_minor=r["amount_minor"], label=r["label"],
        ))
    return rows


def render_csv(rows: list[ReportRow]) -> bytes:
    import csv

    buf = io.StringIO(newline="")
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(OUTPUT_HEADERS)
    for r in rows:
        writer.writerow([
            r.sample_id, r.account_id, r.audit_at,
            r.audit_at - WINDOW_SECONDS, r.audit_at,
            r.count, r.amount_text, r.label,
        ])
    return buf.getvalue().encode("utf-8")


def source_digest(conn: sqlite3.Connection, txn_batch_hi: int,
                  sample_batch_hi: int) -> tuple[str, int, int]:
    """Digest the exact pinned source rows, independent of insertion order.

    Used to prove that a regenerated report was built from the same version.
    """
    h = hashlib.sha256()
    n_txn = 0
    for r in conn.execute(
        """SELECT event_id, account_id, occurred_at, ingested_at,
                  amount_text, action, raw_hash, batch_id
           FROM transactions WHERE batch_id <= ?
           ORDER BY batch_id, id""", (txn_batch_hi,)
    ):
        h.update(("\x1f".join(str(x) for x in tuple(r)) + "\x1e").encode())
        n_txn += 1
    n_sample = 0
    for r in conn.execute(
        """SELECT sample_id, account_id, audit_at, label, batch_id
           FROM samples WHERE batch_id <= ?
           ORDER BY batch_id, id""", (sample_batch_hi,)
    ):
        h.update(("\x1f".join(str(x) for x in tuple(r)) + "\x1e").encode())
        n_sample += 1
    return h.hexdigest(), n_txn, n_sample


def iter_preview(rows: list[ReportRow], limit: int):
    for r in rows[:limit]:
        yield (r.sample_id, r.account_id, r.audit_at,
               r.audit_at - WINDOW_SECONDS, r.audit_at,
               r.count, r.amount_text, r.label)
