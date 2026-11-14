"""Domain service: batch ingestion, report pinning, atomic output."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from . import features
from .config import Settings
from .csvparse import BatchRejected, parse_samples, parse_transactions
from .db import Database

UTC = timezone.utc


def now_epoch() -> int:
    return int(time.time())


class ConflictError(Exception):
    def __init__(self, message: str, existing_batch_id: int | None = None):
        super().__init__(message)
        self.message = message
        self.existing_batch_id = existing_batch_id


def _hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class AppService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.settings.ensure_dirs()
        self.db = Database(settings.db_path)
        self.db.init()

    # ------------------------------------------------------------------ #
    # Uploads
    # ------------------------------------------------------------------ #
    def _store_upload(self, kind: str, filename: str, data: bytes,
                      parsed_rows: int, stored_rows: int,
                      insert_fn) -> int:
        """Persist bytes first, then rows, in one DB transaction.

        If the DB commit fails, remove the just-written file. Files are named
        by content hash plus a UUID so a retry never overwrites a source that
        an existing report is pinned to.
        """
        digest = hashlib.sha256(data).hexdigest()
        safe_name = Path(filename).name[:120] or f"{kind}.csv"
        target = self.settings.uploads_dir / f"{digest[:16]}-{uuid.uuid4().hex[:12]}-{safe_name}"
        tmp = self.settings.uploads_dir / f".tmp-{uuid.uuid4().hex}"
        tmp.write_bytes(data)
        os.replace(tmp, target)
        try:
            with self.db.write_transaction() as conn:
                cur = conn.execute(
                    """INSERT INTO batches(kind, filename, byte_size, sha256,
                                           submitted_rows, stored_rows, stored_path,
                                           uploaded_at)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (kind, safe_name, len(data), digest, parsed_rows,
                     stored_rows, str(target), now_epoch()),
                )
                batch_id = cur.lastrowid
                inserted = insert_fn(conn, batch_id)
                if inserted is not None and inserted != stored_rows:
                    conn.execute(
                        "UPDATE batches SET stored_rows=? WHERE id=?",
                        (inserted, batch_id))
            return batch_id
        except BaseException:
            try:
                target.unlink(missing_ok=True)
            finally:
                pass
            raise

    def upload_transactions(self, filename: str, data: bytes) -> dict:
        parsed = parse_transactions(data)
        stored = {"rows": 0, "skipped_same_value": 0}

        def insert(conn: sqlite3.Connection, batch_id: int) -> None:
            conflicts: list[dict] = []
            seen: dict[tuple[str, int], str] = {}
            for t in parsed.transactions:
                key = (t.event_id, t.ingested_at)
                if key in seen:
                    if seen[key] != t.raw_hash:
                        conflicts.append({
                            "row": t.row_number, "field": "event_id",
                            "code": "same_ingest_conflict",
                            "message": f"事件 {t.event_id} 在入库时刻 {t.ingested_at} "
                                       "在本批次内出现不同取值，整批拒收。",
                            "value": t.event_id})
                    else:
                        stored["skipped_same_value"] += 1
                    continue
                seen[key] = t.raw_hash
                prior = conn.execute(
                    """SELECT raw_hash FROM transactions
                       WHERE event_id=? AND ingested_at=?""",
                    key).fetchone()
                if prior is not None:
                    if prior["raw_hash"] != t.raw_hash:
                        conflicts.append({
                            "row": t.row_number, "field": "event_id",
                            "code": "same_ingest_conflict",
                            "message": f"事件 {t.event_id} 在入库时刻 {t.ingested_at} "
                                       "已存在不同取值的修订，整批拒收。",
                            "value": t.event_id})
                    else:
                        stored["skipped_same_value"] += 1
                    continue
                conn.execute(
                    """INSERT INTO transactions(batch_id, event_id, account_id,
                           occurred_at, ingested_at, amount_minor, amount_text,
                           action, raw_hash, row_number)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (batch_id, t.event_id, t.account_id, t.occurred_at,
                     t.ingested_at, t.amount_minor, t.amount_text, t.action,
                     t.raw_hash, t.row_number),
                )
                stored["rows"] += 1
            if conflicts:
                raise BatchRejected(
                    "交易批次整批拒收：同一事件同一入库时刻出现不同取值。",
                    conflicts)
            return stored["rows"]

        batch_id = self._store_upload(
            "transactions", filename, data, len(parsed.transactions),
            len(parsed.transactions), insert)
        return {
            "batch_id": batch_id,
            "kind": "transactions",
            "rows": len(parsed.transactions),
            "stored_rows": stored["rows"],
            "skipped_same_value": stored["skipped_same_value"],
            "events": len({t.event_id for t in parsed.transactions}),
            "sha256": hashlib.sha256(data).hexdigest(),
        }

    def upload_samples(self, filename: str, data: bytes) -> dict:
        parsed = parse_samples(data)

        def insert(conn: sqlite3.Connection, batch_id: int) -> None:
            conflicts = []
            for s in parsed.samples:
                prior = conn.execute(
                    "SELECT batch_id FROM samples WHERE sample_id=?",
                    (s.sample_id,)).fetchone()
                if prior is not None:
                    conflicts.append({
                        "row": s.row_number, "field": "sample_id",
                        "code": "duplicate_sample_id",
                        "message": f"样本号 {s.sample_id} 已在批次 "
                                   f"#{prior['batch_id']} 中存在；样本号必须全局唯一。",
                        "value": s.sample_id})
                    continue
                conn.execute(
                    """INSERT INTO samples(batch_id, sample_id, account_id,
                           audit_at, label, row_number)
                       VALUES (?,?,?,?,?,?)""",
                    (batch_id, s.sample_id, s.account_id, s.audit_at,
                     s.label, s.row_number),
                )
            if conflicts:
                raise BatchRejected("样本批次整批拒收：样本号重复。", conflicts)

        batch_id = self._store_upload(
            "samples", filename, data, len(parsed.samples),
            len(parsed.samples), insert)
        return {
            "batch_id": batch_id,
            "kind": "samples",
            "rows": len(parsed.samples),
            "sha256": hashlib.sha256(data).hexdigest(),
        }

    # ------------------------------------------------------------------ #
    # Reports
    # ------------------------------------------------------------------ #
    def create_report(self) -> dict:
        """Freeze the source high-watermarks at the instant of creation.

        Only batches committed before this transaction's BEGIN IMMEDIATE
        snapshot are included; later uploads get a higher batch_id and are
        invisible to this report forever, even if they arrive during
        computation.
        """
        with self.db.write_transaction() as conn:
            txn_hi = conn.execute(
                "SELECT COALESCE(MAX(id),0) AS v FROM batches WHERE kind='transactions'"
            ).fetchone()["v"]
            sample_hi = conn.execute(
                "SELECT COALESCE(MAX(id),0) AS v FROM batches WHERE kind='samples'"
            ).fetchone()["v"]
            has_samples = conn.execute(
                "SELECT COUNT(*) AS n FROM samples WHERE batch_id <= ?", (sample_hi,)
            ).fetchone()["n"]
            if sample_hi == 0 or has_samples == 0:
                raise ConflictError("尚无已上传样本，无法生成报告。")
            if txn_hi == 0:
                raise ConflictError("尚无已上传交易，无法生成报告。")
            digest, txn_rows, sample_rows = features.source_digest(
                conn, txn_hi, sample_hi)
            manifest = {
                "txn_batch_hi": txn_hi,
                "sample_batch_hi": sample_hi,
                "txn_batches": self._batch_manifest(conn, "transactions", txn_hi),
                "sample_batches": self._batch_manifest(conn, "samples", sample_hi),
                "transaction_rows": txn_rows,
                "sample_rows": sample_rows,
                "source_digest": digest,
                "window_seconds": features.WINDOW_SECONDS,
                "spec": "latest revision with ingested_at <= audit_at; count "
                        "revise revisions whose occurred_at lies in "
                        "[audit_at-7d, audit_at).",
            }
            public_id = uuid.uuid4().hex
            conn.execute(
                """INSERT INTO reports(public_id, status, txn_batch_hi,
                       sample_batch_hi, created_at, manifest_json)
                   VALUES (?,?,?,?,?,?)""",
                (public_id, "pending", txn_hi, sample_hi, now_epoch(),
                 json.dumps(manifest, ensure_ascii=False, sort_keys=True)),
            )
        return {"report_id": public_id, "status": "pending", "manifest": manifest}

    def _batch_manifest(self, conn, kind: str, hi: int) -> list[dict]:
        return [
            {"batch_id": r["id"], "filename": r["filename"],
             "sha256": r["sha256"], "stored_rows": r["stored_rows"]}
            for r in conn.execute(
                "SELECT id, filename, sha256, stored_rows FROM batches "
                "WHERE kind = ? AND id <= ? ORDER BY id", (kind, hi))
        ]

    def get_report(self, public_id: str) -> dict | None:
        with self.db.read() as conn:
            row = conn.execute(
                "SELECT * FROM reports WHERE public_id = ?", (public_id,)
            ).fetchone()
        if row is None:
            return None
        out = {
            "report_id": row["public_id"],
            "status": row["status"],
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "completed_at": row["completed_at"],
            "sample_count": row["sample_count"],
            "output_bytes": row["output_bytes"],
            "output_sha256": row["output_sha256"],
            "error": row["error"],
            "manifest": json.loads(row["manifest_json"]),
            "downloadable": False,
        }
        if row["status"] == "ready":
            path = Path(row["output_path"]) if row["output_path"] else None
            if path is not None and path.is_file():
                out["downloadable"] = True
            else:
                out["downloadable"] = False
                out["integrity_error"] = "报告文件缺失，需按原版本重新生成。"
        return out

    def report_file(self, public_id: str) -> tuple[Path, str] | None:
        """Return the ready file only if its integrity checks pass."""
        report = self.get_report(public_id)
        if report is None or report["status"] != "ready":
            return None
        with self.db.read() as conn:
            row = conn.execute(
                "SELECT output_path, output_sha256 FROM reports WHERE public_id = ?",
                (public_id,),
            ).fetchone()
        if not row or not row["output_path"]:
            return None
        path = Path(row["output_path"])
        if not path.is_file():
            self._mark_failed(
                public_id, "报告文件缺失，需按原版本重新生成。", allow_ready=True)
            return None
        digest = _hash_file(path)
        if digest != row["output_sha256"]:
            self._mark_failed(
                public_id, "报告文件校验失败（内容与生成时不一致）。", allow_ready=True)
            return None
        return path, digest

    def list_reports(self) -> list[dict]:
        with self.db.read() as conn:
            rows = conn.execute(
                "SELECT * FROM reports ORDER BY id DESC").fetchall()
        return [{
            "report_id": r["public_id"],
            "status": r["status"],
            "created_at": r["created_at"],
            "completed_at": r["completed_at"],
            "sample_count": r["sample_count"],
            "txn_batch_hi": r["txn_batch_hi"],
            "sample_batch_hi": r["sample_batch_hi"],
        } for r in rows]

    def _mark_failed(self, public_id: str, message: str,
                     allow_ready: bool = False) -> None:
        guard = "" if allow_ready else " AND status <> 'ready'"
        with self.db.write_transaction() as conn:
            conn.execute(
                "UPDATE reports SET status='failed', error=?, completed_at=?,"
                " output_sha256=NULL WHERE public_id=?" + guard,
                (message, now_epoch(), public_id),
            )

    def claim_next_report(self) -> str | None:
        with self.db.write_transaction() as conn:
            row = conn.execute(
                "SELECT public_id FROM reports WHERE status='pending' "
                "ORDER BY id LIMIT 1").fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE reports SET status='running', started_at=? "
                "WHERE public_id=?", (now_epoch(), row["public_id"]))
            return row["public_id"]

    def run_report(self, public_id: str) -> None:
        """Build output in a temp file, fsync, then atomically publish.

        ``status`` flips to ``ready`` only after the file is durably in place,
        so a partial write can never be downloaded.
        """
        try:
            with self.db.read() as conn:
                row = conn.execute(
                    "SELECT * FROM reports WHERE public_id=?", (public_id,)
                ).fetchone()
                if row is None:
                    return
                manifest = json.loads(row["manifest_json"])
                digest_before, _, _ = features.source_digest(
                    conn, row["txn_batch_hi"], row["sample_batch_hi"])
                if digest_before != manifest["source_digest"]:
                    raise RuntimeError("源数据与冻结清单不一致，拒绝生成。")
                rows = features.compute_rows(
                    conn, row["txn_batch_hi"], row["sample_batch_hi"])
            payload = features.render_csv(rows)
            if not rows:
                raise RuntimeError("样本集为空，拒绝产出空报告。")

            final_path = self.settings.reports_dir / f"{public_id}.csv"
            fd, tmp_name = tempfile.mkstemp(
                dir=self.settings.reports_dir, prefix=f".{public_id}-", suffix=".part")
            try:
                with os.fdopen(fd, "wb") as fh:
                    fh.write(payload)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp_name, final_path)
                self._fsync_dir(self.settings.reports_dir)
            except BaseException:
                try:
                    os.unlink(tmp_name)
                except FileNotFoundError:
                    pass
                raise
            out_digest = hashlib.sha256(payload).hexdigest()
            with self.db.write_transaction() as conn:
                current = conn.execute(
                    "SELECT status FROM reports WHERE public_id=?",
                    (public_id,)).fetchone()
                if current is None or current["status"] != "running":
                    # Attempt was superseded; discard unpublished output.
                    final_path.unlink(missing_ok=True)
                    return
                conn.execute(
                    """UPDATE reports SET status='ready', completed_at=?,
                           sample_count=?, output_path=?, output_sha256=?,
                           output_bytes=?, error=NULL
                       WHERE public_id=?""",
                    (now_epoch(), len(rows), str(final_path), out_digest,
                     len(payload), public_id))
        except Exception as exc:  # noqa: BLE001 - surfaced to operator
            self._mark_failed(public_id, f"{type(exc).__name__}: {exc}")

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        try:
            dfd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)

    def recover_interrupted(self) -> int:
        """A crash mid-build leaves 'running' rows; requeue them."""
        with self.db.write_transaction() as conn:
            cur = conn.execute(
                "UPDATE reports SET status='pending', started_at=NULL "
                "WHERE status='running'")
            return cur.rowcount

    # ------------------------------------------------------------------ #
    # Regeneration from the original version
    # ------------------------------------------------------------------ #
    def regenerate_report(self, public_id: str) -> dict:
        """Re-run against the frozen high-watermarks, never current data."""
        report = self.get_report(public_id)
        if report is None:
            raise LookupError(public_id)
        manifest = report["manifest"]
        with self.db.write_transaction() as conn:
            digest_now, _, _ = features.source_digest(
                conn, manifest["txn_batch_hi"], manifest["sample_batch_hi"])
            if digest_now != manifest["source_digest"]:
                raise ConflictError(
                    "冻结源数据校验失败：已入库批次内容被篡改，无法按原版本重取。")
            new_id = uuid.uuid4().hex
            conn.execute(
                """INSERT INTO reports(public_id, status, txn_batch_hi,
                       sample_batch_hi, created_at, manifest_json)
                   VALUES (?,?,?,?,?,?)""",
                (new_id, "pending", manifest["txn_batch_hi"],
                 manifest["sample_batch_hi"], now_epoch(),
                 json.dumps({**manifest, "regenerated_from": public_id},
                            ensure_ascii=False, sort_keys=True)),
            )
        return {"report_id": new_id, "status": "pending",
                "regenerated_from": public_id}

    # ------------------------------------------------------------------ #
    # Preview surfaces for the UI
    # ------------------------------------------------------------------ #
    def batch_summary(self) -> dict:
        with self.db.read() as conn:
            batches = [
                dict(r) for r in conn.execute(
                    """SELECT id, kind, filename, byte_size, sha256, stored_rows,
                              uploaded_at FROM batches ORDER BY id DESC""")
            ]
        return {"batches": batches}

    def preview_report(self, public_id: str, limit: int) -> dict | None:
        report = self.get_report(public_id)
        if report is None:
            return None
        rows: list[features.ReportRow] = []
        if report["status"] == "ready" and report["downloadable"]:
            m = report["manifest"]
            with self.db.read() as conn:
                rows = features.compute_rows(conn, m["txn_batch_hi"],
                                             m["sample_batch_hi"])
        return {
            "report": report,
            "headers": features.OUTPUT_HEADERS,
            "rows": [list(r) for r in features.iter_preview(rows, limit)],
            "total": len(rows),
        }


def format_utc(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, UTC).strftime("%Y-%m-%d %H:%M:%SZ")
