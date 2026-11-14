import csv
import io
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from fastapi.testclient import TestClient

from app.config import Settings
from app.service import AppService

UTC = timezone.utc
DAY = 86400


def ts(text: str) -> int:
    return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())


AUDIT = ts("2025-01-10T12:00:00Z")


def txn_csv(rows: list[tuple]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["event_id", "account_id", "occurred_at", "ingested_at",
                "amount", "action"])
    for r in rows:
        w.writerow(r)
    return buf.getvalue().encode()


def sample_csv(rows: list[tuple]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["sample_id", "account_id", "audit_at", "label"])
    for r in rows:
        w.writerow(r)
    return buf.getvalue().encode()


class ServiceTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.svc = AppService(Settings(data_dir=self.dir, worker_poll_seconds=0.05))

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def build(self, txns: bytes, samples: bytes) -> str:
        self.svc.upload_transactions("t.csv", txns)
        self.svc.upload_samples("s.csv", samples)
        report = self.svc.create_report()
        pid = report["report_id"]
        self.svc.claim_next_report()
        self.svc.run_report(pid)
        return pid

    def output_rows(self, pid: str) -> list[dict]:
        found = self.svc.report_file(pid)
        self.assertIsNotNone(found, "report should be downloadable")
        return list(csv.DictReader(
            io.StringIO(found[0].read_text(encoding="utf-8"))))

    # ------------------------------------------------------------------ #
    def test_latest_visible_revision_wins(self):
        """A revision ingested later but before audit supersedes the old one."""
        pid = self.build(
            txn_csv([
                ("E1", "A", AUDIT - 2 * DAY, AUDIT - 5 * DAY, "100.00", "revise"),
                ("E1", "A", AUDIT - 2 * DAY, AUDIT - 1 * DAY, "250.00", "revise"),
            ]),
            sample_csv([("S1", "A", AUDIT, 1)]),
        )
        row = self.output_rows(pid)[0]
        self.assertEqual(row["txn_count_7d"], "1")
        self.assertEqual(row["txn_amount_7d"], "250.00")

    def test_future_ingest_is_invisible(self):
        """A revision ingested after the audit instant must not be used."""
        pid = self.build(
            txn_csv([
                ("E1", "A", AUDIT - 2 * DAY, AUDIT - 5 * DAY, "100.00", "revise"),
                ("E1", "A", AUDIT - 2 * DAY, AUDIT + 60, "999.00", "revise"),
            ]),
            sample_csv([("S1", "A", AUDIT, 1)]),
        )
        row = self.output_rows(pid)[0]
        self.assertEqual(row["txn_amount_7d"], "100.00")

        # Exactly at audit time is also excluded: visibility is <= audit_at,
        # but a revision ingested at the audit instant is a tie at the
        # boundary; our rule keeps `ingested_at <= audit_at`. Verify it.
        pid2 = self.build(
            txn_csv([
                ("E2", "B", AUDIT - 2 * DAY, AUDIT - 5 * DAY, "10.00", "revise"),
                ("E2", "B", AUDIT - 2 * DAY, AUDIT, "20.00", "revise"),
            ]),
            sample_csv([("S9", "B", AUDIT, 0)]),
        )
        # second build reuses the then-current batches; inspect directly
        row2 = [r for r in self.output_rows(pid2) if r["sample_id"] == "S9"][0]
        self.assertEqual(row2["txn_amount_7d"], "20.00")

    def test_cancel_drops_event_and_is_not_counted(self):
        pid = self.build(
            txn_csv([
                ("E1", "A", AUDIT - 2 * DAY, AUDIT - 5 * DAY, "100.00", "revise"),
                ("E1", "A", AUDIT - 2 * DAY, AUDIT - 1 * DAY, "0.00", "cancel"),
                ("E2", "A", AUDIT - 2 * DAY, AUDIT - 5 * DAY, "50.00", "revise"),
            ]),
            sample_csv([("S1", "A", AUDIT, 1)]),
        )
        row = self.output_rows(pid)[0]
        self.assertEqual(row["txn_count_7d"], "1")
        self.assertEqual(row["txn_amount_7d"], "50.00")

    def test_cancel_then_revive_counts_only_newer_revision(self):
        pid = self.build(
            txn_csv([
                ("E1", "A", AUDIT - 2 * DAY, AUDIT - 6 * DAY, "100.00", "revise"),
                ("E1", "A", AUDIT - 2 * DAY, AUDIT - 4 * DAY, "0.00", "cancel"),
                ("E1", "A", AUDIT - 2 * DAY, AUDIT - 2 * DAY, "70.00", "revise"),
            ]),
            sample_csv([("S1", "A", AUDIT, 1)]),
        )
        row = self.output_rows(pid)[0]
        self.assertEqual(row["txn_amount_7d"], "70.00")

    def test_revision_moved_out_of_window_does_not_keep_old_value(self):
        """The in-window old revision is superseded by one moved before the
        window; therefore the event drops out entirely, not 100.00."""
        pid = self.build(
            txn_csv([
                ("E1", "A", AUDIT - 1 * DAY, AUDIT - 6 * DAY, "100.00", "revise"),
                ("E1", "A", AUDIT - 30 * DAY, AUDIT - 2 * DAY, "100.00", "revise"),
            ]),
            sample_csv([("S1", "A", AUDIT, 1)]),
        )
        row = self.output_rows(pid)[0]
        self.assertEqual(row["txn_count_7d"], "0")
        self.assertEqual(row["txn_amount_7d"], "0.00")

    def test_window_boundaries_inclusive_start_exclusive_audit(self):
        start = AUDIT - 7 * DAY
        pid = self.build(
            txn_csv([
                ("ON_START", "A", start, AUDIT - 9 * DAY, "10.00", "revise"),
                ("BEFORE", "A", start - 1, AUDIT - 9 * DAY, "20.00", "revise"),
                ("AT_AUDIT", "A", AUDIT, AUDIT - 9 * DAY, "30.00", "revise"),
                ("IN", "A", AUDIT - 1, AUDIT - 9 * DAY, "40.00", "revise"),
            ]),
            sample_csv([("S1", "A", AUDIT, 1)]),
        )
        row = self.output_rows(pid)[0]
        self.assertEqual(row["txn_count_7d"], "2")
        self.assertEqual(row["txn_amount_7d"], "50.00")
        self.assertEqual(int(row["window_start_at"]), start)
        self.assertEqual(int(row["window_end_at"]), AUDIT)

    def test_late_revision_arriving_after_audit_ignored_but_used_for_later_audit(self):
        """One uploaded file serves two audits with different visibility."""
        self.svc.upload_transactions("t.csv", txn_csv([
            ("E1", "A", AUDIT - 2 * DAY, AUDIT - 3 * DAY, "10.00", "revise"),
            # Ingested after the early audit: invisible there, visible later.
            ("E1", "A", AUDIT - 2 * DAY, AUDIT + 5 * DAY, "90.00", "revise"),
            # Occurs inside the *late* window and is also ingested after the
            # early audit; appears only for LATE.
            ("E2", "A", AUDIT + 8 * DAY, AUDIT + 5 * DAY, "7.00", "revise"),
        ]))
        self.svc.upload_samples("s.csv", sample_csv([
            ("EARLY", "A", AUDIT, 0),
            ("LATE", "A", AUDIT + 10 * DAY, 1),
        ]))
        pid = self.svc.create_report()["report_id"]
        self.svc.claim_next_report()
        self.svc.run_report(pid)
        rows = {r["sample_id"]: r for r in self.output_rows(pid)}
        self.assertEqual(rows["EARLY"]["txn_amount_7d"], "10.00")
        self.assertEqual(rows["EARLY"]["txn_count_7d"], "1")
        # E1 was revised to a time outside the late window (so no value is
        # inherited); E2 occurs inside it and is now visible.
        self.assertEqual(rows["LATE"]["txn_amount_7d"], "7.00")
        self.assertEqual(rows["LATE"]["txn_count_7d"], "1")

    def test_same_event_same_ingest_different_value_rejects_whole_batch(self):
        with self.assertRaises(Exception) as ctx:
            self.svc.upload_transactions("t.csv", txn_csv([
                ("OK", "A", AUDIT, AUDIT, "5.00", "revise"),
                ("E1", "A", AUDIT, AUDIT, "10.00", "revise"),
                ("E1", "A", AUDIT, AUDIT, "11.00", "revise"),
            ]))
        self.assertEqual(getattr(ctx.exception, "code", None), "batch_rejected")
        self.assertEqual(self.svc.batch_summary()["batches"], [])

    def test_same_event_same_ingest_across_batches_rejects(self):
        self.svc.upload_transactions("a.csv", txn_csv([
            ("E1", "A", AUDIT, AUDIT, "10.00", "revise")]))
        with self.assertRaises(Exception) as ctx:
            self.svc.upload_transactions("b.csv", txn_csv([
                ("E1", "A", AUDIT, AUDIT, "11.00", "revise")]))
        self.assertEqual(getattr(ctx.exception, "code", None), "batch_rejected")
        self.assertEqual(len(self.svc.batch_summary()["batches"]), 1)

    def test_identical_retry_is_idempotent(self):
        payload = txn_csv([("E1", "A", AUDIT, AUDIT, "10.00", "revise")])
        self.svc.upload_transactions("a.csv", payload)
        result = self.svc.upload_transactions("a.csv", payload)
        self.assertEqual(result["stored_rows"], 0)
        self.assertEqual(result["skipped_same_value"], 1)

    def test_validation_rejects_batch_atomically(self):
        bad = txn_csv([
            ("E1", "A", AUDIT, AUDIT, "10.00", "revise"),
            ("E2", "A", AUDIT, AUDIT, "1e3", "revise"),         # scientific notation
            ("E3", "A", "2025-01-01T00:00:00Z", AUDIT, "1.00", "revise"),  # not epoch
        ])
        with self.assertRaises(Exception):
            self.svc.upload_transactions("bad.csv", bad)
        with self.svc.db.read() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM transactions")
                             .fetchone()[0], 0)

    def test_report_pins_version_and_ignores_late_uploads(self):
        self.svc.upload_transactions("t.csv", txn_csv([
            ("E1", "A", AUDIT - 2 * DAY, AUDIT - 3 * DAY, "10.00", "revise"),
        ]))
        self.svc.upload_samples("s.csv", sample_csv([("S1", "A", AUDIT, 1)]))
        pid = self.svc.create_report()["report_id"]
        # A late batch lands between creation and computation.
        self.svc.upload_transactions("late.csv", txn_csv([
            ("E2", "A", AUDIT - 1 * DAY, AUDIT - 1 * DAY, "500.00", "revise"),
        ]))
        self.svc.claim_next_report()
        self.svc.run_report(pid)
        row = self.output_rows(pid)[0]
        self.assertEqual(row["txn_count_7d"], "1")
        self.assertEqual(row["txn_amount_7d"], "10.00")

        # A brand-new report *does* see the late data.
        pid2 = self.svc.create_report()["report_id"]
        self.svc.claim_next_report()
        self.svc.run_report(pid2)
        row2 = self.output_rows(pid2)[0]
        self.assertEqual(row2["txn_amount_7d"], "510.00")

    def test_regenerate_uses_pinned_version(self):
        self.svc.upload_transactions("t.csv", txn_csv([
            ("E1", "A", AUDIT - 2 * DAY, AUDIT - 3 * DAY, "10.00", "revise")]))
        self.svc.upload_samples("s.csv", sample_csv([("S1", "A", AUDIT, 1)]))
        pid = self.svc.create_report()["report_id"]
        self.svc.claim_next_report()
        self.svc.run_report(pid)
        self.svc.upload_transactions("late.csv", txn_csv([
            ("E2", "A", AUDIT - 1 * DAY, AUDIT - 1 * DAY, "500.00", "revise")]))
        new = self.svc.regenerate_report(pid)
        self.svc.claim_next_report()
        self.svc.run_report(new["report_id"])
        row = self.output_rows(new["report_id"])[0]
        self.assertEqual(row["txn_amount_7d"], "10.00")

    def test_write_failure_blocks_download_then_regeneration_recovers(self):
        self.svc.upload_transactions("t.csv", txn_csv([
            ("E1", "A", AUDIT - 2 * DAY, AUDIT - 3 * DAY, "10.00", "revise")]))
        self.svc.upload_samples("s.csv", sample_csv([("S1", "A", AUDIT, 1)]))
        pid = self.svc.create_report()["report_id"]
        self.svc.claim_next_report()
        with mock.patch("app.service.os.fsync", side_effect=OSError("disk full")):
            self.svc.run_report(pid)
        report = self.svc.get_report(pid)
        self.assertEqual(report["status"], "failed")
        self.assertFalse(report["downloadable"])
        self.assertIsNone(self.svc.report_file(pid))
        # No partial artefact left behind.
        self.assertEqual(list(Path(self.svc.settings.reports_dir).iterdir()), [])

        new = self.svc.regenerate_report(pid)
        self.svc.claim_next_report()
        self.svc.run_report(new["report_id"])
        self.assertEqual(self.svc.get_report(new["report_id"])["status"], "ready")

    def test_corrupt_output_is_not_served(self):
        pid = self.build(
            txn_csv([("E1", "A", AUDIT - 2 * DAY, AUDIT - 3 * DAY, "10.00", "revise")]),
            sample_csv([("S1", "A", AUDIT, 1)]),
        )
        path = self.svc.report_file(pid)[0]
        path.write_bytes(b"tampered")
        self.assertIsNone(self.svc.report_file(pid))
        self.assertEqual(self.svc.get_report(pid)["status"], "failed")

    def test_sources_survive_decompression_not_needed_but_file_bytes_immutable(self):
        payload = txn_csv([("E1", "A", AUDIT, AUDIT, "10.00", "revise")])
        self.svc.upload_transactions("t.csv", payload)
        with self.svc.db.read() as conn:
            row = conn.execute("SELECT stored_path, sha256 FROM batches").fetchone()
        stored = Path(row["stored_path"])
        import hashlib
        self.assertEqual(hashlib.sha256(stored.read_bytes()).hexdigest(), row["sha256"])


class ApiTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        os.environ["DATA_DIR"] = self.dir
        from app.main import create_app
        self.app = create_app(Settings(data_dir=self.dir, worker_poll_seconds=0.05),
                              start_worker=False)
        self.client = TestClient(self.app)
        self.svc = self.app.state.service

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)
        os.environ.pop("DATA_DIR", None)

    def test_upload_validation_error_returns_422_with_details(self):
        resp = self.client.post("/api/uploads/transactions", files={
            "file": ("t.csv", txn_csv([("E1", "A", AUDIT, AUDIT, "abc", "revise")]),
                     "text/csv")})
        self.assertEqual(resp.status_code, 422)
        body = resp.json()
        self.assertEqual(body["error"], "batch_rejected")
        self.assertTrue(body["details"])

    def test_full_flow_and_download(self):
        r = self.client.post("/api/uploads/transactions", files={
            "file": ("t.csv", txn_csv([
                ("E1", "A", AUDIT - 2 * DAY, AUDIT - 3 * DAY, "10.00", "revise")]),
                "text/csv")})
        self.assertEqual(r.status_code, 200, r.text)
        r = self.client.post("/api/uploads/samples", files={
            "file": ("s.csv", sample_csv([("S1", "A", AUDIT, 1)]), "text/csv")})
        self.assertEqual(r.status_code, 200, r.text)
        r = self.client.post("/api/reports")
        self.assertEqual(r.status_code, 200, r.text)
        pid = r.json()["report_id"]
        # Drive the worker deterministically for the test.
        cid = self.svc.claim_next_report()
        self.assertEqual(cid, pid)
        self.svc.run_report(pid)
        detail = self.client.get(f"/api/reports/{pid}").json()
        self.assertEqual(detail["status"], "ready")
        dl = self.client.get(f"/api/reports/{pid}/download")
        self.assertEqual(dl.status_code, 200)
        self.assertIn("txn_amount_7d", dl.text)
        self.assertEqual(dl.headers["content-type"].split(";")[0], "text/csv")
        # HTML pages render.
        self.assertEqual(self.client.get("/").status_code, 200)
        page = self.client.get(f"/reports/{pid}")
        self.assertEqual(page.status_code, 200)
        self.assertIn("S1", page.text)

    def test_download_409_before_ready(self):
        self.svc.upload_transactions("t.csv", txn_csv([
            ("E1", "A", AUDIT - 2 * DAY, AUDIT - 3 * DAY, "10.00", "revise")]))
        self.svc.upload_samples("s.csv", sample_csv([("S1", "A", AUDIT, 1)]))
        pid = self.client.post("/api/reports").json()["report_id"]
        resp = self.client.get(f"/reports/{pid}/download")
        self.assertEqual(resp.status_code, 409)

    def test_create_report_without_samples_is_409(self):
        resp = self.client.post("/api/reports")
        self.assertEqual(resp.status_code, 409)


if __name__ == "__main__":
    unittest.main()
