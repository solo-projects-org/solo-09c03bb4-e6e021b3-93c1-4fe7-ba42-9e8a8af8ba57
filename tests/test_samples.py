"""End-to-end check that shipped samples produce the documented features."""

import csv
import io
import shutil
import tempfile
import unittest
from pathlib import Path

from app.config import Settings
from app.service import AppService

ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"


class SampleFixtureTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.svc = AppService(Settings(data_dir=self.dir))

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def run_report(self) -> dict[str, dict]:
        info = self.svc.create_report()
        pid = info["report_id"]
        claimed = self.svc.claim_next_report()
        self.assertEqual(claimed, pid)
        self.svc.run_report(pid)
        found = self.svc.report_file(pid)
        self.assertIsNotNone(found)
        text = found[0].read_text(encoding="utf-8")
        return {r["sample_id"]: r for r in csv.DictReader(io.StringIO(text))}

    def test_base_version(self):
        self.svc.upload_transactions(
            "base.csv", (SAMPLES / "transactions_base.csv").read_bytes())
        self.svc.upload_samples(
            "samples.csv", (SAMPLES / "samples.csv").read_bytes())
        rows = self.run_report()
        self.assertEqual(rows["S001"]["txn_count_7d"], "4")
        self.assertEqual(rows["S001"]["txn_amount_7d"], "280.00")
        self.assertEqual(rows["S002"]["txn_amount_7d"], "105.00")
        self.assertEqual(rows["S003"]["txn_amount_7d"], "10.00")
        self.assertEqual(rows["S004"]["txn_amount_7d"], "20.00")
        self.assertEqual(rows["S005"]["txn_count_7d"], "0")
        self.assertEqual(rows["S005"]["txn_amount_7d"], "0.00")

    def test_late_batch_changes_only_new_report(self):
        self.svc.upload_transactions(
            "base.csv", (SAMPLES / "transactions_base.csv").read_bytes())
        self.svc.upload_samples(
            "samples.csv", (SAMPLES / "samples.csv").read_bytes())
        first = self.svc.create_report()["report_id"]

        self.svc.upload_transactions(
            "late.csv", (SAMPLES / "transactions_late.csv").read_bytes())
        # A second, newer report sees late data.
        second = self.svc.create_report()["report_id"]
        self.svc.claim_next_report()   # FIFO: first report
        self.svc.run_report(first)
        self.svc.claim_next_report()
        self.svc.run_report(second)

        def load(pid):
            path, _ = self.svc.report_file(pid)
            return {r["sample_id"]: r for r in csv.DictReader(
                io.StringIO(path.read_text(encoding="utf-8")))}

        old, new = load(first), load(second)
        # First report stays pinned despite the upload.
        self.assertEqual(old["S001"]["txn_amount_7d"], "280.00")
        self.assertEqual(old["S002"]["txn_amount_7d"], "105.00")
        self.assertEqual(old["S004"]["txn_amount_7d"], "20.00")
        # New report: E7 moved to 2025-01-06 (inside late window) => 40 -> 70
        # E9 adds 15; E10 cancelled; E11 moved out of window.
        self.assertEqual(new["S002"]["txn_count_7d"], "3")
        self.assertEqual(new["S002"]["txn_amount_7d"], "110.00")
        self.assertEqual(new["S004"]["txn_amount_7d"], "55.00")

    def test_conflict_sample_is_rejected(self):
        with self.assertRaises(Exception) as ctx:
            self.svc.upload_transactions(
                "conflict.csv",
                (SAMPLES / "transactions_conflict_rejected.csv").read_bytes())
        self.assertEqual(getattr(ctx.exception, "code", None), "batch_rejected")


if __name__ == "__main__":
    unittest.main()
