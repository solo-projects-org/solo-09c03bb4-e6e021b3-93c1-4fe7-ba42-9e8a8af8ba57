"""Single-process background report worker.

The compose deployment runs one API container, so an in-process worker is
sufficient and avoids an extra moving part. A cross-process claim is still
safe because ``claim_next_report`` is an IMMEDIATE transaction.
"""

from __future__ import annotations

import logging
import threading

from .service import AppService

log = logging.getLogger("ptf.worker")


class ReportWorker(threading.Thread):
    def __init__(self, service: AppService, poll_seconds: float = 0.5):
        super().__init__(name="report-worker", daemon=True)
        self.service = service
        self.poll_seconds = poll_seconds
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        self.service.recover_interrupted()
        while not self._stop_event.is_set():
            try:
                public_id = self.service.claim_next_report()
            except Exception:  # noqa: BLE001
                log.exception("failed to claim report")
                public_id = None
            if public_id is None:
                self._stop_event.wait(self.poll_seconds)
                continue
            log.info("building report %s", public_id)
            try:
                self.service.run_report(public_id)
            except Exception:  # noqa: BLE001 - run_report records failure itself
                log.exception("report %s failed", public_id)
