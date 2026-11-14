"""Runtime configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    max_upload_bytes: int = 64 * 1024 * 1024
    worker_poll_seconds: float = 0.5
    preview_max_rows: int = 2000

    def __post_init__(self) -> None:
        object.__setattr__(self, "data_dir", Path(self.data_dir))

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            data_dir=Path(os.environ.get("DATA_DIR", "./data")),
            max_upload_bytes=_env_int("MAX_UPLOAD_BYTES", 64 * 1024 * 1024),
            worker_poll_seconds=float(
                _env_int("WORKER_POLL_MS", 500)) / 1000.0,
            preview_max_rows=_env_int("PREVIEW_MAX_ROWS", 2000),
        )

    @property
    def db_path(self) -> Path:
        return self.data_dir / "app.sqlite3"

    @property
    def uploads_dir(self) -> Path:
        return self.data_dir / "uploads"

    @property
    def reports_dir(self) -> Path:
        return self.data_dir / "reports"

    def ensure_dirs(self) -> None:
        for directory in (self.data_dir, self.uploads_dir, self.reports_dir):
            directory.mkdir(parents=True, exist_ok=True)
