from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    upload_dir: Path
    report_dir: Path
    db_path: Path
    max_concurrent_jobs: int
    checkpoint_every_n_lines: int
    max_retries: int
    excel_cell_limit: int
    text_encoding: str

    @staticmethod
    def load() -> "Settings":
        base_dir = Path(__file__).resolve().parent
        data_dir = base_dir / "data"

        upload_dir = data_dir / "uploads"
        report_dir = data_dir / "reports"
        db_path = data_dir / "analysis.db"

        return Settings(
            data_dir=data_dir,
            upload_dir=upload_dir,
            report_dir=report_dir,
            db_path=db_path,
            max_concurrent_jobs=int(os.getenv("MAX_CONCURRENT_JOBS", "2")),
            checkpoint_every_n_lines=int(os.getenv("CHECKPOINT_EVERY_N_LINES", "2000")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            excel_cell_limit=int(os.getenv("EXCEL_CELL_LIMIT", "32767")),
            text_encoding=os.getenv("TEXT_ENCODING", "utf-8"),
        )


settings = Settings.load()

