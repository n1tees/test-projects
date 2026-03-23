from __future__ import annotations

from backend.application.job_manager import JobManager
from backend.infrastructure.db import SQLiteAnalysisRepository, create_connection


def test_cleanup_uploaded_file_deletes_source(tmp_path):
    db_path = tmp_path / "test.db"
    conn = create_connection(db_path)
    repo = SQLiteAnalysisRepository(conn)
    repo.init()

    analysis_id = "analysis-cleanup"
    source_path = tmp_path / "uploaded.txt"
    source_path.write_text("line 1\nline 2\n", encoding="utf-8")

    repo.create_analysis(
        analysis_id=analysis_id,
        file_path=str(source_path),
        target_lemma="житель",
    )

    manager = JobManager()
    manager._cleanup_uploaded_file(repo, analysis_id)

    assert not source_path.exists()
    conn.close()
