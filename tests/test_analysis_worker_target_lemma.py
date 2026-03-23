from __future__ import annotations

from backend.application.analysis_worker import AnalysisWorker
from backend.infrastructure.db import SQLiteAnalysisRepository, create_connection


def test_analysis_worker_counts_only_target_lemma(tmp_path):
    db_path = tmp_path / "analysis.db"
    conn = create_connection(db_path)
    repo = SQLiteAnalysisRepository(conn)
    repo.init()

    source_path = tmp_path / "source.txt"
    source_path.write_text(
        "Житель гуляет с другом\n"
        "Жителем называли жителя в отчете\n"
        "Совсем другие слова\n",
        encoding="utf-8",
    )

    analysis_id = "analysis-target-lemma"
    repo.create_analysis(
        analysis_id=analysis_id,
        file_path=str(source_path),
        target_lemma="житель",
    )

    worker = AnalysisWorker()
    total_lines = worker.process(analysis_id, repo)

    assert total_lines == 3
    totals = repo.get_word_totals(analysis_id)
    assert len(totals) == 1
    assert totals[0]["lemma"] == "житель"
    assert totals[0]["total_count"] == 3

    line_rows = repo.get_word_line_rows(analysis_id, "житель")
    assert [int(row["line_no"]) for row in line_rows] == [1, 2]
    assert [int(row["count"]) for row in line_rows] == [1, 2]
    conn.close()


def test_line_save_is_idempotent_for_same_line(tmp_path):
    db_path = tmp_path / "analysis.db"
    conn = create_connection(db_path)
    repo = SQLiteAnalysisRepository(conn)
    repo.init()

    source_path = tmp_path / "source.txt"
    source_path.write_text("Житель\n", encoding="utf-8")
    analysis_id = "analysis-idempotent"
    repo.create_analysis(
        analysis_id=analysis_id,
        file_path=str(source_path),
        target_lemma="житель",
    )

    repo.save_line_result_with_checkpoint(
        analysis_id=analysis_id,
        lemma="житель",
        line_no=1,
        line_count=2,
        checkpoint_line=1,
    )
    repo.save_line_result_with_checkpoint(
        analysis_id=analysis_id,
        lemma="житель",
        line_no=1,
        line_count=2,
        checkpoint_line=1,
    )

    totals = repo.get_word_totals(analysis_id)
    assert len(totals) == 1
    assert int(totals[0]["total_count"]) == 2
    line_rows = repo.get_word_line_rows(analysis_id, "житель")
    assert len(line_rows) == 1
    assert int(line_rows[0]["count"]) == 2
    conn.close()
