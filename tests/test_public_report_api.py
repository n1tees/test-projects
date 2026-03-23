from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.config import Settings, settings as app_settings
from backend.infrastructure.db import SQLiteAnalysisRepository, create_connection
from backend.interfaces.http import public_report as public_report_module
from backend.interfaces.http.public_report import router


class _StubJobManager:
    def __init__(self) -> None:
        self.enqueued: list[str] = []

    async def enqueue(self, analysis_id: str) -> None:
        self.enqueued.append(analysis_id)


def _build_app(job_manager: _StubJobManager) -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    app.state.job_manager = job_manager
    return app


def _build_test_settings(tmp_path) -> Settings:
    return Settings(
        data_dir=tmp_path,
        upload_dir=tmp_path / "uploads",
        report_dir=tmp_path / "reports",
        db_path=tmp_path / "analysis.db",
        max_concurrent_jobs=app_settings.max_concurrent_jobs,
        checkpoint_every_n_lines=app_settings.checkpoint_every_n_lines,
        max_retries=app_settings.max_retries,
        excel_cell_limit=app_settings.excel_cell_limit,
        text_encoding=app_settings.text_encoding,
    )


def test_export_report_requires_target_lemma(tmp_path, monkeypatch):
    test_settings = _build_test_settings(tmp_path)
    monkeypatch.setattr(public_report_module, "settings", test_settings)
    client = TestClient(_build_app(_StubJobManager()))

    response = client.post(
        "/public/report/export",
        files={"file": ("sample.txt", b"\xd0\xb6\xd0\xb8\xd1\x82\xd0\xb5\xd0\xbb\xd1\x8c\n", "text/plain")},
    )

    assert response.status_code == 422


def test_export_report_rejects_non_txt_file(tmp_path, monkeypatch):
    test_settings = _build_test_settings(tmp_path)
    monkeypatch.setattr(public_report_module, "settings", test_settings)
    client = TestClient(_build_app(_StubJobManager()))

    response = client.post(
        "/public/report/export",
        files={"file": ("sample.csv", b"a,b,c\n", "text/csv")},
        data={"target_lemma": "житель"},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "only .txt files are supported"


def test_export_report_normalizes_target_lemma_and_enqueues(tmp_path, monkeypatch):
    test_settings = _build_test_settings(tmp_path)
    monkeypatch.setattr(public_report_module, "settings", test_settings)
    job_manager = _StubJobManager()
    client = TestClient(_build_app(job_manager))

    response = client.post(
        "/public/report/export",
        files={"file": ("sample.txt", "Житель и жителем\n".encode("utf-8"), "text/plain")},
        data={"target_lemma": "  ЖИТЕЛЬ  "},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["target_lemma"] == "житель"
    assert len(job_manager.enqueued) == 1
    assert payload["id"] == job_manager.enqueued[0]

    conn = create_connection(test_settings.db_path)
    repo = SQLiteAnalysisRepository(conn)
    repo.init()
    row = repo.get_analysis(payload["id"])
    assert row is not None
    assert row["target_lemma"] == "житель"
    conn.close()
