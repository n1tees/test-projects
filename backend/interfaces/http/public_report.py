from __future__ import annotations

import io
import logging
import uuid
from pathlib import Path

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from backend.config import settings
from backend.infrastructure.db import SQLiteAnalysisRepository, create_connection
from backend.infrastructure.xlsx_writer import XlsxReportWriter


router = APIRouter()
logger = logging.getLogger("backend.http.public_report")


class ExportReportResponse(BaseModel):
    id: str
    target_lemma: str


class ReportStatusResponse(BaseModel):
    id: str
    status: str
    error_message: str | None = None
    checkpoint_line: int
    total_lines: int | None = None


async def _save_uploaded_file(upload: UploadFile, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with open(target_path, "wb") as f:
        while True:
            chunk = await upload.read(1024 * 1024)
            if not chunk:
                break
            f.write(chunk)


@router.post(
    "/public/report/export",
    summary="Загрузка txt на анализ",
    description="Принимает txt и target_lemma, создает задачу анализа и возвращает идентификатор.",
    response_model=ExportReportResponse,
)
async def export_report(
    request: Request,
    file: UploadFile = File(...),
    target_lemma: str = Form(...),
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="filename is required")
    if not file.filename.lower().endswith(".txt"):
        raise HTTPException(status_code=400, detail="only .txt files are supported")
    normalized_target_lemma = target_lemma.strip().lower()
    if not normalized_target_lemma:
        raise HTTPException(status_code=400, detail="target_lemma is required")

    analysis_id = str(uuid.uuid4())
    upload_path = settings.upload_dir / f"{analysis_id}.txt"

    try:
        await _save_uploaded_file(file, upload_path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"upload_save_failed: {type(e).__name__}: {e}")

    conn = create_connection(settings.db_path)
    repo = SQLiteAnalysisRepository(conn)
    repo.init()
    try:
        repo.create_analysis(
            analysis_id=analysis_id,
            file_path=str(upload_path),
            target_lemma=normalized_target_lemma,
        )
    finally:
        conn.close()

    job_manager = request.app.state.job_manager  # type: ignore[attr-defined]
    await job_manager.enqueue(analysis_id)
    logger.info(
        "analysis_enqueued analysis_id=%s filename=%s target_lemma=%s",
        analysis_id,
        file.filename,
        normalized_target_lemma,
    )

    return ExportReportResponse(id=analysis_id, target_lemma=normalized_target_lemma)


@router.get(
    "/public/report/{analysis_id}",
    summary="Получение статуса или xlsx",
    description="Возвращает статус обработки. Если обработка завершена успешно, возвращает xlsx-файл.",
    response_model=ReportStatusResponse,
    responses={
        200: {
            "description": "JSON со статусом или xlsx файл при success",
        },
        404: {"description": "Задача не найдена"},
    },
)
async def get_report(analysis_id: str):
    conn = create_connection(settings.db_path)
    repo = SQLiteAnalysisRepository(conn)
    repo.init()

    try:
        row = repo.get_analysis(analysis_id)
        if not row:
            raise HTTPException(status_code=404, detail="analysis not found")

        status = row["status"]
        error_message = row["error_message"]

        if status != "success":
            return ReportStatusResponse(
                id=analysis_id,
                status=status,
                error_message=error_message,
                checkpoint_line=int(row["checkpoint_line"]),
                total_lines=row["total_lines"],
            )

        writer = XlsxReportWriter()
        xlsx_bytes = writer.generate_report_xlsx_bytes(analysis_id, repo)
        file_name = f"report_{analysis_id}.xlsx"
        headers = {"X-Status": "success"}
        logger.info("analysis_result_downloaded analysis_id=%s", analysis_id)
        return StreamingResponse(
            io.BytesIO(xlsx_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers | {"Content-Disposition": f'attachment; filename="{file_name}"'},
        )
    finally:
        conn.close()

