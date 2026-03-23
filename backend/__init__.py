from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from backend.application.job_manager import JobManager
from backend.config import settings
from backend.infrastructure.db import SQLiteAnalysisRepository, create_connection
from backend.interfaces.http import api_router


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.upload_dir.mkdir(parents=True, exist_ok=True)
    settings.report_dir.mkdir(parents=True, exist_ok=True)

    conn = create_connection(settings.db_path)
    repo = SQLiteAnalysisRepository(conn)
    repo.init()
    conn.close()

    job_manager = JobManager()
    app.state.job_manager = job_manager
    await job_manager.start()
    try:
        yield
    finally:
        # [Останавливаем менеджер задач перед завершением приложения]
        await job_manager.stop()


app = FastAPI(title="Report Export API", lifespan=lifespan)
app.include_router(api_router)

setup_logging()
logger = logging.getLogger("backend.api")


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    started = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    logger.info(
        "request_finished method=%s path=%s status=%s elapsed_ms=%s",
        request.method,
        request.url.path,
        response.status_code,
        elapsed_ms,
    )
    return response
