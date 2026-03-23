from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

from backend.config import settings
from backend.infrastructure.db import (
    STATUS_FAILED,
    STATUS_PAUSED,
    create_connection,
    SQLiteAnalysisRepository,
)
from backend.application.analysis_worker import AnalysisWorker


logger = logging.getLogger("backend.jobs")


class JobManager:
    def __init__(self) -> None:
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._semaphore = asyncio.Semaphore(settings.max_concurrent_jobs)
        self._consumer_task: asyncio.Task[None] | None = None
        self._stopping = False
        self._worker = AnalysisWorker()
        self._active_tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        self._stopping = False
        self._consumer_task = asyncio.create_task(self._consume_loop())
        await self._requeue_unfinished()
        logger.info("job_manager_started max_concurrent_jobs=%s", settings.max_concurrent_jobs)

    async def stop(self) -> None:
        self._stopping = True
        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
        if self._active_tasks:
            await asyncio.gather(*self._active_tasks, return_exceptions=True)
        logger.info("job_manager_stopped")

    async def enqueue(self, analysis_id: str) -> None:
        await self._queue.put(analysis_id)
        logger.info("job_enqueued analysis_id=%s", analysis_id)

    async def _requeue_unfinished(self) -> None:
        conn = create_connection(settings.db_path)
        repo = SQLiteAnalysisRepository(conn)
        repo.init()
        try:
            for analysis_id in repo.list_unfinished_analysis_ids():
                await self.enqueue(analysis_id)
        finally:
            conn.close()

    async def _consume_loop(self) -> None:
        while not self._stopping:
            analysis_id = await self._queue.get()
            task = asyncio.create_task(self._run_single(analysis_id))
            self._active_tasks.add(task)
            task.add_done_callback(self._active_tasks.discard)

    async def _run_single(self, analysis_id: str) -> None:
        async with self._semaphore:
            await asyncio.to_thread(self._process_with_retry, analysis_id)

    def _cleanup_uploaded_file(self, repo: SQLiteAnalysisRepository, analysis_id: str) -> None:
        try:
            checkpoint = repo.get_checkpoint(analysis_id)
            file_path = Path(checkpoint.file_path)
            if not file_path.exists():
                return
            file_path.unlink()
            logger.info("upload_deleted_after_success analysis_id=%s path=%s", analysis_id, file_path)
        except Exception as exc:
            # [Если удалить файл не получилось, задачу не валим]
            logger.warning("upload_delete_failed analysis_id=%s error=%s", analysis_id, exc)

    def _process_with_retry(self, analysis_id: str) -> None:
        while True:
            if self._stopping:
                return
            conn = create_connection(settings.db_path)
            repo = SQLiteAnalysisRepository(conn)
            repo.init()
            try:
                repo.mark_running(analysis_id)
                logger.info("job_started analysis_id=%s", analysis_id)
                total_lines = self._worker.process(
                    analysis_id,
                    repo,
                    stop_requested=lambda: self._stopping,
                )
                if self._stopping:
                    return
                repo.mark_success(analysis_id, total_lines=total_lines)
                self._cleanup_uploaded_file(repo, analysis_id)
                logger.info("job_success analysis_id=%s total_lines=%s", analysis_id, total_lines)
                return
            except Exception as exc:
                retry_count = repo.get_retry_count(analysis_id)
                if self._stopping:
                    checkpoint = repo.get_checkpoint(analysis_id)
                    repo.set_status(
                        analysis_id,
                        status=STATUS_PAUSED,
                        checkpoint_line=checkpoint.checkpoint_line,
                        checkpoint_offset=checkpoint.checkpoint_offset,
                    )
                    logger.info(
                        "job_paused_on_shutdown analysis_id=%s checkpoint_line=%s",
                        analysis_id,
                        checkpoint.checkpoint_line,
                    )
                    return
                if retry_count < settings.max_retries:
                    repo.increment_retry_and_reset(analysis_id)
                    # [Даем небольшой backoff, чтобы не крутить ретраи без паузы]
                    time.sleep(min(2 ** retry_count, 8))
                    logger.warning(
                        "job_retry analysis_id=%s next_retry=%s error=%s",
                        analysis_id,
                        retry_count + 1,
                        type(exc).__name__,
                    )
                    continue
                repo.set_status(
                    analysis_id,
                    status=STATUS_FAILED,
                    error_message=f"{type(exc).__name__}: {exc}",
                )
                logger.error("job_failed analysis_id=%s error=%s", analysis_id, exc)
                return
            finally:
                conn.close()
