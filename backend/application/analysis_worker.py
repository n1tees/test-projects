from __future__ import annotations

import logging
from pathlib import Path

import pymorphy3

from backend.config import settings
from backend.infrastructure.db import SQLiteAnalysisRepository
from backend.infrastructure.text_parser import extract_words

logger = logging.getLogger("backend.worker")


class LemmaNormalizer:
    def __init__(self) -> None:
        self._morph = pymorphy3.MorphAnalyzer()

    def normalize(self, word: str) -> str:
        parsed = self._morph.parse(word)
        if not parsed:
            return word
        return parsed[0].normal_form


class AnalysisWorker:
    def __init__(self) -> None:
        self._normalizer = LemmaNormalizer()

    def process(self, analysis_id: str, repo: SQLiteAnalysisRepository, stop_requested: callable | None = None) -> int:
        checkpoint = repo.get_checkpoint(analysis_id)
        target_lemma = self._normalizer.normalize(repo.get_target_lemma(analysis_id))
        file_path = Path(checkpoint.file_path)
        checkpoint_line = max(int(checkpoint.checkpoint_line), 0)
        line_no = checkpoint_line
        logger.info(
            "worker_resume analysis_id=%s checkpoint_line=%s target_lemma=%s",
            analysis_id,
            checkpoint_line,
            target_lemma,
        )

        with file_path.open("r", encoding=settings.text_encoding, errors="ignore") as text_file:
            for _ in range(checkpoint_line):
                skipped = text_file.readline()
                if not skipped:
                    repo.save_checkpoint(analysis_id, checkpoint_line=line_no, checkpoint_offset=0)
                    return line_no

            for raw_line in text_file:
                if stop_requested and stop_requested():
                    # [Фиксируем прогресс и выходим для корректного resume]
                    repo.mark_paused(analysis_id, checkpoint_line=line_no, checkpoint_offset=0)
                    logger.info("worker_paused analysis_id=%s checkpoint_line=%s", analysis_id, line_no)
                    return line_no

                line_no += 1
                line_count = 0
                for word in extract_words(raw_line):
                    if self._normalizer.normalize(word) == target_lemma:
                        line_count += 1

                repo.save_line_result_with_checkpoint(
                    analysis_id=analysis_id,
                    lemma=target_lemma,
                    line_no=line_no,
                    line_count=line_count,
                    checkpoint_line=line_no,
                    checkpoint_offset=0,
                )

                if line_no % settings.checkpoint_every_n_lines == 0:
                    logger.info("worker_checkpoint analysis_id=%s checkpoint_line=%s", analysis_id, line_no)

        repo.save_checkpoint(analysis_id, checkpoint_line=line_no, checkpoint_offset=0)
        logger.info("worker_done analysis_id=%s total_lines=%s", analysis_id, line_no)
        return line_no
