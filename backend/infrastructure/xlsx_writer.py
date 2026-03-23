from __future__ import annotations

from io import BytesIO

from openpyxl import Workbook

from backend.config import settings
from backend.infrastructure.db import SQLiteAnalysisRepository


class XlsxReportWriter:
    def __init__(self, cell_limit: int | None = None):
        self._cell_limit = cell_limit or settings.excel_cell_limit

    def _build_line_count_chunks(self, line_rows: list, total_lines: int) -> list[str]:
        if total_lines <= 0:
            return [""]

        line_to_count = {int(row["line_no"]): int(row["count"]) for row in line_rows}
        chunks: list[str] = []
        current_parts: list[str] = []
        current_len = 0

        for line_no in range(1, total_lines + 1):
            value = str(line_to_count.get(line_no, 0))
            piece = value if line_no == 1 else f",{value}"
            piece_len = len(piece)

            if current_parts and current_len + piece_len > self._cell_limit:
                chunks.append("".join(current_parts))
                current_parts = [value]
                current_len = len(value)
                continue

            current_parts.append(piece)
            current_len += piece_len

        if current_parts:
            chunks.append("".join(current_parts))
        return chunks or [""]

    def generate_report_xlsx_bytes(self, analysis_id: str, repo: SQLiteAnalysisRepository) -> bytes:
        wb = Workbook()
        ws = wb.active
        ws.title = "report"
        ws.append(["lemma", "total_count", "line_counts"])

        total_lines = repo.get_total_lines(analysis_id)
        if total_lines is None:
            raise ValueError("analysis has no total_lines; report cannot be generated")

        totals = repo.get_word_totals(analysis_id)
        for total_row in totals:
            lemma = str(total_row["lemma"])
            total_count = int(total_row["total_count"])
            line_rows = repo.get_word_line_rows(analysis_id, lemma)
            chunks = self._build_line_count_chunks(line_rows, total_lines)

            ws.append([lemma, total_count, chunks[0] if chunks else ""])
            for chunk in chunks[1:]:
                ws.append([lemma, "", chunk])

        output = BytesIO()
        wb.save(output)
        return output.getvalue()
