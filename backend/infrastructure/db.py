from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"
STATUS_PAUSED = "paused"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


def init_database(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS analyses (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            retry_count INTEGER NOT NULL DEFAULT 0,
            checkpoint_line INTEGER NOT NULL DEFAULT 0,
            checkpoint_offset INTEGER NOT NULL DEFAULT 0,
            total_lines INTEGER,
            file_path TEXT NOT NULL,
            target_lemma TEXT NOT NULL DEFAULT '',
            error_message TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS word_totals (
            analysis_id TEXT NOT NULL,
            lemma TEXT NOT NULL,
            total_count INTEGER NOT NULL,
            PRIMARY KEY (analysis_id, lemma),
            FOREIGN KEY (analysis_id) REFERENCES analyses(id) ON DELETE CASCADE
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS word_line_counts (
            analysis_id TEXT NOT NULL,
            lemma TEXT NOT NULL,
            line_no INTEGER NOT NULL,
            count INTEGER NOT NULL,
            PRIMARY KEY (analysis_id, lemma, line_no),
            FOREIGN KEY (analysis_id) REFERENCES analyses(id) ON DELETE CASCADE
        );
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_word_line_counts_lookup
        ON word_line_counts(analysis_id, lemma, line_no);
        """
    )
    # [Добавляем колонку для старой БД без мигратора]
    columns = conn.execute("PRAGMA table_info(analyses);").fetchall()
    column_names = {str(row["name"]) for row in columns}
    if "target_lemma" not in column_names:
        conn.execute("ALTER TABLE analyses ADD COLUMN target_lemma TEXT NOT NULL DEFAULT '';")
    conn.commit()


@dataclass(frozen=True)
class AnalysisCheckpoint:
    file_path: str
    checkpoint_line: int
    checkpoint_offset: int


class SQLiteAnalysisRepository:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._lock = threading.Lock()

    def init(self) -> None:
        init_database(self._conn)

    def create_analysis(self, *, analysis_id: str, file_path: str, target_lemma: str) -> None:
        now = utc_now_iso()
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO analyses (
                    id, status, retry_count, checkpoint_line, checkpoint_offset, total_lines,
                    file_path, target_lemma, error_message, created_at, updated_at
                ) VALUES (?, ?, 0, 0, 0, NULL, ?, ?, NULL, ?, ?);
                """,
                (analysis_id, STATUS_PENDING, file_path, target_lemma, now, now),
            )

    def get_analysis(self, analysis_id: str) -> sqlite3.Row | None:
        cur = self._conn.execute("SELECT * FROM analyses WHERE id = ?;", (analysis_id,))
        return cur.fetchone()

    def set_status(
        self,
        analysis_id: str,
        *,
        status: str,
        checkpoint_line: int | None = None,
        checkpoint_offset: int | None = None,
        total_lines: int | None = None,
        error_message: str | None = None,
    ) -> None:
        now = utc_now_iso()
        set_parts = ["status = ?", "updated_at = ?"]
        params: list[object] = [status, now]

        if checkpoint_line is not None:
            set_parts.append("checkpoint_line = ?")
            params.append(checkpoint_line)
        if checkpoint_offset is not None:
            set_parts.append("checkpoint_offset = ?")
            params.append(checkpoint_offset)
        if total_lines is not None:
            set_parts.append("total_lines = ?")
            params.append(total_lines)
        if error_message is not None:
            set_parts.append("error_message = ?")
            params.append(error_message)

        sql = f"UPDATE analyses SET {', '.join(set_parts)} WHERE id = ?;"
        params.append(analysis_id)
        with self._lock, self._conn:
            self._conn.execute(sql, tuple(params))

    def increment_retry_and_reset(self, analysis_id: str) -> None:
        now = utc_now_iso()
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM word_line_counts WHERE analysis_id = ?;", (analysis_id,))
            self._conn.execute("DELETE FROM word_totals WHERE analysis_id = ?;", (analysis_id,))
            self._conn.execute(
                """
                UPDATE analyses
                SET retry_count = retry_count + 1,
                    status = ?,
                    checkpoint_line = 0,
                    checkpoint_offset = 0,
                    total_lines = NULL,
                    error_message = NULL,
                    updated_at = ?
                WHERE id = ?;
                """,
                (STATUS_PENDING, now, analysis_id),
            )

    def list_unfinished_analysis_ids(self) -> list[str]:
        cur = self._conn.execute(
            "SELECT id FROM analyses WHERE status IN (?, ?, ?);",
            (STATUS_PENDING, STATUS_RUNNING, STATUS_PAUSED),
        )
        return [row["id"] for row in cur.fetchall()]

    def mark_running(self, analysis_id: str) -> None:
        self.set_status(analysis_id, status=STATUS_RUNNING)

    def mark_paused(self, analysis_id: str, *, checkpoint_line: int, checkpoint_offset: int) -> None:
        self.set_status(
            analysis_id,
            status=STATUS_PAUSED,
            checkpoint_line=checkpoint_line,
            checkpoint_offset=checkpoint_offset,
        )

    def mark_success(self, analysis_id: str, *, total_lines: int) -> None:
        self.set_status(analysis_id, status=STATUS_SUCCESS, total_lines=total_lines, error_message="")

    def mark_failed(self, analysis_id: str, *, error_message: str) -> None:
        self.set_status(analysis_id, status=STATUS_FAILED, error_message=error_message)

    def get_checkpoint(self, analysis_id: str) -> AnalysisCheckpoint:
        cur = self._conn.execute(
            """
            SELECT file_path, checkpoint_line, checkpoint_offset
            FROM analyses
            WHERE id = ?;
            """,
            (analysis_id,),
        )
        row = cur.fetchone()
        if not row:
            raise KeyError(f"analysis not found: {analysis_id}")
        return AnalysisCheckpoint(
            file_path=row["file_path"],
            checkpoint_line=int(row["checkpoint_line"]),
            checkpoint_offset=int(row["checkpoint_offset"]),
        )

    def save_checkpoint(self, analysis_id: str, *, checkpoint_line: int, checkpoint_offset: int) -> None:
        row = self.get_analysis(analysis_id)
        if not row:
            raise KeyError(f"analysis not found: {analysis_id}")
        self.set_status(
            analysis_id,
            status=row["status"],
            checkpoint_line=checkpoint_line,
            checkpoint_offset=checkpoint_offset,
        )

    def get_retry_count(self, analysis_id: str) -> int:
        row = self.get_analysis(analysis_id)
        if not row:
            raise KeyError(f"analysis not found: {analysis_id}")
        return int(row["retry_count"])

    def get_total_lines(self, analysis_id: str) -> int | None:
        row = self.get_analysis(analysis_id)
        if not row:
            raise KeyError(f"analysis not found: {analysis_id}")
        if row["total_lines"] is None:
            return None
        return int(row["total_lines"])

    def get_target_lemma(self, analysis_id: str) -> str:
        row = self.get_analysis(analysis_id)
        if not row:
            raise KeyError(f"analysis not found: {analysis_id}")
        return str(row["target_lemma"])

    def save_line_result_with_checkpoint(
        self,
        *,
        analysis_id: str,
        lemma: str,
        line_no: int,
        line_count: int,
        checkpoint_line: int,
        checkpoint_offset: int = 0,
    ) -> None:
        now = utc_now_iso()
        with self._lock, self._conn:
            previous_row = self._conn.execute(
                """
                SELECT count
                FROM word_line_counts
                WHERE analysis_id = ? AND lemma = ? AND line_no = ?;
                """,
                (analysis_id, lemma, int(line_no)),
            ).fetchone()
            previous_count = int(previous_row["count"]) if previous_row else 0
            new_count = int(line_count)
            delta = new_count - previous_count

            if new_count <= 0:
                self._conn.execute(
                    """
                    DELETE FROM word_line_counts
                    WHERE analysis_id = ? AND lemma = ? AND line_no = ?;
                    """,
                    (analysis_id, lemma, int(line_no)),
                )
            else:
                self._conn.execute(
                    """
                    INSERT INTO word_line_counts (analysis_id, lemma, line_no, count)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(analysis_id, lemma, line_no)
                    DO UPDATE SET count = excluded.count;
                    """,
                    (analysis_id, lemma, int(line_no), new_count),
                )

            self._conn.execute(
                """
                INSERT INTO word_totals (analysis_id, lemma, total_count)
                VALUES (?, ?, 0)
                ON CONFLICT(analysis_id, lemma)
                DO NOTHING;
                """,
                (analysis_id, lemma),
            )
            if delta != 0:
                self._conn.execute(
                    """
                    UPDATE word_totals
                    SET total_count = total_count + ?
                    WHERE analysis_id = ? AND lemma = ?;
                    """,
                    (delta, analysis_id, lemma),
                )

            self._conn.execute(
                """
                UPDATE analyses
                SET checkpoint_line = ?,
                    checkpoint_offset = ?,
                    updated_at = ?
                WHERE id = ?;
                """,
                (int(checkpoint_line), int(checkpoint_offset), now, analysis_id),
            )

    def upsert_word_totals_batch(self, analysis_id: str, lemma_to_add: dict[str, int]) -> None:
        if not lemma_to_add:
            return
        items = [(analysis_id, lemma, int(count)) for lemma, count in lemma_to_add.items()]
        with self._lock, self._conn:
            self._conn.executemany(
                """
                INSERT INTO word_totals (analysis_id, lemma, total_count)
                VALUES (?, ?, ?)
                ON CONFLICT(analysis_id, lemma)
                DO UPDATE SET total_count = word_totals.total_count + excluded.total_count;
                """,
                items,
            )

    def upsert_word_line_counts_batch(self, analysis_id: str, line_no: int, lemma_to_add: dict[str, int]) -> None:
        if not lemma_to_add:
            return
        items = [(analysis_id, lemma, int(line_no), int(count)) for lemma, count in lemma_to_add.items()]
        with self._lock, self._conn:
            self._conn.executemany(
                """
                INSERT INTO word_line_counts (analysis_id, lemma, line_no, count)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(analysis_id, lemma, line_no)
                DO UPDATE SET count = word_line_counts.count + excluded.count;
                """,
                items,
            )

    def get_word_totals(self, analysis_id: str) -> list[sqlite3.Row]:
        cur = self._conn.execute(
            """
            SELECT lemma, total_count
            FROM word_totals
            WHERE analysis_id = ?
            ORDER BY lemma ASC;
            """,
            (analysis_id,),
        )
        return cur.fetchall()

    def get_word_line_rows(self, analysis_id: str, lemma: str) -> list[sqlite3.Row]:
        cur = self._conn.execute(
            """
            SELECT line_no, count
            FROM word_line_counts
            WHERE analysis_id = ? AND lemma = ?
            ORDER BY line_no ASC;
            """,
            (analysis_id, lemma),
        )
        return cur.fetchall()

