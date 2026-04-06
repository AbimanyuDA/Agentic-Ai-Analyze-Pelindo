"""
src/utils/cache_manager.py - SQLite cache for storing analysis results
Ensures we can resume processing if interrupted.
"""
from __future__ import annotations
import sqlite3
import json
import time
from pathlib import Path
from datetime import datetime


class CacheManager:
    def __init__(self, db_path: Path):
        self.db_path = str(db_path)
        self._init_db()

    def _get_conn(self):
        return sqlite3.connect(self.db_path, timeout=30)

    def _init_db(self):
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS analyzed_tickets (
                    no_tiket TEXT PRIMARY KEY,
                    result_json TEXT NOT NULL,
                    model_used TEXT,
                    processed_at TEXT DEFAULT (datetime('now'))
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS categories (
                    id INTEGER PRIMARY KEY,
                    categories_json TEXT NOT NULL,
                    created_at TEXT DEFAULT (datetime('now'))
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS file_status (
                    filename TEXT PRIMARY KEY,
                    total_rows INTEGER,
                    processed_rows INTEGER DEFAULT 0,
                    last_updated TEXT DEFAULT (datetime('now'))
                )
            """)
            conn.commit()

    # ── Ticket Results ─────────────────────────────────────────────────────────
    def save_ticket_result(self, no_tiket: str, result: dict, model: str = ""):
        with self._get_conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO analyzed_tickets 
                   (no_tiket, result_json, model_used, processed_at)
                   VALUES (?, ?, ?, ?)""",
                (no_tiket, json.dumps(result, ensure_ascii=False),
                 model, datetime.now().isoformat()),
            )
            conn.commit()

    def save_batch_results(self, results: list[dict], model: str = ""):
        """Bulk save a list of analysis result dicts."""
        with self._get_conn() as conn:
            for r in results:
                no_tiket = r.get("no_tiket", "")
                if no_tiket:
                    conn.execute(
                        """INSERT OR REPLACE INTO analyzed_tickets
                           (no_tiket, result_json, model_used, processed_at)
                           VALUES (?, ?, ?, ?)""",
                        (no_tiket, json.dumps(r, ensure_ascii=False),
                         model, datetime.now().isoformat()),
                    )
            conn.commit()

    def get_ticket_result(self, no_tiket: str) -> object:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT result_json FROM analyzed_tickets WHERE no_tiket = ?",
                (no_tiket,),
            ).fetchone()
        return json.loads(row[0]) if row else None

    def get_processed_ids(self) -> set:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT no_tiket FROM analyzed_tickets"
            ).fetchall()
        return {r[0] for r in rows}

    def get_all_results(self) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT result_json FROM analyzed_tickets ORDER BY processed_at"
            ).fetchall()
        return [json.loads(r[0]) for r in rows]

    def get_total_processed(self) -> int:
        with self._get_conn() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM analyzed_tickets"
            ).fetchone()[0]
        return count

    # ── Categories ─────────────────────────────────────────────────────────────
    def save_categories(self, categories: list[dict]):
        with self._get_conn() as conn:
            conn.execute("DELETE FROM categories")
            conn.execute(
                "INSERT INTO categories (categories_json) VALUES (?)",
                (json.dumps(categories, ensure_ascii=False),),
            )
            conn.commit()

    def get_categories(self) -> object:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT categories_json FROM categories ORDER BY id DESC LIMIT 1"
            ).fetchone()
        return json.loads(row[0]) if row else None

    # ── File Status ─────────────────────────────────────────────────────────────
    def update_file_status(self, filename: str, total: int, processed: int):
        with self._get_conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO file_status
                   (filename, total_rows, processed_rows, last_updated)
                   VALUES (?, ?, ?, ?)""",
                (filename, total, processed, datetime.now().isoformat()),
            )
            conn.commit()

    def get_all_file_status(self) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT filename, total_rows, processed_rows, last_updated FROM file_status"
            ).fetchall()
        return [
            {"filename": r[0], "total_rows": r[1], "processed_rows": r[2], "last_updated": r[3]}
            for r in rows
        ]
