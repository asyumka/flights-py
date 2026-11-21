"""Менеджер взаємодії з аналітичною базою даних PostgreSQL (запис вітрин Gold, логів та читання в BI)."""
from __future__ import annotations
import json
import logging
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extensions import AsIs, register_adapter
from psycopg2.extras import execute_values

from src.utils.config_manager import ConfigManager

# Реєстрація адаптерів NumPy 2.x для psycopg2
register_adapter(np.int64, AsIs)
register_adapter(np.int32, AsIs)
register_adapter(np.float64, AsIs)
register_adapter(np.float32, AsIs)
register_adapter(np.bool_, AsIs)

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Забезпечує надійне збереження агрегованих вітрин Gold, метрик ML та аудит-логів у PostgreSQL."""

    def __init__(self, config: ConfigManager | None = None) -> None:
        self.config = config or ConfigManager()

    def get_connection(self):
        return psycopg2.connect(self.config.pg_dsn)

    def is_available(self) -> bool:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
            return True
        except Exception as exc:
            logger.warning("PostgreSQL наразі недоступний: %s", exc)
            return False

    def init_schema(self, sql_file_path: str | None = None) -> bool:
        """Виконує SQL-скрипт ініціалізації таблиць, якщо база запущена."""
        if not self.is_available():
            return False
        from src.utils.config_manager import PROJECT_ROOT
        path = sql_file_path or str(PROJECT_ROOT / "sql" / "init.sql")
        with open(path, "r", encoding="utf-8") as f:
            sql_script = f.read()
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_script)
                cur.execute("ALTER TABLE dim_airports ALTER COLUMN state TYPE VARCHAR(64);")
                cur.execute("ALTER TABLE gold_airport_performance ALTER COLUMN state TYPE VARCHAR(64);")
            conn.commit()
        return True

    def replace_table_from_df(self, df: pd.DataFrame, table_name: str) -> int:
        """Очищує цільову вітрину в PostgreSQL та пакетно вставляє актуальний DataFrame."""
        if df.empty or not self.is_available():
            return 0
        clean_df = df.copy()
        clean_df = clean_df.where(pd.notnull(clean_df), None)
        columns = [c.lower() for c in clean_df.columns]
        cols_str = ", ".join(columns)
        records = [tuple(row) for row in clean_df.to_numpy()]

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
                insert_query = f"INSERT INTO {table_name} ({cols_str}) VALUES %s"
                execute_values(cur, insert_query, records, page_size=1000)
            conn.commit()
        return len(records)

    def append_records(self, df: pd.DataFrame, table_name: str, conflict_col: str | None = None) -> int:
        """Додає нові записи (наприклад, потокові події з Kafka) у таблицю PostgreSQL."""
        if df.empty or not self.is_available():
            return 0
        clean_df = df.copy().where(pd.notnull(df), None)
        columns = [c.lower() for c in clean_df.columns]
        cols_str = ", ".join(columns)
        records = [tuple(row) for row in clean_df.to_numpy()]

        conflict_clause = ""
        if conflict_col:
            update_cols = [c for c in columns if c != conflict_col.lower()]
            set_stmt = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
            conflict_clause = f" ON CONFLICT ({conflict_col.lower()}) DO UPDATE SET {set_stmt}"

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                query = f"INSERT INTO {table_name} ({cols_str}) VALUES %s{conflict_clause}"
                execute_values(cur, query, records, page_size=500)
            conn.commit()
        return len(records)

    def log_pipeline_run(
        self,
        run_id: str,
        stage_name: str,
        status: str,
        input_rows: int,
        valid_rows: int,
        quarantine_rows: int,
        duration_sec: float,
        memory_mb: float,
        details: Dict[str, Any] | None = None,
    ) -> None:
        """Записує протокол виконання етапу конвеєра у таблицю pipeline_audit_runs."""
        if not self.is_available():
            return
        throughput = round(input_rows / max(duration_sec, 0.001), 2)
        details_str = json.dumps(details or {}, ensure_ascii=False)
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO pipeline_audit_runs
                        (run_id, stage_name, status, input_rows, valid_rows, quarantine_rows,
                         duration_sec, throughput_rows_per_sec, memory_mb, details_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (run_id) DO UPDATE SET
                            status = EXCLUDED.status,
                            valid_rows = EXCLUDED.valid_rows,
                            quarantine_rows = EXCLUDED.quarantine_rows,
                            duration_sec = EXCLUDED.duration_sec,
                            throughput_rows_per_sec = EXCLUDED.throughput_rows_per_sec,
                            memory_mb = EXCLUDED.memory_mb,
                            details_json = EXCLUDED.details_json;
                        """,
                        (
                            run_id,
                            stage_name,
                            status,
                            input_rows,
                            valid_rows,
                            quarantine_rows,
                            round(duration_sec, 3),
                            throughput,
                            round(memory_mb, 2),
                            details_str,
                        ),
                    )
                conn.commit()
        except Exception as exc:
            logger.warning("Не вдалося записати аудит-лог у БД: %s", exc)

    def fetch_dataframe(self, query: str) -> pd.DataFrame:
        """Виконує SQL-запит і повертає результат у вигляді Pandas DataFrame."""
        if not self.is_available():
            return pd.DataFrame()
        with self.get_connection() as conn:
            return pd.read_sql_query(query, conn)
