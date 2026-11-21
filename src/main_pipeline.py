"""
Головний оркестратор програмно-аналітичного комплексу "ПАКАПА" (Варіант 3).

Послідовно виконує повний цикл обробки великих даних:
1. Ініціалізація схеми аналітичної СУБД PostgreSQL.
2. Генерація/завантаження датасету US DOT BTS On-Time Performance та довідників (airlines, airports).
3. Ingest у шар Bronze (Parquet).
4. Модуль Data Quality: валідація записів, ізоляція невалідних даних у Quarantine, формування Silver (Parquet).
5. Побудова 6 аналітичних вітрин шару Gold (Parquet + синхронізація у PostgreSQL).
6. Навчання та оцінювання регресійних моделей затримок (ARR_DELAY) у PySpark MLlib + Feature Importance.
7. Потоковий цикл (OpenSky Network API -> Apache Kafka -> Stream Processor -> PostgreSQL).
8. Створення резервної копії вітрин Gold та збереження метрик швидкодії у протокол виконання.
"""
from __future__ import annotations
import argparse
import json
import logging
import time
import uuid
from datetime import datetime
from typing import Any, Dict

import pandas as pd
import psutil

from src.etl.medallion_processor import MedallionETLProcessor
from src.ingestion.dataset_loader import BTSDatasetLoader
from src.ml.delay_regressor import FlightDelayRegressor
from src.streaming.stream_processor import FlightStreamProcessor
from src.utils.backup_manager import BackupManager
from src.utils.config_manager import ConfigManager
from src.utils.db_manager import DatabaseManager
from src.utils.spark_session import create_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
logger = logging.getLogger("PAKAPA.Orchestrator")


def get_memory_mb() -> float:
    proc = psutil.Process()
    return round(proc.memory_info().rss / (1024 * 1024), 2)


def run_full_pipeline(
    num_records: int = 200_000,
    force_regenerate: bool = False,
    run_streaming: bool = True,
) -> Dict[str, Any]:
    """Виконує повний наскрізний прогін конвеєра ПАКАПА з фіксацією телеметрії кожного етапу."""
    pipeline_start = time.perf_counter()
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    cfg = ConfigManager()
    db = DatabaseManager(cfg)

    logger.info("=== СТАРТ КОНВЕЄРА ПАКАПА (Run ID: %s, цільовий обсяг: %d записів) ===", batch_id, num_records)
    db_ready = db.init_schema()
    logger.info("Статус підключення до PostgreSQL: %s", "АКТИВНО" if db_ready else "ОФЛАЙН (Parquet Lake Mode)")

    audit_records = []

    # -------------------------------------------------------------------------
    # ЕТАП 1: Завантаження / формування датасету US DOT BTS + Довідники
    # -------------------------------------------------------------------------
    t0 = time.perf_counter()
    loader = BTSDatasetLoader(cfg)
    airlines_pdf, airports_pdf = loader.build_reference_tables()
    gen_info = loader.generate_bts_flights_dataset(
        num_records=num_records,
        force_regenerate=force_regenerate,
    )
    raw_rows = int(gen_info["rows_generated"])
    d1 = round(time.perf_counter() - t0, 3)
    mem1 = get_memory_mb()
    db.log_pipeline_run(
        run_id=f"{batch_id}_01_ingestion",
        stage_name="1. Data Ingestion (US DOT BTS + Reference Tables)",
        status="SUCCESS",
        input_rows=raw_rows,
        valid_rows=raw_rows,
        quarantine_rows=0,
        duration_sec=d1,
        memory_mb=mem1,
        details=gen_info,
    )
    audit_records.append(
        {
            "run_id": f"{batch_id}_01_ingestion",
            "stage_name": "1. Data Ingestion (US DOT BTS + Reference Tables)",
            "status": "SUCCESS",
            "input_rows": raw_rows,
            "valid_rows": raw_rows,
            "quarantine_rows": 0,
            "duration_sec": d1,
            "throughput_rows_per_sec": round(raw_rows / max(d1, 0.001), 1),
            "memory_mb": mem1,
        }
    )
    logger.info("Етап 1 завершено за %.2f с (рядків: %d)", d1, raw_rows)

    # -------------------------------------------------------------------------
    # ЕТАП 2: Ініціалізація PySpark та запис у шар Bronze (Parquet)
    # -------------------------------------------------------------------------
    t0 = time.perf_counter()
    spark = create_spark_session(config=cfg)
    etl = MedallionETLProcessor(spark, cfg, db)
    bronze_df = etl.ingest_to_bronze(gen_info["flights_csv"])
    bronze_count = bronze_df.count()
    d2 = round(time.perf_counter() - t0, 3)
    mem2 = get_memory_mb()
    db.log_pipeline_run(
        run_id=f"{batch_id}_02_bronze",
        stage_name="2. Bronze Layer Ingestion (Parquet Snappy)",
        status="SUCCESS",
        input_rows=raw_rows,
        valid_rows=bronze_count,
        quarantine_rows=0,
        duration_sec=d2,
        memory_mb=mem2,
    )
    audit_records.append(
        {
            "run_id": f"{batch_id}_02_bronze",
            "stage_name": "2. Bronze Layer Ingestion (Parquet Snappy)",
            "status": "SUCCESS",
            "input_rows": raw_rows,
            "valid_rows": bronze_count,
            "quarantine_rows": 0,
            "duration_sec": d2,
            "throughput_rows_per_sec": round(bronze_count / max(d2, 0.001), 1),
            "memory_mb": mem2,
        }
    )
    logger.info("Етап 2 (Bronze Parquet) завершено за %.2f с (рядків: %d)", d2, bronze_count)

    # -------------------------------------------------------------------------
    # ЕТАП 3: Модуль Data Quality, Quarantine та побудова шару Silver
    # -------------------------------------------------------------------------
    t0 = time.perf_counter()
    silver_df, quarantine_df, dq_summary_pdf = etl.build_silver_and_quarantine(
        bronze_df, airlines_pdf, airports_pdf
    )
    silver_count = silver_df.count()
    quarantine_count = int(dq_summary_pdf["rejected_count"].sum()) if not dq_summary_pdf.empty else 0
    dq_summary_pdf.to_parquet(cfg.gold_dir / "dq_quarantine_summary.parquet", index=False)

    d3 = round(time.perf_counter() - t0, 3)
    mem3 = get_memory_mb()
    db.log_pipeline_run(
        run_id=f"{batch_id}_03_silver_dq",
        stage_name="3. Data Quality Validation & Silver Enrichment",
        status="SUCCESS",
        input_rows=bronze_count,
        valid_rows=silver_count,
        quarantine_rows=quarantine_count,
        duration_sec=d3,
        memory_mb=mem3,
    )
    audit_records.append(
        {
            "run_id": f"{batch_id}_03_silver_dq",
            "stage_name": "3. Data Quality Validation & Silver Enrichment",
            "status": "SUCCESS",
            "input_rows": bronze_count,
            "valid_rows": silver_count,
            "quarantine_rows": quarantine_count,
            "duration_sec": d3,
            "throughput_rows_per_sec": round(bronze_count / max(d3, 0.001), 1),
            "memory_mb": mem3,
        }
    )
    logger.info(
        "Етап 3 (Silver + DQ) завершено за %.2f с: валідних=%d, у карантині=%d (%.2f%%)",
        d3,
        silver_count,
        quarantine_count,
        (quarantine_count / max(bronze_count, 1)) * 100.0,
    )

    # -------------------------------------------------------------------------
    # ЕТАП 4: Формування 6 аналітичних вітрин шару Gold (Parquet + PostgreSQL)
    # -------------------------------------------------------------------------
    t0 = time.perf_counter()
    marts = etl.build_gold_marts(silver_df, airlines_pdf, airports_pdf)
    d4 = round(time.perf_counter() - t0, 3)
    mem4 = get_memory_mb()
    db.log_pipeline_run(
        run_id=f"{batch_id}_04_gold_marts",
        stage_name="4. Gold Analytical Marts Aggregation (6 Marts + PostgreSQL)",
        status="SUCCESS",
        input_rows=silver_count,
        valid_rows=silver_count,
        quarantine_rows=0,
        duration_sec=d4,
        memory_mb=mem4,
    )
    audit_records.append(
        {
            "run_id": f"{batch_id}_04_gold_marts",
            "stage_name": "4. Gold Analytical Marts Aggregation (6 Marts + PostgreSQL)",
            "status": "SUCCESS",
            "input_rows": silver_count,
            "valid_rows": silver_count,
            "quarantine_rows": 0,
            "duration_sec": d4,
            "throughput_rows_per_sec": round(silver_count / max(d4, 0.001), 1),
            "memory_mb": mem4,
        }
    )
    logger.info("Етап 4 (Gold Marts) завершено за %.2f с", d4)

    # -------------------------------------------------------------------------
    # ЕТАП 5: Навчання регресійних моделей затримки у Spark MLlib (LR + RF)
    # -------------------------------------------------------------------------
    t0 = time.perf_counter()
    ml_regressor = FlightDelayRegressor(spark, cfg, db)
    ml_results = ml_regressor.train_and_evaluate(silver_df)
    d5 = round(time.perf_counter() - t0, 3)
    mem5 = get_memory_mb()
    db.log_pipeline_run(
        run_id=f"{batch_id}_05_spark_mllib",
        stage_name="5. Spark MLlib Delay Regression (Linear & Random Forest)",
        status="SUCCESS",
        input_rows=silver_count,
        valid_rows=silver_count,
        quarantine_rows=0,
        duration_sec=d5,
        memory_mb=mem5,
    )
    audit_records.append(
        {
            "run_id": f"{batch_id}_05_spark_mllib",
            "stage_name": "5. Spark MLlib Delay Regression (Linear & Random Forest)",
            "status": "SUCCESS",
            "input_rows": silver_count,
            "valid_rows": silver_count,
            "quarantine_rows": 0,
            "duration_sec": d5,
            "throughput_rows_per_sec": round(silver_count / max(d5, 0.001), 1),
            "memory_mb": mem5,
        }
    )
    logger.info("Етап 5 (Spark MLlib) завершено за %.2f с", d5)

    # -------------------------------------------------------------------------
    # ЕТАП 6: Потоковий шар (OpenSky Network API -> Apache Kafka -> PostgreSQL)
    # -------------------------------------------------------------------------
    stream_stats: Dict[str, Any] = {}
    if run_streaming:
        t0 = time.perf_counter()
        stream_proc = FlightStreamProcessor(cfg, db)
        stream_stats = stream_proc.consume_from_kafka_or_direct(max_messages=65, mode="auto")
        d6 = round(time.perf_counter() - t0, 3)
        mem6 = get_memory_mb()
        db.log_pipeline_run(
            run_id=f"{batch_id}_06_kafka_opensky",
            stage_name=f"6. Kafka Live Streaming ({stream_stats.get('source_mode', 'auto')})",
            status="SUCCESS",
            input_rows=stream_stats.get("received", 0),
            valid_rows=stream_stats.get("valid_processed", 0),
            quarantine_rows=stream_stats.get("dlq_rejected", 0),
            duration_sec=d6,
            memory_mb=mem6,
            details=stream_stats,
        )
        audit_records.append(
            {
                "run_id": f"{batch_id}_06_kafka_opensky",
                "stage_name": f"6. Kafka Live Streaming ({stream_stats.get('source_mode', 'auto')})",
                "status": "SUCCESS",
                "input_rows": stream_stats.get("received", 0),
                "valid_rows": stream_stats.get("valid_processed", 0),
                "quarantine_rows": stream_stats.get("dlq_rejected", 0),
                "duration_sec": d6,
                "throughput_rows_per_sec": round(stream_stats.get("received", 0) / max(d6, 0.001), 1),
                "memory_mb": mem6,
            }
        )
        logger.info("Етап 6 (Kafka + OpenSky Streaming) завершено за %.2f с: %s", d6, stream_stats)

    # -------------------------------------------------------------------------
    # ЕТАП 7: Резервне копіювання золотих вітрин та фіналізація звіту
    # -------------------------------------------------------------------------
    audit_pdf = pd.DataFrame(audit_records)
    audit_pdf.to_parquet(cfg.gold_dir / "pipeline_audit_runs.parquet", index=False)

    backup_mgr = BackupManager(cfg)
    backup_info = backup_mgr.create_backup(tag="post_etl")

    total_duration = round(time.perf_counter() - pipeline_start, 2)
    summary_report = {
        "batch_id": batch_id,
        "total_duration_sec": total_duration,
        "bronze_rows": bronze_count,
        "silver_valid_rows": silver_count,
        "quarantine_rejected_rows": quarantine_count,
        "quarantine_rate_pct": round((quarantine_count / max(bronze_count, 1)) * 100.0, 2),
        "ml_metrics": ml_results["metrics"].to_dict(orient="records"),
        "top_features": ml_results["feature_importance"].head(5).to_dict(orient="records"),
        "streaming": stream_stats,
        "backup": backup_info,
    }
    with open(cfg.gold_dir / "latest_run_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary_report, f, indent=2, ensure_ascii=False)

    logger.info("=== КОНВЕЄР ПАКАПА УСПІШНО ЗАВЕРШЕНО ЗА %.2f с ===", total_duration)
    return summary_report


def main() -> None:
    parser = argparse.ArgumentParser(description="PAKAPA Main Big Data Pipeline Orchestrator")
    parser.add_argument("--records", type=int, default=200_000, help="Кількість записів US DOT BTS")
    parser.add_argument("--force-regen", action="store_true", help="Примусово перегенерувати сирий CSV")
    parser.add_argument("--skip-stream", action="store_true", help="Пропустити потоковий крок Kafka")
    args = parser.parse_args()

    run_full_pipeline(
        num_records=args.records,
        force_regenerate=args.force_regen,
        run_streaming=not args.skip_stream,
    )


if __name__ == "__main__":
    main()
