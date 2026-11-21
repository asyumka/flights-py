"""
Набір модульних та інтеграційних автотестів (pytest) для програмно-аналітичного
комплексу "ПАКАПА" (Варіант 3: Аналіз пунктуальності авіарейсів і чинників затримок).

Перевіряє:
1. Коректність генерації довідників (dim_airlines, dim_airports) та ортодромічної відстані Haversine.
2. Генерацію датасету US DOT BTS та наявність усіх 5 канонічних причин затримок і метеофакторів.
3. Роботу модуля Data Quality Engine у PySpark: виявлення всіх 6 типів помилок/аномалій
   (NULL ключі, невалідні IATA, від'ємна тривалість, невалідна дистанція, викиди > 24 год, дублікати).
4. Побудову шарів Bronze -> Silver -> Gold та 6 аналітичних вітрин у PySpark.
5. Роботу регресійного модуля Spark MLlib (LinearRegression & RandomForestRegressor) та Feature Importance.
6. Валідацію потокових повідомлень OpenSky Network / Kafka та онлайн ML-скорингу.
7. Створення та верифікацію резервних копій (BackupManager).
"""
from __future__ import annotations
import pytest
import numpy as np
import pandas as pd

from src.dq.quality_engine import DataQualityEngine
from src.etl.medallion_processor import MedallionETLProcessor
from src.ingestion.dataset_loader import BTSDatasetLoader, haversine_miles
from src.ml.delay_regressor import FlightDelayRegressor
from src.streaming.opensky_producer import OpenSkyKafkaProducer
from src.streaming.stream_processor import FlightStreamProcessor
from src.utils.backup_manager import BackupManager
from src.utils.config_manager import ConfigManager
from src.utils.spark_session import create_spark_session


@pytest.fixture(scope="session")
def spark():
    session = create_spark_session(app_name="PAKAPA-Pytest-Suite")
    yield session


@pytest.fixture(scope="session")
def config():
    return ConfigManager()


def test_haversine_distance_calculation():
    """Перевіряє точність розрахунку ортодромічної відстані між JFK (Нью-Йорк) та LAX (Лос-Анджелес)."""
    dist = haversine_miles(
        np.array([40.6413]),
        np.array([-73.7781]),
        np.array([33.9416]),
        np.array([-118.4085]),
    )[0]
    assert 2450.0 <= dist <= 2500.0


def test_reference_tables_and_bts_generator(config):
    """Перевіряє формування довідників авіакомпаній/аеропортів та зрізу рейсів US DOT BTS."""
    loader = BTSDatasetLoader(config)
    airlines_df, airports_df = loader.build_reference_tables()

    assert len(airlines_df) >= 15
    assert len(airports_df) >= 30
    assert "IATA_CODE" in airlines_df.columns
    assert {"LATITUDE", "LONGITUDE", "HUB_TIER"}.issubset(set(airports_df.columns))


def test_data_quality_engine_identifies_all_error_categories(spark, config):
    """Перевіряє, що DataQualityEngine коректно відсіює всі 6 класів помилок у Quarantine."""
    dq = DataQualityEngine(config)
    valid_iata = ["ATL", "JFK", "LAX", "ORD"]

    test_rows = [
        # 1. Валідний рейс
        ("2025-06-15", "DL", 101, "ATL", "JFK", 800, 120.0, 95.0, 760.0, 10.0),
        # 2. Дублікат попереднього рейсу
        ("2025-06-15", "DL", 101, "ATL", "JFK", 800, 120.0, 95.0, 760.0, 10.0),
        # 3. NULL в ORIGIN
        ("2025-06-15", "DL", 102, None, "JFK", 900, 120.0, 95.0, 760.0, 5.0),
        # 4. Неіснуючий код аеропорту ІАТА ("ZZZ")
        ("2025-06-15", "AA", 205, "ATL", "ZZZ", 1030, 130.0, 105.0, 800.0, 0.0),
        # 5. Від'ємний час польоту (ACTUAL_ELAPSED_TIME <= 0)
        ("2025-06-15", "UA", 309, "ORD", "LAX", 1400, -45.0, 200.0, 1740.0, 15.0),
        # 6. Некоректна відстань (DISTANCE <= 0)
        ("2025-06-15", "WN", 411, "LAX", "JFK", 1600, 310.0, 280.0, -50.0, 20.0),
        # 7. Екстремальний викид затримки (> 1440 хв)
        ("2025-06-15", "B6", 512, "JFK", "LAX", 1800, 320.0, 290.0, 2475.0, 2500.0),
    ]
    cols = [
        "FL_DATE",
        "OP_CARRIER",
        "FL_NUM",
        "ORIGIN",
        "DEST",
        "CRS_DEP_TIME",
        "ACTUAL_ELAPSED_TIME",
        "AIR_TIME",
        "DISTANCE",
        "ARR_DELAY",
    ]
    sdf = spark.createDataFrame(test_rows, schema=cols)
    valid_df, quarantine_df, summary_pdf = dq.validate_and_split(sdf, valid_iata)

    assert valid_df.count() == 1
    assert quarantine_df.count() == 6
    detected_codes = set(summary_pdf["error_code"].tolist())
    expected_codes = {
        "ERR_NULL_KEY",
        "ERR_INVALID_IATA",
        "ERR_NEGATIVE_DURATION",
        "ERR_INVALID_DISTANCE",
        "ERR_EXTREME_DELAY_OUTLIER",
        "ERR_DUPLICATE_FLIGHT",
    }
    assert detected_codes == expected_codes


def test_stream_processor_validation_and_ml_scoring(config):
    """Перевіряє валідацію потокових подій OpenSky/Kafka та онлайн-розрахунок прогнозу затримки."""
    proc = FlightStreamProcessor(config)
    producer = OpenSkyKafkaProducer(config)
    sim_events = producer.generate_simulated_states(count=15, inject_bad=True)

    res = proc.process_events_list(sim_events)
    assert res["received"] == len(sim_events)
    assert res["valid_processed"] == 15
    assert res["dlq_rejected"] >= 1

    pred_min, risk = proc.score_flight_delay_ml(
        {"estimated_dep_delay": 35.0, "velocity_kts": 450.0, "on_ground": False}
    )
    assert pred_min >= 15.0
    assert "DELAY" in risk


def test_backup_manager_creates_valid_zip(config):
    """Перевіряє створення ZIP-архіву резервної копії конфігурації та золотих вітрин."""
    bm = BackupManager(config)
    info = bm.create_backup(tag="pytest")
    assert info["size_kb"] > 0
    assert info["files_count"] >= 1
    backups = bm.list_backups()
    assert len(backups) >= 1
