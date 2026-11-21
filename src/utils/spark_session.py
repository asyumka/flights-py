"""Фабрика ініціалізації Apache Spark Session з підтримкою Parquet та оптимізацією."""
from __future__ import annotations
import os
import sys
from pyspark.sql import SparkSession
from src.utils.config_manager import ConfigManager


def create_spark_session(app_name: str | None = None, config: ConfigManager | None = None) -> SparkSession:
    """Створює та налаштовує екземпляр SparkSession для пакетної та ML обробки."""
    cfg = config or ConfigManager()
    spark_cfg = cfg.raw_config.get("spark", {})
    name = app_name or spark_cfg.get("app_name", "PAKAPA-Flight-Punctuality-BigData")
    master = os.getenv("SPARK_MASTER", spark_cfg.get("master", "local[*]"))
    driver_mem = os.getenv("SPARK_DRIVER_MEMORY", spark_cfg.get("driver_memory", "2g"))
    shuffle_parts = str(spark_cfg.get("shuffle_partitions", 8))

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    builder = (
        SparkSession.builder.appName(name)
        .master(master)
        .config("spark.driver.memory", driver_mem)
        .config("spark.sql.shuffle.partitions", shuffle_parts)
        .config("spark.sql.parquet.compression.codec", spark_cfg.get("parquet_compression", "snappy"))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
