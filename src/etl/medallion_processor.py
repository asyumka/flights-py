"""
Реалізація пакетного ETL-конвеєра на базі Apache Spark (PySpark SQL & DataFrame API)
за архітектурою Medallion (Bronze -> Silver -> Gold + Quarantine).

Основні операції:
1. Bronze Layer: читання сирого CSV US DOT BTS з явною типізацією схеми та збереження у Parquet.
2. Data Quality & Quarantine: валідація записів, ізоляція аномалій у Quarantine Parquet + БД.
3. Silver Layer: збагачення довідниками авіакомпаній та аеропортів (Broadcast Join),
   конструювання аналітичних ознак (IS_DELAYED_15, IS_SEVERE_DELAY_60, ROUTE_ID, TIME_OF_DAY_BUCKET,
   DELAY_PROPAGATION_RATIO), збереження очищеного шару у Parquet.
4. Gold Layer: розрахунок 6 багатовимірних аналітичних вітрин за pomocą Spark SQL / віконних
   та агрегатних функцій, збереження у Parquet та синхронізація з PostgreSQL.
"""
from __future__ import annotations
from typing import Dict, Tuple

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.dq.quality_engine import DataQualityEngine
from src.utils.config_manager import ConfigManager
from src.utils.db_manager import DatabaseManager

BTS_RAW_SCHEMA = StructType(
    [
        StructField("FL_DATE", StringType(), True),
        StructField("MONTH", IntegerType(), True),
        StructField("DAY_OF_WEEK", IntegerType(), True),
        StructField("OP_CARRIER", StringType(), True),
        StructField("FL_NUM", IntegerType(), True),
        StructField("ORIGIN", StringType(), True),
        StructField("DEST", StringType(), True),
        StructField("CRS_DEP_TIME", IntegerType(), True),
        StructField("DEP_HOUR", IntegerType(), True),
        StructField("DEP_DELAY", DoubleType(), True),
        StructField("TAXI_OUT", DoubleType(), True),
        StructField("TAXI_IN", DoubleType(), True),
        StructField("ARR_DELAY", DoubleType(), True),
        StructField("CANCELLED", IntegerType(), True),
        StructField("CANCELLATION_CODE", StringType(), True),
        StructField("CRS_ELAPSED_TIME", DoubleType(), True),
        StructField("ACTUAL_ELAPSED_TIME", DoubleType(), True),
        StructField("AIR_TIME", DoubleType(), True),
        StructField("DISTANCE", DoubleType(), True),
        StructField("CARRIER_DELAY", DoubleType(), True),
        StructField("WEATHER_DELAY", DoubleType(), True),
        StructField("NAS_DELAY", DoubleType(), True),
        StructField("SECURITY_DELAY", DoubleType(), True),
        StructField("LATE_AIRCRAFT_DELAY", DoubleType(), True),
        StructField("PREV_AIRCRAFT_DELAY", DoubleType(), True),
        StructField("WEATHER_CONDITION", StringType(), True),
        StructField("TEMP_C", DoubleType(), True),
        StructField("WIND_SPEED_KTS", DoubleType(), True),
        StructField("VISIBILITY_MILES", DoubleType(), True),
        StructField("PRECIP_MM", DoubleType(), True),
        StructField("WEATHER_SEVERITY_IDX", DoubleType(), True),
    ]
)


class MedallionETLProcessor:
    """Клас керування багатошаровим ETL-конвеєром обробки авіарейсів у PySpark."""

    def __init__(
        self,
        spark: SparkSession,
        config: ConfigManager | None = None,
        db: DatabaseManager | None = None,
    ) -> None:
        self.spark = spark
        self.config = config or ConfigManager()
        self.db = db or DatabaseManager(self.config)
        self.dq_engine = DataQualityEngine(self.config)

    def ingest_to_bronze(self, raw_csv_path: str) -> DataFrame:
        """Читає сирий CSV-масив US DOT BTS із заданою схемою та записує у шар Bronze (Parquet)."""
        raw_df = (
            self.spark.read.option("header", "true")
            .schema(BTS_RAW_SCHEMA)
            .csv(raw_csv_path)
            .withColumn("INGESTED_AT", F.current_timestamp())
        )
        bronze_path = str(self.config.bronze_dir / "flights_bronze.parquet")
        raw_df.write.mode("overwrite").parquet(bronze_path)
        return self.spark.read.parquet(bronze_path)

    def build_silver_and_quarantine(
        self,
        bronze_df: DataFrame,
        airlines_pdf: pd.DataFrame,
        airports_pdf: pd.DataFrame,
    ) -> Tuple[DataFrame, DataFrame, pd.DataFrame]:
        """
        Виконує перевірку якості даних, зберігає відхилені записи у Quarantine,
        а валідні збагачує довідниками через Broadcast Join та зберігає у шар Silver.
        """
        valid_iata = airports_pdf["IATA_CODE"].dropna().unique().tolist()
        valid_df, quarantine_df, dq_summary_pdf = self.dq_engine.validate_and_split(bronze_df, valid_iata)

        # Збереження карантинного шару у Parquet
        quarantine_path = str(self.config.quarantine_dir / "flights_quarantine.parquet")
        quarantine_df.write.mode("overwrite").parquet(quarantine_path)

        # Запис зразків карантину та зведення у PostgreSQL
        if self.db.is_available():
            self.db.replace_table_from_df(dq_summary_pdf, "dq_quarantine_summary")
            q_sample_pdf = (
                quarantine_df.select(
                    "FL_DATE",
                    "OP_CARRIER",
                    F.col("FL_NUM").cast("string").alias("FL_NUM"),
                    "ORIGIN",
                    "DEST",
                    "DEP_DELAY",
                    "ARR_DELAY",
                    "ACTUAL_ELAPSED_TIME",
                    "DISTANCE",
                    "DQ_ERROR_REASON",
                )
                .limit(200)
                .toPandas()
            )
            self.db.replace_table_from_df(q_sample_pdf, "dq_quarantine_samples")

        # Broadcast Join із довідниками авіакомпаній та аеропортів
        airlines_sdf = F.broadcast(self.spark.createDataFrame(airlines_pdf))
        airports_orig_sdf = F.broadcast(
            self.spark.createDataFrame(airports_pdf).select(
                F.col("IATA_CODE").alias("ORIGIN"),
                F.col("AIRPORT_NAME").alias("ORIGIN_AIRPORT_NAME"),
                F.col("CITY").alias("ORIGIN_CITY"),
                F.col("STATE").alias("ORIGIN_STATE"),
                F.col("LATITUDE").alias("ORIGIN_LAT"),
                F.col("LONGITUDE").alias("ORIGIN_LON"),
                F.col("HUB_TIER").alias("ORIGIN_HUB_TIER"),
                F.col("CLIMATE_ZONE").alias("ORIGIN_CLIMATE_ZONE"),
            )
        )
        airports_dest_sdf = F.broadcast(
            self.spark.createDataFrame(airports_pdf).select(
                F.col("IATA_CODE").alias("DEST"),
                F.col("CITY").alias("DEST_CITY"),
                F.col("STATE").alias("DEST_STATE"),
                F.col("LATITUDE").alias("DEST_LAT"),
                F.col("LONGITUDE").alias("DEST_LON"),
            )
        )

        otp_thresh = float(self.config.dq_rules.get("otp_threshold_minutes", 15.0))
        severe_thresh = float(self.config.dq_rules.get("severe_delay_minutes", 60.0))

        silver_df = (
            valid_df.join(
                airlines_sdf.select(
                    F.col("IATA_CODE").alias("OP_CARRIER"),
                    "AIRLINE_NAME",
                    "ALLIANCE",
                    "FLEET_TYPE",
                ),
                on="OP_CARRIER",
                how="left",
            )
            .join(airports_orig_sdf, on="ORIGIN", how="left")
            .join(airports_dest_sdf, on="DEST", how="left")
            .withColumn("IS_DELAYED_15", F.when(F.col("ARR_DELAY") >= otp_thresh, 1).otherwise(0))
            .withColumn("IS_DEP_DELAYED_15", F.when(F.col("DEP_DELAY") >= otp_thresh, 1).otherwise(0))
            .withColumn("IS_SEVERE_DELAY_60", F.when(F.col("ARR_DELAY") >= severe_thresh, 1).otherwise(0))
            .withColumn("ROUTE_ID", F.concat_ws(" -> ", F.col("ORIGIN"), F.col("DEST")))
            .withColumn(
                "TIME_OF_DAY_BUCKET",
                F.when(F.col("DEP_HOUR").between(5, 11), F.lit("Morning (05-11)"))
                .when(F.col("DEP_HOUR").between(12, 16), F.lit("Afternoon (12-16)"))
                .when(F.col("DEP_HOUR").between(17, 21), F.lit("Evening Peak (17-21)"))
                .otherwise(F.lit("Night (22-04)")),
            )
            .withColumn(
                "TOTAL_CAUSE_DELAY",
                F.col("CARRIER_DELAY")
                + F.col("WEATHER_DELAY")
                + F.col("NAS_DELAY")
                + F.col("SECURITY_DELAY")
                + F.col("LATE_AIRCRAFT_DELAY"),
            )
        )

        silver_path = str(self.config.silver_dir / "flights_silver.parquet")
        silver_df.write.mode("overwrite").parquet(silver_path)
        silver_reloaded = self.spark.read.parquet(silver_path)

        return silver_reloaded, quarantine_df, dq_summary_pdf

    def build_gold_marts(
        self,
        silver_df: DataFrame,
        airlines_pdf: pd.DataFrame,
        airports_pdf: pd.DataFrame,
    ) -> Dict[str, pd.DataFrame]:
        """
        Формує 6 аналітичних вітрин шару Gold у форматі Parquet та синхронізує їх із СУБД PostgreSQL.
        """
        silver_df.createOrReplaceTempView("v_flights_silver")
        marts: Dict[str, pd.DataFrame] = {}

        # Вітрина 1: gold_airline_punctuality (KPI пунктуальності та причини затримок за авіакомпаніями)
        airline_gold_sdf = self.spark.sql(
            """
            SELECT
                OP_CARRIER AS op_carrier,
                FIRST(AIRLINE_NAME) AS airline_name,
                COUNT(*) AS total_flights,
                SUM(CASE WHEN CANCELLED = 0 THEN 1 ELSE 0 END) AS completed_flights,
                SUM(CANCELLED) AS cancelled_flights,
                SUM(IS_DELAYED_15) AS delayed_flights_15m,
                SUM(IS_SEVERE_DELAY_60) AS severe_delayed_60m,
                ROUND((1.0 - AVG(IS_DELAYED_15)) * 100.0, 2) AS otp_15_pct,
                ROUND(AVG(CANCELLED) * 100.0, 2) AS cancellation_rate_pct,
                ROUND(AVG(DEP_DELAY), 2) AS avg_dep_delay,
                ROUND(AVG(ARR_DELAY), 2) AS avg_arr_delay,
                ROUND(PERCENTILE_APPROX(ARR_DELAY, 0.5), 2) AS median_arr_delay,
                ROUND(PERCENTILE_APPROX(ARR_DELAY, 0.9), 2) AS p90_arr_delay,
                ROUND(AVG(TAXI_OUT), 2) AS avg_taxi_out,
                ROUND(AVG(CARRIER_DELAY), 2) AS avg_carrier_delay,
                ROUND(AVG(WEATHER_DELAY), 2) AS avg_weather_delay,
                ROUND(AVG(NAS_DELAY), 2) AS avg_nas_delay,
                ROUND(AVG(SECURITY_DELAY), 2) AS avg_security_delay,
                ROUND(AVG(LATE_AIRCRAFT_DELAY), 2) AS avg_late_aircraft_delay,
                ROUND(SUM(GREATEST(ARR_DELAY, 0.0)), 1) AS total_delay_minutes
            FROM v_flights_silver
            GROUP BY OP_CARRIER
            ORDER BY otp_15_pct DESC
            """
        )
        airline_gold_sdf.write.mode("overwrite").parquet(str(self.config.gold_dir / "gold_airline_punctuality.parquet"))
        marts["gold_airline_punctuality"] = airline_gold_sdf.toPandas()

        # Вітрина 2: gold_airport_performance (просторовий аналіз хабів та індекс вузьких місць)
        airport_gold_sdf = self.spark.sql(
            """
            SELECT
                ORIGIN AS origin,
                FIRST(ORIGIN_AIRPORT_NAME) AS airport_name,
                FIRST(ORIGIN_CITY) AS city,
                FIRST(ORIGIN_STATE) AS state,
                FIRST(ORIGIN_LAT) AS latitude,
                FIRST(ORIGIN_LON) AS longitude,
                FIRST(ORIGIN_HUB_TIER) AS hub_tier,
                COUNT(*) AS total_departures,
                SUM(IS_DELAYED_15) AS delayed_departures,
                ROUND((1.0 - AVG(IS_DELAYED_15)) * 100.0, 2) AS otp_departure_pct,
                ROUND(AVG(DEP_DELAY), 2) AS avg_dep_delay,
                ROUND(AVG(ARR_DELAY), 2) AS avg_arr_delay,
                ROUND(AVG(TAXI_OUT), 2) AS avg_taxi_out,
                ROUND(AVG(TAXI_IN), 2) AS avg_taxi_in,
                ROUND(SUM(WEATHER_DELAY) / GREATEST(SUM(TOTAL_CAUSE_DELAY), 1.0) * 100.0, 2) AS weather_delay_share_pct,
                ROUND(SUM(NAS_DELAY) / GREATEST(SUM(TOTAL_CAUSE_DELAY), 1.0) * 100.0, 2) AS nas_delay_share_pct,
                ROUND(SUM(LATE_AIRCRAFT_DELAY) / GREATEST(SUM(TOTAL_CAUSE_DELAY), 1.0) * 100.0, 2) AS late_aircraft_share_pct,
                ROUND(AVG(WIND_SPEED_KTS), 2) AS avg_wind_speed_kts,
                ROUND(AVG(VISIBILITY_MILES), 2) AS avg_visibility_miles,
                ROUND(AVG(TAXI_OUT) * 0.45 + AVG(GREATEST(DEP_DELAY, 0.0)) * 0.55, 2) AS bottleneck_index
            FROM v_flights_silver
            GROUP BY ORIGIN
            ORDER BY total_departures DESC
            """
        )
        airport_gold_sdf.write.mode("overwrite").parquet(str(self.config.gold_dir / "gold_airport_performance.parquet"))
        marts["gold_airport_performance"] = airport_gold_sdf.toPandas()

        # Вітрина 3: gold_temporal_dynamics (місяць, день тижня, година вильоту)
        temporal_gold_sdf = self.spark.sql(
            """
            SELECT
                MONTH AS month,
                DAY_OF_WEEK AS day_of_week,
                DEP_HOUR AS dep_hour,
                COUNT(*) AS total_flights,
                SUM(IS_DELAYED_15) AS delayed_flights,
                ROUND((1.0 - AVG(IS_DELAYED_15)) * 100.0, 2) AS otp_15_pct,
                ROUND(AVG(DEP_DELAY), 2) AS avg_dep_delay,
                ROUND(AVG(ARR_DELAY), 2) AS avg_arr_delay,
                ROUND(AVG(LATE_AIRCRAFT_DELAY), 2) AS avg_late_aircraft_delay,
                ROUND(AVG(WEATHER_DELAY), 2) AS avg_weather_delay,
                ROUND(AVG(NAS_DELAY), 2) AS avg_nas_delay
            FROM v_flights_silver
            GROUP BY MONTH, DAY_OF_WEEK, DEP_HOUR
            ORDER BY MONTH, DAY_OF_WEEK, DEP_HOUR
            """
        )
        temporal_gold_sdf.write.mode("overwrite").parquet(str(self.config.gold_dir / "gold_temporal_dynamics.parquet"))
        marts["gold_temporal_dynamics"] = temporal_gold_sdf.toPandas()

        # Вітрина 4: gold_delay_factors_summary (структура 5 канонічних причин затримок + кореляція)
        factors_meta = [
            ("LATE_AIRCRAFT_DELAY", "Пізнє прибуття літака з попереднього рейсу (Каскадний ефект)"),
            ("CARRIER_DELAY", "Внутрішні операційні причини авіакомпанії (екіпаж, ТО, борт)"),
            ("NAS_DELAY", "Національна система повітряного простору (перевантаження, УПР)"),
            ("WEATHER_DELAY", "Екстремальні метеорологічні умови (грози, снігопад, шторм)"),
            ("SECURITY_DELAY", "Заходи авіаційної безпеки та контроль у терміналі"),
        ]

        # Обчислення сум та кореляцій одним проходом у Spark
        agg_exprs = [F.sum("TOTAL_CAUSE_DELAY").alias("grand_total_delay")]
        for col_name, _ in factors_meta:
            agg_exprs.extend(
                [
                    F.sum(F.when(F.col(col_name) > 0, 1).otherwise(0)).alias(f"{col_name}_cnt"),
                    F.sum(F.col(col_name)).alias(f"{col_name}_sum"),
                    F.avg(F.when(F.col(col_name) > 0, F.col(col_name))).alias(f"{col_name}_avg_pos"),
                    F.max(F.col(col_name)).alias(f"{col_name}_max"),
                    F.corr("ARR_DELAY", col_name).alias(f"{col_name}_corr"),
                ]
            )

        factor_stats_row = silver_df.agg(*agg_exprs).collect()[0].asDict()
        grand_total = max(float(factor_stats_row.get("grand_total_delay") or 1.0), 1.0)

        factor_rows = []
        for col_name, name_ua in factors_meta:
            f_sum = float(factor_stats_row.get(f"{col_name}_sum") or 0.0)
            factor_rows.append(
                {
                    "factor_code": col_name,
                    "factor_name_ua": name_ua,
                    "affected_flights": int(factor_stats_row.get(f"{col_name}_cnt") or 0),
                    "total_delay_minutes": round(f_sum, 1),
                    "share_of_total_delay_pct": round((f_sum / grand_total) * 100.0, 2),
                    "avg_minutes_when_present": round(float(factor_stats_row.get(f"{col_name}_avg_pos") or 0.0), 2),
                    "max_minutes_observed": round(float(factor_stats_row.get(f"{col_name}_max") or 0.0), 1),
                    "correlation_with_arr_delay": round(float(factor_stats_row.get(f"{col_name}_corr") or 0.0), 4),
                }
            )
        factors_pdf = pd.DataFrame(factor_rows).sort_values("total_delay_minutes", ascending=False).reset_index(drop=True)
        self.spark.createDataFrame(factors_pdf).write.mode("overwrite").parquet(
            str(self.config.gold_dir / "gold_delay_factors_summary.parquet")
        )
        marts["gold_delay_factors_summary"] = factors_pdf

        # Вітрина 5: gold_route_cascades (Топ-120 завантажених маршрутів із геокоординатами та каскадністю)
        routes_gold_sdf = self.spark.sql(
            """
            SELECT
                ROUTE_ID AS route_id,
                ORIGIN AS origin,
                DEST AS dest,
                FIRST(ORIGIN_CITY) AS origin_city,
                FIRST(DEST_CITY) AS dest_city,
                FIRST(ORIGIN_LAT) AS origin_lat,
                FIRST(ORIGIN_LON) AS origin_lon,
                FIRST(DEST_LAT) AS dest_lat,
                FIRST(DEST_LON) AS dest_lon,
                ROUND(AVG(DISTANCE), 1) AS distance_miles,
                COUNT(*) AS flights_count,
                ROUND((1.0 - AVG(IS_DELAYED_15)) * 100.0, 2) AS otp_15_pct,
                ROUND(AVG(ARR_DELAY), 2) AS avg_arr_delay,
                ROUND(AVG(AIR_TIME), 2) AS avg_air_time,
                ROUND(SUM(LATE_AIRCRAFT_DELAY) / GREATEST(SUM(TOTAL_CAUSE_DELAY), 1.0) * 100.0, 2) AS cascade_ratio_pct
            FROM v_flights_silver
            GROUP BY ROUTE_ID, ORIGIN, DEST
            HAVING COUNT(*) >= 25
            ORDER BY flights_count DESC
            LIMIT 120
            """
        )
        routes_gold_sdf.write.mode("overwrite").parquet(str(self.config.gold_dir / "gold_route_cascades.parquet"))
        marts["gold_route_cascades"] = routes_gold_sdf.toPandas()

        # Вітрина 6: gold_weather_impact (Аналіз впливу погодних режимів на затримки)
        weather_gold_sdf = self.spark.sql(
            """
            SELECT
                WEATHER_CONDITION AS weather_condition,
                COUNT(*) AS flights_count,
                ROUND((1.0 - AVG(IS_DELAYED_15)) * 100.0, 2) AS otp_15_pct,
                ROUND(AVG(DEP_DELAY), 2) AS avg_dep_delay,
                ROUND(AVG(ARR_DELAY), 2) AS avg_arr_delay,
                ROUND(AVG(WEATHER_DELAY), 2) AS avg_weather_delay,
                ROUND(AVG(NAS_DELAY), 2) AS avg_nas_delay,
                ROUND(AVG(WIND_SPEED_KTS), 2) AS avg_wind_speed,
                ROUND(AVG(PRECIP_MM), 2) AS avg_precip_mm
            FROM v_flights_silver
            GROUP BY WEATHER_CONDITION
            ORDER BY avg_arr_delay DESC
            """
        )
        weather_gold_sdf.write.mode("overwrite").parquet(str(self.config.gold_dir / "gold_weather_impact.parquet"))
        marts["gold_weather_impact"] = weather_gold_sdf.toPandas()

        # Синхронізація довідників та всіх 6 вітрин Gold у PostgreSQL
        if self.db.is_available():
            self.db.replace_table_from_df(airlines_pdf, "dim_airlines")
            self.db.replace_table_from_df(airports_pdf, "dim_airports")
            for table_name, pdf in marts.items():
                self.db.replace_table_from_df(pdf, table_name)

        return marts
