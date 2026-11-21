"""
Регресійне моделювання тривалості затримки прибуття авіарейсу (ARR_DELAY у хвилинах)
та кількісна оцінка важливості чинників затримок (Feature Importance) у PySpark MLlib.

Порівнюються дві моделі:
1. LinearRegression (базова лінійна регресія з L1/L2 регуляризацією ElasticNet як еталон/baseline).
2. RandomForestRegressor (ансамбль регресійних дерев рішень для нелінійних взаємодій та оцінки ваги ознак).

Метрики якості:
- RMSE (Root Mean Squared Error, хв)
- MAE (Mean Absolute Error, хв)
- R^2 (Коефіцієнт детермінації)
"""
from __future__ import annotations
import json
import time
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.config_manager import ConfigManager
from src.utils.db_manager import DatabaseManager

# Ознаки для прогнозування тривалості затримки прибуття (ARR_DELAY) та їхній опис українською
NUMERIC_FEATURES: List[Tuple[str, str, str]] = [
    ("DEP_DELAY", "Відхилення часу вильоту від розкладу (DEP_DELAY, хв)", "Операційні таймінги"),
    ("PREV_AIRCRAFT_DELAY", "Каскадна затримка борту з попереднього рейсу (хв)", "Каскадний фактор"),
    ("TAXI_OUT", "Тривалість руління на зліт в аеропорту вильоту (TAXI_OUT, хв)", "Інфраструктура аеропорту"),
    ("WEATHER_SEVERITY_IDX", "Індекс суворості метеоумов у вузлі вильоту (0..10)", "Метеорологічний фактор"),
    ("WIND_SPEED_KTS", "Швидкість приземного вітру в аеропорту вильоту (вузлів)", "Метеорологічний фактор"),
    ("VISIBILITY_MILES", "Горизонтальна видимість на злітній смузі (миль)", "Метеорологічний фактор"),
    ("PRECIP_MM", "Інтенсивність опадів за годину вильоту (мм)", "Метеорологічний фактор"),
    ("DEP_HOUR", "Година вильоту за розкладом (0..23, добовий пік)", "Часовий розклад"),
    ("DISTANCE", "Ортодромічна довжина маршруту (миль)", "Геометрія маршруту"),
    ("CRS_ELAPSED_TIME", "Планова тривалість рейсу за розкладом (хв)", "Часовий розклад"),
    ("MONTH", "Місяць виконання рейсу (1..12, сезонність)", "Часовий розклад"),
    ("DAY_OF_WEEK", "День тижня (1..7)", "Часовий розклад"),
]

CATEGORICAL_FEATURES: List[Tuple[str, str, str]] = [
    ("OP_CARRIER_IDX", "Код авіакомпанії-оператора (OP_CARRIER)", "Перевізник"),
    ("ORIGIN_HUB_IDX", "Категорія завантаженості аеропорту-хабу (HUB_TIER)", "Інфраструктура аеропорту"),
]


class FlightDelayRegressor:
    """Модуль навчання, оцінювання та інтерпретації регресійних моделей затримок у Spark MLlib."""

    def __init__(
        self,
        spark: SparkSession,
        config: ConfigManager | None = None,
        db: DatabaseManager | None = None,
    ) -> None:
        self.spark = spark
        self.config = config or ConfigManager()
        self.db = db or DatabaseManager(self.config)

    def train_and_evaluate(self, silver_df: DataFrame) -> Dict[str, Any]:
        """
        Навчає LinearRegression та RandomForestRegressor на очищених даних Silver
        (для виконаних рейсів CANCELLED = 0), обчислює RMSE, MAE, R2, будує рейтинг
        важливості чинників (Feature Importance) і зберігає результати в Gold та PostgreSQL.
        """
        ml_cfg = self.config.ml_params
        seed = int(ml_cfg.get("random_seed", 42))
        train_ratio = float(ml_cfg.get("train_ratio", 0.8))

        # Вибірка виконаних рейсів без пропусків у цільовій змінній ARR_DELAY
        active_df = silver_df.filter((F.col("CANCELLED") == 0) & F.col("ARR_DELAY").isNotNull())

        # Для швидкого та стабільного навчання на локальному кластері обмежуємо навчальну вибірку до 80 000 записів
        total_active = active_df.count()
        if total_active > 80_000:
            fraction = min(1.0, 80_000.0 / total_active)
            modeling_df = active_df.sample(withReplacement=False, fraction=fraction, seed=seed)
        else:
            modeling_df = active_df

        # Індексація категоріальних ознак
        carrier_indexer = StringIndexer(
            inputCol="OP_CARRIER", outputCol="OP_CARRIER_IDX", handleInvalid="keep"
        )
        hub_indexer = StringIndexer(
            inputCol="ORIGIN_HUB_TIER", outputCol="ORIGIN_HUB_IDX", handleInvalid="keep"
        )

        feature_cols = [f[0] for f in NUMERIC_FEATURES] + [c[0] for c in CATEGORICAL_FEATURES]
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features",
            handleInvalid="skip",
        )

        prep_pipeline = Pipeline(stages=[carrier_indexer, hub_indexer, assembler])
        prep_model = prep_pipeline.fit(modeling_df)
        assembled_df = prep_model.transform(modeling_df).select(
            "FL_DATE",
            "OP_CARRIER",
            "ORIGIN",
            "DEST",
            "DEP_HOUR",
            "DISTANCE",
            "DEP_DELAY",
            "PREV_AIRCRAFT_DELAY",
            "TAXI_OUT",
            "WEATHER_SEVERITY_IDX",
            "ARR_DELAY",
            "features",
        )

        train_df, test_df = assembled_df.randomSplit([train_ratio, 1.0 - train_ratio], seed=seed)
        train_df.cache()
        test_df.cache()

        train_rows = train_df.count()
        test_rows = test_df.count()

        # Оцінювачі якості регресії
        eval_rmse = RegressionEvaluator(labelCol="ARR_DELAY", predictionCol="prediction", metricName="rmse")
        eval_mae = RegressionEvaluator(labelCol="ARR_DELAY", predictionCol="prediction", metricName="mae")
        eval_r2 = RegressionEvaluator(labelCol="ARR_DELAY", predictionCol="prediction", metricName="r2")

        # ---------------------------------------------------------------------
        # 1. Навчання базової моделі: LinearRegression (ElasticNet)
        # ---------------------------------------------------------------------
        t_lr0 = time.perf_counter()
        lr = LinearRegression(
            featuresCol="features",
            labelCol="ARR_DELAY",
            maxIter=50,
            regParam=float(ml_cfg.get("lr_reg_param", 0.05)),
            elasticNetParam=float(ml_cfg.get("lr_elastic_net_param", 0.2)),
        )
        lr_model = lr.fit(train_df)
        lr_preds = lr_model.transform(test_df)
        lr_rmse = float(eval_rmse.evaluate(lr_preds))
        lr_mae = float(eval_mae.evaluate(lr_preds))
        lr_r2 = float(eval_r2.evaluate(lr_preds))
        lr_time = round(time.perf_counter() - t_lr0, 3)

        # ---------------------------------------------------------------------
        # 2. Навчання основної нелінійної моделі: RandomForestRegressor
        # ---------------------------------------------------------------------
        t_rf0 = time.perf_counter()
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol="ARR_DELAY",
            numTrees=int(ml_cfg.get("rf_num_trees", 35)),
            maxDepth=int(ml_cfg.get("rf_max_depth", 8)),
            maxBins=32,
            seed=seed,
        )
        rf_model = rf.fit(train_df)
        rf_preds = rf_model.transform(test_df)
        rf_rmse = float(eval_rmse.evaluate(rf_preds))
        rf_mae = float(eval_mae.evaluate(rf_preds))
        rf_r2 = float(eval_r2.evaluate(rf_preds))
        rf_time = round(time.perf_counter() - t_rf0, 3)

        # ---------------------------------------------------------------------
        # 3. Таблиця порівняння метрик моделей
        # ---------------------------------------------------------------------
        metrics_rows = [
            {
                "model_id": "spark_rf_regressor",
                "model_name": "Random Forest Regressor (Spark MLlib)",
                "target_variable": "ARR_DELAY (хв)",
                "train_rows": train_rows,
                "test_rows": test_rows,
                "rmse": round(rf_rmse, 3),
                "mae": round(rf_mae, 3),
                "r2_score": round(rf_r2, 4),
                "training_time_sec": rf_time,
            },
            {
                "model_id": "spark_lr_baseline",
                "model_name": "Linear Regression ElasticNet (Baseline)",
                "target_variable": "ARR_DELAY (хв)",
                "train_rows": train_rows,
                "test_rows": test_rows,
                "rmse": round(lr_rmse, 3),
                "mae": round(lr_mae, 3),
                "r2_score": round(lr_r2, 4),
                "training_time_sec": lr_time,
            },
        ]
        metrics_pdf = pd.DataFrame(metrics_rows)

        # ---------------------------------------------------------------------
        # 4. Вилучення важливості чинників (Feature Importance) з Random Forest
        # ---------------------------------------------------------------------
        importances = rf_model.featureImportances.toArray()
        all_feature_defs = NUMERIC_FEATURES + CATEGORICAL_FEATURES
        fi_rows = []
        for idx, (f_code, f_name_ua, f_cat) in enumerate(all_feature_defs):
            weight = float(importances[idx]) if idx < len(importances) else 0.0
            fi_rows.append(
                {
                    "feature_code": f_code.replace("_IDX", ""),
                    "feature_name_ua": f_name_ua,
                    "importance_weight": round(weight, 5),
                    "importance_pct": round(weight * 100.0, 2),
                    "category": f_cat,
                }
            )
        fi_pdf = pd.DataFrame(fi_rows).sort_values("importance_weight", ascending=False).reset_index(drop=True)

        # ---------------------------------------------------------------------
        # 5. Формування вибірки прогнозів на тестовій множині для візуалізації в BI
        # ---------------------------------------------------------------------
        combined_preds = (
            lr_preds.withColumnRenamed("prediction", "predicted_lr_delay")
            .select(
                "FL_DATE",
                "OP_CARRIER",
                "ORIGIN",
                "DEST",
                "DEP_HOUR",
                "DISTANCE",
                "DEP_DELAY",
                "PREV_AIRCRAFT_DELAY",
                "TAXI_OUT",
                "WEATHER_SEVERITY_IDX",
                "ARR_DELAY",
                "features",
                "predicted_lr_delay",
            )
        )
        combined_preds = rf_model.transform(combined_preds).withColumnRenamed("prediction", "predicted_rf_delay")
        sample_preds_pdf = (
            combined_preds.select(
                F.col("FL_DATE").alias("fl_date"),
                F.col("OP_CARRIER").alias("op_carrier"),
                F.col("ORIGIN").alias("origin"),
                F.col("DEST").alias("dest"),
                F.col("DEP_HOUR").alias("dep_hour"),
                F.col("DISTANCE").alias("distance"),
                F.col("DEP_DELAY").alias("dep_delay"),
                F.col("PREV_AIRCRAFT_DELAY").alias("prev_aircraft_delay"),
                F.col("TAXI_OUT").alias("taxi_out"),
                F.col("WEATHER_SEVERITY_IDX").alias("weather_severity_idx"),
                F.round(F.col("ARR_DELAY"), 1).alias("actual_arr_delay"),
                F.round(F.col("predicted_lr_delay"), 1).alias("predicted_lr_delay"),
                F.round(F.col("predicted_rf_delay"), 1).alias("predicted_rf_delay"),
                F.round(F.abs(F.col("ARR_DELAY") - F.col("predicted_rf_delay")), 2).alias("abs_error_rf"),
            )
            .limit(400)
            .toPandas()
        )

        # ---------------------------------------------------------------------
        # 6. Збереження артефактів моделей, коефіцієнтів для швидкого інференсу та запис у Gold/PostgreSQL
        # ---------------------------------------------------------------------
        metrics_pdf.to_parquet(self.config.gold_dir / "ml_model_metrics.parquet", index=False)
        fi_pdf.to_parquet(self.config.gold_dir / "ml_feature_importance.parquet", index=False)
        sample_preds_pdf.to_parquet(self.config.gold_dir / "ml_predictions_sample.parquet", index=False)

        # Зберігаємо JSON-специфікацію навченої моделі для миттєвого онлайн-прогнозування в Kafka Stream та BI-калькуляторі
        lr_coeffs = lr_model.coefficients.toArray().tolist()
        model_artifact = {
            "intercept": float(lr_model.intercept),
            "feature_names": [f[0] for f in all_feature_defs],
            "lr_coefficients": dict(zip([f[0] for f in all_feature_defs], [round(c, 5) for c in lr_coeffs])),
            "rf_feature_importances": dict(zip(fi_pdf["feature_code"], fi_pdf["importance_pct"])),
            "metrics": metrics_rows,
        }
        with open(self.config.models_dir / "delay_regression_artifact.json", "w", encoding="utf-8") as f:
            json.dump(model_artifact, f, indent=2, ensure_ascii=False)

        if self.db.is_available():
            self.db.replace_table_from_df(metrics_pdf, "ml_model_metrics")
            self.db.replace_table_from_df(fi_pdf, "ml_feature_importance")
            self.db.replace_table_from_df(sample_preds_pdf, "ml_predictions_sample")

        train_df.unpersist()
        test_df.unpersist()

        return {
            "metrics": metrics_pdf,
            "feature_importance": fi_pdf,
            "predictions_sample": sample_preds_pdf,
            "artifact_path": str(self.config.models_dir / "delay_regression_artifact.json"),
        }
