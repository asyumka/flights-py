"""
Ядро перевірки якості даних (Data Quality Engine) на базі PySpark.

Виконує багаторівневу валідацію вхідного потоку/пакету авіарейсів:
1. Перевірка повноти (NULL у ключових полях: FL_DATE, OP_CARRIER, ORIGIN, DEST).
2. Перевірка довідникової цілісності (відповідність кодів ORIGIN та DEST офіційному реєстру IATA).
3. Перевірка фізичної коректності таймінгів (ACTUAL_ELAPSED_TIME > 0, AIR_TIME > 0).
4. Перевірка географічних меж маршруту (20 <= DISTANCE <= 6000 миль, ORIGIN != DEST).
5. Виявлення статистичних та телеметричних аномалій (|ARR_DELAY| > 1440 хв).
6. Дедуплікація записів за композитним ключем рейсу (Window RowNumber).

Валідні записи переходять у шар Silver, а відхилені маркуються кодом помилки DQ_ERROR_REASON
та зберігаються у карантинне сховище (Quarantine Layer у Parquet + зведення у PostgreSQL).
"""
from __future__ import annotations
import json
from typing import Dict, List, Tuple

import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.utils.config_manager import ConfigManager

ERROR_DESCRIPTIONS_UA: Dict[str, Tuple[str, str]] = {
    "ERR_NULL_KEY": (
        "Відсутній обов'язковий ідентифікатор рейсу (NULL у FL_DATE, OP_CARRIER, ORIGIN або DEST)",
        "CRITICAL",
    ),
    "ERR_INVALID_IATA": (
        "Неіснуючий або некоректний код аеропорту ІАТА (відсутній у довіднику dim_airports)",
        "HIGH",
    ),
    "ERR_NEGATIVE_DURATION": (
        "Фізично неможлива тривалість польоту або руління (ACTUAL_ELAPSED_TIME <= 0 або AIR_TIME <= 0)",
        "CRITICAL",
    ),
    "ERR_INVALID_DISTANCE": (
        "Аномальна ортодромічна відстань маршруту (DISTANCE < 20 або > 6000 миль, або ORIGIN == DEST)",
        "HIGH",
    ),
    "ERR_EXTREME_DELAY_OUTLIER": (
        "Екстремальний телеметричний викид затримки прибуття (|ARR_DELAY| > 1440 хв / 24 год)",
        "MEDIUM",
    ),
    "ERR_DUPLICATE_FLIGHT": (
        "Дублікат запису рейсу за ключем (FL_DATE, OP_CARRIER, FL_NUM, ORIGIN, DEST, CRS_DEP_TIME)",
        "MEDIUM",
    ),
}


class DataQualityEngine:
    """Клас багатофакторної валідації якості даних та ізоляції аномалій у PySpark."""

    def __init__(self, config: ConfigManager | None = None) -> None:
        self.config = config or ConfigManager()
        self.rules = self.config.dq_rules

    def validate_and_split(
        self,
        raw_df: DataFrame,
        valid_iata_codes: List[str],
    ) -> Tuple[DataFrame, DataFrame, pd.DataFrame]:
        """
        Розділяє вхідний Spark DataFrame на:
        - valid_df (для шару Silver)
        - quarantine_df (відхилені записи з колонкою DQ_ERROR_REASON для шару Quarantine)
        - summary_pdf (Pandas DataFrame зі статистикою причин відхилення)
        """
        max_delay = float(self.rules.get("extreme_outlier_delay_minutes", 1440.0))
        min_dist = float(self.rules.get("min_distance_miles", 20.0))
        max_dist = float(self.rules.get("max_distance_miles", 6000.0))

        # 1. Визначення дублікатів за композитним ключем рейсу
        dedup_cols = ["FL_DATE", "OP_CARRIER", "FL_NUM", "ORIGIN", "DEST", "CRS_DEP_TIME"]
        window_spec = Window.partitionBy(*dedup_cols).orderBy(F.lit(1))
        df_flagged = raw_df.withColumn("_row_num", F.row_number().over(window_spec))

        # 2. Каскадне правило класифікації помилок якості даних
        error_expr = (
            F.when(
                F.col("FL_DATE").isNull()
                | F.col("OP_CARRIER").isNull()
                | F.col("ORIGIN").isNull()
                | F.col("DEST").isNull(),
                F.lit("ERR_NULL_KEY"),
            )
            .when(
                (~F.col("ORIGIN").isin(valid_iata_codes)) | (~F.col("DEST").isin(valid_iata_codes)),
                F.lit("ERR_INVALID_IATA"),
            )
            .when(
                (F.col("ACTUAL_ELAPSED_TIME").isNull())
                | (F.col("ACTUAL_ELAPSED_TIME") <= 0.0)
                | (F.col("AIR_TIME") <= 0.0),
                F.lit("ERR_NEGATIVE_DURATION"),
            )
            .when(
                (F.col("DISTANCE").isNull())
                | (F.col("DISTANCE") < min_dist)
                | (F.col("DISTANCE") > max_dist)
                | (F.col("ORIGIN") == F.col("DEST")),
                F.lit("ERR_INVALID_DISTANCE"),
            )
            .when(
                (F.col("ARR_DELAY").isNull()) | (F.abs(F.col("ARR_DELAY")) > max_delay),
                F.lit("ERR_EXTREME_DELAY_OUTLIER"),
            )
            .when(F.col("_row_num") > 1, F.lit("ERR_DUPLICATE_FLIGHT"))
            .otherwise(F.lit("VALID"))
        )

        df_evaluated = df_flagged.withColumn("DQ_ERROR_REASON", error_expr).drop("_row_num")

        valid_df = df_evaluated.filter(F.col("DQ_ERROR_REASON") == "VALID").drop("DQ_ERROR_REASON")
        quarantine_df = df_evaluated.filter(F.col("DQ_ERROR_REASON") != "VALID")

        # 3. Агрегація статистики карантинного шару
        counts_pdf = (
            quarantine_df.groupBy("DQ_ERROR_REASON")
            .agg(F.count("*").alias("rejected_count"))
            .toPandas()
        )

        total_rejected = int(counts_pdf["rejected_count"].sum()) if not counts_pdf.empty else 0
        summary_rows = []
        for _, row in counts_pdf.iterrows():
            code = str(row["DQ_ERROR_REASON"])
            cnt = int(row["rejected_count"])
            desc_ua, severity = ERROR_DESCRIPTIONS_UA.get(code, ("Невідома помилка якості даних", "LOW"))
            share = round((cnt / max(total_rejected, 1)) * 100.0, 2)
            summary_rows.append(
                {
                    "error_code": code,
                    "error_description_ua": desc_ua,
                    "severity": severity,
                    "rejected_count": cnt,
                    "share_of_rejected_pct": share,
                    "sample_record_json": json.dumps({"rule": code, "rejected_rows": cnt}, ensure_ascii=False),
                }
            )

        summary_pdf = pd.DataFrame(summary_rows)
        if not summary_pdf.empty:
            summary_pdf = summary_pdf.sort_values("rejected_count", ascending=False).reset_index(drop=True)

        return valid_df, quarantine_df, summary_pdf
