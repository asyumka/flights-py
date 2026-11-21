"""
Генератор ілюстративних матеріалів (архітектурні схеми, UML-діаграми та аналітичні графіки)
для пояснювальної записки курсової роботи (Варіант 3: ПАКАПА).
"""
from __future__ import annotations
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch

plt.rcParams["font.family"] = "DejaVu Sans"
plt.rcParams["axes.unicode_minus"] = False

OUT_DIR = Path("d:/ed/course/flights/docs/figures")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def draw_box(ax, x, y, w, h, title, subtitle="", fc="#EBF3FA", ec="#1F4E79", title_color="#0F2C59", fontsize=9.5):
    box = FancyBboxPatch(
        (x, y),
        w,
        h,
        boxstyle="round,pad=0.015,rounding_size=0.02",
        facecolor=fc,
        edgecolor=ec,
        linewidth=1.5,
    )
    ax.add_patch(box)
    if subtitle:
        ax.text(
            x + w / 2,
            y + h * 0.66,
            title,
            ha="center",
            va="center",
            fontsize=fontsize,
            fontweight="bold",
            color=title_color,
        )
        ax.text(
            x + w / 2,
            y + h * 0.30,
            subtitle,
            ha="center",
            va="center",
            fontsize=fontsize - 1.3,
            color="#222222",
        )
    else:
        ax.text(
            x + w / 2,
            y + h / 2,
            title,
            ha="center",
            va="center",
            fontsize=fontsize,
            fontweight="bold",
            color=title_color,
        )


def draw_arrow(ax, x1, y1, x2, y2, label="", color="#1F4E79"):
    ax.annotate(
        "",
        xy=(x2, y2),
        xytext=(x1, y1),
        arrowprops=dict(arrowstyle="-|>,head_length=0.6,head_width=0.35", lw=1.6, color=color),
    )
    if label:
        mx, my = (x1 + x2) / 2, (y1 + y2) / 2
        ax.text(
            mx,
            my + 0.022,
            label,
            ha="center",
            va="bottom",
            fontsize=8,
            color="#111111",
            bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.85),
        )


def fig_1_1_lambda_medallion_architecture():
    fig, ax = plt.subplots(figsize=(12, 6.6), dpi=220)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    # Групові контейнери
    groups = [
        (0.02, 0.12, 0.20, 0.80, "ДЖЕРЕЛА ДАНИХ\n(Data Sources)", "#F4F6F9", "#5B6D8A"),
        (0.25, 0.12, 0.48, 0.80, "КОНВЕЄР ОБРОБКИ ТА СХОВИЩЕ\n(Lambda + Medallion Lakehouse)", "#F7FAFC", "#1F4E79"),
        (0.76, 0.12, 0.22, 0.80, "СЕРВІСНИЙ ШАР ТА BI\n(Serving & Presentation)", "#F4F9F5", "#2E6F40"),
    ]
    for gx, gy, gw, gh, gtitle, gfc, gec in groups:
        rect = FancyBboxPatch(
            (gx, gy), gw, gh, boxstyle="round,pad=0.01,rounding_size=0.02", facecolor=gfc, edgecolor=gec, lw=1.4, ls="--"
        )
        ax.add_patch(rect)
        ax.text(gx + gw / 2, gy + gh - 0.045, gtitle, ha="center", va="center", fontsize=9.5, fontweight="bold", color=gec)

    # Блоки джерел
    draw_box(ax, 0.035, 0.64, 0.17, 0.17, "US DOT BTS Dataset", "Історичні рейси (CSV)\n200 700 записів, 31 поле", "#E3F2FD", "#1565C0")
    draw_box(ax, 0.035, 0.42, 0.17, 0.16, "Довідники IATA & Метео", "dim_airlines (24 світові)\ndim_airports (47 хабів)", "#E8EAF6", "#283593")
    draw_box(ax, 0.035, 0.18, 0.17, 0.18, "OpenSky Network API", "Глобальна ADS-B телеметрія\nкоординати, швидкість, курс", "#E0F2F1", "#00695C")

    # Пакетний та медальйонний шар
    draw_box(ax, 0.27, 0.67, 0.19, 0.14, "Bronze Layer (Parquet)", "Сирі типізовані дані\nSnappy Compression", "#FFF3E0", "#EF6C00")
    draw_box(ax, 0.27, 0.46, 0.19, 0.15, "PySpark Data Quality", "6 правил валідації\nWindow Deduplication", "#FCE4EC", "#AD1457")
    draw_box(ax, 0.27, 0.28, 0.19, 0.12, "Quarantine Layer", "Ізоляція 4 200 (2,09%)\nаномальних записів + DLQ", "#FFEBEE", "#C62828")

    draw_box(ax, 0.51, 0.67, 0.20, 0.14, "Silver Layer (Parquet)", "196 500 валідних рейсів\nBroadcast Join + Ознаки", "#E8F5E9", "#2E7D32")
    draw_box(ax, 0.51, 0.46, 0.20, 0.15, "Spark MLlib Regression", "Random Forest & Linear\nПрогноз ARR_DELAY (R²=0,940)", "#F3E5F5", "#6A1B9A")

    # Потоковий шар Kafka
    draw_box(ax, 0.27, 0.14, 0.44, 0.10, "Apache Kafka (KRaft) + Stream Processor", "Топіки: flights.live.stream (3 парт.) | flights.dlq | Онлайн ML-скоринг", "#E0F7FA", "#00838F")

    # Сервісний шар
    draw_box(ax, 0.78, 0.56, 0.18, 0.25, "Gold Layer &\nPostgreSQL 16 DWH", "6 аналітичних вітрин\nТаблиці ML, DQ, Stream\n(15 таблиць у схемі)", "#FFFDE7", "#F9A825")
    draw_box(ax, 0.78, 0.20, 0.18, 0.28, "Інтерактивний\nBI-Дашборд\n(Streamlit + Plotly)", "6 аналітичних вкладок:\nOTP, Причини, Глоб. карта,\nML-калькулятор, Live, DQ", "#E8F5E9", "#1B5E20")

    # Стрілки
    draw_arrow(ax, 0.205, 0.73, 0.27, 0.73, "CSV")
    draw_arrow(ax, 0.365, 0.67, 0.365, 0.61, "")
    draw_arrow(ax, 0.365, 0.46, 0.365, 0.40, "Відхилені")
    draw_arrow(ax, 0.46, 0.54, 0.51, 0.72, "Валідні")
    draw_arrow(ax, 0.205, 0.50, 0.51, 0.74, "Broadcast")
    draw_arrow(ax, 0.61, 0.67, 0.61, 0.61, "")
    draw_arrow(ax, 0.71, 0.74, 0.78, 0.72, "Spark SQL")
    draw_arrow(ax, 0.71, 0.53, 0.78, 0.64, "Метрики")
    draw_arrow(ax, 0.205, 0.24, 0.27, 0.19, "JSON")
    draw_arrow(ax, 0.71, 0.19, 0.78, 0.58, "Upsert")
    draw_arrow(ax, 0.87, 0.56, 0.87, 0.48, "SQL / Parquet")

    ax.text(
        0.5,
        0.96,
        "Узагальнена архітектурна схема програмно-аналітичного комплексу «ПАКАПА» (Lambda + Medallion)",
        ha="center",
        va="center",
        fontsize=11.5,
        fontweight="bold",
        color="#0F2C59",
    )
    fig.tight_layout()
    fig.savefig(OUT_DIR / "fig_1_1_architecture.png", dpi=220)
    plt.close(fig)


def fig_1_2_er_data_model():
    fig, ax = plt.subplots(figsize=(11.5, 6.2), dpi=220)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    draw_box(
        ax, 0.36, 0.32, 0.28, 0.46,
        "ФАКТ: v_flights_silver (Рейси)",
        "PK: (FL_DATE, OP_CARRIER, FL_NUM, ORIGIN, DEST)\nFK: OP_CARRIER -> dim_airlines\nFK: ORIGIN, DEST -> dim_airports\nТаймінги: DEP_DELAY, TAXI_OUT, AIR_TIME, ARR_DELAY\nПричини: CARRIER, WEATHER, NAS, SECURITY, LATE_AIRCRAFT\nМетео: TEMP_C, WIND_SPEED_KTS, VISIBILITY, PRECIP_MM\nОзнаки: IS_DELAYED_15, IS_SEVERE_DELAY_60, ROUTE_ID",
        "#E3F2FD", "#0D47A1", fontsize=9.0
    )

    draw_box(
        ax, 0.03, 0.60, 0.26, 0.26,
        "ВИМІР: dim_airlines",
        "PK: iata_code (VARCHAR)\nairline_name, alliance\nfleet_type, base_otp_rating\n(24 світові авіакомпанії)",
        "#E8F5E9", "#1B5E20", fontsize=9.0
    )

    draw_box(
        ax, 0.03, 0.20, 0.26, 0.28,
        "ВИМІР: dim_airports",
        "PK: iata_code (VARCHAR)\nairport_name, city, state (регіон)\nlatitude, longitude (WGS-84)\nhub_tier, climate_zone\n(47 глобальних хабів світу)",
        "#E8F5E9", "#1B5E20", fontsize=9.0
    )

    draw_box(
        ax, 0.71, 0.64, 0.26, 0.24,
        "ЗОЛОТІ ВІТРИНИ (Gold Marts)",
        "1. gold_airline_punctuality (24)\n2. gold_airport_performance (47)\n3. gold_temporal_dynamics (2016)\n4. gold_delay_factors_summary (5)\n5. gold_route_cascades (120)\n6. gold_weather_impact (6)",
        "#FFF8E1", "#E65100", fontsize=8.8
    )

    draw_box(
        ax, 0.71, 0.34, 0.26, 0.24,
        "ML ТА КАРАНТИН (Spark ML & DQ)",
        "• ml_model_metrics (RF, LR)\n• ml_feature_importance (14 ознак)\n• ml_predictions_sample (400)\n• dq_quarantine_summary (6 кодів)\n• dq_quarantine_samples (300)",
        "#F3E5F5", "#4A148C", fontsize=8.8
    )

    draw_box(
        ax, 0.71, 0.06, 0.26, 0.22,
        "ПОТОКОВИЙ ТА АУДИТ ШАР",
        "• live_flights_stream (OpenSky/Kafka)\n  PK: event_id (icao24, callsign,\n  lat, lon, predicted_arr_delay_min)\n• pipeline_audit_runs (телеметрія)",
        "#E0F7FA", "#006064", fontsize=8.8
    )

    draw_arrow(ax, 0.29, 0.72, 0.36, 0.64, "1 : N")
    draw_arrow(ax, 0.29, 0.35, 0.36, 0.46, "1 : N (Origin/Dest)")
    draw_arrow(ax, 0.64, 0.65, 0.71, 0.74, "Агрегація")
    draw_arrow(ax, 0.64, 0.50, 0.71, 0.46, "ML / DQ")
    draw_arrow(ax, 0.50, 0.32, 0.71, 0.17, "Онлайн-скоринг")

    ax.text(
        0.5, 0.94,
        "Інформаційна модель даних та схема аналітичного сховища комплексу «ПАКАПА»",
        ha="center", va="center", fontsize=11.5, fontweight="bold", color="#0F2C59"
    )
    fig.tight_layout()
    fig.savefig(OUT_DIR / "fig_1_2_data_model.png", dpi=220)
    plt.close(fig)


def fig_3_1_etl_pipeline_flow():
    fig, ax = plt.subplots(figsize=(12, 5.2), dpi=220)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    steps = [
        (0.02, 0.36, 0.16, 0.42, "Етап 1. Ingestion", "Читання CSV BTS\nПриведення до схеми\nBTS_RAW_SCHEMA\n(200 700 рядків)", "#E3F2FD", "#1565C0"),
        (0.21, 0.36, 0.16, 0.42, "Етап 2. Bronze", "Запис сирого шару\nflights_bronze.parquet\nКодек: Snappy\nЧас: 15,93 с", "#FFF3E0", "#EF6C00"),
        (0.40, 0.36, 0.18, 0.42, "Етап 3. Data Quality", "Window Deduplication\nПеревірка IATA, NULL,\nтаймінгів, дистанції\nВідсіяно: 4 200 (2,09%)", "#FCE4EC", "#AD1457"),
        (0.61, 0.36, 0.17, 0.42, "Етап 4. Silver", "Broadcast Join (24/47)\nРозрахунок IS_DELAYED_15\nTOTAL_CAUSE_DELAY\n(196 500 рядків)", "#E8F5E9", "#2E7D32"),
        (0.81, 0.36, 0.17, 0.42, "Етап 5. Gold & ML", "6 вітрин Spark SQL\nPostgreSQL Upsert\nSpark MLlib (RF + LR)\nZIP-бекап вітрин", "#FFF8E1", "#F57F17"),
    ]
    for x, y, w, h, t, s, fc, ec in steps:
        draw_box(ax, x, y, w, h, t, s, fc, ec, fontsize=9.2)

    draw_box(
        ax, 0.40, 0.06, 0.18, 0.20,
        "Шар Quarantine",
        "flights_quarantine.parquet\ndq_quarantine_summary\nЗбереження коду помилки",
        "#FFEBEE", "#C62828", fontsize=8.8
    )

    draw_arrow(ax, 0.18, 0.57, 0.21, 0.57)
    draw_arrow(ax, 0.37, 0.57, 0.40, 0.57)
    draw_arrow(ax, 0.58, 0.57, 0.61, 0.57)
    draw_arrow(ax, 0.78, 0.57, 0.81, 0.57)
    draw_arrow(ax, 0.49, 0.36, 0.49, 0.26, "DQ != VALID", "#C62828")

    ax.text(
        0.5, 0.91,
        "Послідовність етапів трансформації даних у конвеєрі Medallion (Bronze -> Silver -> Gold + Quarantine)",
        ha="center", va="center", fontsize=11, fontweight="bold", color="#0F2C59"
    )
    fig.tight_layout()
    fig.savefig(OUT_DIR / "fig_3_1_etl_flow.png", dpi=220)
    plt.close(fig)


def fig_3_2_uml_class_diagram():
    fig, ax = plt.subplots(figsize=(11.5, 6.8), dpi=220)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    classes = [
        (0.03, 0.62, 0.27, 0.27, "ConfigManager", "- config_path: Path\n- raw_config: Dict\n+ spark_params(): Dict\n+ dq_rules(): Dict\n+ ml_params(): Dict"),
        (0.36, 0.62, 0.28, 0.27, "MedallionETLProcessor", "- spark: SparkSession\n- dq_engine: DataQualityEngine\n+ ingest_to_bronze(): DataFrame\n+ build_silver_and_quarantine()\n+ build_gold_marts(): Dict"),
        (0.70, 0.62, 0.27, 0.27, "DataQualityEngine", "- rules: Dict[str, Any]\n+ validate_and_split(raw_df,\n  valid_iata): Tuple[DataFrame,\n  DataFrame, pd.DataFrame]"),
        (0.03, 0.14, 0.27, 0.32, "BTSDatasetLoader", "- config: ConfigManager\n+ build_reference_tables()\n+ generate_bts_flights_dataset()\n+ haversine_miles(): ndarray"),
        (0.36, 0.14, 0.28, 0.32, "FlightDelayRegressor", "- spark: SparkSession\n- db: DatabaseManager\n+ train_and_evaluate(silver_df)\n+ save_regression_artifact()"),
        (0.70, 0.14, 0.27, 0.32, "FlightStreamProcessor", "- producer: OpenSkyKafkaProducer\n+ validate_stream_event()\n+ score_flight_delay_ml()\n+ consume_from_kafka_or_direct()"),
    ]
    for x, y, w, h, cname, body in classes:
        draw_box(ax, x, y, w, h, f"«class»\n{cname}", body, "#F8FAFC", "#1E3A8A", fontsize=9.2)

    draw_arrow(ax, 0.30, 0.75, 0.36, 0.75, "конфігурує")
    draw_arrow(ax, 0.64, 0.75, 0.70, 0.75, "викликає DQ")
    draw_arrow(ax, 0.165, 0.46, 0.38, 0.62, "постачає CSV")
    draw_arrow(ax, 0.50, 0.62, 0.50, 0.46, "передає Silver DF")
    draw_arrow(ax, 0.64, 0.30, 0.70, 0.30, "артефакт коеф.")

    ax.text(
        0.5, 0.95,
        "UML-діаграма класів програмно-аналітичного комплексу «ПАКАПА»",
        ha="center", va="center", fontsize=11.5, fontweight="bold", color="#0F2C59"
    )
    fig.tight_layout()
    fig.savefig(OUT_DIR / "fig_3_2_class_diagram.png", dpi=220)
    plt.close(fig)


def fig_3_3_uml_state_and_sequence():
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.5, 5.6), dpi=220)

    # Ліва панель: Діаграма станів конвеєра
    ax1.set_xlim(0, 1)
    ax1.set_ylim(0, 1)
    ax1.axis("off")
    ax1.set_title("а) UML-діаграма станів конвеєра даних", fontsize=10.5, fontweight="bold", color="#0F2C59")

    draw_box(ax1, 0.30, 0.84, 0.40, 0.10, "Очікування даних (Idle)", "", "#E3F2FD", "#1565C0", fontsize=9)
    draw_box(ax1, 0.05, 0.64, 0.40, 0.12, "Пакетне читання\n(Batch Ingestion BTS)", "", "#FFF3E0", "#EF6C00", fontsize=8.5)
    draw_box(ax1, 0.55, 0.64, 0.40, 0.12, "Потокове приймання\n(OpenSky Kafka Stream)", "", "#E0F7FA", "#00838F", fontsize=8.5)
    draw_box(ax1, 0.25, 0.44, 0.50, 0.12, "Контроль якості (Data Quality)\nта дедуплікація", "", "#FCE4EC", "#AD1457", fontsize=8.8)
    draw_box(ax1, 0.68, 0.24, 0.28, 0.12, "Карантин / DLQ\n(Ізоляція помилок)", "", "#FFEBEE", "#C62828", fontsize=8.3)
    draw_box(ax1, 0.06, 0.24, 0.54, 0.12, "Збагачення Silver, агрегація Gold\nта навчання Spark MLlib", "", "#E8F5E9", "#2E7D32", fontsize=8.5)
    draw_box(ax1, 0.22, 0.05, 0.56, 0.11, "Синхронізація PostgreSQL, BI та Backup", "", "#FFFDE7", "#F9A825", fontsize=8.5)

    draw_arrow(ax1, 0.42, 0.84, 0.25, 0.76, "[batch]")
    draw_arrow(ax1, 0.58, 0.84, 0.75, 0.76, "[stream]")
    draw_arrow(ax1, 0.25, 0.64, 0.42, 0.56)
    draw_arrow(ax1, 0.75, 0.64, 0.58, 0.56)
    draw_arrow(ax1, 0.62, 0.44, 0.80, 0.36, "[помилка]")
    draw_arrow(ax1, 0.40, 0.44, 0.33, 0.36, "[валідні]")
    draw_arrow(ax1, 0.33, 0.24, 0.45, 0.16)

    # Права панель: UML-діаграма взаємодії
    ax2.set_xlim(0, 1)
    ax2.set_ylim(0, 1)
    ax2.axis("off")
    ax2.set_title("б) UML-діаграма послідовності взаємодії", fontsize=10.5, fontweight="bold", color="#0F2C59")

    lifelines = [
        (0.12, "Аналітик /\nАдмін"),
        (0.36, "BI-Дашборд\n(Streamlit)"),
        (0.62, "Оркестратор\nPySpark / Kafka"),
        (0.88, "PostgreSQL /\nParquet Lake"),
    ]
    for lx, ltitle in lifelines:
        draw_box(ax2, lx - 0.10, 0.84, 0.20, 0.10, ltitle, "", "#EBF3FA", "#1F4E79", fontsize=8.2)
        ax2.plot([lx, lx], [0.06, 0.84], ls="--", color="#64748B", lw=1.2)

    msgs = [
        (0.12, 0.36, 0.76, "1. Вибір фільтрів / запит KPI"),
        (0.36, 0.88, 0.68, "2. SQL-запит до вітрин Gold"),
        (0.88, 0.36, 0.60, "3. Агреговані дані та метрики ML"),
        (0.36, 0.12, 0.52, "4. Візуалізація графіків та карти"),
        (0.12, 0.62, 0.40, "5. Запуск конвеєра / оновлення потоку"),
        (0.62, 0.88, 0.30, "6. Запис Silver, Gold, Quarantine"),
        (0.62, 0.36, 0.20, "7. Статус виконання та ZIP-бекап"),
    ]
    for x1, x2, y, text in msgs:
        draw_arrow(ax2, x1, y, x2, y)
        ax2.text((x1 + x2) / 2, y + 0.018, text, ha="center", va="bottom", fontsize=7.8, color="#0F172A")

    fig.tight_layout()
    fig.savefig(OUT_DIR / "fig_3_3_state_sequence.png", dpi=220)
    plt.close(fig)


def fig_4_1_airline_punctuality():
    names = [
        "All Nippon (NH)", "Japan Airlines (JL)", "Qatar Airways (QR)", "Singapore Air (SQ)",
        "Iberia (IB)", "Delta Air Lines (DL)", "SWISS (LX)", "LOT Polish (LO)",
        "Emirates (EK)", "KLM (KL)", "Ryanair (FR)", "Lufthansa (LH)",
        "British Airways (BA)", "Turkish Airlines (TK)", "Air France (AF)", "United (UA)",
        "American (AA)", "Southwest (WN)", "Air Canada (AC)", "Wizz Air (W6)", "JetBlue (B6)"
    ]
    otp = [
        73.59, 73.54, 73.53, 73.09, 72.97, 72.45, 72.21, 72.06,
        71.97, 71.80, 71.69, 71.35, 70.61, 70.12, 70.11, 70.06,
        69.25, 68.58, 67.15, 67.14, 64.87
    ]
    avg_delay = [
        1.56, 0.85, 2.44, 2.08, 2.09, 1.85, 1.81, 2.35,
        2.01, 2.63, 3.68, 2.65, 3.21, 3.84, 3.26, 3.59,
        3.92, 4.70, 5.47, 6.29, 8.33
    ]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.2, 6.0), dpi=220, sharey=True)
    y = np.arange(len(names))

    bars1 = ax1.barh(y, otp, color="#1D4ED8", alpha=0.88, edgecolor="#1E3A8A")
    ax1.set_yticks(y)
    ax1.set_yticklabels(names, fontsize=8.4)
    ax1.invert_yaxis()
    ax1.set_xlabel("Пунктуальність прибуття OTP-15 (%)", fontsize=9.5, fontweight="bold")
    ax1.set_xlim(55, 80)
    ax1.grid(axis="x", ls="--", alpha=0.5)
    for b, v in zip(bars1, otp):
        ax1.text(v + 0.3, b.get_y() + b.get_height() / 2, f"{v:.2f}%", va="center", fontsize=7.8)

    bars2 = ax2.barh(y, avg_delay, color="#DC2626", alpha=0.85, edgecolor="#991B1B")
    ax2.set_xlabel("Середня затримка прибуття ARR_DELAY (хв)", fontsize=9.5, fontweight="bold")
    ax2.set_xlim(0, 10.5)
    ax2.grid(axis="x", ls="--", alpha=0.5)
    for b, v in zip(bars2, avg_delay):
        ax2.text(v + 0.18, b.get_y() + b.get_height() / 2, f"{v:.2f} хв", va="center", fontsize=7.8)

    fig.suptitle("Рейтинг світових авіакомпаній за показником пунктуальності OTP-15 та середньою затримкою (N = 196 500 рейсів)", fontsize=10.8, fontweight="bold", color="#0F2C59")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "fig_4_1_airline_otp.png", dpi=220)
    plt.close(fig)


def fig_4_2_delay_factors_and_weather():
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.2, 4.8), dpi=220)

    factors = [
        "Погодні умови\n(WEATHER_DELAY)",
        "Пізнє прибуття літака\n(LATE_AIRCRAFT)",
        "Перевантаження УПР\n(NAS_DELAY)",
        "Причини перевізника\n(CARRIER_DELAY)",
        "Авіабезпека\n(SECURITY_DELAY)",
    ]
    shares = [50.62, 21.80, 15.39, 11.21, 0.98]
    colors = ["#0284C7", "#7C3AED", "#16A34A", "#EA580C", "#64748B"]

    y = np.arange(len(factors))
    bars = ax1.barh(y, shares, color=colors, edgecolor="#1E293B")
    ax1.set_yticks(y)
    ax1.set_yticklabels(factors, fontsize=8.8)
    ax1.invert_yaxis()
    ax1.set_xlabel("Частка у сумарному часі затримок (%)", fontsize=9.2, fontweight="bold")
    ax1.set_title("а) Структура сумарного часу затримок за чинниками", fontsize=10, fontweight="bold", color="#0F2C59")
    ax1.set_xlim(0, 60)
    ax1.grid(axis="x", ls="--", alpha=0.5)
    for b, s in zip(bars, shares):
        ax1.text(s + 0.8, b.get_y() + b.get_height() / 2, f"{s:.2f}%", va="center", fontsize=8.5, fontweight="bold")

    w_cond = ["Clear\n(Ясно)", "Cloudy\n(Хмарно)", "Rain\n(Дощ)", "Dense Fog\n(Туман)", "Thunderstorm\n(Гроза)", "Snow / Ice\n(Сніг/Ожеледь)"]
    w_delay = [-4.84, -1.26, 15.01, 74.98, 102.12, 104.65]
    w_colors = ["#22C55E", "#84CC16", "#F59E0B", "#F97316", "#EF4444", "#991B1B"]

    x = np.arange(len(w_cond))
    bars2 = ax2.bar(x, w_delay, color=w_colors, edgecolor="#1E293B", width=0.58)
    ax2.axhline(0, color="#334155", lw=1.0)
    ax2.set_xticks(x)
    ax2.set_xticklabels(w_cond, fontsize=8.3)
    ax2.set_ylabel("Середня затримка прибуття ARR_DELAY (хв)", fontsize=9.2, fontweight="bold")
    ax2.set_title("б) Вплив метеорологічних режимів на затримку рейсу", fontsize=10, fontweight="bold", color="#0F2C59")
    ax2.set_ylim(-15, 122)
    ax2.grid(axis="y", ls="--", alpha=0.5)
    for b, v in zip(bars2, w_delay):
        offset = 2.5 if v >= 0 else -7.5
        ax2.text(b.get_x() + b.get_width() / 2, v + offset, f"{v:+.1f} хв", ha="center", fontsize=8.1, fontweight="bold")

    fig.tight_layout()
    fig.savefig(OUT_DIR / "fig_4_2_factors_weather.png", dpi=220)
    plt.close(fig)


def fig_4_3_ml_feature_importance_and_quarantine():
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.2, 4.9), dpi=220)

    feats = [
        "CRS_ELAPSED_TIME (Плановий час у польоті)",
        "DISTANCE (Ортодромічна дистанція)",
        "DEP_DELAY (Затримка вильоту)",
        "VISIBILITY_MILES (Видимість на ЗПС)",
        "WEATHER_SEVERITY (Суворість погоди)",
        "PRECIP_MM (Інтенсивність опадів)",
        "PREV_AIRCRAFT_DELAY (Каскад борту)",
        "TAXI_OUT (Час руління на зліт)",
        "WIND_SPEED_KTS (Швидкість вітру)",
    ]
    imp = [38.43, 24.75, 17.59, 7.20, 4.93, 1.74, 1.71, 1.57, 1.06]

    y = np.arange(len(feats))
    b1 = ax1.barh(y, imp, color="#4F46E5", alpha=0.88, edgecolor="#312E81")
    ax1.set_yticks(y)
    ax1.set_yticklabels(feats, fontsize=8.3)
    ax1.invert_yaxis()
    ax1.set_xlabel("Важливість ознаки у Random Forest (%)", fontsize=9.2, fontweight="bold")
    ax1.set_title("а) Важливість чинників затримки (Spark MLlib RF, R²=0,940)", fontsize=9.8, fontweight="bold", color="#0F2C59")
    ax1.set_xlim(0, 46)
    ax1.grid(axis="x", ls="--", alpha=0.5)
    for b, v in zip(b1, imp):
        ax1.text(v + 0.6, b.get_y() + b.get_height() / 2, f"{v:.2f}%", va="center", fontsize=8.2)

    q_codes = [
        "ERR_NEGATIVE_DURATION\n(Від'ємна тривалість)",
        "ERR_DUPLICATE_FLIGHT\n(Дублікат рейсу)",
        "ERR_NULL_KEY\n(Пропуск ключа NULL)",
        "ERR_INVALID_IATA\n(Невірний код IATA)",
        "ERR_EXTREME_OUTLIER\n(Викид > 24 год)",
        "ERR_INVALID_DISTANCE\n(Аномальна дистанція)",
    ]
    q_counts = [800, 700, 700, 700, 700, 600]
    y2 = np.arange(len(q_codes))
    b2 = ax2.barh(y2, q_counts, color="#E11D48", alpha=0.85, edgecolor="#881337")
    ax2.set_yticks(y2)
    ax2.set_yticklabels(q_codes, fontsize=8.3)
    ax2.invert_yaxis()
    ax2.set_xlabel("Кількість ізольованих записів у Карантині (од.)", fontsize=9.2, fontweight="bold")
    ax2.set_title("б) Розподіл аномалій у шарі Quarantine (всього 4 200)", fontsize=10, fontweight="bold", color="#0F2C59")
    ax2.set_xlim(0, 960)
    ax2.grid(axis="x", ls="--", alpha=0.5)
    for b, v in zip(b2, q_counts):
        ax2.text(v + 12, b.get_y() + b.get_height() / 2, f"{v} ({v/4200*100:.1f}%)", va="center", fontsize=8.2)

    fig.tight_layout()
    fig.savefig(OUT_DIR / "fig_4_3_ml_and_dq.png", dpi=220)
    plt.close(fig)


if __name__ == "__main__":
    fig_1_1_lambda_medallion_architecture()
    fig_1_2_er_data_model()
    fig_3_1_etl_pipeline_flow()
    fig_3_2_uml_class_diagram()
    fig_3_3_uml_state_and_sequence()
    fig_4_1_airline_punctuality()
    fig_4_2_delay_factors_and_weather()
    fig_4_3_ml_feature_importance_and_quarantine()
    print("Усі 8 рисунків успішно згенеровано у:", OUT_DIR)
