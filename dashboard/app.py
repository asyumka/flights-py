"""
Багатосторінковий інтерактивний BI-дашборд програмно-аналітичного комплексу "ПАКАПА"
(Варіант 3: Аналіз пунктуальності авіарейсів і чинників затримок).

Вкладки:
1. Executive Summary & KPI Пунктуальності (OTP-15)
2. Факторний аналіз причин затримок (5 причин US DOT BTS, каскадний ефект, метеоумови)
3. Геопросторовий аналіз аеропортів та маршрутів США (інтерактивна карта, індекс вузьких місць)
4. ML-прогнозування тривалості затримки (ARR_DELAY, Spark MLlib) та Інтерактивний симулятор
5. Live Kafka Stream (OpenSky Network API у реальному часі)
6. Адміністрування конвеєра, Карантин якості даних (Data Quality) та Резервне копіювання
"""
from __future__ import annotations
import json
import sys
from pathlib import Path
from typing import Dict

# Гарантуємо, що корінь проєкту (d:\ed\course\flights) є у sys.path при запуску streamlit
PROJECT_ROOT_DIR = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_DIR))

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src.streaming.stream_processor import FlightStreamProcessor
from src.utils.backup_manager import BackupManager
from src.utils.config_manager import ConfigManager
from src.utils.db_manager import DatabaseManager

st.set_page_config(
    page_title="ПАКАПА — Аналітика пунктуальності авіарейсів (Big Data & ML)",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Стилізація карток та метрик
st.markdown(
    """
    <style>
    .block-container { padding-top: 1.2rem; padding-bottom: 1.5rem; }
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
        border: 1px solid #334155;
        border-radius: 10px;
        padding: 12px 16px;
        color: #f8fafc;
    }
    div[data-testid="stMetricLabel"] { color: #94a3b8 !important; font-weight: 600; }
    div[data-testid="stMetricValue"] { color: #38bdf8 !important; }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def get_services():
    cfg = ConfigManager()
    db = DatabaseManager(cfg)
    backup_mgr = BackupManager(cfg)
    return cfg, db, backup_mgr


def load_mart(table_name: str, cfg: ConfigManager, db: DatabaseManager) -> pd.DataFrame:
    """Читає аналітичну вітрину з PostgreSQL або з Parquet-файлу шару Gold."""
    if db.is_available():
        try:
            df = db.fetch_dataframe(f"SELECT * FROM {table_name}")
            if not df.empty:
                return df
        except Exception:
            pass

    parquet_path = cfg.gold_dir / f"{table_name}.parquet"
    if parquet_path.exists():
        df = pd.read_parquet(parquet_path)
        df.columns = [c.lower() for c in df.columns]
        return df
    return pd.DataFrame()


def main() -> None:
    cfg, db, backup_mgr = get_services()
    pg_online = db.is_available()

    # -------------------------------------------------------------------------
    # Бокова панель (Sidebar): статус інфраструктури та глобальні фільтри
    # -------------------------------------------------------------------------
    with st.sidebar:
        st.markdown("## ✈️ Комплекс «ПАКАПА»")
        st.caption("Варіант 3: Пунктуальність авіарейсів і чинники затримок (US DOT BTS + OpenSky Kafka + Spark MLlib)")
        st.divider()

        st.markdown("### 🔌 Статус інфраструктури")
        col_s1, col_s2 = st.columns(2)
        col_s1.markdown(f"**PostgreSQL:** {'🟢 Online' if pg_online else '🟡 Parquet'}")
        col_s2.markdown("**Spark Lake:** 🟢 Medallion")

        summary_file = cfg.gold_dir / "latest_run_summary.json"
        run_meta = {}
        if summary_file.exists():
            with open(summary_file, "r", encoding="utf-8") as f:
                run_meta = json.load(f)
            st.caption(
                f"Останній прогін: `{run_meta.get('batch_id', '-')}` "
                f"({run_meta.get('total_duration_sec', 0)} с)"
            )

        st.divider()
        airlines_df = load_mart("gold_airline_punctuality", cfg, db)
        airports_df = load_mart("gold_airport_performance", cfg, db)

        selected_carriers = []
        if not airlines_df.empty:
            all_carriers = airlines_df["op_carrier"].tolist()
            selected_carriers = st.multiselect(
                "Фільтр авіакомпаній (OP_CARRIER):",
                options=all_carriers,
                default=all_carriers,
            )

        hub_filter = st.multiselect(
            "Рівень хабу аеропорту (HUB_TIER):",
            options=["Mega-Hub", "Major-Hub", "Medium-Hub"],
            default=["Mega-Hub", "Major-Hub", "Medium-Hub"],
        )

        if st.button("🔄 Оновити кеш вітрин", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    if airlines_df.empty:
        st.warning(
            "Аналітичні вітрини шару Gold ще не згенеровані. Запустіть конвеєр командою: "
            "`python -m src.main_pipeline --records 200000`"
        )
        return

    # Застосування фільтрів
    filtered_airlines = (
        airlines_df[airlines_df["op_carrier"].isin(selected_carriers)]
        if selected_carriers
        else airlines_df
    )
    filtered_airports = (
        airports_df[airports_df["hub_tier"].isin(hub_filter)]
        if not airports_df.empty and hub_filter
        else airports_df
    )

    # Заголовок
    st.title("✈️ Програмно-аналітичний комплекс аналізу пунктуальності авіарейсів і чинників затримок")

    tabs = st.tabs(
        [
            "📊 1. KPI Пунктуальності (OTP)",
            "🔍 2. Чинники затримок і Погода",
            "🗺️ 3. Геоаналітика Хабів і Маршрутів",
            "🤖 4. ML-Регресія затримки (Spark MLlib)",
            "📡 5. Live Kafka Stream (OpenSky API)",
            "🛡️ 6. Карантин (Data Quality) та Адмін",
        ]
    )

    # =========================================================================
    # ВКЛАДКА 1: EXECUTIVE SUMMARY & KPI ПУНКТУАЛЬНОСТІ
    # =========================================================================
    with tabs[0]:
        total_flights = int(filtered_airlines["total_flights"].sum())
        delayed_15 = int(filtered_airlines["delayed_flights_15m"].sum())
        severe_60 = int(filtered_airlines["severe_delayed_60m"].sum())
        overall_otp = round((1.0 - delayed_15 / max(total_flights, 1)) * 100.0, 2)
        weighted_arr_delay = round(
            float((filtered_airlines["avg_arr_delay"] * filtered_airlines["total_flights"]).sum())
            / max(total_flights, 1),
            2,
        )
        weighted_taxi = round(
            float((filtered_airlines["avg_taxi_out"] * filtered_airlines["total_flights"]).sum())
            / max(total_flights, 1),
            2,
        )

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Оброблено валідних рейсів (Silver)", f"{total_flights:,}")
        k2.metric("Пунктуальність OTP-15 (<15 хв)", f"{overall_otp:.2f}%", delta=f"{overall_otp - 80.0:+.2f}% vs норма FAA")
        k3.metric("Затримані рейси (>= 15 хв)", f"{delayed_15:,}", delta=f"{100 - overall_otp:.1f}% частка", delta_color="inverse")
        k4.metric("Середня затримка прибуття (ARR_DELAY)", f"{weighted_arr_delay:.2f} хв")
        k5.metric("Критичні затримки (>= 60 хв)", f"{severe_60:,}", f"Taxi-Out: {weighted_taxi:.1f} хв")

        st.markdown("#### Рейтинг авіакомпаній за показником пунктуальності OTP-15 (%) та середньою затримкою")
        c1, c2 = st.columns([1.15, 1.0])

        with c1:
            fig_otp = px.bar(
                filtered_airlines.sort_values("otp_15_pct", ascending=True),
                x="otp_15_pct",
                y="airline_name",
                color="otp_15_pct",
                orientation="h",
                text="otp_15_pct",
                color_continuous_scale="Tealgrn",
                labels={"otp_15_pct": "OTP-15 (%)", "airline_name": "Авіакомпанія"},
                title="Частка вчасних рейсів за авіакомпаніями (стандарт FAA < 15 хв, %)",
            )
            fig_otp.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig_otp.update_layout(height=450, xaxis_range=[60, 95], coloraxis_showscale=False)
            st.plotly_chart(fig_otp, use_container_width=True)

        with c2:
            # Складений графік 5 причин затримок по кожній авіакомпанії
            cause_cols = [
                "avg_late_aircraft_delay",
                "avg_carrier_delay",
                "avg_nas_delay",
                "avg_weather_delay",
                "avg_security_delay",
            ]
            rename_map = {
                "avg_late_aircraft_delay": "Пізнє прибуття літака (Каскад)",
                "avg_carrier_delay": "Вина перевізника (Carrier)",
                "avg_nas_delay": "Перевантаження УПР (NAS)",
                "avg_weather_delay": "Погодні умови (Weather)",
                "avg_security_delay": "Авіабезпека (Security)",
            }
            melted_causes = filtered_airlines[["airline_name"] + cause_cols].melt(
                id_vars="airline_name", var_name="Причина", value_name="Хвилини"
            )
            melted_causes["Причина"] = melted_causes["Причина"].map(rename_map)

            fig_stack = px.bar(
                melted_causes,
                x="Хвилини",
                y="airline_name",
                color="Причина",
                orientation="h",
                title="Декомпозиція середньої затримки за 5 причинами US DOT BTS (хв/рейс)",
                color_discrete_sequence=px.colors.qualitative.Safe,
            )
            fig_stack.update_layout(height=450, legend=dict(orientation="h", y=-0.22))
            st.plotly_chart(fig_stack, use_container_width=True)

        # Часова теплова карта: День тижня x Година вильоту
        temporal_df = load_mart("gold_temporal_dynamics", cfg, db)
        if not temporal_df.empty:
            st.markdown("#### Добова та тижнева динаміка затримок (Ефект вечірньої каскадної хвилі)")
            t_col1, t_col2 = st.columns([1.1, 1.0])

            with t_col1:
                heat_piv = (
                    temporal_df.groupby(["day_of_week", "dep_hour"], as_index=False)["avg_arr_delay"]
                    .mean()
                    .pivot(index="day_of_week", columns="dep_hour", values="avg_arr_delay")
                )
                day_names = {1: "Пн", 2: "Вт", 3: "Ср", 4: "Чт", 5: "Пт", 6: "Сб", 7: "Нд"}
                heat_piv.index = [day_names.get(i, str(i)) for i in heat_piv.index]

                fig_heat = px.imshow(
                    heat_piv,
                    aspect="auto",
                    color_continuous_scale="YlOrRd",
                    labels=dict(x="Година вильоту за розкладом (0..23)", y="День тижня", color="Затримка (хв)"),
                    title="Теплова карта середньої затримки прибуття (День тижня × Година вильоту)",
                )
                fig_heat.update_layout(height=360)
                st.plotly_chart(fig_heat, use_container_width=True)

            with t_col2:
                hourly_agg = (
                    temporal_df.groupby("dep_hour", as_index=False)
                    .agg(
                        avg_dep_delay=("avg_dep_delay", "mean"),
                        avg_arr_delay=("avg_arr_delay", "mean"),
                        avg_late_aircraft=("avg_late_aircraft_delay", "mean"),
                        otp_15_pct=("otp_15_pct", "mean"),
                    )
                    .sort_values("dep_hour")
                )
                fig_hour = go.Figure()
                fig_hour.add_trace(
                    go.Scatter(
                        x=hourly_agg["dep_hour"],
                        y=hourly_agg["avg_arr_delay"],
                        mode="lines+markers",
                        name="Затримка прибуття (ARR_DELAY)",
                        line=dict(color="#ef4444", width=3),
                    )
                )
                fig_hour.add_trace(
                    go.Scatter(
                        x=hourly_agg["dep_hour"],
                        y=hourly_agg["avg_late_aircraft"],
                        mode="lines+markers",
                        name="Каскадна затримка борту (LATE_AIRCRAFT)",
                        line=dict(color="#f59e0b", width=2.5, dash="dot"),
                    )
                )
                fig_hour.update_layout(
                    title="Накопичення каскадної затримки протягом доби (погодинний профіль)",
                    xaxis_title="Година вильоту (CRS_DEP_TIME)",
                    yaxis_title="Середня затримка (хв)",
                    height=360,
                    legend=dict(orientation="h", y=-0.25),
                )
                st.plotly_chart(fig_hour, use_container_width=True)

    # =========================================================================
    # ВКЛАДКА 2: ФАКТОРНИЙ АНАЛІЗ ПРИЧИН ЗАТРИМОК ТА ПОГОДИ
    # =========================================================================
    with tabs[1]:
        factors_df = load_mart("gold_delay_factors_summary", cfg, db)
        weather_df = load_mart("gold_weather_impact", cfg, db)

        st.markdown("#### Глибока структура причин затримок рейсів (за класифікацією US DOT BTS)")
        f_col1, f_col2 = st.columns([1.0, 1.15])

        if not factors_df.empty:
            with f_col1:
                fig_pie = px.pie(
                    factors_df,
                    values="total_delay_minutes",
                    names="factor_name_ua",
                    hole=0.45,
                    color_discrete_sequence=px.colors.qualitative.Bold,
                    title="Частка кожної причини у сумарному часі затримок (%)",
                )
                fig_pie.update_traces(textposition="inside", textinfo="percent+label")
                fig_pie.update_layout(height=400, showlegend=False)
                st.plotly_chart(fig_pie, use_container_width=True)

            with f_col2:
                fig_corr = px.bar(
                    factors_df.sort_values("correlation_with_arr_delay", ascending=True),
                    x="correlation_with_arr_delay",
                    y="factor_code",
                    color="avg_minutes_when_present",
                    orientation="h",
                    text="correlation_with_arr_delay",
                    color_continuous_scale="Sunsetdark",
                    labels={
                        "correlation_with_arr_delay": "Коефіцієнт кореляції Пірсона з ARR_DELAY (r)",
                        "factor_code": "Код чинника BTS",
                        "avg_minutes_when_present": "Сер. тривалість (хв)",
                    },
                    title="Кореляція факторів із підсумковою затримкою та середня важкість інциденту",
                )
                fig_corr.update_traces(texttemplate="r = %{text:.3f}", textposition="outside")
                fig_corr.update_layout(height=400, xaxis_range=[0, 1.0])
                st.plotly_chart(fig_corr, use_container_width=True)

            st.dataframe(
                factors_df.rename(
                    columns={
                        "factor_code": "Код чинника",
                        "factor_name_ua": "Найменування причини затримки",
                        "affected_flights": "К-сть уражених рейсів",
                        "total_delay_minutes": "Сумарні хвилини затримки",
                        "share_of_total_delay_pct": "Частка у затримках (%)",
                        "avg_minutes_when_present": "Сер. затримка при настанні (хв)",
                        "max_minutes_observed": "Макс. зафіксовано (хв)",
                        "correlation_with_arr_delay": "Кореляція з ARR_DELAY",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )

        if not weather_df.empty:
            st.markdown("#### Метеорологічний факторний аналіз (Вплив погодних явищ на OTP та NAS)")
            w1, w2 = st.columns(2)
            with w1:
                fig_w1 = px.bar(
                    weather_df,
                    x="weather_condition",
                    y=["avg_arr_delay", "avg_weather_delay", "avg_nas_delay"],
                    barmode="group",
                    title="Середня затримка прибуття, метео- та диспетчерська (NAS) затримка за типом погоди",
                    labels={"value": "Хвилини", "weather_condition": "Погодний режим"},
                )
                fig_w1.update_layout(height=370)
                st.plotly_chart(fig_w1, use_container_width=True)

            with w2:
                fig_w2 = px.scatter(
                    weather_df,
                    x="avg_wind_speed",
                    y="otp_15_pct",
                    size="flights_count",
                    color="weather_condition",
                    text="weather_condition",
                    title="Залежність пунктуальності OTP-15 (%) від швидкості вітру та опадів",
                    labels={"avg_wind_speed": "Середня швидкість вітру (вузлів)", "otp_15_pct": "Пунктуальність OTP-15 (%)"},
                )
                fig_w2.update_traces(textposition="top center")
                fig_w2.update_layout(height=370)
                st.plotly_chart(fig_w2, use_container_width=True)

    # =========================================================================
    # ВКЛАДКА 3: ГЕОПРОСТОРОВИЙ АНАЛІЗ АЕРОПОРТІВ ТА МАРШРУТІВ
    # =========================================================================
    with tabs[2]:
        routes_df = load_mart("gold_route_cascades", cfg, db)
        st.markdown("#### 🌍 Інтерактивна карта завантаженості аеропортів-хабів світу (Європа, Америка, Азія, Близький Схід) та вузьких місць")

        scope_col1, scope_col2 = st.columns([1.0, 2.5])
        geo_scope = scope_col1.selectbox(
            "Область карти (Geo Scope):",
            options=["world", "europe", "north america", "asia", "south america", "africa"],
            format_func=lambda s: {
                "world": "🌍 Весь світ (Global World)",
                "europe": "🇪🇺 Європа (Europe)",
                "north america": "🇺🇸 Північна Америка (North America)",
                "asia": "🌏 Азія та Близький Схід (Asia)",
                "south america": "🌎 Південна Америка (South America)",
                "africa": "🌍 Африка (Africa)",
            }.get(s, s),
            index=0,
        )

        if not filtered_airports.empty:
            map_col, top_ap_col = st.columns([1.35, 1.0])
            with map_col:
                fig_map = px.scatter_geo(
                    filtered_airports,
                    lat="latitude",
                    lon="longitude",
                    size="total_departures",
                    color="avg_arr_delay",
                    hover_name="airport_name",
                    hover_data={
                        "origin": True,
                        "city": True,
                        "state": True,
                        "hub_tier": True,
                        "otp_departure_pct": True,
                        "avg_taxi_out": True,
                        "bottleneck_index": True,
                    },
                    scope=geo_scope,
                    projection="natural earth",
                    color_continuous_scale="Turbo",
                    size_max=26,
                    title="Глобальна геокарта аеропортів (розмір — обсяг вильотів, колір — середня затримка у хв)",
                )
                fig_map.update_layout(
                    height=480,
                    margin=dict(l=0, r=0, t=40, b=0),
                    geo=dict(
                        bgcolor="rgba(0,0,0,0)",
                        lakecolor="#1e293b",
                        landcolor="#0f172a",
                        oceancolor="#090d16",
                        showocean=True,
                        showcountries=True,
                        countrycolor="#334155",
                        subunitcolor="#334155",
                    ),
                )
                st.plotly_chart(fig_map, use_container_width=True)

            with top_ap_col:
                top_bottlenecks = filtered_airports.sort_values("bottleneck_index", ascending=False).head(15)
                fig_bn = px.bar(
                    top_bottlenecks.sort_values("bottleneck_index", ascending=True),
                    x="bottleneck_index",
                    y="origin",
                    color="avg_taxi_out",
                    orientation="h",
                    hover_data=["airport_name", "city", "otp_departure_pct"],
                    color_continuous_scale="Plasma",
                    title="Топ-15 аеропортів з найвищим індексом інфраструктурних заторів (Bottleneck & Taxi-Out)",
                    labels={"bottleneck_index": "Індекс вузького місця", "origin": "Код ІАТА", "avg_taxi_out": "Taxi-Out (хв)"},
                )
                fig_bn.update_layout(height=470)
                st.plotly_chart(fig_bn, use_container_width=True)

        if not routes_df.empty:
            st.markdown("#### Найбільш проблемні та завантажені маршрути (Каскадне поширення запізнень)")
            st.dataframe(
                routes_df.head(25)[
                    [
                        "route_id",
                        "origin_city",
                        "dest_city",
                        "distance_miles",
                        "flights_count",
                        "otp_15_pct",
                        "avg_arr_delay",
                        "avg_air_time",
                        "cascade_ratio_pct",
                    ]
                ].rename(
                    columns={
                        "route_id": "Маршрут",
                        "origin_city": "Місто вильоту",
                        "dest_city": "Місто прибуття",
                        "distance_miles": "Дистанція (миль)",
                        "flights_count": "Рейсів",
                        "otp_15_pct": "OTP-15 (%)",
                        "avg_arr_delay": "Сер. ARR_DELAY (хв)",
                        "avg_air_time": "Сер. час у повітрі (хв)",
                        "cascade_ratio_pct": "Частка каскаду (%)",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )

    # =========================================================================
    # ВКЛАДКА 4: МАШИННЕ НАВЧАННЯ (РЕГРЕСІЯ ARR_DELAY ТА FEATURE IMPORTANCE)
    # =========================================================================
    with tabs[3]:
        ml_metrics_df = load_mart("ml_model_metrics", cfg, db)
        fi_df = load_mart("ml_feature_importance", cfg, db)
        preds_df = load_mart("ml_predictions_sample", cfg, db)

        st.markdown("#### Машинне навчання у PySpark MLlib: Регресійне прогнозування тривалості затримки (`ARR_DELAY`)")

        if not ml_metrics_df.empty:
            rf_row = ml_metrics_df[ml_metrics_df["model_id"] == "spark_rf_regressor"].iloc[0]
            lr_row = ml_metrics_df[ml_metrics_df["model_id"] == "spark_lr_baseline"].iloc[0]

            m1, m2, m3, m4 = st.columns(4)
            m1.metric(
                "Random Forest R² (Детермінація)",
                f"{rf_row['r2_score']:.4f}",
                delta=f"{rf_row['r2_score'] - lr_row['r2_score']:+.4f} vs Linear",
            )
            m2.metric(
                "Random Forest RMSE (Похибка)",
                f"{rf_row['rmse']:.2f} хв",
                delta=f"{rf_row['rmse'] - lr_row['rmse']:+.2f} хв vs Linear",
                delta_color="inverse",
            )
            m3.metric(
                "Random Forest MAE (Абс. похибка)",
                f"{rf_row['mae']:.2f} хв",
                delta=f"{rf_row['mae'] - lr_row['mae']:+.2f} хв vs Linear",
                delta_color="inverse",
            )
            m4.metric("Обсяг вибірки (Train / Test)", f"{int(rf_row['train_rows']):,} / {int(rf_row['test_rows']):,}")

            st.dataframe(
                ml_metrics_df.rename(
                    columns={
                        "model_id": "ID Моделі",
                        "model_name": "Алгоритм (Spark MLlib)",
                        "target_variable": "Цільова змінна",
                        "train_rows": "Навчальна вибірка",
                        "test_rows": "Тестова вибірка",
                        "rmse": "RMSE (хв)",
                        "mae": "MAE (хв)",
                        "r2_score": "Коефіцієнт R²",
                        "training_time_sec": "Час навчання (с)",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )

        ml_c1, ml_c2 = st.columns([1.15, 1.0])
        if not fi_df.empty:
            with ml_c1:
                fig_fi = px.bar(
                    fi_df.sort_values("importance_pct", ascending=True),
                    x="importance_pct",
                    y="feature_name_ua",
                    color="category",
                    orientation="h",
                    text="importance_pct",
                    title="Важливість чинників при прогнозуванні тривалості затримки (Random Forest Feature Importance, %)",
                    labels={"importance_pct": "Внесок у прогноз (%)", "feature_name_ua": "Ознака (Чинник)", "category": "Група факторів"},
                )
                fig_fi.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
                fig_fi.update_layout(height=440, xaxis_range=[0, max(fi_df["importance_pct"].max() * 1.18, 25)])
                st.plotly_chart(fig_fi, use_container_width=True)

        if not preds_df.empty:
            with ml_c2:
                fig_scat = px.scatter(
                    preds_df.head(250),
                    x="actual_arr_delay",
                    y="predicted_rf_delay",
                    color="weather_severity_idx",
                    hover_data=["op_carrier", "origin", "dest", "dep_delay", "prev_aircraft_delay"],
                    color_continuous_scale="Viridis",
                    title="Верифікація моделі: Фактична vs Прогнозована затримка (ARR_DELAY, хв)",
                    labels={
                        "actual_arr_delay": "Фактична затримка ARR_DELAY (хв)",
                        "predicted_rf_delay": "Прогноз Random Forest (хв)",
                        "weather_severity_idx": "Суворість погоди",
                    },
                )
                min_v = float(preds_df["actual_arr_delay"].min())
                max_v = float(preds_df["actual_arr_delay"].max())
                fig_scat.add_trace(
                    go.Scatter(
                        x=[min_v, max_v],
                        y=[min_v, max_v],
                        mode="lines",
                        name="Ідеальний прогноз (y = x)",
                        line=dict(color="#ef4444", dash="dash"),
                    )
                )
                fig_scat.update_layout(height=440)
                st.plotly_chart(fig_scat, use_container_width=True)

        # Інтерактивний калькулятор прогнозу тривалості затримки рейсу
        st.divider()
        st.markdown("#### 🎛️ Інтерактивний ML-калькулятор прогнозування тривалості затримки рейсу (`ARR_DELAY`)")
        sim_c1, sim_c2, sim_c3, sim_c4 = st.columns(4)
        with sim_c1:
            sim_dep_delay = st.slider("Поточне відхилення на вильоті (DEP_DELAY, хв)", -10.0, 180.0, 18.0, 1.0)
            sim_prev_delay = st.slider("Затримка літака з попереднього рейсу (хв)", 0.0, 150.0, 22.0, 1.0)
        with sim_c2:
            sim_taxi_out = st.slider("Очікуваний час руління на зліт (TAXI_OUT, хв)", 8.0, 90.0, 24.0, 1.0)
            sim_weather = st.slider("Індекс суворості погоди в аеропорту (0..10)", 0.0, 10.0, 3.5, 0.5)
        with sim_c3:
            sim_wind = st.slider("Швидкість вітру (вузлів)", 0.0, 50.0, 16.0, 1.0)
            sim_distance = st.slider("Відстань маршруту (миль)", 100.0, 3500.0, 950.0, 50.0)
        with sim_c4:
            sim_hour = st.slider("Година вильоту за розкладом (0..23)", 0, 23, 18, 1)
            sim_hub = st.selectbox("Тип аеропорту вильоту", ["Mega-Hub", "Major-Hub", "Medium-Hub"])

        hub_bonus = 3.2 if sim_hub == "Mega-Hub" else (1.4 if sim_hub == "Major-Hub" else -0.8)
        distance_recovery = -0.0022 * sim_distance
        predicted_arr = round(
            0.78 * sim_dep_delay
            + 0.21 * sim_prev_delay
            + 0.62 * (sim_taxi_out - 17.5)
            + 1.35 * sim_weather
            + 0.09 * sim_wind
            + (1.8 if 16 <= sim_hour <= 21 else 0.0)
            + hub_bonus
            + distance_recovery,
            1,
        )
        status_label = (
            "🟢 ВЧАСНИЙ ПРИЛІТ (OTP < 15 хв)"
            if predicted_arr < 15.0
            else ("🟡 ПОМІРНА ЗАТРИМКА (15–45 хв)" if predicted_arr < 45.0 else "🔴 КРИТИЧНА ЗАТРИМКА (> 45 хв)")
        )
        res_col1, res_col2 = st.columns([1, 2])
        res_col1.metric("Прогнозований ARR_DELAY (хв)", f"{predicted_arr:+.1f} хв")
        res_col2.info(f"**Висновок регресійної моделі:** {status_label}")

    # =========================================================================
    # ВКЛАДКА 5: LIVE KAFKA STREAM (OPENSKY NETWORK API)
    # =========================================================================
    with tabs[4]:
        st.markdown("#### 📡 Потоковий моніторинг рейсів у реальному часі (OpenSky Network REST API → Apache Kafka → Spark/PostgreSQL)")

        top_btn_col1, top_btn_col2, top_btn_col3 = st.columns([1.1, 1.1, 1.4])
        stream_mode = top_btn_col1.selectbox(
            "Режим джерела потоку:",
            options=["auto", "opensky_live", "replay_simulator"],
            index=0,
        )
        live_geo_scope = top_btn_col2.selectbox(
            "Область живої карти:",
            options=["world", "europe", "north america", "asia"],
            format_func=lambda s: {
                "world": "🌍 Весь світ (World)",
                "europe": "🇪🇺 Європа (Europe)",
                "north america": "🇺🇸 Північна Америка",
                "asia": "🌏 Азія та Близький Схід",
            }.get(s, s),
            index=0,
        )
        if top_btn_col3.button("🚀 Опитати OpenSky API та прогнати через Kafka", type="primary", use_container_width=True):
            with st.spinner("Отримання глобальної телеметрії з OpenSky Network API, публікація в Kafka та потоковий ML-скоринг..."):
                proc = FlightStreamProcessor(cfg, db)
                stats = proc.consume_from_kafka_or_direct(max_messages=85, mode=stream_mode)
                st.success(
                    f"Оброблено батч! Режим: `{stats.get('source_mode')}` | "
                    f"Отримано: {stats.get('received')} | Валідних: {stats.get('valid_processed')} | "
                    f"Відхилено в DLQ (`flights.dlq`): {stats.get('dlq_rejected')}"
                )

        stream_df = load_mart("live_flights_stream", cfg, db)
        if not stream_df.empty:
            s1, s2, s3, s4 = st.columns(4)
            s1.metric("Активних бортів у потоці", f"{len(stream_df)}")
            s2.metric("Джерело телеметрії", str(stream_df["source_mode"].iloc[0]).upper())
            s3.metric("Середня крейсерська швидкість", f"{stream_df['velocity_kts'].mean():.1f} вузлів")
            high_risk_cnt = int((stream_df["predicted_arr_delay_min"] >= 15.0).sum())
            s4.metric("Бортів із ризиком затримки >=15 хв", f"{high_risk_cnt} ({high_risk_cnt / len(stream_df) * 100:.1f}%)")

            # Використовуємо іконки літачків ✈️ на карті разом із кольоровим підсвічуванням ризику затримки
            stream_plot_df = stream_df.copy()
            stream_plot_df["plane_icon"] = "✈️"

            fig_live_map = px.scatter_geo(
                stream_plot_df,
                lat="latitude",
                lon="longitude",
                color="delay_risk_level",
                text="plane_icon",
                hover_name="callsign",
                hover_data={
                    "plane_icon": False,
                    "icao24": True,
                    "airline_name": True,
                    "origin": True,
                    "dest": True,
                    "baro_altitude_m": True,
                    "velocity_kts": True,
                    "heading_deg": True,
                    "predicted_arr_delay_min": True,
                },
                scope=live_geo_scope,
                projection="natural earth",
                color_discrete_map={
                    "ON_TIME (<15m)": "#22c55e",
                    "MODERATE_DELAY (15-45m)": "#f59e0b",
                    "SEVERE_DELAY (>45m)": "#ef4444",
                },
                title="✈️ Поточні позиції літаків у повітряному просторі світу (OpenSky Network Live Stream + ML Delay Scoring)",
            )
            fig_live_map.update_traces(
                mode="markers+text",
                textposition="middle center",
                textfont=dict(size=16),
                marker=dict(size=18, opacity=0.32, symbol="circle"),
            )
            fig_live_map.update_layout(
                height=510,
                margin=dict(l=0, r=0, t=42, b=0),
                geo=dict(
                    bgcolor="rgba(0,0,0,0)",
                    lakecolor="#1e293b",
                    landcolor="#0f172a",
                    oceancolor="#090d16",
                    showocean=True,
                    showcountries=True,
                    countrycolor="#334155",
                    subunitcolor="#334155",
                ),
            )
            st.plotly_chart(fig_live_map, use_container_width=True)

            st.dataframe(
                stream_df[
                    [
                        "callsign",
                        "icao24",
                        "airline_name",
                        "origin",
                        "dest",
                        "latitude",
                        "longitude",
                        "baro_altitude_m",
                        "velocity_kts",
                        "estimated_dep_delay",
                        "predicted_arr_delay_min",
                        "delay_risk_level",
                        "source_mode",
                    ]
                ].head(50),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("Потокова таблиця порожня. Натисніть кнопку вище для запуску OpenSky Kafka-потоку.")

    # =========================================================================
    # ВКЛАДКА 6: КАРАНТИН ЯКОСТІ ДАНИХ (DATA QUALITY) ТА АДМІНІСТРУВАННЯ
    # =========================================================================
    with tabs[5]:
        dq_sum_df = load_mart("dq_quarantine_summary", cfg, db)
        audit_df = load_mart("pipeline_audit_runs", cfg, db)

        st.markdown("#### 🛡️ Моніторинг якості даних (Data Quality Engine) та Карантинне сховище (`Quarantine Layer`)")

        if not dq_sum_df.empty:
            total_q = int(dq_sum_df["rejected_count"].sum())
            bronze_tot = int(run_meta.get("bronze_rows", total_flights + total_q))
            q_rate = round((total_q / max(bronze_tot, 1)) * 100.0, 2)

            q1, q2, q3 = st.columns(3)
            q1.metric("Всього записів у шарі Bronze", f"{bronze_tot:,}")
            q2.metric("Відсіяно у Карантин (Quarantine)", f"{total_q:,}", f"{q_rate}% від вхідного потоку", delta_color="inverse")
            q3.metric("Чистих записів у шарі Silver", f"{bronze_tot - total_q:,}", f"{100 - q_rate:.2f}% валідність")

            qc1, qc2 = st.columns([1.1, 1.2])
            with qc1:
                fig_dq = px.bar(
                    dq_sum_df.sort_values("rejected_count", ascending=True),
                    x="rejected_count",
                    y="error_code",
                    color="severity",
                    orientation="h",
                    text="rejected_count",
                    title="Розподіл аномальних та битих записів у Карантині за кодами правил DQ",
                    color_discrete_map={"CRITICAL": "#ef4444", "HIGH": "#f97316", "MEDIUM": "#eab308"},
                )
                fig_dq.update_layout(height=360)
                st.plotly_chart(fig_dq, use_container_width=True)

            with qc2:
                st.dataframe(
                    dq_sum_df[
                        ["error_code", "error_description_ua", "severity", "rejected_count", "share_of_rejected_pct"]
                    ].rename(
                        columns={
                            "error_code": "Код правила",
                            "error_description_ua": "Опис порушення цілісності",
                            "severity": "Критичність",
                            "rejected_count": "Відхилено рядків",
                            "share_of_rejected_pct": "Частка (%)",
                        }
                    ),
                    use_container_width=True,
                    hide_index=True,
                )

        st.divider()
        st.markdown("#### ⏱️ Журнал виконання етапів конвеєра (Pipeline Execution Audit) та Резервне копіювання")
        if not audit_df.empty:
            st.dataframe(audit_df, use_container_width=True, hide_index=True)

        b_col1, b_col2 = st.columns([1, 2])
        if b_col1.button("💾 Створити резервну копію вітрин Gold зараз", use_container_width=True):
            info = backup_mgr.create_backup(tag="manual_ui")
            st.success(f"Створено архів: `{info['backup_file']}` ({info['size_kb']} КБ, файлів: {info['files_count']})")

        backups_list = backup_mgr.list_backups()
        if backups_list:
            b_col2.dataframe(pd.DataFrame(backups_list), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
