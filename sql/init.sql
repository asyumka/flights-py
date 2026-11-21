-- ============================================================================
-- ІНІЦІАЛІЗАЦІЯ СХЕМИ ДАНИХ АНАЛІТИЧНОГО СХОВИЩА POSTGRESQL (ШАР GOLD + STREAM)
-- Тема: Аналіз пунктуальності авіарейсів і чинників затримок (Варіант 3)
-- ============================================================================

-- 1. Довідник авіакомпаній
CREATE TABLE IF NOT EXISTS dim_airlines (
    iata_code VARCHAR(8) PRIMARY KEY,
    airline_name VARCHAR(128) NOT NULL,
    alliance VARCHAR(64),
    fleet_type VARCHAR(64),
    base_otp_rating DOUBLE PRECISION
);

-- 2. Довідник аеропортів (із геокоординатами та метеозонами)
CREATE TABLE IF NOT EXISTS dim_airports (
    iata_code VARCHAR(8) PRIMARY KEY,
    airport_name VARCHAR(160) NOT NULL,
    city VARCHAR(96) NOT NULL,
    state VARCHAR(64) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    hub_tier VARCHAR(32),
    climate_zone VARCHAR(48),
    avg_taxi_out_base DOUBLE PRECISION
);

-- 3. Золота вітрина 1: Показники пунктуальності та причини затримок за авіакомпаніями
CREATE TABLE IF NOT EXISTS gold_airline_punctuality (
    op_carrier VARCHAR(8) PRIMARY KEY,
    airline_name VARCHAR(128),
    total_flights BIGINT,
    completed_flights BIGINT,
    cancelled_flights BIGINT,
    delayed_flights_15m BIGINT,
    severe_delayed_60m BIGINT,
    otp_15_pct DOUBLE PRECISION,
    cancellation_rate_pct DOUBLE PRECISION,
    avg_dep_delay DOUBLE PRECISION,
    avg_arr_delay DOUBLE PRECISION,
    median_arr_delay DOUBLE PRECISION,
    p90_arr_delay DOUBLE PRECISION,
    avg_taxi_out DOUBLE PRECISION,
    avg_carrier_delay DOUBLE PRECISION,
    avg_weather_delay DOUBLE PRECISION,
    avg_nas_delay DOUBLE PRECISION,
    avg_security_delay DOUBLE PRECISION,
    avg_late_aircraft_delay DOUBLE PRECISION,
    total_delay_minutes DOUBLE PRECISION,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Золота вітрина 2: Просторова аналітика аеропортів та вузьких місць (Bottlenecks)
CREATE TABLE IF NOT EXISTS gold_airport_performance (
    origin VARCHAR(8) PRIMARY KEY,
    airport_name VARCHAR(160),
    city VARCHAR(96),
    state VARCHAR(64),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    hub_tier VARCHAR(32),
    total_departures BIGINT,
    delayed_departures BIGINT,
    otp_departure_pct DOUBLE PRECISION,
    avg_dep_delay DOUBLE PRECISION,
    avg_arr_delay DOUBLE PRECISION,
    avg_taxi_out DOUBLE PRECISION,
    avg_taxi_in DOUBLE PRECISION,
    weather_delay_share_pct DOUBLE PRECISION,
    nas_delay_share_pct DOUBLE PRECISION,
    late_aircraft_share_pct DOUBLE PRECISION,
    avg_wind_speed_kts DOUBLE PRECISION,
    avg_visibility_miles DOUBLE PRECISION,
    bottleneck_index DOUBLE PRECISION
);

-- 5. Золота вітрина 3: Часова динаміка (місяць, день тижня, година вильоту)
CREATE TABLE IF NOT EXISTS gold_temporal_dynamics (
    month INT,
    day_of_week INT,
    dep_hour INT,
    total_flights BIGINT,
    delayed_flights BIGINT,
    otp_15_pct DOUBLE PRECISION,
    avg_dep_delay DOUBLE PRECISION,
    avg_arr_delay DOUBLE PRECISION,
    avg_late_aircraft_delay DOUBLE PRECISION,
    avg_weather_delay DOUBLE PRECISION,
    avg_nas_delay DOUBLE PRECISION,
    PRIMARY KEY (month, day_of_week, dep_hour)
);

-- 6. Золота вітрина 4: Факторний аналіз причин затримок та метеоумов
CREATE TABLE IF NOT EXISTS gold_delay_factors_summary (
    factor_code VARCHAR(48) PRIMARY KEY,
    factor_name_ua VARCHAR(128),
    affected_flights BIGINT,
    total_delay_minutes DOUBLE PRECISION,
    share_of_total_delay_pct DOUBLE PRECISION,
    avg_minutes_when_present DOUBLE PRECISION,
    max_minutes_observed DOUBLE PRECISION,
    correlation_with_arr_delay DOUBLE PRECISION
);

-- 7. Золота вітрина 5: Топ маршрутів та каскадне поширення затримок
CREATE TABLE IF NOT EXISTS gold_route_cascades (
    route_id VARCHAR(32) PRIMARY KEY,
    origin VARCHAR(8),
    dest VARCHAR(8),
    origin_city VARCHAR(96),
    dest_city VARCHAR(96),
    origin_lat DOUBLE PRECISION,
    origin_lon DOUBLE PRECISION,
    dest_lat DOUBLE PRECISION,
    dest_lon DOUBLE PRECISION,
    distance_miles DOUBLE PRECISION,
    flights_count BIGINT,
    otp_15_pct DOUBLE PRECISION,
    avg_arr_delay DOUBLE PRECISION,
    avg_air_time DOUBLE PRECISION,
    cascade_ratio_pct DOUBLE PRECISION
);

-- 8. Золота вітрина 6: Вплив погодних умов на пунктуальність
CREATE TABLE IF NOT EXISTS gold_weather_impact (
    weather_condition VARCHAR(48) PRIMARY KEY,
    flights_count BIGINT,
    otp_15_pct DOUBLE PRECISION,
    avg_dep_delay DOUBLE PRECISION,
    avg_arr_delay DOUBLE PRECISION,
    avg_weather_delay DOUBLE PRECISION,
    avg_nas_delay DOUBLE PRECISION,
    avg_wind_speed DOUBLE PRECISION,
    avg_precip_mm DOUBLE PRECISION
);

-- 9. Результати машинного навчання (Метрики моделей регресії)
CREATE TABLE IF NOT EXISTS ml_model_metrics (
    model_id VARCHAR(64) PRIMARY KEY,
    model_name VARCHAR(128),
    target_variable VARCHAR(64),
    train_rows BIGINT,
    test_rows BIGINT,
    rmse DOUBLE PRECISION,
    mae DOUBLE PRECISION,
    r2_score DOUBLE PRECISION,
    training_time_sec DOUBLE PRECISION,
    trained_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 10. Важливість ознак (Feature Importance) регресійної моделі затримок
CREATE TABLE IF NOT EXISTS ml_feature_importance (
    feature_code VARCHAR(64) PRIMARY KEY,
    feature_name_ua VARCHAR(128),
    importance_weight DOUBLE PRECISION,
    importance_pct DOUBLE PRECISION,
    category VARCHAR(64)
);

-- 11. Вибірка прогнозів ML (Фактична vs Прогнозована затримка ARR_DELAY)
CREATE TABLE IF NOT EXISTS ml_predictions_sample (
    sample_id SERIAL PRIMARY KEY,
    fl_date VARCHAR(16),
    op_carrier VARCHAR(8),
    origin VARCHAR(8),
    dest VARCHAR(8),
    dep_hour INT,
    distance DOUBLE PRECISION,
    dep_delay DOUBLE PRECISION,
    prev_aircraft_delay DOUBLE PRECISION,
    taxi_out DOUBLE PRECISION,
    weather_severity_idx DOUBLE PRECISION,
    actual_arr_delay DOUBLE PRECISION,
    predicted_lr_delay DOUBLE PRECISION,
    predicted_rf_delay DOUBLE PRECISION,
    abs_error_rf DOUBLE PRECISION
);

-- 12. Зведення карантинного шару (Data Quality Quarantine Summary)
CREATE TABLE IF NOT EXISTS dq_quarantine_summary (
    error_code VARCHAR(64) PRIMARY KEY,
    error_description_ua VARCHAR(256),
    severity VARCHAR(32),
    rejected_count BIGINT,
    share_of_rejected_pct DOUBLE PRECISION,
    sample_record_json TEXT,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 13. Приклади відхилених записів у Карантині для інспекції адміністратором
CREATE TABLE IF NOT EXISTS dq_quarantine_samples (
    id SERIAL PRIMARY KEY,
    fl_date VARCHAR(32),
    op_carrier VARCHAR(16),
    fl_num VARCHAR(32),
    origin VARCHAR(16),
    dest VARCHAR(16),
    dep_delay DOUBLE PRECISION,
    arr_delay DOUBLE PRECISION,
    actual_elapsed_time DOUBLE PRECISION,
    distance DOUBLE PRECISION,
    dq_error_reason VARCHAR(128),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 14. Потоковий шар (Speed Layer): Події реального часу з Kafka / OpenSky Network API
CREATE TABLE IF NOT EXISTS live_flights_stream (
    event_id VARCHAR(64) PRIMARY KEY,
    icao24 VARCHAR(24),
    callsign VARCHAR(24),
    op_carrier VARCHAR(16),
    airline_name VARCHAR(128),
    origin VARCHAR(16),
    dest VARCHAR(16),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    baro_altitude_m DOUBLE PRECISION,
    velocity_kts DOUBLE PRECISION,
    heading_deg DOUBLE PRECISION,
    on_ground BOOLEAN,
    source_mode VARCHAR(32),
    estimated_dep_delay DOUBLE PRECISION,
    predicted_arr_delay_min DOUBLE PRECISION,
    delay_risk_level VARCHAR(32),
    event_timestamp TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 15. Журнал запусків конвеєра та метрик швидкодії (Pipeline Execution Audit Log)
CREATE TABLE IF NOT EXISTS pipeline_Audit_runs (
    run_id VARCHAR(64) PRIMARY KEY,
    stage_name VARCHAR(96),
    status VARCHAR(32),
    input_rows BIGINT,
    valid_rows BIGINT,
    quarantine_rows BIGINT,
    duration_sec DOUBLE PRECISION,
    throughput_rows_per_sec DOUBLE PRECISION,
    memory_mb DOUBLE PRECISION,
    details_json TEXT,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
