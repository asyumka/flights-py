# Програмно-аналітичний комплекс обробки та аналізу великих даних пунктуальності авіарейсів і чинників затримок («ПАКАПА»)

**Варіант №3** курсової роботи з дисципліни *«Технології й інфраструктура великих даних для аналізу даних»* (спеціальність F3 «Комп’ютерні науки», ОПП «Технології обробки великих даних»).

---

## 1. Архітектура комплексу

Комплекс побудовано за гібридною **Lambda + Medallion архітектурою**:
1. **Пакетний шар (Batch Layer — US DOT BTS On-Time Performance):**
   * Обробка 200 000+ записів авіарейсів з довідниками 15 авіакомпаній (`dim_airlines`) та 35 аеропортів-хабів США (`dim_airports`) і метеорологічними показниками.
   * **Модуль контролю якості даних (`DataQualityEngine` у PySpark):** автоматичне виявлення 6 категорій аномалій і порушень цілісності (пропущені ключі, неіснуючі коди ІАТА, від'ємна тривалість польоту, помилкова дистанція, екстремальні викиди затримок > 24 год, дублікати) з ізоляцією у шар **`Quarantine`**.
   * **Багатошарове сховище (`Medallion Lakehouse`):**
     * `Bronze` (`data/lake/bronze/`): сирі типізовані дані у форматі Apache Parquet (Snappy).
     * `Silver` (`data/lake/silver/`): очищені та збагачені через `Broadcast Join` дані у форматі Parquet.
     * `Gold` (`data/lake/gold/` + СУБД **PostgreSQL**): 6 спеціалізованих аналітичних вітрин (`gold_airline_punctuality`, `gold_airport_performance`, `gold_temporal_dynamics`, `gold_delay_factors_summary`, `gold_route_cascades`, `gold_weather_impact`).
2. **Машинне навчання (`Apache Spark MLlib`):**
   * Регресійне прогнозування тривалості затримки прибуття рейсу (`ARR_DELAY` у хвилинах).
   * Порівняння базової лінійної моделі `LinearRegression (ElasticNet)` та ансамблевої моделі `RandomForestRegressor` за метриками **RMSE**, **MAE** та **$R^2$**.
   * Кількісна оцінка важливості чинників затримки (**Feature Importance**).
3. **Потоковий шар (Speed Layer — OpenSky Network REST API + Apache Kafka):**
   * Гібридний продюсер (`src/streaming/opensky_producer.py`) у реальному часі опитує публічний API `https://opensky-network.org/api/states/all` (з автоматичним резервним перемиканням у `replay_simulator`), публікує телеметрію літаків у топік Kafka `flights.live.stream`.
   * Потоковий обробник (`src/streaming/stream_processor.py`) читає події з Kafka, відсіює пошкоджені пакети у Dead Letter Queue (`flights.dlq`), виконує онлайн-скоринг очікуваної затримки за навченою ML-моделлю та записує результати у таблицю `live_flights_stream` в PostgreSQL.
4. **Інтерактивний BI-дашборд (`Streamlit + Plotly`):**
   * 6 аналітичних вкладок (KPI пунктуальності OTP-15, факторний аналіз 5 причин затримок та погоди, геокарта хабів та маршрутів США, ML-регресія та інтерактивний калькулятор затримки, жива карта потоку OpenSky + Kafka, панель адміністратора Data Quality та резервного копіювання).

---

## 2. Структура репозиторію

```text
flights/
├── config/
│   └── pipeline_config.yaml      # Конфігурація Spark, правил Data Quality, ML та Kafka
├── dashboard/
│   └── app.py                    # Багатосторінковий BI-дашборд (Streamlit + Plotly, 6 вкладок)
├── sql/
│   └── init.sql                  # DDL-схема аналітичного сховища PostgreSQL (15 таблиць)
├── src/
│   ├── dq/
│   │   └── quality_engine.py     # Модуль валідації якості даних та Quarantine у PySpark
│   ├── etl/
│   │   └── medallion_processor.py# PySpark ETL конвеєр (Bronze -> Silver -> Gold + PostgreSQL)
│   ├── ingestion/
│   │   └── dataset_loader.py     # Завантажувач/генератор US DOT BTS та довідників IATA
│   ├── ml/
│   │   └── delay_regressor.py    # Регресійні моделі Spark MLlib (Linear & Random Forest)
│   ├── streaming/
│   │   ├── opensky_producer.py   # Конектор OpenSky Network API -> Apache Kafka
│   │   └── stream_processor.py   # Консьюмер Kafka + потоковий DQ + ML-інференс
│   ├── utils/
│   │   ├── backup_manager.py     # Резервне копіювання золотих вітрин та конфігурацій
│   │   ├── config_manager.py     # Менеджер конфігурацій та змінних середовища
│   │   ├── db_manager.py         # Клієнт пакетного завантаження вітрин у PostgreSQL
│   │   └── spark_session.py      # Ініціалізація Apache Spark Session
│   └── main_pipeline.py          # Головний оркестратор наскрізного прогону конвеєра
├── tests/
│   └── test_pipeline.py          # Модульні та інтеграційні тести (pytest)
├── docker-compose.yml            # Оркестрація PostgreSQL, Kafka (KRaft), Spark та BI-дашборду
├── Dockerfile                    # Образ Apache Spark 3.5.3 + Python + PostgreSQL/Kafka JARs
└── requirements.txt              # Залежності Python
```

---

## 3. Швидкий старт та команди запуску (Оптимізовано для ноутбуків з 8 ГБ ОЗП)

Проєкт спеціально оптимізовано для комфортної роботи на ноутбуках із **8 ГБ оперативної пам'яті**:
* У репозиторій вже включені попередньо розраховані золоті вітрини (`data/lake/gold/`), довідники (`data/raw/airlines.csv`, `airports.csv`) та навчений артефакт ML-моделі (`models/delay_regression_artifact.json`) загальним розміром лише **~170 КБ**.
* Ліміти пам'яті знижено: `PostgreSQL` — до **512 МБ**, `Apache Kafka (KRaft)` — до **768 МБ** (`HEAP -Xmx384m`), `PySpark Driver` — до **1 ГБ** (`local[2]`, 4 shuffle-партиції).

### Варіант А: Миттєвий запуск одним кліком на Windows (`run_light.bat`)
```powershell
.\run_light.bat
```
Скрипт автоматично підніме легкі контейнери PostgreSQL і Kafka (або перейде в автономний режим читання вітрин із Parquet без Docker) і одразу відкриє BI-дашборд на **[http://localhost:8501](http://localhost:8501)**. Живий потік **OpenSky Network API** оновлюється прямо з вкладки №5 дашборду!

### Варіант Б: Покроковий запуск у терміналі
```powershell
# 1. Встановлення залежностей Python (у віртуальне середовище)
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt -r requirements-dev.txt

# 2. Підняття PostgreSQL та Apache Kafka (KRaft) у Docker
docker compose up -d postgres kafka kafka-init

# 3. (Опційно) Повний перерахунок конвеєра PySpark ETL + MLlib + OpenSky Kafka
# Для слабкого ноутбука (8 ГБ ОЗП) рекомендовано 50 000 - 100 000 записів:
python -m src.main_pipeline --records 50000

# 4. Запуск інтерактивного BI-дашборду (відкриється на http://localhost:8501)
streamlit run dashboard/app.py --server.port=8501
```

### Запуск автотестів
```powershell
pytest -v
```
