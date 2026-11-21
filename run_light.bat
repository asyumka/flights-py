@echo off
chcp 65001 >nul
echo ============================================================================
echo   ЗАПУСК КОМПЛЕКСУ "ПАКАПА" (Оптимізований режим для ноутбука 8 ГБ ОЗП)
echo ============================================================================
echo.

if not exist ".env" (
    copy ".env.example" ".env" >nul
    echo [OK] Створено файл .env із шаблону .env.example
)

echo [1/3] Перевірка Docker контейнерів (PostgreSQL + Apache Kafka KRaft)...
docker compose up -d postgres kafka kafka-init
if %errorlevel% neq 0 (
    echo [УВАГА] Docker не запущено або недоступний. Дашборд працюватиме в автономному режимі Parquet Gold Lakehouse!
) else (
    echo [OK] Контейнери PostgreSQL та Kafka запущено у легкому режимі.
)

echo.
echo [2/3] Перевірка наявності золотих вітрин даних (data/lake/gold)...
if exist "data\lake\gold\gold_airline_punctuality.parquet" (
    echo [OK] Вітрини Gold вже підготовлені! Важкий PySpark-розрахунок можна пропустити.
    echo      (Щоб перезапустити PySpark конвеєр з нуля, виконайте: python -m src.main_pipeline --records 50000)
) else (
    echo [INFO] Запуск полегшеного конвеєра PySpark (50 000 записів для 8 ГБ ОЗП)...
    python -m src.main_pipeline --records 50000
)

echo.
echo [3/3] Запуск інтерактивного BI-дашборду на http://localhost:8501 ...
streamlit run dashboard/app.py --server.port=8501
