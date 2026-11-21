"""
Частина 4 генератора Пояснювальної записки: Висновки, Список використаних джерел, Додатки А, Б, В та фінальна збірка DOCX.
"""
from __future__ import annotations
from pathlib import Path
from docx import Document
from docx.shared import Cm, Mm, Pt

from build_report_part1 import (
    OUT_DOCX_DOCS,
    OUT_DOCX_MAIN,
    ROOT_DIR,
    add_body_p,
    add_bullet_p,
    add_chapter_heading,
    add_code_block,
    add_page_number_footer,
    add_styled_table,
    add_sub_heading,
    build_front_matter,
    format_run,
)
from build_report_part2 import build_intro_and_chapters_1_2
from build_report_part3 import build_chapters_3_4
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_LINE_SPACING


def add_appendix_heading(doc: Document, letter: str, title: str):
    """
    Оформлення додатків за стор. 17 методички:
    - Кожен додаток на новій сторінці.
    - Праворуч прописними літерами слово «Додаток А» (без літер Г, Є, І, Ї, И, О, Ч, Ь -> А, Б, В, Д).
    - На наступному рядку напівжирним шрифтом по середині малими літерами з першої великої заголовок додатка.
    """
    doc.add_page_break()
    p_top = doc.add_paragraph()
    p_top.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    p_top.paragraph_format.first_line_indent = Cm(0)
    p_top.paragraph_format.space_before = Pt(0)
    p_top.paragraph_format.space_after = Pt(4)
    r_top = p_top.add_run(f"Додаток {letter}")
    format_run(r_top, 14, bold=True)

    p_title = doc.add_paragraph()
    p_title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_title.paragraph_format.first_line_indent = Cm(0)
    p_title.paragraph_format.space_before = Pt(2)
    p_title.paragraph_format.space_after = Pt(14)
    r_title = p_title.add_run(title.rstrip("."))
    format_run(r_title, 14, bold=True)


def build_conclusions_refs_appendices(doc: Document):
    # =========================================================================
    # ВИСНОВКИ
    # =========================================================================
    add_chapter_heading(doc, "ВИСНОВКИ", new_page=True)
    add_body_p(
        doc,
        "У результаті виконання курсової роботи з дисципліни «Технології й інфраструктура великих даних для аналізу даних» розроблено, контейнеризовано та експериментально досліджено повномасштабний програмно-аналітичний комплекс «ПАКАПА» для приймання, багатошарового зберігання, контролю якості, розподіленої пакетно-потокової обробки, машинного навчання та інтерактивної BI-візуалізації великих даних пунктуальності авіарейсів і чинників їх затримок (Варіант № 3). Основні науково-технічні та практичні результати роботи полягають у наступному:",
    )
    concl_items = [
        "1. Проведено комплексний аналіз предметної області регулярності комерційних авіаперевезень за стандартами ICAO, FAA та US DOT BTS. Досліджено структуру канонічної п’ятифакторної декомпозиції часу затримок (CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY) та механізми каскадного поширення запізнень між оборотними рейсами у мережі вузлових аеропортів.",
        "2. Розроблено та обґрунтовано технічне завдання на створення програмно-аналітичного комплексу «ПАКАПА», у якому формалізовано функціональні характеристики, організацію вхідних і вихідних потоків, вимоги до відмовостійкості, швидкодії, резервного копіювання та порядок приймально-здавальних випробувань.",
        "3. Спроєктовано та реалізовано гібридну архітектуру платформи великих даних, що органічно поєднує парадигму Lambda (пакетний контур US DOT BTS + потоковий контур OpenSky Network REST API через Apache Kafka у режимі KRaft) із чотиришаровою моделлю озера-сховища даних Medallion (Bronze, Silver, Gold, Quarantine) на базі колонкового формату Apache Parquet (Snappy) та реляційної СУБД PostgreSQL 16.",
        "4. Розроблено векторизоване ядро контролю якості даних (DataQualityEngine у PySpark), яке за допомогою віконної дедуплікації та системи правил цілісності виявило й ізолювало у карантинному шарі 4 200 дефектних та аномальних записів (2,09% від початкового обсягу 200 700 рядків за 6 категоріями помилок), гарантувавши стовідсоткову чистоту 196 500 записів аналітичного шару Silver.",
        "5. Засобами Spark SQL та Broadcast Join (24 світові авіакомпанії та 47 глобальних аеропортів-хабів) сформовано 6 золотих аналітичних вітрин шару Gold. Встановлено, що середній показник пунктуальності прибуття OTP-15 становить 70,69% (середня затримка вильоту — 11,03 хв, прибуття — 3,47 хв), при цьому лідерами регулярності у світі є All Nippon Airways (73,59%), Japan Airlines (73,54%), Qatar Airways (73,53%), Singapore Airlines (73,09%), Iberia (72,97%) та Delta Air Lines (72,45%). Доведено, що домінуючим чинником тривалих збоїв є метеорологічний фактор (50,62% сумарного часу причинних затримок, r = 0,5506) та каскадне запізнення літака з попереднього рейсу (21,80%), а найбільш перевантаженими за індексом наземного руління та вильоту є мега-хаби JFK (TAXI_OUT = 29,70 хв, BI_hub = 20,88), Дубай DXB (BI_hub = 19,54), Лондон Гітроу LHR (27,29 хв, BI_hub = 19,26), Чикаго ORD (25,33 хв, BI_hub = 19,10) та Амстердам Схіпгол AMS (26,15 хв, BI_hub = 18,79).",
        "6. У бібліотеці Apache Spark MLlib реалізовано та порівняно дві регресійні моделі прогнозування тривалості затримки прибуття ARR_DELAY (хв): ансамбль Random Forest Regressor (RMSE = 11,007 хв, MAE = 7,322 хв, R² = 0,9399) та регуляризовану лінійну регресію ElasticNet (RMSE = 30,332 хв, MAE = 20,589 хв, R² = 0,5432). Доведено, що на глобальній мережі міжконтинентальних маршрутів нелінійна ансамблева модель Random Forest суттєво перевершує лінійну регресію. За результатами оцінки важливості ознак (Feature Importance) встановлено, що найбільшу прогностичну вагу мають планова тривалість перельоту CRS_ELAPSED_TIME (38,43%), ортодромічна дистанція DISTANCE (24,75%), початкове відхилення вильоту DEP_DELAY (17,59%), видимість на злітній смузі VISIBILITY_MILES (7,20%) та індекс суворості погоди WEATHER_SEVERITY_IDX (4,93%).",
        "7. Створено шестисторінковий інтерактивний BI-дашборд (Streamlit + Plotly) з глобальною картою хабів, живим моніторингом літаків OpenSky Network («✈️») та ML-калькулятором затримок, пакет автоматизованих модульних та інтеграційних тестів pytest (100% проходження) і конфігурацію контейнерного розгортання Docker Compose. Повний наскрізний прогін конвеєра на 200 700 записах виконується за 58,51 с при піковому споживанні пам’яті 129,69 Мбайт RSS, що повністю підтверджує високу ефективність і відтворюваність розробленого рішення.",
    ]
    for ci in concl_items:
        add_body_p(doc, ci)

    # =========================================================================
    # СПИСОК ВИКОРИСТАНИХ ДЖЕРЕЛ
    # =========================================================================
    add_chapter_heading(doc, "СПИСОК ВИКОРИСТАНИХ ДЖЕРЕЛ", new_page=True)
    references = [
        "1. Олещенко Л.М. Технології оброблення великих даних. Київ: КПІ ім. Ігоря Сікорського, 2021. 227 с.",
        "2. Хабарлак К.С., Хом’як Т.В. Аналіз та обробка великих даних. Дніпро: НТУ «Дніпровська політехніка», 2024. 111 с.",
        "3. Сугоняк І.І. Управління великими даними. Житомир: Житомирська політехніка, 2021. 49 с.",
        "4. Klosterman S. Data Science Projects with Python: A case study approach to gaining valuable insights from real data with machine learning. 2nd ed. Birmingham: Packt Publishing, 2021. 432 p.",
        "5. Chambers B., Zaharia M. Spark: The Definitive Guide: Big Data Processing Made Simple. Sebastopol: O’Reilly Media, 2018. 604 p.",
        "6. Kimball R., Ross M. The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling. 3rd ed. Indianapolis: Wiley, 2013. 608 p.",
        "7. Olson D.L., Wu D. Predictive Data Mining Models. 2nd ed. Cham: Springer, 2020. 125 p.",
        "8. Положення про кваліфікаційні роботи (проєкти) здобувачів вищої освіти НУ КАІ. СМЯ П 03.01(10) – 03 – 2024. Київ: НУ КАІ, 2024. 42 с.",
        "9. Apache Spark Documentation (PySpark SQL, DataFrames and MLlib Guide). URL: https://spark.apache.org/docs/latest/ (дата звернення: 10.11.2026).",
        "10. Apache Kafka Documentation (KRaft Architecture & Streams API). URL: https://kafka.apache.org/documentation/ (дата звернення: 10.11.2026).",
        "11. Apache Hadoop & Parquet Columnar Storage Format Documentation. URL: https://hadoop.apache.org/docs/ (дата звернення: 10.11.2026).",
        "12. Docker & Docker Compose Official Documentation. URL: https://docs.docker.com/ (дата звернення: 10.11.2026).",
        "13. US Department of Transportation, Bureau of Transportation Statistics (BTS). Reporting Carrier On-Time Performance Dataset. URL: https://www.transtats.bts.gov/ (дата звернення: 10.11.2026).",
        "14. Schäfer M., Strohmeier M., Lenders V., Martinovic I., Wilhelm M. Bringing up OpenSky: A large-scale ADS-B sensor network for research // IPSN-14 Proceedings of the 13th International Symposium on Information Processing in Sensor Networks. IEEE, 2014. P. 83–94. URL: https://opensky-network.org/ (дата звернення: 10.11.2026).",
        "15. Armbrust M., Ghodsi A., Xin R., Zaharia M. Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics // Proceedings of CIDR. 2021. P. 1–8.",
        "16. Kleppmann M. Designing Data-Intensive Applications: The Big Ideas Behind Reliable, Scalable, and Maintainable Systems. Sebastopol: O’Reilly Media, 2017. 616 p.",
        "17. PostgreSQL 16 Official Documentation. The PostgreSQL Global Development Group. URL: https://www.postgresql.org/docs/16/ (дата звернення: 10.11.2026).",
        "18. Rebollo J.J., Balakrishnan H. Characterization and prediction of air traffic delays // Transportation Research Part C: Emerging Technologies. 2014. Vol. 44. P. 231–241.",
        "19. Sternberg A., Soares J., Carvalho D., Ogasawara E. A Review on Flight Delay Prediction // IEEE Transactions on Intelligent Transportation Systems. 2017. Vol. 18, No. 11. P. 1–14.",
        "20. Технології й інфраструктура великих даних для аналізу даних: методичні рекомендації до виконання курсової роботи для здобувачів ОС «Магістр» спеціальності F3 «Комп’ютерні науки» / уклад. К.І. Прокопенко. Київ: НУ КАІ, 2026. 37 с.",
    ]
    for ref in references:
        add_body_p(doc, ref, indent_cm=1.25)

    # =========================================================================
    # ДОДАТОК А. Паспорт набору даних та специфікація таблиць сховища
    # =========================================================================
    add_appendix_heading(doc, "А", "Паспорт набору даних та специфікація таблиць аналітичного сховища")
    add_body_p(
        doc,
        "У таблиці А.1 наведено повний паспорт атрибутів вхідного масиву авіарейсів за схемою BTS_RAW_SCHEMA та розрахункових полів очищеного шару Silver комплексу «ПАКАПА».",
    )

    schema_headers = ["Назва поля (Column)", "Тип даних у PySpark / SQL", "Група атрибута", "Опис та фізичний зміст показника (одиниці вимірювання)"]
    schema_rows = [
        ["FL_DATE", "StringType / VARCHAR(16)", "Ключ рейсу", "Календарна дата виконання рейсу у форматі YYYY-MM-DD"],
        ["MONTH, DAY_OF_WEEK", "IntegerType / INT", "Часовий вимір", "Номер місяця (1..12) та порядковий день тижня (1 — Пн .. 7 — Нд)"],
        ["OP_CARRIER, FL_NUM", "StringType, IntegerType", "Ключ рейсу", "Дволітерний код ІАТА авіакомпанії-оператора та номер рейсу"],
        ["ORIGIN, DEST", "StringType / VARCHAR(8)", "Маршрут", "Трилітерні коди ІАТА аеропорту вильоту та аеропорту призначення"],
        ["CRS_DEP_TIME, DEP_HOUR", "IntegerType / INT", "Розклад", "Плановий час вильоту за розкладом (HHMM) та година доби (0..23)"],
        ["DEP_DELAY", "DoubleType / DOUBLE", "Таймінг", "Відхилення фактичного часу вильоту від планового розкладу, хв"],
        ["TAXI_OUT, TAXI_IN", "DoubleType / DOUBLE", "Наземний рух", "Тривалість наземного руління на зліт та руління після посадки до гейту, хв"],
        ["ARR_DELAY", "DoubleType / DOUBLE", "Цільова змінна", "Підсумкова затримка прибуття до гейту аеропорту призначення, хв"],
        ["CANCELLED, CANCELLATION_CODE", "IntegerType, StringType", "Статус рейсу", "Індикатор скасування рейсу (0/1) та код причини скасування (A/B/C/D)"],
        ["CRS_ELAPSED_TIME, ACTUAL_ELAPSED_TIME, AIR_TIME", "DoubleType / DOUBLE", "Тривалість", "Планова та фактична загальна тривалість рейсу і чистий час у повітрі, хв"],
        ["DISTANCE", "DoubleType / DOUBLE", "Геометрія", "Ортодромічна відстань між аеропортами вильоту і прильоту за Haversine, миль"],
        ["CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY", "DoubleType / DOUBLE", "5 чинників BTS", "Декомпозиція часу затримки прибуття за 5 канонічними причинами, хв"],
        ["PREV_AIRCRAFT_DELAY", "DoubleType / DOUBLE", "Каскадність", "Затримка повітряного судна, накопичена на попередньому оборотному рейсі, хв"],
        ["WEATHER_CONDITION, TEMP_C, WIND_SPEED_KTS, VISIBILITY_MILES, PRECIP_MM, WEATHER_SEVERITY_IDX", "StringType, DoubleType", "Метеофактори", "Погодний режим, температура (°C), швидкість вітру (вузлів), видимість (миль), опади (мм) та індекс суворості погоди (0..10)"],
        ["IS_DELAYED_15, IS_SEVERE_DELAY_60, ROUTE_ID, TIME_OF_DAY_BUCKET", "IntegerType, StringType", "Ознаки Silver", "Бінарні індикатори затримки >=15 хв та >=60 хв, код маршруту ORIGIN->DEST та часове вікно доби"],
    ]
    add_styled_table(
        doc,
        "А.1",
        "Паспорт атрибутів набору даних авіарейсів у шарах Bronze та Silver",
        schema_headers,
        schema_rows,
        [4.2, 3.3, 2.7, 7.3],
        font_size=10,
    )

    # =========================================================================
    # ДОДАТОК Б. Лістинги ключових програмних модулів конвеєра «ПАКАПА»
    # =========================================================================
    add_appendix_heading(doc, "Б", "Лістинги ключових програмних модулів конвеєра «ПАКАПА»")
    add_body_p(
        doc,
        "Нижче наведено вихідний код основних розроблених модулів програмно-аналітичного комплексу: ядра перевірки якості даних (src/dq/quality_engine.py), пакетного ETL-конвеєра Medallion (src/etl/medallion_processor.py), регресійного модуля Spark MLlib (src/ml/delay_regressor.py) та потокового обробника OpenSky Network / Apache Kafka (src/streaming/stream_processor.py).",
    )

    dq_code = (ROOT_DIR / "src" / "dq" / "quality_engine.py").read_text(encoding="utf-8")
    add_code_block(doc, "Лістинг Б.1 – Модуль багатофакторного контролю якості даних та карантину (src/dq/quality_engine.py)", dq_code)

    etl_code_lines = (ROOT_DIR / "src" / "etl" / "medallion_processor.py").read_text(encoding="utf-8").splitlines()
    add_code_block(
        doc,
        "Лістинг Б.2 – Модуль пакетного ETL-конвеєра PySpark за архітектурою Medallion (src/etl/medallion_processor.py, фрагмент ядра)",
        "\n".join(etl_code_lines[:250]),
    )

    ml_code_lines = (ROOT_DIR / "src" / "ml" / "delay_regressor.py").read_text(encoding="utf-8").splitlines()
    add_code_block(
        doc,
        "Лістинг Б.3 – Модуль регресійного прогнозування затримок та оцінки важливості чинників у Spark MLlib (src/ml/delay_regressor.py)",
        "\n".join(ml_code_lines[:215]),
    )

    stream_code_lines = (ROOT_DIR / "src" / "streaming" / "stream_processor.py").read_text(encoding="utf-8").splitlines()
    add_code_block(
        doc,
        "Лістинг Б.4 – Потоковий обробник подій OpenSky Network / Apache Kafka з онлайн ML-скорингом (src/streaming/stream_processor.py)",
        "\n".join(stream_code_lines[:145]),
    )

    # =========================================================================
    # ДОДАТОК В. Конфігурація середовища Docker Compose та результати автотестів
    # =========================================================================
    add_appendix_heading(doc, "В", "Конфігурація середовища Docker Compose та результати автотестів")
    compose_code = (ROOT_DIR / "docker-compose.yml").read_text(encoding="utf-8")
    add_code_block(doc, "Лістинг В.1 – Конфігурація контейнерної оркестрації комплексу (docker-compose.yml)", compose_code)

    summary_json = (ROOT_DIR / "data" / "lake" / "gold" / "latest_run_summary.json").read_text(encoding="utf-8")
    add_code_block(
        doc,
        "Лістинг В.2 – Підсумковий JSON-протокол контрольного запуску конвеєра (data/lake/gold/latest_run_summary.json)",
        summary_json,
    )


def generate_complete_docx():
    doc = Document()

    # Налаштування параметрів сторінки за розд. 7 методички (А4: ліве, верхнє, нижнє — 20 мм, праве — 10 мм)
    section = doc.sections[0]
    section.page_width = Mm(210)
    section.page_height = Mm(297)
    section.left_margin = Mm(20)
    section.top_margin = Mm(20)
    section.bottom_margin = Mm(20)
    section.right_margin = Mm(10)

    add_page_number_footer(section)

    build_front_matter(doc)
    build_intro_and_chapters_1_2(doc)
    build_chapters_3_4(doc)
    build_conclusions_refs_appendices(doc)

    OUT_DOCX_DOCS.parent.mkdir(parents=True, exist_ok=True)
    doc.save(str(OUT_DOCX_MAIN))
    doc.save(str(OUT_DOCX_DOCS))
    print("Пояснювальну записку успішно збережено у:")
    print("  1)", OUT_DOCX_MAIN)
    print("  2)", OUT_DOCX_DOCS)


if __name__ == "__main__":
    generate_complete_docx()
