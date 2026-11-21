"""
Модуль формування/завантаження набору даних US DOT Bureau of Transportation Statistics (BTS)
On-Time Performance та довідників авіакомпаній і аеропортів США.

Забезпечує:
1. Реальні довідники 18 авіакомпаній США (dim_airlines) та 45 найбільших аеропортів-хабів (dim_airports)
   із точними координатами WGS84, містом, штатом, кліматичною зоною та базовим часом руління.
2. Повномасштабний масив рейсів (за замовчуванням 200 000 записів) у форматі US DOT BTS з усіма
   канонічними полями таймінгів, 5 причинами затримок (CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY,
   SECURITY_DELAY, LATE_AIRCRAFT_DELAY) та метеорологічними показниками.
3. Контрольовану інжекцію «брудних» та аномальних записів (~2.2% обсягу) для перевірки та демонстрації
   роботи модуля контролю якості даних (Data Quality Engine) і карантинного сховища (Quarantine Layer).
"""
from __future__ import annotations
import math
import time
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from src.utils.config_manager import ConfigManager

# Світові авіакомпанії (Європа, Північна Америка, Азія, Близький Схід, Латинська Америка, Океанія)
AIRLINES_DATA: List[Tuple[str, str, str, str, float]] = [
    # Європа (Europe)
    ("LH", "Lufthansa (Deutsche Lufthansa AG)", "Star Alliance", "Mainline Narrow/Wide", 82.6),
    ("FR", "Ryanair DAC", "European Low-Cost", "Boeing 737-800/MAX", 84.1),
    ("BA", "British Airways Plc", "Oneworld", "Mainline Narrow/Wide", 79.8),
    ("AF", "Air France", "SkyTeam", "Mainline Narrow/Wide", 80.4),
    ("KL", "KLM Royal Dutch Airlines", "SkyTeam", "Mainline Narrow/Wide", 83.2),
    ("TK", "Turkish Airlines (THY)", "Star Alliance", "Global Mega-Carrier", 81.9),
    ("W6", "Wizz Air Hungary Ltd.", "European Ultra Low-Cost", "Airbus A320/A321neo", 77.5),
    ("LO", "LOT Polish Airlines", "Star Alliance", "Embraer / B737 / B787", 83.7),
    ("LX", "SWISS International Air Lines", "Star Alliance", "Airbus / Boeing Wide", 84.8),
    ("IB", "Iberia Lineas Aereas de Espana", "Oneworld", "Airbus A320/A350", 85.9),
    # Північна Америка (North America)
    ("DL", "Delta Air Lines Inc.", "SkyTeam", "Mainline Narrow/Wide", 84.2),
    ("UA", "United Air Lines Inc.", "Star Alliance", "Mainline Narrow/Wide", 80.5),
    ("AA", "American Airlines Inc.", "Oneworld", "Mainline Narrow/Wide", 78.9),
    ("WN", "Southwest Airlines Co.", "Low-Cost Point-to-Point", "Boeing 737 Fleet", 79.4),
    ("AC", "Air Canada", "Star Alliance", "Mainline Narrow/Wide", 76.8),
    ("B6", "JetBlue Airways", "Independent / Hybrid", "Airbus A320/A220", 74.2),
    # Близький Схід та Азійсько-Тихоокеанський регіон (Middle East & Asia-Pacific)
    ("EK", "Emirates Airline", "Independent Global", "Airbus A380 / Boeing 777", 85.4),
    ("QR", "Qatar Airways", "Oneworld", "Airbus A350 / Boeing 777/787", 86.1),
    ("SQ", "Singapore Airlines", "Star Alliance", "Widebody Long-Haul", 87.3),
    ("NH", "All Nippon Airways (ANA)", "Star Alliance", "Mainline Narrow/Wide", 88.5),
    ("JL", "Japan Airlines (JAL)", "Oneworld", "Mainline Narrow/Wide", 88.1),
    ("CX", "Cathay Pacific Airways", "Oneworld", "Widebody Long-Haul", 82.0),
    # Латинська Америка та Океанія (LatAm & Oceania)
    ("LA", "LATAM Airlines Group", "Independent Global", "Airbus A320 / Boeing 787", 83.9),
    ("QF", "Qantas Airways Ltd.", "Oneworld", "Mainline Narrow/Wide", 81.2),
]

# Глобальна мережа аеропортів-хабів світу (STATE містить "Регіон (ISO)")
AIRPORTS_DATA: List[Tuple[str, str, str, str, float, float, str, str, float]] = [
    # Європа (Europe)
    ("LHR", "London Heathrow International", "London", "Europe (UK)", 51.4700, -0.4543, "Mega-Hub", "Marine West Coast", 24.5),
    ("CDG", "Paris Charles de Gaulle Airport", "Paris", "Europe (FR)", 49.0097, 2.5479, "Mega-Hub", "Temperate Oceanic", 22.8),
    ("FRA", "Frankfurt am Main International", "Frankfurt", "Europe (DE)", 50.0379, 8.5622, "Mega-Hub", "Central European", 21.9),
    ("AMS", "Amsterdam Airport Schiphol", "Amsterdam", "Europe (NL)", 52.3105, 4.7683, "Mega-Hub", "Marine Fog-Prone", 23.4),
    ("IST", "Istanbul International Airport", "Istanbul", "Europe (TR)", 41.2753, 28.7519, "Mega-Hub", "Mediterranean", 21.2),
    ("MAD", "Adolfo Suarez Madrid-Barajas", "Madrid", "Europe (ES)", 40.4983, -3.5676, "Major-Hub", "Mediterranean Continental", 18.6),
    ("MUC", "Munich Franz Josef Strauss Intl", "Munich", "Europe (DE)", 48.3537, 11.7750, "Major-Hub", "Alpine Continental", 17.8),
    ("FCO", "Rome Leonardo da Vinci-Fiumicino", "Rome", "Europe (IT)", 41.8003, 12.2389, "Major-Hub", "Mediterranean Coastal", 18.9),
    ("BCN", "Josep Tarradellas Barcelona-El Prat", "Barcelona", "Europe (ES)", 41.2974, 2.0833, "Major-Hub", "Mediterranean Coastal", 17.4),
    ("ZRH", "Zurich Kloten Airport", "Zurich", "Europe (CH)", 47.4582, 8.5555, "Major-Hub", "Alpine Snowy", 17.2),
    ("VIE", "Vienna International Airport", "Vienna", "Europe (AT)", 48.1103, 16.5697, "Major-Hub", "Central European", 15.8),
    ("WAW", "Warsaw Chopin Airport", "Warsaw", "Europe (PL)", 52.1672, 20.9679, "Major-Hub", "Continental Snowy", 16.1),
    ("DUB", "Dublin International Airport", "Dublin", "Europe (IE)", 53.4264, -6.2499, "Major-Hub", "Marine West Coast", 18.5),
    ("CPH", "Copenhagen Kastrup Airport", "Copenhagen", "Europe (DK)", 55.6180, 12.6508, "Medium-Hub", "Nordic Maritime", 15.2),
    ("OSL", "Oslo Gardermoen Airport", "Oslo", "Europe (NO)", 60.1976, 11.1004, "Medium-Hub", "Continental Snowy", 16.4),
    ("ARN", "Stockholm Arlanda Airport", "Stockholm", "Europe (SE)", 59.6498, 17.9238, "Medium-Hub", "Continental Snowy", 15.9),
    ("LIS", "Humberto Delgado Airport", "Lisbon", "Europe (PT)", 38.7756, -9.1354, "Major-Hub", "Atlantic Coastal", 18.1),
    ("ATH", "Athens Eleftherios Venizelos Intl", "Athens", "Europe (GR)", 37.9364, 23.9445, "Medium-Hub", "Mediterranean", 15.6),
    ("PRG", "Vaclav Havel Airport Prague", "Prague", "Europe (CZ)", 50.1008, 14.2600, "Medium-Hub", "Central European", 14.7),
    ("BUD", "Budapest Ferenc Liszt International", "Budapest", "Europe (HU)", 47.4369, 19.2556, "Medium-Hub", "Central European", 14.9),
    # Близький Схід (Middle East)
    ("DXB", "Dubai International Airport", "Dubai", "Middle East (AE)", 25.2532, 55.3657, "Mega-Hub", "Arid Desert", 23.1),
    ("DOH", "Hamad International Airport", "Doha", "Middle East (QA)", 25.2731, 51.6081, "Mega-Hub", "Arid Desert", 19.4),
    # Азія та Тихоокеанський регіон (Asia-Pacific)
    ("HND", "Tokyo Haneda International", "Tokyo", "Asia-Pacific (JP)", 35.5494, 139.7798, "Mega-Hub", "Humid Subtropical", 19.2),
    ("SIN", "Singapore Changi Airport", "Singapore", "Asia-Pacific (SG)", 1.3644, 103.9915, "Mega-Hub", "Tropical Monsoon", 18.5),
    ("ICN", "Incheon International Airport", "Seoul", "Asia-Pacific (KR)", 37.4602, 126.4407, "Mega-Hub", "Continental Monsoon", 19.0),
    ("HKG", "Hong Kong International Airport", "Hong Kong", "Asia-Pacific (HK)", 22.3080, 113.9185, "Mega-Hub", "Tropical Convective", 20.3),
    ("BKK", "Suvarnabhumi International", "Bangkok", "Asia-Pacific (TH)", 13.6900, 100.7501, "Major-Hub", "Tropical Monsoon", 18.2),
    ("DEL", "Indira Gandhi International", "New Delhi", "Asia-Pacific (IN)", 28.5562, 77.1000, "Mega-Hub", "Subtropical Fog-Prone", 21.6),
    ("SYD", "Sydney Kingsford Smith Intl", "Sydney", "Oceania (AU)", -33.9399, 151.1753, "Major-Hub", "Oceanic Subtropical", 17.7),
    ("MEL", "Melbourne Tullamarine Airport", "Melbourne", "Oceania (AU)", -37.6690, 144.8410, "Major-Hub", "Temperate Oceanic", 16.5),
    # Північна Америка (North America)
    ("ATL", "Hartsfield-Jackson Atlanta Intl", "Atlanta", "North America (US)", 33.6407, -84.4277, "Mega-Hub", "Humid Subtropical", 19.5),
    ("DFW", "Dallas/Fort Worth International", "Dallas", "North America (US)", 32.8998, -97.0403, "Mega-Hub", "Subtropical Steppe", 18.2),
    ("DEN", "Denver International Airport", "Denver", "North America (US)", 39.8561, -104.6737, "Mega-Hub", "Alpine Semi-Arid", 17.8),
    ("ORD", "Chicago O'Hare International", "Chicago", "North America (US)", 41.9742, -87.9073, "Mega-Hub", "Continental Snowy", 22.4),
    ("LAX", "Los Angeles International", "Los Angeles", "North America (US)", 33.9416, -118.4085, "Mega-Hub", "Mediterranean Coastal", 18.9),
    ("JFK", "John F. Kennedy International", "New York", "North America (US)", 40.6413, -73.7781, "Mega-Hub", "Northeast Coastal", 26.8),
    ("MIA", "Miami International Airport", "Miami", "North America (US)", 25.7959, -80.2870, "Major-Hub", "Tropical Monsoon", 19.3),
    ("SFO", "San Francisco International", "San Francisco", "North America (US)", 37.6213, -122.3790, "Major-Hub", "Marine Fog-Prone", 20.8),
    ("SEA", "Seattle-Tacoma International", "Seattle", "North America (US)", 47.4502, -122.3088, "Major-Hub", "Marine West Coast", 18.4),
    ("YYZ", "Toronto Pearson International", "Toronto", "North America (CA)", 43.6777, -79.6248, "Mega-Hub", "Continental Snowy", 21.1),
    ("YVR", "Vancouver International Airport", "Vancouver", "North America (CA)", 49.1967, -123.1815, "Major-Hub", "Marine West Coast", 17.6),
    ("MEX", "Mexico City Benito Juarez Intl", "Mexico City", "Latin America (MX)", 19.4361, -99.0719, "Mega-Hub", "Highland Subtropical", 22.0),
    # Південна Америка та Африка (Latin America & Africa)
    ("GRU", "Sao Paulo-Guarulhos International", "Sao Paulo", "Latin America (BR)", -23.4356, -46.4731, "Mega-Hub", "Humid Subtropical", 20.4),
    ("BOG", "El Dorado International Airport", "Bogota", "Latin America (CO)", 4.7016, -74.1469, "Major-Hub", "Andean Highland", 19.8),
    ("EZE", "Ministro Pistarini International", "Buenos Aires", "Latin America (AR)", -34.8222, -58.5358, "Major-Hub", "Humid Pampas", 16.9),
    ("JNB", "O.R. Tambo International Airport", "Johannesburg", "Africa (ZA)", -26.1367, 28.2411, "Major-Hub", "Highveld Subtropical", 17.3),
    ("CAI", "Cairo International Airport", "Cairo", "Africa (EG)", 30.1219, 31.4056, "Major-Hub", "Arid Desert", 18.0),
]


def haversine_miles(lat1: np.ndarray, lon1: np.ndarray, lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    """Обчислює ортодромічну відстань між аеропортами у сухопутних милях (стандарт US DOT)."""
    r_miles = 3958.8
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2.0) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2.0) ** 2
    return 2.0 * r_miles * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a))


class BTSDatasetLoader:
    """Генератор та завантажувач масиву даних US DOT BTS On-Time Performance із метеофакторами."""

    def __init__(self, config: ConfigManager | None = None) -> None:
        self.config = config or ConfigManager()

    def build_reference_tables(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Формує та зберігає довідники авіакомпаній (airlines.csv) та аеропортів (airports.csv)."""
        airlines_df = pd.DataFrame(
            AIRLINES_DATA,
            columns=["IATA_CODE", "AIRLINE_NAME", "ALLIANCE", "FLEET_TYPE", "BASE_OTP_RATING"],
        )
        airports_df = pd.DataFrame(
            AIRPORTS_DATA,
            columns=[
                "IATA_CODE",
                "AIRPORT_NAME",
                "CITY",
                "STATE",
                "LATITUDE",
                "LONGITUDE",
                "HUB_TIER",
                "CLIMATE_ZONE",
                "AVG_TAXI_OUT_BASE",
            ],
        )

        airlines_path = self.config.raw_dir / "airlines.csv"
        airports_path = self.config.raw_dir / "airports.csv"
        airlines_df.to_csv(airlines_path, index=False)
        airports_df.to_csv(airports_path, index=False)
        return airlines_df, airports_df

    def generate_bts_flights_dataset(
        self,
        num_records: int = 200_000,
        seed: int = 42,
        force_regenerate: bool = False,
    ) -> Dict[str, any]:
        """
        Формує репрезентативний масив рейсів за стандартом US DOT BTS з метеорологічними
        чинниками та контрольованими помилками для шару Quarantine.
        """
        t0 = time.perf_counter()
        airlines_df, airports_df = self.build_reference_tables()
        flights_csv_path = self.config.raw_dir / "flights_bts_raw.csv"

        if flights_csv_path.exists() and not force_regenerate:
            existing_rows = sum(1 for _ in open(flights_csv_path, "r", encoding="utf-8")) - 1
            if existing_rows >= int(num_records * 0.9):
                return {
                    "flights_csv": str(flights_csv_path),
                    "rows_generated": existing_rows,
                    "cached": True,
                    "duration_sec": round(time.perf_counter() - t0, 3),
                }

        rng = np.random.default_rng(seed)

        # Ваги популярності авіакомпаній та аеропортів (реалістичний розподіл Парето для світових мега-хабів)
        carrier_codes = airlines_df["IATA_CODE"].to_numpy()
        n_carriers = len(carrier_codes)
        carrier_weights = np.linspace(2.6, 0.7, n_carriers) ** 1.15
        carrier_weights /= carrier_weights.sum()

        airport_codes = airports_df["IATA_CODE"].to_numpy()
        n_airports = len(airport_codes)
        ap_weights = np.linspace(3.2, 0.8, n_airports) ** 1.25
        ap_weights /= ap_weights.sum()

        # 1. Вибір дат протягом усього 2025 року (з сезонними піками влітку та в грудні)
        days_offset = rng.integers(0, 365, size=num_records)
        base_dates = pd.to_datetime("2025-01-01") + pd.to_timedelta(days_offset, unit="D")
        months = base_dates.month.to_numpy()
        days_of_week = base_dates.dayofweek.to_numpy() + 1  # 1=Пн .. 7=Нд

        # 2. Перевізники та номери рейсів
        op_carrier = rng.choice(carrier_codes, size=num_records, p=carrier_weights)
        fl_num = rng.integers(100, 6999, size=num_records)

        # 3. Маршрути (ORIGIN != DEST)
        origin_idx = rng.choice(n_airports, size=num_records, p=ap_weights)
        dest_idx = rng.choice(n_airports, size=num_records, p=ap_weights)
        same_mask = origin_idx == dest_idx
        dest_idx[same_mask] = (dest_idx[same_mask] + rng.integers(1, n_airports, size=same_mask.sum())) % n_airports

        origin = airport_codes[origin_idx]
        dest = airport_codes[dest_idx]

        # Геометрія маршрутів та базовий час руління
        orig_lat = airports_df["LATITUDE"].to_numpy()[origin_idx]
        orig_lon = airports_df["LONGITUDE"].to_numpy()[origin_idx]
        dest_lat = airports_df["LATITUDE"].to_numpy()[dest_idx]
        dest_lon = airports_df["LONGITUDE"].to_numpy()[dest_idx]
        orig_taxi_base = airports_df["AVG_TAXI_OUT_BASE"].to_numpy()[origin_idx]
        orig_climate = airports_df["CLIMATE_ZONE"].to_numpy()[origin_idx]
        orig_hub = airports_df["HUB_TIER"].to_numpy()[origin_idx]

        distance = np.round(haversine_miles(orig_lat, orig_lon, dest_lat, dest_lon), 0)
        distance = np.clip(distance, 65.0, 9800.0)

        # 4. Час вильоту за розкладом (CRS_DEP_TIME: від 05:00 до 23:00 з ранковим і вечірнім піками)
        hour_probs = np.array([
            0.005, 0.003, 0.002, 0.002, 0.008,  # 00..04
            0.045, 0.072, 0.080, 0.078, 0.068,  # 05..09
            0.062, 0.060, 0.062, 0.064, 0.066,  # 10..14
            0.068, 0.072, 0.075, 0.068, 0.055,  # 15..19
            0.042, 0.026, 0.012, 0.005          # 20..23
        ])
        hour_probs /= hour_probs.sum()
        dep_hour = rng.choice(np.arange(24), size=num_records, p=hour_probs)
        dep_minute = rng.choice([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55], size=num_records)
        crs_dep_time = dep_hour * 100 + dep_minute

        # 5. Метеорологічні умови у вузлі вильоту (Weather Enrichment)
        is_winter = np.isin(months, [12, 1, 2])
        is_summer_storm = np.isin(months, [6, 7, 8])
        is_snow_zone = np.isin(orig_climate, ["Continental Snowy", "Alpine Semi-Arid", "Northeast Coastal"])
        is_tropical = np.isin(orig_climate, ["Tropical Convective", "Tropical Monsoon", "Gulf Subtropical"])

        weather_roll = rng.random(num_records)
        weather_cond = np.full(num_records, "Clear", dtype=object)
        weather_cond[weather_roll > 0.62] = "Cloudy"
        weather_cond[weather_roll > 0.80] = "Rain"
        weather_cond[(weather_roll > 0.90) & is_tropical & is_summer_storm] = "Thunderstorm"
        weather_cond[(weather_roll > 0.91) & is_snow_zone & is_winter] = "Snow / Ice"
        weather_cond[(weather_roll > 0.94) & (orig_climate == "Marine Fog-Prone")] = "Dense Fog"
        weather_cond[(weather_roll > 0.96) & (weather_cond == "Rain")] = "Thunderstorm"

        # Кількісні метеопоказники
        temp_c = np.round(
            18.0
            - 14.0 * is_winter.astype(float)
            + 9.0 * is_summer_storm.astype(float)
            + rng.normal(0, 5.5, size=num_records),
            1,
        )
        wind_speed_kts = np.round(
            np.clip(
                rng.gamma(shape=2.4, scale=4.2, size=num_records)
                + np.where(np.isin(weather_cond, ["Thunderstorm", "Snow / Ice"]), 14.0, 0.0),
                2.0,
                58.0,
            ),
            1,
        )
        visibility_miles = np.round(
            np.where(
                weather_cond == "Clear",
                10.0,
                np.where(
                    weather_cond == "Cloudy",
                    rng.uniform(7.0, 10.0, size=num_records),
                    np.where(
                        weather_cond == "Rain",
                        rng.uniform(3.0, 7.5, size=num_records),
                        rng.uniform(0.5, 3.5, size=num_records),
                    ),
                ),
            ),
            1,
        )
        precip_mm = np.round(
            np.where(
                np.isin(weather_cond, ["Clear", "Cloudy", "Dense Fog"]),
                0.0,
                rng.exponential(scale=6.5, size=num_records),
            ),
            1,
        )

        # Індекс суворості погоди (0..10) для аналітики та ML
        weather_severity_idx = np.round(
            np.clip(
                (10.0 - visibility_miles) * 0.45
                + (wind_speed_kts / 10.0) * 0.85
                + np.where(weather_cond == "Thunderstorm", 3.5, 0.0)
                + np.where(weather_cond == "Snow / Ice", 3.8, 0.0)
                + np.where(weather_cond == "Dense Fog", 2.6, 0.0),
                0.0,
                10.0,
            ),
            2,
        )

        # 6. Каскадна затримка попереднього рейсу того самого борту (PREV_AIRCRAFT_DELAY)
        # Накопичується протягом дня (особливо після 14:00)
        evening_cascade_factor = np.clip((dep_hour - 7) / 14.0, 0.05, 1.25)
        has_prev_delay = rng.random(num_records) < (0.12 + 0.24 * evening_cascade_factor)
        prev_aircraft_delay = np.where(
            has_prev_delay,
            np.round(rng.gamma(shape=1.9, scale=16.5, size=num_records) * evening_cascade_factor, 1),
            0.0,
        )

        # 7. Генерація 5 канонічних причин затримок US DOT BTS та DEP_DELAY / ARR_DELAY
        carrier_otp_map = dict(zip(airlines_df["IATA_CODE"], airlines_df["BASE_OTP_RATING"]))
        carrier_base_rating = np.array([carrier_otp_map[c] for c in op_carrier])
        carrier_risk_mult = (90.0 - carrier_base_rating) / 12.0  # ~0.4 .. 1.5

        hub_congestion = np.where(orig_hub == "Mega-Hub", 1.35, np.where(orig_hub == "Major-Hub", 1.12, 0.85))

        # Складові затримки (генеруються узгоджено з фізичними чинниками)
        late_aircraft_comp = np.round(prev_aircraft_delay * rng.uniform(0.68, 0.96, size=num_records), 1)

        weather_comp = np.where(
            weather_severity_idx > 2.2,
            np.round((weather_severity_idx ** 1.65) * rng.uniform(1.4, 3.2, size=num_records), 1),
            0.0,
        )

        nas_comp = np.where(
            (hub_congestion > 1.0) & ((weather_severity_idx > 1.5) | (np.isin(dep_hour, [7, 8, 9, 16, 17, 18, 19]))),
            np.round(rng.gamma(shape=1.6, scale=8.5, size=num_records) * hub_congestion * (0.5 + 0.15 * weather_severity_idx), 1),
            0.0,
        )
        # Не всі рейси у годину пік затримуються через NAS
        nas_comp = np.where(rng.random(num_records) < 0.32, nas_comp, 0.0)

        carrier_comp = np.where(
            rng.random(num_records) < (0.14 * carrier_risk_mult),
            np.round(rng.gamma(shape=1.7, scale=14.0, size=num_records) * carrier_risk_mult, 1),
            0.0,
        )

        security_comp = np.where(
            rng.random(num_records) < 0.008,
            np.round(rng.gamma(shape=1.5, scale=12.0, size=num_records), 1),
            0.0,
        )

        total_cause_delay = late_aircraft_comp + weather_comp + nas_comp + carrier_comp + security_comp

        # Час руління на виліт (TAXI_OUT) та приліт (TAXI_IN)
        taxi_out = np.round(
            orig_taxi_base
            + 0.25 * nas_comp
            + 1.2 * weather_severity_idx
            + rng.normal(0.0, 3.2, size=num_records),
            1,
        )
        taxi_out = np.clip(taxi_out, 6.0, 165.0)
        taxi_in = np.round(np.clip(rng.gamma(shape=2.6, scale=3.3, size=num_records), 3.0, 55.0), 1)

        # Плановий та фактичний час у повітрі
        crs_air_time = np.round(distance / 7.4 + 18.0, 1)
        air_time = np.round(
            crs_air_time + rng.normal(-1.5, 4.5, size=num_records) + 0.12 * weather_severity_idx,
            1,
        )
        air_time = np.clip(air_time, 14.0, 1180.0)

        crs_elapsed_time = np.round(crs_air_time + orig_taxi_base + 8.5, 1)
        actual_elapsed_time = np.round(air_time + taxi_out + taxi_in, 1)

        # Затримка вильоту (DEP_DELAY): вчасні рейси можуть вилітати на 1..7 хв раніше розкладу
        early_jitter = rng.normal(-2.8, 3.8, size=num_records)
        dep_delay = np.round(
            np.where(
                total_cause_delay >= 5.0,
                total_cause_delay - 0.35 * nas_comp + rng.normal(0.0, 2.5, size=num_records),
                early_jitter,
            ),
            1,
        )

        # Затримка прибуття (ARR_DELAY): залежить від DEP_DELAY та різниці часу руління/польоту
        arr_delay = np.round(dep_delay + (actual_elapsed_time - crs_elapsed_time), 1)

        # За стандартом US DOT BTS: детальні поля 5 причин заповнюються лише тоді, коли ARR_DELAY >= 15 хв
        is_delayed_15 = arr_delay >= 15.0

        # Узгодження суми 5 причин із фактичним ARR_DELAY для затриманих рейсів
        raw_sum = np.maximum(total_cause_delay, 1.0)
        scale_factor = np.where(is_delayed_15, np.maximum(arr_delay, 15.0) / raw_sum, 0.0)

        carrier_delay = np.where(is_delayed_15, np.round(carrier_comp * scale_factor, 1), 0.0)
        weather_delay = np.where(is_delayed_15, np.round(weather_comp * scale_factor, 1), 0.0)
        nas_delay = np.where(is_delayed_15, np.round(nas_comp * scale_factor, 1), 0.0)
        security_delay = np.where(is_delayed_15, np.round(security_comp * scale_factor, 1), 0.0)
        late_aircraft_delay = np.where(
            is_delayed_15,
            np.round(np.maximum(0.0, arr_delay - (carrier_delay + weather_delay + nas_delay + security_delay)), 1),
            0.0,
        )

        # Статус скасування (CANCELLED: ~1.2% рейсів, частіше при штормі/снігопаді)
        cancel_prob = 0.006 + np.where(weather_severity_idx > 6.5, 0.06, 0.0)
        cancelled = (rng.random(num_records) < cancel_prob).astype(int)
        cancellation_code = np.where(
            cancelled == 1,
            np.where(weather_severity_idx > 5.0, "B", rng.choice(["A", "C"], size=num_records)),
            "",
        )

        df = pd.DataFrame(
            {
                "FL_DATE": base_dates.strftime("%Y-%m-%d"),
                "MONTH": months,
                "DAY_OF_WEEK": days_of_week,
                "OP_CARRIER": op_carrier,
                "FL_NUM": fl_num,
                "ORIGIN": origin,
                "DEST": dest,
                "CRS_DEP_TIME": crs_dep_time,
                "DEP_HOUR": dep_hour,
                "DEP_DELAY": dep_delay,
                "TAXI_OUT": taxi_out,
                "TAXI_IN": taxi_in,
                "ARR_DELAY": arr_delay,
                "CANCELLED": cancelled,
                "CANCELLATION_CODE": cancellation_code,
                "CRS_ELAPSED_TIME": crs_elapsed_time,
                "ACTUAL_ELAPSED_TIME": actual_elapsed_time,
                "AIR_TIME": air_time,
                "DISTANCE": distance,
                "CARRIER_DELAY": carrier_delay,
                "WEATHER_DELAY": weather_delay,
                "NAS_DELAY": nas_delay,
                "SECURITY_DELAY": security_delay,
                "LATE_AIRCRAFT_DELAY": late_aircraft_delay,
                "PREV_AIRCRAFT_DELAY": prev_aircraft_delay,
                "WEATHER_CONDITION": weather_cond,
                "TEMP_C": temp_c,
                "WIND_SPEED_KTS": wind_speed_kts,
                "VISIBILITY_MILES": visibility_miles,
                "PRECIP_MM": precip_mm,
                "WEATHER_SEVERITY_IDX": weather_severity_idx,
            }
        )

        # 8. Інжекція контрольованих помилок та аномалій для тестування модуля Data Quality & Quarantine
        df = self._inject_data_quality_anomalies(df, rng)

        df.to_csv(flights_csv_path, index=False)
        duration = round(time.perf_counter() - t0, 3)
        return {
            "flights_csv": str(flights_csv_path),
            "rows_generated": len(df),
            "cached": False,
            "duration_sec": duration,
        }

    def _inject_data_quality_anomalies(self, df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        """
        Інжектує 6 реалістичних типів аномалій і порушень цілісності у сирий потік даних
        (сумарно ~2.1% записів), щоб модуль Data Quality класифікував і відправив їх у Quarantine.
        """
        n = len(df)
        indices = rng.permutation(n)

        # Тип 1: Пропущені ключові ідентифікатори (NULL ORIGIN або OP_CARRIER) - 0.35%
        c1 = int(n * 0.0035)
        idx1 = indices[0:c1]
        df.loc[idx1[: c1 // 2], "ORIGIN"] = None
        df.loc[idx1[c1 // 2 :], "OP_CARRIER"] = None

        # Тип 2: Невалідний / неіснуючий код аеропорту ІАТА (напр., "ZZZ", "999", "XX1") - 0.35%
        c2 = int(n * 0.0035)
        idx2 = indices[c1 : c1 + c2]
        df.loc[idx2, "DEST"] = rng.choice(["ZZZ", "999", "XX1", "BAD"], size=len(idx2))

        # Тип 3: Фізично неможливий від'ємний або нульовий час польоту (ACTUAL_ELAPSED_TIME <= 0) - 0.40%
        c3 = int(n * 0.0040)
        idx3 = indices[c1 + c2 : c1 + c2 + c3]
        df.loc[idx3, "ACTUAL_ELAPSED_TIME"] = rng.uniform(-95.0, 0.0, size=len(idx3)).round(1)

        # Тип 4: Помилкова відстань маршруту (DISTANCE <= 0 або > 10000 миль) - 0.30%
        c4 = int(n * 0.0030)
        idx4 = indices[c1 + c2 + c3 : c1 + c2 + c3 + c4]
        df.loc[idx4, "DISTANCE"] = rng.choice([-150.0, 0.0, 14500.0], size=len(idx4))

        # Тип 5: Екстремальний викид затримки (> 1440 хв / > 24 годин — збій телеметрії) - 0.35%
        c5 = int(n * 0.0035)
        idx5 = indices[c1 + c2 + c3 + c4 : c1 + c2 + c3 + c4 + c5]
        df.loc[idx5, "ARR_DELAY"] = rng.uniform(1600.0, 4200.0, size=len(idx5)).round(1)

        # Тип 6: Повні дублікати записів телеметрії - 0.35%
        c6 = int(n * 0.0035)
        idx6 = indices[c1 + c2 + c3 + c4 + c5 : c1 + c2 + c3 + c4 + c5 + c6]
        duplicates = df.iloc[idx6].copy()
        df_combined = pd.concat([df, duplicates], ignore_index=True)

        return df_combined
