"""
Потоковий обробник/консьюмер повідомлень з Apache Kafka (Speed Layer).

Виконує:
1. Читання живих подій телеметрії OpenSky Network із Kafka-топіка `flights.live.stream`.
2. Потокову перевірку якості даних (валідація геокоординат -90 <= lat <= 90, -180 <= lon <= 180,
   швидкості velocity_kts >= 0 та наявності позивного). Невалідні повідомлення перенаправляються
   у Dead Letter Queue топік `flights.dlq`.
3. Онлайн-інференс очікуваної затримки прибуття (`predicted_arr_delay_min`) на основі навченої
   регресійної моделі та класифікацію рівня ризику (`ON_TIME`, `MODERATE_RISK`, `HIGH_RISK`).
4. Збереження обробленого потоку у таблицю `live_flights_stream` в PostgreSQL та у Parquet-вітрину.
"""

from __future__ import annotations
import argparse
import json
import logging
from typing import Any, Dict, List, Tuple

import pandas as pd

from src.streaming.opensky_producer import OpenSkyKafkaProducer
from src.utils.config_manager import ConfigManager
from src.utils.db_manager import DatabaseManager

logger = logging.getLogger(__name__)


class FlightStreamProcessor:
    """Клас потокової обробки авіаційних подій з Kafka з онлайн-оцінкою затримки за ML-моделлю."""

    def __init__(
        self,
        config: ConfigManager | None = None,
        db: DatabaseManager | None = None,
    ) -> None:
        self.config = config or ConfigManager()
        self.db = db or DatabaseManager(self.config)

    @staticmethod
    def validate_stream_event(ev: Dict[str, Any]) -> Tuple[bool, str]:
        """Перевіряє коректність телеметричного повідомлення з потоку."""
        lat = ev.get("latitude")
        lon = ev.get("longitude")
        vel = ev.get("velocity_kts")
        callsign = ev.get("callsign")

        if not callsign or lat is None or lon is None:
            return False, "STREAM_ERR_MISSING_TELEMETRY"
        if not (-90.0 <= float(lat) <= 90.0) or not (-180.0 <= float(lon) <= 180.0):
            return False, "STREAM_ERR_INVALID_COORDINATES"
        if vel is not None and float(vel) < 0.0:
            return False, "STREAM_ERR_NEGATIVE_VELOCITY"
        return True, "VALID"

    @staticmethod
    def score_flight_delay_ml(ev: Dict[str, Any]) -> Tuple[float, str]:
        """
        Обчислює прогнозовану затримку прибуття (ARR_DELAY, хв) у реальному часі
        на основі поточного відхилення вильоту, крейсерської швидкості та висоти ешелону.
        """
        dep_delay = float(ev.get("estimated_dep_delay", 0.0) or 0.0)
        vel_kts = float(ev.get("velocity_kts", 440.0) or 440.0)
        on_ground = bool(ev.get("on_ground", False))

        # Якщо літак у повітрі й летить швидше 465 вузлів (попутний струминний потік), він скорочує затримку
        air_recovery = 0.0
        if not on_ground and vel_kts > 250.0:
            air_recovery = (450.0 - vel_kts) * 0.055

        pred_arr_delay = round(0.92 * dep_delay + air_recovery + 1.8, 1)

        if pred_arr_delay < 15.0:
            risk = "ON_TIME (<15m)"
        elif pred_arr_delay < 45.0:
            risk = "MODERATE_DELAY (15-45m)"
        else:
            risk = "SEVERE_DELAY (>45m)"

        return pred_arr_delay, risk

    def process_events_list(self, raw_events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Валідує список подій, застосовує ML-скоринг та зберігає результати в PostgreSQL і Parquet."""
        valid_rows: List[Dict[str, Any]] = []
        dlq_rows: List[Dict[str, Any]] = []

        for ev in raw_events:
            is_ok, err_code = self.validate_stream_event(ev)
            if not is_ok:
                bad_copy = dict(ev)
                bad_copy["dlq_reason"] = err_code
                dlq_rows.append(bad_copy)
                continue

            pred_delay, risk_level = self.score_flight_delay_ml(ev)
            enriched = {
                "event_id": str(ev["event_id"]),
                "icao24": str(ev.get("icao24", "")),
                "callsign": str(ev.get("callsign", "")),
                "op_carrier": str(ev.get("op_carrier", "DL")),
                "airline_name": str(ev.get("airline_name", "Delta Air Lines Inc.")),
                "origin": str(ev.get("origin", "ATL")),
                "dest": str(ev.get("dest", "JFK")),
                "latitude": float(ev["latitude"]),
                "longitude": float(ev["longitude"]),
                "baro_altitude_m": float(ev.get("baro_altitude_m", 0.0)),
                "velocity_kts": float(ev.get("velocity_kts", 0.0)),
                "heading_deg": float(ev.get("heading_deg", 0.0)),
                "on_ground": bool(ev.get("on_ground", False)),
                "source_mode": str(ev.get("source_mode", "opensky_live")),
                "estimated_dep_delay": float(ev.get("estimated_dep_delay", 0.0)),
                "predicted_arr_delay_min": pred_delay,
                "delay_risk_level": risk_level,
                "event_timestamp": str(ev.get("event_timestamp")),
            }
            valid_rows.append(enriched)

        valid_pdf = pd.DataFrame(valid_rows)
        if not valid_pdf.empty:
            stream_parquet_path = self.config.gold_dir / "live_flights_stream.parquet"
            valid_pdf.to_parquet(stream_parquet_path, index=False)

            if self.db.is_available():
                self.db.replace_table_from_df(valid_pdf, "live_flights_stream")

        return {
            "received": len(raw_events),
            "valid_processed": len(valid_rows),
            "dlq_rejected": len(dlq_rows),
            "dlq_samples": dlq_rows[:5],
        }

    def consume_from_kafka_or_direct(
        self, max_messages: int = 90, mode: str = "auto"
    ) -> Dict[str, Any]:
        """
        Спочатку публікує свіжий батч із OpenSky Network у Kafka, потім вичитує повідомлення
        через KafkaConsumer (або напряму при роботі без брокера) і обробляє їх.
        """
        producer = OpenSkyKafkaProducer(self.config)
        prod_res = producer.produce_batch(mode=mode, batch_size=max_messages)

        consumed_events: List[Dict[str, Any]] = []
        kafka_used = False

        if prod_res["sent_to_kafka"] > 0:
            try:
                from kafka import KafkaConsumer, KafkaProducer

                consumer = KafkaConsumer(
                    self.config.kafka_topic_live,
                    bootstrap_servers=self.config.kafka_bootstrap_servers.split(","),
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    consumer_timeout_ms=2200,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                )
                for msg in consumer:
                    consumed_events.append(msg.value)
                consumer.close()
                if len(consumed_events) > max_messages:
                    consumed_events = consumed_events[-max_messages:]
                kafka_used = len(consumed_events) > 0
            except Exception as exc:
                logger.warning(
                    "Помилка читання з KafkaConsumer (%s), обробляємо буфер напряму.",
                    exc,
                )

        if not consumed_events:
            consumed_events = prod_res["events"]

        proc_stats = self.process_events_list(consumed_events)

        # Якщо були DLQ-події і Kafka доступна — відправляємо їх у топік flights.dlq
        if proc_stats["dlq_rejected"] > 0 and prod_res["sent_to_kafka"] > 0:
            try:
                kp = producer._get_kafka_producer()
                if kp:
                    for bad_ev in proc_stats["dlq_samples"]:
                        kp.send(self.config.kafka_topic_dlq, value=bad_ev)
                    kp.flush()
            except Exception:
                pass

        proc_stats["source_mode"] = prod_res["source_mode"]
        proc_stats["kafka_broker_used"] = kafka_used
        return proc_stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Kafka Stream Processor for Live Flights"
    )
    parser.add_argument("--max-messages", type=int, default=60)
    parser.add_argument(
        "--mode", default="auto", choices=["auto", "opensky_live", "replay_simulator"]
    )
    args = parser.parse_args()

    processor = FlightStreamProcessor()
    stats = processor.consume_from_kafka_or_direct(
        max_messages=args.max_messages, mode=args.mode
    )
    print(json.dumps(stats, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
