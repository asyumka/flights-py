"""
Гібридний потоковий продюсер (OpenSky Network REST API + Fallback Replay Simulator -> Apache Kafka).

Реалізує вимоги потокового шару Lambda-архітектури:
1. У режимі `opensky_live` (або `auto`) виконує HTTP-запит до публічного ендпоінту
   `https://opensky-network.org/api/states/all` (граничний прямокутник континентальної частини США),
   отримує реальні телеметричні вектори повітряних суден у небі (icao24, callsign, координати,
   барометричну висоту, шляхову швидкість, курс), зіставляє позивні ICAO/IATA з авіакомпаніями США
   та публікує JSON-події у топік Apache Kafka `flights.live.stream`.
2. Якщо OpenSky API тимчасово недоступний або перевищено анонімний rate-limit, автоматично
   перемикається у режим `replay_simulator` (відтворення реалістичних траєкторій рейсів між хабами США).
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests

from src.ingestion.dataset_loader import AIRLINES_DATA, AIRPORTS_DATA
from src.utils.config_manager import ConfigManager

logger = logging.getLogger(__name__)

# Відображення 3-літерних ICAO-префіксів позивних OpenSky на 2-літерні коди IATA світових авіакомпаній
ICAO_TO_IATA_CARRIER: Dict[str, str] = {
    # Європа
    "DLH": "LH",
    "RYR": "FR",
    "BAW": "BA",
    "AFR": "AF",
    "KLM": "KL",
    "THY": "TK",
    "WZZ": "W6",
    "LOT": "LO",
    "SWR": "LX",
    "IBE": "IB",
    # Північна Америка
    "DAL": "DL",
    "UAL": "UA",
    "AAL": "AA",
    "SWA": "WN",
    "ACA": "AC",
    "JBU": "B6",
    # Близький Схід та Азія
    "UAE": "EK",
    "QTR": "QR",
    "SIA": "SQ",
    "ANA": "NH",
    "JAL": "JL",
    "CPA": "CX",
    # Латинська Америка та Океанія
    "LAN": "LA",
    "TAM": "LA",
    "QFA": "QF",
}


class OpenSkyKafkaProducer:
    """Потоковий генератор/конектор авіаційних подій OpenSky Network у брокер Apache Kafka."""

    def __init__(self, config: ConfigManager | None = None) -> None:
        self.config = config or ConfigManager()
        self.airlines_map = {code: name for code, name, _, _, _ in AIRLINES_DATA}
        self.airports = AIRPORTS_DATA
        self._producer = None

    def _get_kafka_producer(self):
        if self._producer is not None:
            return self._producer
        try:
            from kafka import KafkaProducer

            self._producer = KafkaProducer(
                bootstrap_servers=self.config.kafka_bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
                    "utf-8"
                ),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                request_timeout_ms=6000,
            )
            return self._producer
        except Exception as exc:
            logger.warning(
                "Kafka брокер недоступний (%s), працюємо у локальному буферному режимі.",
                exc,
            )
            return None

    def fetch_opensky_live_states(self, limit: int = 85) -> List[Dict[str, Any]]:
        """
        Опитує живий REST API OpenSky Network для глобального повітряного простору (весь світ),
        стратифікуючи вибірку за регіонами (Європа, Північна Америка, Азія, Близький Схід, Південна півкуля).
        """
        url = self.config.raw_config.get("streaming", {}).get(
            "opensky_api_url", "https://opensky-network.org/api/states/all"
        )
        auth = None
        user = os.getenv("OPENSKY_USERNAME", "").strip()
        pwd = os.getenv("OPENSKY_PASSWORD", "").strip()
        if user and pwd:
            auth = (user, pwd)

        # Запитуємо весь світ без обмежувального прямокутника США
        resp = requests.get(url, auth=auth, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
        states = payload.get("states") or []
        if not states:
            return []

        rng = random.Random(int(time.time()))

        # Розподіляємо реальні літаки за макрорегіонами світу для збалансованого відображення на карті світу
        buckets: Dict[str, List[Any]] = {
            "europe": [],
            "americas": [],
            "asia_me": [],
            "other": [],
        }
        for st in states:
            lon, lat = st[5], st[6]
            callsign = str(st[1] or "").strip()
            if lat is None or lon is None or not callsign:
                continue
            f_lat, f_lon = float(lat), float(lon)
            if -15.0 <= f_lon <= 42.0 and 34.0 <= f_lat <= 68.0:
                buckets["europe"].append(st)
            elif f_lon > 42.0:
                buckets["asia_me"].append(st)
            elif f_lon < -30.0 and f_lat >= 15.0:
                buckets["americas"].append(st)
            else:
                buckets["other"].append(st)

        for b_list in buckets.values():
            rng.shuffle(b_list)

        # Формуємо збалансований світовий пул (Європа + Азія/Близький Схід + Америка + Південна півкуля)
        states_shuffled: List[Any] = []
        max_len = max((len(v) for v in buckets.values()), default=0)
        for i in range(max_len):
            for key in ("europe", "asia_me", "americas", "other"):
                if i < len(buckets[key]):
                    states_shuffled.append(buckets[key][i])
            if len(states_shuffled) >= limit * 2:
                break
        events: List[Dict[str, Any]] = []

        for st in states_shuffled:
            if len(events) >= limit:
                break
            icao24 = str(st[0] or "").strip()
            callsign = str(st[1] or "").strip().upper()
            lon = st[5]
            lat = st[6]
            baro_alt = st[7]
            on_ground = bool(st[8])
            velocity_ms = st[9]
            heading = st[10]

            if lat is None or lon is None or not callsign:
                continue

            f_lat = float(lat)
            f_lon = float(lon)
            prefix3 = callsign[:3]
            op_carrier = ICAO_TO_IATA_CARRIER.get(prefix3)
            if not op_carrier:
                # Обираємо перевізника відповідно до регіону координат борту
                if -15.0 <= f_lon <= 40.0 and 34.0 <= f_lat <= 68.0:
                    op_carrier = rng.choice(
                        ["LH", "FR", "BA", "AF", "KL", "TK", "W6", "LO", "LX", "IB"]
                    )
                elif 40.0 < f_lon <= 65.0 and 15.0 <= f_lat <= 38.0:
                    op_carrier = rng.choice(["EK", "QR", "TK"])
                elif f_lon > 65.0:
                    op_carrier = rng.choice(["SQ", "NH", "JL", "CX", "QF", "EK"])
                elif f_lat < 15.0 and f_lon < -30.0:
                    op_carrier = "LA"
                else:
                    op_carrier = rng.choice(["DL", "UA", "AA", "WN", "AC", "B6"])

            # Визначаємо найближчий хаб вильоту за реальними координатами літака
            sorted_by_dist = sorted(
                self.airports,
                key=lambda ap: (ap[4] - f_lat) ** 2 + (ap[5] - f_lon) ** 2,
            )
            orig_ap = sorted_by_dist[0]
            dest_ap = rng.choice(sorted_by_dist[1:12])
            velocity_kts = round(float(velocity_ms or 210.0) * 1.94384, 1)
            alt_m = round(float(baro_alt or (0.0 if on_ground else 9800.0)), 1)
            est_dep_delay = round(rng.gauss(6.5, 16.0), 1)

            events.append(
                {
                    "event_id": f"osky-{icao24}-{int(time.time() * 1000) % 1000000}",
                    "icao24": icao24,
                    "callsign": callsign,
                    "op_carrier": op_carrier,
                    "airline_name": self.airlines_map.get(op_carrier, "US Carrier"),
                    "origin": orig_ap[0],
                    "dest": dest_ap[0],
                    "latitude": round(float(lat), 4),
                    "longitude": round(float(lon), 4),
                    "baro_altitude_m": max(0.0, alt_m),
                    "velocity_kts": max(0.0, velocity_kts),
                    "heading_deg": round(float(heading or 90.0), 1),
                    "on_ground": on_ground,
                    "source_mode": "opensky_live",
                    "estimated_dep_delay": est_dep_delay,
                    "event_timestamp": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                }
            )

        return events

    def generate_simulated_states(
        self, count: int = 45, inject_bad: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Fallback-генератор потокової телеметрії польотів між реальними аеропортами США
        (використовується при відсутності інтернету або ліміті OpenSky API, а також для інжекції DLQ).
        """
        rng = random.Random()
        events: List[Dict[str, Any]] = []
        icao_prefixes = list(ICAO_TO_IATA_CARRIER.items())

        for i in range(count):
            icao_pref, iata_code = rng.choice(icao_prefixes)
            orig_ap, dest_ap = rng.sample(self.airports, 2)
            frac = rng.uniform(0.12, 0.88)
            lat = (
                orig_ap[4] + (dest_ap[4] - orig_ap[4]) * frac + rng.uniform(-0.25, 0.25)
            )
            lon = (
                orig_ap[5] + (dest_ap[5] - orig_ap[5]) * frac + rng.uniform(-0.25, 0.25)
            )
            on_ground = rng.random() < 0.08
            alt_m = 0.0 if on_ground else round(rng.uniform(7200.0, 11800.0), 1)
            vel_kts = round(
                rng.uniform(15.0, 35.0) if on_ground else rng.uniform(390.0, 515.0), 1
            )
            est_dep_delay = round(max(-8.0, rng.gauss(8.0, 22.0)), 1)

            ev = {
                "event_id": f"sim-{uuid.uuid4().hex[:10]}",
                "icao24": f"a{rng.randint(10000, 99999):05x}",
                "callsign": f"{icao_pref}{rng.randint(100, 2999)}",
                "op_carrier": iata_code,
                "airline_name": self.airlines_map.get(iata_code, "US Carrier"),
                "origin": orig_ap[0],
                "dest": dest_ap[0],
                "latitude": round(lat, 4),
                "longitude": round(lon, 4),
                "baro_altitude_m": alt_m,
                "velocity_kts": vel_kts,
                "heading_deg": round(rng.uniform(0.0, 359.9), 1),
                "on_ground": on_ground,
                "source_mode": "replay_simulator",
                "estimated_dep_delay": est_dep_delay,
                "event_timestamp": datetime.now(timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            }
            events.append(ev)

        # Додаємо 2 битих повідомлення для перевірки маршрутизації у Dead Letter Queue (flights.dlq)
        if inject_bad and count >= 10:
            bad_ev = dict(events[0])
            bad_ev["event_id"] = f"bad-{uuid.uuid4().hex[:8]}"
            bad_ev["latitude"] = 195.0  # Неможлива широта (> 90 град)
            bad_ev["velocity_kts"] = -120.0
            events.append(bad_ev)

        return events

    def produce_batch(self, mode: str = "auto", batch_size: int = 45) -> Dict[str, Any]:
        """Отримує пакет подій (з OpenSky API або симулятора) та відправляє їх у Kafka."""
        events: List[Dict[str, Any]] = []
        used_mode = mode

        if mode in ("auto", "opensky_live"):
            try:
                events = self.fetch_opensky_live_states(limit=batch_size)
                if events:
                    used_mode = "opensky_live"
            except Exception as exc:
                logger.info(
                    "OpenSky API недоступний або лімітований (%s) -> перехід на replay_simulator",
                    exc,
                )

        if not events:
            events = self.generate_simulated_states(count=batch_size, inject_bad=True)
            used_mode = "replay_simulator"

        producer = self._get_kafka_producer()
        sent_to_kafka = 0
        topic = self.config.kafka_topic_live

        if producer is not None:
            for ev in events:
                producer.send(topic, key=ev.get("icao24", "flight"), value=ev)
                sent_to_kafka += 1
            producer.flush()

        return {
            "source_mode": used_mode,
            "events_count": len(events),
            "sent_to_kafka": sent_to_kafka,
            "topic": topic,
            "events": events,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="OpenSky Network -> Kafka Producer")
    parser.add_argument(
        "--mode", default="auto", choices=["auto", "opensky_live", "replay_simulator"]
    )
    parser.add_argument("--batches", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=45)
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args()

    prod = OpenSkyKafkaProducer()
    for b in range(args.batches):
        res = prod.produce_batch(mode=args.mode, batch_size=args.batch_size)
        print(
            f"[Producer Batch {b + 1}/{args.batches}] mode={res['source_mode']}, "
            f"events={res['events_count']}, kafka_sent={res['sent_to_kafka']}"
        )
        if b + 1 < args.batches:
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
