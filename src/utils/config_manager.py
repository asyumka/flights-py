"""Модуль керування конфігурацією та змінними середовища комплексу ПАКАПА."""
from __future__ import annotations
import os
from pathlib import Path
from typing import Any, Dict

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


class ConfigManager:
    """Єдина точка доступу до параметрів конфігурації конвеєра, БД та Kafka."""

    def __init__(self, config_path: str | Path | None = None) -> None:
        if config_path is None:
            config_path = PROJECT_ROOT / "config" / "pipeline_config.yaml"
        self.config_path = Path(config_path)
        self._config: Dict[str, Any] = self._load_yaml()

    def _load_yaml(self) -> Dict[str, Any]:
        if not self.config_path.exists():
            return {}
        with open(self.config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    @property
    def raw_config(self) -> Dict[str, Any]:
        return self._config

    @property
    def pg_host(self) -> str:
        return os.getenv("PG_HOST", "localhost")

    @property
    def pg_port(self) -> int:
        return int(os.getenv("PG_PORT", "5432"))

    @property
    def pg_db(self) -> str:
        return os.getenv("PG_DB", "flights_dw")

    @property
    def pg_user(self) -> str:
        return os.getenv("PG_USER", "flights_user")

    @property
    def pg_password(self) -> str:
        return os.getenv("PG_PASSWORD", "flights_pass")

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password}"
        )

    @property
    def pg_jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_db}"

    @property
    def kafka_bootstrap_servers(self) -> str:
        return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")

    @property
    def kafka_topic_live(self) -> str:
        return os.getenv(
            "KAFKA_TOPIC_LIVE",
            self._config.get("streaming", {}).get("kafka_topic_live", "flights.live.stream"),
        )

    @property
    def kafka_topic_dlq(self) -> str:
        return os.getenv(
            "KAFKA_TOPIC_DLQ",
            self._config.get("streaming", {}).get("kafka_topic_dlq", "flights.dlq"),
        )

    @property
    def raw_dir(self) -> Path:
        p = PROJECT_ROOT / os.getenv("RAW_DIR", "data/raw")
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def bronze_dir(self) -> Path:
        p = PROJECT_ROOT / os.getenv("BRONZE_DIR", "data/lake/bronze")
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def silver_dir(self) -> Path:
        p = PROJECT_ROOT / os.getenv("SILVER_DIR", "data/lake/silver")
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def gold_dir(self) -> Path:
        p = PROJECT_ROOT / os.getenv("GOLD_DIR", "data/lake/gold")
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def quarantine_dir(self) -> Path:
        p = PROJECT_ROOT / os.getenv("QUARANTINE_DIR", "data/lake/quarantine")
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def models_dir(self) -> Path:
        p = PROJECT_ROOT / os.getenv("MODELS_DIR", "models")
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def backup_dir(self) -> Path:
        p = PROJECT_ROOT / os.getenv("BACKUP_DIR", "backups")
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def dq_rules(self) -> Dict[str, Any]:
        return self._config.get("data_quality", {})

    @property
    def ml_params(self) -> Dict[str, Any]:
        return self._config.get("ml", {})
