from __future__ import annotations

from dataclasses import dataclass
import math
import logging
import os
from typing import Mapping


class ConfigError(ValueError):
    """Raised when listener environment configuration is invalid."""


def _split_csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _optional(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _required(value: str | None, env_name: str) -> str:
    normalized = _optional(value)
    if normalized is None:
        raise ConfigError(f"{env_name} is required")
    return normalized


def _required_or_default(value: str | None, env_name: str, default: str) -> str:
    if value is None:
        return default
    normalized = value.strip()
    if not normalized:
        raise ConfigError(f"{env_name} is required")
    return normalized


def _parse_int(value: str | None, env_name: str) -> int:
    if value is None:
        raise ConfigError(f"{env_name} is required")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{env_name} must be an integer, got {value!r}") from exc


def _parse_float(value: str | None, env_name: str) -> float:
    if value is None:
        raise ConfigError(f"{env_name} is required")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{env_name} must be a number, got {value!r}") from exc


def _validate_mqtt_port(port: int) -> int:
    if not 1 <= port <= 65535:
        raise ConfigError("TELEMETRY_LISTENER_MQTT_PORT must be in the range 1..65535")
    return port


def _validate_idle_sleep_seconds(seconds: float) -> float:
    if not math.isfinite(seconds):
        raise ConfigError("TELEMETRY_LISTENER_IDLE_SLEEP_SECONDS must be finite")
    if seconds <= 0:
        raise ConfigError("TELEMETRY_LISTENER_IDLE_SLEEP_SECONDS must be greater than 0")
    return seconds


def _validate_retry_count(count: int) -> int:
    if count < 0:
        raise ConfigError("TELEMETRY_LISTENER_RETRY_COUNT must be 0 or greater")
    return count


def _validate_retry_backoff_seconds(seconds: float) -> float:
    if not math.isfinite(seconds):
        raise ConfigError("TELEMETRY_LISTENER_RETRY_BACKOFF_SECONDS must be finite")
    if seconds <= 0:
        raise ConfigError("TELEMETRY_LISTENER_RETRY_BACKOFF_SECONDS must be greater than 0")
    return seconds


def _parse_topics(raw_topics: str, env_name: str) -> tuple[str, ...]:
    topics = _split_csv(raw_topics)
    if not topics:
        raise ConfigError(f"{env_name} must contain at least one topic")
    return topics


def _parse_log_level(raw_level: str, env_name: str) -> str:
    normalized = raw_level.strip().upper()
    if not normalized:
        raise ConfigError(f"{env_name} must contain a valid logging level")

    valid_levels = logging.getLevelNamesMapping()
    if normalized not in valid_levels:
        raise ConfigError(f"{env_name} must contain a valid logging level, got {raw_level!r}")
    return normalized


@dataclass(frozen=True)
class ListenerConfig:
    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_username: str | None = None
    mqtt_password: str | None = None
    mqtt_topics: tuple[str, ...] = ("telemetry/#",)
    mqtt_client_id: str = "service-telemetry-listener"
    hub_base_url: str = "http://service-telemetry-hub:8000"
    hub_ingest_path: str = "/ingest/raw/"
    hub_ingest_key: str = ""
    dead_letter_base_url: str = "http://service-telemetry-dead-letter:8000"
    dead_letter_ingest_path: str = "/ingest/"
    dead_letter_source_service: str = "service-telemetry-listener"
    dead_letter_ingest_key: str = ""
    retry_count: int = 3
    retry_backoff_seconds: float = 1.0
    log_level: str = "INFO"
    idle_sleep_seconds: float = 5.0

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "ListenerConfig":
        env = os.environ if environ is None else environ
        mqtt_port_raw = env.get("TELEMETRY_LISTENER_MQTT_PORT")
        retry_count_raw = env.get("TELEMETRY_LISTENER_RETRY_COUNT")
        retry_backoff_seconds_raw = env.get("TELEMETRY_LISTENER_RETRY_BACKOFF_SECONDS")
        idle_sleep_seconds_raw = env.get("TELEMETRY_LISTENER_IDLE_SLEEP_SECONDS")
        return cls(
            mqtt_host=env.get("TELEMETRY_LISTENER_MQTT_HOST", cls.mqtt_host),
            mqtt_port=_validate_mqtt_port(
                _parse_int(
                    mqtt_port_raw if mqtt_port_raw is not None else str(cls.mqtt_port),
                    "TELEMETRY_LISTENER_MQTT_PORT",
                )
            ),
            mqtt_username=_optional(env.get("TELEMETRY_LISTENER_MQTT_USERNAME")),
            mqtt_password=_optional(env.get("TELEMETRY_LISTENER_MQTT_PASSWORD")),
            mqtt_topics=_parse_topics(
                env.get(
                    "TELEMETRY_LISTENER_MQTT_TOPICS",
                    ",".join(cls.mqtt_topics),
                ),
                "TELEMETRY_LISTENER_MQTT_TOPICS",
            ),
            mqtt_client_id=env.get("TELEMETRY_LISTENER_CLIENT_ID", cls.mqtt_client_id),
            hub_base_url=env.get("TELEMETRY_HUB_BASE_URL", cls.hub_base_url),
            hub_ingest_path=env.get("TELEMETRY_HUB_INGEST_PATH", cls.hub_ingest_path),
            hub_ingest_key=_required(env.get("TELEMETRY_HUB_INGEST_KEY"), "TELEMETRY_HUB_INGEST_KEY"),
            dead_letter_base_url=_required_or_default(
                env.get("TELEMETRY_DEAD_LETTER_BASE_URL"),
                "TELEMETRY_DEAD_LETTER_BASE_URL",
                cls.dead_letter_base_url,
            ),
            dead_letter_ingest_path=env.get("TELEMETRY_DEAD_LETTER_INGEST_PATH", cls.dead_letter_ingest_path),
            dead_letter_source_service=_required_or_default(
                env.get("TELEMETRY_DEAD_LETTER_SOURCE_SERVICE"),
                "TELEMETRY_DEAD_LETTER_SOURCE_SERVICE",
                cls.dead_letter_source_service,
            ),
            dead_letter_ingest_key=_required(
                env.get("TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER"),
                "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER",
            ),
            retry_count=_validate_retry_count(
                _parse_int(
                    retry_count_raw if retry_count_raw is not None else str(cls.retry_count),
                    "TELEMETRY_LISTENER_RETRY_COUNT",
                )
            ),
            retry_backoff_seconds=_validate_retry_backoff_seconds(
                _parse_float(
                    retry_backoff_seconds_raw if retry_backoff_seconds_raw is not None else str(cls.retry_backoff_seconds),
                    "TELEMETRY_LISTENER_RETRY_BACKOFF_SECONDS",
                )
            ),
            log_level=_parse_log_level(
                env.get("TELEMETRY_LISTENER_LOG_LEVEL", cls.log_level),
                "TELEMETRY_LISTENER_LOG_LEVEL",
            ),
            idle_sleep_seconds=_validate_idle_sleep_seconds(
                _parse_float(
                    idle_sleep_seconds_raw if idle_sleep_seconds_raw is not None else str(cls.idle_sleep_seconds),
                    "TELEMETRY_LISTENER_IDLE_SLEEP_SECONDS",
                )
            ),
        )

    def redacted(self) -> dict[str, object]:
        return {
            "mqtt_host": self.mqtt_host,
            "mqtt_port": self.mqtt_port,
            "mqtt_username": self.mqtt_username,
            "mqtt_password": "***" if self.mqtt_password else None,
            "mqtt_topics": self.mqtt_topics,
            "mqtt_client_id": self.mqtt_client_id,
            "hub_base_url": self.hub_base_url,
            "hub_ingest_path": self.hub_ingest_path,
            "hub_ingest_key": "***" if self.hub_ingest_key else None,
            "dead_letter_base_url": self.dead_letter_base_url,
            "dead_letter_ingest_path": self.dead_letter_ingest_path,
            "dead_letter_source_service": self.dead_letter_source_service,
            "dead_letter_ingest_key": "***" if self.dead_letter_ingest_key else None,
            "retry_count": self.retry_count,
            "retry_backoff_seconds": self.retry_backoff_seconds,
            "log_level": self.log_level,
            "idle_sleep_seconds": self.idle_sleep_seconds,
        }
