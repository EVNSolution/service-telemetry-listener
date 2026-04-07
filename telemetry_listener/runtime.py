from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import logging
from time import sleep as default_sleep
from typing import Callable, Protocol

from telemetry_listener.config import ListenerConfig
from telemetry_listener.hub_client import HubClient, HubIngestDisposition, HubIngestResult
from telemetry_listener.parser import ParseFailure, parse_message


class MqttWorkerClient(Protocol):
    def set_message_handler(self, handler: Callable[[str, bytes | str, datetime], None]) -> None: ...

    def connect(self) -> None: ...

    def subscribe(self, topics: tuple[str, ...]) -> None: ...

    def loop_forever(self) -> None: ...


class DeadLetterWriter(Protocol):
    def post_ingest(self, payload: dict[str, object]): ...


@dataclass(frozen=True)
class TelemetryMessageOutcome:
    envelope_topic: str
    result: HubIngestResult
    attempts: int


class TelemetryListenerRuntime:
    def __init__(
        self,
        config: ListenerConfig,
        hub_client: HubClient,
        mqtt_client: MqttWorkerClient,
        dead_letter_client: DeadLetterWriter | None = None,
        logger: logging.Logger | None = None,
        sleep: Callable[[float], None] = default_sleep,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._config = config
        self._hub_client = hub_client
        self._mqtt_client = mqtt_client
        self._dead_letter_client = dead_letter_client
        self._logger = logger or logging.getLogger("telemetry_listener.runtime")
        self._sleep = sleep
        self._now = now or (lambda: datetime.now(timezone.utc))

    def run(self) -> int:
        self._mqtt_client.set_message_handler(self._handle_message)
        try:
            self._mqtt_client.connect()
            self._mqtt_client.subscribe(self._config.mqtt_topics)
            self._mqtt_client.loop_forever()
        except Exception as exc:
            self._logger.error("mqtt startup/runtime failure: %s", exc)
            return 1
        return 0

    def _handle_message(self, topic: str, payload: bytes | str, received_at: datetime) -> TelemetryMessageOutcome:
        outcome = parse_message(topic=topic, payload=payload, received_at=received_at)
        if isinstance(outcome, ParseFailure):
            self._logger.error("telemetry parse failure topic=%s error=%s", topic, outcome.error)
            self._write_parse_failure_dead_letter(outcome)
            result = HubIngestResult(
                disposition=HubIngestDisposition.DROP,
                error=f"malformed telemetry message: {outcome.error}",
            )
            return TelemetryMessageOutcome(envelope_topic=topic, result=result, attempts=0)
        result, attempts = self._forward_envelope(outcome.to_hub_ingest_payload())
        return TelemetryMessageOutcome(envelope_topic=outcome.message_topic, result=result, attempts=attempts)

    def _forward_envelope(self, ingest_payload: dict[str, object]) -> tuple[HubIngestResult, int]:
        attempts = 0
        while True:
            attempts += 1
            result = self._hub_client.post_raw(ingest_payload)

            if result.disposition is HubIngestDisposition.SUCCESS:
                self._logger.info(
                    "telemetry ingest success topic=%s status=%s attempts=%s",
                    ingest_payload.get("message_topic"),
                    result.status_code,
                    attempts,
                )
                return result, attempts

            if result.disposition is HubIngestDisposition.DROP:
                self._logger.error(
                    "telemetry ingest drop topic=%s status=%s attempts=%s",
                    ingest_payload.get("message_topic"),
                    result.status_code,
                    attempts,
                )
                if result.status_code is not None and 400 <= result.status_code < 500:
                    self._write_ingest_dead_letter(
                        ingest_payload=ingest_payload,
                        failure_class="hub_4xx",
                        error_message=self._hub_error_message(result),
                        retry_attempts=attempts,
                    )
                return result, attempts

            if attempts > self._config.retry_count:
                self._logger.error(
                    "telemetry ingest retry exhausted topic=%s attempts=%s error=%s",
                    ingest_payload.get("message_topic"),
                    attempts,
                    result.error,
                )
                self._write_ingest_dead_letter(
                    ingest_payload=ingest_payload,
                    failure_class=self._retry_exhausted_failure_class(result),
                    error_message=self._hub_error_message(result),
                    retry_attempts=attempts,
                )
                return result, attempts

            self._logger.warning(
                "telemetry ingest retry topic=%s attempts=%s error=%s",
                ingest_payload.get("message_topic"),
                attempts,
                result.error,
            )
            self._sleep(self._config.retry_backoff_seconds)

    def _write_parse_failure_dead_letter(self, outcome: ParseFailure) -> None:
        self._write_dead_letter(
            {
                "message_topic": outcome.message_topic,
                "source_terminal_id": None,
                "source_vehicle_id": None,
                "message_type": None,
                "payload_json": outcome.raw_payload,
                "received_at": self._format_received_at(outcome.received_at),
                "failure_class": "parse_error",
                "error_message": outcome.error,
                "retry_attempts": 0,
                "failure_fingerprint": self._failure_fingerprint(
                    message_topic=outcome.message_topic,
                    payload_json=outcome.raw_payload,
                    failure_class="parse_error",
                    error_message=outcome.error,
                ),
            }
        )

    def _write_ingest_dead_letter(
        self,
        ingest_payload: dict[str, object],
        failure_class: str,
        error_message: str,
        retry_attempts: int,
    ) -> None:
        self._write_dead_letter(
            {
                "message_topic": ingest_payload.get("message_topic"),
                "source_terminal_id": ingest_payload.get("source_terminal_id"),
                "source_vehicle_id": ingest_payload.get("source_vehicle_id"),
                "message_type": ingest_payload.get("message_type"),
                "payload_json": ingest_payload.get("payload_json"),
                "received_at": ingest_payload.get("received_at"),
                "failure_class": failure_class,
                "error_message": error_message,
                "retry_attempts": retry_attempts,
                "failure_fingerprint": self._failure_fingerprint(
                    message_topic=str(ingest_payload.get("message_topic")),
                    payload_json=ingest_payload.get("payload_json"),
                    failure_class=failure_class,
                    error_message=error_message,
                ),
            }
        )

    def _write_dead_letter(self, payload: dict[str, object]) -> None:
        if self._dead_letter_client is None:
            return
        dead_letter_payload = {
            **payload,
            "failed_at": self._format_timestamp(self._now()),
        }
        try:
            result = self._dead_letter_client.post_ingest(dead_letter_payload)
        except Exception as exc:  # pragma: no cover - defensive path
            self._logger.error(
                "telemetry dead-letter write failure topic=%s error=%s",
                dead_letter_payload.get("message_topic"),
                exc,
            )
            return

        if result.accepted:
            self._logger.info(
                "telemetry dead-letter write success topic=%s failure_class=%s status=%s",
                dead_letter_payload.get("message_topic"),
                dead_letter_payload.get("failure_class"),
                result.status_code,
            )
            return

        self._logger.error(
            "telemetry dead-letter write drop topic=%s failure_class=%s status=%s",
            dead_letter_payload.get("message_topic"),
            dead_letter_payload.get("failure_class"),
            result.status_code,
        )

    def _hub_error_message(self, result: HubIngestResult) -> str:
        if result.error:
            return result.error
        if result.response_text:
            return result.response_text
        if result.response_json is not None:
            return json.dumps(result.response_json, separators=(",", ":"), sort_keys=True)
        if result.status_code is not None:
            return f"hub ingest returned status {result.status_code}"
        return "hub ingest failed"

    def _retry_exhausted_failure_class(self, result: HubIngestResult) -> str:
        retry_cause = result.retry_cause
        if retry_cause == "timeout":
            return "timeout_retry_exhausted"
        if retry_cause == "connection_failure":
            return "connection_failure_retry_exhausted"
        return "hub_5xx_retry_exhausted"

    def _failure_fingerprint(
        self,
        *,
        message_topic: str,
        payload_json: object,
        failure_class: str,
        error_message: str,
    ) -> str:
        serialized_payload = json.dumps(payload_json, separators=(",", ":"), sort_keys=True, default=str)
        raw = "|".join((message_topic, failure_class, error_message, serialized_payload))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _format_received_at(self, received_at: datetime) -> str:
        return self._format_timestamp(received_at)

    def _format_timestamp(self, value: datetime) -> str:
        return value.isoformat().replace("+00:00", "Z")
