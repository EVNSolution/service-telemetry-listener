from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from telemetry_listener.config import ListenerConfig
from telemetry_listener.dead_letter_client import DeadLetterWriteDisposition, DeadLetterWriteResult
from telemetry_listener.hub_client import HubIngestDisposition, HubIngestResult
from telemetry_listener.mqtt_client import MqttClient
from telemetry_listener.parser import ParseFailure
from telemetry_listener.runtime import TelemetryListenerRuntime


class RecordingLogger:
    def __init__(self) -> None:
        self.records: list[tuple[str, str]] = []

    def _record(self, level: str, message: str, *args: object) -> None:
        rendered = message % args if args else message
        self.records.append((level, rendered))

    def debug(self, message: str, *args: object) -> None:
        self._record("debug", message, *args)

    def info(self, message: str, *args: object) -> None:
        self._record("info", message, *args)

    def warning(self, message: str, *args: object) -> None:
        self._record("warning", message, *args)

    def error(self, message: str, *args: object) -> None:
        self._record("error", message, *args)

    def exception(self, message: str, *args: object) -> None:
        self._record("exception", message, *args)


@dataclass
class FakeHubClient:
    responses: list[HubIngestResult]
    calls: list[dict[str, object]] = field(default_factory=list)

    def post_raw(self, payload: dict[str, object]) -> HubIngestResult:
        self.calls.append(payload)
        if not self.responses:
            raise AssertionError("unexpected hub ingest call")
        return self.responses.pop(0)


@dataclass
class FakeDeadLetterClient:
    responses: list[DeadLetterWriteResult]
    calls: list[dict[str, object]] = field(default_factory=list)

    def post_ingest(self, payload: dict[str, object]) -> DeadLetterWriteResult:
        self.calls.append(payload)
        if not self.responses:
            raise AssertionError("unexpected dead-letter ingest call")
        return self.responses.pop(0)


class ExplodingHubClient:
    def __init__(self, error: Exception) -> None:
        self.error = error
        self.calls: list[dict[str, object]] = []

    def post_raw(self, payload: dict[str, object]) -> HubIngestResult:
        self.calls.append(payload)
        raise self.error


class FakeMqttClient:
    def __init__(self, messages: list[tuple[str, bytes, datetime]]) -> None:
        self.messages = messages
        self.handler = None
        self.connect_calls = 0
        self.subscribe_calls: list[tuple[str, ...]] = []
        self.loop_calls = 0

    def set_message_handler(self, handler) -> None:  # noqa: ANN001
        self.handler = handler

    def connect(self) -> None:
        self.connect_calls += 1

    def subscribe(self, topics: tuple[str, ...]) -> None:
        self.subscribe_calls.append(tuple(topics))

    def loop_forever(self) -> None:
        self.loop_calls += 1
        if self.handler is None:
            raise AssertionError("message handler was not configured")
        for topic, payload, received_at in self.messages:
            self.handler(topic, payload, received_at)


class FailingConnectMqttClient(FakeMqttClient):
    def connect(self) -> None:
        self.connect_calls += 1
        raise RuntimeError("broker unavailable")


class FakePahoClient:
    def __init__(self, connect_reason_code: int = 0, subscribe_return_codes: list[int] | None = None) -> None:
        self.connect_reason_code = connect_reason_code
        self.subscribe_return_codes = subscribe_return_codes or []
        self.on_connect = None
        self.on_disconnect = None
        self.on_message = None
        self.connect_calls = 0
        self.subscribe_calls: list[str] = []
        self.disconnect_calls = 0
        self.loop_calls = 0

    def username_pw_set(self, username: str, password: str | None) -> None:  # noqa: ARG002
        return None

    def reconnect_delay_set(self, min_delay: int, max_delay: int) -> None:  # noqa: ARG002
        return None

    def connect(self, host: str, port: int, keepalive: int) -> None:  # noqa: ARG002
        self.connect_calls += 1

    def subscribe(self, topic: str) -> tuple[int, int]:
        self.subscribe_calls.append(topic)
        if self.subscribe_return_codes:
            return self.subscribe_return_codes.pop(0), 1
        return 0, 1

    def disconnect(self) -> None:
        self.disconnect_calls += 1

    def loop_forever(self, retry_first_connection: bool = False) -> None:  # noqa: ARG002
        self.loop_calls += 1
        if self.on_connect is not None:
            self.on_connect(self, None, None, SimpleNamespace(value=self.connect_reason_code), None)


class TelemetryListenerRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.received_at = datetime(2026, 3, 21, 9, 0, 5, tzinfo=timezone.utc)
        self.failed_at = datetime(2026, 3, 21, 9, 1, 30, tzinfo=timezone.utc)
        self.config = ListenerConfig(
            mqtt_host="mqtt-broker",
            mqtt_port=1883,
            mqtt_topics=("telemetry/#",),
            mqtt_client_id="listener-worker",
            hub_base_url="http://telemetry-hub:8000",
            hub_ingest_key="shared-key",
            retry_count=1,
            retry_backoff_seconds=0.01,
        )

    def test_run_connects_subscribes_and_processes_successful_message(self) -> None:
        fake_hub = FakeHubClient(
            [
                HubIngestResult(
                    disposition=HubIngestDisposition.SUCCESS,
                    status_code=201,
                    response_json={"telemetry_raw_ingest_id": "raw-1"},
                )
            ]
        )
        fake_dead_letter = FakeDeadLetterClient([])
        fake_mqtt = FakeMqttClient(
            [
                (
                    "telemetry/vehicles/123",
                    b'{"terminal_id":"terminal-1","message_type":"location_update"}',
                    self.received_at,
                )
            ]
        )
        logger = RecordingLogger()
        sleeps: list[float] = []

        runtime = TelemetryListenerRuntime(
            config=self.config,
            hub_client=fake_hub,
            dead_letter_client=fake_dead_letter,
            mqtt_client=fake_mqtt,
            logger=logger,
            sleep=sleeps.append,
        )

        result = runtime.run()

        self.assertEqual(result, 0)
        self.assertEqual(fake_mqtt.connect_calls, 1)
        self.assertEqual(fake_mqtt.subscribe_calls, [("telemetry/#",)])
        self.assertEqual(fake_mqtt.loop_calls, 1)
        self.assertEqual(len(fake_hub.calls), 1)
        self.assertEqual(fake_dead_letter.calls, [])
        self.assertEqual(fake_hub.calls[0]["source_terminal_id"], "terminal-1")
        self.assertEqual(fake_hub.calls[0]["message_type"], "location_update")
        self.assertEqual(sleeps, [])
        self.assertTrue(any(level == "info" and "success" in message.lower() for level, message in logger.records))

    def test_run_retries_retry_worthy_failure_before_success(self) -> None:
        fake_hub = FakeHubClient(
            [
                HubIngestResult(disposition=HubIngestDisposition.RETRY, error="temporary failure"),
                HubIngestResult(disposition=HubIngestDisposition.SUCCESS, status_code=200),
            ]
        )
        fake_dead_letter = FakeDeadLetterClient([])
        fake_mqtt = FakeMqttClient(
            [
                (
                    "telemetry/vehicles/123",
                    b'{"vehicle_id":"vehicle-1","message_type":"location_update"}',
                    self.received_at,
                )
            ]
        )
        logger = RecordingLogger()
        sleeps: list[float] = []

        runtime = TelemetryListenerRuntime(
            config=self.config,
            hub_client=fake_hub,
            dead_letter_client=fake_dead_letter,
            mqtt_client=fake_mqtt,
            logger=logger,
            sleep=sleeps.append,
        )

        result = runtime.run()

        self.assertEqual(result, 0)
        self.assertEqual(len(fake_hub.calls), 2)
        self.assertEqual(fake_dead_letter.calls, [])
        self.assertEqual(sleeps, [0.01])
        self.assertTrue(any(level == "warning" and "retry" in message.lower() for level, message in logger.records))
        self.assertTrue(any(level == "info" and "success" in message.lower() for level, message in logger.records))

    def test_run_writes_dead_letter_for_hub_4xx_without_retry(self) -> None:
        fake_hub = FakeHubClient(
            [
                HubIngestResult(
                    disposition=HubIngestDisposition.DROP,
                    status_code=400,
                    response_json={"detail": "invalid payload"},
                )
            ]
        )
        fake_dead_letter = FakeDeadLetterClient(
            [DeadLetterWriteResult(disposition=DeadLetterWriteDisposition.SUCCESS, status_code=201)]
        )
        fake_mqtt = FakeMqttClient(
            [
                (
                    "telemetry/vehicles/123",
                    b'{"message_type":"location_update"}',
                    self.received_at,
                )
            ]
        )
        logger = RecordingLogger()
        sleeps: list[float] = []

        runtime = TelemetryListenerRuntime(
            config=self.config,
            hub_client=fake_hub,
            dead_letter_client=fake_dead_letter,
            mqtt_client=fake_mqtt,
            logger=logger,
            sleep=sleeps.append,
            now=lambda: self.failed_at,
        )

        result = runtime.run()

        self.assertEqual(result, 0)
        self.assertEqual(len(fake_hub.calls), 1)
        self.assertEqual(len(fake_dead_letter.calls), 1)
        self.assertEqual(fake_dead_letter.calls[0]["failure_class"], "hub_4xx")
        self.assertEqual(fake_dead_letter.calls[0]["retry_attempts"], 1)
        self.assertEqual(fake_dead_letter.calls[0]["failed_at"], "2026-03-21T09:01:30Z")
        self.assertNotEqual(fake_dead_letter.calls[0]["failed_at"], fake_dead_letter.calls[0]["received_at"])
        self.assertTrue(fake_dead_letter.calls[0]["failure_fingerprint"])
        self.assertEqual(sleeps, [])
        self.assertTrue(any(level == "error" and "drop" in message.lower() for level, message in logger.records))

    def test_run_logs_3xx_drop_without_writing_dead_letter(self) -> None:
        fake_hub = FakeHubClient(
            [
                HubIngestResult(
                    disposition=HubIngestDisposition.DROP,
                    status_code=307,
                    response_text="temporary redirect",
                )
            ]
        )
        fake_dead_letter = FakeDeadLetterClient([])
        fake_mqtt = FakeMqttClient(
            [
                (
                    "telemetry/vehicles/123",
                    b'{"message_type":"location_update"}',
                    self.received_at,
                )
            ]
        )
        logger = RecordingLogger()
        sleeps: list[float] = []

        runtime = TelemetryListenerRuntime(
            config=self.config,
            hub_client=fake_hub,
            dead_letter_client=fake_dead_letter,
            mqtt_client=fake_mqtt,
            logger=logger,
            sleep=sleeps.append,
        )

        result = runtime.run()

        self.assertEqual(result, 0)
        self.assertEqual(len(fake_hub.calls), 1)
        self.assertEqual(fake_dead_letter.calls, [])
        self.assertEqual(sleeps, [])
        self.assertTrue(any(level == "error" and "drop" in message.lower() for level, message in logger.records))

    def test_run_writes_dead_letter_for_parse_failure_without_calling_hub(self) -> None:
        fake_hub = FakeHubClient([])
        fake_dead_letter = FakeDeadLetterClient(
            [DeadLetterWriteResult(disposition=DeadLetterWriteDisposition.SUCCESS, status_code=201)]
        )
        fake_mqtt = FakeMqttClient(
            [
                (
                    "telemetry/vehicles/123",
                    b"broken",
                    self.received_at,
                )
            ]
        )
        logger = RecordingLogger()

        runtime = TelemetryListenerRuntime(
            config=self.config,
            hub_client=fake_hub,
            dead_letter_client=fake_dead_letter,
            mqtt_client=fake_mqtt,
            logger=logger,
            now=lambda: self.failed_at,
        )

        with patch(
            "telemetry_listener.runtime.parse_message",
            return_value=ParseFailure(
                message_topic="telemetry/vehicles/123",
                received_at=self.received_at,
                raw_payload="broken",
                error="invalid JSON: bad payload",
            ),
        ):
            result = runtime.run()

        self.assertEqual(result, 0)
        self.assertEqual(fake_hub.calls, [])
        self.assertEqual(len(fake_dead_letter.calls), 1)
        self.assertEqual(fake_dead_letter.calls[0]["payload_json"], "broken")
        self.assertEqual(fake_dead_letter.calls[0]["failure_class"], "parse_error")
        self.assertEqual(fake_dead_letter.calls[0]["retry_attempts"], 0)
        self.assertEqual(fake_dead_letter.calls[0]["failed_at"], "2026-03-21T09:01:30Z")
        self.assertNotEqual(fake_dead_letter.calls[0]["failed_at"], fake_dead_letter.calls[0]["received_at"])
        self.assertTrue(fake_dead_letter.calls[0]["failure_fingerprint"])
        self.assertTrue(any(level == "error" and "parse" in message.lower() for level, message in logger.records))

    def test_run_surfaces_unexpected_hub_client_exception_without_dead_letter(self) -> None:
        fake_hub = ExplodingHubClient(RuntimeError("unexpected hub client failure"))
        fake_dead_letter = FakeDeadLetterClient([])
        fake_mqtt = FakeMqttClient(
            [
                (
                    "telemetry/vehicles/123",
                    b'{"message_type":"location_update"}',
                    self.received_at,
                )
            ]
        )
        logger = RecordingLogger()
        sleeps: list[float] = []

        runtime = TelemetryListenerRuntime(
            config=self.config,
            hub_client=fake_hub,
            dead_letter_client=fake_dead_letter,
            mqtt_client=fake_mqtt,
            logger=logger,
            sleep=sleeps.append,
        )

        result = runtime.run()

        self.assertEqual(result, 1)
        self.assertEqual(len(fake_hub.calls), 1)
        self.assertEqual(fake_dead_letter.calls, [])
        self.assertEqual(sleeps, [])
        self.assertTrue(
            any(
                level == "error"
                and "startup/runtime failure" in message.lower()
                and "unexpected hub client failure" in message.lower()
                for level, message in logger.records
            )
        )

    def test_run_returns_non_success_when_connect_fails_before_loop(self) -> None:
        fake_hub = FakeHubClient([])
        fake_mqtt = FailingConnectMqttClient([])
        logger = RecordingLogger()
        sleeps: list[float] = []

        runtime = TelemetryListenerRuntime(
            config=self.config,
            hub_client=fake_hub,
            mqtt_client=fake_mqtt,
            logger=logger,
            sleep=sleeps.append,
        )

        result = runtime.run()

        self.assertEqual(result, 1)
        self.assertEqual(fake_mqtt.connect_calls, 1)
        self.assertEqual(fake_mqtt.subscribe_calls, [])
        self.assertEqual(fake_mqtt.loop_calls, 0)
        self.assertEqual(sleeps, [])
        self.assertTrue(any(level == "error" and "startup" in message.lower() for level, message in logger.records))

    def test_run_returns_non_success_when_on_connect_reports_failure(self) -> None:
        fake_hub = FakeHubClient([])
        fake_paho = FakePahoClient(connect_reason_code=1)
        mqtt_client = MqttClient(self.config, RecordingLogger(), client=fake_paho)
        logger = RecordingLogger()

        runtime = TelemetryListenerRuntime(
            config=self.config,
            hub_client=fake_hub,
            mqtt_client=mqtt_client,
            logger=logger,
        )

        result = runtime.run()

        self.assertEqual(result, 1)
        self.assertEqual(fake_paho.connect_calls, 1)
        self.assertEqual(fake_paho.loop_calls, 1)
        self.assertEqual(fake_paho.disconnect_calls, 1)
        self.assertEqual(fake_paho.subscribe_calls, [])
        self.assertTrue(any(level == "error" and "connect failed" in message.lower() for level, message in logger.records))

    def test_run_returns_non_success_when_subscription_fails_during_startup(self) -> None:
        fake_hub = FakeHubClient([])
        fake_paho = FakePahoClient(connect_reason_code=0, subscribe_return_codes=[1])
        mqtt_client = MqttClient(self.config, RecordingLogger(), client=fake_paho)
        logger = RecordingLogger()

        runtime = TelemetryListenerRuntime(
            config=self.config,
            hub_client=fake_hub,
            mqtt_client=mqtt_client,
            logger=logger,
        )

        result = runtime.run()

        self.assertEqual(result, 1)
        self.assertEqual(fake_paho.connect_calls, 1)
        self.assertEqual(fake_paho.loop_calls, 1)
        self.assertEqual(fake_paho.disconnect_calls, 1)
        self.assertEqual(fake_paho.subscribe_calls, ["telemetry/#"])
        self.assertTrue(any(level == "error" and "subscribe" in message.lower() for level, message in logger.records))

    def test_run_exhausts_retries_when_retry_count_is_zero(self) -> None:
        fake_hub = FakeHubClient(
            [
                HubIngestResult(
                    disposition=HubIngestDisposition.RETRY,
                    error="temporary outage",
                    retry_cause="hub_5xx",
                )
            ]
        )
        fake_dead_letter = FakeDeadLetterClient(
            [DeadLetterWriteResult(disposition=DeadLetterWriteDisposition.SUCCESS, status_code=201)]
        )
        fake_mqtt = FakeMqttClient(
            [
                (
                    "telemetry/vehicles/123",
                    b'{"message_type":"location_update"}',
                    self.received_at,
                )
            ]
        )
        logger = RecordingLogger()
        sleeps: list[float] = []
        config = ListenerConfig(
            mqtt_host="mqtt-broker",
            mqtt_port=1883,
            mqtt_topics=("telemetry/#",),
            mqtt_client_id="listener-worker",
            hub_base_url="http://telemetry-hub:8000",
            hub_ingest_key="shared-key",
            retry_count=0,
            retry_backoff_seconds=0.01,
        )

        runtime = TelemetryListenerRuntime(
            config=config,
            hub_client=fake_hub,
            dead_letter_client=fake_dead_letter,
            mqtt_client=fake_mqtt,
            logger=logger,
            sleep=sleeps.append,
        )

        result = runtime.run()

        self.assertEqual(result, 0)
        self.assertEqual(len(fake_hub.calls), 1)
        self.assertEqual(len(fake_dead_letter.calls), 1)
        self.assertEqual(fake_dead_letter.calls[0]["failure_class"], "hub_5xx_retry_exhausted")
        self.assertEqual(sleeps, [])
        self.assertTrue(any(level == "error" and "exhaust" in message.lower() for level, message in logger.records))

    def test_run_writes_timeout_retry_exhausted_dead_letter(self) -> None:
        fake_hub = FakeHubClient(
            [
                HubIngestResult(disposition=HubIngestDisposition.RETRY, error="request timed out", retry_cause="timeout"),
                HubIngestResult(disposition=HubIngestDisposition.RETRY, error="request timed out", retry_cause="timeout"),
            ]
        )
        fake_dead_letter = FakeDeadLetterClient(
            [DeadLetterWriteResult(disposition=DeadLetterWriteDisposition.SUCCESS, status_code=201)]
        )
        fake_mqtt = FakeMqttClient(
            [
                (
                    "telemetry/vehicles/123",
                    b'{"message_type":"location_update"}',
                    self.received_at,
                )
            ]
        )
        logger = RecordingLogger()
        sleeps: list[float] = []
        config = ListenerConfig(
            mqtt_host="mqtt-broker",
            mqtt_port=1883,
            mqtt_topics=("telemetry/#",),
            mqtt_client_id="listener-worker",
            hub_base_url="http://telemetry-hub:8000",
            hub_ingest_key="shared-key",
            retry_count=1,
            retry_backoff_seconds=0.01,
        )

        runtime = TelemetryListenerRuntime(
            config=config,
            hub_client=fake_hub,
            dead_letter_client=fake_dead_letter,
            mqtt_client=fake_mqtt,
            logger=logger,
            sleep=sleeps.append,
        )

        result = runtime.run()

        self.assertEqual(result, 0)
        self.assertEqual(len(fake_hub.calls), 2)
        self.assertEqual(len(fake_dead_letter.calls), 1)
        self.assertEqual(fake_dead_letter.calls[0]["failure_class"], "timeout_retry_exhausted")
        self.assertEqual(sleeps, [0.01])
        self.assertTrue(any(level == "error" and "exhaust" in message.lower() for level, message in logger.records))

    def test_run_writes_connection_failure_retry_exhausted_dead_letter(self) -> None:
        fake_hub = FakeHubClient(
            [
                HubIngestResult(
                    disposition=HubIngestDisposition.RETRY,
                    error="connection failed",
                    retry_cause="connection_failure",
                ),
                HubIngestResult(
                    disposition=HubIngestDisposition.RETRY,
                    error="connection failed again",
                    retry_cause="connection_failure",
                ),
            ]
        )
        fake_dead_letter = FakeDeadLetterClient(
            [DeadLetterWriteResult(disposition=DeadLetterWriteDisposition.SUCCESS, status_code=201)]
        )
        fake_mqtt = FakeMqttClient(
            [
                (
                    "telemetry/vehicles/123",
                    b'{"message_type":"location_update"}',
                    self.received_at,
                )
            ]
        )
        logger = RecordingLogger()
        sleeps: list[float] = []
        config = ListenerConfig(
            mqtt_host="mqtt-broker",
            mqtt_port=1883,
            mqtt_topics=("telemetry/#",),
            mqtt_client_id="listener-worker",
            hub_base_url="http://telemetry-hub:8000",
            hub_ingest_key="shared-key",
            retry_count=1,
            retry_backoff_seconds=0.01,
        )

        runtime = TelemetryListenerRuntime(
            config=config,
            hub_client=fake_hub,
            dead_letter_client=fake_dead_letter,
            mqtt_client=fake_mqtt,
            logger=logger,
            sleep=sleeps.append,
        )

        result = runtime.run()

        self.assertEqual(result, 0)
        self.assertEqual(len(fake_hub.calls), 2)
        self.assertEqual(len(fake_dead_letter.calls), 1)
        self.assertEqual(fake_dead_letter.calls[0]["failure_class"], "connection_failure_retry_exhausted")
        self.assertEqual(sleeps, [0.01])
        self.assertTrue(any(level == "error" and "exhaust" in message.lower() for level, message in logger.records))
