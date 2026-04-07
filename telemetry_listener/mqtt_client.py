from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Callable

try:  # pragma: no cover - optional import path
    from paho.mqtt import client as paho_mqtt_client
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    paho_mqtt_client = None

from telemetry_listener.config import ListenerConfig


class MqttClientConfigurationError(RuntimeError):
    """Raised when the MQTT worker client cannot be created."""


class MqttClientStartupError(RuntimeError):
    """Raised when MQTT startup fails during connect or subscribe."""


class MqttClient:
    def __init__(
        self,
        config: ListenerConfig,
        logger: logging.Logger,
        client: object | None = None,
    ) -> None:
        self._config = config
        self._logger = logger
        self._handler: Callable[[str, bytes | str, datetime], None] | None = None
        self._topics: tuple[str, ...] = tuple(config.mqtt_topics)
        self._connected = False
        self._startup_error: MqttClientStartupError | None = None

        if client is not None:
            self._client = client
        else:
            if paho_mqtt_client is None:
                raise MqttClientConfigurationError("paho-mqtt is required for the MQTT worker client")
            self._client = paho_mqtt_client.Client(client_id=config.mqtt_client_id)

        if config.mqtt_username:
            self._client.username_pw_set(config.mqtt_username, config.mqtt_password)

        if hasattr(self._client, "reconnect_delay_set"):
            self._client.reconnect_delay_set(min_delay=1, max_delay=30)

    def set_message_handler(self, handler: Callable[[str, bytes | str, datetime], None]) -> None:
        self._handler = handler

    def connect(self) -> None:
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        self._client.connect(self._config.mqtt_host, self._config.mqtt_port, keepalive=60)

    def subscribe(self, topics: tuple[str, ...]) -> None:
        self._topics = tuple(topics)
        if self._connected:
            self._subscribe_topics()

    def loop_forever(self) -> None:
        try:
            self._client.loop_forever(retry_first_connection=True)
        except TypeError:  # pragma: no cover - compatibility fallback
            self._client.loop_forever()
        if self._startup_error is not None:
            raise self._startup_error

    def _on_connect(self, client: object, userdata: object, flags: object, reason_code: object, properties: object | None = None) -> None:  # noqa: ARG002
        if not self._is_successful_reason_code(reason_code):
            self._mark_startup_failure(f"mqtt connect failed reason={reason_code}")
            return

        self._connected = True
        self._subscribe_topics()
        if self._startup_error is not None:
            return
        self._logger.info("mqtt connected and subscribed")

    def _on_disconnect(self, client: object, userdata: object, reason_code: object, properties: object | None = None) -> None:  # noqa: ARG002
        self._connected = False
        self._logger.warning("mqtt disconnected reason=%s", reason_code)

    def _on_message(self, client: object, userdata: object, message: object) -> None:  # noqa: ARG002
        if self._handler is None:
            self._logger.warning("mqtt message received before handler was configured")
            return

        topic = getattr(message, "topic", "")
        payload = getattr(message, "payload", b"")
        self._handler(topic, payload, datetime.now(timezone.utc))

    def _subscribe_topics(self) -> None:
        for topic in self._topics:
            result = self._client.subscribe(topic)
            if self._subscription_failed(result):
                self._mark_startup_failure(f"mqtt subscribe failed topic={topic} result={result}")
                return

    def _mark_startup_failure(self, message: str) -> None:
        self._startup_error = MqttClientStartupError(message)
        self._logger.error(message)
        if hasattr(self._client, "disconnect"):
            try:
                self._client.disconnect()
            except Exception:  # pragma: no cover - defensive disconnect cleanup
                pass

    @staticmethod
    def _is_successful_reason_code(reason_code: object) -> bool:
        value = getattr(reason_code, "value", reason_code)
        try:
            return int(value) == 0
        except (TypeError, ValueError):
            return str(reason_code).strip().lower() in {"success", "success (0)"}

    @staticmethod
    def _subscription_failed(result: object) -> bool:
        rc = result[0] if isinstance(result, tuple) and result else result
        value = getattr(rc, "value", rc)
        try:
            return int(value) != 0
        except (TypeError, ValueError):
            return True
