from __future__ import annotations

import sys

from telemetry_listener.config import ConfigError, ListenerConfig
from telemetry_listener.dead_letter_client import DeadLetterClient
from telemetry_listener.exceptions import HubClientConfigurationError
from telemetry_listener.hub_client import HubClient
from telemetry_listener.logging import configure_logging
from telemetry_listener.mqtt_client import MqttClient, MqttClientConfigurationError
from telemetry_listener.runtime import TelemetryListenerRuntime


def main() -> int:
    try:
        config = ListenerConfig.from_env()
        logger = configure_logging(config.log_level)
        hub_client = HubClient(config.hub_base_url, config.hub_ingest_key, config.hub_ingest_path)
        dead_letter_client = DeadLetterClient(
            config.dead_letter_base_url,
            config.dead_letter_ingest_key,
            config.dead_letter_source_service,
            config.dead_letter_ingest_path,
        )
        mqtt_client = MqttClient(config, logger)
    except (ConfigError, HubClientConfigurationError, MqttClientConfigurationError) as exc:
        print(f"telemetry listener configuration error: {exc}", file=sys.stderr)
        return 2

    runtime = TelemetryListenerRuntime(
        config=config,
        hub_client=hub_client,
        dead_letter_client=dead_letter_client,
        mqtt_client=mqtt_client,
        logger=logger,
    )
    try:
        return runtime.run()
    except Exception as exc:
        print(f"telemetry listener runtime error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
