from __future__ import annotations

import io
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import telemetry_listener.main as main_module


class MainTests(unittest.TestCase):
    def test_main_returns_non_success_when_runtime_aborts_unexpectedly(self) -> None:
        fake_config = SimpleNamespace(
            log_level="INFO",
            hub_base_url="http://telemetry-hub:8000",
            hub_ingest_key="shared-key",
            hub_ingest_path="/ingest/raw/",
            dead_letter_base_url="http://telemetry-dead-letter:8000",
            dead_letter_ingest_key="dead-letter-key",
            dead_letter_source_service="service-telemetry-listener",
            dead_letter_ingest_path="/ingest/",
        )
        fake_logger = object()

        with (
            patch.object(main_module.ListenerConfig, "from_env", return_value=fake_config),
            patch.object(main_module, "configure_logging", return_value=fake_logger),
            patch.object(main_module, "HubClient", return_value=object()) as hub_client_cls,
            patch.object(main_module, "DeadLetterClient", return_value=object()) as dead_letter_client_cls,
            patch.object(main_module, "MqttClient", return_value=object()),
            patch.object(main_module, "TelemetryListenerRuntime") as runtime_cls,
            patch("sys.stderr", new_callable=io.StringIO) as stderr,
        ):
            runtime = runtime_cls.return_value
            runtime.run.side_effect = RuntimeError("unexpected runtime abort")

            result = main_module.main()

        self.assertEqual(result, 1)
        hub_client_cls.assert_called_once_with("http://telemetry-hub:8000", "shared-key", "/ingest/raw/")
        dead_letter_client_cls.assert_called_once_with(
            "http://telemetry-dead-letter:8000",
            "dead-letter-key",
            "service-telemetry-listener",
            "/ingest/",
        )
        self.assertIn("runtime error", stderr.getvalue())
        self.assertIn("unexpected runtime abort", stderr.getvalue())
