import unittest

from telemetry_listener.config import ConfigError, ListenerConfig


class ListenerConfigTests(unittest.TestCase):
    def test_from_env_uses_defaults_when_values_are_missing_except_required_keys(self) -> None:
        config = ListenerConfig.from_env(
            {
                "TELEMETRY_HUB_INGEST_KEY": "shared-key",
                "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER": "dead-letter-key",
            }
        )

        self.assertEqual(config.mqtt_host, "localhost")
        self.assertEqual(config.mqtt_port, 1883)
        self.assertEqual(config.mqtt_topics, ("telemetry/#",))
        self.assertEqual(config.mqtt_client_id, "service-telemetry-listener")
        self.assertEqual(config.hub_base_url, "http://service-telemetry-hub:8000")
        self.assertEqual(config.hub_ingest_path, "/ingest/raw/")
        self.assertEqual(config.hub_ingest_key, "shared-key")
        self.assertEqual(config.dead_letter_base_url, "http://service-telemetry-dead-letter:8000")
        self.assertEqual(config.dead_letter_ingest_path, "/ingest/")
        self.assertEqual(config.dead_letter_source_service, "service-telemetry-listener")
        self.assertEqual(config.dead_letter_ingest_key, "dead-letter-key")
        self.assertEqual(config.retry_count, 3)
        self.assertEqual(config.retry_backoff_seconds, 1.0)
        self.assertEqual(config.idle_sleep_seconds, 5.0)

    def test_from_env_reads_container_friendly_configuration(self) -> None:
        config = ListenerConfig.from_env(
            {
                "TELEMETRY_LISTENER_MQTT_HOST": "mqtt-broker",
                "TELEMETRY_LISTENER_MQTT_PORT": "1884",
                "TELEMETRY_LISTENER_MQTT_USERNAME": "listener",
                "TELEMETRY_LISTENER_MQTT_PASSWORD": "secret",
                "TELEMETRY_LISTENER_MQTT_TOPICS": "vehicles/telemetry, terminals/#",
                "TELEMETRY_LISTENER_CLIENT_ID": "listener-worker",
                "TELEMETRY_HUB_BASE_URL": "http://telemetry-hub:8000",
                "TELEMETRY_HUB_INGEST_PATH": "/internal/ingest/raw/",
                "TELEMETRY_HUB_INGEST_KEY": "shared-key",
                "TELEMETRY_DEAD_LETTER_BASE_URL": "http://telemetry-dead-letter:8000",
                "TELEMETRY_DEAD_LETTER_INGEST_PATH": "/internal/ingest/",
                "TELEMETRY_DEAD_LETTER_SOURCE_SERVICE": "service-telemetry-listener",
                "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER": "dead-letter-key",
                "TELEMETRY_LISTENER_RETRY_COUNT": "5",
                "TELEMETRY_LISTENER_RETRY_BACKOFF_SECONDS": "2.5",
            }
        )

        self.assertEqual(config.mqtt_host, "mqtt-broker")
        self.assertEqual(config.mqtt_port, 1884)
        self.assertEqual(config.mqtt_username, "listener")
        self.assertEqual(config.mqtt_password, "secret")
        self.assertEqual(config.mqtt_topics, ("vehicles/telemetry", "terminals/#"))
        self.assertEqual(config.mqtt_client_id, "listener-worker")
        self.assertEqual(config.hub_base_url, "http://telemetry-hub:8000")
        self.assertEqual(config.hub_ingest_path, "/internal/ingest/raw/")
        self.assertEqual(config.hub_ingest_key, "shared-key")
        self.assertEqual(config.dead_letter_base_url, "http://telemetry-dead-letter:8000")
        self.assertEqual(config.dead_letter_ingest_path, "/internal/ingest/")
        self.assertEqual(config.dead_letter_source_service, "service-telemetry-listener")
        self.assertEqual(config.dead_letter_ingest_key, "dead-letter-key")
        self.assertEqual(config.retry_count, 5)
        self.assertEqual(config.retry_backoff_seconds, 2.5)

    def test_from_env_rejects_missing_or_blank_ingest_key(self) -> None:
        with self.assertRaises(ConfigError) as missing_ctx:
            ListenerConfig.from_env({})

        with self.assertRaises(ConfigError) as blank_ctx:
            ListenerConfig.from_env(
                {
                    "TELEMETRY_HUB_INGEST_KEY": "   ",
                    "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER": "dead-letter-key",
                }
            )

        self.assertIn("TELEMETRY_HUB_INGEST_KEY", str(missing_ctx.exception))
        self.assertIn("required", str(missing_ctx.exception))
        self.assertIn("TELEMETRY_HUB_INGEST_KEY", str(blank_ctx.exception))
        self.assertIn("required", str(blank_ctx.exception))

    def test_from_env_rejects_missing_or_blank_dead_letter_listener_key(self) -> None:
        with self.assertRaises(ConfigError) as missing_ctx:
            ListenerConfig.from_env({"TELEMETRY_HUB_INGEST_KEY": "shared-key"})

        with self.assertRaises(ConfigError) as blank_ctx:
            ListenerConfig.from_env(
                {
                    "TELEMETRY_HUB_INGEST_KEY": "shared-key",
                    "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER": "   ",
                }
            )

        self.assertIn(
            "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER",
            str(missing_ctx.exception),
        )
        self.assertIn("required", str(missing_ctx.exception))
        self.assertIn(
            "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER",
            str(blank_ctx.exception),
        )
        self.assertIn("required", str(blank_ctx.exception))

    def test_from_env_rejects_missing_or_blank_dead_letter_base_url(self) -> None:
        with self.assertRaises(ConfigError) as blank_ctx:
            ListenerConfig.from_env(
                {
                    "TELEMETRY_HUB_INGEST_KEY": "shared-key",
                    "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER": "dead-letter-key",
                    "TELEMETRY_DEAD_LETTER_BASE_URL": "   ",
                    "TELEMETRY_DEAD_LETTER_SOURCE_SERVICE": "service-telemetry-listener",
                }
            )

        self.assertIn("TELEMETRY_DEAD_LETTER_BASE_URL", str(blank_ctx.exception))
        self.assertIn("required", str(blank_ctx.exception))

    def test_from_env_rejects_missing_or_blank_dead_letter_source_service(self) -> None:
        with self.assertRaises(ConfigError) as blank_ctx:
            ListenerConfig.from_env(
                {
                    "TELEMETRY_HUB_INGEST_KEY": "shared-key",
                    "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER": "dead-letter-key",
                    "TELEMETRY_DEAD_LETTER_BASE_URL": "http://telemetry-dead-letter:8000",
                    "TELEMETRY_DEAD_LETTER_SOURCE_SERVICE": "   ",
                }
            )

        self.assertIn("TELEMETRY_DEAD_LETTER_SOURCE_SERVICE", str(blank_ctx.exception))
        self.assertIn("required", str(blank_ctx.exception))

    def test_from_env_rejects_invalid_integer_env(self) -> None:
        with self.assertRaises(ConfigError) as ctx:
            ListenerConfig.from_env(
                {
                    "TELEMETRY_HUB_INGEST_KEY": "shared-key",
                    "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER": "dead-letter-key",
                    "TELEMETRY_LISTENER_MQTT_PORT": "not-an-int",
                }
            )

        self.assertIn("TELEMETRY_LISTENER_MQTT_PORT", str(ctx.exception))
        self.assertIn("integer", str(ctx.exception))

    def test_from_env_rejects_mqtt_port_outside_valid_range(self) -> None:
        with self.assertRaises(ConfigError) as ctx:
            ListenerConfig.from_env(
                {
                    "TELEMETRY_HUB_INGEST_KEY": "shared-key",
                    "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER": "dead-letter-key",
                    "TELEMETRY_LISTENER_MQTT_PORT": "0",
                }
            )

        self.assertIn("TELEMETRY_LISTENER_MQTT_PORT", str(ctx.exception))
        self.assertIn("1..65535", str(ctx.exception))

    def test_from_env_rejects_mqtt_port_above_valid_range(self) -> None:
        with self.assertRaises(ConfigError) as ctx:
            ListenerConfig.from_env(
                {
                    "TELEMETRY_HUB_INGEST_KEY": "shared-key",
                    "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER": "dead-letter-key",
                    "TELEMETRY_LISTENER_MQTT_PORT": "65536",
                }
            )

        self.assertIn("TELEMETRY_LISTENER_MQTT_PORT", str(ctx.exception))
        self.assertIn("1..65535", str(ctx.exception))

    def test_from_env_rejects_invalid_float_env(self) -> None:
        with self.assertRaises(ConfigError) as ctx:
            ListenerConfig.from_env(
                {
                    "TELEMETRY_HUB_INGEST_KEY": "shared-key",
                    "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER": "dead-letter-key",
                    "TELEMETRY_LISTENER_IDLE_SLEEP_SECONDS": "oops",
                }
            )

        self.assertIn("TELEMETRY_LISTENER_IDLE_SLEEP_SECONDS", str(ctx.exception))
        self.assertIn("number", str(ctx.exception))

    def test_from_env_rejects_invalid_retry_count(self) -> None:
        with self.assertRaises(ConfigError) as ctx:
            ListenerConfig.from_env(
                {
                    "TELEMETRY_HUB_INGEST_KEY": "shared-key",
                    "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER": "dead-letter-key",
                    "TELEMETRY_LISTENER_RETRY_COUNT": "not-a-number",
                }
            )

        self.assertIn("TELEMETRY_LISTENER_RETRY_COUNT", str(ctx.exception))
        self.assertIn("integer", str(ctx.exception))

    def test_from_env_rejects_negative_retry_count(self) -> None:
        with self.assertRaises(ConfigError) as ctx:
            ListenerConfig.from_env(
                {
                    "TELEMETRY_HUB_INGEST_KEY": "shared-key",
                    "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER": "dead-letter-key",
                    "TELEMETRY_LISTENER_RETRY_COUNT": "-1",
                }
            )

        self.assertIn("TELEMETRY_LISTENER_RETRY_COUNT", str(ctx.exception))
        self.assertIn("0 or greater", str(ctx.exception))

    def test_from_env_rejects_invalid_retry_backoff(self) -> None:
        with self.assertRaises(ConfigError) as ctx:
            ListenerConfig.from_env(
                {
                    "TELEMETRY_HUB_INGEST_KEY": "shared-key",
                    "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER": "dead-letter-key",
                    "TELEMETRY_LISTENER_RETRY_BACKOFF_SECONDS": "oops",
                }
            )

        self.assertIn("TELEMETRY_LISTENER_RETRY_BACKOFF_SECONDS", str(ctx.exception))
        self.assertIn("number", str(ctx.exception))

    def test_from_env_rejects_non_finite_retry_backoff_and_idle_sleep_values(self) -> None:
        with self.assertRaises(ConfigError) as retry_backoff_ctx:
            ListenerConfig.from_env(
                {
                    "TELEMETRY_HUB_INGEST_KEY": "shared-key",
                    "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER": "dead-letter-key",
                    "TELEMETRY_LISTENER_RETRY_BACKOFF_SECONDS": "nan",
                }
            )

        with self.assertRaises(ConfigError) as idle_sleep_ctx:
            ListenerConfig.from_env(
                {
                    "TELEMETRY_HUB_INGEST_KEY": "shared-key",
                    "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER": "dead-letter-key",
                    "TELEMETRY_LISTENER_IDLE_SLEEP_SECONDS": "inf",
                }
            )

        self.assertIn("TELEMETRY_LISTENER_RETRY_BACKOFF_SECONDS", str(retry_backoff_ctx.exception))
        self.assertIn("finite", str(retry_backoff_ctx.exception))
        self.assertIn("TELEMETRY_LISTENER_IDLE_SLEEP_SECONDS", str(idle_sleep_ctx.exception))
        self.assertIn("finite", str(idle_sleep_ctx.exception))

    def test_from_env_rejects_non_positive_idle_sleep_seconds(self) -> None:
        with self.assertRaises(ConfigError) as ctx:
            ListenerConfig.from_env(
                {
                    "TELEMETRY_HUB_INGEST_KEY": "shared-key",
                    "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER": "dead-letter-key",
                    "TELEMETRY_LISTENER_IDLE_SLEEP_SECONDS": "0",
                }
            )

        self.assertIn("TELEMETRY_LISTENER_IDLE_SLEEP_SECONDS", str(ctx.exception))
        self.assertIn("greater than 0", str(ctx.exception))

    def test_from_env_rejects_blank_topic_configuration(self) -> None:
        with self.assertRaises(ConfigError) as ctx:
            ListenerConfig.from_env(
                {
                    "TELEMETRY_HUB_INGEST_KEY": "shared-key",
                    "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER": "dead-letter-key",
                    "TELEMETRY_LISTENER_MQTT_TOPICS": "   ,  ",
                }
            )

        self.assertIn("TELEMETRY_LISTENER_MQTT_TOPICS", str(ctx.exception))
        self.assertIn("at least one topic", str(ctx.exception))

    def test_from_env_rejects_invalid_log_level(self) -> None:
        with self.assertRaises(ConfigError) as ctx:
            ListenerConfig.from_env(
                {
                    "TELEMETRY_HUB_INGEST_KEY": "shared-key",
                    "TELEMETRY_DEAD_LETTER_KEY_SERVICE_TELEMETRY_LISTENER": "dead-letter-key",
                    "TELEMETRY_LISTENER_LOG_LEVEL": "NOTALEVEL",
                }
            )

        self.assertIn("TELEMETRY_LISTENER_LOG_LEVEL", str(ctx.exception))
        self.assertIn("valid logging level", str(ctx.exception))
