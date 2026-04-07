from __future__ import annotations

import unittest
from uuid import uuid4

import httpx

from telemetry_listener.dead_letter_client import (
    DEAD_LETTER_KEY_HEADER,
    DeadLetterClient,
    DeadLetterWriteDisposition,
)


class DeadLetterClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.payload = {
            "message_topic": "vehicles/telemetry",
            "source_terminal_id": str(uuid4()),
            "source_vehicle_id": str(uuid4()),
            "message_type": "location_update",
            "payload_json": {"captured_at": "2026-03-21T09:00:00Z"},
            "received_at": "2026-03-21T09:00:05Z",
            "failure_class": "hub_4xx",
            "error_message": "invalid payload",
            "retry_attempts": 1,
            "failure_fingerprint": "abc123",
        }

    def test_post_ingest_sends_source_service_and_listener_key(self) -> None:
        calls: dict[str, object] = {}

        def fake_post(url, json, headers, timeout):  # noqa: A002
            calls["url"] = url
            calls["json"] = json
            calls["headers"] = headers
            calls["timeout"] = timeout
            return httpx.Response(201, json={"telemetry_dead_letter_id": str(uuid4())})

        client = DeadLetterClient(
            base_url="http://telemetry-dead-letter:8000",
            ingest_key="listener-dead-letter-key",
            source_service="service-telemetry-listener",
            post=fake_post,
        )

        result = client.post_ingest(self.payload)

        self.assertEqual(result.disposition, DeadLetterWriteDisposition.SUCCESS)
        self.assertEqual(calls["url"], "http://telemetry-dead-letter:8000/ingest/")
        self.assertEqual(calls["headers"][DEAD_LETTER_KEY_HEADER], "listener-dead-letter-key")
        self.assertEqual(calls["json"]["source_service"], "service-telemetry-listener")
        self.assertEqual(calls["json"]["failure_class"], "hub_4xx")

    def test_post_ingest_returns_drop_for_non_2xx_response(self) -> None:
        client = DeadLetterClient(
            base_url="http://telemetry-dead-letter:8000",
            ingest_key="listener-dead-letter-key",
            source_service="service-telemetry-listener",
            post=lambda url, json, headers, timeout: httpx.Response(503, json={"detail": "unavailable"}),
        )

        result = client.post_ingest(self.payload)

        self.assertEqual(result.disposition, DeadLetterWriteDisposition.DROP)
        self.assertEqual(result.status_code, 503)
        self.assertFalse(result.accepted)

