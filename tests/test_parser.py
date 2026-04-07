import json
from datetime import datetime, timezone
import unittest

from telemetry_listener.parser import ParseFailure, ParsedTelemetryEnvelope, parse_message


class MessageParserTests(unittest.TestCase):
    def setUp(self) -> None:
        self.received_at = datetime(2026, 3, 21, 9, 0, 5, tzinfo=timezone.utc)

    def test_parse_message_preserves_full_identity_and_builds_hub_envelope(self) -> None:
        envelope = parse_message(
            topic="vehicles/telemetry",
            payload=json.dumps(
                {
                    "terminal_id": "70000000-0000-0000-0000-000000000001",
                    "vehicle_id": "50000000-0000-0000-0000-000000000001",
                    "message_type": "location_update",
                    "captured_at": "2026-03-21T09:00:00Z",
                }
            ),
            received_at=self.received_at,
        )

        self.assertIsInstance(envelope, ParsedTelemetryEnvelope)
        self.assertEqual(envelope.message_topic, "vehicles/telemetry")
        self.assertEqual(envelope.received_at, self.received_at)
        self.assertEqual(envelope.terminal_id, "70000000-0000-0000-0000-000000000001")
        self.assertEqual(envelope.vehicle_id, "50000000-0000-0000-0000-000000000001")
        self.assertEqual(envelope.message_type, "location_update")
        self.assertEqual(envelope.payload_json["captured_at"], "2026-03-21T09:00:00Z")
        self.assertEqual(
            envelope.to_hub_ingest_payload(),
            {
                "source_terminal_id": "70000000-0000-0000-0000-000000000001",
                "source_vehicle_id": "50000000-0000-0000-0000-000000000001",
                "message_topic": "vehicles/telemetry",
                "message_type": "location_update",
                "payload_json": {
                    "terminal_id": "70000000-0000-0000-0000-000000000001",
                    "vehicle_id": "50000000-0000-0000-0000-000000000001",
                    "message_type": "location_update",
                    "captured_at": "2026-03-21T09:00:00Z",
                },
                "received_at": "2026-03-21T09:00:05Z",
            },
        )

    def test_parse_message_supports_bytes_payload_and_alias_identity_fields(self) -> None:
        envelope = parse_message(
            topic="vehicles/telemetry",
            payload=(
                b"{"
                b'"source_terminal_id":"70000000-0000-0000-0000-000000000001",'
                b'"source_vehicle_id":"50000000-0000-0000-0000-000000000001",'
                b'"type":"location_update"'
                b"}"
            ),
            received_at=self.received_at,
        )

        self.assertIsInstance(envelope, ParsedTelemetryEnvelope)
        self.assertEqual(envelope.raw_payload, '{"source_terminal_id":"70000000-0000-0000-0000-000000000001","source_vehicle_id":"50000000-0000-0000-0000-000000000001","type":"location_update"}')
        self.assertEqual(envelope.terminal_id, "70000000-0000-0000-0000-000000000001")
        self.assertEqual(envelope.vehicle_id, "50000000-0000-0000-0000-000000000001")
        self.assertEqual(envelope.message_type, "location_update")
        self.assertEqual(envelope.to_hub_ingest_payload()["received_at"], "2026-03-21T09:00:05Z")

    def test_parse_message_keeps_terminal_identity_when_vehicle_id_is_missing(self) -> None:
        envelope = parse_message(
            topic="vehicles/telemetry",
            payload=json.dumps(
                {
                    "terminal_id": "70000000-0000-0000-0000-000000000001",
                    "message_type": "location_update",
                }
            ),
            received_at=self.received_at,
        )

        self.assertIsInstance(envelope, ParsedTelemetryEnvelope)
        self.assertEqual(envelope.terminal_id, "70000000-0000-0000-0000-000000000001")
        self.assertIsNone(envelope.vehicle_id)
        self.assertEqual(envelope.to_hub_ingest_payload()["source_terminal_id"], "70000000-0000-0000-0000-000000000001")
        self.assertIsNone(envelope.to_hub_ingest_payload()["source_vehicle_id"])

    def test_parse_message_keeps_vehicle_identity_when_terminal_id_is_missing(self) -> None:
        envelope = parse_message(
            topic="vehicles/telemetry",
            payload=json.dumps(
                {
                    "vehicle_id": "50000000-0000-0000-0000-000000000001",
                    "message_type": "location_update",
                }
            ),
            received_at=self.received_at,
        )

        self.assertIsInstance(envelope, ParsedTelemetryEnvelope)
        self.assertIsNone(envelope.terminal_id)
        self.assertEqual(envelope.vehicle_id, "50000000-0000-0000-0000-000000000001")
        self.assertIsNone(envelope.to_hub_ingest_payload()["source_terminal_id"])
        self.assertEqual(envelope.to_hub_ingest_payload()["source_vehicle_id"], "50000000-0000-0000-0000-000000000001")

    def test_parse_message_keeps_raw_ingest_possible_when_identity_is_missing(self) -> None:
        envelope = parse_message(
            topic="vehicles/telemetry",
            payload=json.dumps({"captured_at": "2026-03-21T09:00:00Z"}),
            received_at=self.received_at,
        )

        self.assertIsInstance(envelope, ParsedTelemetryEnvelope)
        self.assertIsNone(envelope.terminal_id)
        self.assertIsNone(envelope.vehicle_id)
        self.assertEqual(envelope.to_hub_ingest_payload()["message_type"], "unknown")
        self.assertEqual(envelope.to_hub_ingest_payload()["payload_json"]["captured_at"], "2026-03-21T09:00:00Z")

    def test_parse_message_returns_parse_failure_for_malformed_json(self) -> None:
        outcome = parse_message(
            topic="vehicles/telemetry",
            payload="{not-json",
            received_at=self.received_at,
        )

        self.assertIsInstance(outcome, ParseFailure)
        self.assertEqual(outcome.message_topic, "vehicles/telemetry")
        self.assertEqual(outcome.received_at, self.received_at)
        self.assertEqual(outcome.raw_payload, "{not-json")
        self.assertIn("invalid JSON", outcome.error)

    def test_parse_message_returns_parse_failure_for_non_object_json(self) -> None:
        outcome = parse_message(
            topic="vehicles/telemetry",
            payload='["not","an","object"]',
            received_at=self.received_at,
        )

        self.assertIsInstance(outcome, ParseFailure)
        self.assertEqual(outcome.message_topic, "vehicles/telemetry")
        self.assertEqual(outcome.received_at, self.received_at)
        self.assertEqual(outcome.raw_payload, '["not","an","object"]')
        self.assertIn("top-level", outcome.error)
