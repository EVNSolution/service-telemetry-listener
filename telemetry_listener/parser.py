from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any


def _normalize_received_at(received_at: datetime | None) -> datetime:
    if received_at is None:
        return datetime.now(timezone.utc)
    if received_at.tzinfo is None:
        return received_at.replace(tzinfo=timezone.utc)
    return received_at.astimezone(timezone.utc)


def _format_received_at(received_at: datetime) -> str:
    return received_at.isoformat().replace("+00:00", "Z")


def _coerce_raw_payload(payload: str | bytes | dict[str, Any]) -> str:
    if isinstance(payload, bytes):
        return payload.decode("utf-8", errors="replace")
    if isinstance(payload, str):
        return payload
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _parse_payload_json(raw_payload: str) -> dict[str, Any]:
    try:
        parsed = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        return {
            "raw_payload": raw_payload,
            "parse_error": f"invalid JSON: {exc.msg}",
        }

    if isinstance(parsed, dict):
        return parsed

    return {
        "raw_payload": raw_payload,
        "parsed_payload": parsed,
    }


def _extract_optional_identity(payload_json: dict[str, Any]) -> tuple[str | None, str | None, str | None]:
    terminal_id = payload_json.get("terminal_id") or payload_json.get("source_terminal_id")
    vehicle_id = payload_json.get("vehicle_id") or payload_json.get("source_vehicle_id")
    message_type = payload_json.get("message_type") or payload_json.get("type")
    return terminal_id, vehicle_id, message_type


@dataclass(frozen=True)
class ParsedTelemetryEnvelope:
    message_topic: str
    received_at: datetime
    raw_payload: str
    payload_json: dict[str, Any]
    terminal_id: str | None
    vehicle_id: str | None
    message_type: str | None

    def to_hub_ingest_payload(self) -> dict[str, Any]:
        return {
            "source_terminal_id": self.terminal_id,
            "source_vehicle_id": self.vehicle_id,
            "message_topic": self.message_topic,
            "message_type": self.message_type or "unknown",
            "payload_json": self.payload_json,
            "received_at": _format_received_at(self.received_at),
        }


@dataclass(frozen=True)
class ParseFailure:
    message_topic: str
    received_at: datetime
    raw_payload: str
    error: str


def parse_message(
    topic: str,
    payload: str | bytes | dict[str, Any],
    received_at: datetime | None = None,
) -> ParsedTelemetryEnvelope | ParseFailure:
    raw_payload = _coerce_raw_payload(payload)
    normalized_received_at = _normalize_received_at(received_at)
    try:
        parsed = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        return ParseFailure(
            message_topic=topic,
            received_at=normalized_received_at,
            raw_payload=raw_payload,
            error=f"invalid JSON: {exc.msg}",
        )

    if not isinstance(parsed, dict):
        return ParseFailure(
            message_topic=topic,
            received_at=normalized_received_at,
            raw_payload=raw_payload,
            error="unsupported top-level payload shape: expected object",
        )

    payload_json = parsed
    terminal_id, vehicle_id, message_type = _extract_optional_identity(payload_json)
    return ParsedTelemetryEnvelope(
        message_topic=topic,
        received_at=normalized_received_at,
        raw_payload=raw_payload,
        payload_json=payload_json,
        terminal_id=terminal_id,
        vehicle_id=vehicle_id,
        message_type=message_type,
    )
