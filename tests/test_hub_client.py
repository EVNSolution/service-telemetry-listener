import unittest
from uuid import uuid4

import httpx

from telemetry_listener.exceptions import HubClientConfigurationError, HubIngestRequestError
from telemetry_listener.hub_client import HubClient, HubIngestDisposition


class HubClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.payload = {
            "source_terminal_id": str(uuid4()),
            "source_vehicle_id": str(uuid4()),
            "message_topic": "vehicles/telemetry",
            "message_type": "location_update",
            "payload_json": {"captured_at": "2026-03-21T09:00:00Z"},
            "received_at": "2026-03-21T09:00:05Z",
        }

    def test_post_raw_returns_success_for_2xx_response(self) -> None:
        calls: dict[str, object] = {}

        def fake_post(url, json, headers, timeout):  # noqa: A002
            calls["url"] = url
            calls["json"] = json
            calls["headers"] = headers
            calls["timeout"] = timeout
            return httpx.Response(201, json={"telemetry_raw_ingest_id": str(uuid4())})

        client = HubClient("http://telemetry-hub:8000", "shared-key", post=fake_post)

        result = client.post_raw(self.payload)

        self.assertEqual(result.disposition, HubIngestDisposition.SUCCESS)
        self.assertEqual(result.status_code, 201)
        self.assertEqual(calls["url"], "http://telemetry-hub:8000/ingest/raw/")
        self.assertEqual(calls["json"], self.payload)
        self.assertEqual(calls["headers"]["X-Telemetry-Ingest-Key"], "shared-key")

    def test_post_raw_returns_drop_for_4xx_response(self) -> None:
        client = HubClient(
            "http://telemetry-hub:8000",
            "shared-key",
            post=lambda url, json, headers, timeout: httpx.Response(400, json={"detail": "bad request"}),
        )

        result = client.post_raw(self.payload)

        self.assertEqual(result.disposition, HubIngestDisposition.DROP)
        self.assertEqual(result.status_code, 400)
        self.assertFalse(result.should_retry)

    def test_post_raw_returns_drop_for_3xx_response(self) -> None:
        client = HubClient(
            "http://telemetry-hub:8000",
            "shared-key",
            post=lambda url, json, headers, timeout: httpx.Response(307, headers={"location": "/ingest/raw/"}),
        )

        result = client.post_raw(self.payload)

        self.assertEqual(result.disposition, HubIngestDisposition.DROP)
        self.assertEqual(result.status_code, 307)
        self.assertFalse(result.should_retry)

    def test_post_raw_returns_retry_for_5xx_response(self) -> None:
        client = HubClient(
            "http://telemetry-hub:8000",
            "shared-key",
            post=lambda url, json, headers, timeout: httpx.Response(503, json={"detail": "unavailable"}),
        )

        result = client.post_raw(self.payload)

        self.assertEqual(result.disposition, HubIngestDisposition.RETRY)
        self.assertEqual(result.status_code, 503)
        self.assertTrue(result.should_retry)
        self.assertEqual(result.retry_cause, "hub_5xx")

    def test_post_raw_returns_retry_for_timeout(self) -> None:
        request = httpx.Request("POST", "http://telemetry-hub:8000/ingest/raw/")

        def fake_post(url, json, headers, timeout):  # noqa: A002
            raise httpx.TimeoutException("request timed out", request=request)

        client = HubClient("http://telemetry-hub:8000", "shared-key", post=fake_post)

        result = client.post_raw(self.payload)

        self.assertEqual(result.disposition, HubIngestDisposition.RETRY)
        self.assertIsNone(result.status_code)
        self.assertIn("timeout", result.error or "")
        self.assertEqual(result.retry_cause, "timeout")

    def test_post_raw_returns_retry_for_connection_failure(self) -> None:
        request = httpx.Request("POST", "http://telemetry-hub:8000/ingest/raw/")

        def fake_post(url, json, headers, timeout):  # noqa: A002
            raise httpx.ConnectError("connection failed", request=request)

        client = HubClient("http://telemetry-hub:8000", "shared-key", post=fake_post)

        result = client.post_raw(self.payload)

        self.assertEqual(result.disposition, HubIngestDisposition.RETRY)
        self.assertIsNone(result.status_code)
        self.assertIn("connection", result.error or "")
        self.assertEqual(result.retry_cause, "connection_failure")

    def test_post_raw_returns_retry_for_request_error(self) -> None:
        request = httpx.Request("POST", "http://telemetry-hub:8000/ingest/raw/")

        def fake_post(url, json, headers, timeout):  # noqa: A002
            raise httpx.RemoteProtocolError("protocol error", request=request)

        client = HubClient("http://telemetry-hub:8000", "shared-key", post=fake_post)

        result = client.post_raw(self.payload)

        self.assertEqual(result.disposition, HubIngestDisposition.RETRY)
        self.assertIsNone(result.status_code)
        self.assertIn("request error", result.error or "")
        self.assertEqual(result.retry_cause, "connection_failure")

    def test_post_raw_rejects_non_mapping_payload(self) -> None:
        client = HubClient("http://telemetry-hub:8000", "shared-key")

        with self.assertRaises(HubIngestRequestError):
            client.post_raw(["not", "a", "mapping"])  # type: ignore[arg-type]

    def test_post_raw_normalizes_non_mapping_json_response_body(self) -> None:
        client = HubClient(
            "http://telemetry-hub:8000",
            "shared-key",
            post=lambda url, json, headers, timeout: httpx.Response(200, json=["queued", "accepted"]),
        )

        result = client.post_raw(self.payload)

        self.assertEqual(result.disposition, HubIngestDisposition.SUCCESS)
        self.assertEqual(result.response_json, {"value": ["queued", "accepted"]})
        self.assertIsNone(result.response_text)

    def test_post_raw_returns_response_text_for_non_json_response_body(self) -> None:
        client = HubClient(
            "http://telemetry-hub:8000",
            "shared-key",
            post=lambda url, json, headers, timeout: httpx.Response(
                500,
                content=b"temporary upstream failure",
                headers={"content-type": "text/plain"},
            ),
        )

        result = client.post_raw(self.payload)

        self.assertEqual(result.disposition, HubIngestDisposition.RETRY)
        self.assertEqual(result.response_json, None)
        self.assertEqual(result.response_text, "temporary upstream failure")

    def test_hub_client_requires_non_blank_base_url_and_ingest_key(self) -> None:
        with self.assertRaises(HubClientConfigurationError):
            HubClient("", "shared-key")

        with self.assertRaises(HubClientConfigurationError):
            HubClient("http://telemetry-hub:8000", "   ")

    def test_post_raw_uses_custom_ingest_path(self) -> None:
        calls: dict[str, object] = {}

        def fake_post(url, json, headers, timeout):  # noqa: A002
            calls["url"] = url
            return httpx.Response(201, json={"telemetry_raw_ingest_id": str(uuid4())})

        client = HubClient(
            "http://telemetry-hub:8000",
            "shared-key",
            ingest_path="/internal/ingest/raw/",
            post=fake_post,
        )

        result = client.post_raw(self.payload)

        self.assertEqual(result.disposition, HubIngestDisposition.SUCCESS)
        self.assertEqual(calls["url"], "http://telemetry-hub:8000/internal/ingest/raw/")
