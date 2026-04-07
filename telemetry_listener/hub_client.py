from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Mapping

import httpx

from telemetry_listener.exceptions import HubClientConfigurationError, HubIngestRequestError

INGEST_KEY_HEADER = "X-Telemetry-Ingest-Key"


class HubIngestDisposition(str, Enum):
    SUCCESS = "success"
    DROP = "drop"
    RETRY = "retry"


@dataclass(frozen=True)
class HubIngestResult:
    disposition: HubIngestDisposition
    status_code: int | None = None
    response_json: Mapping[str, Any] | None = None
    response_text: str | None = None
    error: str | None = None
    retry_cause: str | None = None

    @property
    def should_retry(self) -> bool:
        return self.disposition is HubIngestDisposition.RETRY


class HubClient:
    def __init__(
        self,
        base_url: str,
        ingest_key: str,
        ingest_path: str = "/ingest/raw/",
        timeout: float = 5.0,
        post: Callable[..., httpx.Response] | None = None,
    ) -> None:
        normalized_base_url = base_url.strip().rstrip("/")
        normalized_ingest_key = ingest_key.strip()
        normalized_ingest_path = ingest_path.strip()
        if not normalized_base_url:
            raise HubClientConfigurationError("hub base URL is required")
        if not normalized_ingest_key:
            raise HubClientConfigurationError("hub ingest key is required")
        if not normalized_ingest_path:
            raise HubClientConfigurationError("hub ingest path is required")
        self._base_url = normalized_base_url
        self._ingest_key = normalized_ingest_key
        self._ingest_path = normalized_ingest_path if normalized_ingest_path.startswith("/") else f"/{normalized_ingest_path}"
        self._timeout = timeout
        self._post = post or httpx.post

    def post_raw(self, payload: Mapping[str, Any]) -> HubIngestResult:
        if not isinstance(payload, Mapping):
            raise HubIngestRequestError("payload must be a mapping")

        url = f"{self._base_url}{self._ingest_path}"
        headers = {INGEST_KEY_HEADER: self._ingest_key}
        try:
            response = self._post(url, json=dict(payload), headers=headers, timeout=self._timeout)
        except httpx.TimeoutException as exc:
            return HubIngestResult(
                disposition=HubIngestDisposition.RETRY,
                error=f"timeout while posting raw ingest: {exc}",
                retry_cause="timeout",
            )
        except httpx.ConnectError as exc:
            return HubIngestResult(
                disposition=HubIngestDisposition.RETRY,
                error=f"connection failure while posting raw ingest: {exc}",
                retry_cause="connection_failure",
            )
        except httpx.RequestError as exc:
            return HubIngestResult(
                disposition=HubIngestDisposition.RETRY,
                error=f"request error while posting raw ingest: {exc}",
                retry_cause="connection_failure",
            )

        disposition = self._classify_status_code(response.status_code)
        response_json, response_text = self._extract_response_body(response)
        return HubIngestResult(
            disposition=disposition,
            status_code=response.status_code,
            response_json=response_json,
            response_text=response_text,
            retry_cause="hub_5xx" if disposition is HubIngestDisposition.RETRY else None,
        )

    def _classify_status_code(self, status_code: int) -> HubIngestDisposition:
        if 200 <= status_code < 300:
            return HubIngestDisposition.SUCCESS
        if 300 <= status_code < 400:
            return HubIngestDisposition.DROP
        if 400 <= status_code < 500:
            return HubIngestDisposition.DROP
        return HubIngestDisposition.RETRY

    def _extract_response_body(self, response: httpx.Response) -> tuple[Mapping[str, Any] | None, str | None]:
        try:
            parsed = response.json()
        except ValueError:
            return None, response.text
        if isinstance(parsed, Mapping):
            return parsed, None
        return {"value": parsed}, None
