from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Mapping

import httpx

DEAD_LETTER_KEY_HEADER = "X-Telemetry-Dead-Letter-Key"


class DeadLetterWriteDisposition(str, Enum):
    SUCCESS = "success"
    DROP = "drop"


@dataclass(frozen=True)
class DeadLetterWriteResult:
    disposition: DeadLetterWriteDisposition
    status_code: int | None = None
    response_json: Mapping[str, Any] | None = None
    response_text: str | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is DeadLetterWriteDisposition.SUCCESS


class DeadLetterClient:
    def __init__(
        self,
        base_url: str,
        ingest_key: str,
        source_service: str,
        ingest_path: str = "/ingest/",
        timeout: float = 5.0,
        post: Callable[..., httpx.Response] | None = None,
    ) -> None:
        self._base_url = base_url.strip().rstrip("/")
        self._ingest_key = ingest_key.strip()
        self._source_service = source_service.strip()
        self._ingest_path = ingest_path.strip()
        self._timeout = timeout
        self._post = post or httpx.post

    def post_ingest(self, payload: Mapping[str, Any]) -> DeadLetterWriteResult:
        url = f"{self._base_url}{self._normalized_ingest_path}"
        headers = {DEAD_LETTER_KEY_HEADER: self._ingest_key}
        response = self._post(
            url,
            json={**dict(payload), "source_service": self._source_service},
            headers=headers,
            timeout=self._timeout,
        )
        response_json, response_text = self._extract_response_body(response)
        return DeadLetterWriteResult(
            disposition=self._classify_status_code(response.status_code),
            status_code=response.status_code,
            response_json=response_json,
            response_text=response_text,
        )

    @property
    def _normalized_ingest_path(self) -> str:
        if self._ingest_path.startswith("/"):
            return self._ingest_path
        return f"/{self._ingest_path}"

    def _classify_status_code(self, status_code: int) -> DeadLetterWriteDisposition:
        if 200 <= status_code < 300:
            return DeadLetterWriteDisposition.SUCCESS
        return DeadLetterWriteDisposition.DROP

    def _extract_response_body(self, response: httpx.Response) -> tuple[Mapping[str, Any] | None, str | None]:
        try:
            parsed = response.json()
        except ValueError:
            return None, response.text
        if isinstance(parsed, Mapping):
            return parsed, None
        return {"value": parsed}, None
