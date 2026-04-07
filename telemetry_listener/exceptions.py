class HubClientError(Exception):
    """Base exception for telemetry hub client failures."""


class HubClientConfigurationError(HubClientError):
    """Raised when the hub client is misconfigured."""


class HubIngestRequestError(HubClientError):
    """Raised when the ingest request payload is invalid."""
