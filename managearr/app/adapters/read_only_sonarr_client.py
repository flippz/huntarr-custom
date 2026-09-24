"""A read-only guard around ``SonarrClient`` for the M5 refresh worker.

``SonarrScanService`` and ``ReconciliationService`` already never call
``search_episodes`` (see their module docstrings), but the scheduled refresh
worker is unattended and long-lived, so it gets an extra, structural
safeguard: this wrapper only forwards the handful of GET-based methods those
two services use. Any other attribute - including ``search_episodes`` and any
future write method added to ``SonarrClient`` - raises
``ReadOnlySonarrAdapterError`` instead of reaching the network layer.
"""
from .sonarr_client import SonarrClient

_ALLOWED_METHODS = frozenset({
    "system_status", "get_series", "get_episodes",
    "get_command", "get_history", "get_queue_details",
})


class ReadOnlySonarrAdapterError(Exception):
    """Raised when scheduled read-only refresh code attempts a Sonarr write
    or any other operation this adapter does not explicitly allow."""


class ReadOnlySonarrClient:
    """Read-only facade with the same constructor shape as ``SonarrClient``."""

    def __init__(self, base_url: str, api_key: str, *, timeout: int | None = None, session=None):
        kwargs: dict = {}
        if timeout is not None:
            kwargs["timeout"] = timeout
        if session is not None:
            kwargs["session"] = session
        self._client = SonarrClient(base_url, api_key, **kwargs)

    def __getattr__(self, name: str):
        if name in _ALLOWED_METHODS:
            return getattr(self._client, name)
        raise ReadOnlySonarrAdapterError(
            f"read-only Sonarr adapter does not expose '{name}'; only GET-based methods are available"
        )
