"""Domain model for a configured *Arr library connection."""
from dataclasses import dataclass
from typing import Optional

# The six *Arr ecosystems Managearr v1 can talk to.
ARR_TYPES = ("sonarr", "radarr", "lidarr", "readarr", "whisparr", "eros")


class ArrType:
    SONARR = "sonarr"
    RADARR = "radarr"
    LIDARR = "lidarr"
    READARR = "readarr"
    WHISPARR = "whisparr"
    EROS = "eros"


@dataclass
class ArrLibrary:
    """A single configured connection to an *Arr instance.

    ``api_key`` is sensitive and must never be serialized into an API
    response body for list/detail endpoints - see
    ``app.adapters.redaction``.
    """

    id: Optional[int]
    name: str
    type: str
    url: str
    api_key: str
    enabled: bool
    created_at: str
    updated_at: str

    def to_dict(self) -> dict:
        """Full internal representation, including the secret api_key.

        Only used for persistence / internal service calls - never return
        this directly from an HTTP handler.
        """
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "url": self.url,
            "api_key": self.api_key,
            "enabled": self.enabled,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


def validate_library_input(data: dict, *, partial: bool = False) -> list[str]:
    """Validate raw input for creating/updating an ArrLibrary.

    ``partial=True`` allows a subset of fields (used for PATCH-style
    updates); any field present is still validated.
    """
    errors: list[str] = []

    def required(field: str) -> bool:
        if partial:
            return field in data
        return True

    if required("name"):
        name = data.get("name")
        if not isinstance(name, str) or not name.strip():
            errors.append("name is required and must be a non-empty string")

    if required("type"):
        arr_type = data.get("type")
        if arr_type not in ARR_TYPES:
            errors.append(f"type must be one of: {', '.join(ARR_TYPES)}")

    if required("url"):
        url = data.get("url")
        if not isinstance(url, str) or not (url.startswith("http://") or url.startswith("https://")):
            errors.append("url is required and must start with http:// or https://")

    if required("api_key"):
        api_key = data.get("api_key")
        if not isinstance(api_key, str) or not api_key.strip():
            errors.append("api_key is required and must be a non-empty string")

    if "enabled" in data and not isinstance(data.get("enabled"), bool):
        errors.append("enabled must be a boolean")

    return errors
