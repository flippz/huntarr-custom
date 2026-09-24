"""Shared "is this library ready to talk to Sonarr" validation.

Used by both ``SonarrScanService`` (read-only scan) and
``DispatchPlanningService``/``DispatchService`` (manual search dispatch)
so the two features apply identical rules and error messages: the
library must exist, be a ``sonarr``-type library, be enabled, and have a
non-empty API key.
"""
from ..persistence.library_repository import LibraryRepository

LIBRARY_NOT_FOUND_ERROR = "library not found"
UNSUPPORTED_LIBRARY_TYPE_ERROR = "unsupported library type: only sonarr libraries support scanning"
LIBRARY_DISABLED_ERROR = "library is disabled"
MISSING_API_KEY_ERROR = "library is missing an API key"

# Validation-style errors (as opposed to upstream Sonarr errors) that the
# API layer maps to 4xx instead of 502.
VALIDATION_ERRORS = {
    LIBRARY_NOT_FOUND_ERROR,
    UNSUPPORTED_LIBRARY_TYPE_ERROR,
    LIBRARY_DISABLED_ERROR,
    MISSING_API_KEY_ERROR,
}


def ready_sonarr_library(library_repo: LibraryRepository, library_id: int, *, conn=None):
    """Return ``(library, None)`` or ``(None, error_message)``."""
    library = library_repo.get(library_id, conn=conn)
    if library is None:
        return None, LIBRARY_NOT_FOUND_ERROR
    if library.type != "sonarr":
        return None, UNSUPPORTED_LIBRARY_TYPE_ERROR
    if not library.enabled:
        return None, LIBRARY_DISABLED_ERROR
    if not library.api_key or not library.api_key.strip():
        return None, MISSING_API_KEY_ERROR
    return library, None
