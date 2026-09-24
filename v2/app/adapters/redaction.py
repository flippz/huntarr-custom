"""Boundary adapter that strips secrets before data crosses into an
HTTP response.

``ArrLibrary.api_key`` must never appear in list/detail API output. We
don't just mask it (e.g. "abcd****") because a partial key is still a
partial secret - instead we drop the field entirely and expose a
boolean so the UI can show "API key configured" without ever handling
the value.
"""
from ..domain.arr_library import ArrLibrary


def redact_library(library: ArrLibrary) -> dict:
    data = library.to_dict()
    has_api_key = bool(data.pop("api_key", None))
    data["has_api_key"] = has_api_key
    return data


def redact_libraries(libraries: list[ArrLibrary]) -> list[dict]:
    return [redact_library(lib) for lib in libraries]
