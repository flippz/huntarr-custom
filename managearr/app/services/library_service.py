"""Application service orchestrating ArrLibrary CRUD + validation."""
from ..domain.arr_library import ArrLibrary, validate_library_input
from ..persistence.library_repository import LibraryRepository


class LibraryService:
    def __init__(self, repository: LibraryRepository):
        self.repository = repository

    def list_libraries(self) -> list[ArrLibrary]:
        return self.repository.list_all()

    def get_library(self, library_id: int) -> ArrLibrary | None:
        return self.repository.get(library_id)

    def create_library(self, data: dict) -> tuple[ArrLibrary | None, list[str]]:
        errors = validate_library_input(data, partial=False)
        if errors:
            return None, errors
        clean = {
            "name": data["name"].strip(),
            "type": data["type"],
            "url": data["url"].strip(),
            "api_key": data["api_key"],
            "enabled": data.get("enabled", True),
        }
        return self.repository.create(clean), []

    def update_library(self, library_id: int, data: dict) -> tuple[ArrLibrary | None, list[str]]:
        if self.repository.get(library_id) is None:
            return None, ["library not found"]

        errors = validate_library_input(data, partial=True)
        if errors:
            return None, errors

        clean = dict(data)
        if "name" in clean:
            clean["name"] = clean["name"].strip()
        if "url" in clean:
            clean["url"] = clean["url"].strip()

        return self.repository.update(library_id, clean), []

    def delete_library(self, library_id: int) -> bool:
        return self.repository.delete(library_id)

    def counts(self) -> dict:
        return self.repository.counts()
