import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest

from app import create_app
from app.persistence.database import Database
from app.persistence.library_repository import LibraryRepository
from app.persistence.policy_repository import PolicyRepository
from app.persistence.activity_repository import ActivityRepository


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "huntarr_v2_test.db")


@pytest.fixture
def app(db_path):
    application = create_app({"DB_PATH": db_path, "TESTING": True})
    yield application


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def database(db_path):
    db = Database(db_path)
    db.init_schema()
    return db


@pytest.fixture
def library_repo(database):
    return LibraryRepository(database)


@pytest.fixture
def policy_repo(database):
    return PolicyRepository(database)


@pytest.fixture
def activity_repo(database):
    return ActivityRepository(database)
