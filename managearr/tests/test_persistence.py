import os
import sqlite3

from app.domain.automation_policy import BALANCED_DEFAULTS
from app.persistence.database import Database


def test_database_health_check_ok(database):
    assert database.health_check() is True


def test_database_creates_directory_and_file(db_path, database):
    database.init_schema()
    assert os.path.exists(db_path)


def test_database_migrates_preview_activity_table(tmp_path):
    db_path = tmp_path / "preview.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE activity_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                library_id INTEGER,
                library_name TEXT NOT NULL,
                state TEXT NOT NULL,
                title TEXT NOT NULL,
                details TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO activity_jobs
                (library_name, state, title, created_at, updated_at)
            VALUES ('Sonarr', 'completed', 'Preview job', 'now', 'now')
            """
        )

    database = Database(str(db_path))
    database.init_schema()

    with database.connect() as conn:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(activity_jobs)")}
        migrated = conn.execute(
            "SELECT job_type, candidate_count FROM activity_jobs WHERE title = 'Preview job'"
        ).fetchone()

    assert {"job_type", "candidate_count"}.issubset(columns)
    assert dict(migrated) == {"job_type": "legacy", "candidate_count": 0}


def test_library_repository_crud_round_trip(library_repo):
    created = library_repo.create(
        {"name": "Lidarr", "type": "lidarr", "url": "http://lidarr:8686", "api_key": "key", "enabled": True}
    )
    assert created.id is not None
    assert created.created_at == created.updated_at

    fetched = library_repo.get(created.id)
    assert fetched.name == "Lidarr"

    updated = library_repo.update(created.id, {"name": "Lidarr Renamed"})
    assert updated.name == "Lidarr Renamed"
    assert updated.updated_at >= created.updated_at

    all_libs = library_repo.list_all()
    assert len(all_libs) == 1

    assert library_repo.delete(created.id) is True
    assert library_repo.get(created.id) is None
    assert library_repo.delete(created.id) is False


def test_library_repository_counts(library_repo):
    library_repo.create({"name": "A", "type": "sonarr", "url": "http://a", "api_key": "k", "enabled": True})
    library_repo.create({"name": "B", "type": "radarr", "url": "http://b", "api_key": "k", "enabled": False})
    assert library_repo.counts() == {"configured": 2, "enabled": 1}


def test_policy_repository_seeds_balanced_defaults(policy_repo):
    policy = policy_repo.get()
    for key, value in BALANCED_DEFAULTS.items():
        assert getattr(policy, key) == value
    assert policy.updated_at


def test_policy_repository_persists_across_instances(database):
    from app.persistence.policy_repository import PolicyRepository

    repo_a = PolicyRepository(database)
    repo_a.update({"hourly_api_cap": 42})

    repo_b = PolicyRepository(database)
    policy = repo_b.get()
    assert policy.hourly_api_cap == 42


def test_activity_repository_starts_empty(activity_repo):
    assert activity_repo.list_all() == []


def test_activity_repository_create_and_filter(activity_repo):
    activity_repo.create(
        {"library_id": None, "library_name": "Sonarr", "state": "planned", "title": "Episode S01E01"}
    )
    activity_repo.create(
        {"library_id": None, "library_name": "Radarr", "state": "completed", "title": "Some Movie"}
    )

    all_jobs = activity_repo.list_all()
    assert len(all_jobs) == 2

    planned_only = activity_repo.list_all(state="planned")
    assert len(planned_only) == 1
    assert planned_only[0].title == "Episode S01E01"


def test_no_seed_data_in_fresh_app(client):
    resp = client.get("/api/v1/activity")
    assert resp.get_json() == {"jobs": []}
