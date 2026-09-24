"""Tests for file-based secret resolution (config.read_secret).

No hardcoded/default password is ever used to reach PostgreSQL - the
password always comes from MANAGEARR_DB_PASSWORD_FILE (preferred) or
MANAGEARR_DB_PASSWORD, resolved by this helper.
"""
import pytest

from config import read_secret


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("MANAGEARR_TEST_SECRET", raising=False)
    monkeypatch.delenv("MANAGEARR_TEST_SECRET_FILE", raising=False)


def test_reads_from_file_when_file_env_set(tmp_path, monkeypatch):
    secret_file = tmp_path / "password.txt"
    secret_file.write_text("s3cr3t-from-file\n")
    monkeypatch.setenv("MANAGEARR_TEST_SECRET_FILE", str(secret_file))

    assert read_secret("MANAGEARR_TEST_SECRET", "MANAGEARR_TEST_SECRET_FILE") == "s3cr3t-from-file"


def test_file_strips_surrounding_whitespace(tmp_path, monkeypatch):
    secret_file = tmp_path / "password.txt"
    secret_file.write_text("  padded-secret  \n\n")
    monkeypatch.setenv("MANAGEARR_TEST_SECRET_FILE", str(secret_file))

    assert read_secret("MANAGEARR_TEST_SECRET", "MANAGEARR_TEST_SECRET_FILE") == "padded-secret"


def test_file_takes_precedence_over_plain_env_var(tmp_path, monkeypatch):
    secret_file = tmp_path / "password.txt"
    secret_file.write_text("from-file")
    monkeypatch.setenv("MANAGEARR_TEST_SECRET_FILE", str(secret_file))
    monkeypatch.setenv("MANAGEARR_TEST_SECRET", "from-plain-env")

    assert read_secret("MANAGEARR_TEST_SECRET", "MANAGEARR_TEST_SECRET_FILE") == "from-file"


def test_falls_back_to_plain_env_var_when_no_file_configured(monkeypatch):
    monkeypatch.setenv("MANAGEARR_TEST_SECRET", "from-plain-env")

    assert read_secret("MANAGEARR_TEST_SECRET", "MANAGEARR_TEST_SECRET_FILE") == "from-plain-env"


def test_returns_default_when_neither_is_set():
    assert read_secret("MANAGEARR_TEST_SECRET", "MANAGEARR_TEST_SECRET_FILE") is None
    assert read_secret("MANAGEARR_TEST_SECRET", "MANAGEARR_TEST_SECRET_FILE", default="fallback") == "fallback"


def test_missing_file_path_raises_instead_of_silently_falling_back(monkeypatch):
    monkeypatch.setenv("MANAGEARR_TEST_SECRET_FILE", "/nonexistent/path/to/secret")

    with pytest.raises(OSError):
        read_secret("MANAGEARR_TEST_SECRET", "MANAGEARR_TEST_SECRET_FILE")
