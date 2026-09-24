from datetime import date

from app.domain.scan_candidate import MISSING_MONITORED_AIRED_REASON, derive_missing_candidate

TODAY = date(2026, 9, 24)


def make_series(**overrides):
    defaults = {"id": 1, "title": "Example Show", "monitored": True}
    defaults.update(overrides)
    return defaults


def make_episode(**overrides):
    defaults = {
        "id": 10,
        "seasonNumber": 1,
        "episodeNumber": 2,
        "monitored": True,
        "hasFile": False,
        "airDate": "2026-01-01",
    }
    defaults.update(overrides)
    return defaults


def test_missing_monitored_aired_episode_is_a_candidate():
    candidate = derive_missing_candidate(make_series(), make_episode(), today=TODAY)
    assert candidate == {
        "series_id": 1,
        "series_title": "Example Show",
        "episode_id": 10,
        "season_number": 1,
        "episode_number": 2,
        "air_date": "2026-01-01",
        "reason": MISSING_MONITORED_AIRED_REASON,
    }


def test_unmonitored_series_excluded_even_if_episode_monitored():
    candidate = derive_missing_candidate(make_series(monitored=False), make_episode(), today=TODAY)
    assert candidate is None


def test_unmonitored_episode_excluded():
    candidate = derive_missing_candidate(make_series(), make_episode(monitored=False), today=TODAY)
    assert candidate is None


def test_episode_with_file_excluded():
    candidate = derive_missing_candidate(make_series(), make_episode(hasFile=True), today=TODAY)
    assert candidate is None


def test_future_episode_excluded():
    candidate = derive_missing_candidate(make_series(), make_episode(airDate="2099-01-01"), today=TODAY)
    assert candidate is None


def test_episode_airing_today_is_included():
    candidate = derive_missing_candidate(make_series(), make_episode(airDate=str(TODAY)), today=TODAY)
    assert candidate is not None


def test_missing_air_date_excluded():
    candidate = derive_missing_candidate(make_series(), make_episode(airDate=None), today=TODAY)
    assert candidate is None


def test_malformed_air_date_excluded():
    candidate = derive_missing_candidate(make_series(), make_episode(airDate="not-a-date"), today=TODAY)
    assert candidate is None


def test_missing_required_ids_excluded():
    candidate = derive_missing_candidate(make_series(id=None), make_episode(), today=TODAY)
    assert candidate is None
    candidate = derive_missing_candidate(make_series(), make_episode(id=None), today=TODAY)
    assert candidate is None
    candidate = derive_missing_candidate(make_series(), make_episode(seasonNumber=None), today=TODAY)
    assert candidate is None
    candidate = derive_missing_candidate(make_series(), make_episode(episodeNumber=None), today=TODAY)
    assert candidate is None


def test_missing_series_title_falls_back_to_unknown():
    candidate = derive_missing_candidate(make_series(title=None), make_episode(), today=TODAY)
    assert candidate["series_title"] == "Unknown"
