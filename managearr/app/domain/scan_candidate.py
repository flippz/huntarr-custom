"""Domain model for a single scan candidate row.

A candidate is one (series, episode) pair identified by a read-only Sonarr
scan as either monitored, aired, and missing a file, or explicitly returned
by Sonarr's cutoff-unmet endpoint. Candidates are a
durable snapshot tied to the ``ActivityJob`` that produced them - a
repeat scan creates new candidate rows under a new job rather than
overwriting the previous job's rows.
"""
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Optional

MISSING_MONITORED_AIRED_REASON = "monitored episode aired with no file on disk"
UPGRADE_CUTOFF_NOT_MET_REASON = "monitored aired episode has a file below Sonarr quality cutoff"
CANDIDATE_KINDS = ("missing", "upgrade")


@dataclass
class ScanCandidate:
    id: Optional[int]
    job_id: int
    library_id: Optional[int]
    series_id: int
    series_title: str
    episode_id: int
    season_number: int
    episode_number: int
    candidate_kind: str
    air_date: Optional[str]
    reason: str
    created_at: str

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "job_id": self.job_id,
            "library_id": self.library_id,
            "series_id": self.series_id,
            "series_title": self.series_title,
            "episode_id": self.episode_id,
            "season_number": self.season_number,
            "episode_number": self.episode_number,
            "candidate_kind": self.candidate_kind,
            "air_date": self.air_date,
            "reason": self.reason,
            "created_at": self.created_at,
        }


def derive_missing_candidate(series: dict, episode: dict, *, today) -> Optional[dict]:
    """Return a candidate dict for one episode, or ``None`` if it doesn't
    qualify.

    A candidate must belong to a monitored series, be itself monitored,
    have no file on disk, and have a valid air date that is today or in
    the past (future episodes are excluded even if monitored).
    """
    if not series.get("monitored"):
        return None
    if not episode.get("monitored"):
        return None
    if episode.get("hasFile"):
        return None

    air_date_str = episode.get("airDate")
    if not air_date_str:
        return None
    try:
        air_date = date.fromisoformat(air_date_str)
    except (TypeError, ValueError):
        return None
    if air_date > today:
        return None

    series_id = series.get("id")
    episode_id = episode.get("id")
    season_number = episode.get("seasonNumber")
    episode_number = episode.get("episodeNumber")
    if series_id is None or episode_id is None or season_number is None or episode_number is None:
        return None

    return {
        "series_id": series_id,
        "series_title": series.get("title") or "Unknown",
        "episode_id": episode_id,
        "season_number": season_number,
        "episode_number": episode_number,
        "candidate_kind": "missing",
        "air_date": air_date_str,
        "reason": MISSING_MONITORED_AIRED_REASON,
    }


def derive_upgrade_candidate(series: dict, episode: dict, *, today) -> Optional[dict]:
    """Build a fail-closed upgrade candidate from a wanted/cutoff record.

    Membership in Sonarr's read-only wanted/cutoff response is the explicit
    cutoff-not-met evidence. Exact monitored/file flags, a known local
    series, valid identities, and an aired timestamp are also required.
    """
    if series.get("monitored") is not True or episode.get("monitored") is not True:
        return None
    if episode.get("hasFile") is not True:
        return None
    series_id = series.get("id")
    episode_series_id = episode.get("seriesId")
    episode_id = episode.get("id")
    season_number = episode.get("seasonNumber")
    episode_number = episode.get("episodeNumber")
    if (
        not isinstance(series_id, int) or isinstance(series_id, bool) or series_id <= 0
        or episode_series_id != series_id
        or not isinstance(episode_id, int) or isinstance(episode_id, bool) or episode_id <= 0
        or not isinstance(season_number, int) or isinstance(season_number, bool) or season_number < 0
        or not isinstance(episode_number, int) or isinstance(episode_number, bool) or episode_number < 0
    ):
        return None

    air_date_text = episode.get("airDate")
    if isinstance(air_date_text, str):
        try:
            aired_date = date.fromisoformat(air_date_text)
        except ValueError:
            return None
    else:
        utc_text = episode.get("airDateUtc")
        if not isinstance(utc_text, str):
            return None
        try:
            aired_at = datetime.fromisoformat(utc_text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if aired_at.tzinfo is None:
            return None
        aired_date = aired_at.astimezone(timezone.utc).date()
        air_date_text = aired_date.isoformat()
    if aired_date > today:
        return None

    return {
        "series_id": series_id,
        "series_title": series.get("title") or "Unknown",
        "episode_id": episode_id,
        "season_number": season_number,
        "episode_number": episode_number,
        "candidate_kind": "upgrade",
        "air_date": air_date_text,
        "reason": UPGRADE_CUTOFF_NOT_MET_REASON,
    }
