"""
Magnetarr app module for Huntarr
Scrapes a configured URL (e.g. a subreddit) for magnet links, stores them
deduplicated by info-hash, and exposes them as a Torznab-compatible indexer.
"""

from src.primary.utils.logger import get_logger

magnetarr_logger = get_logger("magnetarr")

__all__ = ["magnetarr_logger"]
