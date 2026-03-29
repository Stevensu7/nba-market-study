from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ..config import AppSettings
from ..utils import normalize_team_name, parse_datetime, team_key


logger = logging.getLogger(__name__)


class ESPNResultsCollector:
    """Resolve NBA schedule and final scores through ESPN's public scoreboard endpoint."""

    def __init__(self, settings: AppSettings):
        self.settings = settings
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "nba-market-study/0.1"})

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def fetch_scoreboard(self, yyyymmdd: str) -> list[dict[str, Any]]:
        response = self.session.get(
            self.settings.apis.espn_scoreboard_base_url,
            params={"dates": yyyymmdd},
            timeout=self.settings.apis.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        return payload.get("events", [])

    def fetch_window(self, center_time: str | None) -> list[dict[str, Any]]:
        base = parse_datetime(center_time)
        if base is None:
            return []
        events: list[dict[str, Any]] = []
        for delta in (-1, 0, 1):
            day = (base + timedelta(days=delta)).strftime("%Y%m%d")
            try:
                events.extend(self.fetch_scoreboard(day))
            except requests.RequestException as exc:
                logger.warning("Failed to fetch ESPN scoreboard for %s: %s", day, exc)
        return events

    def match_game(self, home_team: str, away_team: str, tipoff_time_utc: str | None) -> dict[str, Any] | None:
        target_home = team_key(home_team)
        target_away = team_key(away_team)
        for event in self.fetch_window(tipoff_time_utc):
            competition = (event.get("competitions") or [{}])[0]
            competitors = competition.get("competitors") or []
            home = next((c for c in competitors if c.get("homeAway") == "home"), None)
            away = next((c for c in competitors if c.get("homeAway") == "away"), None)
            if not home or not away:
                continue
            home_name = normalize_team_name(home.get("team", {}).get("displayName", ""))
            away_name = normalize_team_name(away.get("team", {}).get("displayName", ""))
            if team_key(home_name) == target_home and team_key(away_name) == target_away:
                return event
        return None
