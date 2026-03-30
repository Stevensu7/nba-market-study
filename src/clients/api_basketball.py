from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ..config import AppSettings
from ..utils import safe_float


logger = logging.getLogger(__name__)

MAINSTREAM_BOOKMAKERS = [
    "Bet365",
    "Pinnacle",
    "Betfair",
    "Bwin",
    "WilliamHill",
    "Betano",
    "Unibet",
    "Betway",
]


@dataclass(slots=True)
class SportsbookConsensus:
    home_team: str
    away_team: str
    game_time_utc: str
    home_probability: float | None
    away_probability: float | None
    bookmaker_count: int
    bookmaker_names: list[str]
    status: str
    message: str = ""


class APIBasketballClient:
    def __init__(self, settings: AppSettings):
        self.settings = settings
        self.base_url = settings.apis.api_basketball_base_url.rstrip("/")
        self.api_key = settings.apis.api_basketball_key
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "nba-market-study/0.1"})
        if self.api_key:
            self.session.headers.update({"x-apisports-key": self.api_key})

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        response = self.session.get(
            f"{self.base_url}/{path.lstrip('/')}",
            params=params or {},
            timeout=self.settings.apis.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Unexpected API-Basketball payload.")
        return payload

    def get_games_by_date(self, date_str: str) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        payload = self._get("games", params={"date": date_str})
        return payload.get("response", [])

    def get_odds_by_game(self, game_id: int) -> dict[str, Any]:
        if not self.enabled:
            return {"response": [], "errors": {"auth": "missing API_BASKETBALL_KEY"}}
        return self._get("odds", params={"game": game_id})

    def build_consensus_for_game(self, game_payload: dict[str, Any]) -> SportsbookConsensus:
        home_team = str(game_payload.get("teams", {}).get("home", {}).get("name", ""))
        away_team = str(game_payload.get("teams", {}).get("away", {}).get("name", ""))
        game_time_utc = str(game_payload.get("date", ""))
        odds_payload = self.get_odds_by_game(int(game_payload.get("id")))
        errors = odds_payload.get("errors", {}) or {}
        if errors:
            message = "; ".join(f"{key}: {value}" for key, value in errors.items())
            return SportsbookConsensus(home_team, away_team, game_time_utc, None, None, 0, [], "unavailable", message)
        responses = odds_payload.get("response", [])
        if not responses:
            return SportsbookConsensus(home_team, away_team, game_time_utc, None, None, 0, [], "unavailable", "No odds returned")
        bookmakers = responses[0].get("bookmakers", [])
        home_probs: list[float] = []
        away_probs: list[float] = []
        bookmaker_names: list[str] = []
        for bookmaker in bookmakers:
            name = str(bookmaker.get("name") or "")
            if name not in MAINSTREAM_BOOKMAKERS:
                continue
            bet = next((item for item in bookmaker.get("bets", []) if item.get("id") == 2 or item.get("name") == "Home/Away"), None)
            if not bet:
                continue
            values = bet.get("values", [])
            home_odd = next((safe_float(item.get("odd")) for item in values if str(item.get("value")) == "Home"), None)
            away_odd = next((safe_float(item.get("odd")) for item in values if str(item.get("value")) == "Away"), None)
            if not home_odd or not away_odd or home_odd <= 0 or away_odd <= 0:
                continue
            raw_home = 1.0 / home_odd
            raw_away = 1.0 / away_odd
            total = raw_home + raw_away
            if total <= 0:
                continue
            home_probs.append(raw_home / total)
            away_probs.append(raw_away / total)
            bookmaker_names.append(name)
        if not home_probs or not away_probs:
            return SportsbookConsensus(home_team, away_team, game_time_utc, None, None, 0, [], "unavailable", "No mainstream Home/Away prices")
        return SportsbookConsensus(
            home_team=home_team,
            away_team=away_team,
            game_time_utc=game_time_utc,
            home_probability=sum(home_probs) / len(home_probs),
            away_probability=sum(away_probs) / len(away_probs),
            bookmaker_count=len(bookmaker_names),
            bookmaker_names=bookmaker_names,
            status="ok",
        )
