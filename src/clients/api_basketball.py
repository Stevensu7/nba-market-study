from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ..config import AppSettings
from ..utils import safe_float, utc_now


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
    "DraftKings",
    "FanDuel",
]

PREFERRED_BOOKMAKERS = ["Bet365", "Pinnacle", "DraftKings", "FanDuel"]

NBA_LEAGUE_ID = 12  # API-Basketball NBA league ID
NBA_SEASON = "2024-2025"  # Current NBA season


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
    odds_fetched_at_utc: str | None = None  # 赔率抓取时间


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

    def get_games_by_date_range(self, from_date: str, to_date: str) -> list[dict[str, Any]]:
        """Get games for a date range - supports future dates."""
        if not self.enabled:
            return []
        payload = self._get("games", params={
            "league": NBA_LEAGUE_ID,
            "season": NBA_SEASON,
            "date": f"{from_date}:{to_date}",
        })
        return payload.get("response", [])

    def get_upcoming_games(self, days_ahead: int = 7) -> list[dict[str, Any]]:
        """Get upcoming games for the next N days."""
        if not self.enabled:
            return []
        now = utc_now()
        games = []
        for day_offset in range(days_ahead + 1):
            date = (now + timedelta(days=day_offset)).strftime("%Y-%m-%d")
            day_games = self.get_games_by_date(date)
            games.extend(day_games)
            logger.info("Fetched %s games from API-Basketball for date %s", len(day_games), date)
        return games

    def get_odds_by_game(self, game_id: int) -> dict[str, Any]:
        if not self.enabled:
            return {"response": [], "errors": {"auth": "missing API_BASKETBALL_KEY"}}
        return self._get("odds", params={"game": game_id})

    def get_odds_by_league_and_date(self, date_str: str) -> list[dict[str, Any]]:
        """
        Get all odds for NBA games on a specific date.
        This can work for both past and future dates.
        """
        if not self.enabled:
            return []
        payload = self._get("odds", params={
            "league": NBA_LEAGUE_ID,
            "season": NBA_SEASON,
            "date": date_str,
            "bookmaker": "4,5,15",  # Bet365, Pinnacle, Betfair
        })
        return payload.get("response", [])

    def build_consensus_for_game(self, game_payload: dict[str, Any]) -> SportsbookConsensus:
        """Build consensus from bookmaker odds for a specific game."""
        home_team = str(game_payload.get("teams", {}).get("home", {}).get("name", ""))
        away_team = str(game_payload.get("teams", {}).get("away", {}).get("name", ""))
        game_time_utc = str(game_payload.get("date", ""))
        game_id = game_payload.get("id")
        
        # Record when we fetched the odds
        odds_fetched_at = utc_now().isoformat()
        
        if game_id is None:
            return SportsbookConsensus(
                home_team, away_team, game_time_utc, None, None, 0, [], 
                "unavailable", "Missing game id", odds_fetched_at
            )
            
        odds_payload = self.get_odds_by_game(int(game_id))
        errors = odds_payload.get("errors", {}) or {}
        if errors:
            message = "; ".join(f"{key}: {value}" for key, value in errors.items())
            return SportsbookConsensus(
                home_team, away_team, game_time_utc, None, None, 0, [], 
                "unavailable", message, odds_fetched_at
            )
            
        responses = odds_payload.get("response", [])
        if not responses:
            return SportsbookConsensus(
                home_team, away_team, game_time_utc, None, None, 0, [], 
                "unavailable", "No odds returned", odds_fetched_at
            )
            
        bookmakers = responses[0].get("bookmakers", [])
        captured: list[tuple[str, float, float]] = []
        
        for bookmaker in bookmakers:
            name = str(bookmaker.get("name") or "")
            if name not in MAINSTREAM_BOOKMAKERS:
                continue
            
            # Look for Moneyline / Winner bets (id=1 is typically Winner/Moneyline)
            # id=2 is Home/Away which also works
            bet = next((
                item for item in bookmaker.get("bets", []) 
                if item.get("id") in [1, 2] or item.get("name") in ["Winner", "Home/Away", "1X2", "Moneyline"]
            ), None)
            
            if not bet:
                continue
                
            values = bet.get("values", [])
            
            # Try to find Home and Away odds
            home_odd = None
            away_odd = None
            
            for item in values:
                value_str = str(item.get("value", ""))
                odd_val = safe_float(item.get("odd"))
                
                # Match by "Home" or team name matching
                if value_str in ["Home", "1"] or value_str.lower() == home_team.lower():
                    home_odd = odd_val
                elif value_str in ["Away", "2"] or value_str.lower() == away_team.lower():
                    away_odd = odd_val
                    
            if not home_odd or not away_odd or home_odd <= 0 or away_odd <= 0:
                continue
                
            raw_home = 1.0 / home_odd
            raw_away = 1.0 / away_odd
            total = raw_home + raw_away
            if total <= 0:
                continue
            captured.append((name, raw_home / total, raw_away / total))
            
        selected = [item for item in captured if item[0] in PREFERRED_BOOKMAKERS]
        if len(selected) < 1:
            selected = captured[:2]
        elif len(selected) > 2:
            selected = [item for item in selected if item[0] in PREFERRED_BOOKMAKERS][:2]
        if len(selected) == 1 and len(captured) > 1:
            for item in captured:
                if item[0] != selected[0][0]:
                    selected.append(item)
                    break
                    
        if not selected:
            return SportsbookConsensus(
                home_team, away_team, game_time_utc, None, None, 0, [], 
                "unavailable", "No mainstream Home/Away prices", odds_fetched_at
            )
            
        home_probs = [item[1] for item in selected]
        away_probs = [item[2] for item in selected]
        bookmaker_names = [item[0] for item in selected]
        
        return SportsbookConsensus(
            home_team=home_team,
            away_team=away_team,
            game_time_utc=game_time_utc,
            home_probability=sum(home_probs) / len(home_probs),
            away_probability=sum(away_probs) / len(away_probs),
            bookmaker_count=len(bookmaker_names),
            bookmaker_names=bookmaker_names,
            status="ok",
            odds_fetched_at_utc=odds_fetched_at,
        )
