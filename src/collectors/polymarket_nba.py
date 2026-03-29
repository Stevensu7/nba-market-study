from __future__ import annotations

import logging
from typing import Any

from ..clients.polymarket import PolymarketClient
from ..collectors.results import ESPNResultsCollector
from ..config import AppSettings
from ..db import Database
from ..models import GameRecord, PriceSnapshotRecord, ResultRecord
from ..utils import (
    dump_json,
    extract_matchup_teams,
    midpoint,
    normalize_team_name,
    parse_datetime,
    safe_float,
    spread,
    to_utc_iso,
    utc_now,
)


logger = logging.getLogger(__name__)


class PolymarketNBACollector:
    def __init__(self, settings: AppSettings, db: Database):
        self.settings = settings
        self.db = db
        self.client = PolymarketClient(settings)
        self.results_collector = ESPNResultsCollector(settings)

    def discover_games(self) -> list[int]:
        discovered_ids: list[int] = []
        candidates = self.client.list_candidate_nba_markets()
        for candidate in candidates:
            event = candidate["event"]
            market = candidate["market"]
            matchup = self._extract_market_matchup(event, market)
            if matchup is None:
                continue
            away_team, home_team = matchup
            matched_event = self.results_collector.match_game(home_team, away_team, candidate["tipoff"])
            if matched_event:
                competition = (matched_event.get("competitions") or [{}])[0]
                competitors = competition.get("competitors") or []
                home = next((c for c in competitors if c.get("homeAway") == "home"), None)
                away = next((c for c in competitors if c.get("homeAway") == "away"), None)
                external_game_id = matched_event.get("id") or f"{home_team}-{away_team}-{candidate['tipoff']}"
                home_team_name = normalize_team_name(home.get("team", {}).get("displayName", home_team)) if home else home_team
                away_team_name = normalize_team_name(away.get("team", {}).get("displayName", away_team)) if away else away_team
                tipoff_iso = matched_event.get("date") or candidate["tipoff"]
            else:
                external_game_id = market.get("id") or event.get("id") or f"{home_team}-{away_team}-{candidate['tipoff']}"
                home_team_name = home_team
                away_team_name = away_team
                tipoff_iso = candidate["tipoff"]

            token_ids = self._extract_token_ids(market)
            outcomes = self._extract_outcomes(market)
            home_token_id = self._pick_token_for_team(home_team_name, token_ids, outcomes)
            away_token_id = self._pick_token_for_team(away_team_name, token_ids, outcomes)
            if not home_token_id or not away_token_id:
                logger.info("Skipping market without clear home/away token mapping: %s", market.get("question"))
                continue

            game_id = self.db.upsert_game(
                GameRecord(
                    platform="polymarket",
                    external_game_id=str(external_game_id),
                    market_id=str(market.get("id")) if market.get("id") is not None else None,
                    event_id=str(event.get("id")) if event.get("id") is not None else None,
                    game_date_utc=to_utc_iso(parse_datetime(tipoff_iso).date().isoformat() if parse_datetime(tipoff_iso) else None),
                    tipoff_time_utc=to_utc_iso(tipoff_iso),
                    home_team=home_team_name,
                    away_team=away_team_name,
                    home_token_id=home_token_id,
                    away_token_id=away_token_id,
                    status="scheduled",
                )
            )
            discovered_ids.append(game_id)
        logger.info("Upserted %s games from Polymarket discovery", len(discovered_ids))
        return discovered_ids

    def collect_due_snapshots(self) -> int:
        inserted = 0
        now = utc_now()
        tolerance = self.settings.collection.snapshot_tolerance_minutes
        games = self.db.list_games(where_sql="platform = ? AND status IN ('scheduled', 'created')", params=("polymarket",))
        for game in games:
            tipoff = parse_datetime(game["tipoff_time_utc"])
            if tipoff is None:
                continue
            minutes_to_tipoff = int((tipoff - now).total_seconds() / 60)
            if minutes_to_tipoff < -240:
                continue
            for label, target_minutes in self.settings.collection.snapshot_labels.items():
                if abs(minutes_to_tipoff - target_minutes) <= tolerance or (minutes_to_tipoff < target_minutes and minutes_to_tipoff >= -5):
                    if self._snapshot_exists(game["id"], label):
                        continue
                    snapshot = self._build_snapshot(dict(game), label, minutes_to_tipoff)
                    if snapshot and self.db.insert_snapshot(snapshot):
                        inserted += 1
        logger.info("Inserted %s new price snapshots", inserted)
        return inserted

    def resolve_results(self) -> int:
        resolved = 0
        games = self.db.list_games(where_sql="platform = ?", params=("polymarket",))
        for game in games:
            if self._result_exists(game["id"]):
                continue
            event = self.results_collector.match_game(game["home_team"], game["away_team"], game["tipoff_time_utc"])
            if not event:
                continue
            competition = (event.get("competitions") or [{}])[0]
            if competition.get("status", {}).get("type", {}).get("completed") is not True:
                continue
            competitors = competition.get("competitors") or []
            home = next((c for c in competitors if c.get("homeAway") == "home"), None)
            away = next((c for c in competitors if c.get("homeAway") == "away"), None)
            if not home or not away:
                continue
            home_score = int(home.get("score", 0))
            away_score = int(away.get("score", 0))
            if home_score == away_score:
                continue
            winner_team = game["home_team"] if home_score > away_score else game["away_team"]
            loser_team = game["away_team"] if home_score > away_score else game["home_team"]
            self.db.upsert_result(
                ResultRecord(
                    game_id=game["id"],
                    winner_team=winner_team,
                    loser_team=loser_team,
                    home_win=1 if home_score > away_score else 0,
                    away_win=1 if away_score > home_score else 0,
                    final_score_json=dump_json(
                        {
                            "home_team": game["home_team"],
                            "away_team": game["away_team"],
                            "home_score": home_score,
                            "away_score": away_score,
                            "event_id": event.get("id"),
                        }
                    ),
                    source="espn_scoreboard",
                    resolved_at_utc=to_utc_iso(event.get("date")) or utc_now().isoformat(),
                )
            )
            resolved += 1
        logger.info("Resolved %s game results", resolved)
        return resolved

    def _build_snapshot(self, game: dict[str, Any], label: str, minutes_to_tipoff: int) -> PriceSnapshotRecord | None:
        home_book = self._safe_order_book(game.get("home_token_id"))
        away_book = self._safe_order_book(game.get("away_token_id"))
        if not home_book or not away_book:
            return None

        home_best_bid = self._best_price(home_book.get("bids"), maximize=True)
        home_best_ask = self._best_price(home_book.get("asks"), maximize=False)
        away_best_bid = self._best_price(away_book.get("bids"), maximize=True)
        away_best_ask = self._best_price(away_book.get("asks"), maximize=False)
        home_mid = self.client.get_midpoint(game["home_token_id"]) or midpoint(home_best_bid, home_best_ask)
        away_mid = self.client.get_midpoint(game["away_token_id"]) or midpoint(away_best_bid, away_best_ask)
        home_last = safe_float(home_book.get("last_trade_price")) or self.client.get_last_trade_price(game["home_token_id"])
        away_last = safe_float(away_book.get("last_trade_price")) or self.client.get_last_trade_price(game["away_token_id"])

        payload = {"home_book": home_book, "away_book": away_book}
        return PriceSnapshotRecord(
            game_id=int(game["id"]),
            platform="polymarket",
            snapshot_time_utc=utc_now().isoformat(),
            minutes_to_tipoff=minutes_to_tipoff,
            snapshot_label=label,
            home_best_bid=home_best_bid,
            home_best_ask=home_best_ask,
            home_mid_price=home_mid,
            home_last_trade_price=home_last,
            away_best_bid=away_best_bid,
            away_best_ask=away_best_ask,
            away_mid_price=away_mid,
            away_last_trade_price=away_last,
            home_spread=spread(home_best_bid, home_best_ask),
            away_spread=spread(away_best_bid, away_best_ask),
            market_volume=safe_float(home_book.get("volume")) or safe_float(away_book.get("volume")),
            market_liquidity=safe_float(home_book.get("liquidity")) or safe_float(away_book.get("liquidity")),
            raw_payload_json=dump_json(payload),
        )

    def _safe_order_book(self, token_id: str | None) -> dict[str, Any] | None:
        if not token_id:
            return None
        try:
            payload = self.client.get_order_book(token_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to fetch order book for token %s: %s", token_id, exc)
            return None
        if isinstance(payload, dict) and payload.get("error"):
            logger.warning("Polymarket returned order book error for token %s: %s", token_id, payload["error"])
            return None
        return payload

    def _snapshot_exists(self, game_id: int, label: str) -> bool:
        rows = self.db.list_games(where_sql="id IN (SELECT game_id FROM price_snapshots WHERE game_id = ? AND snapshot_label = ?)", params=(game_id, label))
        return bool(rows)

    def _result_exists(self, game_id: int) -> bool:
        rows = self.db.list_games(where_sql="id IN (SELECT game_id FROM results WHERE game_id = ?)", params=(game_id,))
        return bool(rows)

    @staticmethod
    def _extract_market_matchup(event: dict[str, Any], market: dict[str, Any]) -> tuple[str, str] | None:
        for text in [market.get("question"), event.get("title"), market.get("description")]:
            if not text:
                continue
            matchup = extract_matchup_teams(text)
            if matchup:
                return matchup
        return None

    @staticmethod
    def _extract_token_ids(market: dict[str, Any]) -> list[str]:
        raw = market.get("clobTokenIds") or market.get("tokenIds") or []
        if isinstance(raw, str):
            try:
                import json

                parsed = json.loads(raw)
                return [str(token) for token in parsed]
            except Exception:  # noqa: BLE001
                return []
        return [str(token) for token in raw]

    @staticmethod
    def _extract_outcomes(market: dict[str, Any]) -> list[str]:
        raw = market.get("outcomes") or []
        if isinstance(raw, str):
            try:
                import json

                parsed = json.loads(raw)
                return [str(item) for item in parsed]
            except Exception:  # noqa: BLE001
                return []
        return [str(item) for item in raw]

    @staticmethod
    def _pick_token_for_team(team_name: str, token_ids: list[str], outcomes: list[str]) -> str | None:
        team = normalize_team_name(team_name).lower()
        for idx, outcome in enumerate(outcomes):
            if normalize_team_name(outcome).lower() == team and idx < len(token_ids):
                return token_ids[idx]
        return None

    @staticmethod
    def _best_price(levels: list[dict[str, Any]] | None, maximize: bool) -> float | None:
        prices = [safe_float(level.get("price")) for level in levels or []]
        prices = [price for price in prices if price is not None]
        if not prices:
            return None
        return max(prices) if maximize else min(prices)
