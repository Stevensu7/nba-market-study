from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class GameRecord:
    platform: str
    external_game_id: str
    market_id: str | None
    event_id: str | None
    game_date_utc: str | None
    tipoff_time_utc: str | None
    home_team: str
    away_team: str
    home_token_id: str | None
    away_token_id: str | None
    status: str = "scheduled"


@dataclass(slots=True)
class PriceSnapshotRecord:
    game_id: int
    platform: str
    snapshot_time_utc: str
    minutes_to_tipoff: int | None
    snapshot_label: str
    home_best_bid: float | None
    home_best_ask: float | None
    home_mid_price: float | None
    home_last_trade_price: float | None
    away_best_bid: float | None
    away_best_ask: float | None
    away_mid_price: float | None
    away_last_trade_price: float | None
    home_spread: float | None
    away_spread: float | None
    market_volume: float | None
    market_liquidity: float | None
    raw_payload_json: str


@dataclass(slots=True)
class ResultRecord:
    game_id: int
    winner_team: str
    loser_team: str
    home_win: int
    away_win: int
    final_score_json: str
    source: str
    resolved_at_utc: str


@dataclass(slots=True)
class BettingCandidate:
    game_id: int
    platform: str
    snapshot_label: str
    tipoff_time_utc: datetime
    side_team: str
    side_type: str
    implied_prob_q: float
    estimated_prob_p_hat: float
    edge: float
    outcome: int


@dataclass(slots=True)
class StrategyDecision:
    should_bet: bool
    stake: float = 0.0
    reason: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class BacktestBetRecord:
    run_id: int
    game_id: int
    strategy_name: str
    bet_time_basis: str
    side_team: str
    side_type: str
    implied_prob_q: float
    estimated_prob_p_hat: float
    edge: float
    stake: float
    price: float
    odds_style: str
    result: str
    pnl: float
    bankroll_before: float
    bankroll_after: float
    reason_json: str
