from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

from .models import BacktestBetRecord, GameRecord, PriceSnapshotRecord, ResultRecord
from .utils import utc_now


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS games (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,
    external_game_id TEXT NOT NULL,
    market_id TEXT,
    event_id TEXT,
    game_date_utc TEXT,
    tipoff_time_utc TEXT,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    home_token_id TEXT,
    away_token_id TEXT,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(platform, external_game_id)
);

CREATE TABLE IF NOT EXISTS price_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER NOT NULL,
    platform TEXT NOT NULL,
    snapshot_time_utc TEXT NOT NULL,
    minutes_to_tipoff INTEGER,
    snapshot_label TEXT NOT NULL,
    home_best_bid REAL,
    home_best_ask REAL,
    home_mid_price REAL,
    home_last_trade_price REAL,
    away_best_bid REAL,
    away_best_ask REAL,
    away_mid_price REAL,
    away_last_trade_price REAL,
    home_spread REAL,
    away_spread REAL,
    market_volume REAL,
    market_liquidity REAL,
    raw_payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(game_id) REFERENCES games(id),
    UNIQUE(game_id, snapshot_label)
);

CREATE TABLE IF NOT EXISTS results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER NOT NULL UNIQUE,
    winner_team TEXT NOT NULL,
    loser_team TEXT NOT NULL,
    home_win INTEGER NOT NULL,
    away_win INTEGER NOT NULL,
    final_score_json TEXT NOT NULL,
    source TEXT NOT NULL,
    resolved_at_utc TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(game_id) REFERENCES games(id)
);

CREATE TABLE IF NOT EXISTS backtest_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_name TEXT NOT NULL,
    start_bankroll REAL NOT NULL,
    end_bankroll REAL NOT NULL,
    total_bets INTEGER NOT NULL,
    wins INTEGER NOT NULL,
    losses INTEGER NOT NULL,
    pushes INTEGER NOT NULL,
    win_rate REAL NOT NULL,
    roi REAL NOT NULL,
    max_drawdown REAL NOT NULL,
    config_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS backtest_bets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    strategy_name TEXT NOT NULL,
    bet_time_basis TEXT NOT NULL,
    side_team TEXT NOT NULL,
    side_type TEXT NOT NULL,
    implied_prob_q REAL NOT NULL,
    estimated_prob_p_hat REAL NOT NULL,
    edge REAL NOT NULL,
    stake REAL NOT NULL,
    price REAL NOT NULL,
    odds_style TEXT NOT NULL,
    result TEXT NOT NULL,
    pnl REAL NOT NULL,
    bankroll_before REAL NOT NULL,
    bankroll_after REAL NOT NULL,
    reason_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(run_id) REFERENCES backtest_runs(id),
    FOREIGN KEY(game_id) REFERENCES games(id)
);

CREATE INDEX IF NOT EXISTS idx_games_tipoff ON games(tipoff_time_utc);
CREATE INDEX IF NOT EXISTS idx_games_platform_status ON games(platform, status);
CREATE INDEX IF NOT EXISTS idx_snapshots_game_label ON price_snapshots(game_id, snapshot_label);
CREATE INDEX IF NOT EXISTS idx_snapshots_time ON price_snapshots(snapshot_time_utc);
CREATE INDEX IF NOT EXISTS idx_results_game_id ON results(game_id);
CREATE INDEX IF NOT EXISTS idx_backtest_bets_run_id ON backtest_bets(run_id);
CREATE INDEX IF NOT EXISTS idx_backtest_bets_game_id ON backtest_bets(game_id);
"""


class Database:
    def __init__(self, db_path: Path):
        self.db_path = db_path

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def initialize(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA_SQL)

    def upsert_game(self, record: GameRecord) -> int:
        now = utc_now().isoformat()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO games (
                    platform, external_game_id, market_id, event_id, game_date_utc,
                    tipoff_time_utc, home_team, away_team, home_token_id, away_token_id,
                    status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(platform, external_game_id) DO UPDATE SET
                    market_id=excluded.market_id,
                    event_id=excluded.event_id,
                    game_date_utc=excluded.game_date_utc,
                    tipoff_time_utc=excluded.tipoff_time_utc,
                    home_team=excluded.home_team,
                    away_team=excluded.away_team,
                    home_token_id=excluded.home_token_id,
                    away_token_id=excluded.away_token_id,
                    status=excluded.status,
                    updated_at=excluded.updated_at
                """,
                (
                    record.platform,
                    record.external_game_id,
                    record.market_id,
                    record.event_id,
                    record.game_date_utc,
                    record.tipoff_time_utc,
                    record.home_team,
                    record.away_team,
                    record.home_token_id,
                    record.away_token_id,
                    record.status,
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT id FROM games WHERE platform = ? AND external_game_id = ?",
                (record.platform, record.external_game_id),
            ).fetchone()
            return int(row["id"])

    def list_games(self, where_sql: str = "", params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        query = "SELECT * FROM games"
        if where_sql:
            query += f" WHERE {where_sql}"
        query += " ORDER BY tipoff_time_utc"
        with self.connect() as conn:
            return list(conn.execute(query, params).fetchall())

    def insert_snapshot(self, record: PriceSnapshotRecord) -> bool:
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO price_snapshots (
                    game_id, platform, snapshot_time_utc, minutes_to_tipoff, snapshot_label,
                    home_best_bid, home_best_ask, home_mid_price, home_last_trade_price,
                    away_best_bid, away_best_ask, away_mid_price, away_last_trade_price,
                    home_spread, away_spread, market_volume, market_liquidity,
                    raw_payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.game_id,
                    record.platform,
                    record.snapshot_time_utc,
                    record.minutes_to_tipoff,
                    record.snapshot_label,
                    record.home_best_bid,
                    record.home_best_ask,
                    record.home_mid_price,
                    record.home_last_trade_price,
                    record.away_best_bid,
                    record.away_best_ask,
                    record.away_mid_price,
                    record.away_last_trade_price,
                    record.home_spread,
                    record.away_spread,
                    record.market_volume,
                    record.market_liquidity,
                    record.raw_payload_json,
                    utc_now().isoformat(),
                ),
            )
            return cursor.rowcount > 0

    def upsert_result(self, record: ResultRecord) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO results (
                    game_id, winner_team, loser_team, home_win, away_win, final_score_json,
                    source, resolved_at_utc, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(game_id) DO UPDATE SET
                    winner_team=excluded.winner_team,
                    loser_team=excluded.loser_team,
                    home_win=excluded.home_win,
                    away_win=excluded.away_win,
                    final_score_json=excluded.final_score_json,
                    source=excluded.source,
                    resolved_at_utc=excluded.resolved_at_utc
                """,
                (
                    record.game_id,
                    record.winner_team,
                    record.loser_team,
                    record.home_win,
                    record.away_win,
                    record.final_score_json,
                    record.source,
                    record.resolved_at_utc,
                    utc_now().isoformat(),
                ),
            )

    def insert_backtest_run(self, payload: dict[str, Any]) -> int:
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO backtest_runs (
                    strategy_name, start_bankroll, end_bankroll, total_bets, wins, losses,
                    pushes, win_rate, roi, max_drawdown, config_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["strategy_name"],
                    payload["start_bankroll"],
                    payload["end_bankroll"],
                    payload["total_bets"],
                    payload["wins"],
                    payload["losses"],
                    payload["pushes"],
                    payload["win_rate"],
                    payload["roi"],
                    payload["max_drawdown"],
                    payload["config_json"],
                    utc_now().isoformat(),
                ),
            )
            lastrowid = cursor.lastrowid
            if lastrowid is None:
                raise RuntimeError("Failed to insert backtest run row.")
            return int(lastrowid)

    def insert_backtest_bets(self, bets: list[BacktestBetRecord]) -> None:
        if not bets:
            return
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO backtest_bets (
                    run_id, game_id, strategy_name, bet_time_basis, side_team, side_type,
                    implied_prob_q, estimated_prob_p_hat, edge, stake, price, odds_style,
                    result, pnl, bankroll_before, bankroll_after, reason_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        bet.run_id,
                        bet.game_id,
                        bet.strategy_name,
                        bet.bet_time_basis,
                        bet.side_team,
                        bet.side_type,
                        bet.implied_prob_q,
                        bet.estimated_prob_p_hat,
                        bet.edge,
                        bet.stake,
                        bet.price,
                        bet.odds_style,
                        bet.result,
                        bet.pnl,
                        bet.bankroll_before,
                        bet.bankroll_after,
                        bet.reason_json,
                        utc_now().isoformat(),
                    )
                    for bet in bets
                ],
            )

    def load_analysis_frame(self, platform: str, snapshot_label: str) -> pd.DataFrame:
        query = """
        SELECT
            g.id AS game_id,
            g.platform,
            g.tipoff_time_utc,
            g.game_date_utc,
            g.home_team,
            g.away_team,
            ps.snapshot_time_utc,
            ps.minutes_to_tipoff,
            ps.snapshot_label,
            ps.home_best_bid,
            ps.home_best_ask,
            ps.home_mid_price,
            ps.home_last_trade_price,
            ps.away_best_bid,
            ps.away_best_ask,
            ps.away_mid_price,
            ps.away_last_trade_price,
            ps.home_spread,
            ps.away_spread,
            ps.market_volume,
            ps.market_liquidity,
            r.winner_team,
            r.loser_team,
            r.home_win,
            r.away_win
        FROM games g
        JOIN price_snapshots ps ON g.id = ps.game_id
        JOIN results r ON g.id = r.game_id
        WHERE g.platform = ? AND ps.snapshot_label = ?
        ORDER BY g.tipoff_time_utc
        """
        with self.connect() as conn:
            return pd.read_sql_query(query, conn, params=[platform, snapshot_label])

    def list_snapshots_for_game(self, game_id: int) -> list[sqlite3.Row]:
        """Get all price snapshots for a specific game."""
        query = """
        SELECT * FROM price_snapshots 
        WHERE game_id = ? 
        ORDER BY snapshot_time_utc
        """
        with self.connect() as conn:
            return list(conn.execute(query, (game_id,)).fetchall())
