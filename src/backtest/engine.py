from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

from ..analytics.calibration import CalibrationModel, build_side_frame
from ..analytics.metrics import max_drawdown
from ..analytics.plots import plot_equity_curve
from ..db import Database
from ..models import BacktestBetRecord, BettingCandidate
from ..utils import clamp_probability, dump_json, parse_datetime, rolling_max_drawdown


class BacktestEngine:
    def __init__(self, db: Database, reports_dir: Path, processed_dir: Path):
        self.db = db
        self.reports_dir = reports_dir
        self.processed_dir = processed_dir

    def run(
        self,
        platform: str,
        snapshot_label: str,
        price_field: str,
        bins: list[float],
        min_bin_samples: int,
        smoothing_prior_strength: int,
        strategy: Any,
        start_bankroll: float,
    ) -> dict[str, Any]:
        frame = self.db.load_analysis_frame(platform=platform, snapshot_label=snapshot_label)
        if frame.empty:
            return {
                "strategy_name": strategy.name,
                "start_bankroll": start_bankroll,
                "end_bankroll": start_bankroll,
                "total_bets": 0,
                "wins": 0,
                "losses": 0,
                "pushes": 0,
                "win_rate": 0.0,
                "roi": 0.0,
                "max_drawdown": 0.0,
                "bets": [],
            }

        frame["tipoff_dt"] = pd.to_datetime(frame["tipoff_time_utc"], utc=True)
        frame = frame.sort_values("tipoff_dt").reset_index(drop=True)
        bankroll = start_bankroll
        equity_points = [{"date": frame.iloc[0]["tipoff_dt"].date().isoformat(), "bankroll": bankroll}]
        bet_details: list[dict[str, Any]] = []
        db_bets: list[BacktestBetRecord] = []

        for idx, current_game in frame.iterrows():
            history_frame = frame.iloc[:idx]
            history_side = build_side_frame(history_frame, price_field=price_field)
            calibration_model = CalibrationModel(
                bins=bins,
                min_samples=min_bin_samples,
                smoothing_prior_strength=smoothing_prior_strength,
            ).fit(history_side)

            candidates = self._build_candidates(current_game.to_dict(), calibration_model, price_field, snapshot_label)
            selected, decision = strategy.select_bet(candidates, bankroll)
            if not selected or not decision.should_bet:
                equity_points.append({"date": current_game["tipoff_dt"].date().isoformat(), "bankroll": bankroll})
                continue

            bankroll_before = bankroll
            pnl = self._settle_pnl(selected.implied_prob_q, decision.stake, selected.outcome)
            bankroll = bankroll + pnl
            result = "win" if selected.outcome == 1 else "loss"
            detail = {
                "game_id": selected.game_id,
                "date": current_game["tipoff_dt"].date().isoformat(),
                "strategy_name": strategy.name,
                "side_team": selected.side_team,
                "side_type": selected.side_type,
                "implied_prob_q": selected.implied_prob_q,
                "estimated_prob_p_hat": selected.estimated_prob_p_hat,
                "edge": selected.edge,
                "stake": decision.stake,
                "result": result,
                "pnl": pnl,
                "bankroll_before": bankroll_before,
                "bankroll_after": bankroll,
                "reason": decision.reason,
            }
            bet_details.append(detail)
            equity_points.append({"date": current_game["tipoff_dt"].date().isoformat(), "bankroll": bankroll})
            db_bets.append(
                BacktestBetRecord(
                    run_id=0,
                    game_id=selected.game_id,
                    strategy_name=strategy.name,
                    bet_time_basis=snapshot_label,
                    side_team=selected.side_team,
                    side_type=selected.side_type,
                    implied_prob_q=selected.implied_prob_q,
                    estimated_prob_p_hat=selected.estimated_prob_p_hat,
                    edge=selected.edge,
                    stake=decision.stake,
                    price=selected.implied_prob_q,
                    odds_style="binary_contract",
                    result=result,
                    pnl=pnl,
                    bankroll_before=bankroll_before,
                    bankroll_after=bankroll,
                    reason_json=dump_json(decision.reason),
                )
            )

        wins = sum(1 for bet in bet_details if bet["result"] == "win")
        losses = sum(1 for bet in bet_details if bet["result"] == "loss")
        total_bets = len(bet_details)
        win_rate = wins / total_bets if total_bets else 0.0
        total_pnl = bankroll - start_bankroll
        roi = total_pnl / start_bankroll if start_bankroll else 0.0
        equity_df = pd.DataFrame(equity_points).drop_duplicates(subset=["date"], keep="last")
        weekly_pnl = self._aggregate_period_pnl(pd.DataFrame(bet_details), freq="W")
        daily_pnl = self._aggregate_period_pnl(pd.DataFrame(bet_details), freq="D")
        max_dd = rolling_max_drawdown(equity_df["bankroll"].tolist())

        summary = {
            "strategy_name": strategy.name,
            "start_bankroll": start_bankroll,
            "end_bankroll": bankroll,
            "total_bets": total_bets,
            "wins": wins,
            "losses": losses,
            "pushes": 0,
            "win_rate": win_rate,
            "roi": roi,
            "max_drawdown": max_dd,
            "config_json": dump_json({"strategy": asdict(strategy) if hasattr(strategy, "__dataclass_fields__") else strategy.__dict__}),
            "bets": bet_details,
            "db_bets": db_bets,
            "equity_curve": equity_df,
            "weekly_pnl": weekly_pnl,
            "daily_pnl": daily_pnl,
        }
        return summary

    @staticmethod
    def _build_candidates(
        current_game: dict[str, Any], calibration_model: CalibrationModel, price_field: str, snapshot_label: str
    ) -> list[BettingCandidate]:
        side_frame = build_side_frame(pd.DataFrame([current_game]), price_field=price_field)
        if side_frame.empty:
            return []
        annotated = calibration_model.annotate(side_frame)
        candidates: list[BettingCandidate] = []
        for row in annotated.to_dict(orient="records"):
            candidates.append(
                BettingCandidate(
                    game_id=int(row["game_id"]),
                    platform=row["platform"],
                    snapshot_label=snapshot_label,
                    tipoff_time_utc=parse_datetime(str(row["tipoff_time_utc"])),
                    side_team=row["team"],
                    side_type=row["side_type"],
                    implied_prob_q=clamp_probability(float(row["implied_prob"])),
                    estimated_prob_p_hat=clamp_probability(float(row["p_hat"])),
                    edge=float(row["edge"]),
                    outcome=int(row["outcome"]),
                )
            )
        candidates.sort(key=lambda item: item.edge, reverse=True)
        return candidates

    @staticmethod
    def _settle_pnl(q: float, stake: float, outcome: int) -> float:
        if outcome == 1:
            return stake * ((1 - q) / q)
        return -stake

    def _aggregate_period_pnl(self, bets_df: pd.DataFrame, freq: str) -> pd.DataFrame:
        if bets_df.empty:
            return pd.DataFrame(columns=["period", "pnl"])
        frame = bets_df.copy()
        frame["date"] = pd.to_datetime(frame["date"])
        grouped = frame.set_index("date").resample(freq)["pnl"].sum().reset_index()
        grouped["period"] = grouped["date"].dt.strftime("%Y-%m-%d")
        return grouped[["period", "pnl"]]

    def persist_results(self, result: dict[str, Any]) -> int:
        run_id = self.db.insert_backtest_run(
            {
                key: result[key]
                for key in [
                    "strategy_name",
                    "start_bankroll",
                    "end_bankroll",
                    "total_bets",
                    "wins",
                    "losses",
                    "pushes",
                    "win_rate",
                    "roi",
                    "max_drawdown",
                    "config_json",
                ]
            }
        )
        db_bets: list[BacktestBetRecord] = result.get("db_bets", [])
        for bet in db_bets:
            bet.run_id = run_id
        self.db.insert_backtest_bets(db_bets)

        result["equity_curve"].to_csv(self.processed_dir / f"equity_curve_{result['strategy_name']}.csv", index=False)
        result["weekly_pnl"].to_csv(self.processed_dir / f"weekly_pnl_{result['strategy_name']}.csv", index=False)
        result["daily_pnl"].to_csv(self.processed_dir / f"daily_pnl_{result['strategy_name']}.csv", index=False)
        pd.DataFrame(result["bets"]).to_csv(self.processed_dir / f"backtest_bets_{result['strategy_name']}.csv", index=False)
        plot_equity_curve(
            result["equity_curve"],
            self.reports_dir / f"equity_curve_{result['strategy_name']}.png",
            title=f"{result['strategy_name']} equity curve",
        )
        return run_id
