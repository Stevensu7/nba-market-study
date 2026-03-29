from __future__ import annotations

import argparse
import json
import logging

from ..backtest.engine import BacktestEngine
from ..backtest.strategies import FixedStakeStrategy, HalfKellyStrategy
from ..config import load_settings
from ..db import Database
from ..logging_utils import setup_logging


logger = logging.getLogger(__name__)


def run(platform: str, snapshot_label: str, strategy_name: str, price_field: str) -> dict[str, object]:
    settings = load_settings()
    setup_logging(settings.logging.level, settings.paths.logs_dir)
    db = Database(settings.paths.database)
    db.initialize()
    engine = BacktestEngine(db, settings.paths.reports_dir, settings.paths.processed_data_dir)

    if strategy_name == "fixed":
        strategy = FixedStakeStrategy(
            fixed_stake=settings.backtest.fixed.fixed_stake,
            min_edge=settings.backtest.fixed.min_edge,
        )
        start_bankroll = settings.backtest.fixed.start_bankroll
    elif strategy_name == "half-kelly":
        strategy = HalfKellyStrategy(
            min_edge=settings.backtest.half_kelly.min_edge,
            max_bet=settings.backtest.half_kelly.max_bet,
            min_bet=settings.backtest.half_kelly.min_bet,
            kelly_fraction=settings.backtest.half_kelly.kelly_fraction,
        )
        start_bankroll = settings.backtest.half_kelly.start_bankroll
    else:
        raise ValueError(f"Unsupported strategy: {strategy_name}")

    result = engine.run(
        platform=platform,
        snapshot_label=snapshot_label,
        price_field=price_field,
        bins=settings.analysis.probability_bins,
        min_bin_samples=settings.analysis.min_bin_samples,
        smoothing_prior_strength=settings.analysis.smoothing_prior_strength,
        strategy=strategy,
        start_bankroll=start_bankroll,
    )
    run_id = engine.persist_results(result)
    summary_path = settings.paths.reports_dir / f"backtest_summary_{strategy_name}_{snapshot_label}.json"
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump({k: v for k, v in result.items() if k not in {"db_bets", "equity_curve", "weekly_pnl", "daily_pnl", "bets"}}, handle, indent=2)
    logger.info("Backtest finished. run_id=%s end_bankroll=%.2f", run_id, result["end_bankroll"])
    return result


def main() -> None:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Run NBA market backtest.")
    parser.add_argument("--platform", default=settings.collection.default_platform)
    parser.add_argument("--snapshot-label", default=settings.analysis.default_snapshot_label)
    parser.add_argument("--strategy", required=True, choices=["fixed", "half-kelly"])
    parser.add_argument("--price-field", default=settings.analysis.default_price_field)
    args = parser.parse_args()
    run(platform=args.platform, snapshot_label=args.snapshot_label, strategy_name=args.strategy, price_field=args.price_field)


if __name__ == "__main__":
    main()
