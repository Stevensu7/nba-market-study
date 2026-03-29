from __future__ import annotations

import argparse

from .config import load_settings
from .pipelines.collect_polymarket_snapshots import run as run_collect
from .pipelines.export_latest_nba_markets_excel import export_excel
from .pipelines.run_daily_tracker import run as run_daily_tracker
from .pipelines.resolve_game_results import run as run_resolve
from .pipelines.run_analysis import run as run_analysis
from .pipelines.run_backtest import run as run_backtest


def main() -> None:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="NBA prediction market study CLI.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("collect")
    subparsers.add_parser("resolve")
    subparsers.add_parser("refresh")
    track_parser = subparsers.add_parser("track-day")
    track_parser.add_argument("--poll-seconds", type=int, default=300)

    analyze_parser = subparsers.add_parser("analyze")
    analyze_parser.add_argument("--platform", default=settings.collection.default_platform)
    analyze_parser.add_argument("--snapshot-label", default=settings.analysis.default_snapshot_label)
    analyze_parser.add_argument("--price-field", default=settings.analysis.default_price_field)

    backtest_parser = subparsers.add_parser("backtest")
    backtest_parser.add_argument("--platform", default=settings.collection.default_platform)
    backtest_parser.add_argument("--snapshot-label", default=settings.analysis.default_snapshot_label)
    backtest_parser.add_argument("--strategy", required=True, choices=["fixed", "half-kelly"])
    backtest_parser.add_argument("--price-field", default=settings.analysis.default_price_field)

    args = parser.parse_args()
    if args.command == "collect":
        run_collect()
    elif args.command == "resolve":
        run_resolve()
    elif args.command == "refresh":
        export_excel(settings.root_dir / "backtest.xlsx")
    elif args.command == "track-day":
        run_daily_tracker(output_path=settings.root_dir / "backtest.xlsx", poll_seconds=args.poll_seconds)
    elif args.command == "analyze":
        run_analysis(platform=args.platform, snapshot_label=args.snapshot_label, price_field=args.price_field)
    elif args.command == "backtest":
        run_backtest(
            platform=args.platform,
            snapshot_label=args.snapshot_label,
            strategy_name=args.strategy,
            price_field=args.price_field,
        )


if __name__ == "__main__":
    main()
