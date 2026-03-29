from __future__ import annotations

import argparse
import logging
import time
from datetime import timedelta
from pathlib import Path

from ..config import load_settings
from ..logging_utils import setup_logging
from .export_latest_nba_markets_excel import beijing_now, export_excel


logger = logging.getLogger(__name__)


def run(output_path: Path | None = None, poll_seconds: int = 300) -> None:
    settings = load_settings()
    setup_logging(settings.logging.level, settings.paths.logs_dir)
    target_path = output_path or settings.root_dir / "backtest.xlsx"
    start_bjt = beijing_now()
    next_midnight = (start_bjt + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    logger.info("Starting daily tracker until %s Beijing time", next_midnight.isoformat())
    while beijing_now() < next_midnight:
        export_excel(target_path)
        remaining = max((next_midnight - beijing_now()).total_seconds(), 0)
        sleep_for = min(poll_seconds, int(remaining))
        if sleep_for <= 0:
            break
        logger.info("Sleeping %s seconds before next refresh", sleep_for)
        time.sleep(sleep_for)
    export_excel(target_path)
    logger.info("Daily tracker finished for workbook %s", target_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the all-day NBA market tracker from Beijing midnight until the next midnight.")
    parser.add_argument("--output", default=None, help="Optional workbook path")
    parser.add_argument("--poll-seconds", type=int, default=300, help="Refresh interval in seconds")
    args = parser.parse_args()
    run(output_path=Path(args.output) if args.output else None, poll_seconds=args.poll_seconds)


if __name__ == "__main__":
    main()
