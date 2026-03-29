from __future__ import annotations

import argparse
import logging

from ..config import load_settings
from ..db import Database
from ..collectors.polymarket_nba import PolymarketNBACollector
from ..logging_utils import setup_logging


logger = logging.getLogger(__name__)


def run() -> None:
    settings = load_settings()
    setup_logging(settings.logging.level, settings.paths.logs_dir)
    db = Database(settings.paths.database)
    db.initialize()
    collector = PolymarketNBACollector(settings, db)
    discovered = collector.discover_games()
    inserted = collector.collect_due_snapshots()
    logger.info("Collect pipeline finished. discovered=%s snapshots=%s", len(discovered), inserted)


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect Polymarket NBA snapshots.")
    parser.parse_args()
    run()


if __name__ == "__main__":
    main()
