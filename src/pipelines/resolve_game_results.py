from __future__ import annotations

import argparse
import logging

from ..collectors.polymarket_nba import PolymarketNBACollector
from ..config import load_settings
from ..db import Database
from ..logging_utils import setup_logging


logger = logging.getLogger(__name__)


def run() -> None:
    settings = load_settings()
    setup_logging(settings.logging.level, settings.paths.logs_dir)
    db = Database(settings.paths.database)
    db.initialize()
    collector = PolymarketNBACollector(settings, db)
    resolved = collector.resolve_results()
    logger.info("Resolve pipeline finished. resolved=%s", resolved)


def main() -> None:
    parser = argparse.ArgumentParser(description="Resolve NBA game results.")
    parser.parse_args()
    run()


if __name__ == "__main__":
    main()
