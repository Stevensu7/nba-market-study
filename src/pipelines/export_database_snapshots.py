from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from ..config import load_settings
from ..db import Database
from ..logging_utils import setup_logging

logger = logging.getLogger(__name__)


def _to_frame(rows: list[dict] | list) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    if isinstance(rows[0], dict):
        return pd.DataFrame(rows)
    return pd.DataFrame([dict(row) for row in rows])


def run(output_dir: Path | None = None) -> Path:
    settings = load_settings()
    setup_logging(settings.logging.level, settings.paths.logs_dir)
    db = Database(settings.paths.database, settings.paths.database_url, settings.paths.database_auth_token)
    db.initialize()
    target_dir = output_dir or (settings.paths.processed_data_dir / "db_exports")
    target_dir.mkdir(parents=True, exist_ok=True)

    games = _to_frame(db.list_games())
    games.to_csv(target_dir / "games.csv", index=False)

    if not games.empty and "id" in games.columns:
        snapshot_rows = []
        result_rows = []
        for game_id in games["id"].tolist():
            snapshot_rows.extend(db.list_snapshots_for_game(int(game_id)))
        snapshots = _to_frame(snapshot_rows)
        snapshots.to_csv(target_dir / "price_snapshots.csv", index=False)

        seen = set()
        for _, row in games.iterrows():
            key = (
                str(row.get("platform", "")),
                str(row.get("home_team", "")),
                str(row.get("away_team", "")),
                str(row.get("tipoff_time_utc", ""))[:10],
            )
            if key in seen:
                continue
            seen.add(key)
            result = db.get_game_result(*key)
            if result:
                result_rows.append(result)
        results = _to_frame(result_rows)
        results.to_csv(target_dir / "results.csv", index=False)
    else:
        pd.DataFrame().to_csv(target_dir / "price_snapshots.csv", index=False)
        pd.DataFrame().to_csv(target_dir / "results.csv", index=False)

    logger.info("Exported database snapshots to %s", target_dir)
    return target_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Export database tables to CSV backups.")
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()
    run(output_dir=Path(args.output_dir) if args.output_dir else None)


if __name__ == "__main__":
    main()
