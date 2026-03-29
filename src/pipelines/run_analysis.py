from __future__ import annotations

import argparse
import json
import logging

import pandas as pd

from ..analytics.calibration import CalibrationModel, build_side_frame
from ..analytics.metrics import accuracy_score, brier_score, log_loss
from ..analytics.plots import plot_calibration_curve
from ..config import load_settings
from ..db import Database
from ..logging_utils import setup_logging
from ..utils import dump_json


logger = logging.getLogger(__name__)


def run(platform: str, snapshot_label: str, price_field: str) -> dict[str, float | str]:
    settings = load_settings()
    setup_logging(settings.logging.level, settings.paths.logs_dir)
    db = Database(settings.paths.database)
    db.initialize()

    frame = db.load_analysis_frame(platform=platform, snapshot_label=snapshot_label)
    if frame.empty:
        logger.warning("No analysis rows available for platform=%s snapshot_label=%s", platform, snapshot_label)
        return {}

    side_frame = build_side_frame(frame, price_field=price_field)
    if side_frame.empty:
        logger.warning("No side-level rows could be constructed.")
        return {}

    model = CalibrationModel(
        bins=settings.analysis.probability_bins,
        min_samples=settings.analysis.min_bin_samples,
        smoothing_prior_strength=settings.analysis.smoothing_prior_strength,
    ).fit(side_frame)
    reliability = model.reliability_table()
    annotated = model.annotate(side_frame)

    game_level = frame.copy()
    game_level["home_q"] = frame[f"home_{price_field}"].fillna((frame["home_best_bid"] + frame["home_best_ask"]) / 2)
    game_level["away_q"] = frame[f"away_{price_field}"].fillna((frame["away_best_bid"] + frame["away_best_ask"]) / 2)
    game_level = game_level.dropna(subset=["home_q", "away_q"]).copy()
    if not game_level.empty:
        game_level["pred_home_win"] = (game_level["home_q"] >= game_level["away_q"]).astype(int)
        game_accuracy = float((game_level["pred_home_win"] == game_level["home_win"]).mean())
    else:
        game_accuracy = float("nan")

    summary = {
        "platform": platform,
        "snapshot_label": snapshot_label,
        "price_field": price_field,
        "sample_games": int(frame["game_id"].nunique()),
        "sample_sides": int(len(side_frame)),
        "accuracy": game_accuracy,
        "brier_score": brier_score(annotated["outcome"], annotated["implied_prob"]),
        "log_loss": log_loss(annotated["outcome"], annotated["implied_prob"]),
    }

    reliability_path = settings.paths.processed_data_dir / f"reliability_table_{platform}_{snapshot_label}.csv"
    summary_path = settings.paths.reports_dir / f"analysis_summary_{platform}_{snapshot_label}.json"
    annotated_path = settings.paths.processed_data_dir / f"analysis_side_frame_{platform}_{snapshot_label}.csv"
    chart_path = settings.paths.reports_dir / f"calibration_curve_{platform}_{snapshot_label}.png"

    reliability.to_csv(reliability_path, index=False)
    annotated.to_csv(annotated_path, index=False)
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)
    plot_calibration_curve(reliability, chart_path, title=f"Calibration curve {platform} {snapshot_label}")
    logger.info("Analysis outputs written to %s", settings.paths.reports_dir)
    return summary


def main() -> None:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Run NBA market analysis.")
    parser.add_argument("--platform", default=settings.collection.default_platform)
    parser.add_argument("--snapshot-label", default=settings.analysis.default_snapshot_label)
    parser.add_argument("--price-field", default=settings.analysis.default_price_field)
    args = parser.parse_args()
    run(platform=args.platform, snapshot_label=args.snapshot_label, price_field=args.price_field)


if __name__ == "__main__":
    main()
