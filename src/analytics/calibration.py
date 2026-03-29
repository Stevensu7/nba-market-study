from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from ..utils import clamp_probability


@dataclass(slots=True)
class CalibrationBin:
    left: float
    right: float
    calibrated_prob: float
    sample_count: int
    empirical_win_rate: float
    avg_implied_prob: float


class CalibrationModel:
    def __init__(self, bins: list[float], min_samples: int = 3, smoothing_prior_strength: int = 5):
        if bins[0] > 0.0:
            bins = [0.0] + bins
        if bins[-1] < 1.0:
            bins = bins + [1.0]
        self.bins = bins
        self.min_samples = min_samples
        self.smoothing_prior_strength = smoothing_prior_strength
        self.mapping: list[CalibrationBin] = []
        self.global_rate = 0.5

    def fit(self, df: pd.DataFrame, prob_col: str = "implied_prob", outcome_col: str = "outcome") -> "CalibrationModel":
        if df.empty:
            self.mapping = []
            self.global_rate = 0.5
            return self
        work = df[[prob_col, outcome_col]].copy()
        work[prob_col] = work[prob_col].clip(0.0, 1.0)
        self.global_rate = float(work[outcome_col].mean())
        work["bin"] = pd.cut(work[prob_col], bins=self.bins, include_lowest=True, right=True)
        grouped = work.groupby("bin", observed=False).agg(
            sample_count=(outcome_col, "size"),
            empirical_win_rate=(outcome_col, "mean"),
            avg_implied_prob=(prob_col, "mean"),
        )
        mapping: list[CalibrationBin] = []
        for interval, row in grouped.iterrows():
            if pd.isna(interval):
                continue
            sample_count = int(row["sample_count"])
            empirical = float(row["empirical_win_rate"]) if sample_count else self.global_rate
            avg_q = float(row["avg_implied_prob"]) if sample_count else float(interval.mid)
            calibrated = self._smoothed_prob(empirical, sample_count)
            mapping.append(
                CalibrationBin(
                    left=float(interval.left),
                    right=float(interval.right),
                    calibrated_prob=calibrated,
                    sample_count=sample_count,
                    empirical_win_rate=empirical,
                    avg_implied_prob=avg_q,
                )
            )
        self.mapping = mapping
        return self

    def reliability_table(self) -> pd.DataFrame:
        if not self.mapping:
            return pd.DataFrame(columns=["bin_left", "bin_right", "sample_count", "empirical_win_rate", "avg_implied_prob", "calibrated_prob"])
        return pd.DataFrame(
            [
                {
                    "bin_left": item.left,
                    "bin_right": item.right,
                    "sample_count": item.sample_count,
                    "empirical_win_rate": item.empirical_win_rate,
                    "avg_implied_prob": item.avg_implied_prob,
                    "calibrated_prob": item.calibrated_prob,
                }
                for item in self.mapping
            ]
        )

    def predict(self, q: float) -> float:
        q = clamp_probability(q)
        for item in self.mapping:
            if item.left <= q <= item.right:
                if item.sample_count >= self.min_samples:
                    return item.calibrated_prob
                return self._smoothed_prob(item.empirical_win_rate, item.sample_count)
        return self.global_rate

    def annotate(self, df: pd.DataFrame, prob_col: str = "implied_prob") -> pd.DataFrame:
        annotated = df.copy()
        annotated["p_hat"] = annotated[prob_col].apply(self.predict)
        annotated["edge"] = annotated["p_hat"] - annotated[prob_col]
        return annotated

    def _smoothed_prob(self, empirical: float, sample_count: int) -> float:
        if sample_count <= 0:
            return self.global_rate
        alpha = self.smoothing_prior_strength
        smoothed = (empirical * sample_count + self.global_rate * alpha) / (sample_count + alpha)
        return float(np.clip(smoothed, 0.0, 1.0))


def build_side_frame(frame: pd.DataFrame, price_field: str = "mid_price") -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in frame.to_dict(orient="records"):
        home_q = _select_price(row, "home", price_field)
        away_q = _select_price(row, "away", price_field)
        if home_q is not None:
            rows.append(
                {
                    "game_id": row["game_id"],
                    "platform": row["platform"],
                    "tipoff_time_utc": row["tipoff_time_utc"],
                    "snapshot_label": row["snapshot_label"],
                    "team": row["home_team"],
                    "side_type": "home",
                    "implied_prob": home_q,
                    "outcome": int(row["home_win"]),
                }
            )
        if away_q is not None:
            rows.append(
                {
                    "game_id": row["game_id"],
                    "platform": row["platform"],
                    "tipoff_time_utc": row["tipoff_time_utc"],
                    "snapshot_label": row["snapshot_label"],
                    "team": row["away_team"],
                    "side_type": "away",
                    "implied_prob": away_q,
                    "outcome": int(row["away_win"]),
                }
            )
    return pd.DataFrame(rows)


def _select_price(row: dict[str, Any], prefix: str, price_field: str) -> float | None:
    direct = row.get(f"{prefix}_{price_field}")
    if direct is not None and not pd.isna(direct):
        return float(direct)
    bid = row.get(f"{prefix}_best_bid")
    ask = row.get(f"{prefix}_best_ask")
    if bid is None and ask is None:
        return None
    if bid is None:
        return float(ask)
    if ask is None:
        return float(bid)
    return float((bid + ask) / 2.0)
