from __future__ import annotations

import math
from typing import Iterable

import numpy as np

from ..utils import clamp_probability


def accuracy_score(y_true: Iterable[int], y_prob: Iterable[float], threshold: float = 0.5) -> float:
    y_true_arr = np.array(list(y_true), dtype=int)
    y_prob_arr = np.array(list(y_prob), dtype=float)
    if y_true_arr.size == 0:
        return float("nan")
    predictions = (y_prob_arr >= threshold).astype(int)
    return float((predictions == y_true_arr).mean())


def brier_score(y_true: Iterable[int], y_prob: Iterable[float]) -> float:
    y_true_arr = np.array(list(y_true), dtype=float)
    y_prob_arr = np.array(list(y_prob), dtype=float)
    if y_true_arr.size == 0:
        return float("nan")
    return float(np.mean((y_prob_arr - y_true_arr) ** 2))


def log_loss(y_true: Iterable[int], y_prob: Iterable[float], eps: float = 1e-6) -> float:
    y_true_arr = np.array(list(y_true), dtype=float)
    y_prob_arr = np.array([clamp_probability(float(p), eps=eps) for p in y_prob], dtype=float)
    if y_true_arr.size == 0:
        return float("nan")
    return float(-np.mean(y_true_arr * np.log(y_prob_arr) + (1 - y_true_arr) * np.log(1 - y_prob_arr)))


def max_drawdown(equity_curve: Iterable[float]) -> float:
    peak = -math.inf
    max_dd = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        if peak > 0:
            max_dd = max(max_dd, (peak - value) / peak)
    return max_dd
