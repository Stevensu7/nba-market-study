from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from ..models import BettingCandidate, StrategyDecision


@dataclass(slots=True)
class FixedStakeStrategy:
    fixed_stake: float
    min_edge: float

    @property
    def name(self) -> str:
        return "fixed"

    def select_bet(self, candidates: Sequence[BettingCandidate], bankroll: float) -> tuple[BettingCandidate | None, StrategyDecision]:
        best = max(candidates, key=lambda item: item.edge, default=None)
        if best is None:
            return None, StrategyDecision(False, reason={"reason": "no_candidates"})
        if best.edge < self.min_edge:
            return None, StrategyDecision(False, reason={"reason": "edge_below_threshold", "edge": best.edge})
        if bankroll <= 0:
            return None, StrategyDecision(False, reason={"reason": "bankroll_depleted"})
        stake = min(self.fixed_stake, bankroll)
        return best, StrategyDecision(True, stake=stake, reason={"reason": "bet", "edge": best.edge})


@dataclass(slots=True)
class HalfKellyStrategy:
    min_edge: float
    max_bet: float
    min_bet: float
    kelly_fraction: float

    @property
    def name(self) -> str:
        return "half-kelly"

    def select_bet(self, candidates: Sequence[BettingCandidate], bankroll: float) -> tuple[BettingCandidate | None, StrategyDecision]:
        best_candidate = None
        best_stake = 0.0
        best_reason: dict[str, float | str] = {"reason": "no_positive_kelly"}
        for candidate in sorted(candidates, key=lambda item: item.edge, reverse=True):
            if candidate.edge < self.min_edge:
                continue
            stake, detail = half_kelly_bet_size(
                bankroll=bankroll,
                q=candidate.implied_prob_q,
                p_hat=candidate.estimated_prob_p_hat,
                max_bet=self.max_bet,
                min_bet=self.min_bet,
                kelly_fraction=self.kelly_fraction,
            )
            if stake > best_stake:
                best_candidate = candidate
                best_stake = stake
                best_reason = detail
        if best_candidate is None or best_stake <= 0:
            return None, StrategyDecision(False, reason=best_reason)
        return best_candidate, StrategyDecision(True, stake=best_stake, reason=best_reason)


def half_kelly_bet_size(
    bankroll: float,
    q: float,
    p_hat: float,
    max_bet: float,
    min_bet: float,
    kelly_fraction: float = 0.5,
) -> tuple[float, dict[str, float | str]]:
    if bankroll <= 0:
        return 0.0, {"reason": "bankroll_depleted"}
    if q <= 0 or q >= 1:
        return 0.0, {"reason": "invalid_price", "q": q}
    if p_hat <= 0 or p_hat >= 1:
        p_hat = min(max(p_hat, 1e-6), 1 - 1e-6)
    b = (1 - q) / q
    if b <= 0:
        return 0.0, {"reason": "invalid_odds", "b": b}
    full_kelly = (b * p_hat - (1 - p_hat)) / b
    fraction = kelly_fraction * full_kelly
    if fraction <= 0:
        return 0.0, {"reason": "non_positive_kelly", "fraction": fraction}
    raw_bet = bankroll * fraction
    stake = min(raw_bet, max_bet, bankroll)
    if stake < min_bet:
        return 0.0, {"reason": "below_min_bet", "stake": stake}
    return float(stake), {"reason": "bet", "fraction": fraction, "raw_bet": raw_bet}
