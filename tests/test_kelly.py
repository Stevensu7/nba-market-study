from src.backtest.strategies import half_kelly_bet_size


def test_half_kelly_bet_size_positive_case() -> None:
    stake, detail = half_kelly_bet_size(bankroll=500, q=0.40, p_hat=0.50, max_bet=50, min_bet=5, kelly_fraction=0.5)
    assert round(stake, 2) == 41.67
    assert detail["reason"] == "bet"


def test_half_kelly_boundary_cases() -> None:
    stake, detail = half_kelly_bet_size(bankroll=500, q=1.0, p_hat=0.6, max_bet=50, min_bet=5)
    assert stake == 0.0
    assert detail["reason"] == "invalid_price"

    stake, detail = half_kelly_bet_size(bankroll=500, q=0.55, p_hat=0.50, max_bet=50, min_bet=5)
    assert stake == 0.0
    assert detail["reason"] == "non_positive_kelly"
