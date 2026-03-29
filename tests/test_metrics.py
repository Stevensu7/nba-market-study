from src.analytics.metrics import accuracy_score, brier_score, log_loss


def test_accuracy_brier_logloss() -> None:
    y_true = [1, 0, 1, 1]
    y_prob = [0.8, 0.3, 0.6, 0.4]
    assert accuracy_score(y_true, y_prob) == 0.75
    assert round(brier_score(y_true, y_prob), 4) == 0.1625
    assert round(log_loss(y_true, y_prob), 4) == 0.5017
