import pandas as pd

from src.analytics.calibration import CalibrationModel


def test_calibration_mapping_and_prediction() -> None:
    df = pd.DataFrame(
        {
            "implied_prob": [0.52, 0.53, 0.54, 0.71, 0.72, 0.73],
            "outcome": [1, 0, 1, 1, 1, 0],
        }
    )
    model = CalibrationModel(bins=[0.50, 0.55, 0.70, 0.75, 1.00], min_samples=2, smoothing_prior_strength=2).fit(df)
    table = model.reliability_table()
    assert not table.empty
    pred = model.predict(0.53)
    assert 0.4 <= pred <= 0.8


def test_small_sample_falls_back_smoothly() -> None:
    df = pd.DataFrame({"implied_prob": [0.91], "outcome": [1]})
    model = CalibrationModel(bins=[0.50, 0.80, 0.95, 1.00], min_samples=3, smoothing_prior_strength=5).fit(df)
    pred = model.predict(0.92)
    assert 0.5 <= pred <= 1.0
