from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def plot_calibration_curve(reliability_table: pd.DataFrame, output_path: Path, title: str) -> None:
    plt.figure(figsize=(8, 6))
    plt.plot([0, 1], [0, 1], linestyle="--", color="gray", label="Perfect calibration")
    if not reliability_table.empty:
        plt.plot(
            reliability_table["avg_implied_prob"],
            reliability_table["empirical_win_rate"],
            marker="o",
            label="Observed",
        )
    plt.xlabel("Average implied probability")
    plt.ylabel("Observed win rate")
    plt.title(title)
    plt.legend()
    plt.grid(alpha=0.2)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def plot_equity_curve(equity_df: pd.DataFrame, output_path: Path, title: str) -> None:
    plt.figure(figsize=(10, 6))
    if not equity_df.empty:
        plt.plot(pd.to_datetime(equity_df["date"]), equity_df["bankroll"], marker="o")
    plt.xlabel("Date")
    plt.ylabel("Bankroll")
    plt.title(title)
    plt.grid(alpha=0.2)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
