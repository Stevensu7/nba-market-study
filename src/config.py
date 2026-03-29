from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
import os

from .utils import ensure_dir


@dataclass(slots=True)
class APISettings:
    polymarket_api_base_url: str
    polymarket_clob_base_url: str
    kalshi_api_base_url: str
    espn_scoreboard_base_url: str
    timeout_seconds: int


@dataclass(slots=True)
class CollectionSettings:
    default_platform: str
    snapshot_labels: dict[str, int]
    snapshot_tolerance_minutes: int
    market_discovery_days_ahead: int
    market_discovery_limit: int


@dataclass(slots=True)
class AnalysisSettings:
    default_snapshot_label: str
    default_price_field: str
    probability_bins: list[float]
    min_bin_samples: int
    smoothing_prior_strength: int


@dataclass(slots=True)
class FixedStrategySettings:
    start_bankroll: float
    fixed_stake: float
    min_edge: float


@dataclass(slots=True)
class HalfKellySettings:
    start_bankroll: float
    min_edge: float
    max_bet: float
    min_bet: float
    kelly_fraction: float


@dataclass(slots=True)
class BacktestSettings:
    fixed: FixedStrategySettings
    half_kelly: HalfKellySettings


@dataclass(slots=True)
class PathSettings:
    database: Path
    raw_data_dir: Path
    processed_data_dir: Path
    reports_dir: Path
    logs_dir: Path


@dataclass(slots=True)
class LoggingSettings:
    level: str


@dataclass(slots=True)
class AppSettings:
    project_name: str
    timezone: str
    root_dir: Path
    paths: PathSettings
    apis: APISettings
    collection: CollectionSettings
    analysis: AnalysisSettings
    backtest: BacktestSettings
    logging: LoggingSettings


def _merge_env(cfg: dict[str, Any]) -> dict[str, Any]:
    cfg["apis"]["polymarket_api_base_url"] = os.getenv(
        "POLYMARKET_API_BASE_URL", cfg["apis"]["polymarket_api_base_url"]
    )
    cfg["apis"]["polymarket_clob_base_url"] = os.getenv(
        "POLYMARKET_CLOB_BASE_URL", cfg["apis"]["polymarket_clob_base_url"]
    )
    cfg["apis"]["kalshi_api_base_url"] = os.getenv(
        "KALSHI_API_BASE_URL", cfg["apis"]["kalshi_api_base_url"]
    )
    cfg["apis"]["espn_scoreboard_base_url"] = os.getenv(
        "ESPN_SCOREBOARD_BASE_URL", cfg["apis"]["espn_scoreboard_base_url"]
    )
    cfg["paths"]["database"] = os.getenv("DATABASE_PATH", cfg["paths"]["database"])
    cfg["logging"]["level"] = os.getenv("LOG_LEVEL", cfg["logging"]["level"])
    cfg["project"]["timezone"] = os.getenv("TIMEZONE", cfg["project"]["timezone"])
    return cfg


def load_settings() -> AppSettings:
    root_dir = Path(__file__).resolve().parent.parent
    load_dotenv(root_dir / ".env")
    with (root_dir / "config" / "settings.yaml").open("r", encoding="utf-8") as handle:
        cfg: dict[str, Any] = yaml.safe_load(handle)
    cfg = _merge_env(cfg)

    paths = PathSettings(
        database=root_dir / cfg["paths"]["database"],
        raw_data_dir=ensure_dir(root_dir / cfg["paths"]["raw_data_dir"]),
        processed_data_dir=ensure_dir(root_dir / cfg["paths"]["processed_data_dir"]),
        reports_dir=ensure_dir(root_dir / cfg["paths"]["reports_dir"]),
        logs_dir=ensure_dir(root_dir / cfg["paths"]["logs_dir"]),
    )
    ensure_dir(paths.database.parent)

    return AppSettings(
        project_name=cfg["project"]["name"],
        timezone=cfg["project"]["timezone"],
        root_dir=root_dir,
        paths=paths,
        apis=APISettings(**cfg["apis"]),
        collection=CollectionSettings(**cfg["collection"]),
        analysis=AnalysisSettings(**cfg["analysis"]),
        backtest=BacktestSettings(
            fixed=FixedStrategySettings(**cfg["backtest"]["fixed"]),
            half_kelly=HalfKellySettings(**cfg["backtest"]["half_kelly"]),
        ),
        logging=LoggingSettings(**cfg["logging"]),
    )
