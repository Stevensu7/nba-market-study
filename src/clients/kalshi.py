from __future__ import annotations

from typing import Any

import requests

from ..config import AppSettings


class KalshiClient:
    """Phase-1 skeleton for future Kalshi support."""

    def __init__(self, settings: AppSettings):
        self.settings = settings
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "nba-market-study/0.1"})

    def list_markets(self, params: dict[str, Any] | None = None) -> dict[str, Any]:
        response = self.session.get(
            f"{self.settings.apis.kalshi_api_base_url.rstrip('/')}/markets",
            params=params or {},
            timeout=self.settings.apis.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    def get_price_snapshot(self, market_ticker: str) -> dict[str, Any]:
        raise NotImplementedError("Kalshi integration is reserved for a later phase.")
