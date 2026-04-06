from __future__ import annotations

import logging
from typing import Any

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ..config import AppSettings
from ..utils import parse_datetime, safe_float, utc_now

logger = logging.getLogger(__name__)

NBA_EVENT_TICKER_PREFIX = "KXNBAGAME-"


class KalshiClient:
    """Kalshi API client for NBA markets."""

    def __init__(self, settings: AppSettings):
        self.settings = settings
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "nba-market-study/0.1"})

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        response = self.session.get(
            f"{self.settings.apis.kalshi_api_base_url.rstrip('/')}/{path.lstrip('/')}",
            params=params or {},
            timeout=self.settings.apis.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    def list_markets(self, params: dict[str, Any] | None = None) -> dict[str, Any]:
        response = self.session.get(
            f"{self.settings.apis.kalshi_api_base_url.rstrip('/')}/markets",
            params=params or {},
            timeout=self.settings.apis.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    def get_event(self, event_ticker: str) -> dict[str, Any] | None:
        """Fetch a single event by ticker."""
        try:
            return self._get(f"/events/{event_ticker}")
        except requests.HTTPError as exc:
            if exc.response.status_code == 404:
                return None
            raise

    def list_nba_events(self, status: str = "open", limit: int = 100) -> list[dict[str, Any]]:
        """
        List all NBA game events from Kalshi API.
        
        Uses the /events endpoint with series_ticker filter to get all NBA events.
        """
        try:
            # Get events by series ticker for NBA daily games
            params = {
                "series_ticker": "KXNBADAILY",  # NBA daily series
                "status": status,
                "limit": limit,
            }
            response = self._get("/events", params=params)
            events = response.get("events", [])
            logger.info("Found %s NBA events from Kalshi", len(events))
            return events
        except Exception as exc:
            logger.warning("Failed to list NBA events from Kalshi: %s", exc)
            return []

    def list_nba_markets(self, status: str = "open", limit: int = 200) -> list[dict[str, Any]]:
        """
        List all NBA markets directly from Kalshi API.
        
        Uses the /markets endpoint with event_ticker prefix to filter NBA markets.
        """
        try:
            # Get all markets with NBA event ticker prefix
            params = {
                "status": status,
                "limit": limit,
            }
            response = self._get("/markets", params=params)
            all_markets = response.get("markets", [])
            
            # Filter for NBA game markets only
            nba_markets = [
                m for m in all_markets 
                if (m.get("event_ticker") or "").startswith(NBA_EVENT_TICKER_PREFIX)
                or "nba" in (m.get("title") or "").lower()
            ]
            logger.info("Found %s NBA markets from Kalshi (filtered from %s total)", 
                       len(nba_markets), len(all_markets))
            return nba_markets
        except Exception as exc:
            logger.warning("Failed to list NBA markets from Kalshi: %s", exc)
            return []

    def get_market_snapshot(self, market_ticker: str) -> dict[str, Any] | None:
        """
        Get current price snapshot for a specific market.
        
        Returns market data including yes_bid, yes_ask, last_price, etc.
        """
        try:
            response = self._get(f"/markets/{market_ticker}")
            market = response.get("market", {})
            return {
                "ticker": market_ticker,
                "title": market.get("title", ""),
                "yes_bid": safe_float(market.get("yes_bid")),
                "yes_ask": safe_float(market.get("yes_ask")),
                "last_price": safe_float(market.get("last_price")),
                "yes_sub_title": market.get("yes_sub_title", ""),
                "event_ticker": market.get("event_ticker", ""),
                "status": market.get("status", ""),
                "close_time": market.get("close_time"),
                "volume": safe_float(market.get("volume")),
                "snapshot_time_utc": utc_now().isoformat(),
            }
        except requests.HTTPError as exc:
            if exc.response.status_code == 404:
                return None
            raise
        except Exception as exc:
            logger.warning("Failed to get market snapshot for %s: %s", market_ticker, exc)
            return None

    def get_price_snapshot(self, market_ticker: str) -> dict[str, Any]:
        """Legacy method - use get_market_snapshot instead."""
        result = self.get_market_snapshot(market_ticker)
        if result is None:
            raise ValueError(f"Market {market_ticker} not found")
        return result
