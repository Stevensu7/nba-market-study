from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ..config import AppSettings
from ..utils import parse_datetime, safe_float, utc_now


logger = logging.getLogger(__name__)


class PolymarketClient:
    """Read-only client for public Polymarket endpoints."""

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
    def _get(self, base_url: str, path: str, params: dict[str, Any] | None = None) -> Any:
        response = self.session.get(
            f"{base_url.rstrip('/')}/{path.lstrip('/')}",
            params=params,
            timeout=self.settings.apis.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    def list_events(self, limit: int | None = None, active: bool = True, closed: bool = False) -> list[dict[str, Any]]:
        params = {
            "limit": limit or self.settings.collection.market_discovery_limit,
            "active": str(active).lower(),
            "closed": str(closed).lower(),
        }
        payload = self._get(self.settings.apis.polymarket_api_base_url, "/events", params=params)
        return payload if isinstance(payload, list) else payload.get("data", [])

    def get_order_book(self, token_id: str) -> dict[str, Any]:
        return self._get(self.settings.apis.polymarket_clob_base_url, "/book", params={"token_id": token_id})

    def get_midpoint(self, token_id: str) -> float | None:
        try:
            payload = self._get(self.settings.apis.polymarket_clob_base_url, "/midpoint", params={"token_id": token_id})
        except requests.RequestException:
            return None
        if isinstance(payload, dict):
            return safe_float(payload.get("mid")) or safe_float(payload.get("midpoint"))
        return safe_float(payload)

    def get_last_trade_price(self, token_id: str) -> float | None:
        try:
            payload = self._get(self.settings.apis.polymarket_clob_base_url, "/last-trade-price", params={"token_id": token_id})
        except requests.RequestException:
            return None
        if isinstance(payload, dict):
            return safe_float(payload.get("price") or payload.get("last_trade_price"))
        return safe_float(payload)

    def list_candidate_nba_markets(self) -> list[dict[str, Any]]:
        events = self.list_events()
        now = utc_now()
        max_tipoff = now + timedelta(days=self.settings.collection.market_discovery_days_ahead)
        candidates: list[dict[str, Any]] = []
        for event in events:
            title = (event.get("title") or "").lower()
            if "nba" not in title and "nba" not in str(event.get("slug", "")).lower():
                continue
            for market in event.get("markets", []):
                question = market.get("question") or ""
                market_text = f"{title} {question}".lower()
                if not self._is_moneyline_market(market_text):
                    continue
                tipoff = parse_datetime(
                    market.get("gameStartTime")
                    or market.get("endDate")
                    or event.get("startDate")
                    or event.get("endDate")
                )
                if tipoff and not (now - timedelta(days=1) <= tipoff <= max_tipoff):
                    continue
                candidates.append({"event": event, "market": market, "tipoff": tipoff.isoformat() if tipoff else None})
        logger.info("Found %s candidate NBA markets on Polymarket", len(candidates))
        return candidates

    @staticmethod
    def _is_moneyline_market(text: str) -> bool:
        excluded_terms = [
            "spread",
            "total",
            "points",
            "series",
            "champion",
            "playoffs",
            "division",
            "conference",
            "season",
        ]
        if any(term in text for term in excluded_terms):
            return False
        return any(term in text for term in [" vs ", " v ", " at "])
