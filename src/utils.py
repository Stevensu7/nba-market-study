from __future__ import annotations

import json
import math
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def utc_now() -> datetime:
    return datetime.now(UTC)


def to_utc_iso(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        parsed = parse_datetime(value)
        return parsed.isoformat() if parsed else None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    cleaned = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def dump_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=True, sort_keys=True, default=str)


def load_json(text: str | None, default: Any = None) -> Any:
    if not text:
        return default
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return default


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if value == "":
            return None
    except Exception:  # noqa: BLE001
        pass
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def midpoint(best_bid: float | None, best_ask: float | None) -> float | None:
    if best_bid is None and best_ask is None:
        return None
    if best_bid is None:
        return best_ask
    if best_ask is None:
        return best_bid
    return (best_bid + best_ask) / 2.0


def spread(best_bid: float | None, best_ask: float | None) -> float | None:
    if best_bid is None or best_ask is None:
        return None
    return best_ask - best_bid


def clamp_probability(value: float, eps: float = 1e-6) -> float:
    return min(max(value, eps), 1.0 - eps)


def team_key(name: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", name.lower()).strip()
    aliases = {
        "ny knicks": "new york knicks",
        "la lakers": "los angeles lakers",
        "la clippers": "los angeles clippers",
        "okc thunder": "oklahoma city thunder",
        "gs warriors": "golden state warriors",
        "no pelicans": "new orleans pelicans",
    }
    return aliases.get(cleaned, cleaned)


def normalize_team_name(name: str) -> str:
    tokens = [token.capitalize() for token in re.split(r"\s+", team_key(name)) if token]
    return " ".join(tokens)


def extract_matchup_teams(text: str) -> tuple[str, str] | None:
    patterns = [
        r"(?P<away>[A-Za-z .'-]+?)\s+vs\.?\s+(?P<home>[A-Za-z .'-]+)",
        r"(?P<away>[A-Za-z .'-]+?)\s+v\.?\s+(?P<home>[A-Za-z .'-]+)",
        r"(?P<away>[A-Za-z .'-]+?)\s+at\s+(?P<home>[A-Za-z .'-]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            away = normalize_team_name(match.group("away"))
            home = normalize_team_name(match.group("home"))
            return away, home
    return None


def choose_price(snapshot_row: dict[str, Any], side_prefix: str, price_field: str) -> float | None:
    field_map = {
        "mid_price": f"{side_prefix}_mid_price",
        "best_bid": f"{side_prefix}_best_bid",
        "best_ask": f"{side_prefix}_best_ask",
        "last_trade_price": f"{side_prefix}_last_trade_price",
    }
    selected = safe_float(snapshot_row.get(field_map.get(price_field, f"{side_prefix}_mid_price")))
    if selected is not None:
        return selected
    best_bid = safe_float(snapshot_row.get(f"{side_prefix}_best_bid"))
    best_ask = safe_float(snapshot_row.get(f"{side_prefix}_best_ask"))
    return midpoint(best_bid, best_ask)


def rolling_max_drawdown(equity: Iterable[float]) -> float:
    peak = None
    max_drawdown = 0.0
    for value in equity:
        if peak is None or value > peak:
            peak = value
        if peak and peak > 0:
            drawdown = (peak - value) / peak
            max_drawdown = max(max_drawdown, drawdown)
    return max_drawdown
