from __future__ import annotations

import argparse
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from ..clients.api_basketball import APIBasketballClient, SportsbookConsensus
from ..config import load_settings
from ..logging_utils import setup_logging
from ..utils import extract_matchup_teams, midpoint, normalize_team_name, parse_datetime, safe_float


logger = logging.getLogger(__name__)
BEIJING_TZ = ZoneInfo("Asia/Shanghai")


TEAM_HINT_ALIASES = {
    "atlanta": "atlanta hawks",
    "boston": "boston celtics",
    "brooklyn": "brooklyn nets",
    "charlotte": "charlotte hornets",
    "chicago": "chicago bulls",
    "cleveland": "cleveland cavaliers",
    "dallas": "dallas mavericks",
    "denver": "denver nuggets",
    "detroit": "detroit pistons",
    "golden state": "golden state warriors",
    "houston": "houston rockets",
    "indiana": "indiana pacers",
    "los angeles c": "los angeles clippers",
    "los angeles cl": "los angeles clippers",
    "los angeles l": "los angeles lakers",
    "memphis": "memphis grizzlies",
    "miami": "miami heat",
    "milwaukee": "milwaukee bucks",
    "minnesota": "minnesota timberwolves",
    "new orleans": "new orleans pelicans",
    "new york": "new york knicks",
    "oklahoma city": "oklahoma city thunder",
    "orlando": "orlando magic",
    "philadelphia": "philadelphia 76ers",
    "phoenix": "phoenix suns",
    "portland": "portland trail blazers",
    "sacramento": "sacramento kings",
    "san antonio": "san antonio spurs",
    "toronto": "toronto raptors",
    "utah": "utah jazz",
    "washington": "washington wizards",
}

ESPN_TO_KALSHI_ABBR = {
    "GS": "GSW",
    "WSH": "WAS",
}


@dataclass(slots=True)
class UpcomingGame:
    game_time_utc: str
    home_team: str
    away_team: str
    home_abbr: str
    away_abbr: str
    winner_team: str | None = None
    completed: bool = False


def utc_now() -> datetime:
    return datetime.now(UTC)


def beijing_now() -> datetime:
    return datetime.now(BEIJING_TZ)


def to_beijing_label(value: str | datetime | None) -> str:
    if value is None:
        return ""
    dt = parse_datetime(value) if isinstance(value, str) else value
    if dt is None:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _price_to_string(home_team: str, away_team: str, home_price: float | None, away_price: float | None) -> str:
    home_text = f"{home_team} {home_price:.3f}" if home_price is not None else f"{home_team} N/A"
    away_text = f"{away_team} {away_price:.3f}" if away_price is not None else f"{away_team} N/A"
    return f"{home_text} | {away_text}"


def _parse_probs_from_odds_text(odds_text: str, home_team: str, away_team: str) -> tuple[float | None, float | None]:
    if not odds_text:
        return None, None
    parts = [part.strip() for part in odds_text.split("|")]
    home_prob = None
    away_prob = None
    for part in parts:
        match = re.match(r"(.+?)\s+([0-9]*\.?[0-9]+)$", part)
        if not match:
            continue
        team = match.group(1).strip()
        prob = safe_float(match.group(2))
        if prob is None:
            continue
        if _team_matches(team, home_team):
            home_prob = prob
        elif _team_matches(team, away_team):
            away_prob = prob
    return home_prob, away_prob


def _predicted_winner(home_team: str, away_team: str, home_price: float | None, away_price: float | None) -> str:
    if home_price is not None and home_price > 0.5:
        return home_team
    if away_price is not None and away_price > 0.5:
        return away_team
    if home_price is not None and away_price is not None:
        return home_team if home_price >= away_price else away_team
    return ""


def _matchup_from_text(text: str) -> tuple[str, str] | None:
    matchup = extract_matchup_teams(text)
    if matchup is None:
        return None
    away_team = normalize_team_name(matchup[0])
    home_team = normalize_team_name(matchup[1])
    return home_team, away_team


def _canonical_team_hint(name: str) -> str:
    normalized = normalize_team_name(name).lower().strip()
    return TEAM_HINT_ALIASES.get(normalized, normalized)


def _team_matches(hint: str, full_name: str) -> bool:
    hint_norm = _canonical_team_hint(hint)
    full_norm = normalize_team_name(full_name).lower().strip()
    if not hint_norm or not full_norm:
        return False
    if hint_norm == full_norm:
        return True
    if hint_norm in full_norm or full_norm in hint_norm:
        return True
    hint_tokens = [token for token in hint_norm.split() if token not in {"the"}]
    full_tokens = [token for token in full_norm.split() if token not in {"the"}]
    return bool(hint_tokens) and all(token in full_tokens for token in hint_tokens)


def _find_schedule_game(
    home_hint: str,
    away_hint: str,
    schedule_map: dict[tuple[str, str], UpcomingGame],
) -> UpcomingGame | None:
    direct = schedule_map.get((home_hint, away_hint))
    if direct is not None:
        return direct
    for (home_team, away_team), game in schedule_map.items():
        if _team_matches(home_hint, home_team) and _team_matches(away_hint, away_team):
            return game
    return None


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:  # noqa: BLE001
        pass
    return str(value).strip()


def _build_schedule_map() -> dict[tuple[str, str], UpcomingGame]:
    scoreboard_url = load_settings().apis.espn_scoreboard_base_url
    session = requests.Session()
    session.headers.update({"User-Agent": "nba-market-study/0.1"})
    now = utc_now()
    games: dict[tuple[str, str], UpcomingGame] = {}
    for delta in range(-1, 8):
        date_str = (now + timedelta(days=delta)).strftime("%Y%m%d")
        response = session.get(scoreboard_url, params={"dates": date_str}, timeout=20)
        response.raise_for_status()
        payload = response.json()
        for event in payload.get("events", []):
            event_time = parse_datetime(event.get("date"))
            if event_time is None:
                continue
            competition = (event.get("competitions") or [{}])[0]
            competitors = competition.get("competitors") or []
            home = next((item for item in competitors if item.get("homeAway") == "home"), None)
            away = next((item for item in competitors if item.get("homeAway") == "away"), None)
            if not home or not away:
                continue
            home_team = normalize_team_name(home.get("team", {}).get("displayName", ""))
            away_team = normalize_team_name(away.get("team", {}).get("displayName", ""))
            home_abbr = str(home.get("team", {}).get("abbreviation", ""))
            away_abbr = str(away.get("team", {}).get("abbreviation", ""))
            home_score = safe_float(home.get("score"))
            away_score = safe_float(away.get("score"))
            completed = bool(competition.get("status", {}).get("type", {}).get("completed"))
            winner_team = None
            if completed and home_score is not None and away_score is not None and home_score != away_score:
                winner_team = home_team if home_score > away_score else away_team
            games[(home_team, away_team)] = UpcomingGame(
                game_time_utc=event_time.isoformat(),
                home_team=home_team,
                away_team=away_team,
                home_abbr=home_abbr,
                away_abbr=away_abbr,
                winner_team=winner_team,
                completed=completed,
            )
    return games


def _kalshi_abbr(abbr: str) -> str:
    return ESPN_TO_KALSHI_ABBR.get(abbr, abbr)


def _kalshi_event_ticker(game: UpcomingGame) -> str:
    dt = parse_datetime(game.game_time_utc)
    if dt is None:
        raise ValueError("Invalid game time")
    date_part = dt.strftime("%y%b%d").upper()
    return f"KXNBAGAME-{date_part}{_kalshi_abbr(game.away_abbr)}{_kalshi_abbr(game.home_abbr)}"


def _parse_polymarket_cards(html: str) -> list[dict[str, Any]]:
    pattern = re.compile(
        r'href="/event/(?P<slug>nba-[^"?#]+)".*?'
        r'group-hover:underline decoration-2">(?P<team1>[^<]+)</p></div><p class="text-heading-lg font-semibold whitespace-nowrap">(?P<p1>\d+)%</p>.*?'
        r'group-hover:underline decoration-2">(?P<team2>[^<]+)</p></div><p class="text-heading-lg font-semibold whitespace-nowrap">(?P<p2>\d+)%</p>',
        flags=re.DOTALL,
    )
    seen: set[str] = set()
    cards: list[dict[str, Any]] = []
    for match in pattern.finditer(html):
        slug = match.group("slug")
        if slug in seen:
            continue
        seen.add(slug)
        cards.append(
            {
                "slug": slug,
                "team1": normalize_team_name(match.group("team1")),
                "team2": normalize_team_name(match.group("team2")),
                "p1": float(match.group("p1")) / 100.0,
                "p2": float(match.group("p2")) / 100.0,
            }
        )
    return cards


def fetch_polymarket_rows(schedule_map: dict[tuple[str, str], UpcomingGame]) -> list[dict[str, Any]]:
    now = datetime.now(UTC)
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    html = requests.get("https://polymarket.com/sports/nba", timeout=30).text
    for card in _parse_polymarket_cards(html):
        team1 = card["team1"]
        team2 = card["team2"]
        matched_game = None
        home_price = None
        away_price = None
        for (candidate_home, candidate_away), game in schedule_map.items():
            if _team_matches(team1, candidate_home) and _team_matches(team2, candidate_away):
                matched_game = game
                home_price = card["p1"]
                away_price = card["p2"]
                break
            if _team_matches(team2, candidate_home) and _team_matches(team1, candidate_away):
                matched_game = game
                home_price = card["p2"]
                away_price = card["p1"]
                break
        if matched_game is None:
            continue
        tipoff = parse_datetime(matched_game.game_time_utc)
        if tipoff is None or tipoff <= now:
            continue
        rows[(matched_game.home_team, matched_game.away_team)] = {
                "平台": "Polymarket",
                "比赛时间": matched_game.game_time_utc,
                "比赛时间(北京时间)": to_beijing_label(matched_game.game_time_utc),
                "记录日期(北京时间)": beijing_now().strftime("%Y-%m-%d"),
                "发现时间UTC": utc_now().isoformat(),
                "主队": matched_game.home_team,
                "客队": matched_game.away_team,
                "主队概率": round(home_price, 3) if home_price is not None else None,
                "客队概率": round(away_price, 3) if away_price is not None else None,
                "赔率": _price_to_string(matched_game.home_team, matched_game.away_team, home_price, away_price),
                "预测嬴方": _predicted_winner(matched_game.home_team, matched_game.away_team, home_price, away_price),
                "实际嬴方（后续补充）": "",
            }
    return sorted(rows.values(), key=lambda item: item["比赛时间"])


def fetch_kalshi_rows(schedule_map: dict[tuple[str, str], UpcomingGame]) -> list[dict[str, Any]]:
    settings = load_settings()
    base_url = settings.apis.kalshi_api_base_url.rstrip("/")
    now = utc_now()
    rows: list[dict[str, Any]] = []

    future_games: list[UpcomingGame] = []
    for game in schedule_map.values():
        game_dt = parse_datetime(game.game_time_utc)
        if game_dt is not None and game_dt > now:
            future_games.append(game)
    seen_tickers: set[str] = set()
    for game in sorted(future_games, key=lambda item: item.game_time_utc):
        event_ticker = _kalshi_event_ticker(game)
        if event_ticker in seen_tickers:
            continue
        seen_tickers.add(event_ticker)
        response = requests.get(f"{base_url}/events/{event_ticker}", timeout=30)
        if response.status_code == 404:
            continue
        response.raise_for_status()
        payload = response.json()
        event = payload.get("event", {})
        title = str(event.get("title", ""))
        matchup = _matchup_from_text(title)
        if matchup is None:
            continue
        home_team, away_team = matchup
        game = _find_schedule_game(home_team, away_team, schedule_map)
        if game is None:
            continue
        game_time = parse_datetime(game.game_time_utc)
        if game_time is None or game_time <= now:
            continue

        home_price = None
        away_price = None
        for market in payload.get("markets", []):
            team_name = str(market.get("yes_sub_title", ""))
            yes_bid = safe_float(market.get("yes_bid_dollars"))
            yes_ask = safe_float(market.get("yes_ask_dollars"))
            price = midpoint(yes_bid, yes_ask) or safe_float(market.get("last_price_dollars"))
            if _team_matches(team_name, home_team):
                home_price = price
            elif _team_matches(team_name, away_team):
                away_price = price

        if home_price is None and away_price is None:
            continue

        rows.append(
            {
                "平台": "Kalshi",
                "比赛时间": game.game_time_utc,
                "比赛时间(北京时间)": to_beijing_label(game.game_time_utc),
                "记录日期(北京时间)": beijing_now().strftime("%Y-%m-%d"),
                "发现时间UTC": utc_now().isoformat(),
                "主队": game.home_team,
                "客队": game.away_team,
                "主队概率": round(home_price, 3) if home_price is not None else None,
                "客队概率": round(away_price, 3) if away_price is not None else None,
                "赔率": _price_to_string(game.home_team, game.away_team, home_price, away_price),
                "预测嬴方": _predicted_winner(game.home_team, game.away_team, home_price, away_price),
                "实际嬴方（后续补充）": "",
            }
        )
    return sorted(rows, key=lambda item: item["比赛时间"])


def _load_existing_rows(workbook_path: Path) -> pd.DataFrame:
    if not workbook_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_excel(workbook_path, sheet_name="backtest")
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


def _normalize_existing_rows(existing: pd.DataFrame, schedule_map: dict[tuple[str, str], UpcomingGame]) -> pd.DataFrame:
    if existing.empty:
        return existing
    normalized = existing.copy()
    for idx, row in normalized.iterrows():
        game = _find_schedule_game(_clean_text(row.get("主队")), _clean_text(row.get("客队")), schedule_map)
        if game is not None:
            normalized.at[idx, "主队"] = game.home_team
            normalized.at[idx, "客队"] = game.away_team
            normalized.at[idx, "比赛时间"] = game.game_time_utc
            normalized.at[idx, "比赛时间(北京时间)"] = to_beijing_label(game.game_time_utc)
    return normalized


def fetch_sportsbook_consensus(schedule_map: dict[tuple[str, str], UpcomingGame]) -> dict[tuple[str, str], SportsbookConsensus]:
    settings = load_settings()
    client = APIBasketballClient(settings)
    if not client.enabled:
        return {}
    dates = sorted({game.game_time_utc[:10] for game in schedule_map.values() if game.game_time_utc})
    consensus_map: dict[tuple[str, str], SportsbookConsensus] = {}
    for date_str in dates:
        try:
            games = client.get_games_by_date(date_str)
        except Exception as exc:  # noqa: BLE001
            logger.warning("API-Basketball games lookup failed for %s: %s", date_str, exc)
            continue
        for game_payload in games:
            home_team = normalize_team_name(str(game_payload.get("teams", {}).get("home", {}).get("name", "")))
            away_team = normalize_team_name(str(game_payload.get("teams", {}).get("away", {}).get("name", "")))
            matched = _find_schedule_game(home_team, away_team, schedule_map)
            if matched is None:
                continue
            try:
                consensus = client.build_consensus_for_game(game_payload)
            except Exception as exc:  # noqa: BLE001
                logger.warning("API-Basketball odds lookup failed for %s vs %s: %s", home_team, away_team, exc)
                continue
            consensus.home_team = matched.home_team
            consensus.away_team = matched.away_team
            consensus_map[(matched.home_team, matched.away_team)] = consensus
    return consensus_map


def _merge_rows(existing: pd.DataFrame, fresh_rows: list[dict[str, Any]]) -> pd.DataFrame:
    fresh_df = pd.DataFrame(fresh_rows)
    if existing.empty and fresh_df.empty:
        return pd.DataFrame()
    if existing.empty:
        combined = fresh_df
    elif fresh_df.empty:
        combined = existing.copy()
    else:
        key_cols = ["平台", "比赛时间", "主队", "客队"]
        existing = existing.copy()
        fresh_df = fresh_df.copy()
        existing["_fresh"] = 0
        fresh_df["_fresh"] = 1
        combined = pd.concat([existing, fresh_df], ignore_index=True, sort=False)
        combined = combined.sort_values(by=key_cols + ["_fresh"]).drop_duplicates(subset=key_cols, keep="last")
        combined = combined.drop(columns=["_fresh"])
    return combined


def _apply_results_and_pnl(
    df: pd.DataFrame,
    schedule_map: dict[tuple[str, str], UpcomingGame],
    sportsbook_map: dict[tuple[str, str], SportsbookConsensus],
) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.copy()
    defaults = {
        "比赛时间(北京时间)": "",
        "记录日期(北京时间)": beijing_now().strftime("%Y-%m-%d"),
        "发现时间UTC": "",
        "状态": "scheduled",
        "开赛快照时间UTC": "",
        "开赛主队概率": pd.NA,
        "开赛客队概率": pd.NA,
        "主流博彩主队概率": pd.NA,
        "主流博彩客队概率": pd.NA,
        "主流博彩样本数": pd.NA,
        "主流博彩来源": "",
        "主流博彩状态": "",
        "主队概率": pd.NA,
        "客队概率": pd.NA,
        "预测嬴方": "",
        "实际嬴方（后续补充）": "",
        "是否命中": "",
        "下注方向": "",
        "跨市场主队价差": pd.NA,
        "跨市场客队价差": pd.NA,
        "价差预警": "",
        "相对主流博彩主队价差": pd.NA,
        "相对主流博彩客队价差": pd.NA,
        "10U收益": pd.NA,
        "累计收益": pd.NA,
    }
    for column, default in defaults.items():
        if column not in work.columns:
            work[column] = default
    for column in [
        "比赛时间(北京时间)",
        "记录日期(北京时间)",
        "发现时间UTC",
        "状态",
        "开赛快照时间UTC",
        "主流博彩来源",
        "主流博彩状态",
        "预测嬴方",
        "实际嬴方（后续补充）",
        "是否命中",
        "下注方向",
        "赔率",
        "价差预警",
    ]:
        if column in work.columns:
            work[column] = work[column].astype("object")

    for idx, row in work.iterrows():
        game = _find_schedule_game(_clean_text(row.get("主队")), _clean_text(row.get("客队")), schedule_map)
        if game:
            work.at[idx, "比赛时间(北京时间)"] = to_beijing_label(game.game_time_utc)
        game_dt = parse_datetime(game.game_time_utc) if game else None
        if game and game.completed and game.winner_team:
            work.at[idx, "实际嬴方（后续补充）"] = game.winner_team
            work.at[idx, "状态"] = "final"
        elif game and game_dt is not None and game_dt <= utc_now():
            work.at[idx, "状态"] = "in_play"
        else:
            work.at[idx, "状态"] = "scheduled"
        if not _clean_text(row.get("发现时间UTC")):
            work.at[idx, "发现时间UTC"] = utc_now().isoformat()
        work.at[idx, "记录日期(北京时间)"] = _clean_text(row.get("记录日期(北京时间)")) or beijing_now().strftime("%Y-%m-%d")
        if game:
            consensus = sportsbook_map.get((game.home_team, game.away_team))
            if consensus:
                work.at[idx, "主流博彩主队概率"] = round(consensus.home_probability, 4) if consensus.home_probability is not None else pd.NA
                work.at[idx, "主流博彩客队概率"] = round(consensus.away_probability, 4) if consensus.away_probability is not None else pd.NA
                work.at[idx, "主流博彩样本数"] = consensus.bookmaker_count
                work.at[idx, "主流博彩来源"] = ", ".join(consensus.bookmaker_names)
                work.at[idx, "主流博彩状态"] = consensus.status if not consensus.message else f"{consensus.status}: {consensus.message}"
        prediction = _clean_text(work.at[idx, "预测嬴方"])
        actual = _clean_text(work.at[idx, "实际嬴方（后续补充）"])
        work.at[idx, "下注方向"] = prediction
        home_prob = safe_float(work.at[idx, "主队概率"])
        away_prob = safe_float(work.at[idx, "客队概率"])
        if home_prob is None or away_prob is None:
            parsed_home_prob, parsed_away_prob = _parse_probs_from_odds_text(
                _clean_text(work.at[idx, "赔率"]),
                _clean_text(row.get("主队")),
                _clean_text(row.get("客队")),
            )
            home_prob = home_prob if home_prob is not None else parsed_home_prob
            away_prob = away_prob if away_prob is not None else parsed_away_prob
            work.at[idx, "主队概率"] = home_prob
            work.at[idx, "客队概率"] = away_prob
        game_time = parse_datetime(_clean_text(row.get("比赛时间")))
        start_home_prob = safe_float(work.at[idx, "开赛主队概率"])
        start_away_prob = safe_float(work.at[idx, "开赛客队概率"])
        if game_time is not None and utc_now() >= game_time and (start_home_prob is None and start_away_prob is None):
            work.at[idx, "开赛快照时间UTC"] = utc_now().isoformat()
            work.at[idx, "开赛主队概率"] = home_prob
            work.at[idx, "开赛客队概率"] = away_prob
            start_home_prob = home_prob
            start_away_prob = away_prob
        bet_prob = None
        if prediction and prediction == _clean_text(row.get("主队")):
            bet_prob = start_home_prob if start_home_prob is not None else home_prob
        elif prediction and prediction == _clean_text(row.get("客队")):
            bet_prob = start_away_prob if start_away_prob is not None else away_prob
        if prediction and actual and bet_prob is not None and 0 < bet_prob < 1:
            hit = prediction == actual
            pnl = 10.0 * ((1 - bet_prob) / bet_prob) if hit else -10.0
            work.at[idx, "是否命中"] = "win" if hit else "loss"
            work.at[idx, "10U收益"] = round(pnl, 4)
        else:
            work.at[idx, "是否命中"] = ""
            work.at[idx, "10U收益"] = pd.NA
        sportsbook_home = safe_float(work.at[idx, "主流博彩主队概率"])
        sportsbook_away = safe_float(work.at[idx, "主流博彩客队概率"])
        if home_prob is not None and sportsbook_home is not None:
            work.at[idx, "相对主流博彩主队价差"] = round(home_prob - sportsbook_home, 4)
        if away_prob is not None and sportsbook_away is not None:
            work.at[idx, "相对主流博彩客队价差"] = round(away_prob - sportsbook_away, 4)

    group_cols = ["比赛时间", "主队", "客队"]
    for _, indices in work.groupby(group_cols, dropna=False).groups.items():
        idx_list = list(indices)
        if len(idx_list) < 2:
            continue
        subset = work.loc[idx_list]
        if set(subset["平台"].tolist()) >= {"Polymarket", "Kalshi"}:
            poly = subset[subset["平台"] == "Polymarket"].iloc[0]
            kal = subset[subset["平台"] == "Kalshi"].iloc[0]
            poly_home = safe_float(poly.get("开赛主队概率")) or safe_float(poly.get("主队概率"))
            kal_home = safe_float(kal.get("开赛主队概率")) or safe_float(kal.get("主队概率"))
            poly_away = safe_float(poly.get("开赛客队概率")) or safe_float(poly.get("客队概率"))
            kal_away = safe_float(kal.get("开赛客队概率")) or safe_float(kal.get("客队概率"))
            home_gap = abs(poly_home - kal_home) if poly_home is not None and kal_home is not None else None
            away_gap = abs(poly_away - kal_away) if poly_away is not None and kal_away is not None else None
            warn = (home_gap is not None and home_gap > 0.05) or (away_gap is not None and away_gap > 0.05)
            for i in idx_list:
                work.at[i, "跨市场主队价差"] = round(home_gap, 4) if home_gap is not None else pd.NA
                work.at[i, "跨市场客队价差"] = round(away_gap, 4) if away_gap is not None else pd.NA
                work.at[i, "价差预警"] = "YES" if warn else ""

    work = work.sort_values(by=["比赛时间", "平台", "主队", "客队"]).reset_index(drop=True)
    cumulative = 0.0
    cumulative_values: list[float] = []
    for pnl in work["10U收益"].tolist():
        numeric_pnl = safe_float(pnl)
        if numeric_pnl is not None:
            cumulative += numeric_pnl
            cumulative_values.append(round(cumulative, 4))
        else:
            cumulative_values.append(round(cumulative, 4))
    work["累计收益"] = cumulative_values
    return work


def _summary_metrics(df: pd.DataFrame) -> dict[str, Any]:
    resolved = df[df["实际嬴方（后续补充）"].fillna("") != ""] if not df.empty else pd.DataFrame()
    pnl_series = pd.Series(dtype=float)
    if not resolved.empty and "10U收益" in resolved.columns:
        pnl_series = pd.Series(resolved.loc[:, "10U收益"], dtype="float64").dropna()
    wins = int((resolved.get("是否命中", pd.Series(dtype=str)) == "win").sum()) if not resolved.empty else 0
    losses = int((resolved.get("是否命中", pd.Series(dtype=str)) == "loss").sum()) if not resolved.empty else 0
    total_pnl = float(pnl_series.sum()) if not pnl_series.empty else 0.0
    total_bets = int(len(pnl_series))
    roi = (total_pnl / (10.0 * total_bets)) if total_bets else 0.0
    warning_rows = 0
    if not df.empty and "价差预警" in df.columns:
        warning_rows = int((df["价差预警"].fillna("") == "YES").sum())
    sportsbook_rows = 0
    if not df.empty and "主流博彩样本数" in df.columns:
        sportsbook_values = [safe_float(value) or 0.0 for value in df["主流博彩样本数"].tolist()]
        sportsbook_rows = sum(1 for value in sportsbook_values if value > 0)
    platform_metrics: dict[str, dict[str, Any]] = {}
    if not resolved.empty and "平台" in resolved.columns:
        for platform, subset in resolved.groupby("平台", dropna=False):
            wins_by_platform = int((subset.get("是否命中", pd.Series(dtype=str)) == "win").sum())
            losses_by_platform = int((subset.get("是否命中", pd.Series(dtype=str)) == "loss").sum())
            settled_rows = int(len(subset))
            accuracy = (wins_by_platform / settled_rows) if settled_rows else 0.0
            platform_metrics[str(platform)] = {
                "resolved_rows": settled_rows,
                "wins": wins_by_platform,
                "losses": losses_by_platform,
                "accuracy": round(accuracy, 4),
            }
    return {
        "total_rows": int(len(df)),
        "resolved_rows": int(len(resolved)),
        "warning_rows": warning_rows,
        "sportsbook_rows": sportsbook_rows,
        "wins": wins,
        "losses": losses,
        "total_pnl": round(total_pnl, 4),
        "roi": round(roi, 4),
        "platform_counts": df["平台"].value_counts(dropna=False).to_dict() if not df.empty else {},
        "platform_metrics": platform_metrics,
    }


def _build_equity_svg(df: pd.DataFrame) -> str:
    resolved = df.copy()
    if "10U收益" not in resolved.columns or resolved.empty:
        return "<div class='muted'>No resolved bets yet.</div>"
    resolved["10U收益"] = pd.to_numeric(resolved["10U收益"], errors="coerce")
    resolved = resolved.dropna(subset=["10U收益"]).reset_index(drop=True)
    if resolved.empty:
        return "<div class='muted'>No resolved bets yet.</div>"
    values = [0.0]
    cumulative = 0.0
    for pnl in resolved["10U收益"].tolist():
        cumulative += float(pnl)
        values.append(cumulative)
    min_v = min(values)
    max_v = max(values)
    width = 920
    height = 260
    pad_x = 36
    pad_y = 20
    span = max(max_v - min_v, 1.0)
    points: list[str] = []
    for idx, value in enumerate(values):
        x = pad_x + (idx * (width - pad_x * 2) / max(len(values) - 1, 1))
        y = height - pad_y - ((value - min_v) / span) * (height - pad_y * 2)
        points.append(f"{x:.2f},{y:.2f}")
    baseline = height - pad_y - ((0 - min_v) / span) * (height - pad_y * 2)
    return (
        f"<svg viewBox='0 0 {width} {height}' class='chart' preserveAspectRatio='none'>"
        f"<line x1='{pad_x}' y1='{baseline:.2f}' x2='{width-pad_x}' y2='{baseline:.2f}' class='baseline' />"
        f"<polyline fill='none' stroke='url(#grad)' stroke-width='3' points='{' '.join(points)}' />"
        f"<defs><linearGradient id='grad' x1='0' y1='0' x2='1' y2='0'><stop offset='0%' stop-color='#0b57d0'/><stop offset='100%' stop-color='#0f9d58'/></linearGradient></defs>"
        f"</svg>"
    )


def _table_rows_html(rows: list[dict[str, Any]], settled_only: bool = False) -> str:
    fragments: list[str] = []
    for row in rows:
        actual = _clean_text(row.get("实际嬴方（后续补充）"))
        if settled_only and not actual:
            continue
        platform = _clean_text(row.get("平台"))
        result = _clean_text(row.get("是否命中"))
        status = _clean_text(row.get("状态"))
        warning = _clean_text(row.get("价差预警"))
        sportsbook_home = _clean_text(row.get("主流博彩主队概率"))
        sportsbook_away = _clean_text(row.get("主流博彩客队概率"))
        sportsbook_diff_home = _clean_text(row.get("相对主流博彩主队价差"))
        sportsbook_diff_away = _clean_text(row.get("相对主流博彩客队价差"))
        tag_class = "win" if result == "win" else "loss" if result == "loss" else "muted"
        row_class = "warning-row" if warning == "YES" else ""
        fragments.append(
            f"<tr class='{row_class}' data-platform='{platform}' data-settled='{str(bool(actual)).lower()}'>"
            f"<td>{platform}</td>"
            f"<td>{_clean_text(row.get('比赛时间(北京时间)') or row.get('比赛时间'))}</td>"
            f"<td>{_clean_text(row.get('主队'))}</td>"
            f"<td>{_clean_text(row.get('客队'))}</td>"
            f"<td>{_clean_text(row.get('赔率'))}</td>"
            f"<td>{sportsbook_home} | {sportsbook_away}</td>"
            f"<td>{sportsbook_diff_home} | {sportsbook_diff_away}</td>"
            f"<td>{_clean_text(row.get('预测嬴方'))}</td>"
            f"<td>{actual}</td>"
            f"<td>{status}</td>"
            f"<td>{warning}</td>"
            f"<td><span class='tag {tag_class}'>{result}</span></td>"
            f"<td>{_clean_text(row.get('10U收益'))}</td>"
            f"<td>{_clean_text(row.get('累计收益'))}</td>"
            "</tr>"
        )
    return "".join(fragments) or "<tr><td colspan='14' class='muted'>No rows available.</td></tr>"


def _write_html_report(df: pd.DataFrame, output_path: Path) -> Path:
    summary = _summary_metrics(df)
    rows_json = df.fillna("").to_dict(orient="records")
    updated_bjt = to_beijing_label(utc_now())
    platforms = sorted({row.get("平台", "") for row in rows_json if row.get("平台", "")})
    platform_buttons = "".join(
        f"<button class='filter-btn' data-platform='{platform}'>{platform}</button>" for platform in platforms
    )
    all_rows_html = _table_rows_html(rows_json, settled_only=False)
    settled_rows_html = _table_rows_html(rows_json, settled_only=True)
    chart_html = _build_equity_svg(df)
    warning_rows = [row for row in rows_json if _clean_text(row.get("价差预警")) == "YES"]
    warning_html = "".join(
        f"<div class='warning-item'><div class='warning-kicker'>ALERT</div><div><strong>{_clean_text(row.get('主队'))} vs {_clean_text(row.get('客队'))}</strong></div><div class='muted'>{_clean_text(row.get('比赛时间(北京时间)') or row.get('比赛时间'))}</div><div>Home gap: {_clean_text(row.get('跨市场主队价差'))} | Away gap: {_clean_text(row.get('跨市场客队价差'))}</div></div>"
        for row in warning_rows
    ) or "<div class='muted'>No divergence warnings above 0.05 right now.</div>"
    platform_accuracy_cards = "".join(
        (
            f"<div class='platform-card'>"
            f"<div class='platform-head'><span class='platform-name'>{platform}</span><span class='platform-accuracy'>{metrics['accuracy']:.1%}</span></div>"
            f"<div class='platform-bar'><span style='width:{metrics['accuracy'] * 100:.1f}%'></span></div>"
            f"<div class='platform-meta'>Resolved: {metrics['resolved_rows']} | Wins: {metrics['wins']} | Losses: {metrics['losses']}</div>"
            f"</div>"
        )
        for platform, metrics in sorted(summary["platform_metrics"].items())
    ) or "<div class='muted'>Prediction accuracy will appear after games settle.</div>"
    html = f"""<!doctype html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Backtest Dashboard</title>
  <style>
    :root {{ --bg:#f3efe8; --card:#fffdf8; --text:#1d2736; --muted:#6f7481; --line:#e6ddd0; --good:#0e8a4d; --bad:#cc3d2f; --accent:#b65a2a; --ink:#102034; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:Georgia, 'Segoe UI', serif; background:radial-gradient(circle at top left, #f4d8c4 0, #f7f0e7 22%, #f3efe8 65%); color:var(--text); }}
    .wrap {{ max-width:1280px; margin:0 auto; padding:36px 20px 56px; }}
    .hero {{ margin-bottom:24px; padding:28px; border:1px solid rgba(182,90,42,.18); border-radius:24px; background:linear-gradient(135deg, rgba(255,250,245,.92), rgba(255,255,255,.72)); box-shadow:0 16px 44px rgba(86,47,25,.08); position:relative; overflow:hidden; }}
    .hero:before {{ content:''; position:absolute; inset:auto -60px -80px auto; width:220px; height:220px; background:radial-gradient(circle, rgba(182,90,42,.22), rgba(182,90,42,0)); border-radius:50%; }}
    .hero h1 {{ margin:0 0 10px; font-size:38px; letter-spacing:.2px; color:var(--ink); }}
    .hero p {{ margin:0; color:var(--muted); max-width:760px; line-height:1.6; font-family:'Segoe UI', Arial, sans-serif; }}
    .hero-meta {{ margin-top:16px; display:flex; gap:12px; flex-wrap:wrap; font-family:'Segoe UI', Arial, sans-serif; }}
    .hero-pill {{ display:inline-flex; align-items:center; gap:8px; padding:8px 12px; border-radius:999px; background:rgba(255,255,255,.76); border:1px solid rgba(182,90,42,.18); color:var(--ink); font-size:13px; }}
    .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:16px; margin:24px 0; }}
    .card {{ background:var(--card); border:1px solid var(--line); border-radius:20px; padding:18px; box-shadow:0 14px 34px rgba(46,32,20,.06); }}
    .label {{ color:var(--muted); font-size:13px; margin-bottom:8px; font-family:'Segoe UI', Arial, sans-serif; }}
    .value {{ font-size:28px; font-weight:700; }}
    .value.good {{ color:var(--good); }}
    .value.bad {{ color:var(--bad); }}
    .split {{ display:grid; grid-template-columns:1.35fr .65fr; gap:18px; margin-bottom:18px; }}
    .platform-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); gap:16px; margin-bottom:20px; }}
    .platform-card {{ background:var(--card); border:1px solid var(--line); border-radius:20px; padding:18px; box-shadow:0 14px 34px rgba(46,32,20,.06); }}
    .platform-head {{ display:flex; align-items:baseline; justify-content:space-between; gap:12px; margin-bottom:10px; }}
    .platform-name {{ font-size:18px; font-weight:700; color:var(--ink); }}
    .platform-accuracy {{ font-size:28px; font-weight:700; color:var(--accent); }}
    .platform-bar {{ height:10px; border-radius:999px; background:#efe5d8; overflow:hidden; margin-bottom:10px; }}
    .platform-bar span {{ display:block; height:100%; border-radius:999px; background:linear-gradient(90deg, #d97706, #0e8a4d); }}
    .platform-meta {{ color:var(--muted); font-size:13px; font-family:'Segoe UI', Arial, sans-serif; }}
    .warning-item {{ padding:14px 14px 14px 16px; border:1px solid rgba(204,61,47,.18); border-left:5px solid var(--bad); border-radius:16px; background:linear-gradient(180deg, rgba(255,244,242,.98), rgba(255,250,248,.98)); margin-bottom:12px; font-family:'Segoe UI', Arial, sans-serif; }}
    .warning-item:last-child {{ border-bottom:none; }}
    .warning-kicker {{ display:inline-block; margin-bottom:8px; padding:4px 8px; border-radius:999px; background:var(--bad); color:white; font-size:11px; font-weight:700; letter-spacing:.08em; }}
    table {{ width:100%; border-collapse:collapse; background:var(--card); border:1px solid var(--line); border-radius:16px; overflow:hidden; }}
    th, td {{ padding:12px 10px; border-bottom:1px solid var(--line); text-align:left; font-size:14px; vertical-align:top; font-family:'Segoe UI', Arial, sans-serif; }}
    th {{ background:#fbf6ef; position:sticky; top:0; z-index:1; }}
    .table-wrap {{ overflow:auto; border-radius:16px; box-shadow:0 10px 30px rgba(15,23,42,.06); }}
    .tag {{ display:inline-block; padding:4px 8px; border-radius:999px; font-size:12px; font-weight:600; }}
    .win {{ background:#e8f5ee; color:var(--good); }}
    .loss {{ background:#fdecec; color:var(--bad); }}
    .muted {{ color:var(--muted); }}
    .warning-row td {{ background:rgba(204,61,47,.05); }}
    .toolbar {{ display:flex; flex-wrap:wrap; gap:10px; margin:18px 0; }}
    .filter-btn, .tab-btn {{ border:1px solid var(--line); background:white; color:var(--text); border-radius:999px; padding:8px 14px; cursor:pointer; font-weight:600; font-family:'Segoe UI', Arial, sans-serif; }}
    .filter-btn.active, .tab-btn.active {{ background:var(--accent); color:white; border-color:var(--accent); }}
    .panel {{ display:none; }}
    .panel.active {{ display:block; }}
    .chart {{ width:100%; height:260px; display:block; }}
    .baseline {{ stroke:#cbd5e1; stroke-dasharray:4 4; }}
    @media (max-width: 900px) {{ .split {{ grid-template-columns:1fr; }} .hero h1 {{ font-size:30px; }} }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <div class=\"hero\">
      <h1>NBA Market Lifecycle Dashboard</h1>
      <p>Beijing-midnight daily workflow. The workbook captures game discovery, game-time snapshots, final winners, divergence warnings between Polymarket and Kalshi, and 10U fixed-stake PnL once results settle.</p>
      <div class=\"hero-meta\">
        <div class=\"hero-pill\">Last updated (Beijing): {updated_bjt}</div>
        <div class=\"hero-pill\">Warning threshold: 0.05</div>
      </div>
    </div>
    <div class=\"grid\">
      <div class=\"card\"><div class=\"label\">Total Rows</div><div class=\"value\">{summary['total_rows']}</div></div>
      <div class=\"card\"><div class=\"label\">Resolved Rows</div><div class=\"value\">{summary['resolved_rows']}</div></div>
      <div class=\"card\"><div class=\"label\">Sportsbook Consensus Rows</div><div class=\"value\">{summary['sportsbook_rows']}</div><div class=\"label\">API-Basketball mainstream books</div></div>
      <div class=\"card\"><div class=\"label\">Divergence Warnings</div><div class=\"value {'bad' if summary['warning_rows'] else ''}\">{summary['warning_rows']}</div><div class=\"label\">Cross-market gap &gt; 0.05</div></div>
      <div class=\"card\"><div class=\"label\">Wins</div><div class=\"value good\">{summary['wins']}</div></div>
      <div class=\"card\"><div class=\"label\">Losses</div><div class=\"value bad\">{summary['losses']}</div></div>
      <div class=\"card\"><div class=\"label\">Total PnL</div><div class=\"value {'good' if summary['total_pnl'] >= 0 else 'bad'}\">{summary['total_pnl']:.4f}U</div></div>
      <div class=\"card\"><div class=\"label\">ROI</div><div class=\"value {'good' if summary['roi'] >= 0 else 'bad'}\">{summary['roi']:.2%}</div></div>
    </div>
    <div class='split'>
      <div class=\"card\">
        <div class=\"label\">Cumulative PnL</div>
        {chart_html}
      </div>
      <div class=\"card\">
        <div class=\"label\">Divergence Warnings (&gt; 0.05)</div>
        {warning_html}
      </div>
    </div>
    <div class="platform-grid">
      {platform_accuracy_cards}
    </div>
    <div class=\"card\" style=\"margin-bottom:20px\">
      <div class=\"label\">Platforms</div>
      <div>{', '.join(f'{k}: {v}' for k, v in summary['platform_counts'].items()) or 'None'}</div>
    </div>
    <div class=\"toolbar\">
      <button class='filter-btn active' data-platform='all'>All Platforms</button>
      {platform_buttons}
    </div>
    <div class=\"toolbar\">
      <button class='tab-btn active' data-target='all-panel'>All Games</button>
      <button class='tab-btn' data-target='settled-panel'>Settled Only</button>
    </div>
    <div id='all-panel' class='panel active'>
      <div class=\"table-wrap\">
        <table>
          <thead><tr><th>平台</th><th>比赛时间</th><th>主队</th><th>客队</th><th>赔率</th><th>主流博彩共识</th><th>平台-主流差</th><th>预测嬴方</th><th>实际嬴方</th><th>状态</th><th>价差预警</th><th>是否命中</th><th>10U收益</th><th>累计收益</th></tr></thead>
          <tbody id='all-body'>{all_rows_html}</tbody>
        </table>
      </div>
    </div>
    <div id='settled-panel' class='panel'>
      <div class=\"table-wrap\">
        <table>
          <thead><tr><th>平台</th><th>比赛时间</th><th>主队</th><th>客队</th><th>赔率</th><th>主流博彩共识</th><th>平台-主流差</th><th>预测嬴方</th><th>实际嬴方</th><th>状态</th><th>价差预警</th><th>是否命中</th><th>10U收益</th><th>累计收益</th></tr></thead>
          <tbody id='settled-body'>{settled_rows_html}</tbody>
        </table>
      </div>
    </div>
  </div>
  <script>
    const filterButtons = Array.from(document.querySelectorAll('.filter-btn'));
    const tabButtons = Array.from(document.querySelectorAll('.tab-btn'));
    function applyPlatformFilter(platform) {{
      document.querySelectorAll('tbody tr[data-platform]').forEach((row) => {{
        row.style.display = (platform === 'all' || row.dataset.platform === platform) ? '' : 'none';
      }});
      filterButtons.forEach((btn) => btn.classList.toggle('active', btn.dataset.platform === platform));
    }}
    filterButtons.forEach((btn) => btn.addEventListener('click', () => applyPlatformFilter(btn.dataset.platform)));
    tabButtons.forEach((btn) => btn.addEventListener('click', () => {{
      tabButtons.forEach((item) => item.classList.toggle('active', item === btn));
      document.querySelectorAll('.panel').forEach((panel) => panel.classList.toggle('active', panel.id === btn.dataset.target));
    }}));
  </script>
</body>
</html>"""
    output_path.write_text(html, encoding="utf-8")
    return output_path


def export_excel(output_path: Path) -> Path:
    settings = load_settings()
    setup_logging(settings.logging.level, settings.paths.logs_dir)
    schedule_map = _build_schedule_map()
    existing = _normalize_existing_rows(_load_existing_rows(output_path), schedule_map)
    polymarket_rows = fetch_polymarket_rows(schedule_map)
    kalshi_rows = fetch_kalshi_rows(schedule_map)
    sportsbook_map = fetch_sportsbook_consensus(schedule_map)
    merged = _merge_rows(existing, polymarket_rows + kalshi_rows)
    markets_df = _apply_results_and_pnl(merged, schedule_map, sportsbook_map)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        if markets_df.empty:
            final_df = pd.DataFrame(
                {
                    "平台": pd.Series(dtype="string"),
                    "比赛时间": pd.Series(dtype="string"),
                    "比赛时间(北京时间)": pd.Series(dtype="string"),
                    "记录日期(北京时间)": pd.Series(dtype="string"),
                    "发现时间UTC": pd.Series(dtype="string"),
                    "状态": pd.Series(dtype="string"),
                    "开赛快照时间UTC": pd.Series(dtype="string"),
                    "开赛主队概率": pd.Series(dtype="float"),
                    "开赛客队概率": pd.Series(dtype="float"),
                    "主流博彩主队概率": pd.Series(dtype="float"),
                    "主流博彩客队概率": pd.Series(dtype="float"),
                    "主流博彩样本数": pd.Series(dtype="float"),
                    "主流博彩来源": pd.Series(dtype="string"),
                    "主流博彩状态": pd.Series(dtype="string"),
                    "主队": pd.Series(dtype="string"),
                    "客队": pd.Series(dtype="string"),
                    "主队概率": pd.Series(dtype="float"),
                    "客队概率": pd.Series(dtype="float"),
                    "赔率": pd.Series(dtype="string"),
                    "预测嬴方": pd.Series(dtype="string"),
                    "实际嬴方（后续补充）": pd.Series(dtype="string"),
                    "跨市场主队价差": pd.Series(dtype="float"),
                    "跨市场客队价差": pd.Series(dtype="float"),
                    "价差预警": pd.Series(dtype="string"),
                    "相对主流博彩主队价差": pd.Series(dtype="float"),
                    "相对主流博彩客队价差": pd.Series(dtype="float"),
                    "是否命中": pd.Series(dtype="string"),
                    "下注方向": pd.Series(dtype="string"),
                    "10U收益": pd.Series(dtype="float"),
                    "累计收益": pd.Series(dtype="float"),
                }
            )
        else:
            final_df = markets_df[
                [
                    "平台",
                    "比赛时间",
                    "比赛时间(北京时间)",
                    "记录日期(北京时间)",
                    "发现时间UTC",
                    "状态",
                    "开赛快照时间UTC",
                    "开赛主队概率",
                    "开赛客队概率",
                    "主流博彩主队概率",
                    "主流博彩客队概率",
                    "主流博彩样本数",
                    "主流博彩来源",
                    "主流博彩状态",
                    "主队",
                    "客队",
                    "主队概率",
                    "客队概率",
                    "赔率",
                    "预测嬴方",
                    "实际嬴方（后续补充）",
                    "跨市场主队价差",
                    "跨市场客队价差",
                    "价差预警",
                    "相对主流博彩主队价差",
                    "相对主流博彩客队价差",
                    "是否命中",
                    "下注方向",
                    "10U收益",
                    "累计收益",
                ]
            ]
        final_df = pd.DataFrame(final_df)
        final_df.to_excel(writer, sheet_name="backtest", index=False)
        settled_mask = final_df["实际嬴方（后续补充）"].astype("string").fillna("") != ""
        settled_df = final_df[settled_mask]
        settled_df.to_excel(writer, sheet_name="settled_only", index=False)
    _write_html_report(final_df, settings.root_dir / "backtest.html")
    logger.info("Wrote Excel snapshot to %s", output_path)
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Export latest NBA full-game winner markets from Polymarket and Kalshi to Excel.")
    parser.add_argument("--output", default=None, help="Optional output .xlsx path")
    args = parser.parse_args()
    settings = load_settings()
    output_path = Path(args.output) if args.output else settings.root_dir / "backtest.xlsx"
    export_excel(output_path)


if __name__ == "__main__":
    main()
