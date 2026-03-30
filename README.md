# NBA Market Study

`nba-market-study` is a research-first Python project for recording, analyzing, and backtesting NBA single-game winner markets from prediction exchanges. Phase 1 completes the Polymarket data path end to end: discover NBA markets, capture pre-game price snapshots, resolve final game outcomes, evaluate predictive quality, and backtest simple staking rules.

The current scope is intentionally narrow:

- Phase 1 focuses on research, recording, and backtesting only.
- Real-money automated execution is explicitly out of scope.
- Kalshi is scaffolded for later integration, but not fully connected yet.
- Traditional sportsbooks are not included.

## Features

- Discover Polymarket NBA single-game winner markets using public read-only endpoints.
- Track price snapshots at `T-24h`, `T-1h`, `T-30m`, and `T-5m`.
- Store games, snapshots, results, and backtest outputs in SQLite.
- Resolve true results through ESPN's public NBA scoreboard endpoint.
- Compute accuracy, calibration, Brier score, and log loss.
- Build a historical calibration mapping and use it as `p_hat`.
- Backtest two strategies:
  - fixed 20 USDC stake when `edge >= min_edge`
  - half-Kelly with bankroll, min bet, and max bet controls

## Project Layout

```text
nba-market-study/
  config/settings.yaml
  data/raw/
  data/processed/
  data/reports/
  db/
  logs/
  src/
  tests/
```

## Requirements

- Python 3.11+
- Public internet access for Polymarket and ESPN endpoints

## Installation

```bash
python -m venv .venv
source .venv/bin/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Environment Variables

Copy `.env.example` to `.env` if you want to override defaults.

```bash
cp .env.example .env
```

Supported values include:

- `API_BASKETBALL_BASE_URL`
- `API_BASKETBALL_KEY`
- `POLYMARKET_API_BASE_URL`
- `POLYMARKET_CLOB_BASE_URL`
- `KALSHI_API_BASE_URL`
- `ESPN_SCOREBOARD_BASE_URL`
- `DATABASE_PATH`
- `LOG_LEVEL`
- `TIMEZONE`

## SQLite Database

The database lives at `db/nba_market_study.sqlite` by default.

Tables:

- `games`: normalized NBA game metadata and market/token mapping
- `price_snapshots`: pre-game market snapshots by label
- `results`: final outcomes and scores
- `backtest_runs`: per-run summary metrics
- `backtest_bets`: bet-level details

The schema is initialized automatically whenever a pipeline runs.

## Data Flow

1. Discover candidate Polymarket NBA markets.
2. Parse matchup text and try to align teams with ESPN schedule data.
3. Store normalized games in `games`.
4. When a game is near `T-24h`, `T-1h`, `T-30m`, or `T-5m`, fetch order books and store one snapshot per label.
5. After completion, resolve game outcomes from ESPN and write `results`.
6. Join snapshots with results for analysis and backtesting.

## Discover Games and Collect Snapshots

Run the snapshot collector:

```bash
python -m src.pipelines.collect_polymarket_snapshots
```

Or use the unified CLI:

```bash
python -m src.main collect
```

Notes:

- The collector avoids duplicate `games` rows with a `(platform, external_game_id)` uniqueness constraint.
- The collector avoids duplicate snapshots with a `(game_id, snapshot_label)` uniqueness constraint.
- If the process starts late, it can still capture the most recent applicable snapshot label and records the true snapshot timestamp.

## Resolve Results

```bash
python -m src.pipelines.resolve_game_results
```

Or:

```bash
python -m src.main resolve
```

This step uses ESPN scoreboard data to determine home/away teams and final score. If the score cannot be matched yet, the pipeline skips the game without failing the whole run.

## Run Analysis

Default research view uses `T-30m` and `mid_price`.

```bash
python -m src.pipelines.run_analysis --platform polymarket --snapshot-label T-30m --price-field mid_price
```

Or:

```bash
python -m src.main analyze --platform polymarket --snapshot-label T-30m --price-field mid_price
```

Outputs:

- reliability table CSV in `data/processed/`
- side-level analysis dataset in `data/processed/`
- calibration chart PNG in `data/reports/`
- summary JSON in `data/reports/`

Metrics included:

- Accuracy
- Brier Score
- Log Loss
- Reliability table
- Calibration curve

## Run Backtests

Fixed stake:

```bash
python -m src.pipelines.run_backtest --platform polymarket --snapshot-label T-30m --strategy fixed
```

Half-Kelly:

```bash
python -m src.pipelines.run_backtest --platform polymarket --snapshot-label T-30m --strategy half-kelly
```

Or use the unified CLI:

```bash
python -m src.main backtest --strategy fixed
python -m src.main backtest --strategy half-kelly
```

Backtest outputs:

- bet-level CSV in `data/processed/`
- daily and weekly PnL CSVs in `data/processed/`
- equity curve CSV in `data/processed/`
- equity curve PNG in `data/reports/`
- summary JSON in `data/reports/`
- run and bet records in SQLite

## Refresh Workbook And HTML

To refresh the consolidated workbook, fill any newly resolved winners, recompute 10U fixed-stake PnL, and rebuild the HTML dashboard:

```bash
python -m src.main refresh
```

Artifacts:

- `backtest.xlsx` with sheets `backtest` and `settled_only`
- `backtest.html` with platform filters, a settled-only tab, and a cumulative PnL chart

## Daily Lifecycle Tracker

To run the all-day lifecycle tracker starting at Beijing `00:00`, schedule this command once per day in Windows Task Scheduler:

```bash
python -m src.main track-day --poll-seconds 300
```

What it does during the day:

- records the day's NBA games and start times
- refreshes Polymarket and Kalshi markets on a polling loop
- freezes game-time probabilities once a game reaches tipoff
- writes final winners after ESPN marks the game complete
- recomputes 10U fixed-stake PnL
- flags cross-market gaps when `|Polymarket - Kalshi| > 0.05`
- fetches API-Basketball mainstream bookmaker odds consensus when `API_BASKETBALL_KEY` is configured
- compares Polymarket and Kalshi probabilities against sportsbook consensus
- rebuilds `backtest.xlsx` and `backtest.html`

Convenience launcher:

```bash
run_daily_tracker.bat
```

## GitHub Automation

The repository includes `.github/workflows/refresh-dashboard.yml`.

- runs every 15 minutes via GitHub Actions
- refreshes `backtest.xlsx` and `backtest.html`
- commits updated artifacts back to the repository
- deploys `backtest.html` and `backtest.xlsx` to GitHub Pages
- reads sportsbook consensus from the `API_BASKETBALL_KEY` GitHub Actions secret when configured

This schedule is more practical on GitHub than a single long-running process from Beijing midnight to the next midnight, while still capturing pregame, in-play boundary, and postgame updates throughout the day.

## Calibration Design

Phase 1 does not train a complex model. Instead:

1. Convert each snapshot into side-level probabilities for home and away.
2. Bin implied probabilities into configurable buckets.
3. Estimate empirical win rate per bucket.
4. Smooth low-sample bins toward the global win rate.
5. Use the calibrated value as `p_hat`.
6. Define `edge = p_hat - q`.

The default bet trigger is `edge >= 0.03`, configurable in `config/settings.yaml`.

## Kalshi Extension Path

`src/clients/kalshi.py` is a placeholder client with a configurable base URL and method stubs. The project is structured so a later phase can add Kalshi market discovery and snapshot capture without changing the analytics or backtest layers.

## Limitations and Notes

- Phase 1 does not place live orders or connect to wallet/authenticated trading APIs.
- Polymarket public response fields can change. The client and collector are written to degrade gracefully, but some field names may need adjustment.
- ESPN matching is a practical public-data bridge for home/away orientation and results, but team-name parsing can require further refinement.
- API-Basketball sportsbook comparison depends on your plan and key permissions. If the API denies a season or game, the dashboard will show sportsbook data as unavailable rather than fail the whole refresh.
- Liquidity and volume fields are recorded only if exposed by the public response.
- No traditional sportsbook data is used.

## Testing

Run tests with:

```bash
pytest
```

## How To Start

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Initialize the database implicitly by running any pipeline, for example:

```bash
python -m src.main collect
```

3. Collect data:

```bash
python -m src.pipelines.collect_polymarket_snapshots
```

4. Resolve results:

```bash
python -m src.pipelines.resolve_game_results
```

5. Run analysis:

```bash
python -m src.pipelines.run_analysis --platform polymarket --snapshot-label T-30m
```

6. Run backtests:

```bash
python -m src.pipelines.run_backtest --platform polymarket --snapshot-label T-30m --strategy fixed
python -m src.pipelines.run_backtest --platform polymarket --snapshot-label T-30m --strategy half-kelly
```
