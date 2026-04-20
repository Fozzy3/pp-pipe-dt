# pp-pipe-dt — GTFS-RT Transit Data Pipeline

## What is this?
Python pipeline that collects real-time transit data (GTFS-RT) from SF Muni via 511.org API,
stores it in DuckDB, and computes three metrics (headway deviation, delay drift, bunching index)
for a research paper submission to PeerJ Computer Science.

## Tech Stack
- Python 3.11+, managed with `uv`
- DuckDB for analytics storage
- httpx for HTTP, pydantic-settings for config
- gtfs-realtime-bindings + protobuf for GTFS-RT parsing
- matplotlib + seaborn for publication figures
- systemd timer for continuous collection (130s interval)

## Project Structure
```
src/pipeline/
  collector/   — fetcher.py, parser.py, validator.py, runner.py, models.py
  storage/     — duckdb_store.py, snapshots.py
  analysis/    — headway.py, delay_drift.py, bunching.py
  visualization/ — plots.py
  config/      — settings.py (pydantic-settings, loads .env)
scripts/       — collect.py, ingest_longitudinal.py, analyze_longitudinal.py (CLI entry points)
systemd/       — service + timer files
```

## Commands
```bash
# Install
uv sync --all-extras

# Collect one snapshot
uv run python scripts/collect.py

# Run longitudinal paper analysis
uv run python scripts/analyze_longitudinal.py

# Tests
uv run pytest

# Lint
uv run ruff check src/ tests/
```

## Key Constraints
- 511.org rate limit: 60 req/hr total. Each cycle = 2 requests → poll every 130s max
- Target routes: SF Muni 14, 38, 49
- Config via `.env` file (see `env.example`)

## Conventions
- Dataclasses (frozen, slots) for data records, not dicts
- httpx (not requests) for HTTP
- All paths resolved via `settings.resolve_path()` relative to project root
- Logging: stdlib logging, configured in CLI scripts
- Tests: pytest, fixtures in conftest.py
