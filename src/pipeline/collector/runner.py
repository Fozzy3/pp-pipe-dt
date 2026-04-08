"""Orchestrate a single GTFS-RT collection cycle."""

import logging
import time
from datetime import UTC, datetime

from pipeline.collector.fetcher import fetch_trip_updates, fetch_vehicle_positions
from pipeline.collector.models import CollectionResult
from pipeline.collector.parser import (
    extract_trip_updates,
    extract_vehicle_positions,
    parse_feed,
)
from pipeline.collector.validator import validate_feed
from pipeline.config import get_settings
from pipeline.storage.duckdb_store import DuckDBStore
from pipeline.storage.snapshots import save_snapshot

logger = logging.getLogger(__name__)


def run_collection_cycle() -> CollectionResult:
    """Execute one full collection cycle: fetch → validate → parse → store."""
    settings = get_settings()
    raw_dir = settings.resolve_path(settings.raw_dir)
    db_path = settings.resolve_path(settings.db_path)

    snapshot_ts = datetime.now(tz=UTC)
    t0 = time.monotonic()

    # Fetch both feeds
    logger.info("Fetching feeds for agency=%s", settings.feed_agency)
    tu_raw = fetch_trip_updates(settings.api_key, settings.feed_agency)
    vp_raw = fetch_vehicle_positions(settings.api_key, settings.feed_agency)

    fetch_ms = int((time.monotonic() - t0) * 1000)

    # Save raw snapshots
    save_snapshot(raw_dir, snapshot_ts, "trip_updates", tu_raw)
    save_snapshot(raw_dir, snapshot_ts, "vehicle_positions", vp_raw)

    # Parse
    tu_feed = parse_feed(tu_raw)
    vp_feed = parse_feed(vp_raw)

    # Validate
    tu_report = validate_feed(tu_feed)
    vp_report = validate_feed(vp_feed)

    total_errors = len(tu_report.errors) + len(vp_report.errors)
    total_warnings = len(tu_report.warnings) + len(vp_report.warnings)

    if not tu_report.is_usable:
        logger.warning("TripUpdates feed not usable: %s", tu_report.errors)
    if not vp_report.is_usable:
        logger.warning("VehiclePositions feed not usable: %s", vp_report.errors)

    # Extract records
    trip_updates = extract_trip_updates(tu_feed, snapshot_ts)
    vehicle_positions = extract_vehicle_positions(vp_feed, snapshot_ts)

    result = CollectionResult(
        snapshot_ts=snapshot_ts,
        trip_updates=trip_updates,
        vehicle_positions=vehicle_positions,
        fetch_duration_ms=fetch_ms,
        validation_errors=total_errors,
        validation_warnings=total_warnings,
    )

    # Store in DuckDB
    store = DuckDBStore(db_path)
    try:
        store.insert_result(result)
    finally:
        store.close()

    logger.info(
        "Collected: %d trip updates, %d vehicle positions (fetch=%dms, errors=%d)",
        len(trip_updates),
        len(vehicle_positions),
        fetch_ms,
        total_errors,
    )

    return result
