"""Tests for DuckDB storage."""

from datetime import UTC, datetime

from pipeline.collector.models import CollectionResult, TripUpdateRecord, VehiclePositionRecord


def test_insert_and_query(tmp_db):
    ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

    result = CollectionResult(
        snapshot_ts=ts,
        trip_updates=[
            TripUpdateRecord(
                snapshot_ts=ts,
                trip_id="trip-1",
                route_id="14",
                direction_id=0,
                stop_sequence=1,
                stop_id="stop-100",
                arrival_delay=60,
                departure_delay=65,
            )
        ],
        vehicle_positions=[
            VehiclePositionRecord(
                snapshot_ts=ts,
                vehicle_id="bus-101",
                trip_id="trip-1",
                route_id="14",
                direction_id=0,
                latitude=37.77,
                longitude=-122.42,
                bearing=None,
                speed=None,
                stop_sequence=5,
                current_status="STOPPED_AT",
            )
        ],
        fetch_duration_ms=500,
        validation_errors=0,
        validation_warnings=1,
    )

    tmp_db.insert_result(result)

    # Verify trip updates
    rows = tmp_db.query("SELECT trip_id, arrival_delay FROM trip_updates")
    assert len(rows) == 1
    assert rows[0] == ("trip-1", 60)

    # Verify vehicle positions
    rows = tmp_db.query("SELECT vehicle_id, latitude FROM vehicle_positions")
    assert len(rows) == 1
    assert rows[0][0] == "bus-101"

    # Verify collection log
    rows = tmp_db.query("SELECT trip_update_count FROM collection_log")
    assert rows[0][0] == 1


def test_idempotent_insert(tmp_db):
    """Inserting the same record twice should not fail (INSERT OR IGNORE)."""
    ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

    result = CollectionResult(
        snapshot_ts=ts,
        trip_updates=[
            TripUpdateRecord(
                snapshot_ts=ts,
                trip_id="trip-1",
                route_id="14",
                direction_id=0,
                stop_sequence=1,
                stop_id="stop-100",
                arrival_delay=60,
                departure_delay=None,
            )
        ],
        vehicle_positions=[],
        fetch_duration_ms=100,
        validation_errors=0,
        validation_warnings=0,
    )

    tmp_db.insert_result(result)
    tmp_db.insert_result(result)  # Should not raise

    rows = tmp_db.query("SELECT COUNT(*) FROM trip_updates")
    assert rows[0][0] == 1
