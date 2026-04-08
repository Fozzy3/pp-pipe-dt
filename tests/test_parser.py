"""Tests for protobuf parser."""

from datetime import UTC, datetime

from google.transit import gtfs_realtime_pb2

from pipeline.collector.parser import (
    extract_trip_updates,
    extract_vehicle_positions,
    feed_timestamp,
    parse_feed,
)


def _make_vehicle_feed() -> bytes:
    """Build a minimal VehiclePositions feed for testing."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1712500000

    entity = feed.entity.add()
    entity.id = "v1"
    vp = entity.vehicle
    vp.vehicle.id = "bus-101"
    vp.trip.trip_id = "trip-1"
    vp.trip.route_id = "14"
    vp.trip.direction_id = 0
    vp.position.latitude = 37.7749
    vp.position.longitude = -122.4194
    vp.current_stop_sequence = 5
    vp.current_status = 1  # STOPPED_AT

    return feed.SerializeToString()


def _make_trip_update_feed() -> bytes:
    """Build a minimal TripUpdates feed for testing."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1712500000

    entity = feed.entity.add()
    entity.id = "tu1"
    tu = entity.trip_update
    tu.trip.trip_id = "trip-1"
    tu.trip.route_id = "14"
    tu.trip.direction_id = 0

    stu = tu.stop_time_update.add()
    stu.stop_sequence = 3
    stu.stop_id = "stop-100"
    stu.arrival.delay = 120

    return feed.SerializeToString()


def test_parse_vehicle_positions():
    raw = _make_vehicle_feed()
    feed = parse_feed(raw)
    ts = datetime(2025, 1, 1, tzinfo=UTC)

    records = extract_vehicle_positions(feed, ts)
    assert len(records) == 1

    r = records[0]
    assert r.vehicle_id == "bus-101"
    assert r.route_id == "14"
    assert abs(r.latitude - 37.7749) < 0.001
    assert r.current_status == "STOPPED_AT"


def test_parse_trip_updates():
    raw = _make_trip_update_feed()
    feed = parse_feed(raw)
    ts = datetime(2025, 1, 1, tzinfo=UTC)

    records = extract_trip_updates(feed, ts)
    assert len(records) == 1

    r = records[0]
    assert r.trip_id == "trip-1"
    assert r.stop_id == "stop-100"
    assert r.arrival_delay == 120


def test_feed_timestamp():
    raw = _make_vehicle_feed()
    feed = parse_feed(raw)
    ts = feed_timestamp(feed)
    assert ts.year >= 2024
    assert ts.tzinfo is not None
