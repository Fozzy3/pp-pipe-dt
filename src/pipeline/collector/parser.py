"""Parse GTFS-RT protobuf feeds into typed records."""

import logging
from datetime import UTC, datetime

from google.transit import gtfs_realtime_pb2

from pipeline.collector.models import TripUpdateRecord, VehiclePositionRecord

logger = logging.getLogger(__name__)

# VehicleStopStatus enum mapping
_STOP_STATUS = {
    0: "INCOMING_AT",
    1: "STOPPED_AT",
    2: "IN_TRANSIT_TO",
}


def parse_feed(raw: bytes) -> gtfs_realtime_pb2.FeedMessage:
    """Deserialize raw protobuf bytes into a FeedMessage."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw)
    return feed


def extract_trip_updates(
    feed: gtfs_realtime_pb2.FeedMessage,
    snapshot_ts: datetime,
) -> list[TripUpdateRecord]:
    """Extract TripUpdateRecords from a FeedMessage."""
    records: list[TripUpdateRecord] = []

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        tu = entity.trip_update
        trip_id = tu.trip.trip_id
        route_id = tu.trip.route_id
        direction_id = tu.trip.direction_id if tu.trip.HasField("direction_id") else None

        for stu in tu.stop_time_update:
            arrival_delay = stu.arrival.delay if stu.HasField("arrival") else None
            departure_delay = stu.departure.delay if stu.HasField("departure") else None

            records.append(
                TripUpdateRecord(
                    snapshot_ts=snapshot_ts,
                    trip_id=trip_id,
                    route_id=route_id,
                    direction_id=direction_id,
                    stop_sequence=stu.stop_sequence,
                    stop_id=stu.stop_id,
                    arrival_delay=arrival_delay,
                    departure_delay=departure_delay,
                )
            )

    logger.debug("Parsed %d trip update records", len(records))
    return records


def extract_vehicle_positions(
    feed: gtfs_realtime_pb2.FeedMessage,
    snapshot_ts: datetime,
) -> list[VehiclePositionRecord]:
    """Extract VehiclePositionRecords from a FeedMessage."""
    records: list[VehiclePositionRecord] = []

    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue

        vp = entity.vehicle
        vehicle_id = vp.vehicle.id or vp.vehicle.label or ""
        if not vehicle_id:
            continue

        pos = vp.position
        records.append(
            VehiclePositionRecord(
                snapshot_ts=snapshot_ts,
                vehicle_id=vehicle_id,
                trip_id=vp.trip.trip_id or None,
                route_id=vp.trip.route_id or None,
                direction_id=vp.trip.direction_id if vp.trip.HasField("direction_id") else None,
                latitude=pos.latitude,
                longitude=pos.longitude,
                bearing=pos.bearing if pos.bearing else None,
                speed=pos.speed if pos.speed else None,
                stop_sequence=vp.current_stop_sequence if vp.current_stop_sequence else None,
                current_status=_STOP_STATUS.get(vp.current_status),
            )
        )

    logger.debug("Parsed %d vehicle position records", len(records))
    return records


def feed_timestamp(feed: gtfs_realtime_pb2.FeedMessage) -> datetime:
    """Extract the feed header timestamp as a UTC datetime."""
    return datetime.fromtimestamp(feed.header.timestamp, tz=UTC)
