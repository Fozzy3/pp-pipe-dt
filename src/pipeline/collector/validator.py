"""Validate GTFS-RT feed data quality."""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime

from google.transit import gtfs_realtime_pb2

logger = logging.getLogger(__name__)

# SF Bay Area bounding box
SF_BBOX = {
    "min_lat": 37.3,
    "max_lat": 38.0,
    "min_lon": -122.6,
    "max_lon": -122.0,
}

MAX_FEED_AGE_SECONDS = 300  # 5 minutes — 511.org feeds can lag


@dataclass
class ValidationReport:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    entity_count: int = 0

    @property
    def is_usable(self) -> bool:
        """Feed is usable if error rate is below 5%."""
        if self.entity_count == 0:
            return False
        return len(self.errors) / self.entity_count < 0.05


def validate_feed(feed: gtfs_realtime_pb2.FeedMessage) -> ValidationReport:
    """Run validation checks on a parsed FeedMessage."""
    report = ValidationReport()

    # Check feed timestamp freshness
    now = datetime.now(tz=UTC).timestamp()
    feed_age = now - feed.header.timestamp
    if feed_age > MAX_FEED_AGE_SECONDS:
        report.warnings.append(f"Feed is {feed_age:.0f}s old (max {MAX_FEED_AGE_SECONDS}s)")

    if feed_age < 0:
        report.errors.append(f"Feed timestamp is in the future by {-feed_age:.0f}s")

    report.entity_count = len(feed.entity)
    if report.entity_count == 0:
        report.errors.append("Feed contains zero entities")
        return report

    for entity in feed.entity:
        if entity.HasField("vehicle"):
            _validate_vehicle(entity.vehicle, report)
        if entity.HasField("trip_update"):
            _validate_trip_update(entity.trip_update, report)

    if report.errors:
        logger.warning(
            "Validation: %d errors, %d warnings", len(report.errors), len(report.warnings)
        )
    return report


def _validate_vehicle(vp, report: ValidationReport) -> None:
    if not (vp.vehicle.id or vp.vehicle.label):
        report.errors.append("Vehicle missing ID and label")
        return

    pos = vp.position
    if not (SF_BBOX["min_lat"] <= pos.latitude <= SF_BBOX["max_lat"]):
        report.warnings.append(f"Vehicle {vp.vehicle.id} lat {pos.latitude} outside SF bbox")
    if not (SF_BBOX["min_lon"] <= pos.longitude <= SF_BBOX["max_lon"]):
        report.warnings.append(f"Vehicle {vp.vehicle.id} lon {pos.longitude} outside SF bbox")


def _validate_trip_update(tu, report: ValidationReport) -> None:
    if not tu.trip.trip_id:
        report.errors.append("TripUpdate missing trip_id")
