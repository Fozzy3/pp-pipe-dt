"""Data models for parsed GTFS-RT entities."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class TripUpdateRecord:
    snapshot_ts: datetime
    trip_id: str
    route_id: str
    direction_id: int | None
    stop_sequence: int
    stop_id: str
    arrival_delay: int | None
    departure_delay: int | None


@dataclass(frozen=True, slots=True)
class VehiclePositionRecord:
    snapshot_ts: datetime
    vehicle_id: str
    trip_id: str | None
    route_id: str | None
    direction_id: int | None
    latitude: float
    longitude: float
    bearing: float | None
    speed: float | None
    stop_sequence: int | None
    current_status: str | None


@dataclass(frozen=True, slots=True)
class CollectionResult:
    snapshot_ts: datetime
    trip_updates: list[TripUpdateRecord]
    vehicle_positions: list[VehiclePositionRecord]
    fetch_duration_ms: int
    validation_errors: int
    validation_warnings: int
