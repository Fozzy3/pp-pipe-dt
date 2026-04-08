"""DuckDB storage for parsed GTFS-RT data."""

import logging
from pathlib import Path

import duckdb

from pipeline.collector.models import (
    CollectionResult,
    TripUpdateRecord,
    VehiclePositionRecord,
)

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS trip_updates (
    snapshot_ts  TIMESTAMP NOT NULL,
    trip_id      VARCHAR NOT NULL,
    route_id     VARCHAR,
    direction_id INTEGER,
    stop_sequence INTEGER,
    stop_id      VARCHAR,
    arrival_delay INTEGER,
    departure_delay INTEGER,
    PRIMARY KEY (snapshot_ts, trip_id, stop_sequence)
);

CREATE TABLE IF NOT EXISTS vehicle_positions (
    snapshot_ts    TIMESTAMP NOT NULL,
    vehicle_id     VARCHAR NOT NULL,
    trip_id        VARCHAR,
    route_id       VARCHAR,
    direction_id   INTEGER,
    latitude       DOUBLE,
    longitude      DOUBLE,
    bearing        FLOAT,
    speed          FLOAT,
    stop_sequence  INTEGER,
    current_status VARCHAR,
    PRIMARY KEY (snapshot_ts, vehicle_id)
);

CREATE TABLE IF NOT EXISTS collection_log (
    snapshot_ts       TIMESTAMP PRIMARY KEY,
    trip_update_count INTEGER,
    vehicle_pos_count INTEGER,
    fetch_duration_ms INTEGER,
    validation_errors INTEGER,
    validation_warnings INTEGER
);

CREATE INDEX IF NOT EXISTS idx_tu_route_ts ON trip_updates(route_id, snapshot_ts);
CREATE INDEX IF NOT EXISTS idx_tu_stop_ts ON trip_updates(stop_id, snapshot_ts);
CREATE INDEX IF NOT EXISTS idx_vp_route_ts ON vehicle_positions(route_id, snapshot_ts);
"""


class DuckDBStore:
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(db_path))
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.execute(SCHEMA_SQL)
        logger.info("DuckDB schema initialized")

    def insert_result(self, result: CollectionResult) -> None:
        """Insert a full collection result (trip updates + vehicle positions + log)."""
        self._insert_trip_updates(result.trip_updates)
        self._insert_vehicle_positions(result.vehicle_positions)
        self._insert_log(result)

    def _insert_trip_updates(self, records: list[TripUpdateRecord]) -> None:
        if not records:
            return
        rows = [
            (
                r.snapshot_ts,
                r.trip_id,
                r.route_id,
                r.direction_id,
                r.stop_sequence,
                r.stop_id,
                r.arrival_delay,
                r.departure_delay,
            )
            for r in records
        ]
        self.conn.executemany(
            """INSERT OR IGNORE INTO trip_updates
            (snapshot_ts, trip_id, route_id, direction_id, stop_sequence,
             stop_id, arrival_delay, departure_delay)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        logger.debug("Inserted %d trip updates", len(rows))

    def _insert_vehicle_positions(self, records: list[VehiclePositionRecord]) -> None:
        if not records:
            return
        rows = [
            (
                r.snapshot_ts,
                r.vehicle_id,
                r.trip_id,
                r.route_id,
                r.direction_id,
                r.latitude,
                r.longitude,
                r.bearing,
                r.speed,
                r.stop_sequence,
                r.current_status,
            )
            for r in records
        ]
        self.conn.executemany(
            """INSERT OR IGNORE INTO vehicle_positions
            (snapshot_ts, vehicle_id, trip_id, route_id, direction_id,
             latitude, longitude, bearing, speed, stop_sequence, current_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        logger.debug("Inserted %d vehicle positions", len(rows))

    def _insert_log(self, result: CollectionResult) -> None:
        self.conn.execute(
            """INSERT OR IGNORE INTO collection_log
            (snapshot_ts, trip_update_count, vehicle_pos_count,
             fetch_duration_ms, validation_errors, validation_warnings)
            VALUES (?, ?, ?, ?, ?, ?)""",
            [
                result.snapshot_ts,
                len(result.trip_updates),
                len(result.vehicle_positions),
                result.fetch_duration_ms,
                result.validation_errors,
                result.validation_warnings,
            ],
        )

    def query(self, sql: str, params: list | None = None) -> list[tuple]:
        """Run a read query and return rows."""
        if params:
            return self.conn.execute(sql, params).fetchall()
        return self.conn.execute(sql).fetchall()

    def query_df(self, sql: str, params: list | None = None):
        """Run a read query and return a pandas DataFrame."""
        if params:
            return self.conn.execute(sql, params).fetchdf()
        return self.conn.execute(sql).fetchdf()

    def close(self) -> None:
        self.conn.close()
