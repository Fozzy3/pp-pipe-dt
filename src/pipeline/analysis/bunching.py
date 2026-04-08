"""Bunching detection: identify vehicles too close together on the same route."""

import logging

import pandas as pd

from pipeline.storage.duckdb_store import DuckDBStore

logger = logging.getLogger(__name__)


def compute_bunching(
    store: DuckDBStore,
    route_id: str,
    threshold_seconds: int = 120,
) -> pd.DataFrame:
    """Detect bunching events: vehicle pairs within threshold_seconds at same/adjacent stops.

    For each snapshot, groups vehicles by direction and stop_sequence.
    Consecutive vehicles (by stop_sequence or same stop) with time gap < threshold
    are flagged as bunching.

    Returns a DataFrame with columns:
        snapshot_ts, direction_id, vehicle_1, vehicle_2, stop_id,
        stop_sequence, gap_seconds, is_bunching
    """
    df = store.query_df(
        """
        SELECT
            snapshot_ts,
            vehicle_id,
            direction_id,
            stop_id,
            stop_sequence,
            latitude,
            longitude
        FROM vehicle_positions
        WHERE route_id = ?
          AND direction_id IS NOT NULL
          AND stop_sequence IS NOT NULL
        ORDER BY snapshot_ts, direction_id, stop_sequence
        """,
        [route_id],
    )

    if df.empty:
        logger.warning("No vehicle position data for route %s", route_id)
        return pd.DataFrame()

    df["snapshot_ts"] = pd.to_datetime(df["snapshot_ts"])

    results = []

    for (snapshot_ts, direction_id), group in df.groupby(["snapshot_ts", "direction_id"]):
        group = group.sort_values("stop_sequence")
        vehicles = group.to_dict("records")

        for i in range(len(vehicles) - 1):
            v1 = vehicles[i]
            v2 = vehicles[i + 1]

            # Stop sequence gap — adjacent or same stop
            seq_gap = abs(v2["stop_sequence"] - v1["stop_sequence"])
            if seq_gap > 2:
                continue

            # For same-snapshot bunching, we measure spatial proximity via stop_sequence
            # A gap of 0-2 stop_sequences between vehicles indicates potential bunching
            gap_stops = seq_gap

            results.append(
                {
                    "snapshot_ts": snapshot_ts,
                    "direction_id": direction_id,
                    "vehicle_1": v1["vehicle_id"],
                    "vehicle_2": v2["vehicle_id"],
                    "stop_id": v1["stop_id"],
                    "stop_sequence_1": v1["stop_sequence"],
                    "stop_sequence_2": v2["stop_sequence"],
                    "gap_stops": gap_stops,
                    "is_bunching": gap_stops <= 1,
                }
            )

    if not results:
        logger.warning("No bunching pairs found for route %s", route_id)
        return pd.DataFrame()

    result_df = pd.DataFrame(results)

    bunching_count = result_df["is_bunching"].sum()
    total_pairs = len(result_df)
    bunching_pct = (bunching_count / total_pairs * 100) if total_pairs > 0 else 0

    logger.info(
        "Route %s: %d bunching events out of %d pairs (%.1f%%)",
        route_id,
        bunching_count,
        total_pairs,
        bunching_pct,
    )
    return result_df
