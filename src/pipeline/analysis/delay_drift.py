"""Delay drift analysis: cumulative delay growth along a route."""

import logging

import pandas as pd

from pipeline.storage.duckdb_store import DuckDBStore

logger = logging.getLogger(__name__)

# Time-of-day periods for aggregation
TIME_PERIODS = {
    "AM_PEAK": (6, 9),
    "MIDDAY": (9, 15),
    "PM_PEAK": (15, 19),
    "EVENING": (19, 23),
    "NIGHT": (23, 6),
}


def _classify_period(hour: int) -> str:
    for name, (start, end) in TIME_PERIODS.items():
        if start <= end:
            if start <= hour < end:
                return name
        else:  # wraps midnight
            if hour >= start or hour < end:
                return name
    return "NIGHT"


def compute_delay_drift(
    store: DuckDBStore,
    route_id: str,
) -> pd.DataFrame:
    """Compute how delay grows from first to last stop of each trip.

    For each trip on the route, tracks arrival_delay at every stop_sequence.
    Drift = delay_at_stop_n - delay_at_first_stop.

    Returns a DataFrame with columns:
        trip_id, direction_id, stop_sequence, stop_id, arrival_delay,
        drift_from_origin, time_period, snapshot_ts
    """
    # Get the most recent snapshot per trip (latest predictions)
    df = store.query_df(
        """
        WITH ranked AS (
            SELECT
                trip_id,
                direction_id,
                stop_sequence,
                stop_id,
                arrival_delay,
                snapshot_ts,
                ROW_NUMBER() OVER (
                    PARTITION BY trip_id, stop_sequence
                    ORDER BY snapshot_ts DESC
                ) as rn
            FROM trip_updates
            WHERE route_id = ?
              AND arrival_delay IS NOT NULL
        )
        SELECT trip_id, direction_id, stop_sequence, stop_id,
               arrival_delay, snapshot_ts
        FROM ranked
        WHERE rn = 1
        ORDER BY trip_id, stop_sequence
        """,
        [route_id],
    )

    if df.empty:
        logger.warning("No trip update data for route %s", route_id)
        return pd.DataFrame()

    df["snapshot_ts"] = pd.to_datetime(df["snapshot_ts"])

    results = []

    for trip_id, group in df.groupby("trip_id"):
        group = group.sort_values("stop_sequence")

        if len(group) < 2:
            continue

        origin_delay = group["arrival_delay"].iloc[0]
        group = group.copy()
        group["drift_from_origin"] = group["arrival_delay"] - origin_delay

        # Classify time period from first stop timestamp
        hour = group["snapshot_ts"].iloc[0].hour
        group["time_period"] = _classify_period(hour)

        results.append(group)

    if not results:
        logger.warning("No delay drift data for route %s", route_id)
        return pd.DataFrame()

    result_df = pd.concat(results, ignore_index=True)
    logger.info(
        "Route %s: delay drift for %d trips, mean final drift %.1fs",
        route_id,
        result_df["trip_id"].nunique(),
        result_df.groupby("trip_id")["drift_from_origin"].last().mean(),
    )
    return result_df
