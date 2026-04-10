"""Headway deviation analysis: scheduled vs actual bus headways."""

import logging

import pandas as pd

from pipeline.config.settings import Settings
from pipeline.storage.duckdb_store import DuckDBStore

logger = logging.getLogger(__name__)


def compute_headway_deviation(
    store: DuckDBStore,
    route_id: str,
    settings: Settings,
) -> pd.DataFrame:
    """Compute actual headway between consecutive vehicles at each stop.

    For each stop on the route, groups vehicle arrivals by direction and computes
    the time gap between consecutive vehicles. Without static GTFS scheduled times,
    we compute actual headway statistics and flag deviations from the median.

    Returns a DataFrame with columns:
        stop_id, direction_id, snapshot_ts, vehicle_id, actual_headway_s,
        median_headway_s, deviation_s, deviation_pct
    """
    # Get vehicle positions enriched with stop_id from matching trip updates.
    # vehicle_positions does not store stop_id directly.
    df = store.query_df(
        """
        SELECT
            vp.snapshot_ts,
            vp.vehicle_id,
            tu.stop_id,
            vp.direction_id,
            vp.stop_sequence,
            vp.current_status
        FROM vehicle_positions AS vp
        INNER JOIN trip_updates AS tu
            ON vp.snapshot_ts = tu.snapshot_ts
           AND vp.trip_id = tu.trip_id
           AND vp.stop_sequence = tu.stop_sequence
           AND vp.route_id = tu.route_id
        WHERE vp.route_id = ?
          AND tu.stop_id IS NOT NULL
          AND vp.direction_id IS NOT NULL
        ORDER BY stop_id, direction_id, snapshot_ts
        """,
        [route_id],
    )

    if df.empty:
        logger.warning("No vehicle position data for route %s", route_id)
        return pd.DataFrame()

    df["snapshot_ts"] = pd.to_datetime(df["snapshot_ts"])

    results = []

    for (stop_id, direction_id), group in df.groupby(["stop_id", "direction_id"]):
        # Deduplicate: keep first appearance of each vehicle at this stop
        arrivals = group.drop_duplicates(subset=["vehicle_id"], keep="first").sort_values(
            "snapshot_ts"
        )

        if len(arrivals) < 2:
            continue

        # Compute actual headway (gap between consecutive arrivals)
        arrivals = arrivals.copy()
        arrivals["actual_headway_s"] = arrivals["snapshot_ts"].diff().dt.total_seconds()
        arrivals = arrivals.dropna(subset=["actual_headway_s"])

        # Filter out unreasonable headways (< 30s or > 2 hours)
        arrivals = arrivals[
            (arrivals["actual_headway_s"] >= 30) & (arrivals["actual_headway_s"] <= 7200)
        ]

        if arrivals.empty:
            continue

        # Compute rolling median headway as "expected" baseline
        median_hw = arrivals["actual_headway_s"].median()
        arrivals["median_headway_s"] = median_hw
        arrivals["deviation_s"] = arrivals["actual_headway_s"] - median_hw
        arrivals["deviation_pct"] = (arrivals["deviation_s"] / median_hw * 100).round(1)
        arrivals["stop_id"] = stop_id
        arrivals["direction_id"] = direction_id

        results.append(
            arrivals[
                [
                    "stop_id",
                    "direction_id",
                    "snapshot_ts",
                    "vehicle_id",
                    "actual_headway_s",
                    "median_headway_s",
                    "deviation_s",
                    "deviation_pct",
                ]
            ]
        )

    if not results:
        logger.warning("No headway data computed for route %s", route_id)
        return pd.DataFrame()

    result_df = pd.concat(results, ignore_index=True)
    logger.info(
        "Route %s: %d headway observations across %d stops",
        route_id,
        len(result_df),
        result_df["stop_id"].nunique(),
    )
    return result_df
