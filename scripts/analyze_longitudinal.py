#!/usr/bin/env python3
"""Run longitudinal monthly transit analysis over Hive-partitioned data."""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from pipeline.config import get_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze longitudinal GTFS observations grouped by year/month"
    )
    parser.add_argument("--route", help="Single route to analyze (default: all target routes)")
    parser.add_argument(
        "--db-path",
        default="data/processed/transit_longitudinal.db",
        help="DuckDB path containing longitudinal view",
    )
    parser.add_argument(
        "--view-name",
        default="longitudinal_observations",
        help="View/table with Hive partition columns year and month",
    )
    parser.add_argument("--output", default=None, help="Output directory for longitudinal figures")
    parser.add_argument(
        "--bunching-threshold",
        type=int,
        default=120,
        help="Seconds threshold for bunching event (default: 120)",
    )
    return parser.parse_args()


def safe_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe SQL identifier: {value}")
    return value


def ensure_relation_exists(conn: duckdb.DuckDBPyConnection, relation: str) -> None:
    exists = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name = ?
        """,
        [relation],
    ).fetchone()[0]

    if exists == 0:
        raise RuntimeError(
            f"Relation '{relation}' not found. Run scripts/ingest_longitudinal.py first."
        )


def get_relation_columns(conn: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    rows = conn.execute(f"DESCRIBE {relation}").fetchall()
    return {str(row[0]) for row in rows}


def query_monthly_bunching(
    conn: duckdb.DuckDBPyConnection,
    *,
    relation: str,
    route_id: str,
    bunching_threshold: int,
    direction_expr: str,
    stop_key_expr: str,
) -> pd.DataFrame:
    sql = f"""
    WITH base AS (
        SELECT
            CAST(year AS INTEGER) AS year,
            CAST(month AS INTEGER) AS month,
            route_id,
            {direction_expr} AS direction_id,
            CAST(service_date AS VARCHAR) AS service_date,
            {stop_key_expr} AS stop_key,
            vehicle_id,
            CASE
                WHEN COALESCE(observed_departure_time, observed_arrival_time) IS NULL THEN NULL
                ELSE
                    COALESCE(TRY_CAST(split_part(COALESCE(observed_departure_time, observed_arrival_time), ':', 1) AS INTEGER), 0) * 3600
                    + COALESCE(TRY_CAST(split_part(COALESCE(observed_departure_time, observed_arrival_time), ':', 2) AS INTEGER), 0) * 60
                    + COALESCE(TRY_CAST(split_part(COALESCE(observed_departure_time, observed_arrival_time), ':', 3) AS INTEGER), 0)
            END AS event_seconds
        FROM {relation}
        WHERE route_id = ?
          AND year IS NOT NULL
          AND month IS NOT NULL
    ),
    ordered AS (
        SELECT
            *,
            LAG(event_seconds) OVER (
                PARTITION BY year, month, route_id, service_date, direction_id, stop_key
                ORDER BY event_seconds, vehicle_id
            ) AS prev_seconds
        FROM base
    ),
    gaps AS (
        SELECT
            year,
            month,
            route_id,
            event_seconds - prev_seconds AS gap_seconds
        FROM ordered
        WHERE event_seconds IS NOT NULL
          AND prev_seconds IS NOT NULL
          AND (event_seconds - prev_seconds) > 0
          AND (event_seconds - prev_seconds) <= 7200
    )
    SELECT
        year,
        month,
        route_id,
        COUNT(*) AS total_pairs,
        SUM(CASE WHEN gap_seconds <= ? THEN 1 ELSE 0 END) AS bunching_events,
        AVG(CASE WHEN gap_seconds <= ? THEN 1.0 ELSE 0.0 END) AS bunching_index,
        AVG(gap_seconds) AS mean_gap_seconds,
        MEDIAN(gap_seconds) AS median_gap_seconds
    FROM gaps
    GROUP BY year, month, route_id
    ORDER BY year, month
    """
    return conn.execute(sql, [route_id, bunching_threshold, bunching_threshold]).fetchdf()


def query_monthly_headway_drift(
    conn: duckdb.DuckDBPyConnection,
    *,
    relation: str,
    route_id: str,
    direction_expr: str,
    stop_key_expr: str,
) -> pd.DataFrame:
    sql = f"""
    WITH base AS (
        SELECT
            CAST(year AS INTEGER) AS year,
            CAST(month AS INTEGER) AS month,
            route_id,
            {direction_expr} AS direction_id,
            CAST(service_date AS VARCHAR) AS service_date,
            {stop_key_expr} AS stop_key,
            vehicle_id,
            CASE
                WHEN COALESCE(observed_departure_time, observed_arrival_time) IS NULL THEN NULL
                ELSE
                    COALESCE(TRY_CAST(split_part(COALESCE(observed_departure_time, observed_arrival_time), ':', 1) AS INTEGER), 0) * 3600
                    + COALESCE(TRY_CAST(split_part(COALESCE(observed_departure_time, observed_arrival_time), ':', 2) AS INTEGER), 0) * 60
                    + COALESCE(TRY_CAST(split_part(COALESCE(observed_departure_time, observed_arrival_time), ':', 3) AS INTEGER), 0)
            END AS event_seconds
        FROM {relation}
        WHERE route_id = ?
          AND year IS NOT NULL
          AND month IS NOT NULL
    ),
    ordered AS (
        SELECT
            *,
            LAG(event_seconds) OVER (
                PARTITION BY year, month, route_id, service_date, direction_id, stop_key
                ORDER BY event_seconds, vehicle_id
            ) AS prev_seconds
        FROM base
    ),
    gaps AS (
        SELECT
            year,
            month,
            route_id,
            stop_key,
            event_seconds - prev_seconds AS gap_seconds
        FROM ordered
        WHERE event_seconds IS NOT NULL
          AND prev_seconds IS NOT NULL
          AND (event_seconds - prev_seconds) BETWEEN 30 AND 7200
    ),
    stop_baseline AS (
        SELECT
            year,
            month,
            route_id,
            stop_key,
            MEDIAN(gap_seconds) AS stop_median_gap_seconds
        FROM gaps
        GROUP BY year, month, route_id, stop_key
    ),
    enriched AS (
        SELECT
            g.year,
            g.month,
            g.route_id,
            g.gap_seconds,
            ABS(g.gap_seconds - b.stop_median_gap_seconds) AS abs_drift_seconds
        FROM gaps AS g
        INNER JOIN stop_baseline AS b
            USING (year, month, route_id, stop_key)
    )
    SELECT
        year,
        month,
        route_id,
        COUNT(*) AS observations,
        AVG(gap_seconds) AS mean_headway_seconds,
        MEDIAN(gap_seconds) AS median_headway_seconds,
        STDDEV_SAMP(gap_seconds) AS std_headway_seconds,
        AVG(abs_drift_seconds) AS mean_abs_drift_seconds,
        QUANTILE_CONT(abs_drift_seconds, 0.90) AS p90_abs_drift_seconds,
        STDDEV_SAMP(gap_seconds) / NULLIF(AVG(gap_seconds), 0) AS cv_headway
    FROM enriched
    GROUP BY year, month, route_id
    ORDER BY year, month
    """
    return conn.execute(sql, [route_id]).fetchdf()


def add_time_index(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["period"] = pd.to_datetime(
        out["year"].astype(int).astype(str)
        + "-"
        + out["month"].astype(int).astype(str).str.zfill(2)
        + "-01"
    )
    return out.sort_values(["year", "month"]).reset_index(drop=True)


def compute_seasonal_stability(monthly_headway: pd.DataFrame) -> pd.DataFrame:
    """Create a stability proxy and degradation trend for seasonal reporting."""
    if monthly_headway.empty:
        return monthly_headway

    df = monthly_headway.copy()
    df["stability_score"] = 1.0 / (1.0 + df["cv_headway"].fillna(0).clip(lower=0))
    baseline = df["stability_score"].max()
    if baseline > 0:
        df["seasonal_degradation_pct"] = ((baseline - df["stability_score"]) / baseline) * 100.0
    else:
        df["seasonal_degradation_pct"] = 0.0
    return df


def plot_bunching_index(df: pd.DataFrame, *, route_id: str, output_dir: Path) -> None:
    if df.empty:
        return
    fig, ax = plt.subplots(figsize=(9, 5))
    sns.lineplot(data=df, x="period", y="bunching_index", marker="o", ax=ax, color="firebrick")
    ax.set_title(f"Route {route_id} — Monthly Bunching Index")
    ax.set_xlabel("Month")
    ax.set_ylabel("Bunching Index")
    ax.grid(True, linestyle="--", alpha=0.35)
    fig.autofmt_xdate()
    fig.savefig(output_dir / f"longitudinal_bunching_index_{route_id}.pdf")
    plt.close(fig)


def plot_headway_drift(df: pd.DataFrame, *, route_id: str, output_dir: Path) -> None:
    if df.empty:
        return
    fig, ax = plt.subplots(figsize=(9, 5))
    sns.lineplot(
        data=df,
        x="period",
        y="mean_abs_drift_seconds",
        marker="o",
        ax=ax,
        color="steelblue",
    )
    ax.set_title(f"Route {route_id} — Monthly Headway Drift")
    ax.set_xlabel("Month")
    ax.set_ylabel("Mean Absolute Drift (seconds)")
    ax.grid(True, linestyle="--", alpha=0.35)
    fig.autofmt_xdate()
    fig.savefig(output_dir / f"longitudinal_headway_drift_{route_id}.pdf")
    plt.close(fig)


def plot_seasonal_degradation(df: pd.DataFrame, *, route_id: str, output_dir: Path) -> None:
    if df.empty:
        return
    fig, ax = plt.subplots(figsize=(9, 5))
    sns.lineplot(
        data=df,
        x="period",
        y="seasonal_degradation_pct",
        marker="o",
        ax=ax,
        color="darkorange",
    )
    ax.set_title(f"Route {route_id} — Seasonal Stability Degradation (Proxy)")
    ax.set_xlabel("Month")
    ax.set_ylabel("Degradation (%)")
    ax.grid(True, linestyle="--", alpha=0.35)
    fig.autofmt_xdate()
    fig.savefig(output_dir / f"longitudinal_seasonal_degradation_{route_id}.pdf")
    plt.close(fig)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger(__name__)
    args = parse_args()

    settings = get_settings()
    db_path = settings.resolve_path(Path(args.db_path))
    output_dir = (
        Path(args.output)
        if args.output
        else settings.resolve_path(Path("data/outputs/longitudinal"))
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    routes = [args.route] if args.route else list(settings.target_routes)
    relation = safe_identifier(args.view_name)

    logger.info("Connecting to %s", db_path)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        ensure_relation_exists(conn, relation)
        relation_columns = get_relation_columns(conn, relation)
        direction_expr = (
            "COALESCE(CAST(direction_id AS INTEGER), -1)"
            if "direction_id" in relation_columns
            else "-1"
        )
        stop_key_expr = (
            "COALESCE(from_stop_id, CAST(stop_sequence AS VARCHAR))"
            if "from_stop_id" in relation_columns
            else "CAST(stop_sequence AS VARCHAR)"
        )

        for route_id in routes:
            logger.info("Longitudinal analysis for route %s", route_id)

            monthly_bunching = query_monthly_bunching(
                conn,
                relation=relation,
                route_id=route_id,
                bunching_threshold=args.bunching_threshold,
                direction_expr=direction_expr,
                stop_key_expr=stop_key_expr,
            )
            monthly_bunching = add_time_index(monthly_bunching)

            monthly_headway = query_monthly_headway_drift(
                conn,
                relation=relation,
                route_id=route_id,
                direction_expr=direction_expr,
                stop_key_expr=stop_key_expr,
            )
            monthly_headway = add_time_index(monthly_headway)
            monthly_headway = compute_seasonal_stability(monthly_headway)

            if monthly_bunching.empty and monthly_headway.empty:
                logger.warning("No longitudinal records found for route %s", route_id)
                continue

            plot_bunching_index(monthly_bunching, route_id=route_id, output_dir=output_dir)
            plot_headway_drift(monthly_headway, route_id=route_id, output_dir=output_dir)
            plot_seasonal_degradation(monthly_headway, route_id=route_id, output_dir=output_dir)

            if not monthly_bunching.empty:
                monthly_bunching.to_csv(
                    output_dir / f"monthly_bunching_{route_id}.csv", index=False
                )
            if not monthly_headway.empty:
                monthly_headway.to_csv(output_dir / f"monthly_headway_{route_id}.csv", index=False)

            logger.info("Saved longitudinal outputs for route %s in %s", route_id, output_dir)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
