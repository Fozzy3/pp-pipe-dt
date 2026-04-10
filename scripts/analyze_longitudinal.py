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
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score
from statsmodels.tsa.stattools import adfuller

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
        WHERE (route_id = ? OR regexp_extract(route_id, '([^:]+)$', 1) = ?)
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
    return conn.execute(sql, [route_id, route_id, bunching_threshold, bunching_threshold]).fetchdf()


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
        WHERE (route_id = ? OR regexp_extract(route_id, '([^:]+)$', 1) = ?)
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
    return conn.execute(sql, [route_id, route_id]).fetchdf()


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


def query_transition_rows(
    conn: duckdb.DuckDBPyConnection,
    *,
    relation: str,
    bunching_threshold: int,
    routes: list[str],
) -> pd.DataFrame:
    route_filter = ""
    params: list[object] = []
    if routes:
        placeholders = ", ".join(["?"] * len(routes))
        route_filter = (
            "AND (route_id IN ("
            + placeholders
            + ") OR regexp_extract(route_id, '([^:]+)$', 1) IN ("
            + placeholders
            + "))"
        )
        params.extend(routes)
        params.extend(routes)

    sql = f"""
    WITH base AS (
        SELECT
            CAST(year AS INTEGER) AS year,
            CAST(month AS INTEGER) AS month,
            route_id,
            CAST(service_date AS VARCHAR) AS service_date,
            vehicle_id,
            TRY_CAST(stop_sequence AS INTEGER) AS stop_sequence_int,
            CASE
                WHEN COALESCE(observed_departure_time, observed_arrival_time) IS NULL THEN NULL
                ELSE
                    COALESCE(TRY_CAST(split_part(COALESCE(observed_departure_time, observed_arrival_time), ':', 1) AS INTEGER), 0) * 3600
                    + COALESCE(TRY_CAST(split_part(COALESCE(observed_departure_time, observed_arrival_time), ':', 2) AS INTEGER), 0) * 60
                    + COALESCE(TRY_CAST(split_part(COALESCE(observed_departure_time, observed_arrival_time), ':', 3) AS INTEGER), 0)
            END AS event_seconds
        FROM {relation}
        WHERE year IS NOT NULL
          AND month IS NOT NULL
          {route_filter}
    ),
    stop_events AS (
        SELECT
            *,
            LAG(event_seconds) OVER (
                PARTITION BY year, month, route_id, service_date, stop_sequence_int
                ORDER BY event_seconds, vehicle_id
            ) AS prev_seconds
        FROM base
    ),
    stop_gaps AS (
        SELECT
            year,
            month,
            route_id,
            service_date,
            vehicle_id,
            stop_sequence_int,
            event_seconds,
            event_seconds - prev_seconds AS gap_seconds
        FROM stop_events
        WHERE stop_sequence_int IS NOT NULL
          AND event_seconds IS NOT NULL
          AND prev_seconds IS NOT NULL
          AND (event_seconds - prev_seconds) > 0
          AND (event_seconds - prev_seconds) <= 7200
    ),
    transitions AS (
        SELECT
            year,
            month,
            route_id,
            service_date,
            vehicle_id,
            stop_sequence_int,
            event_seconds,
            gap_seconds,
            CASE WHEN gap_seconds <= ? THEN 1 ELSE 0 END AS is_bunching,
            LAG(CASE WHEN gap_seconds <= ? THEN 1 ELSE 0 END, 1) OVER w AS b_n1,
            LAG(CASE WHEN gap_seconds <= ? THEN 1 ELSE 0 END, 2) OVER w AS b_n2,
            LAG(CASE WHEN gap_seconds <= ? THEN 1 ELSE 0 END, 3) OVER w AS b_n3,
            LAG(CASE WHEN gap_seconds <= ? THEN 1 ELSE 0 END, 4) OVER w AS b_n4,
            LAG(CASE WHEN gap_seconds <= ? THEN 1 ELSE 0 END, 5) OVER w AS b_n5,
            LAG(gap_seconds, 1) OVER w AS g_n1,
            LAG(gap_seconds, 2) OVER w AS g_n2,
            LAG(gap_seconds, 3) OVER w AS g_n3,
            LAG(gap_seconds, 4) OVER w AS g_n4,
            LAG(gap_seconds, 5) OVER w AS g_n5
        FROM stop_gaps
        WINDOW w AS (
            PARTITION BY route_id, service_date, vehicle_id
            ORDER BY stop_sequence_int, event_seconds
        )
    )
    SELECT *
    FROM transitions
    WHERE b_n1 IS NOT NULL
    ORDER BY year, month, route_id, service_date, vehicle_id, stop_sequence_int, event_seconds
    """
    threshold_params = [bunching_threshold] * 6
    return conn.execute(sql, [*params, *threshold_params]).fetchdf()


def compute_post_filter_stats(transition_rows: pd.DataFrame) -> dict[str, int | float]:
    if transition_rows.empty:
        return {
            "total_transition_rows": 0,
            "filtered_total_bn1_eq_0": 0,
            "class_0_to_0": 0,
            "class_0_to_1": 0,
            "filtered_ratio_pct": 0.0,
        }

    filtered = transition_rows[transition_rows["b_n1"] == 0]
    total_rows = int(len(transition_rows))
    filtered_total = int(len(filtered))
    class_0_to_0 = int((filtered["is_bunching"] == 0).sum())
    class_0_to_1 = int((filtered["is_bunching"] == 1).sum())

    return {
        "total_transition_rows": total_rows,
        "filtered_total_bn1_eq_0": filtered_total,
        "class_0_to_0": class_0_to_0,
        "class_0_to_1": class_0_to_1,
        "filtered_ratio_pct": (filtered_total / total_rows * 100.0) if total_rows else 0.0,
    }


def compute_ablation_f1(transition_rows: pd.DataFrame) -> list[dict[str, int | float]]:
    results: list[dict[str, int | float]] = []
    if transition_rows.empty:
        return results

    base = transition_rows[transition_rows["b_n1"] == 0].copy()
    base = base.sort_values(
        [
            "year",
            "month",
            "route_id",
            "service_date",
            "vehicle_id",
            "stop_sequence_int",
            "event_seconds",
        ]
    ).reset_index(drop=True)

    for window in range(1, 6):
        feature_cols = [f"g_n{i}" for i in range(1, window + 1)] + [
            f"b_n{i}" for i in range(1, window + 1)
        ]
        subset = base.dropna(subset=feature_cols + ["is_bunching"]).copy()

        if len(subset) < 200:
            results.append(
                {
                    "window": window,
                    "f1": float("nan"),
                    "train_rows": 0,
                    "test_rows": 0,
                }
            )
            continue

        split_idx = int(len(subset) * 0.8)
        if split_idx <= 0 or split_idx >= len(subset):
            results.append(
                {
                    "window": window,
                    "f1": float("nan"),
                    "train_rows": 0,
                    "test_rows": 0,
                }
            )
            continue

        train = subset.iloc[:split_idx]
        test = subset.iloc[split_idx:]
        y_train = train["is_bunching"].astype(int)
        y_test = test["is_bunching"].astype(int)

        if y_train.nunique() < 2 or y_test.nunique() < 2:
            results.append(
                {
                    "window": window,
                    "f1": float("nan"),
                    "train_rows": int(len(train)),
                    "test_rows": int(len(test)),
                }
            )
            continue

        model = LogisticRegression(max_iter=300, class_weight="balanced", solver="lbfgs")
        model.fit(train[feature_cols].astype(float), y_train)
        preds = model.predict(test[feature_cols].astype(float))
        score = f1_score(y_test, preds, zero_division=0)

        results.append(
            {
                "window": window,
                "f1": float(score),
                "train_rows": int(len(train)),
                "test_rows": int(len(test)),
            }
        )

    return results


def compute_adf_pvalue(transition_rows: pd.DataFrame) -> float:
    if transition_rows.empty:
        return float("nan")

    daily = (
        transition_rows.groupby(["year", "month", "service_date"], as_index=False)["is_bunching"]
        .mean()
        .rename(columns={"is_bunching": "daily_bunching_index"})
    )
    daily["period"] = pd.to_datetime(daily["service_date"], errors="coerce")
    daily = daily.dropna(subset=["period"]).sort_values("period")
    series = daily["daily_bunching_index"].astype(float).dropna()

    if len(series) < 4:
        return float("nan")

    try:
        _, pvalue, *_ = adfuller(series)
        return float(pvalue)
    except ValueError:
        return float("nan")


def write_professor_metrics(
    *,
    metrics_path: Path,
    adf_pvalue: float,
    post_filter_stats: dict[str, int | float],
    ablation_metrics: list[dict[str, int | float]],
    total_observations: int,
) -> None:
    lines = [
        "Longitudinal Data Science Metrics",
        "===============================",
        f"Total observations in longitudinal view: {total_observations}",
        "",
        "ADF test (monthly bunching index, weighted across analyzed routes)",
        f"ADF p-value: {adf_pvalue:.10f}" if not pd.isna(adf_pvalue) else "ADF p-value: NaN",
        "",
        "Post-filter stats (Transition filter: B_{n-1} = 0)",
        f"Total rows with lag available: {post_filter_stats['total_transition_rows']}",
        f"Total rows where B_(n-1)=0: {post_filter_stats['filtered_total_bn1_eq_0']}",
        f"Class 0->0 count: {post_filter_stats['class_0_to_0']}",
        f"Class 0->1 count: {post_filter_stats['class_0_to_1']}",
        f"Filtered ratio (%): {post_filter_stats['filtered_ratio_pct']:.6f}",
        "",
        "Ablation study (F1 by window size)",
    ]

    for item in ablation_metrics:
        window = item["window"]
        f1 = item["f1"]
        f1_text = f"{f1:.6f}" if not pd.isna(f1) else "NaN"
        lines.append(
            f"n-{window}: F1={f1_text} | train_rows={item['train_rows']} | test_rows={item['test_rows']}"
        )

    metrics_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def plot_bunching_index(df: pd.DataFrame, *, route_id: str, output_dir: Path) -> None:
    if df.empty:
        return
    fig, ax = plt.subplots(figsize=(9, 5))
    sns.lineplot(data=df, x="period", y="bunching_index", marker="o", ax=ax, color="firebrick")
    ax.set_title(f"Route {route_id} — Monthly Bunching Index")
    ax.set_xlabel("Month")
    ax.set_ylabel("Bunching Index")
    ax.set_ylim(bottom=0)
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
    ax.set_ylim(bottom=0)
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
    ax.set_ylim(bottom=0)
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

        transition_rows = query_transition_rows(
            conn,
            relation=relation,
            bunching_threshold=args.bunching_threshold,
            routes=routes,
        )
        post_filter_stats = compute_post_filter_stats(transition_rows)
        ablation_metrics = compute_ablation_f1(transition_rows)
        adf_pvalue = compute_adf_pvalue(transition_rows)
        total_observations = int(conn.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()[0])

        metrics_path = settings.resolve_path(Path("professor_metrics.txt"))
        write_professor_metrics(
            metrics_path=metrics_path,
            adf_pvalue=adf_pvalue,
            post_filter_stats=post_filter_stats,
            ablation_metrics=ablation_metrics,
            total_observations=total_observations,
        )
        logger.info("Saved professor metrics to %s", metrics_path)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
