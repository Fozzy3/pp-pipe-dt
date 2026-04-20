#!/usr/bin/env python3
"""Run longitudinal monthly transit analysis over Hive-partitioned data."""

from __future__ import annotations

import argparse
import logging
import re
import time
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import lightgbm as lgb
import xgboost as xgb
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
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
            END AS event_seconds,
            CASE
                WHEN COALESCE(scheduled_departure_time, scheduled_arrival_time) IS NULL THEN NULL
                ELSE
                    COALESCE(TRY_CAST(split_part(COALESCE(scheduled_departure_time, scheduled_arrival_time), ':', 1) AS INTEGER), 0) * 3600
                    + COALESCE(TRY_CAST(split_part(COALESCE(scheduled_departure_time, scheduled_arrival_time), ':', 2) AS INTEGER), 0) * 60
                    + COALESCE(TRY_CAST(split_part(COALESCE(scheduled_departure_time, scheduled_arrival_time), ':', 3) AS INTEGER), 0)
            END AS scheduled_seconds
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
            ) AS prev_seconds,
            LAG(scheduled_seconds) OVER (
                PARTITION BY year, month, route_id, service_date, direction_id, stop_key
                ORDER BY event_seconds, vehicle_id
            ) AS prev_scheduled_seconds
        FROM base
    ),
    gaps AS (
        SELECT
            year,
            month,
            route_id,
            event_seconds - prev_seconds AS gap_seconds,
            scheduled_seconds - prev_scheduled_seconds AS scheduled_headway
        FROM ordered
        WHERE event_seconds IS NOT NULL
          AND prev_seconds IS NOT NULL
          AND (event_seconds - prev_seconds) > 0
          AND (event_seconds - prev_seconds) <= 7200
          AND scheduled_seconds IS NOT NULL
          AND prev_scheduled_seconds IS NOT NULL
          AND (scheduled_seconds - prev_scheduled_seconds) > 0
    )
    SELECT
        year,
        month,
        route_id,
        COUNT(*) AS total_pairs,
        SUM(CASE WHEN gap_seconds < 0.25 * scheduled_headway THEN 1 ELSE 0 END) AS bunching_events,
        AVG(CASE WHEN gap_seconds < 0.25 * scheduled_headway THEN 1.0 ELSE 0.0 END) AS bunching_index,
        AVG(gap_seconds) AS mean_gap_seconds,
        MEDIAN(gap_seconds) AS median_gap_seconds
    FROM gaps
    GROUP BY year, month, route_id
    ORDER BY year, month
    """
    return conn.execute(sql, [route_id, route_id]).fetchdf()


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
            END AS event_seconds,
            CASE
                WHEN COALESCE(scheduled_departure_time, scheduled_arrival_time) IS NULL THEN NULL
                ELSE
                    COALESCE(TRY_CAST(split_part(COALESCE(scheduled_departure_time, scheduled_arrival_time), ':', 1) AS INTEGER), 0) * 3600
                    + COALESCE(TRY_CAST(split_part(COALESCE(scheduled_departure_time, scheduled_arrival_time), ':', 2) AS INTEGER), 0) * 60
                    + COALESCE(TRY_CAST(split_part(COALESCE(scheduled_departure_time, scheduled_arrival_time), ':', 3) AS INTEGER), 0)
            END AS scheduled_seconds
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
            ) AS prev_seconds,
            LAG(scheduled_seconds) OVER (
                PARTITION BY year, month, route_id, service_date, stop_sequence_int
                ORDER BY event_seconds, vehicle_id
            ) AS prev_scheduled_seconds
        FROM base
        WHERE scheduled_seconds IS NOT NULL
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
            event_seconds - prev_seconds AS gap_seconds,
            event_seconds - scheduled_seconds AS delay_seconds,
            scheduled_seconds - prev_scheduled_seconds AS scheduled_headway,
            prev_seconds - prev_scheduled_seconds AS leader_delay_seconds
        FROM stop_events
        WHERE stop_sequence_int IS NOT NULL
          AND event_seconds IS NOT NULL
          AND prev_seconds IS NOT NULL
          AND (event_seconds - prev_seconds) > 0
          AND (event_seconds - prev_seconds) <= 7200
          AND scheduled_seconds IS NOT NULL
    ),
    raw_transitions AS (
        SELECT
            year,
            month,
            route_id,
            service_date,
            vehicle_id,
            stop_sequence_int,
            event_seconds,
            scheduled_headway,
            gap_seconds,
            delay_seconds,
            leader_delay_seconds,
            CASE WHEN gap_seconds < 0.25 * scheduled_headway THEN 1 ELSE 0 END AS raw_bunching,
            LAG(CASE WHEN gap_seconds < 0.25 * scheduled_headway THEN 1 ELSE 0 END, 1) OVER w AS b_n1,
            LAG(CASE WHEN gap_seconds < 0.25 * scheduled_headway THEN 1 ELSE 0 END, 2) OVER w AS b_n2,
            LAG(CASE WHEN gap_seconds < 0.25 * scheduled_headway THEN 1 ELSE 0 END, 3) OVER w AS b_n3,
            LAG(CASE WHEN gap_seconds < 0.25 * scheduled_headway THEN 1 ELSE 0 END, 4) OVER w AS b_n4,
            LAG(CASE WHEN gap_seconds < 0.25 * scheduled_headway THEN 1 ELSE 0 END, 5) OVER w AS b_n5,
            LAG(gap_seconds, 1) OVER w AS g_n1,
            LAG(gap_seconds, 2) OVER w AS g_n2,
            LAG(gap_seconds, 3) OVER w AS g_n3,
            LAG(gap_seconds, 4) OVER w AS g_n4,
            LAG(gap_seconds, 5) OVER w AS g_n5,
            LAG(delay_seconds, 1) OVER w AS d_n1,
            LAG(delay_seconds, 2) OVER w AS d_n2,
            LAG(delay_seconds, 3) OVER w AS d_n3,
            LAG(delay_seconds, 4) OVER w AS d_n4,
            LAG(delay_seconds, 5) OVER w AS d_n5,
            LAG(leader_delay_seconds, 1) OVER w AS ld_n1,
            LAG(leader_delay_seconds, 2) OVER w AS ld_n2,
            LAG(leader_delay_seconds, 3) OVER w AS ld_n3
        FROM stop_gaps
        WINDOW w AS (
            PARTITION BY route_id, service_date, vehicle_id
            ORDER BY stop_sequence_int, event_seconds
        )
    ),
    transitions AS (
        SELECT
            *,
            GREATEST(
                raw_bunching,
                COALESCE(LEAD(raw_bunching, 1) OVER w, 0),
                COALESCE(LEAD(raw_bunching, 2) OVER w, 0)
            ) AS is_bunching
        FROM raw_transitions
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
    return conn.execute(sql, params).fetchdf()


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


def build_paper_features(df: pd.DataFrame, window: int) -> tuple[pd.DataFrame, list[str]]:
    """Build the paper's feature vector from transition rows, augmented with context.

    For window w, features are:
    - d_n1..d_nw: delay at w preceding stops
    - g_n1..g_nw: headway at w preceding stops
    - delta_d_1..delta_d_{w-1}: delay rate of change
    - delta_h_1..delta_h_{w-1}: headway rate of change
    - Contextual: stop_sequence_int, scheduled_headway, time_of_day_sin, time_of_day_cos
    - Ratios: headway_ratio, delay_ratio, hr_n1..hr_nw (headway ratios at prev stops)
    - Leader bus: leader_delay_ratio, relative_delay, ld_n1..ld_nw (leader delay history)
    """
    out = df.copy()
    feature_cols: list[str] = []

    # Absolute delay and headway
    for i in range(1, window + 1):
        feature_cols.extend([f"d_n{i}", f"g_n{i}"])

    # Deltas (rate of change between consecutive stops)
    for i in range(1, window):
        out[f"delta_d_{i}"] = out[f"d_n{i}"] - out[f"d_n{i + 1}"]
        out[f"delta_h_{i}"] = out[f"g_n{i}"] - out[f"g_n{i + 1}"]
        feature_cols.extend([f"delta_d_{i}", f"delta_h_{i}"])

    # Time of day cyclic features (derived from event_seconds modulo 86400)
    hours = (out["event_seconds"] % 86400) / 3600.0
    out["time_sin"] = np.sin(2 * np.pi * hours / 24.0)
    out["time_cos"] = np.cos(2 * np.pi * hours / 24.0)
    feature_cols.extend(["stop_sequence_int", "scheduled_headway", "time_sin", "time_cos"])

    # --- Ratio features (normalize by scheduled_headway) ---
    sh_safe = out["scheduled_headway"].clip(lower=1)
    out["headway_ratio"] = out["gap_seconds"] / sh_safe
    out["delay_ratio"] = out["delay_seconds"] / sh_safe
    feature_cols.extend(["headway_ratio", "delay_ratio"])

    # Historical headway ratios at preceding stops
    for i in range(1, window + 1):
        out[f"hr_n{i}"] = out[f"g_n{i}"] / sh_safe
        feature_cols.append(f"hr_n{i}")

    # --- Leader bus features ---
    if "leader_delay_seconds" in out.columns:
        out["leader_delay_ratio"] = out["leader_delay_seconds"] / sh_safe
        out["relative_delay"] = out["delay_seconds"] - out["leader_delay_seconds"]
        feature_cols.extend(["leader_delay_ratio", "relative_delay"])

        # Leader delay history at preceding stops
        for i in range(1, min(window, 3) + 1):
            col = f"ld_n{i}"
            if col in out.columns:
                out[f"ldr_n{i}"] = out[col] / sh_safe
                feature_cols.append(f"ldr_n{i}")

    # Drop rows with any NaN in features
    out = out.dropna(subset=feature_cols + ["is_bunching"])
    return out, feature_cols


def compute_ablation_f1(transition_rows: pd.DataFrame) -> list[dict[str, int | float]]:
    """Ablation study: F1 by window depth using LightGBM with same temporal split as primary model.

    Uses Route 14, February, transition filter (b_n1=0).
    Train: days 1-16, Cal: days 17-21 (threshold optimization), Test: days 22-28.
    """
    results: list[dict[str, int | float]] = []
    if transition_rows.empty:
        return results

    df = transition_rows.copy()
    df["route_short"] = df["route_id"].str.extract(r"(\d+)$")[0].fillna(df["route_id"])
    df["service_day"] = pd.to_datetime(df["service_date"], errors="coerce").dt.day
    df["service_month"] = pd.to_datetime(df["service_date"], errors="coerce").dt.month

    # Same population as primary model: Route 14, February, transition filter
    r14_feb = df[
        (df["route_short"] == "14") & (df["service_month"] == 2) & (df["b_n1"] == 0)
    ]

    for window in range(1, 6):
        subset, feature_cols = build_paper_features(r14_feb, window)

        train = subset[subset["service_day"] <= 16]
        cal = subset[
            (subset["service_day"] >= 17) & (subset["service_day"] <= 21)
        ]
        test = subset[subset["service_day"] >= 22]

        if len(train) < 200 or len(test) < 50:
            results.append(
                {
                    "window": window,
                    "f1": float("nan"),
                    "n_features": len(feature_cols),
                    "train_rows": 0,
                    "test_rows": 0,
                }
            )
            continue

        y_train = train["is_bunching"].astype(int)
        y_test = test["is_bunching"].astype(int)

        if y_train.nunique() < 2 or y_test.nunique() < 2:
            results.append(
                {
                    "window": window,
                    "f1": float("nan"),
                    "n_features": len(feature_cols),
                    "train_rows": int(len(train)),
                    "test_rows": int(len(test)),
                }
            )
            continue

        n_neg = int((y_train == 0).sum())
        n_pos = int((y_train == 1).sum())
        spw = n_neg / max(n_pos, 1)

        scaler = StandardScaler()
        X_train_s = scaler.fit_transform(train[feature_cols].astype(float))
        X_test_s = scaler.transform(test[feature_cols].astype(float))

        model = lgb.LGBMClassifier(
            n_estimators=300,
            max_depth=6,
            learning_rate=0.05,
            num_leaves=31,
            min_child_samples=20,
            scale_pos_weight=spw,
            subsample=0.8,
            colsample_bytree=0.8,
            deterministic=True,
            force_col_wise=True,
            random_state=42,
            n_jobs=1,
            verbose=-1,
        )
        model.fit(X_train_s, y_train)

        # Optimize threshold on cal set (same protocol as primary model)
        if not cal.empty and cal["is_bunching"].astype(int).nunique() >= 2:
            X_cal_s = scaler.transform(cal[feature_cols].astype(float))
            y_cal = cal["is_bunching"].astype(int)
            probs_cal = model.predict_proba(X_cal_s)[:, 1]
            precs_c, recs_c, threshs_c = precision_recall_curve(y_cal, probs_cal)
            f1s_c = (
                2 * precs_c[:-1] * recs_c[:-1]
                / (precs_c[:-1] + recs_c[:-1] + 1e-10)
            )
            best_tau = float(threshs_c[np.argmax(f1s_c)])
        else:
            best_tau = 0.5

        probs_test = model.predict_proba(X_test_s)[:, 1]
        preds = (probs_test >= best_tau).astype(int)
        score = f1_score(y_test, preds, zero_division=0)

        results.append(
            {
                "window": window,
                "f1": float(score),
                "n_features": len(feature_cols),
                "train_rows": int(len(train)),
                "test_rows": int(len(test)),
            }
        )

    return results


def compute_baseline_metrics(transition_rows: pd.DataFrame) -> dict:
    """Compute naïve single-stop baseline: predict bunching if ΔD_{n-1} > 60s.

    Evaluated on the same test set as the primary model (Route 14, Feb 22-28, b_n1=0).
    """
    if transition_rows.empty:
        return {}

    df = transition_rows.copy()
    df["route_short"] = df["route_id"].str.extract(r"(\d+)$")[0].fillna(df["route_id"])
    df["service_day"] = pd.to_datetime(df["service_date"], errors="coerce").dt.day
    df["service_month"] = pd.to_datetime(df["service_date"], errors="coerce").dt.month

    # Same test population as primary model
    r14_feb = df[
        (df["route_short"] == "14") & (df["service_month"] == 2) & (df["b_n1"] == 0)
    ]
    test = r14_feb[r14_feb["service_day"] >= 22].copy()

    if test.empty or "d_n1" not in test.columns or "d_n2" not in test.columns:
        return {}

    test = test.dropna(subset=["d_n1", "d_n2", "is_bunching"])
    if test.empty:
        return {}

    # Baseline rule: sudden delay spike at preceding stop
    delta_d = test["d_n1"] - test["d_n2"]
    preds = (delta_d > 60).astype(int)
    y_true = test["is_bunching"].astype(int)

    tp = int(((preds == 1) & (y_true == 1)).sum())
    fp = int(((preds == 1) & (y_true == 0)).sum())
    fn = int(((preds == 0) & (y_true == 1)).sum())
    total_predicted = int(preds.sum())

    prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    rec = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0.0
    fdr = fp / (tp + fp) if (tp + fp) > 0 else 0.0

    return {
        "total_test": int(len(test)),
        "total_predicted_positive": total_predicted,
        "true_positives": tp,
        "false_positives": fp,
        "false_negatives": fn,
        "precision": float(prec),
        "recall": float(rec),
        "f1": float(f1),
        "false_discovery_rate": float(fdr),
    }


def train_paper_model(
    transition_rows: pd.DataFrame,
    *,
    window: int = 3,
) -> dict:
    """Train and evaluate the paper's model with proper temporal split.

    Split:
    - Training: Feb 1-16, Route 14
    - Calibration: Feb 17-21, Route 14 (for conformal prediction)
    - Test: Feb 22-28, Route 14 (temporal hold-out)
    - Spatial CV: Routes 38, 49 (full period, no retraining)
    """
    if transition_rows.empty:
        return {}

    # Normalize route_id (strip prefix if present)
    df = transition_rows.copy()
    df["route_short"] = df["route_id"].str.extract(r"(\d+)$")[0].fillna(df["route_id"])

    # Parse service_date to get day of month
    df["service_day"] = pd.to_datetime(df["service_date"], errors="coerce").dt.day
    df["service_month"] = pd.to_datetime(df["service_date"], errors="coerce").dt.month

    # Build features for the chosen window
    df_feat, feature_cols = build_paper_features(df, window)

    # --- Route 14 splits ---
    r14 = df_feat[df_feat["route_short"] == "14"].copy()
    feb_r14 = r14[r14["service_month"] == 2]

    # State-transition filter: B_{n-1} = 0
    feb_r14_filtered = feb_r14[feb_r14["b_n1"] == 0]

    train_set = feb_r14_filtered[feb_r14_filtered["service_day"] <= 16]
    cal_set = feb_r14_filtered[
        (feb_r14_filtered["service_day"] >= 17) & (feb_r14_filtered["service_day"] <= 21)
    ]
    test_set = feb_r14_filtered[feb_r14_filtered["service_day"] >= 22]

    if train_set.empty or test_set.empty:
        return {"error": "Insufficient data for temporal split"}

    X_train = train_set[feature_cols].astype(float)
    y_train = train_set["is_bunching"].astype(int)
    X_cal = cal_set[feature_cols].astype(float) if not cal_set.empty else None
    y_cal = cal_set["is_bunching"].astype(int) if not cal_set.empty else None
    X_test = test_set[feature_cols].astype(float)
    y_test = test_set["is_bunching"].astype(int)

    if y_train.nunique() < 2 or y_test.nunique() < 2:
        return {"error": "Insufficient class diversity"}

    # --- 1. Primary model: LightGBM with scaling ---
    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_cal_s = scaler.transform(X_cal) if X_cal is not None else None
    X_test_s = scaler.transform(X_test)

    # Compute class imbalance ratio for scale_pos_weight
    n_neg_train = int((y_train == 0).sum())
    n_pos_train = int((y_train == 1).sum())
    spw_train = n_neg_train / max(n_pos_train, 1)

    # LightGBM: gradient boosting with sequential error correction, ideal for rare events
    lr = lgb.LGBMClassifier(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        num_leaves=31,
        min_child_samples=20,
        scale_pos_weight=spw_train,
        subsample=0.8,
        colsample_bytree=0.8,
        deterministic=True,
        force_col_wise=True,
        random_state=42,
        n_jobs=1,
        verbose=-1,
    )
    lr.fit(X_train_s, y_train)

    # Optimal threshold via F1 on validation (use cal_set if available, else default)
    if X_cal_s is not None and len(X_cal_s) > 0 and y_cal.nunique() >= 2:
        probs_val = lr.predict_proba(X_cal_s)[:, 1]
        precisions_curve, recalls_curve, thresholds_curve = precision_recall_curve(y_cal, probs_val)
        f1_scores = (
            2
            * precisions_curve[:-1]
            * recalls_curve[:-1]
            / (precisions_curve[:-1] + recalls_curve[:-1] + 1e-10)
        )
        best_tau = float(thresholds_curve[np.argmax(f1_scores)])
    else:
        best_tau = 0.5

    # Evaluate on test
    probs_test = lr.predict_proba(X_test_s)[:, 1]
    preds_test = (probs_test >= best_tau).astype(int)

    # Precision-Recall curve (real hold-out predictions)
    pr_precision, pr_recall, pr_thresholds = precision_recall_curve(y_test, probs_test)

    lr_f1 = float(f1_score(y_test, preds_test, zero_division=0))
    lr_precision = float(precision_score(y_test, preds_test, zero_division=0))
    lr_recall = float(recall_score(y_test, preds_test, zero_division=0))
    lr_prauc = float(average_precision_score(y_test, probs_test))

    # Bootstrap CI
    np.random.seed(42)
    n_test = len(y_test)
    boot_f1s: list[float] = []
    boot_precs: list[float] = []
    for _ in range(1000):
        idx = np.random.randint(0, n_test, size=n_test)
        b_y = y_test.values[idx]
        b_p = preds_test[idx]
        if len(np.unique(b_y)) < 2:
            continue
        tp = np.sum((b_y == 1) & (b_p == 1))
        fp = np.sum((b_y == 0) & (b_p == 1))
        fn = np.sum((b_y == 1) & (b_p == 0))
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
        boot_f1s.append(float(f1))
        boot_precs.append(float(precision))

    lr_f1_ci = (float(np.percentile(boot_f1s, 2.5)), float(np.percentile(boot_f1s, 97.5)))
    lr_prec_ci = (float(np.percentile(boot_precs, 2.5)), float(np.percentile(boot_precs, 97.5)))

    # Feature importance (weights)
    if hasattr(lr, "feature_importances_"):
        weights = dict(zip(feature_cols, lr.feature_importances_.tolist()))
    else:
        weights = dict(zip(feature_cols, lr.coef_[0].tolist()))

    # Inference latency (includes scaling + prediction)
    single_row = X_test.iloc[:1]

    # Keep deterministic single-thread inference for benchmarking
    lr.n_jobs = 1

    t0 = time.perf_counter()
    for _ in range(100):
        lr.predict_proba(scaler.transform(single_row))
    lr_latency_ms = (time.perf_counter() - t0) / 100 * 1000

    # Restore deterministic setting
    lr.n_jobs = 1

    # --- 2. Conformal Prediction ---
    conformal_results: dict = {}
    if X_cal_s is not None and len(X_cal_s) > 50:
        alpha = 0.10  # target 90% coverage
        cal_probs = lr.predict_proba(X_cal_s)[:, 1]
        # Nonconformity scores: s(x) = 1 - p_hat(x) for positive class
        cal_scores = 1.0 - cal_probs
        n_cal = len(cal_scores)
        q_level = np.ceil((n_cal + 1) * (1 - alpha)) / n_cal
        q_threshold = float(np.quantile(cal_scores, min(q_level, 1.0)))

        # Evaluate on test
        test_scores = 1.0 - probs_test
        # Conformal sets
        conformal_sets: list[set] = []
        for s in test_scores:
            pred_set: set = set()
            if s <= q_threshold:  # include label 1
                pred_set.add(1)
            if (1 - s) <= q_threshold:  # include label 0 (score for class 0)
                pred_set.add(0)
            if not pred_set:  # fallback: include both
                pred_set = {0, 1}
            conformal_sets.append(pred_set)

        # Empirical coverage
        coverage = float(
            np.mean([y_test.values[i] in conformal_sets[i] for i in range(len(y_test))])
        )
        avg_set_size = float(np.mean([len(s) for s in conformal_sets]))

        conformal_results = {
            "alpha": alpha,
            "q_threshold": q_threshold,
            "n_cal": n_cal,
            "empirical_coverage": coverage,
            "avg_set_size": avg_set_size,
        }

    # --- 3. Multi-model comparison (WITHOUT state-transition filter) ---
    feb_r14_unfiltered_feat, _ = build_paper_features(feb_r14, window)
    train_unf = feb_r14_unfiltered_feat[feb_r14_unfiltered_feat["service_day"] <= 21]
    test_unf = feb_r14_unfiltered_feat[feb_r14_unfiltered_feat["service_day"] >= 22]

    multi_model: dict = {}
    if not train_unf.empty and not test_unf.empty:
        X_tr_u = train_unf[feature_cols].astype(float)
        y_tr_u = train_unf["is_bunching"].astype(int)
        X_te_u = test_unf[feature_cols].astype(float)
        y_te_u = test_unf["is_bunching"].astype(int)

        scaler_u = StandardScaler()
        X_tr_u_s = scaler_u.fit_transform(X_tr_u)
        X_te_u_s = scaler_u.transform(X_te_u)

        if y_tr_u.nunique() >= 2 and y_te_u.nunique() >= 2:
            # Compute scale_pos_weight for imbalanced boosting models
            n_neg = int((y_tr_u == 0).sum())
            n_pos = int((y_tr_u == 1).sum())
            spw = n_neg / max(n_pos, 1)

            models_mm: dict = {
                "LR_L2": LogisticRegression(
                    max_iter=500,
                    class_weight="balanced",
                    solver="lbfgs",
                ),
                "GBT": HistGradientBoostingClassifier(
                    max_iter=100,
                    max_depth=4,
                    class_weight="balanced",
                    random_state=42,
                ),
                "RF": RandomForestClassifier(
                    n_estimators=100,
                    class_weight="balanced",
                    random_state=42,
                    n_jobs=1,
                ),
                "XGB": xgb.XGBClassifier(
                    n_estimators=200,
                    max_depth=6,
                    learning_rate=0.1,
                    scale_pos_weight=spw,
                    eval_metric="aucpr",
                    random_state=42,
                    n_jobs=1,
                    tree_method="hist",
                ),
                "LGBM": lgb.LGBMClassifier(
                    n_estimators=200,
                    max_depth=6,
                    learning_rate=0.1,
                    scale_pos_weight=spw,
                    deterministic=True,
                    force_col_wise=True,
                    random_state=42,
                    n_jobs=1,
                    verbose=-1,
                ),
            }
            for name, m in models_mm.items():
                m.fit(X_tr_u_s, y_tr_u)

                preds_u = m.predict(X_te_u_s)
                probs_u = m.predict_proba(X_te_u_s)[:, 1]

                # Latency benchmark
                single_u = X_te_u.iloc[:1]
                if hasattr(m, "n_jobs"):
                    m.n_jobs = 1
                t0 = time.perf_counter()
                for _ in range(100):
                    m.predict_proba(scaler_u.transform(single_u))
                lat_ms = (time.perf_counter() - t0) / 100 * 1000
                if hasattr(m, "n_jobs"):
                    m.n_jobs = 1

                multi_model[name] = {
                    "f1": float(f1_score(y_te_u, preds_u, zero_division=0)),
                    "precision": float(precision_score(y_te_u, preds_u, zero_division=0)),
                    "recall": float(recall_score(y_te_u, preds_u, zero_division=0)),
                    "pr_auc": float(average_precision_score(y_te_u, probs_u)),
                    "latency_ms": float(lat_ms),
                }

    # --- 4. Spatial cross-validation ---
    spatial_cv: dict = {}
    for route in ["38", "49"]:
        r_data = df_feat[df_feat["route_short"] == route].copy()
        r_filtered = r_data[r_data["b_n1"] == 0]
        if r_filtered.empty:
            continue
        X_r = r_filtered[feature_cols].astype(float)
        y_r = r_filtered["is_bunching"].astype(int)
        if y_r.nunique() < 2:
            continue
        X_r_s = scaler.transform(X_r)
        probs_r = lr.predict_proba(X_r_s)[:, 1]
        preds_r = (probs_r >= best_tau).astype(int)

        precision_r, recall_r, thresholds_r = precision_recall_curve(y_r, probs_r)
        pr_auc_r = float(average_precision_score(y_r, probs_r))

        spatial_cv[route] = {
            "total_samples": int(len(y_r)),
            "f1": float(f1_score(y_r, preds_r, zero_division=0)),
            "precision": float(precision_score(y_r, preds_r, zero_division=0)),
            "recall": float(recall_score(y_r, preds_r, zero_division=0)),
            "pr_auc": pr_auc_r,
            "pr_curve": {
                "precision": precision_r.tolist(),
                "recall": recall_r.tolist(),
                "thresholds": thresholds_r.tolist(),
                "average_precision": pr_auc_r,
            },
        }

    return {
        "window": window,
        "n_features": len(feature_cols),
        "feature_cols": feature_cols,
        "threshold_tau": best_tau,
        "train_samples": int(len(train_set)),
        "cal_samples": int(len(cal_set)),
        "test_samples": int(len(test_set)),
        "test_transitions": int(y_test.sum()),
        "route14": {
            "f1": lr_f1,
            "f1_ci": lr_f1_ci,
            "precision": lr_precision,
            "precision_ci": lr_prec_ci,
            "recall": lr_recall,
            "pr_auc": lr_prauc,
            "latency_ms": lr_latency_ms,
            "pr_curve": {
                "precision": pr_precision.tolist(),
                "recall": pr_recall.tolist(),
                "thresholds": pr_thresholds.tolist(),
                "average_precision": lr_prauc,
            },
        },
        "weights": weights,
        "conformal": conformal_results,
        "multi_model": multi_model,
        "spatial_cv": spatial_cv,
    }


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
    paper_results: dict | None = None,
    baseline_metrics: dict | None = None,
    duckdb_ms_per_record: float | None = None,
) -> None:
    lines = [
        "Longitudinal Data Science Metrics",
        "===============================",
        f"Total observations in longitudinal view: {total_observations}",
        "",
        "ADF test (daily bunching index, weighted across analyzed routes)",
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
            f"n-{window}: F1={f1_text} | train={item['train_rows']} | test={item['test_rows']}"
        )

    if paper_results:
        pr = paper_results
        feat_str = ", ".join(pr.get("feature_cols", []))
        lines.extend(
            [
                "",
                "Paper Model (Feb 1-16 train / 17-21 cal / 22-28 test)",
                f"Window: n-{pr.get('window', '?')}",
                f"Features: {pr.get('n_features', '?')} ({feat_str})",
                f"Threshold tau: {pr.get('threshold_tau', '?'):.4f}",
                f"Train: {pr.get('train_samples')} "
                f"Cal: {pr.get('cal_samples')} "
                f"Test: {pr.get('test_samples')}",
                f"Test transitions (0->1): {pr.get('test_transitions')}",
                "",
                "Route 14 (temporal hold-out):",
            ]
        )
        r14 = pr.get("route14", {})
        f1_ci = r14.get("f1_ci", (0, 0))
        p_ci = r14.get("precision_ci", (0, 0))
        lines.extend(
            [
                f"  F1: {r14.get('f1', 0):.4f} (CI: [{f1_ci[0]:.4f}, {f1_ci[1]:.4f}])",
                f"  Precision: {r14.get('precision', 0):.4f} (CI: [{p_ci[0]:.4f}, {p_ci[1]:.4f}])",
                f"  Recall: {r14.get('recall', 0):.4f}",
                f"  PR-AUC: {r14.get('pr_auc', 0):.4f}",
                f"  Latency: {r14.get('latency_ms', 0):.5f} ms/rec",
            ]
        )

        weights = paper_results.get("weights", {})
        if weights:
            lines.append("")
            lines.append("Feature weights:")
            for feat, w in sorted(weights.items(), key=lambda x: abs(x[1]), reverse=True):
                lines.append(f"  {feat}: {w:+.6f}")

        conf = paper_results.get("conformal", {})
        if conf:
            lines.extend(
                [
                    "",
                    "Conformal Prediction:",
                    f"  Alpha: {conf.get('alpha', 0):.2f}",
                    f"  Calibration samples: {conf.get('n_cal', 0)}",
                    f"  Quantile threshold: {conf.get('q_threshold', 0):.4f}",
                    f"  Empirical coverage: {conf.get('empirical_coverage', 0):.4f}",
                    f"  Avg set size: {conf.get('avg_set_size', 0):.4f}",
                ]
            )

        mm = paper_results.get("multi_model", {})
        if mm:
            lines.extend(["", "Multi-model comparison (no transition filter):"])
            for name, metrics in mm.items():
                lines.append(
                    f"  {name}: F1={metrics['f1']:.3f} Prec={metrics['precision']:.3f} "
                    f"Rec={metrics['recall']:.3f} PR-AUC={metrics['pr_auc']:.3f} "
                    f"Lat={metrics['latency_ms']:.3f}ms"
                )

        scv = paper_results.get("spatial_cv", {})
        if scv:
            lines.extend(["", "Spatial cross-validation (model from Route 14):"])
            for route, metrics in scv.items():
                lines.append(
                    f"  Route {route}: N={metrics['total_samples']} F1={metrics['f1']:.4f} "
                    f"Prec={metrics['precision']:.4f} Rec={metrics['recall']:.4f} "
                    f"PR-AUC={metrics['pr_auc']:.4f}"
                )

    if baseline_metrics:
        lines.extend(
            [
                "",
                "Baseline (ΔD_{n-1} > 60s, same test set as primary model):",
                f"  Test rows: {baseline_metrics['total_test']}",
                f"  Predicted positive: {baseline_metrics['total_predicted_positive']}",
                f"  True positives: {baseline_metrics['true_positives']}",
                f"  False positives: {baseline_metrics['false_positives']}",
                f"  False negatives: {baseline_metrics['false_negatives']}",
                f"  Precision: {baseline_metrics['precision']:.4f}",
                f"  Recall: {baseline_metrics['recall']:.4f}",
                f"  F1: {baseline_metrics['f1']:.4f}",
                f"  False Discovery Rate: {baseline_metrics['false_discovery_rate']:.4f}",
            ]
        )

    if duckdb_ms_per_record is not None:
        lines.extend(
            [
                "",
                "Latency Breakdown:",
                f"  DuckDB feature extraction: {duckdb_ms_per_record:.5f} ms/record (amortized)",
            ]
        )
        if paper_results:
            model_lat = paper_results.get("route14", {}).get("latency_ms", 0)
            lines.append(f"  Model inference (scaler + predict): {model_lat:.5f} ms/record")
            lines.append(
                f"  Total end-to-end: {duckdb_ms_per_record + model_lat:.5f} ms/record"
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


def save_pr_curve_artifacts(*, paper_results: dict, output_dir: Path) -> None:
    """Save PR curve CSVs for all routes from real hold-out predictions."""
    if not paper_results:
        return

    # 1. Handle Route 14 (primary)
    r14 = paper_results.get("route14", {})
    r14_pr = r14.get("pr_curve", {})
    if r14_pr:
        precision = r14_pr.get("precision", [])
        recall = r14_pr.get("recall", [])
        thresholds = r14_pr.get("thresholds", [])
        threshold_col = [np.nan] + list(thresholds)
        pr_df = pd.DataFrame({"recall": recall, "precision": precision, "threshold": threshold_col})
        pr_df.to_csv(output_dir / "pr_curve_route14.csv", index=False)

    # 2. Handle Spatial CV Routes (38, 49)
    spatial_cv = paper_results.get("spatial_cv", {})
    for route_id, data in spatial_cv.items():
        pr_curve = data.get("pr_curve", {})
        if not pr_curve:
            continue
        precision = pr_curve.get("precision", [])
        recall = pr_curve.get("recall", [])
        thresholds = pr_curve.get("thresholds", [])
        threshold_col = [np.nan] + list(thresholds)
        pr_df = pd.DataFrame({"recall": recall, "precision": precision, "threshold": threshold_col})
        pr_df.to_csv(output_dir / f"pr_curve_route{route_id}.csv", index=False)


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

        # --- DuckDB feature extraction benchmark ---
        logger.info("Benchmarking DuckDB feature extraction")
        t_ddb_start = time.perf_counter()
        transition_rows = query_transition_rows(
            conn,
            relation=relation,
            routes=routes,
        )
        t_ddb_elapsed = time.perf_counter() - t_ddb_start
        n_transition = len(transition_rows)
        duckdb_ms_per_record = (t_ddb_elapsed * 1000 / max(n_transition, 1))
        logger.info(
            "DuckDB feature extraction: %.3f ms total, %.5f ms/record (%d rows)",
            t_ddb_elapsed * 1000,
            duckdb_ms_per_record,
            n_transition,
        )

        logger.info("Computing post filter stats")
        post_filter_stats = compute_post_filter_stats(transition_rows)
        logger.info("Computing ablation F1")
        ablation_metrics = compute_ablation_f1(transition_rows)
        logger.info("Computing baseline metrics")
        baseline_metrics = compute_baseline_metrics(transition_rows)
        if baseline_metrics:
            logger.info(
                "Baseline: FDR=%.3f, predicted=%d, FP=%d, F1=%.4f",
                baseline_metrics["false_discovery_rate"],
                baseline_metrics["total_predicted_positive"],
                baseline_metrics["false_positives"],
                baseline_metrics["f1"],
            )
        logger.info("Training paper model")
        paper_results = train_paper_model(transition_rows, window=3)
        if paper_results and "error" not in paper_results:
            logger.info(
                "Paper model results: F1=%.4f, Precision=%.4f",
                paper_results.get("route14", {}).get("f1", 0),
                paper_results.get("route14", {}).get("precision", 0),
            )
            save_pr_curve_artifacts(paper_results=paper_results, output_dir=output_dir)
            logger.info("Saved real PR-curve artifacts in %s", output_dir)
        adf_pvalue = compute_adf_pvalue(transition_rows)
        total_observations = int(conn.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()[0])

        metrics_path = settings.resolve_path(Path("professor_metrics.txt"))
        write_professor_metrics(
            metrics_path=metrics_path,
            adf_pvalue=adf_pvalue,
            post_filter_stats=post_filter_stats,
            ablation_metrics=ablation_metrics,
            total_observations=total_observations,
            paper_results=paper_results if paper_results and "error" not in paper_results else None,
            baseline_metrics=baseline_metrics,
            duckdb_ms_per_record=duckdb_ms_per_record,
        )
        logger.info("Saved professor metrics to %s", metrics_path)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
