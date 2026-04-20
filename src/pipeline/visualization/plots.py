"""Publication-quality figures for ETASR paper."""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

logger = logging.getLogger(__name__)

# PeerJ-compatible style
plt.rcParams.update(
    {
        "figure.dpi": 300,
        "savefig.dpi": 300,
        "font.size": 10,
        "axes.titlesize": 11,
        "axes.labelsize": 10,
        "figure.figsize": (8, 5),
        "savefig.bbox": "tight",
    }
)


def generate_all_figures(
    headway_df: pd.DataFrame,
    drift_df: pd.DataFrame,
    bunching_df: pd.DataFrame,
    route_id: str,
    output_dir: Path,
) -> None:
    """Generate all 6 publication figures."""
    output_dir.mkdir(parents=True, exist_ok=True)

    if not headway_df.empty:
        fig1_headway_distribution(headway_df, route_id, output_dir)
        fig2_headway_timeseries(headway_df, route_id, output_dir)

    if not drift_df.empty:
        fig3_delay_drift_profile(drift_df, route_id, output_dir)

    if not bunching_df.empty:
        fig4_bunching_heatmap(bunching_df, route_id, output_dir)

    if not headway_df.empty and not drift_df.empty and not bunching_df.empty:
        fig5_correlation_matrix(headway_df, drift_df, bunching_df, route_id, output_dir)

    logger.info("All figures generated for route %s", route_id)


def fig1_headway_distribution(df: pd.DataFrame, route_id: str, output_dir: Path) -> None:
    """Figure 1: Headway deviation distribution (histogram + KDE)."""
    fig, ax = plt.subplots()
    sns.histplot(df["deviation_s"], kde=True, bins=50, ax=ax, color="steelblue")
    ax.axvline(0, color="red", linestyle="--", linewidth=1, label="Expected")
    ax.set_xlabel("Headway Deviation (seconds)")
    ax.set_ylabel("Frequency")
    ax.set_title(f"Route {route_id} — Headway Deviation Distribution")
    ax.legend()
    fig.savefig(output_dir / f"fig1_headway_dist_{route_id}.pdf")
    plt.close(fig)
    logger.info("Saved fig1_headway_dist_%s.pdf", route_id)


def fig2_headway_timeseries(df: pd.DataFrame, route_id: str, output_dir: Path) -> None:
    """Figure 2: Headway time-series (actual vs median baseline)."""
    # Aggregate by hour
    df = df.copy()
    df["hour"] = df["snapshot_ts"].dt.floor("h")
    hourly = (
        df.groupby("hour")
        .agg(
            actual_mean=("actual_headway_s", "mean"),
            median_baseline=("median_headway_s", "first"),
        )
        .reset_index()
    )

    fig, ax = plt.subplots()
    ax.plot(hourly["hour"], hourly["actual_mean"], label="Actual (mean)", color="steelblue")
    ax.axhline(
        hourly["median_baseline"].median(),
        color="red",
        linestyle="--",
        label="Median baseline",
    )
    ax.set_xlabel("Time")
    ax.set_ylabel("Headway (seconds)")
    ax.set_title(f"Route {route_id} — Headway Over Time")
    ax.legend()
    fig.autofmt_xdate()
    fig.savefig(output_dir / f"fig2_headway_ts_{route_id}.pdf")
    plt.close(fig)
    logger.info("Saved fig2_headway_ts_%s.pdf", route_id)


def fig3_delay_drift_profile(df: pd.DataFrame, route_id: str, output_dir: Path) -> None:
    """Figure 3: Delay drift by stop sequence, grouped by time period."""
    fig, ax = plt.subplots()

    for period, group in df.groupby("time_period"):
        profile = group.groupby("stop_sequence")["drift_from_origin"].mean().reset_index()
        ax.plot(
            profile["stop_sequence"],
            profile["drift_from_origin"],
            label=period,
            marker="o",
            markersize=3,
        )

    ax.axhline(0, color="gray", linestyle=":", linewidth=0.8)
    ax.set_xlabel("Stop Sequence")
    ax.set_ylabel("Drift from Origin (seconds)")
    ax.set_title(f"Route {route_id} — Delay Drift Profile")
    ax.legend(title="Period")
    fig.savefig(output_dir / f"fig3_delay_drift_{route_id}.pdf")
    plt.close(fig)
    logger.info("Saved fig3_delay_drift_%s.pdf", route_id)


def fig4_bunching_heatmap(df: pd.DataFrame, route_id: str, output_dir: Path) -> None:
    """Figure 4: Bunching heatmap (hour × stop sequence)."""
    bunching = df[df["is_bunching"]].copy()
    if bunching.empty:
        logger.warning("No bunching events to plot for route %s", route_id)
        return

    bunching["hour"] = bunching["snapshot_ts"].dt.hour
    pivot = bunching.groupby(["hour", "stop_sequence_1"]).size().unstack(fill_value=0)

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.heatmap(pivot, cmap="YlOrRd", ax=ax, cbar_kws={"label": "Bunching Events"})
    ax.set_xlabel("Stop Sequence")
    ax.set_ylabel("Hour of Day")
    ax.set_title(f"Route {route_id} — Bunching Events (Time × Space)")
    fig.savefig(output_dir / f"fig4_bunching_heatmap_{route_id}.pdf")
    plt.close(fig)
    logger.info("Saved fig4_bunching_heatmap_%s.pdf", route_id)


def fig5_correlation_matrix(
    headway_df: pd.DataFrame,
    drift_df: pd.DataFrame,
    bunching_df: pd.DataFrame,
    route_id: str,
    output_dir: Path,
) -> None:
    """Figure 5: Correlation matrix between key metrics."""
    metrics = {}

    if not headway_df.empty:
        metrics["headway_dev"] = headway_df["deviation_s"].describe()
        metrics["headway_cv"] = (
            headway_df["actual_headway_s"].std() / headway_df["actual_headway_s"].mean()
        )

    if not drift_df.empty:
        final_drift = drift_df.groupby("trip_id")["drift_from_origin"].last()
        metrics["mean_final_drift"] = final_drift.mean()

    if not bunching_df.empty:
        metrics["bunching_rate"] = bunching_df["is_bunching"].mean()

    # Build hourly aggregation for correlation
    dfs_to_merge = []

    if not headway_df.empty:
        hw = headway_df.copy()
        hw["hour"] = hw["snapshot_ts"].dt.floor("h")
        hw_hourly = (
            hw.groupby("hour")
            .agg(
                hw_mean_dev=("deviation_s", "mean"),
                hw_std=("actual_headway_s", "std"),
            )
            .reset_index()
        )
        dfs_to_merge.append(hw_hourly)

    if not drift_df.empty:
        dr = drift_df.copy()
        dr["hour"] = dr["snapshot_ts"].dt.floor("h")
        dr_hourly = (
            dr.groupby("hour")
            .agg(
                drift_mean=("drift_from_origin", "mean"),
            )
            .reset_index()
        )
        dfs_to_merge.append(dr_hourly)

    if not bunching_df.empty:
        bu = bunching_df.copy()
        bu["hour"] = bu["snapshot_ts"].dt.floor("h")
        bu_hourly = (
            bu.groupby("hour")
            .agg(
                bunching_rate=("is_bunching", "mean"),
            )
            .reset_index()
        )
        dfs_to_merge.append(bu_hourly)

    if len(dfs_to_merge) < 2:
        logger.warning("Not enough data for correlation matrix")
        return

    merged = dfs_to_merge[0]
    for other in dfs_to_merge[1:]:
        merged = merged.merge(other, on="hour", how="inner")

    numeric_cols = merged.select_dtypes(include="number").columns
    corr = merged[numeric_cols].corr()

    fig, ax = plt.subplots(figsize=(7, 6))
    sns.heatmap(corr, annot=True, fmt=".2f", cmap="RdBu_r", center=0, ax=ax)
    ax.set_title(f"Route {route_id} — Metric Correlations")
    fig.savefig(output_dir / f"fig5_correlation_{route_id}.pdf")
    plt.close(fig)
    logger.info("Saved fig5_correlation_%s.pdf", route_id)
