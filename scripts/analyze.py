#!/usr/bin/env python3
"""Run analysis pipeline: compute metrics and generate figures."""

import argparse
import logging
from pathlib import Path

from pipeline.analysis.bunching import compute_bunching
from pipeline.analysis.delay_drift import compute_delay_drift
from pipeline.analysis.headway import compute_headway_deviation
from pipeline.config import get_settings
from pipeline.storage.duckdb_store import DuckDBStore
from pipeline.visualization.plots import generate_all_figures


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Run transit metrics analysis")
    parser.add_argument("--route", help="Single route to analyze (default: all target routes)")
    parser.add_argument("--output", help="Output directory for figures", default=None)
    args = parser.parse_args()

    settings = get_settings()
    db_path = settings.resolve_path(settings.db_path)
    output_dir = Path(args.output) if args.output else settings.resolve_path(Path("data/outputs"))
    output_dir.mkdir(parents=True, exist_ok=True)

    routes = [args.route] if args.route else settings.target_routes

    store = DuckDBStore(db_path)
    try:
        for route_id in routes:
            logger.info("Analyzing route %s", route_id)

            headway_df = compute_headway_deviation(store, route_id, settings)
            drift_df = compute_delay_drift(store, route_id)
            bunching_df = compute_bunching(store, route_id, settings.bunching_threshold_seconds)

            generate_all_figures(
                headway_df=headway_df,
                drift_df=drift_df,
                bunching_df=bunching_df,
                route_id=route_id,
                output_dir=output_dir,
            )
            logger.info("Figures saved to %s", output_dir)
    finally:
        store.close()


if __name__ == "__main__":
    main()
