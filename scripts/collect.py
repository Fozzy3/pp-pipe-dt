#!/usr/bin/env python3
"""Run a single GTFS-RT collection cycle."""

import logging
import sys

from pipeline.collector.runner import run_collection_cycle


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        result = run_collection_cycle()
        logging.info(
            "Done: %d TU + %d VP records at %s",
            len(result.trip_updates),
            len(result.vehicle_positions),
            result.snapshot_ts.isoformat(),
        )
    except Exception:
        logging.exception("Collection cycle failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
