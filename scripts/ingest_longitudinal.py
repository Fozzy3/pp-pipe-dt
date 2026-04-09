#!/usr/bin/env python3
"""Ingest longitudinal historical files into Hive-partitioned Parquet + DuckDB view."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path

import duckdb

from pipeline.config import get_settings

CSV_SUFFIXES = {".csv", ".txt"}
JSON_SUFFIXES = {".json", ".jsonl", ".ndjson"}


@dataclass(frozen=True, slots=True)
class PartitionMonth:
    year: int
    month: int
    raw_dir: Path
    parquet_dir: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Convert historical raw files into Hive-partitioned parquet and register a DuckDB view"
        )
    )
    parser.add_argument(
        "--raw-historical-root",
        default="data/raw/historical",
        help="Root directory containing YYYY/MM raw historical files",
    )
    parser.add_argument(
        "--parquet-root",
        default="data/processed/parquet",
        help="Destination parquet root (Hive partitioned)",
    )
    parser.add_argument(
        "--db-path",
        default="data/processed/transit_longitudinal.db",
        help="DuckDB database path for the external view",
    )
    parser.add_argument(
        "--view-name",
        default="longitudinal_observations",
        help="DuckDB view name to create/update",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite parquet files that already exist",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover files and print ingestion plan without writing parquet/db",
    )
    return parser.parse_args()


def discover_month_partitions(raw_root: Path, parquet_root: Path) -> list[PartitionMonth]:
    partitions: list[PartitionMonth] = []
    if not raw_root.exists():
        return partitions

    for year_dir in sorted(raw_root.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit() or len(year_dir.name) != 4:
            continue
        year = int(year_dir.name)

        for month_dir in sorted(year_dir.iterdir()):
            if not month_dir.is_dir() or not month_dir.name.isdigit() or len(month_dir.name) != 2:
                continue
            month = int(month_dir.name)
            if month < 1 or month > 12:
                continue

            parquet_dir = parquet_root / f"year={year:04d}" / f"month={month:02d}"
            partitions.append(
                PartitionMonth(year=year, month=month, raw_dir=month_dir, parquet_dir=parquet_dir)
            )

    return partitions


def discover_input_files(month_raw_dir: Path) -> tuple[list[Path], list[Path]]:
    csv_files: list[Path] = []
    json_files: list[Path] = []

    for path in sorted(month_raw_dir.rglob("*")):
        if not path.is_file():
            continue
        suffix = path.suffix.lower()
        if suffix in CSV_SUFFIXES:
            csv_files.append(path)
        elif suffix in JSON_SUFFIXES:
            json_files.append(path)

    return csv_files, json_files


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_path_list(paths: list[Path]) -> str:
    return "[" + ", ".join(sql_literal(str(path)) for path in paths) + "]"


def ingest_csv_partition(
    conn: duckdb.DuckDBPyConnection,
    *,
    files: list[Path],
    output_path: Path,
    overwrite: bool,
) -> str:
    if output_path.exists() and not overwrite:
        return "skipped"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    relation = (
        "read_csv_auto("
        f"{sql_path_list(files)}, "
        "union_by_name=true, all_varchar=true, filename=true, ignore_errors=true"
        ")"
    )
    conn.execute(
        "\n".join(
            [
                "COPY (",
                f"  SELECT * FROM {relation}",
                f") TO {sql_literal(str(output_path))}",
                "(FORMAT PARQUET, COMPRESSION ZSTD)",
            ]
        )
    )
    return "written"


def ingest_json_partition(
    conn: duckdb.DuckDBPyConnection,
    *,
    files: list[Path],
    output_path: Path,
    overwrite: bool,
) -> str:
    if output_path.exists() and not overwrite:
        return "skipped"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    relation = f"read_json_auto({sql_path_list(files)}, union_by_name=true, filename=true)"
    conn.execute(
        "\n".join(
            [
                "COPY (",
                f"  SELECT * FROM {relation}",
                f") TO {sql_literal(str(output_path))}",
                "(FORMAT PARQUET, COMPRESSION ZSTD)",
            ]
        )
    )
    return "written"


def register_view(conn: duckdb.DuckDBPyConnection, view_name: str, parquet_root: Path) -> None:
    parquet_glob = parquet_root / "*" / "*" / "*.parquet"
    conn.execute(
        "\n".join(
            [
                f"CREATE OR REPLACE VIEW {view_name} AS",
                "SELECT *",
                f"FROM read_parquet({sql_literal(str(parquet_glob))}, hive_partitioning=1, union_by_name=1)",
            ]
        )
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger(__name__)
    args = parse_args()

    settings = get_settings()
    raw_root = settings.resolve_path(Path(args.raw_historical_root))
    parquet_root = settings.resolve_path(Path(args.parquet_root))
    db_path = settings.resolve_path(Path(args.db_path))

    partitions = discover_month_partitions(raw_root, parquet_root)
    if not partitions:
        logger.warning("No YYYY/MM partitions found under %s", raw_root)
        return

    logger.info("Discovered %d monthly partitions", len(partitions))
    if args.dry_run:
        for part in partitions:
            csv_files, json_files = discover_input_files(part.raw_dir)
            logger.info(
                "%04d-%02d raw=%s csv/txt=%d json=%d parquet=%s",
                part.year,
                part.month,
                part.raw_dir,
                len(csv_files),
                len(json_files),
                part.parquet_dir,
            )
        logger.info("Dry-run completed. No parquet files or DuckDB view were created.")
        return

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))

    written = 0
    skipped = 0
    failed = 0

    try:
        for part in partitions:
            csv_files, json_files = discover_input_files(part.raw_dir)
            logger.info(
                "%04d-%02d: discovered csv/txt=%d json=%d",
                part.year,
                part.month,
                len(csv_files),
                len(json_files),
            )

            if not csv_files and not json_files:
                logger.warning("%04d-%02d: no ingestable files found", part.year, part.month)
                continue

            if csv_files:
                csv_output = part.parquet_dir / "historical_csv.parquet"
                try:
                    status = ingest_csv_partition(
                        conn,
                        files=csv_files,
                        output_path=csv_output,
                        overwrite=args.overwrite,
                    )
                    if status == "written":
                        written += 1
                    else:
                        skipped += 1
                    logger.info(
                        "%04d-%02d csv -> %s (%s)",
                        part.year,
                        part.month,
                        csv_output,
                        status,
                    )
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    logger.error(
                        "%04d-%02d csv ingestion failed: %s",
                        part.year,
                        part.month,
                        exc,
                    )

            if json_files:
                json_output = part.parquet_dir / "historical_json.parquet"
                try:
                    status = ingest_json_partition(
                        conn,
                        files=json_files,
                        output_path=json_output,
                        overwrite=args.overwrite,
                    )
                    if status == "written":
                        written += 1
                    else:
                        skipped += 1
                    logger.info(
                        "%04d-%02d json -> %s (%s)",
                        part.year,
                        part.month,
                        json_output,
                        status,
                    )
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    logger.error(
                        "%04d-%02d json ingestion failed: %s",
                        part.year,
                        part.month,
                        exc,
                    )

        register_view(conn, args.view_name, parquet_root)
        logger.info(
            "Created/updated view '%s' in %s over %s",
            args.view_name,
            db_path,
            parquet_root / "*" / "*" / "*.parquet",
        )
    finally:
        conn.close()

    logger.info("Summary => written=%d skipped=%d failed=%d", written, skipped, failed)
    if failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
