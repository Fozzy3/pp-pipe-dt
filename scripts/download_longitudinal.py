#!/usr/bin/env python3
"""Download 511.org historical GTFS datasets across multiple months."""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from pathlib import Path

import httpx

from pipeline.config import get_settings

BASE_URL = "https://api.511.org/transit/datafeeds"


@dataclass(frozen=True, slots=True)
class DownloadResult:
    month: str
    path: Path
    status: str
    bytes_written: int


def parse_month(month_str: str) -> tuple[int, int]:
    try:
        year_s, month_s = month_str.split("-")
        year, month = int(year_s), int(month_s)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid month '{month_str}' (expected YYYY-MM)") from exc
    if month < 1 or month > 12:
        raise argparse.ArgumentTypeError(f"Invalid month '{month_str}' (month must be 01-12)")
    return year, month


def month_range(start: str, end: str) -> list[str]:
    sy, sm = parse_month(start)
    ey, em = parse_month(end)
    if (sy, sm) > (ey, em):
        raise ValueError("start-month must be <= end-month")

    out: list[str] = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            y += 1
            m = 1
    return out


def month_list(values: list[str]) -> list[str]:
    parsed = sorted(set(values), key=lambda v: parse_month(v))
    for month in parsed:
        parse_month(month)
    return parsed


def build_output_path(raw_root: Path, month: str, prefix: str) -> Path:
    year, mon = parse_month(month)
    out_dir = raw_root / "historical" / f"{year:04d}" / f"{mon:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{prefix}_{year:04d}_{mon:02d}_so.zip"


def download_month(
    *,
    client: httpx.Client,
    api_key: str,
    operator_id: str,
    month: str,
    output_path: Path,
    overwrite: bool,
    retries: int,
    backoff_seconds: float,
    dry_run: bool,
) -> DownloadResult:
    if output_path.exists() and not overwrite:
        return DownloadResult(month=month, path=output_path, status="skipped", bytes_written=0)

    if dry_run:
        return DownloadResult(month=month, path=output_path, status="planned", bytes_written=0)

    params = {
        "api_key": api_key,
        "operator_id": operator_id,
        "historic": month,
    }

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        tmp_path = output_path.with_suffix(output_path.suffix + ".part")
        try:
            with client.stream("GET", BASE_URL, params=params) as response:
                response.raise_for_status()
                with tmp_path.open("wb") as fp:
                    for chunk in response.iter_bytes():
                        fp.write(chunk)
            tmp_path.replace(output_path)
            return DownloadResult(
                month=month,
                path=output_path,
                status="downloaded",
                bytes_written=output_path.stat().st_size,
            )
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            if attempt < retries:
                time.sleep(backoff_seconds * attempt)
            else:
                break

    raise RuntimeError(f"Failed month {month}: {last_error}") from last_error


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download monthly historical GTFS bundles from 511.org"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--months", nargs="+", help="Explicit months (YYYY-MM YYYY-MM ...)")
    mode.add_argument("--start-month", help="Range start (YYYY-MM)")

    parser.add_argument("--end-month", help="Range end (YYYY-MM). Required with --start-month")
    parser.add_argument("--operator-id", default="RG", help="511 operator_id (default: RG)")
    parser.add_argument("--filename-prefix", default="sf_muni", help="Output zip filename prefix")
    parser.add_argument("--raw-root", default="data/raw", help="Base raw directory")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("--timeout", type=float, default=180.0, help="HTTP timeout seconds")
    parser.add_argument("--retries", type=int, default=3, help="Retries per month")
    parser.add_argument("--backoff-seconds", type=float, default=2.0, help="Backoff multiplier")
    parser.add_argument("--dry-run", action="store_true", help="Plan only, no downloads")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger(__name__)
    args = parse_args()

    settings = get_settings()
    raw_root = settings.resolve_path(Path(args.raw_root))
    raw_root.mkdir(parents=True, exist_ok=True)

    if args.months:
        months = month_list(args.months)
    else:
        if not args.end_month:
            raise SystemExit("--end-month is required when using --start-month")
        months = month_range(args.start_month, args.end_month)

    logger.info("Months requested: %s", ", ".join(months))
    if args.dry_run:
        logger.info("Dry-run mode enabled (no network download).")

    api_key = settings.api_key
    results: list[DownloadResult] = []

    with httpx.Client(timeout=args.timeout, follow_redirects=True) as client:
        for month in months:
            out_path = build_output_path(raw_root, month, args.filename_prefix)
            logger.info("%s -> %s", month, out_path)
            try:
                result = download_month(
                    client=client,
                    api_key=api_key,
                    operator_id=args.operator_id,
                    month=month,
                    output_path=out_path,
                    overwrite=args.overwrite,
                    retries=args.retries,
                    backoff_seconds=args.backoff_seconds,
                    dry_run=args.dry_run,
                )
                results.append(result)
                logger.info(
                    "%s: %s (%d bytes)",
                    result.month,
                    result.status,
                    result.bytes_written,
                )
            except Exception as exc:  # noqa: BLE001
                logger.error("%s: failed (%s)", month, exc)
                results.append(
                    DownloadResult(month=month, path=out_path, status="failed", bytes_written=0)
                )

    downloaded = sum(1 for r in results if r.status == "downloaded")
    skipped = sum(1 for r in results if r.status == "skipped")
    planned = sum(1 for r in results if r.status == "planned")
    failed = sum(1 for r in results if r.status == "failed")
    total_bytes = sum(r.bytes_written for r in results)

    logger.info(
        "Summary => downloaded=%d skipped=%d planned=%d failed=%d bytes=%d",
        downloaded,
        skipped,
        planned,
        failed,
        total_bytes,
    )

    if failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
