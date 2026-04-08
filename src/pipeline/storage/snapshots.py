"""Raw protobuf snapshot file management."""

import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def save_snapshot(raw_dir: Path, timestamp: datetime, label: str, data: bytes) -> Path:
    """Save raw protobuf bytes to disk.

    File layout: {raw_dir}/{YYYY-MM-DD}/{HH-MM-SS}_{label}.bin

    Returns the path to the saved file.
    """
    day_dir = raw_dir / timestamp.strftime("%Y-%m-%d")
    day_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{timestamp.strftime('%H-%M-%S')}_{label}.bin"
    path = day_dir / filename

    if path.exists():
        logger.debug("Snapshot already exists: %s", path)
        return path

    path.write_bytes(data)
    logger.debug("Saved snapshot: %s (%d bytes)", path, len(data))
    return path


def list_snapshots(raw_dir: Path, date: str | None = None) -> list[Path]:
    """List all .bin snapshot files, optionally filtered by date (YYYY-MM-DD)."""
    if date:
        target = raw_dir / date
        if not target.exists():
            return []
        return sorted(target.glob("*.bin"))
    return sorted(raw_dir.rglob("*.bin"))
