"""Shared test fixtures."""

from pathlib import Path

import pytest

from pipeline.storage.duckdb_store import DuckDBStore


@pytest.fixture
def tmp_db(tmp_path: Path) -> DuckDBStore:
    """Create a temporary DuckDB store for testing."""
    db_path = tmp_path / "test.duckdb"
    store = DuckDBStore(db_path)
    yield store
    store.close()


@pytest.fixture
def tmp_raw_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for raw snapshots."""
    raw = tmp_path / "raw"
    raw.mkdir()
    return raw
