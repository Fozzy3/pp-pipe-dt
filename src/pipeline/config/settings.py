"""Application settings loaded from environment variables / .env file."""

from functools import lru_cache
from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[3]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # 511.org API
    api_key: str = "DEMO"
    feed_agency: str = "SF"
    target_routes: str | list[str] = ["14", "38", "49"]

    # Collection
    poll_interval_seconds: int = 130

    # Storage paths (relative to PROJECT_ROOT)
    db_path: Path = Path("data/processed/gtfs_rt.duckdb")
    raw_dir: Path = Path("data/raw")
    static_gtfs_path: Path = Path("data/static/sfmuni.zip")

    # Analysis thresholds
    headway_window_minutes: int = 15
    bunching_threshold_seconds: int = 120
    delay_threshold_seconds: int = 60

    # Logging
    log_level: str = "INFO"

    @field_validator("target_routes", mode="before")
    @classmethod
    def parse_routes(cls, v: str | list[str] | None) -> list[str]:
        if v is None:
            return ["14", "38", "49"]
        if isinstance(v, str):
            import json

            try:
                # Try JSON first
                return json.loads(v)
            except (json.JSONDecodeError, TypeError):
                # Fallback to comma-separated
                return [r.strip() for r in v.split(",")]
        return v

    def resolve_path(self, path: Path) -> Path:
        """Resolve a relative path against the project root."""
        if path.is_absolute():
            return path
        return PROJECT_ROOT / path


@lru_cache
def get_settings() -> Settings:
    return Settings()
