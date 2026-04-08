import logging
import zipfile
from pathlib import Path

import duckdb

from pipeline.config import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def import_historical_csv(zip_path: str, db_path: str):
    """Import historical stop observations from ZIP into DuckDB efficiently."""
    target_routes = ["SF:14", "SF:38", "SF:49"]
    routes_str = ", ".join([f"'{r}'" for r in target_routes])

    # We unzip to a temporary location to ensure DuckDB can read it without issues
    csv_filename = "stop_observations.txt"
    temp_csv = Path("data/raw") / csv_filename

    if not temp_csv.exists():
        if not Path(zip_path).exists():
            logger.error("Archivo ZIP histórico no encontrado: %s", zip_path)
            logger.info("Para reproducir el estudio, debes descargar el dataset histórico de febrero 2026.")
            logger.info("Asegúrate de tener tu API_KEY configurada y ejecuta:")
            logger.info("curl -L 'https://api.511.org/transit/datafeeds?api_key=TU_API_KEY&operator_id=RG&historic=2026-02' -o data/raw/sf_muni_2026_02_so.zip")
            logger.info("(Reemplaza TU_API_KEY con tu clave real de 511.org)")
            return

        logger.info("Extracting %s from ZIP (this may take a minute)...", csv_filename)
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extract(csv_filename, path="data/raw")
        logger.info("Extraction complete.")
    else:
        logger.info("Found existing %s, skipping extraction.", csv_filename)

    logger.info("Connecting to DuckDB: %s", db_path)
    con = duckdb.connect(db_path)

    try:
        logger.info("Importing records from CSV into DuckDB...")
        # Note: we use auto_detect=true but override types for time columns
        query = f"""
        CREATE OR REPLACE TABLE historical_observations AS
        SELECT
            trip_id,
            route_id,
            CAST(service_date AS VARCHAR) as service_date,
            CAST(stop_sequence AS INTEGER) as stop_sequence,
            observed_arrival_time,
            scheduled_arrival_time,
            observed_departure_time,
            scheduled_departure_time,
            vehicle_id
        FROM read_csv('{temp_csv}',
                      auto_detect=true,
                      all_varchar=true)
        WHERE route_id IN ({routes_str})
        """

        con.execute(query)

        count = con.execute("SELECT COUNT(*) FROM historical_observations").fetchone()[0]
        logger.info("Successfully imported %d records for routes %s", count, target_routes)

        logger.info("Creating indexes for analysis...")
        index_query = (
            "CREATE INDEX idx_obs_route ON historical_observations (route_id, service_date)"
        )
        con.execute(index_query)

    except Exception as e:
        logger.error("Failed to import historical data: %s", e)
    finally:
        con.close()
        # Optionally delete the temp CSV to save space (2.6GB)
        # os.remove(temp_csv)


if __name__ == "__main__":
    from pathlib import Path

    settings = get_settings()
    zip_file = settings.resolve_path(Path("data/raw/sf_muni_2026_02_so.zip"))
    db_file = settings.resolve_path(Path(settings.db_path))

    import_historical_csv(str(zip_file), str(db_file))
