# pp-pipe-dt — GTFS-RT Transit Data Pipeline

Pipeline de procesamiento de datos GTFS Realtime para el análisis de divergencia entre proyección y realidad en transporte público, desarrollado para el paper de la revista **ETASR** (2 semanas de deadline).

## 🚀 Inicio Rápido

Para comenzar a recolectar datos, sigue estos pasos:

1.  **API Key**: Regístrate en [511.org](https://511.org/developers) y obtén tu API key (SF Muni Agency).
2.  **Configuración**:
    ```bash
    cp env.example .env
    # Edita .env y coloca tu API_KEY
    ```
3.  **Prueba Manual**:
    ```bash
    uv run python scripts/collect.py
    ```
4.  **Instalación del Recolector Continuo**:
    ```bash
    bash scripts/install-timer.sh
    ```
    *Nota: El servicio recolectará datos cada 130 segundos (ajustable en `systemd/gtfs-collector.timer`).*

## 📁 Estructura del Proyecto

```text
src/pipeline/
├── collector/     # fetcher, parser, validator, runner, models (GTFS-RT)
├── storage/       # duckdb_store, snapshots (Persistencia)
├── analysis/      # headway, delay_drift, bunching (Métricas ETASR)
├── visualization/ # plots (6 figuras publication-ready)
└── config/        # settings (pydantic-settings, .env)

scripts/           # collect.py, analyze.py (Entry points)
systemd/           # gtfs-collector.service + timer (Daemonización)
tests/             # 5 tests passing, lint limpio (Pytest + Ruff)
docs/              # InfoApi.pdf (Especificaciones de 511.org)
```

## 🧠 AI Agent Support

Este proyecto está diseñado para ser operado por agentes de IA con rigor académico Q1:

- **CLAUDE.md**: Instrucciones de bajo nivel, convenciones y comandos.
- **Skill Registry**: Resolución automática de habilidades (`pipeline-dev`, `gtfs-domain`, `ieee-reviewer`, `q1-enhancer`, `q1-pipeline`).
- **Engram**: Memoria persistente cross-session para decisiones arquitectónicas y descubrimientos.

## 📥 Dataset de Reproducibilidad (Histórico)

Para reproducir los análisis de *Delay Drift* y *Bus Bunching* presentados en el paper (febrero 2026), necesitas descargar el volumen histórico completo de 511.org (Regional GTFS) e importarlo a DuckDB.

```bash
# 1. Descargar el dataset histórico de Febrero 2026 (~1M registros)
curl -L 'https://api.511.org/transit/datafeeds?api_key=TU_API_KEY&operator_id=RG&historic=2026-02' -o data/raw/sf_muni_2026_02_so.zip

# 2. Importar los datos a la base de DuckDB (analítica)
uv run python scripts/import_historical.py
```
*(Nota: Reemplaza `TU_API_KEY` por la clave obtenida en el paso 1).*

## 🛠️ Desarrollo y Análisis

### Análisis de Métricas
Para ejecutar el análisis sobre una ruta específica (ej. Muni 14):
```bash
uv run python scripts/analyze.py --route 14
```

### Calidad de Código
```bash
uv run ruff check src/ tests/  # Linter
uv run pytest                  # Tests unitarios e integración
```

## ⚠️ Restricciones de la API (511.org)
- **Límite**: 60 solicitudes por hora en total (1 req/min recomendado).
- **Frecuencia**: Cada ciclo de recolección descarga 2 feeds (VehiclePositions + TripUpdates), por lo que el intervalo mínimo seguro es de **130 segundos**.
- **Agencia**: San Francisco Muni (SF).

---
*Desarrollado con ❤️ y rigor académico para ETASR 2026.*
