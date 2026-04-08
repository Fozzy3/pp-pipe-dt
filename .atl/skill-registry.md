# Skill Registry — pp-pipe-dt

## Project Skills

| Skill | Trigger | Path |
|-------|---------|------|
| pipeline-dev | Python code in pp-pipe-dt | `~/.claude/skills/pipeline-dev/SKILL.md` |
| gtfs-domain | GTFS-RT data, transit metrics, 511.org API | `~/.claude/skills/gtfs-domain/SKILL.md` |
| ieee-reviewer | review paper, revisar paper, ieee access, check draft | `.agent/skills/ieee-reviewer/SKILL.md` |
| q1-enhancer | enhance paper, mejorar paper q1, analizar gaps, gap analysis | `.agent/skills/q1-enhancer/SKILL.md` |
| q1-pipeline | iniciar fabrica editorial, start q1 pipeline, review new batch | `.agent/skills/q1-pipeline/SKILL.md` |

## Compact Rules

### pipeline-dev
- Dataclasses (frozen, slots) for records, never dicts
- httpx not requests, 30s timeout, strip BOM from 511.org
- DuckDB via DuckDBStore, INSERT OR IGNORE, query_df() for analysis
- Config via get_settings() singleton, paths via resolve_path()
- ruff line-length 100, type hints, stdlib logging, no print()
- pytest fixtures: tmp_db for DuckDB, abs() for float comparison

### gtfs-domain
- GTFS-RT: FeedMessage → entity[] → trip_update | vehicle | alert
- 511.org: 60 req/hr TOTAL, agency=SF, strip BOM, protobuf default
- Headway: deduplicate vehicles per stop, filter <30s or >2hr gaps
- Delay drift: arrival_delay per stop_sequence, ROW_NUMBER for latest snapshot
- Bunching: vehicles within 1-2 stop_sequences in same snapshot = bunching
- Gotchas: float32 precision, direction_id can be None, stop_sequence 0 ambiguous
