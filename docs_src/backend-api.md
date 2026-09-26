# Backend API Reference

Base URL (default local): `http://127.0.0.1:8000`

## Health and pages

- `GET /api/health`: service status.
- `GET /`: serves web UI entry.
- `GET /settings`: serves settings page.

## Script discovery and schema

- `GET /api/scripts`
- Returns script summaries (`scriptName`, `scriptPath`).
- `GET /api/scripts/{script_name}`
- Returns script spec:
  - `scriptName`
  - `scriptPath`
  - `fields[]`: generated from argparse help and optional overrides.

## Job lifecycle

- `GET /api/jobs/current`
- Returns current summary plus full log buffer.
- `POST /api/jobs/start`
- Request body fields:
  - `scriptName`
  - `params` (name-value map keyed by field name)
  - `extraArgs` (list of raw CLI tokens)
  - `aoiGeoJson` (optional)
  - `aoiParamFlag` (optional)
- `POST /api/jobs/stop`
- Requests termination of running process.
- `GET /api/jobs/stream`
- SSE stream of `JobEvent` payloads (`summary`, `newLogs`).

## Destination settings

- `GET /api/settings/db`: load effective settings.
- `POST /api/settings/db`: save settings.
- `POST /api/settings/db/validate`: validate a payload or active saved settings.

Supported destination types:

- `geopackage`
- `duckdb`
- `postgresql`

Additional policy controls include:

- `writeMode` (`replace`, `append`, `fail`)
- `gpkgBindingMode` (`direct`, `ingest`)
- `tablePrefix`
- `artifactsDir`

## Artifacts

- `GET /api/files/download?path=...`
- Downloads a generated file by absolute or workspace-relative path.

## Script execution model

- Exactly one job runs at a time.
- Background execution uses a dedicated thread and process handle.
- Logs are captured from stdout/stderr merge.
- Artifact candidates are inferred from command arguments and recent files in output tree.
- On successful completion, backend may ingest artifacts into configured central destination and attach ingest logs.