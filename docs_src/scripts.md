# Scripts Reference

This page documents the automation scripts in `scripts/` with operational grouping and key options.

## Build and model scripts

## `extract_dgfcd_dgrwi_catalogs.py`

Purpose:

- Extracts DGFCD and DGRWI concepts from DGIF XMI.
- Writes INTERLIS catalog XML outputs.

Main options:

- `--xmi-path`
- `--output-dir`

Example:

```bash
python scripts/extract_dgfcd_dgrwi_catalogs.py --output-dir output
```

## `generate_ili_model.py`

Purpose:

- Converts DGIF XMI/UML content into INTERLIS 2.4 model file.

Main options:

- `--xmi-path`
- `--output-file`
- `--output-dir`

Example:

```bash
python scripts/generate_ili_model.py --output-dir output
```

## `generate_gpkg.py`

Purpose:

- Generates DGIF GeoPackage schema using `ili2gpkg`.

Main options:

- `--ili-model`
- `--output-gpkg`
- `--output-dir`
- `--log-file`

Example:

```bash
python scripts/generate_gpkg.py --output-gpkg output/DGIF_V3.gpkg
```

## Mapping table scripts

## `build_osm_dgif_v3.py`

Purpose:

- Builds OSM to DGIF V3 mapping CSV from V2 mapping and current model classes.

Main options:

- `--ili-file`
- `--input-csv`
- `--output-csv`
- `--output-dir`

## `build_swisstlm3d_dgif_v3.py`

Purpose:

- Builds swissTLM3D to DGIF V3 mapping CSV.

Main options:

- `--ili-file`
- `--output-csv`
- `--output-dir`

## `build_overture_dgif_v3.py`

Purpose:

- Builds Overture to DGIF V3 mapping CSV.

Main options:

- `--ili-file`
- `--output-csv`
- `--output-dir`

## ETL orchestrator scripts

These scripts run multi-phase pipelines and call transform steps.

## `etl_swisstlm3d_to_dgif.py`

Purpose:

- End-to-end swissTLM3D ingestion into DGIF GeoPackage.
- Includes download, optional validation, import, and transform.

Main options:

- `--tlm-url`
- `--download-timeout`
- `--download-retries`
- `--download-retry-wait`
- `--no-auto-discover-tlm-url`
- `--tmp-dir`
- `--skip-download`
- `--skip-validation`
- `--skip-extract`
- `--skip-import`
- `--skip-schema`
- `--python`
- `--xtf-dir`
- `--output-dir`
- `--output-gpkg`

## `etl_ofm_to_dgif.py`

Purpose:

- End-to-end OpenFlightMaps OFMX ingestion into DGIF GeoPackage.

Main options:

- `--ofmx-dir` (required)
- `--tmp-dir`
- `--output-name`
- `--output-dir`
- `--output-gpkg`
- `--skip-schema`
- `--python`

## `etl_overture_to_dgif.py`

Purpose:

- End-to-end Overture file ingestion into DGIF GeoPackage.

Main options:

- `--parquet-dir` (required)
- `--tmp-dir`
- `--output-name`
- `--output-dir`
- `--output-gpkg`
- `--skip-schema`
- `--themes`
- `--python`

## Transform and validation scripts

These are usually called by ETL orchestrators but can also run standalone.

## `etl_swisstlm3d_transform.py`

Main options:

- `--tlm-gpkg` (required)
- `--dgif-gpkg` (required)
- `--mapping` (required)

## `etl_ofm_transform_v2.py`

Main options:

- `--dgif-gpkg` (required)
- `--ofmx-files` (required)

## `etl_overture_transform.py`

Main options:

- `--dgif-gpkg` (required)
- `--mapping` (required)
- `--parquet` (required, repeatable `theme/type=path` pairs)

## `etl_ofm_validate.py`

Purpose:

- Performs geometry, topology, integrity, and attribute checks on OFM-generated DGIF output.

Main options:

- `--dgif-gpkg` (required)
- `--ilivalidator`

## Execution patterns

## Full managed run from backend UI

Use the web UI to select a script and let backend inject compatible output flags when available.

## Direct CLI execution

Run scripts directly from repository root with explicit paths.

## Operational recommendations

- Keep all generated artifacts under a controlled output directory.
- Use `--output-gpkg` for deterministic central target paths.
- Use `--skip-schema` only when reusing an existing schema intentionally.
- For corporate networks, ensure proxy environment variables are set for Java/Python downloads.