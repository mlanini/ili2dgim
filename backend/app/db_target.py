from __future__ import annotations

import csv
import json
import os
import re
import sqlite3
import subprocess
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = WORKSPACE_ROOT / "backend" / "config" / "db_target.json"
DEFAULT_GPKG_PATH = WORKSPACE_ROOT / "output" / "DGIF_central.gpkg"
DEFAULT_PREFIX = "dgim_"
DEFAULT_SCHEMA = "swissdgif"
DEFAULT_SSLMODE = "prefer"
DEFAULT_INTERLIS_TOOLS = {
    "ili2c": str(WORKSPACE_ROOT / "ressources" / "ili2c-5.6.8" / "ili2c.jar"),
    "ili2validator": str(
        WORKSPACE_ROOT / "ressources" / "ilivalidator-1.15.0" / "ilivalidator-1.15.0.jar"
    ),
    "ili2gpkg": str(
        WORKSPACE_ROOT / "ressources" / "ili2gpkg-5.3.1" / "ili2gpkg-5.3.1.jar"
    ),
    "ili2duckdb": "",
    "ili2pg": "",
    "ili2imd": str(
        WORKSPACE_ROOT / "ressources" / "ili2imd-1.7.1" / "ili2imd.jar"
    ),
}
OGR2OGR_CANDIDATES = [
    Path(r"C:\Program Files\QGIS 3.40.7\bin\ogr2ogr.exe"),
    Path("ogr2ogr"),
]


@dataclass
class IngestResult:
    ok: bool
    logs: list[str]


def _default_settings() -> dict[str, Any]:
    return {
        "destinationType": "geopackage",
        "writeMode": "replace",
        "gpkgBindingMode": "direct",
        "tablePrefix": DEFAULT_PREFIX,
        "artifactsDir": str(WORKSPACE_ROOT / "output"),
        "execution": {
            "pythonExe": str(Path(r"C:\Program Files\QGIS 3.40.7\apps\Python312\python.exe")),
            "tmpDir": "C:/tmp/dgif",
            "proxyUrl": "http://prp01.adb.intra.admin.ch:8080",
            "logLevel": "INFO",
            "interlisTools": dict(DEFAULT_INTERLIS_TOOLS),
            "envVars": {},
        },
        "geopackage": {
            "path": str(DEFAULT_GPKG_PATH),
        },
        "duckdb": {
            "path": str(WORKSPACE_ROOT / "output" / "DGIF_central.duckdb"),
        },
        "postgresql": {
            "host": "127.0.0.1",
            "port": 5432,
            "database": "dgim",
            "schema": DEFAULT_SCHEMA,
            "user": "postgres",
            "passwordEnvVar": "DGIM_DB_PASSWORD",
            "sslmode": DEFAULT_SSLMODE,
        },
    }


def _slug(value: str, fallback: str = "table") -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", value.strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or fallback


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _quote_sqlite_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def load_settings() -> dict[str, Any]:
    base = _default_settings()
    if not CONFIG_PATH.exists():
        return base
    try:
        stored = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return base
    return sanitize_settings(stored)


def save_settings(data: dict[str, Any]) -> dict[str, Any]:
    settings = sanitize_settings(data)
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(json.dumps(settings, indent=2, ensure_ascii=False), encoding="utf-8")
    return settings


def sanitize_settings(raw: dict[str, Any]) -> dict[str, Any]:
    base = _default_settings()
    destination = str(raw.get("destinationType", base["destinationType"]))
    if destination not in {"geopackage", "duckdb", "postgresql"}:
        destination = base["destinationType"]

    write_mode = str(raw.get("writeMode", base["writeMode"]))
    if write_mode not in {"replace", "append", "fail"}:
        write_mode = "replace"

    gpkg_binding_mode = str(raw.get("gpkgBindingMode", base["gpkgBindingMode"]))
    if gpkg_binding_mode not in {"direct", "ingest"}:
        gpkg_binding_mode = "direct"

    prefix = _slug(str(raw.get("tablePrefix", base["tablePrefix"])), fallback="dgim") + "_"
    artifacts_dir = str(raw.get("artifactsDir", base["artifactsDir"]))

    exec_raw = raw.get("execution") or {}
    default_exec = base["execution"]
    python_exe = str(exec_raw.get("pythonExe", default_exec["pythonExe"])).strip()
    tmp_dir = str(exec_raw.get("tmpDir", default_exec["tmpDir"])).strip()
    proxy_url = str(exec_raw.get("proxyUrl", default_exec["proxyUrl"])).strip()
    log_level = str(exec_raw.get("logLevel", default_exec["logLevel"])).strip().upper()
    if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
        log_level = "INFO"

    interlis_tools: dict[str, str] = dict(default_exec.get("interlisTools", {}))
    if isinstance(exec_raw.get("interlisTools"), dict):
        for key, value in exec_raw["interlisTools"].items():
            clean_key = str(key).strip()
            if clean_key in DEFAULT_INTERLIS_TOOLS:
                interlis_tools[clean_key] = str(value).strip()

    env_vars: dict[str, str] = {}
    if isinstance(exec_raw.get("envVars"), dict):
        for key, value in exec_raw["envVars"].items():
            clean_key = str(key).strip()
            clean_value = str(value).strip()
            if clean_key:
                env_vars[clean_key] = clean_value

    gpkg_raw = raw.get("geopackage") or {}
    gpkg_path = str(gpkg_raw.get("path", base["geopackage"]["path"]))

    duckdb_raw = raw.get("duckdb") or {}
    duckdb_path = str(duckdb_raw.get("path", base["duckdb"]["path"]))

    pg_raw = raw.get("postgresql") or {}
    port_val = pg_raw.get("port", base["postgresql"]["port"])
    try:
        pg_port = int(port_val)
    except (TypeError, ValueError):
        pg_port = int(base["postgresql"]["port"])

    schema = _slug(str(pg_raw.get("schema", base["postgresql"]["schema"])), fallback=DEFAULT_SCHEMA)
    sslmode = str(pg_raw.get("sslmode", base["postgresql"]["sslmode"]))
    if sslmode not in {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}:
        sslmode = DEFAULT_SSLMODE

    result = {
        "destinationType": destination,
        "writeMode": write_mode,
        "gpkgBindingMode": gpkg_binding_mode,
        "tablePrefix": prefix,
        "artifactsDir": artifacts_dir,
        "execution": {
            "pythonExe": python_exe or str(default_exec["pythonExe"]),
            "tmpDir": tmp_dir or str(default_exec["tmpDir"]),
            "proxyUrl": proxy_url,
            "logLevel": log_level,
            "interlisTools": interlis_tools,
            "envVars": env_vars,
        },
        "geopackage": {
            "path": gpkg_path,
        },
        "duckdb": {
            "path": duckdb_path,
        },
        "postgresql": {
            "host": str(pg_raw.get("host", base["postgresql"]["host"])),
            "port": pg_port,
            "database": str(pg_raw.get("database", base["postgresql"]["database"])),
            "schema": schema,
            "user": str(pg_raw.get("user", base["postgresql"]["user"])),
            "passwordEnvVar": str(pg_raw.get("passwordEnvVar", base["postgresql"]["passwordEnvVar"])),
            "sslmode": sslmode,
        },
    }
    return result


def _ensure_gpkg_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _ensure_duckdb_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _get_duckdb_module():
    try:
        import duckdb  # type: ignore

        return duckdb
    except Exception as exc:
        raise RuntimeError(
            "DuckDB destination requires Python package 'duckdb'. Install backend requirements first."
        ) from exc


def _ensure_sqlite_audit(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dgim_job_audit (
            run_id TEXT PRIMARY KEY,
            script_name TEXT,
            status TEXT,
            started_at REAL,
            finished_at REAL,
            return_code INTEGER,
            destination_type TEXT,
            artifacts_json TEXT,
            ingested_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dgim_artifact_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            artifact_path TEXT,
            object_name TEXT,
            rows_written INTEGER,
            note TEXT,
            ingested_at TEXT DEFAULT (datetime('now'))
        )
        """
    )


def _ensure_duckdb_audit(conn: Any) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dgim_job_audit (
            run_id VARCHAR PRIMARY KEY,
            script_name VARCHAR,
            status VARCHAR,
            started_at DOUBLE,
            finished_at DOUBLE,
            return_code INTEGER,
            destination_type VARCHAR,
            artifacts_json VARCHAR,
            ingested_at TIMESTAMP DEFAULT NOW()
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dgim_artifact_audit (
            id BIGINT,
            run_id VARCHAR,
            artifact_path VARCHAR,
            object_name VARCHAR,
            rows_written BIGINT,
            note VARCHAR,
            ingested_at TIMESTAMP DEFAULT NOW()
        )
        """
    )


def _sqlite_write_table(
    conn: sqlite3.Connection,
    table_name: str,
    columns: list[str],
    rows: list[list[Any]],
    write_mode: str,
) -> int:
    q_table = _quote_ident(table_name)
    q_cols = [_quote_ident(_slug(c, f"col{i+1}")) for i, c in enumerate(columns)]

    if write_mode == "replace":
        conn.execute(f"DROP TABLE IF EXISTS {q_table}")
    if write_mode == "fail":
        existing = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        if existing:
            raise RuntimeError(f"Table already exists: {table_name}")

    if write_mode in {"replace", "fail"}:
        cols_sql = ", ".join(f"{c} TEXT" for c in q_cols)
        conn.execute(f"CREATE TABLE IF NOT EXISTS {q_table} ({cols_sql})")

    if rows:
        placeholders = ", ".join("?" for _ in q_cols)
        insert_cols = ", ".join(q_cols)
        conn.executemany(
            f"INSERT INTO {q_table} ({insert_cols}) VALUES ({placeholders})",
            [["" if v is None else str(v) for v in row] for row in rows],
        )
    return len(rows)


def _duckdb_table_exists(conn: Any, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema='main' AND table_name=?",
        [table_name],
    ).fetchone()
    return row is not None


def _duckdb_write_table(
    conn: Any,
    table_name: str,
    columns: list[str],
    rows: list[list[Any]],
    write_mode: str,
) -> int:
    q_table = _quote_ident(table_name)
    q_cols = [_quote_ident(_slug(c, f"col{i+1}")) for i, c in enumerate(columns)]

    if write_mode == "replace":
        conn.execute(f"DROP TABLE IF EXISTS {q_table}")
    if write_mode == "fail" and _duckdb_table_exists(conn, table_name):
        raise RuntimeError(f"Table already exists: {table_name}")

    if write_mode in {"replace", "fail"}:
        cols_sql = ", ".join(f"{c} VARCHAR" for c in q_cols)
        conn.execute(f"CREATE TABLE IF NOT EXISTS {q_table} ({cols_sql})")

    if rows:
        if write_mode == "append" and not _duckdb_table_exists(conn, table_name):
            cols_sql = ", ".join(f"{c} VARCHAR" for c in q_cols)
            conn.execute(f"CREATE TABLE IF NOT EXISTS {q_table} ({cols_sql})")

        placeholders = ", ".join("?" for _ in q_cols)
        insert_cols = ", ".join(q_cols)
        conn.executemany(
            f"INSERT INTO {q_table} ({insert_cols}) VALUES ({placeholders})",
            [["" if v is None else str(v) for v in row] for row in rows],
        )
    return len(rows)


def _import_csv_to_sqlite(
    conn: sqlite3.Connection,
    artifact: Path,
    table_prefix: str,
    write_mode: str,
    run_id: str,
) -> str:
    with artifact.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.reader(handle)
        all_rows = list(reader)

    if not all_rows:
        header = ["value"]
        body: list[list[Any]] = []
    else:
        header = [h.strip() or "value" for h in all_rows[0]]
        body = [r + [""] * (len(header) - len(r)) for r in all_rows[1:]]

    table = f"{table_prefix}{_slug(artifact.stem)}"
    count = _sqlite_write_table(conn, table, header, body, write_mode)
    conn.execute(
        "INSERT INTO dgim_artifact_audit (run_id, artifact_path, object_name, rows_written, note) VALUES (?, ?, ?, ?, ?)",
        (run_id, str(artifact), table, count, "csv"),
    )
    return f"[db-target] CSV -> {table} ({count} rows)"


def _flatten_xml_record(elem: ET.Element) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in elem.attrib.items():
        result[f"attr_{_slug(key, key)}"] = value
    for child in elem:
        key = _slug(child.tag.split("}")[-1], "value")
        text = (child.text or "").strip()
        if text:
            result[key] = text
    return result


def _import_xml_to_sqlite(
    conn: sqlite3.Connection,
    artifact: Path,
    table_prefix: str,
    write_mode: str,
    run_id: str,
) -> str:
    tree = ET.parse(artifact)
    root = tree.getroot()

    records: list[dict[str, str]] = []
    for elem in root.iter():
        if len(list(elem)) == 0 and (elem.text or "").strip():
            records.append({"tag": elem.tag.split("}")[-1], "value": (elem.text or "").strip()})
        elif elem.attrib:
            row = {"tag": elem.tag.split("}")[-1]}
            row.update({f"attr_{_slug(k, k)}": str(v) for k, v in elem.attrib.items()})
            records.append(row)
        else:
            row = _flatten_xml_record(elem)
            if row:
                row["tag"] = elem.tag.split("}")[-1]
                records.append(row)

    if not records:
        records = [{"tag": root.tag.split("}")[-1], "value": ""}]

    columns = sorted({k for r in records for k in r.keys()})
    rows = [[r.get(c, "") for c in columns] for r in records]
    table = f"{table_prefix}{_slug(artifact.stem)}"
    count = _sqlite_write_table(conn, table, columns, rows, write_mode)
    conn.execute(
        "INSERT INTO dgim_artifact_audit (run_id, artifact_path, object_name, rows_written, note) VALUES (?, ?, ?, ?, ?)",
        (run_id, str(artifact), table, count, "xml"),
    )
    return f"[db-target] XML -> {table} ({count} rows)"


def _import_csv_to_duckdb(
    conn: Any,
    artifact: Path,
    table_prefix: str,
    write_mode: str,
    run_id: str,
) -> str:
    with artifact.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.reader(handle)
        all_rows = list(reader)

    if not all_rows:
        header = ["value"]
        body: list[list[Any]] = []
    else:
        header = [h.strip() or "value" for h in all_rows[0]]
        body = [r + [""] * (len(header) - len(r)) for r in all_rows[1:]]

    table = f"{table_prefix}{_slug(artifact.stem)}"
    count = _duckdb_write_table(conn, table, header, body, write_mode)
    conn.execute(
        "INSERT INTO dgim_artifact_audit (run_id, artifact_path, object_name, rows_written, note) VALUES (?, ?, ?, ?, ?)",
        [run_id, str(artifact), table, count, "csv"],
    )
    return f"[db-target] CSV -> {table} ({count} rows)"


def _import_xml_to_duckdb(
    conn: Any,
    artifact: Path,
    table_prefix: str,
    write_mode: str,
    run_id: str,
) -> str:
    tree = ET.parse(artifact)
    root = tree.getroot()

    records: list[dict[str, str]] = []
    for elem in root.iter():
        if len(list(elem)) == 0 and (elem.text or "").strip():
            records.append({"tag": elem.tag.split("}")[-1], "value": (elem.text or "").strip()})
        elif elem.attrib:
            row = {"tag": elem.tag.split("}")[-1]}
            row.update({f"attr_{_slug(k, k)}": str(v) for k, v in elem.attrib.items()})
            records.append(row)
        else:
            row = _flatten_xml_record(elem)
            if row:
                row["tag"] = elem.tag.split("}")[-1]
                records.append(row)

    if not records:
        records = [{"tag": root.tag.split("}")[-1], "value": ""}]

    columns = sorted({k for r in records for k in r.keys()})
    rows = [[r.get(c, "") for c in columns] for r in records]
    table = f"{table_prefix}{_slug(artifact.stem)}"
    count = _duckdb_write_table(conn, table, columns, rows, write_mode)
    conn.execute(
        "INSERT INTO dgim_artifact_audit (run_id, artifact_path, object_name, rows_written, note) VALUES (?, ?, ?, ?, ?)",
        [run_id, str(artifact), table, count, "xml"],
    )
    return f"[db-target] XML -> {table} ({count} rows)"


def _sqlite_decltype_to_duckdb(decl_type: str | None) -> str:
    dt = (decl_type or "").strip().upper()
    if "INT" in dt:
        return "BIGINT"
    if any(t in dt for t in ["CHAR", "CLOB", "TEXT"]):
        return "VARCHAR"
    if "BLOB" in dt:
        return "BLOB"
    if any(t in dt for t in ["REAL", "FLOA", "DOUB"]):
        return "DOUBLE"
    if any(t in dt for t in ["NUMERIC", "DECIMAL"]):
        return "DOUBLE"
    return "VARCHAR"


def _copy_gpkg_tables_to_duckdb(
    conn: Any,
    source_gpkg: Path,
    table_prefix: str,
    write_mode: str,
    run_id: str,
) -> list[str]:
    logs: list[str] = []

    with sqlite3.connect(str(source_gpkg)) as src_conn:
        source_tables = [
            r[0]
            for r in src_conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'gpkg_%' "
                "AND name NOT LIKE 'sqlite_%' "
                "AND name NOT LIKE 'rtree_%'"
            ).fetchall()
        ]

        for src_table in source_tables:
            dest_table = f"{table_prefix}{_slug(source_gpkg.stem)}_{_slug(src_table)}"
            q_dest = _quote_ident(dest_table)
            q_src = _quote_ident(src_table)

            cols_info = src_conn.execute(
                f"PRAGMA table_info({q_src})"
            ).fetchall()
            if not cols_info:
                continue

            col_names = [str(c[1]) for c in cols_info]
            col_types = [_sqlite_decltype_to_duckdb(c[2]) for c in cols_info]
            q_cols = [_quote_ident(c) for c in col_names]
            create_cols_sql = ", ".join(
                f"{name} {dtype}" for name, dtype in zip(q_cols, col_types)
            )

            if write_mode == "replace":
                conn.execute(f"DROP TABLE IF EXISTS {q_dest}")
                conn.execute(f"CREATE TABLE {q_dest} ({create_cols_sql})")
            elif write_mode == "fail":
                if _duckdb_table_exists(conn, dest_table):
                    raise RuntimeError(f"Table already exists: {dest_table}")
                conn.execute(f"CREATE TABLE {q_dest} ({create_cols_sql})")
            else:
                if not _duckdb_table_exists(conn, dest_table):
                    conn.execute(f"CREATE TABLE {q_dest} ({create_cols_sql})")

            select_cols_sql = ", ".join(q_cols)
            placeholders = ", ".join("?" for _ in col_names)
            insert_sql = (
                f"INSERT INTO {q_dest} ({select_cols_sql}) VALUES ({placeholders})"
            )

            cur = src_conn.execute(f"SELECT {select_cols_sql} FROM {q_src}")
            copied = 0
            while True:
                batch = cur.fetchmany(1000)
                if not batch:
                    break
                conn.executemany(insert_sql, [list(row) for row in batch])
                copied += len(batch)

            conn.execute(
                "INSERT INTO dgim_artifact_audit (run_id, artifact_path, object_name, rows_written, note) VALUES (?, ?, ?, ?, ?)",
                [run_id, str(source_gpkg), dest_table, copied, "gpkg-table"],
            )
            logs.append(f"[db-target] GPKG layer -> {dest_table} ({copied} rows)")

    return logs


def _copy_gpkg_tables_to_sqlite(
    conn: sqlite3.Connection,
    source_gpkg: Path,
    table_prefix: str,
    write_mode: str,
    run_id: str,
) -> list[str]:
    logs: list[str] = []
    src_alias = "srcg"
    # ATTACH with an inlined, escaped path avoids driver-specific binding issues.
    conn.execute(
        f"ATTACH DATABASE {_quote_sqlite_string(str(source_gpkg))} AS {src_alias}"
    )
    try:
        source_tables = [
            r[0]
            for r in conn.execute(
                f"SELECT name FROM {src_alias}.sqlite_master WHERE type='table' AND name NOT LIKE 'gpkg_%' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'rtree_%'"
            ).fetchall()
        ]

        for src_table in source_tables:
            dest_table = f"{table_prefix}{_slug(source_gpkg.stem)}_{_slug(src_table)}"
            q_dest = _quote_ident(dest_table)
            q_src = _quote_ident(src_table)

            if write_mode == "replace":
                conn.execute(f"DROP TABLE IF EXISTS {q_dest}")
                conn.execute(f"CREATE TABLE {q_dest} AS SELECT * FROM {src_alias}.{q_src}")
            elif write_mode == "fail":
                exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (dest_table,),
                ).fetchone()
                if exists:
                    raise RuntimeError(f"Table already exists: {dest_table}")
                conn.execute(f"CREATE TABLE {q_dest} AS SELECT * FROM {src_alias}.{q_src}")
            else:
                exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (dest_table,),
                ).fetchone()
                if not exists:
                    conn.execute(f"CREATE TABLE {q_dest} AS SELECT * FROM {src_alias}.{q_src} WHERE 1=0")
                conn.execute(f"INSERT INTO {q_dest} SELECT * FROM {src_alias}.{q_src}")

            rows = conn.execute(f"SELECT COUNT(*) FROM {q_dest}").fetchone()[0]
            conn.execute(
                "INSERT INTO dgim_artifact_audit (run_id, artifact_path, object_name, rows_written, note) VALUES (?, ?, ?, ?, ?)",
                (run_id, str(source_gpkg), dest_table, int(rows), "gpkg-table"),
            )
            logs.append(f"[db-target] GPKG layer -> {dest_table} ({rows} rows)")
    finally:
        conn.execute(f"DETACH DATABASE {src_alias}")

    return logs


def _get_psycopg():
    try:
        import psycopg  # type: ignore

        return psycopg
    except Exception as exc:
        raise RuntimeError(
            "PostgreSQL destination requires psycopg. Install backend requirements first."
        ) from exc


def _pg_connect(pg_cfg: dict[str, Any]):
    psycopg = _get_psycopg()
    env_var = str(pg_cfg.get("passwordEnvVar", "DGIM_DB_PASSWORD"))
    password = os.environ.get(env_var)
    if not password:
        raise RuntimeError(f"Missing PostgreSQL password environment variable: {env_var}")

    return psycopg.connect(
        host=pg_cfg["host"],
        port=int(pg_cfg["port"]),
        dbname=pg_cfg["database"],
        user=pg_cfg["user"],
        password=password,
        sslmode=pg_cfg.get("sslmode", DEFAULT_SSLMODE),
    )


def _ensure_pg_schema_and_audit(conn: Any, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(schema)}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_quote_ident(schema)}.dgim_job_audit (
                run_id TEXT PRIMARY KEY,
                script_name TEXT,
                status TEXT,
                started_at DOUBLE PRECISION,
                finished_at DOUBLE PRECISION,
                return_code INTEGER,
                destination_type TEXT,
                artifacts_json TEXT,
                ingested_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_quote_ident(schema)}.dgim_artifact_audit (
                id BIGSERIAL PRIMARY KEY,
                run_id TEXT,
                artifact_path TEXT,
                object_name TEXT,
                rows_written BIGINT,
                note TEXT,
                ingested_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
    conn.commit()


def _pg_write_table(
    conn: Any,
    schema: str,
    table_name: str,
    columns: list[str],
    rows: list[list[Any]],
    write_mode: str,
) -> int:
    q_schema = _quote_ident(schema)
    q_table = _quote_ident(table_name)
    norm_cols = [_slug(c, f"col{i+1}") for i, c in enumerate(columns)]
    q_cols = [_quote_ident(c) for c in norm_cols]

    with conn.cursor() as cur:
        if write_mode == "replace":
            cur.execute(f"DROP TABLE IF EXISTS {q_schema}.{q_table}")
        if write_mode == "fail":
            cur.execute(
                """
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
                """,
                (schema, table_name),
            )
            if cur.fetchone():
                raise RuntimeError(f"Table already exists: {schema}.{table_name}")

        if write_mode in {"replace", "fail"}:
            cols_sql = ", ".join(f"{c} TEXT" for c in q_cols)
            cur.execute(f"CREATE TABLE IF NOT EXISTS {q_schema}.{q_table} ({cols_sql})")

        if rows:
            placeholders = ", ".join(["%s"] * len(q_cols))
            cols_sql = ", ".join(q_cols)
            cur.executemany(
                f"INSERT INTO {q_schema}.{q_table} ({cols_sql}) VALUES ({placeholders})",
                [["" if v is None else str(v) for v in row] for row in rows],
            )

    conn.commit()
    return len(rows)


def _import_csv_to_postgres(
    conn: Any,
    schema: str,
    artifact: Path,
    table_prefix: str,
    write_mode: str,
    run_id: str,
) -> str:
    with artifact.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.reader(handle)
        all_rows = list(reader)

    if not all_rows:
        header = ["value"]
        body: list[list[Any]] = []
    else:
        header = [h.strip() or "value" for h in all_rows[0]]
        body = [r + [""] * (len(header) - len(r)) for r in all_rows[1:]]

    table = f"{table_prefix}{_slug(artifact.stem)}"
    count = _pg_write_table(conn, schema, table, header, body, write_mode)
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {_quote_ident(schema)}.dgim_artifact_audit (run_id, artifact_path, object_name, rows_written, note) VALUES (%s, %s, %s, %s, %s)",
            (run_id, str(artifact), f"{schema}.{table}", count, "csv"),
        )
    conn.commit()
    return f"[db-target] CSV -> {schema}.{table} ({count} rows)"


def _import_xml_to_postgres(
    conn: Any,
    schema: str,
    artifact: Path,
    table_prefix: str,
    write_mode: str,
    run_id: str,
) -> str:
    tree = ET.parse(artifact)
    root = tree.getroot()
    records: list[dict[str, str]] = []

    for elem in root.iter():
        if len(list(elem)) == 0 and (elem.text or "").strip():
            records.append({"tag": elem.tag.split("}")[-1], "value": (elem.text or "").strip()})
        elif elem.attrib:
            row = {"tag": elem.tag.split("}")[-1]}
            row.update({f"attr_{_slug(k, k)}": str(v) for k, v in elem.attrib.items()})
            records.append(row)

    if not records:
        records = [{"tag": root.tag.split("}")[-1], "value": ""}]

    columns = sorted({k for r in records for k in r.keys()})
    rows = [[r.get(c, "") for c in columns] for r in records]

    table = f"{table_prefix}{_slug(artifact.stem)}"
    count = _pg_write_table(conn, schema, table, columns, rows, write_mode)
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {_quote_ident(schema)}.dgim_artifact_audit (run_id, artifact_path, object_name, rows_written, note) VALUES (%s, %s, %s, %s, %s)",
            (run_id, str(artifact), f"{schema}.{table}", count, "xml"),
        )
    conn.commit()
    return f"[db-target] XML -> {schema}.{table} ({count} rows)"


def _ogr2ogr_path() -> str | None:
    for candidate in OGR2OGR_CANDIDATES:
        if candidate.name == "ogr2ogr" and shutil_which("ogr2ogr"):
            return "ogr2ogr"
        if candidate.exists():
            return str(candidate)
    return None


def shutil_which(cmd: str) -> str | None:
    from shutil import which

    return which(cmd)


def _import_gpkg_to_postgres(
    pg_cfg: dict[str, Any],
    schema: str,
    artifact: Path,
    table_prefix: str,
    write_mode: str,
    run_id: str,
) -> list[str]:
    tool = _ogr2ogr_path()
    if tool is None:
        raise RuntimeError("ogr2ogr not found; required for GeoPackage -> PostgreSQL spatial import")

    env_var = str(pg_cfg.get("passwordEnvVar", "DGIM_DB_PASSWORD"))
    password = os.environ.get(env_var)
    if not password:
        raise RuntimeError(f"Missing PostgreSQL password environment variable: {env_var}")

    with sqlite3.connect(str(artifact)) as src_conn:
        layers = [
            r[0]
            for r in src_conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'gpkg_%' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'rtree_%'"
            ).fetchall()
        ]

    logs: list[str] = []
    pg_dsn = (
        f"PG:host={pg_cfg['host']} port={int(pg_cfg['port'])} dbname={pg_cfg['database']} "
        f"user={pg_cfg['user']} password={password} sslmode={pg_cfg.get('sslmode', DEFAULT_SSLMODE)}"
    )

    for layer in layers:
        dest_table = f"{table_prefix}{_slug(artifact.stem)}_{_slug(layer)}"
        cmd = [
            tool,
            "-f",
            "PostgreSQL",
            pg_dsn,
            str(artifact),
            layer,
            "-lco",
            f"SCHEMA={schema}",
            "-nln",
            f"{schema}.{dest_table}",
        ]
        if write_mode == "replace":
            cmd.append("-overwrite")
        elif write_mode == "append":
            cmd.append("-append")

        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "ogr2ogr failed")

        logs.append(f"[db-target] GPKG layer -> {schema}.{dest_table}")

    conn = _pg_connect(pg_cfg)
    try:
        with conn.cursor() as cur:
            for layer in layers:
                dest_table = f"{table_prefix}{_slug(artifact.stem)}_{_slug(layer)}"
                cur.execute(
                    f"SELECT COUNT(*) FROM {_quote_ident(schema)}.{_quote_ident(dest_table)}"
                )
                rows = int(cur.fetchone()[0])
                cur.execute(
                    f"INSERT INTO {_quote_ident(schema)}.dgim_artifact_audit (run_id, artifact_path, object_name, rows_written, note) VALUES (%s, %s, %s, %s, %s)",
                    (run_id, str(artifact), f"{schema}.{dest_table}", rows, "gpkg-layer"),
                )
        conn.commit()
    finally:
        conn.close()

    return logs


def validate_destination(settings: dict[str, Any]) -> dict[str, Any]:
    settings = sanitize_settings(settings)
    dest = settings["destinationType"]

    if dest == "geopackage":
        gpkg_path = Path(settings["geopackage"]["path"]).expanduser()
        _ensure_gpkg_parent(gpkg_path)
        with sqlite3.connect(str(gpkg_path)) as conn:
            _ensure_sqlite_audit(conn)
            conn.execute("CREATE TABLE IF NOT EXISTS dgim_validate_write (id INTEGER)")
            conn.execute("DELETE FROM dgim_validate_write")
            conn.execute("INSERT INTO dgim_validate_write (id) VALUES (1)")
            conn.commit()
        return {
            "ok": True,
            "message": f"GeoPackage writable: {gpkg_path}",
        }

    if dest == "duckdb":
        duckdb_path = Path(settings["duckdb"]["path"]).expanduser()
        _ensure_duckdb_parent(duckdb_path)
        duckdb = _get_duckdb_module()
        with duckdb.connect(str(duckdb_path)) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dgim_validate_write (
                    id INTEGER
                )
                """
            )
            conn.execute("DELETE FROM dgim_validate_write")
            conn.execute("INSERT INTO dgim_validate_write (id) VALUES (1)")
        return {
            "ok": True,
            "message": f"DuckDB writable: {duckdb_path}",
        }

    pg = settings["postgresql"]
    conn = _pg_connect(pg)
    try:
        _ensure_pg_schema_and_audit(conn, pg["schema"])
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {_quote_ident(pg['schema'])}.dgim_validate_write (id INTEGER)"
            )
            cur.execute(f"DELETE FROM {_quote_ident(pg['schema'])}.dgim_validate_write")
            cur.execute(f"INSERT INTO {_quote_ident(pg['schema'])}.dgim_validate_write (id) VALUES (1)")
        conn.commit()
    finally:
        conn.close()

    return {
        "ok": True,
        "message": f"PostgreSQL writable: {pg['host']}:{pg['port']}/{pg['database']} schema={pg['schema']}",
    }


def test_postgresql_connection(settings: dict[str, Any]) -> dict[str, Any]:
    settings = sanitize_settings(settings)
    pg = settings["postgresql"]

    conn = _pg_connect(pg)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    finally:
        conn.close()

    return {
        "ok": True,
        "message": f"PostgreSQL connection OK: {pg['host']}:{pg['port']}/{pg['database']} schema={pg['schema']}",
    }


def ingest_artifacts(
    settings: dict[str, Any],
    run_id: str,
    script_name: str,
    status: str,
    started_at: float | None,
    finished_at: float | None,
    return_code: int | None,
    artifacts: list[str],
) -> IngestResult:
    cfg = sanitize_settings(settings)
    dest = cfg["destinationType"]
    logs: list[str] = []

    if dest == "geopackage":
        gpkg_path = Path(cfg["geopackage"]["path"]).expanduser()
        binding_mode = str(cfg.get("gpkgBindingMode", "direct"))
        _ensure_gpkg_parent(gpkg_path)
        with sqlite3.connect(str(gpkg_path)) as conn:
            _ensure_sqlite_audit(conn)
            conn.execute(
                "INSERT OR REPLACE INTO dgim_job_audit (run_id, script_name, status, started_at, finished_at, return_code, destination_type, artifacts_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    script_name,
                    status,
                    started_at,
                    finished_at,
                    return_code,
                    "geopackage",
                    json.dumps(artifacts, ensure_ascii=False),
                ),
            )

            for raw_path in artifacts:
                artifact = Path(raw_path)
                if not artifact.exists() or not artifact.is_file():
                    continue
                suffix = artifact.suffix.lower()
                if suffix == ".csv":
                    logs.append(_import_csv_to_sqlite(conn, artifact, cfg["tablePrefix"], cfg["writeMode"], run_id))
                elif suffix == ".xml":
                    logs.append(_import_xml_to_sqlite(conn, artifact, cfg["tablePrefix"], cfg["writeMode"], run_id))
                elif suffix == ".gpkg":
                    if binding_mode == "direct" and artifact.resolve() == gpkg_path.resolve():
                        conn.execute(
                            "INSERT INTO dgim_artifact_audit (run_id, artifact_path, object_name, rows_written, note) VALUES (?, ?, ?, ?, ?)",
                            (run_id, str(artifact), str(gpkg_path), 0, "gpkg-direct-binding"),
                        )
                        logs.append(
                            "[db-target] Direct GeoPackage binding active: output already written to central DB file"
                        )
                        continue
                    logs.extend(
                        _copy_gpkg_tables_to_sqlite(conn, artifact, cfg["tablePrefix"], cfg["writeMode"], run_id)
                    )
            conn.commit()

        return IngestResult(ok=True, logs=logs)

    if dest == "duckdb":
        duckdb_path = Path(cfg["duckdb"]["path"]).expanduser()
        _ensure_duckdb_parent(duckdb_path)
        duckdb = _get_duckdb_module()
        with duckdb.connect(str(duckdb_path)) as conn:
            _ensure_duckdb_audit(conn)
            conn.execute(
                """
                INSERT INTO dgim_job_audit (
                    run_id,
                    script_name,
                    status,
                    started_at,
                    finished_at,
                    return_code,
                    destination_type,
                    artifacts_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    status=excluded.status,
                    finished_at=excluded.finished_at,
                    return_code=excluded.return_code,
                    artifacts_json=excluded.artifacts_json,
                    destination_type=excluded.destination_type
                """,
                [
                    run_id,
                    script_name,
                    status,
                    started_at,
                    finished_at,
                    return_code,
                    "duckdb",
                    json.dumps(artifacts, ensure_ascii=False),
                ],
            )

            for raw_path in artifacts:
                artifact = Path(raw_path)
                if not artifact.exists() or not artifact.is_file():
                    continue
                suffix = artifact.suffix.lower()
                if suffix == ".csv":
                    logs.append(
                        _import_csv_to_duckdb(
                            conn,
                            artifact,
                            cfg["tablePrefix"],
                            cfg["writeMode"],
                            run_id,
                        )
                    )
                elif suffix == ".xml":
                    logs.append(
                        _import_xml_to_duckdb(
                            conn,
                            artifact,
                            cfg["tablePrefix"],
                            cfg["writeMode"],
                            run_id,
                        )
                    )
                elif suffix == ".gpkg":
                    logs.extend(
                        _copy_gpkg_tables_to_duckdb(
                            conn,
                            artifact,
                            cfg["tablePrefix"],
                            cfg["writeMode"],
                            run_id,
                        )
                    )

        return IngestResult(ok=True, logs=logs)

    pg = cfg["postgresql"]
    conn = _pg_connect(pg)
    try:
        _ensure_pg_schema_and_audit(conn, pg["schema"])
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {_quote_ident(pg['schema'])}.dgim_job_audit (run_id, script_name, status, started_at, finished_at, return_code, destination_type, artifacts_json) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (run_id) DO UPDATE SET status = EXCLUDED.status, finished_at = EXCLUDED.finished_at, return_code = EXCLUDED.return_code, artifacts_json = EXCLUDED.artifacts_json",
                (
                    run_id,
                    script_name,
                    status,
                    started_at,
                    finished_at,
                    return_code,
                    "postgresql",
                    json.dumps(artifacts, ensure_ascii=False),
                ),
            )
        conn.commit()

        for raw_path in artifacts:
            artifact = Path(raw_path)
            if not artifact.exists() or not artifact.is_file():
                continue
            suffix = artifact.suffix.lower()
            if suffix == ".csv":
                logs.append(_import_csv_to_postgres(conn, pg["schema"], artifact, cfg["tablePrefix"], cfg["writeMode"], run_id))
            elif suffix == ".xml":
                logs.append(_import_xml_to_postgres(conn, pg["schema"], artifact, cfg["tablePrefix"], cfg["writeMode"], run_id))
            elif suffix == ".gpkg":
                logs.extend(
                    _import_gpkg_to_postgres(pg, pg["schema"], artifact, cfg["tablePrefix"], cfg["writeMode"], run_id)
                )

        return IngestResult(ok=True, logs=logs)
    finally:
        conn.close()
