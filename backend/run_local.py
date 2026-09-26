#!/usr/bin/env python3
"""Local backend server for ili2dgim web application"""
from __future__ import annotations

import argparse
import datetime as dt
from email.parser import BytesParser
from email.policy import default
import json
import os
import re
import sqlite3
import subprocess
import tempfile
import threading
import traceback
import uuid
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qs, unquote, urlparse

try:
    from app.db_target import load_settings, save_settings, sanitize_settings, test_postgresql_connection
except ImportError:
    def load_settings():
        return {}

    def save_settings(payload: dict):
        return payload

    def sanitize_settings(payload: dict):
        return payload

    def test_postgresql_connection(payload: dict):
        return {"ok": False, "message": "Backend database helpers unavailable"}

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = WORKSPACE_ROOT / "scripts"
PYTHON_EXE = r"C:\Program Files\QGIS 3.40.7\apps\Python312\python.exe"
DEFAULT_PROXY = "http://proxy-bvcol.admin.ch:8080"
LEGACY_DEFAULT_PROXY = "http://prp01.adb.intra.admin.ch:8080"
BASELINE_META_FILE = ".dgif_baseline.json"

# Configure proxy
os.environ.setdefault("HTTP_PROXY", DEFAULT_PROXY)
os.environ.setdefault("HTTPS_PROXY", DEFAULT_PROXY)


def build_process_env():
    """Build environment for subprocess execution"""
    settings = load_settings() or {}
    execution = settings.get("execution") or {}
    env = dict(os.environ)
    configured_proxy = str(execution.get("proxyUrl", "")).strip()
    inherited_proxy = (
        str(os.environ.get("HTTPS_PROXY", "")).strip()
        or str(os.environ.get("HTTP_PROXY", "")).strip()
    )
    proxy_url = configured_proxy or inherited_proxy or DEFAULT_PROXY
    if proxy_url == LEGACY_DEFAULT_PROXY:
        proxy_url = DEFAULT_PROXY
    log_level = str(execution.get("logLevel", "INFO")).strip().upper() or "INFO"
    raw_tools = execution.get("interlisTools")
    interlis_tools = raw_tools if isinstance(raw_tools, dict) else {}

    env["HTTP_PROXY"] = proxy_url
    env["HTTPS_PROXY"] = proxy_url
    env["http_proxy"] = proxy_url
    env["https_proxy"] = proxy_url
    env["DGIM_LOG_LEVEL"] = log_level
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"

    tool_env_names = {
        "ili2c": ["ILI2C_JAR"],
        "ili2validator": ["ILIVALIDATOR_JAR", "ILI2VALIDATOR_JAR"],
        "ili2gpkg": ["ILI2GPKG_JAR"],
        "ili2duckdb": ["ILI2DUCKDB_JAR"],
        "ili2pg": ["ILI2PG_JAR"],
        "ili2imd": ["ILI2IMD_JAR"],
    }
    for tool_name, env_names in tool_env_names.items():
        tool_path = str(interlis_tools.get(tool_name, "")).strip()
        if tool_path:
            for env_name in env_names:
                env[env_name] = tool_path

    java_proxy_args = _java_proxy_args(env)
    if java_proxy_args:
        java_proxy_options = [
            *java_proxy_args,
            "-Djava.net.useSystemProxies=true",
        ]
        existing_tool_options = str(env.get("JAVA_TOOL_OPTIONS", "")).strip()
        extra_tool_options = " ".join(java_proxy_options)
        env["JAVA_TOOL_OPTIONS"] = (
            f"{existing_tool_options} {extra_tool_options}".strip()
            if existing_tool_options
            else extra_tool_options
        )
        existing_java_options = str(env.get("_JAVA_OPTIONS", "")).strip()
        env["_JAVA_OPTIONS"] = (
            f"{existing_java_options} {extra_tool_options}".strip()
            if existing_java_options
            else extra_tool_options
        )
    return env


@dataclass
class JobState:
    lock: threading.Lock = field(default_factory=threading.Lock)
    logs: list[str] = field(default_factory=list)
    runtime_label: str = ""
    runtime_phase: str = "idle"
    runtime_running: bool = False
    runtime_total_steps: int = 0
    runtime_completed_steps: int = 0
    runtime_progress: int = 0


JOB = JobState()


def set_runtime_progress(percent: int, *, phase: str | None = None) -> None:
    with JOB.lock:
        JOB.runtime_progress = max(0, min(100, int(percent)))
        if phase is not None:
            JOB.runtime_phase = phase


def append_runtime_log(line: str) -> None:
    """Append log line to job logs"""
    text = line.rstrip("\n")
    if not text:
        return
    match = re.search(r"(\d+(?:\.\d+)?)\s*%", text)
    if match:
        set_runtime_progress(float(match.group(1)))
    with JOB.lock:
        JOB.logs.append(text)
    print(text, flush=True)


def begin_runtime(label: str, total_steps: int) -> None:
    with JOB.lock:
        JOB.logs = []
        JOB.runtime_label = label
        JOB.runtime_phase = "starting"
        JOB.runtime_running = True
        JOB.runtime_total_steps = max(1, total_steps)
        JOB.runtime_completed_steps = 0
        JOB.runtime_progress = 0


def set_runtime_phase(phase: str) -> None:
    with JOB.lock:
        JOB.runtime_phase = phase


def advance_runtime(steps: int = 1) -> None:
    with JOB.lock:
        JOB.runtime_completed_steps = min(
            JOB.runtime_total_steps,
            JOB.runtime_completed_steps + steps,
        )


def end_runtime() -> None:
    with JOB.lock:
        if JOB.runtime_total_steps > 0:
            JOB.runtime_completed_steps = JOB.runtime_total_steps
        JOB.runtime_phase = "completed"
        JOB.runtime_running = False
        JOB.runtime_progress = 100


def fail_runtime() -> None:
    with JOB.lock:
        JOB.runtime_phase = "failed"
        JOB.runtime_running = False


def runtime_snapshot(offset: int) -> dict:
    with JOB.lock:
        total = max(1, JOB.runtime_total_steps)
        completed = min(total, JOB.runtime_completed_steps)
        base_progress = int((completed / total) * 100)
        progress = JOB.runtime_progress if JOB.runtime_progress > base_progress else base_progress
        new_logs = JOB.logs[offset:]
        return {
            "label": JOB.runtime_label,
            "currentPhase": JOB.runtime_phase,
            "running": JOB.runtime_running,
            "totalSteps": total,
            "completedSteps": completed,
            "progress": progress,
            "newLogs": new_logs,
            "nextOffset": offset + len(new_logs),
        }


def select_folder_dialog(initial_path: str | None = None) -> str | None:
    initial_dir = initial_path or str(WORKSPACE_ROOT)
    script = """
Add-Type -AssemblyName System.Windows.Forms
$dialog = New-Object System.Windows.Forms.FolderBrowserDialog
$dialog.ShowNewFolderButton = $true
$dialog.InitialDirectory = $args[0]
if ($dialog.ShowDialog() -eq [System.Windows.Forms.DialogResult]::OK) {
    [Console]::Out.Write($dialog.SelectedPath)
}
""".strip()
    result = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-Command",
            script,
            initial_dir,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    selected = result.stdout.strip()
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "Folder selection failed")
    return selected or None


def parse_multipart_form(
    body: bytes,
    content_type: str,
) -> tuple[dict[str, str], str | None, bytes | None]:
    text_fields: dict[str, str] = {}
    file_name: str | None = None
    file_content: bytes | None = None

    message = BytesParser(policy=default).parsebytes(
        (
            f"Content-Type: {content_type}\r\n"
            "MIME-Version: 1.0\r\n\r\n"
        ).encode("utf-8")
        + body
    )
    if not message.is_multipart():
        return text_fields, file_name, file_content

    for part in message.iter_parts():
        if part.get_content_disposition() != "form-data":
            continue

        field_name = part.get_param("name", header="content-disposition")
        if not field_name:
            continue

        content = part.get_payload(decode=True) or b""
        filename = part.get_filename()
        if filename and field_name == "file":
            file_name = os.path.basename(filename) or "baseline.xmi"
            file_content = content
            continue

        text_fields[field_name] = content.decode("utf-8", errors="replace")

    return text_fields, file_name, file_content


def resolve_baseline_paths(
    fields: dict[str, str],
    file_name: str,
) -> tuple[Path, Path, Path | None]:
    default_output_dir = WORKSPACE_ROOT / "output"
    legacy_output_dir = fields.get("outputFolder", "").strip()
    requested_xmi_folder = fields.get("xmiInputFolder", "").strip()
    requested_model_dir = fields.get("modelOutputFolder", "").strip()
    requested_catalogs_dir = fields.get("catalogsOutputFolder", "").strip()

    if requested_xmi_folder:
        xmi_dir = Path(requested_xmi_folder)
        cleanup_dir = None
    else:
        cleanup_dir = Path(tempfile.mkdtemp(prefix="ili2dgim-xmi-"))
        xmi_dir = cleanup_dir

    output_dir_value = (
        requested_model_dir
        or requested_catalogs_dir
        or legacy_output_dir
    )
    output_dir = Path(output_dir_value) if output_dir_value else default_output_dir

    xmi_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    return (
        xmi_dir / file_name,
        output_dir,
        cleanup_dir,
    )


def resolve_effective_ili_model(
    fields: dict[str, str],
    model_output_dir: Path,
) -> Path:
    model_format = fields.get("modelFormat", "xmi").strip().lower() or "xmi"
    if model_format == "xmi":
        return model_output_dir / "DGIF_V3.ili"

    configured_model = model_output_dir / "DGIF_V3.ili"
    if configured_model.exists():
        return configured_model

    workspace_model = WORKSPACE_ROOT / "models" / "DGIF_V3.ili"
    if workspace_model.exists():
        return workspace_model

    raise RuntimeError(
        "modelFormat=ili requires an existing DGIF_V3.ili in the "
        "configured model output folder or workspace models directory"
    )


def resolve_mapping_ili_model(req: dict) -> Path:
    raw_ili_path = str(req.get("iliModelPath", "")).strip()
    if raw_ili_path:
        candidate = Path(raw_ili_path)
        if candidate.exists():
            return candidate

    requested_model_dir = (
        str(req.get("modelOutputFolder", "")).strip()
        or str(req.get("genericOutputFolder", "")).strip()
        or str(req.get("outputFolder", "")).strip()
    )
    configured_model = (
        Path(requested_model_dir) / "DGIF_V3.ili"
        if requested_model_dir
        else None
    )

    direct_candidates = [
        configured_model,
        WORKSPACE_ROOT / "output" / "DGIF_V3.ili",
        WORKSPACE_ROOT / "models" / "DGIF_V3.ili",
    ]
    for candidate in direct_candidates:
        if candidate and candidate.exists():
            return candidate

    versioned_candidates: list[Path] = []
    for parent in [WORKSPACE_ROOT / "output", WORKSPACE_ROOT / "models"]:
        if parent.exists():
            versioned_candidates.extend(parent.glob("DGIF_V3_*.ili"))
    if versioned_candidates:
        return max(versioned_candidates, key=lambda path: path.stat().st_mtime)

    raise RuntimeError(
        "No DGIF model found for mapping generation. Load a baseline first "
        "or provide a valid iliModelPath."
    )


def _resolve_interlis_tool_jar(tool_name: str, env: dict[str, str]) -> Path | None:
    env_map = {
        "ili2gpkg": ["ILI2GPKG_JAR"],
        "ili2duckdb": ["ILI2DUCKDB_JAR"],
    }
    for env_name in env_map.get(tool_name, []):
        configured = str(env.get(env_name, "")).strip()
        if configured:
            return Path(configured).expanduser()

    settings = load_settings() or {}
    execution = settings.get("execution") or {}
    interlis_tools = execution.get("interlisTools") or {}
    configured_tool = str(interlis_tools.get(tool_name, "")).strip()
    if configured_tool:
        return Path(configured_tool).expanduser()

    defaults = {
        "ili2gpkg": WORKSPACE_ROOT / "ressources" / "ili2gpkg-5.3.1" / "ili2gpkg-5.3.1.jar",
        "ili2duckdb": WORKSPACE_ROOT / "ressources" / "ili2duckdb-5.5.2" / "ili2duckdb-5.5.2.jar",
    }
    candidate = defaults.get(tool_name)
    if candidate and candidate.exists():
        return candidate
    return None


def _run_subprocess_with_logs(
    cmd: list[str],
    *,
    env: dict[str, str],
    log_prefix: str,
    log: Callable[[str], None] | None,
    timeout_s: int,
) -> None:
    proc = subprocess.Popen(
        cmd,
        cwd=str(WORKSPACE_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    assert proc.stdout is not None
    for raw_line in proc.stdout:
        line = raw_line.rstrip("\r\n")
        if not line:
            continue
        if log:
            log(f"{log_prefix}{line}")
    try:
        proc.wait(timeout=timeout_s)
    except subprocess.TimeoutExpired as exc:
        proc.kill()
        raise RuntimeError(
            f"Command timed out after {timeout_s}s: {' '.join(cmd)}"
        ) from exc
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}")


def _java_proxy_args(env: dict[str, str]) -> list[str]:
    proxy_value = (
        str(env.get("HTTPS_PROXY", "")).strip()
        or str(env.get("HTTP_PROXY", "")).strip()
    )
    if not proxy_value:
        return []

    parsed = urlparse(proxy_value)
    host = parsed.hostname
    port = parsed.port
    if not host or not port:
        return []

    return [
        f"-Dhttp.proxyHost={host}",
        f"-Dhttp.proxyPort={port}",
        f"-Dhttps.proxyHost={host}",
        f"-Dhttps.proxyPort={port}",
    ]


def _convert_gpkg_to_duckdb_via_interlis(
    source_gpkg: Path,
    target_duckdb: Path,
    *,
    env: dict[str, str],
    log: Callable[[str], None] | None,
) -> None:
    ili2gpkg_jar = _resolve_interlis_tool_jar("ili2gpkg", env)
    ili2duckdb_jar = _resolve_interlis_tool_jar("ili2duckdb", env)

    if ili2gpkg_jar is None or not ili2gpkg_jar.exists():
        raise RuntimeError(
            "DuckDB fallback requires ili2gpkg JAR. Configure it in Tab 0 > INTERLIS tools."
        )
    if ili2duckdb_jar is None or not ili2duckdb_jar.exists():
        raise RuntimeError(
            "DuckDB fallback requires ili2duckdb JAR. Configure it in Tab 0 > INTERLIS tools."
        )

    target_duckdb.parent.mkdir(parents=True, exist_ok=True)
    if target_duckdb.exists():
        target_duckdb.unlink(missing_ok=True)

    def infer_models_from_baskets(gpkg_path: Path) -> list[str]:
        models: list[str] = []
        conn = sqlite3.connect(str(gpkg_path))
        try:
            cur = conn.cursor()
            cur.execute("SELECT topic FROM T_ILI2DB_BASKET")
            for (topic,) in cur.fetchall():
                topic_text = str(topic or "").strip()
                if not topic_text or "." not in topic_text:
                    continue
                model_name = topic_text.split(".", 1)[0]
                if model_name and model_name not in models:
                    models.append(model_name)
        except sqlite3.Error:
            return []
        finally:
            conn.close()
        return models

    model_names = infer_models_from_baskets(source_gpkg)
    timeout_s = int(os.environ.get("DUCKDB_FALLBACK_TIMEOUT_S", "600"))
    xtf_path = Path(tempfile.gettempdir()) / f"duckdb_fallback_{uuid.uuid4()}.xtf"
    try:
        java_proxy_args = _java_proxy_args(env)
        export_models_args: list[str] = []
        if model_names:
            export_models_args = ["--models", ";".join(model_names)]
            if log:
                log(f"[etl] Fallback export models: {', '.join(model_names)}")

        export_cmd = [
            "java",
            *java_proxy_args,
            "-jar",
            str(ili2gpkg_jar),
            "--export",
            "--dbfile",
            str(source_gpkg),
            *export_models_args,
            str(xtf_path),
        ]
        import_cmd = [
            "java",
            *java_proxy_args,
            "-jar",
            str(ili2duckdb_jar),
            "--import",
            "--doSchemaImport",
            "--dbfile",
            str(target_duckdb),
            str(xtf_path),
        ]

        _run_subprocess_with_logs(
            export_cmd,
            env=env,
            log_prefix="[ili2gpkg] ",
            log=log,
            timeout_s=timeout_s,
        )
        _run_subprocess_with_logs(
            import_cmd,
            env=env,
            log_prefix="[ili2duckdb] ",
            log=log,
            timeout_s=timeout_s,
        )
    finally:
        xtf_path.unlink(missing_ok=True)


def convert_gpkg_to_duckdb(
    source_gpkg: Path,
    target_duckdb: Path,
    *,
    env: dict[str, str] | None = None,
    log: Callable[[str], None] | None = None,
) -> None:
    process_env = env or build_process_env()
    target_duckdb.parent.mkdir(parents=True, exist_ok=True)
    if target_duckdb.exists():
        target_duckdb.unlink(missing_ok=True)

    gdal_error: Exception | None = None
    try:
        from osgeo import gdal

        gdal.UseExceptions()
        translated = gdal.VectorTranslate(
            str(target_duckdb),
            str(source_gpkg),
            format="DuckDB",
        )
        if translated is None:
            raise RuntimeError("DuckDB export failed")
        translated = None
        return
    except Exception as exc:  # pragma: no cover
        gdal_error = exc

    if log:
        log(f"[etl] GDAL DuckDB export failed: {gdal_error}")
        log("[etl] Falling back to INTERLIS conversion (ili2gpkg -> XTF -> ili2duckdb)")

    _convert_gpkg_to_duckdb_via_interlis(
        source_gpkg,
        target_duckdb,
        env=process_env,
        log=log,
    )


def extract_ili_topics(ili_path: Path) -> list[str]:
    topic_re = re.compile(
        r"^\s*TOPIC\s+([A-Za-z_][A-Za-z0-9_]*)\s*=",
        re.IGNORECASE,
    )
    topics: list[str] = []
    with ili_path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            match = topic_re.match(line)
            if match:
                topic = match.group(1)
                if topic not in topics:
                    topics.append(topic)
    return topics


def resolve_ili_model_for_etl(payload: dict) -> Path:
    requested = str(payload.get("iliModelPath", "")).strip()
    if requested:
        candidate = Path(requested)
        if candidate.exists() and candidate.is_file():
            return candidate

    generic_output = str(payload.get("genericOutputFolder", "")).strip()
    if generic_output:
        candidate = Path(generic_output) / "DGIF_V3.ili"
        if candidate.exists() and candidate.is_file():
            return candidate

    return resolve_mapping_ili_model(payload)


def _resolve_existing_directory(candidates: list[str | Path | None]) -> Path | None:
    for candidate in candidates:
        if candidate is None:
            continue
        text = str(candidate).strip()
        if not text:
            continue
        path = Path(text).expanduser()
        if not path.is_absolute():
            path = (WORKSPACE_ROOT / path).resolve()
        if path.exists() and path.is_dir():
            return path
    return None


def _discover_overture_data_directory() -> Path | None:
    search_roots = [
        WORKSPACE_ROOT / "ressources",
        WORKSPACE_ROOT / "output",
        WORKSPACE_ROOT,
    ]
    patterns = [
        "overture_*.parquet",
        "overture_*.geoparquet",
        "overture_*.geojson",
        "overture_*.json",
    ]

    for root in search_roots:
        if not root.exists() or not root.is_dir():
            continue
        try:
            for pattern in patterns:
                match = next(root.rglob(pattern), None)
                if match:
                    return match.parent
        except OSError:
            continue
    return None


def _contains_overture_input_files(directory: Path) -> bool:
    if not directory.exists() or not directory.is_dir():
        return False

    patterns = [
        "overture_*.parquet",
        "overture_*.geoparquet",
        "overture_*.geojson",
        "overture_*.json",
        "*.parquet",
        "*.geoparquet",
        "*.geojson",
    ]
    try:
        for pattern in patterns:
            if next(directory.rglob(pattern), None):
                return True
    except OSError:
        return False
    return False


def resolve_overture_parquet_dir(payload: dict) -> Path | None:
    settings = load_settings() or {}
    execution = settings.get("execution") or {}

    candidates = [
        payload.get("parquetDir"),
        payload.get("parquetDirectory"),
        payload.get("overtureParquetDir"),
        payload.get("sourceDataDir"),
        payload.get("dataDir"),
        execution.get("overtureParquetDir"),
        os.environ.get("OVERTURE_PARQUET_DIR"),
        "C:/tmp/overture_parquet",
    ]

    for raw_candidate in candidates:
        directory = _resolve_existing_directory([raw_candidate])
        if directory and _contains_overture_input_files(directory):
            return directory

    return _discover_overture_data_directory()


def resolve_osm_overpass_endpoint(payload: dict) -> tuple[str | None, str]:
    provider = str(payload.get("osmOverpassProvider", "") or "").strip() or "overpassTurbo"
    custom_url = str(payload.get("osmOverpassUrl", "") or "").strip()

    provider_endpoints = {
        "overpassTurbo": "https://overpass-api.de/api/interpreter",
        "swissOverpass": "https://overpass.osm.ch/api/interpreter",
    }
    if provider == "custom":
        return (custom_url or None, provider)
    return (provider_endpoints.get(provider, provider_endpoints["overpassTurbo"]), provider)


def resolve_ofm_directory(payload: dict) -> Path | None:
    settings = load_settings() or {}
    execution = settings.get("execution") or {}

    return _resolve_existing_directory(
        [
            payload.get("ofmxDir"),
            payload.get("ofmDir"),
            payload.get("sourceDataDir"),
            payload.get("dataDir"),
            execution.get("ofmxDir"),
            os.environ.get("OFMX_DIR"),
            WORKSPACE_ROOT / "ressources" / "ofmx_ls" / "embedded",
            WORKSPACE_ROOT / "ressources" / "ofmx_ls" / "isolated",
        ]
    )


VIEWER_SUPPORTED_EXTENSIONS = {
    ".gpkg": "gpkg",
    ".duckdb": "duckdb",
    ".geojson": "geojson",
}


def _resolve_viewer_output_dir(output_folder: str | None) -> Path:
    if output_folder:
        candidate = Path(output_folder).expanduser()
        if not candidate.is_absolute():
            candidate = (WORKSPACE_ROOT / candidate).resolve()
        if candidate.exists() and candidate.is_dir():
            return candidate

    settings = load_settings() or {}
    artifacts_dir = str(settings.get("artifactsDir", "") or "").strip()
    if artifacts_dir:
        candidate = Path(artifacts_dir).expanduser()
        if not candidate.is_absolute():
            candidate = (WORKSPACE_ROOT / candidate).resolve()
        if candidate.exists() and candidate.is_dir():
            return candidate

    fallback = WORKSPACE_ROOT / "output"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


def _resolve_viewer_file(path_value: str) -> Path:
    candidate = Path(path_value).expanduser()
    if not candidate.is_absolute():
        candidate = (WORKSPACE_ROOT / candidate).resolve()
    if not candidate.exists() or not candidate.is_file():
        raise FileNotFoundError(f"Viewer file not found: {candidate}")
    return candidate


def _viewer_list_files(output_folder: str | None) -> dict:
    folder = _resolve_viewer_output_dir(output_folder)
    files: list[dict] = []
    for candidate in sorted(folder.rglob("*")):
        if not candidate.is_file():
            continue
        suffix = candidate.suffix.lower()
        if suffix not in VIEWER_SUPPORTED_EXTENSIONS:
            continue
        stat = candidate.stat()
        files.append(
            {
                "name": candidate.name,
                "path": str(candidate.resolve()),
                "size": int(stat.st_size),
                "modifiedAt": dt.datetime.fromtimestamp(stat.st_mtime, tz=dt.timezone.utc).isoformat(),
                "format": VIEWER_SUPPORTED_EXTENSIONS[suffix],
            }
        )
    files.sort(key=lambda item: item.get("modifiedAt", ""), reverse=True)
    return {"folder": str(folder), "files": files}


def _viewer_list_layers(path_value: str) -> dict:
    try:
        from osgeo import ogr
        ogr.UseExceptions()
    except Exception as exc:
        raise RuntimeError(f"GDAL/OGR not available for viewer: {exc}") from exc

    dataset_path = _resolve_viewer_file(path_value)
    ds = ogr.Open(str(dataset_path), 0)
    if ds is None:
        raise RuntimeError(
            "Unable to open dataset. The required GDAL driver may be unavailable "
            f"for this format: {dataset_path.suffix.lower()}"
        )

    driver = ds.GetDriver().GetName() if ds.GetDriver() else "unknown"
    layers: list[dict] = []
    for index in range(ds.GetLayerCount()):
        layer = ds.GetLayerByIndex(index)
        if layer is None:
            continue
        try:
            feature_count = int(layer.GetFeatureCount())
        except Exception:
            feature_count = -1
        try:
            if feature_count <= 0:
                extent_value = None
            else:
                extent = layer.GetExtent(force=1)
                extent_value = [float(extent[0]), float(extent[2]), float(extent[1]), float(extent[3])] if extent else None
        except Exception:
            extent_value = None
        srs_obj = layer.GetSpatialRef()
        srs_text = ""
        if srs_obj is not None:
            authority_name = srs_obj.GetAuthorityName(None)
            authority_code = srs_obj.GetAuthorityCode(None)
            if authority_name and authority_code:
                srs_text = f"{authority_name}:{authority_code}"

        layers.append(
            {
                "name": layer.GetName(),
                "geometryType": ogr.GeometryTypeToName(layer.GetGeomType()) or "Unknown",
                "featureCount": feature_count,
                "extent": extent_value,
                "srs": srs_text,
            }
        )

    ds = None
    return {"path": str(dataset_path), "driver": driver, "layers": layers}


def _viewer_layer_geojson(path_value: str, layer_name: str | None, limit: int = 2000) -> dict:
    try:
        from osgeo import ogr, osr
        ogr.UseExceptions()
    except Exception as exc:
        raise RuntimeError(f"GDAL/OGR not available for viewer: {exc}") from exc

    dataset_path = _resolve_viewer_file(path_value)
    ds = ogr.Open(str(dataset_path), 0)
    if ds is None:
        raise RuntimeError(
            "Unable to open dataset. The required GDAL driver may be unavailable "
            f"for this format: {dataset_path.suffix.lower()}"
        )

    layer = ds.GetLayerByName(layer_name) if layer_name else ds.GetLayerByIndex(0)
    if layer is None:
        ds = None
        raise RuntimeError(f"Layer not found: {layer_name or '<default>'}")

    target_srs = osr.SpatialReference()
    target_srs.ImportFromEPSG(4326)
    source_srs = layer.GetSpatialRef()
    transformer = None
    if source_srs is not None and not source_srs.IsSame(target_srs):
        transformer = osr.CoordinateTransformation(source_srs, target_srs)

    layer_defn = layer.GetLayerDefn()
    field_names = [layer_defn.GetFieldDefn(i).GetName() for i in range(layer_defn.GetFieldCount())]

    features: list[dict] = []
    layer.ResetReading()
    count = 0
    minx = miny = maxx = maxy = None
    truncated = False
    max_features = max(1, min(int(limit), 10000))

    feat = layer.GetNextFeature()
    while feat is not None:
        if count >= max_features:
            truncated = True
            break

        geom_ref = feat.GetGeometryRef()
        geom_json = None
        if geom_ref is not None:
            geom = geom_ref.Clone()
            if transformer is not None:
                try:
                    geom.Transform(transformer)
                except Exception:
                    pass
            try:
                env = geom.GetEnvelope()
                gx_min, gx_max, gy_min, gy_max = float(env[0]), float(env[1]), float(env[2]), float(env[3])
                minx = gx_min if minx is None else min(minx, gx_min)
                miny = gy_min if miny is None else min(miny, gy_min)
                maxx = gx_max if maxx is None else max(maxx, gx_max)
                maxy = gy_max if maxy is None else max(maxy, gy_max)
            except Exception:
                pass
            geom_json = json.loads(geom.ExportToJson())

        properties = {field_name: feat.GetField(field_name) for field_name in field_names}
        features.append(
            {
                "type": "Feature",
                "geometry": geom_json,
                "properties": properties,
            }
        )
        count += 1
        feat = layer.GetNextFeature()

    try:
        total_features = int(layer.GetFeatureCount())
    except Exception:
        total_features = count

    bbox = None
    if minx is not None and miny is not None and maxx is not None and maxy is not None:
        bbox = [minx, miny, maxx, maxy]

    driver = ds.GetDriver().GetName() if ds.GetDriver() else "unknown"
    selected_layer_name = layer.GetName()
    ds = None
    return {
        "path": str(dataset_path),
        "layerName": selected_layer_name,
        "driver": driver,
        "totalFeatures": total_features,
        "featuresReturned": count,
        "truncated": truncated,
        "bbox": bbox,
        "geojson": {
            "type": "FeatureCollection",
            "features": features,
        },
    }


def script_supports_flag(script_path: Path, flag: str) -> bool:
    """Best-effort check if a script exposes a CLI flag in --help."""
    try:
        probe = subprocess.run(
            [PYTHON_EXE, "-X", "utf8", str(script_path), "--help"],
            cwd=str(WORKSPACE_ROOT),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=20,
            env=build_process_env(),
        )
    except Exception:
        return False

    help_text = f"{probe.stdout}\n{probe.stderr}"
    return flag in help_text


def extract_baseline_version(source_name: str) -> str | None:
    match = re.search(r"(20\d{2}-\d+)", source_name)
    return match.group(1) if match else None


def extract_baseline_version_from_path(path_value: str | None) -> str | None:
    if not path_value:
        return None
    file_name = Path(path_value).name
    return extract_baseline_version(file_name)


def build_baseline_label(version: str | None) -> str:
    if version:
        return f"DGIF V3 Baseline {version}"
    return "DGIF V3 Baseline"


def write_baseline_metadata(
    output_dir: Path,
    source_xmi: str,
    version: str | None,
) -> None:
    metadata = {
        "sourceXmi": source_xmi,
        "baselineVersion": version,
        "baselineLabel": build_baseline_label(version),
        "modelOutputFolder": str(output_dir),
    }
    (output_dir / BASELINE_META_FILE).write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def detect_existing_baseline(fields: dict[str, str]) -> dict[str, object]:
    default_output_dir = WORKSPACE_ROOT / "output"
    legacy_output_dir = fields.get("outputFolder", "").strip()
    requested_model_dir = fields.get("modelOutputFolder", "").strip()
    requested_catalogs_dir = fields.get("catalogsOutputFolder", "").strip()

    output_dir_value = (
        requested_model_dir
        or requested_catalogs_dir
        or legacy_output_dir
    )
    output_dir = Path(output_dir_value) if output_dir_value else default_output_dir

    ili_model_path = output_dir / "DGIF_V3.ili"
    dgfcd_catalog_path = output_dir / "DGFCD_AttributeConcepts.xml"
    dgrwi_catalog_path = output_dir / "DGRWI_RealWorldObjects.xml"

    existing = {
        "iliModel": str(ili_model_path) if ili_model_path.exists() else None,
        "dgfcdCatalogs": (
            str(dgfcd_catalog_path) if dgfcd_catalog_path.exists() else None
        ),
        "dgrwiCatalogs": (
            str(dgrwi_catalog_path) if dgrwi_catalog_path.exists() else None
        ),
    }
    baseline_active = all(existing.values())
    missing = [key for key, value in existing.items() if not value]
    baseline_version: str | None = None

    metadata_path = output_dir / BASELINE_META_FILE
    if metadata_path.exists():
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            raw_version = metadata.get("baselineVersion")
            if isinstance(raw_version, str) and raw_version.strip():
                baseline_version = raw_version.strip()
            elif isinstance(metadata.get("sourceXmi"), str):
                baseline_version = extract_baseline_version(
                    metadata["sourceXmi"]
                )
        except (OSError, json.JSONDecodeError):
            baseline_version = None

    if not baseline_version:
        baseline_version = extract_baseline_version_from_path(existing["iliModel"])

    baseline_label = build_baseline_label(baseline_version)

    return {
        "baselineActive": baseline_active,
        **existing,
        "baselineVersion": baseline_version,
        "baselineLabel": baseline_label,
        "modelOutputFolder": str(output_dir),
        "missing": missing,
    }


class Handler(BaseHTTPRequestHandler):
    """HTTP request handler"""
    
    def _json(self, payload: dict | list, status: int = 200) -> None:
        """Send JSON response"""
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, file_path: Path, content_type: str | None = None, as_download: bool = False) -> None:
        """Send file response"""
        data = file_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type or "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        if as_download:
            self.send_header("Content-Disposition", f'attachment; filename="{file_path.name}"')
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(data)

    def do_OPTIONS(self) -> None:
        """Handle OPTIONS requests"""
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:
        """Handle GET requests"""
        parsed = urlparse(self.path)
        path = parsed.path

        # Frontend static files
        if path == "/" or path.startswith("/src/"):
            # Try built webapp first
            webapp_dist = WORKSPACE_ROOT / "webapp" / "dist" / "index.html"
            if webapp_dist.exists():
                self._send_file(webapp_dist, "text/html; charset=utf-8")
                return
            # Fallback to static index.html in backend
            static_index = Path(__file__).parent / "static" / "index.html"
            if static_index.exists():
                self._send_file(static_index, "text/html; charset=utf-8")
                return
            self._json({"detail": "Frontend not found"}, 404)
            return

        # Static assets from dist or static folder
        if path.startswith("/assets/") or path.startswith("/"):
            # Try webapp dist first
            candidate = (WORKSPACE_ROOT / "webapp" / "dist" / path.lstrip("/")).resolve()
            if candidate.exists() and candidate.is_file():
                ctype = "text/plain"
                if candidate.suffix == ".css":
                    ctype = "text/css"
                elif candidate.suffix == ".js":
                    ctype = "application/javascript"
                self._send_file(candidate, ctype)
                return
            
            # Try backend static folder
            static_candidate = (Path(__file__).parent / "static" / path.lstrip("/")).resolve()
            if static_candidate.exists() and static_candidate.is_file():
                ctype = "text/plain"
                if static_candidate.suffix == ".css":
                    ctype = "text/css"
                elif static_candidate.suffix == ".js":
                    ctype = "application/javascript"
                elif static_candidate.suffix == ".html":
                    ctype = "text/html; charset=utf-8"
                self._send_file(static_candidate, ctype)
                return
            
            # Skip 404 for unknown paths, let them fallback to not-found
            pass

        # API health
        if path == "/api/health":
            self._json({"status": "ok"})
            return

        if path == "/api/settings/db":
            self._json(load_settings() or {})
            return

        if path == "/api/settings/db/postgresql/test":
            self._json(test_postgresql_connection({}))
            return

        if path == "/api/runtime/status":
            qs = parse_qs(parsed.query)
            try:
                offset = int((qs.get("offset") or ["0"])[0])
            except ValueError:
                offset = 0
            self._json(runtime_snapshot(max(0, offset)))
            return

        if path == "/api/viewer/files":
            qs = parse_qs(parsed.query)
            output_folder = (
                (qs.get("outputFolder") or [""])[0].strip()
                or (qs.get("modelOutputFolder") or [""])[0].strip()
            )
            self._json(_viewer_list_files(output_folder or None))
            return

        if path == "/api/viewer/layers":
            qs = parse_qs(parsed.query)
            path_value = (qs.get("path") or [""])[0].strip()
            if not path_value:
                self._json({"detail": "Missing parameter: path"}, 400)
                return
            try:
                self._json(_viewer_list_layers(path_value))
            except FileNotFoundError as exc:
                self._json({"detail": str(exc)}, 404)
            except Exception as exc:
                self._json({"detail": str(exc)}, 400)
            return

        if path == "/api/viewer/layer-geojson":
            qs = parse_qs(parsed.query)
            path_value = (qs.get("path") or [""])[0].strip()
            if not path_value:
                self._json({"detail": "Missing parameter: path"}, 400)
                return
            layer_name = (qs.get("layer") or [""])[0].strip() or None
            try:
                limit = int((qs.get("limit") or ["2000"])[0])
            except ValueError:
                limit = 2000
            try:
                self._json(_viewer_layer_geojson(path_value, layer_name, limit=limit))
            except FileNotFoundError as exc:
                self._json({"detail": str(exc)}, 404)
            except Exception as exc:
                self._json({"detail": str(exc)}, 400)
            return

        # File download
        if path == "/api/files/download":
            qs = parse_qs(parsed.query)
            file_path = (qs.get("path") or [""])[0]
            if file_path:
                candidate = Path(file_path)
                if candidate.exists() and candidate.is_file():
                    self._send_file(candidate, as_download=True)
                    return
            self._json({"detail": "File not found"}, 404)
            return

        self._json({"detail": "Not found"}, 404)

    def do_POST(self) -> None:
        """Handle POST requests"""
        parsed = urlparse(self.path)
        path = parsed.path
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length > 0 else b"{}"
        content_type = self.headers.get("Content-Type", "")
        
        # Handle JSON requests
        req = {}
        if not content_type.startswith("multipart/form-data"):
            try:
                req = json.loads(body.decode("utf-8"))
            except json.JSONDecodeError:
                req = {}

        # Baseline upload (multipart/form-data with binary XMI file)
        if path == "/api/baseline/upload":
            self._handle_baseline_upload(body, content_type)
            return

        # Mappings generation
        if path == "/api/mappings/generate":
            self._handle_mappings_generate(req)
            return

        # Mappings download
        if path == "/api/mappings/download":
            self._handle_mappings_download(req)
            return

        # ETL start
        if path == "/api/etl/start":
            self._handle_etl_start(req)
            return

        if path == "/api/etl/topics":
            payload = req if isinstance(req, dict) else {}
            ili_model = resolve_ili_model_for_etl(payload)
            topics = extract_ili_topics(ili_model)
            self._json({"iliModel": str(ili_model), "topics": topics})
            return

        # ETL stop
        if path == "/api/etl/stop":
            self._handle_etl_stop()
            return

        if path == "/api/system/select-folder":
            initial_path = req.get("initialPath") if isinstance(req, dict) else None
            self._json({"path": select_folder_dialog(initial_path)})
            return

        if path == "/api/settings/db":
            payload = req if isinstance(req, dict) else {}
            saved = save_settings(payload)
            self._json({"saved": True, "settings": saved})
            return

        if path == "/api/settings/db/postgresql/test":
            payload = req if isinstance(req, dict) else {}
            self._json(test_postgresql_connection(payload))
            return

        if path == "/api/baseline/check":
            payload = req if isinstance(req, dict) else {}
            self._json(detect_existing_baseline(payload))
            return

        self._json({"detail": "Not found"}, 404)

    def _handle_baseline_upload(self, body: bytes, content_type: str) -> None:
        """Handle XMI baseline upload"""
        cleanup_dir: Path | None = None
        try:
            begin_runtime("baseline", 3)
            set_runtime_phase("validating upload")
            length = len(body)
            if length == 0:
                fail_runtime()
                self._json({"detail": "No file uploaded"}, 400)
                return

            # Parse multipart form data
            if not content_type.startswith("multipart/form-data"):
                fail_runtime()
                self._json({"detail": "Expected multipart/form-data"}, 400)
                return

            fields, file_name, xmi_file_content = parse_multipart_form(body, content_type)
            if not xmi_file_content or not file_name:
                fail_runtime()
                self._json({"detail": "No XMI file found"}, 400)
                return

            xmi_path, output_dir, cleanup_dir = resolve_baseline_paths(
                fields,
                file_name,
            )
            set_runtime_phase("writing uploaded xmi")
            xmi_path.write_bytes(xmi_file_content)
            append_runtime_log(f"[baseline] Uploaded XMI saved to: {xmi_path}")
            advance_runtime()

            cmd = [
                PYTHON_EXE,
                "-X",
                "utf8",
                str(SCRIPTS_DIR / "extract_dgfcd_dgrwi_catalogs.py"),
                "--xmi-path",
                str(xmi_path),
                "--output-dir",
                str(output_dir),
            ]
            set_runtime_phase("extracting catalogs")
            append_runtime_log(f"[baseline] Running: {' '.join(cmd)}")
            result = subprocess.run(
                cmd,
                cwd=str(WORKSPACE_ROOT),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=300,
                env=build_process_env(),
            )
            if result.returncode != 0:
                raise RuntimeError(result.stderr.strip() or "Catalog extraction failed")
            if result.stdout:
                append_runtime_log(result.stdout)
            advance_runtime()

            model_format = fields.get("modelFormat", "xmi").strip().lower() or "xmi"
            if model_format == "xmi":
                cmd = [
                    PYTHON_EXE,
                    "-X",
                    "utf8",
                    str(SCRIPTS_DIR / "generate_ili_model.py"),
                    "--xmi-path",
                    str(xmi_path),
                    "--output-dir",
                    str(output_dir),
                ]
                set_runtime_phase("generating interlis model")
                append_runtime_log(f"[baseline] Running: {' '.join(cmd)}")
                result = subprocess.run(
                    cmd,
                    cwd=str(WORKSPACE_ROOT),
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=300,
                    env=build_process_env(),
                )
                if result.returncode != 0:
                    raise RuntimeError(result.stderr.strip() or "INTERLIS model generation failed")
                if result.stdout:
                    append_runtime_log(result.stdout)
            else:
                append_runtime_log(
                    "[baseline] modelFormat=ili, reusing existing DGIF_V3.ili instead of generating it from XMI"
                )
            advance_runtime()

            ili_model_path = resolve_effective_ili_model(fields, output_dir)
            dgfcd_catalog_path = output_dir / "DGFCD_AttributeConcepts.xml"
            dgrwi_catalog_path = output_dir / "DGRWI_RealWorldObjects.xml"

            set_runtime_phase("verifying outputs")

            if not ili_model_path.exists():
                raise RuntimeError(f"Expected model output was not written: {ili_model_path}")
            if not dgfcd_catalog_path.exists():
                raise RuntimeError(f"Expected catalog output was not written: {dgfcd_catalog_path}")
            if not dgrwi_catalog_path.exists():
                raise RuntimeError(f"Expected catalog output was not written: {dgrwi_catalog_path}")

            baseline_version = extract_baseline_version(file_name)
            write_baseline_metadata(
                output_dir=output_dir,
                source_xmi=file_name,
                version=baseline_version,
            )

            end_runtime()
            self._json({
                "iliModel": str(ili_model_path),
                "dgfcdCatalogs": str(dgfcd_catalog_path),
                "dgrwiCatalogs": str(dgrwi_catalog_path),
                "baselineVersion": baseline_version,
                "baselineLabel": build_baseline_label(baseline_version),
            })

        except subprocess.TimeoutExpired:
            fail_runtime()
            self._json({"detail": "Timeout"}, 504)
        except Exception as exc:
            append_runtime_log(f"[baseline-error] {exc}")
            fail_runtime()
            self._json({"detail": str(exc)}, 500)
        finally:
            if cleanup_dir and cleanup_dir.exists():
                for candidate in cleanup_dir.iterdir():
                    if candidate.is_file():
                        candidate.unlink(missing_ok=True)
                cleanup_dir.rmdir()

    def _handle_mappings_generate(self, req: dict) -> None:
        """Generate mapping tables"""
        try:
            source_types = req.get("sourceTypes", [])
            if not source_types:
                self._json({"detail": "No sources specified"}, 400)
                return

            ili_model_path = resolve_mapping_ili_model(req)

            begin_runtime("mappings", len(source_types))
            set_runtime_phase("preparing mappings")
            append_runtime_log(f"[mappings] Using DGIF model: {ili_model_path}")

            mapping_tables = {}
            source_errors: dict[str, str] = {}
            script_map = {
                "swissTLM3D": "build_swisstlm3d_dgif_v3.py",
                "osm": "build_osm_dgif_v3.py",
                "overture": "build_overture_dgif_v3.py"
            }
            output_name_map = {
                "swissTLM3D": "swissTLM3D_to_DGIF_V3.csv",
                "osm": "OSM_to_DGIF_V3.csv",
                "overture": "Overture_to_DGIF_V3.csv",
            }

            requested_output_dir = str(req.get("modelOutputFolder", "")).strip()
            output_dir = (
                Path(requested_output_dir)
                if requested_output_dir
                else WORKSPACE_ROOT / "output"
            )
            output_dir.mkdir(exist_ok=True)
            append_runtime_log(f"[mappings] Output folder: {output_dir}")

            for source in source_types:
                if source not in script_map:
                    continue

                cmd = [
                    PYTHON_EXE,
                    "-X",
                    "utf8",
                    str(SCRIPTS_DIR / script_map[source]),
                    "--ili-file",
                    str(ili_model_path),
                    "--output-dir",
                    str(output_dir),
                ]
                set_runtime_phase(f"generating mapping: {source}")
                append_runtime_log(f"[mappings] Running: {' '.join(cmd)}")
                
                try:
                    result = subprocess.run(
                        cmd,
                        cwd=str(WORKSPACE_ROOT),
                        capture_output=True,
                        text=True,
                        encoding="utf-8",
                        errors="replace",
                        timeout=120,
                        env=build_process_env(),
                    )
                    if result.returncode == 0:
                        # Find generated CSV file
                        expected_name = output_name_map.get(source, "")
                        for candidate in [
                            output_dir / expected_name,
                            output_dir / f"{source.lower()}_to_DGIF_V3.csv",
                            output_dir / f"{source}_to_DGIF_V3.csv",
                            WORKSPACE_ROOT / "models" / f"{source.lower()}_to_DGIF_V3.csv",
                            WORKSPACE_ROOT / "models" / expected_name,
                        ]:
                            if not str(candidate):
                                continue
                            if candidate.exists():
                                mapping_tables[source] = str(candidate)
                                append_runtime_log(f"[mappings] Found: {candidate}")
                                break
                        if source not in mapping_tables:
                            source_errors[source] = (
                                "script exited successfully but no mapping CSV "
                                "was found in output/models"
                            )
                    else:
                        append_runtime_log(f"[mappings] {source} returned {result.returncode}")
                        stderr_tail = result.stderr.strip().splitlines()[-1:] if result.stderr else []
                        stdout_tail = result.stdout.strip().splitlines()[-1:] if result.stdout else []
                        detail = " ".join(stderr_tail or stdout_tail) or "unknown error"
                        source_errors[source] = detail
                except subprocess.TimeoutExpired:
                    append_runtime_log(f"[mappings] Timeout for {source}")
                    source_errors[source] = "timeout after 120 seconds"
                finally:
                    advance_runtime()

            if not mapping_tables:
                details = "; ".join(
                    f"{source}: {msg}" for source, msg in source_errors.items()
                ) or "No mapping CSV generated"
                raise RuntimeError(details)

            end_runtime()
            self._json({"mappingTables": mapping_tables})

        except Exception as exc:
            append_runtime_log(f"[mappings-error] {exc}")
            fail_runtime()
            self._json({"detail": str(exc)}, 500)

    def _handle_mappings_download(self, req: dict) -> None:
        """Download mapping tables as ZIP"""
        try:
            import zipfile
            import io
            
            mapping_tables = req.get("tables", {})
            zip_buffer = io.BytesIO()
            
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                for source, table_path in mapping_tables.items():
                    p = Path(table_path)
                    if p.exists():
                        zf.write(p, arcname=p.name)

            zip_buffer.seek(0)
            data = zip_buffer.getvalue()
            
            self.send_response(200)
            self.send_header("Content-Type", "application/zip")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Content-Disposition", 'attachment; filename="mapping-tables.zip"')
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(data)
        except Exception as exc:
            self._json({"detail": str(exc)}, 500)

    def _handle_etl_start(self, req: dict) -> None:
        """Start ETL process"""
        aoi_file: Path | None = None
        temporary_gpkg: Path | None = None
        try:
            source_types = req.get("sourceTypes", [])
            output_path = req.get("outputPath", "")
            aoi_geojson = req.get("aoiGeoJson")
            aoi_wkt = str(req.get("aoiWkt", "") or "").strip()
            mapping_path_raw = str(req.get("mappingPath", "") or "").strip()
            output_format = str(req.get("outputFormat", "gpkg")).strip().lower() or "gpkg"
            target_topics = req.get("targetTopics", [])
            selected_topics = [
                str(item).strip()
                for item in target_topics
                if isinstance(item, str) and str(item).strip()
            ]

            if output_format == "postgis":
                self._json(
                    {
                        "detail": (
                            "PostGIS output is not implemented yet. "
                            "Tab 0 connection parameters are pending implementation."
                        )
                    },
                    501,
                )
                return

            if not source_types or not output_path:
                self._json({"detail": "Missing parameters"}, 400)
                return

            if output_format not in {"gpkg", "duckdb"}:
                self._json({"detail": f"Unsupported outputFormat: {output_format}"}, 400)
                return

            etl_model_path = resolve_ili_model_for_etl(req)

            if not selected_topics:
                selected_topics = extract_ili_topics(etl_model_path)

            if len(source_types) != 1:
                self._json(
                    {
                        "detail": (
                            "ETL currently supports one source at a time. "
                            "Select a single source in Tab 3."
                        )
                    },
                    400,
                )
                return

            source = source_types[0]
            source_extra_args: list[str] = []
            if source == "overture":
                parquet_dir = resolve_overture_parquet_dir(req)
                if not aoi_geojson and not aoi_wkt:
                    self._json(
                        {
                            "detail": (
                                "Overture ETL requires an AOI for spatial clipping. "
                                "Provide AOI via map (aoiGeoJson) or WKT field (aoiWkt)."
                            )
                        },
                        400,
                    )
                    return
                if parquet_dir is not None:
                    source_extra_args.extend(["--parquet-dir", str(parquet_dir)])

                # Force the ETL model path for Overture; relying on --help
                # probing can miss this flag in constrained environments.
                source_extra_args.extend(["--ili-model", str(etl_model_path)])

            if source == "osm":
                overpass_endpoint, provider_name = resolve_osm_overpass_endpoint(req)
                if not aoi_geojson and not aoi_wkt:
                    self._json(
                        {
                            "detail": (
                                "OSM ETL requires an AOI for spatial clipping. "
                                "Provide AOI via map (aoiGeoJson) or WKT field (aoiWkt)."
                            )
                        },
                        400,
                    )
                    return
                if not overpass_endpoint:
                    self._json(
                        {
                            "detail": (
                                "OSM ETL requires an Overpass endpoint. "
                                "Choose Overpass Turbo, Swiss Overpass API, or provide a custom URL."
                            )
                        },
                        400,
                    )
                    return
                source_extra_args.extend(["--provider", provider_name, "--overpass-url", overpass_endpoint])

            if source in {"overture", "osm"}:
                mapping_path: Path | None = None
                if mapping_path_raw:
                    mapping_path = Path(mapping_path_raw).expanduser()
                    if not mapping_path.is_absolute():
                        mapping_path = (WORKSPACE_ROOT / mapping_path).resolve()
                else:
                    default_mapping_name = {
                        "overture": "Overture_to_DGIF_V3.csv",
                        "osm": "OSM_to_DGIF_V3.csv",
                    }[source]
                    requested_output_dir = str(req.get("modelOutputFolder", "") or "").strip()
                    candidate_dirs = [
                        Path(requested_output_dir) if requested_output_dir else None,
                        WORKSPACE_ROOT / "output",
                        WORKSPACE_ROOT / "models",
                    ]
                    for candidate_dir in candidate_dirs:
                        if candidate_dir is None:
                            continue
                        candidate = candidate_dir / default_mapping_name
                        if candidate.exists() and candidate.is_file():
                            mapping_path = candidate.resolve()
                            break

                if mapping_path is None or not mapping_path.exists() or not mapping_path.is_file():
                    self._json(
                        {
                            "detail": (
                                f"Mapping file for source '{source}' not found. "
                                "Generate mappings in Tab 2 first or pass mappingPath explicitly."
                            )
                        },
                        400,
                    )
                    return

                source_extra_args.extend(["--mapping", str(mapping_path)])

            if source == "ofm":
                ofmx_dir = resolve_ofm_directory(req)
                if ofmx_dir is None:
                    self._json(
                        {
                            "detail": (
                                "OFM ETL requires an OFMX directory (--ofmx-dir). "
                                "No valid directory found. Provide one via request field "
                                "'ofmxDir' (or 'ofmDir') or set env OFMX_DIR."
                            )
                        },
                        400,
                    )
                    return
                source_extra_args.extend(["--ofmx-dir", str(ofmx_dir)])

            begin_runtime("etl", len(source_types))
            set_runtime_phase("preparing etl")

            output_path_obj = Path(output_path)
            output_path_obj.parent.mkdir(parents=True, exist_ok=True)

            etl_gpkg_target = output_path_obj
            if output_format == "duckdb":
                temporary_gpkg = Path(tempfile.gettempdir()) / f"etl_{uuid.uuid4()}.gpkg"
                etl_gpkg_target = temporary_gpkg

            # Save AOI if provided
            if aoi_geojson:
                aoi_file = Path(tempfile.gettempdir()) / f"aoi_{uuid.uuid4()}.geojson"
                aoi_file.write_text(json.dumps(aoi_geojson))
                set_runtime_phase("writing aoi file")
                append_runtime_log(f"[etl] AOI saved to {aoi_file}")

            # Run ETL scripts
            script_map = {
                "swissTLM3D": "etl_swisstlm3d_to_dgif.py",
                "osm": "etl_osm_to_dgif.py",
                "overture": "etl_overture_to_dgif.py",
                "ofm": "etl_ofm_to_dgif.py"
            }

            if source not in script_map:
                raise RuntimeError(f"Unsupported source: {source}")

            script_path = SCRIPTS_DIR / script_map[source]
            if not script_path.exists():
                raise RuntimeError(
                    f"ETL script for source '{source}' is not available: {script_path.name}"
                )

            cmd = [
                PYTHON_EXE,
                "-X",
                "utf8",
                str(script_path),
                *source_extra_args,
                "--output-gpkg",
                str(etl_gpkg_target),
                "--target-topics",
                ",".join(selected_topics),
            ]
            supports_ili_model = source == "osm" or script_supports_flag(script_path, "--ili-model")
            supports_aoi_file = source == "osm" or script_supports_flag(script_path, "--aoi-file")
            supports_aoi_wkt = source == "osm" or script_supports_flag(script_path, "--aoi-wkt")

            if source != "overture" and supports_ili_model:
                cmd.extend(["--ili-model", str(etl_model_path)])
            if aoi_file:
                if supports_aoi_file:
                    cmd.extend(["--aoi-file", str(aoi_file)])
                else:
                    append_runtime_log(
                        f"[etl] AOI provided but '{source}' script "
                        "does not support --aoi-file; "
                        "continuing without AOI clipping"
                    )
            elif aoi_wkt:
                if supports_aoi_wkt:
                    cmd.extend(["--aoi-wkt", aoi_wkt])
                else:
                    append_runtime_log(
                        f"[etl] AOI WKT provided but '{source}' script "
                        "does not support --aoi-wkt; "
                        "continuing without AOI clipping"
                    )

            set_runtime_phase(f"running etl: {source}")
            append_runtime_log(f"[etl] Running: {' '.join(cmd)}")
            process_env = build_process_env()

            process_output: list[str] = []

            def stream_subprocess_output(proc: subprocess.Popen[str]) -> None:
                assert proc.stdout is not None
                for raw_line in proc.stdout:
                    line = raw_line.rstrip("\r\n")
                    if not line:
                        continue
                    process_output.append(line)
                    append_runtime_log(line)

            try:
                proc = subprocess.Popen(
                    cmd,
                    cwd=str(WORKSPACE_ROOT),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    env=process_env,
                )
                stream_thread = threading.Thread(
                    target=stream_subprocess_output,
                    args=(proc,),
                    daemon=True,
                )
                stream_thread.start()
                default_timeout_s = 3600 if source != "swissTLM3D" else 10800
                timeout_s = int(os.environ.get("ETL_SUBPROCESS_TIMEOUT_S", default_timeout_s))
                proc.wait(timeout=timeout_s)
                stream_thread.join(timeout=5)
                if proc.returncode != 0:
                    stdout_text = "\n".join(process_output).strip()
                    raise RuntimeError(f"{source} ETL failed:\n{stdout_text or 'unknown error'}")
                append_runtime_log(f"[etl] {source} completed")
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError(f"{source} ETL timeout") from exc
            finally:
                advance_runtime()

            if output_format == "duckdb":
                set_runtime_phase("converting to duckdb")
                append_runtime_log(f"[etl] Converting {etl_gpkg_target} to DuckDB {output_path_obj}")
                convert_gpkg_to_duckdb(
                    etl_gpkg_target,
                    output_path_obj,
                    env=process_env,
                    log=append_runtime_log,
                )
                append_runtime_log("[etl] DuckDB export completed")

            # Check if output was created
            set_runtime_phase("verifying output")
            if output_path_obj.exists():
                end_runtime()
                self._json({"outputFile": str(output_path_obj)})
            else:
                fail_runtime()
                self._json({"detail": "Output not created"}, 500)

        except Exception as exc:
            append_runtime_log(f"[etl-error] {exc}")
            fail_runtime()
            self._json({"detail": str(exc)}, 500)
        finally:
            # Cleanup
            if aoi_file and aoi_file.exists():
                aoi_file.unlink(missing_ok=True)
            if temporary_gpkg and temporary_gpkg.exists():
                temporary_gpkg.unlink(missing_ok=True)

    def _handle_etl_stop(self) -> None:
        """Stop ETL process"""
        self._json({"stopped": True})

    def log_message(self, fmt: str, *args) -> None:
        """Suppress default logging"""
        pass


def main() -> None:
    """Start the backend server"""
    parser = argparse.ArgumentParser(description="ili2dgim backend server")
    parser.add_argument("--host", default="127.0.0.1", help="Server host")
    parser.add_argument("--port", type=int, default=8000, help="Server port")
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"[backend] Starting on http://{args.host}:{args.port}")
    print(f"[backend] Workspace: {WORKSPACE_ROOT}")
    print(f"[backend] Scripts: {SCRIPTS_DIR}")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[backend] Shutting down...")
        server.server_close()


if __name__ == "__main__":
    main()
