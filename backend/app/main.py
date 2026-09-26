from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import tempfile
import threading
import time
import traceback
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .db_target import ingest_artifacts, load_settings, sanitize_settings, save_settings, test_postgresql_connection, validate_destination
from .models import FieldSpec, JobEvent, JobStartRequest, JobSummary, ScriptSpec

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = WORKSPACE_ROOT / "scripts"
OVERRIDES_PATH = WORKSPACE_ROOT / "backend" / "config" / "script_overrides.json"
STATIC_DIR = WORKSPACE_ROOT / "backend" / "static"
WEBAPP_DIST_DIR = WORKSPACE_ROOT / "webapp" / "dist"
DOCS_SITE_DIR = WORKSPACE_ROOT / "site"
PYTHON_EXE = r"C:\Program Files\QGIS 3.40.7\apps\Python312\python.exe"
DEFAULT_PROXY_URL = "http://prp01.adb.intra.admin.ch:8080"
BASELINE_META_FILE = ".dgif_baseline.json"

SCRIPT_PATTERN = re.compile(r"^(etl_.*_to_dgif|build_.*_dgif_v3|extract_.*|generate_.*)\.py$", re.IGNORECASE)
OPTION_RE = re.compile(
    r"^\s{2,}(?:-\w,\s+)?(?P<long>--[a-zA-Z0-9][a-zA-Z0-9\-]*)"
    r"(?:\s+(?P<metavar>[A-Z][A-Z0-9_\-]*))?\s{2,}(?P<help>.*)$"
)
DEFAULT_RE = re.compile(r"\(default:\s*([^\)]+)\)", re.IGNORECASE)
DEFAULT_SCRIPT_LOG_LEVEL = "INFO"


def configure_network_proxy(default_proxy: str = DEFAULT_PROXY_URL) -> None:
    """Configure proxy defaults for backend and spawned script processes."""
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        if not os.environ.get(key):
            os.environ[key] = default_proxy

    parsed = urlparse(os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or default_proxy)
    if parsed.hostname and parsed.port:
        java_opts = os.environ.get("JAVA_TOOL_OPTIONS", "")
        wanted = (
            f"-Dhttp.proxyHost={parsed.hostname} "
            f"-Dhttp.proxyPort={parsed.port} "
            f"-Dhttps.proxyHost={parsed.hostname} "
            f"-Dhttps.proxyPort={parsed.port}"
        )
        if "-Dhttp.proxyHost" not in java_opts and "-Dhttps.proxyHost" not in java_opts:
            os.environ["JAVA_TOOL_OPTIONS"] = (java_opts + " " + wanted).strip()


configure_network_proxy()


class JobState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.id = ""
        self.status: str = "idle"
        self.script_name: str | None = None
        self.command: list[str] = []
        self.started_at: float | None = None
        self.finished_at: float | None = None
        self.return_code: int | None = None
        self.logs: list[str] = []
        self.artifacts: list[str] = []
        self.process: subprocess.Popen[str] | None = None
        self.paused: bool = False
        self.last_request: dict[str, Any] = {}

    def summary(self) -> JobSummary:
        return JobSummary(
            id=self.id,
            status=self.status,
            scriptName=self.script_name,
            command=self.command,
            startedAt=self.started_at,
            finishedAt=self.finished_at,
            returnCode=self.return_code,
            artifacts=self.artifacts,
        )


app = FastAPI(
    title="ili2dgim orchestrator",
    version="0.1.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def _serve_webapp_dist(relative_path: str) -> FileResponse | None:
    if not WEBAPP_DIST_DIR.exists():
        return None
    candidate = (WEBAPP_DIST_DIR / relative_path).resolve()
    if not str(candidate).startswith(str(WEBAPP_DIST_DIR.resolve())):
        return None
    if not candidate.is_file():
        return None
    return FileResponse(candidate)


def _serve_docs_site(relative_path: str) -> FileResponse | None:
    if not DOCS_SITE_DIR.exists():
        return None
    rel = relative_path.strip("/")
    candidates: list[Path] = []

    if not rel:
        candidates.append(DOCS_SITE_DIR / "index.html")
    else:
        base = DOCS_SITE_DIR / rel
        candidates.append(base)
        candidates.append(base / "index.html")
        if "." not in Path(rel).name:
            candidates.append(DOCS_SITE_DIR / f"{rel}.html")

    docs_root = DOCS_SITE_DIR.resolve()
    for candidate in candidates:
        resolved = candidate.resolve()
        if not str(resolved).startswith(str(docs_root)):
            continue
        if resolved.is_file():
            return FileResponse(resolved)
    return None


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

JOB = JobState()


def load_overrides() -> dict[str, list[dict[str, Any]]]:
    if not OVERRIDES_PATH.exists():
        return {}
    try:
        return json.loads(OVERRIDES_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def discover_scripts() -> list[Path]:
    if not SCRIPTS_DIR.exists():
        return []
    matches = [p for p in SCRIPTS_DIR.glob("*.py") if SCRIPT_PATTERN.match(p.name)]
    return sorted(matches)


def parse_default_from_help(help_text: str) -> str | None:
    match = DEFAULT_RE.search(help_text)
    return match.group(1).strip() if match else None


def parse_required_flags_from_usage(help_output: str) -> set[str]:
    """Extract required long options from argparse usage text.

    In argparse usage, optional args are typically rendered as [--flag ...],
    while required optional args appear without square brackets.
    """
    usage_lines: list[str] = []
    collecting = False
    for line in help_output.splitlines():
        if line.lower().startswith("usage:"):
            usage_lines.append(line)
            collecting = True
            continue
        if collecting and (line.startswith(" ") or line.startswith("\t")):
            usage_lines.append(line)
            continue
        if collecting:
            break

    usage_blob = " ".join(usage_lines)
    if not usage_blob:
        return set()

    return set(re.findall(r"(?<!\[)(--[a-zA-Z0-9][a-zA-Z0-9\-]*)", usage_blob))


def infer_type(field: dict[str, Any]) -> str:
    if field.get("type"):
        return field["type"]
    metavar = field.get("metavar")
    if metavar is None:
        return "boolean"
    if metavar in {"INT", "FLOAT", "NUMBER"}:
        return "number"
    return "string"


def with_verbose_flags(
    command: list[str],
    has_flag: set[str],
    requested_level: str,
) -> tuple[list[str], str]:
    """Inject a best-effort verbose mode when the target script supports it."""
    cmd = list(command)
    level = requested_level.upper().strip() or DEFAULT_SCRIPT_LOG_LEVEL

    if level == "DEBUG" and "--verbose" in has_flag and "--verbose" not in cmd:
        cmd.append("--verbose")
        return cmd, "--verbose"

    if level == "DEBUG" and "--debug" in has_flag and "--debug" not in cmd:
        cmd.append("--debug")
        return cmd, "--debug"

    if "--log-level" in has_flag and "--log-level" not in cmd:
        cmd.extend(["--log-level", level])
        return cmd, f"--log-level {level}"

    if level == "DEBUG" and "--verbosity" in has_flag and "--verbosity" not in cmd:
        cmd.extend(["--verbosity", "2"])
        return cmd, "--verbosity 2"

    return cmd, ""


def execution_config(db_settings: dict[str, Any]) -> dict[str, Any]:
    execution = db_settings.get("execution") or {}
    env_vars = execution.get("envVars")
    if not isinstance(env_vars, dict):
        env_vars = {}

    output_dir = str(
        db_settings.get("artifactsDir", str(WORKSPACE_ROOT / "output"))
    ).strip()

    return {
        "pythonExe": str(execution.get("pythonExe", "")).strip() or PYTHON_EXE,
        "tmpDir": str(execution.get("tmpDir", "")).strip(),
        "proxyUrl": (
            str(execution.get("proxyUrl", DEFAULT_PROXY_URL)).strip()
            or DEFAULT_PROXY_URL
        ),
        "logLevel": str(execution.get("logLevel", DEFAULT_SCRIPT_LOG_LEVEL)).strip().upper() or DEFAULT_SCRIPT_LOG_LEVEL,
        "outputDir": output_dir,
        "interlisTools": (
            execution.get("interlisTools")
            if isinstance(execution.get("interlisTools"), dict)
            else {}
        ),
        "envVars": env_vars,
    }


def build_process_env(db_settings: dict[str, Any]) -> dict[str, str]:
    cfg = execution_config(db_settings)
    env = dict(os.environ)

    tmp_dir = cfg["tmpDir"]
    if tmp_dir:
        env["TMP"] = tmp_dir
        env["TEMP"] = tmp_dir

    proxy_url = cfg["proxyUrl"]
    if proxy_url:
        for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            env[key] = proxy_url

    env["DGIM_LOG_LEVEL"] = cfg["logLevel"]

    interlis_tool_env = {
        "ili2c": ["ILI2C_JAR"],
        "ili2validator": ["ILIVALIDATOR_JAR", "ILI2VALIDATOR_JAR"],
        "ili2gpkg": ["ILI2GPKG_JAR"],
        "ili2duckdb": ["ILI2DUCKDB_JAR"],
        "ili2pg": ["ILI2PG_JAR"],
        "ili2imd": ["ILI2IMD_JAR"],
    }
    for tool_name, env_names in interlis_tool_env.items():
        raw_value = cfg["interlisTools"].get(tool_name)
        tool_path = str(raw_value).strip() if raw_value is not None else ""
        if tool_path:
            for env_name in env_names:
                env[env_name] = tool_path

    for key, value in cfg["envVars"].items():
        clean_key = str(key).strip()
        if clean_key:
            env[clean_key] = str(value)

    return env


def detect_existing_baseline(payload: dict[str, Any]) -> dict[str, Any]:
    default_output_dir = WORKSPACE_ROOT / "output"
    legacy_output_dir = str(payload.get("outputFolder", "")).strip()
    requested_model_dir = str(payload.get("modelOutputFolder", "")).strip()
    requested_catalogs_dir = str(payload.get("catalogsOutputFolder", "")).strip()

    output_dir_value = (
        requested_model_dir
        or requested_catalogs_dir
        or legacy_output_dir
    )
    output_dir = Path(output_dir_value) if output_dir_value else default_output_dir

    ili_model_path = output_dir / "DGIF_V3.ili"
    dgfcd_catalog_path = output_dir / "DGFCD_AttributeConcepts.xml"
    dgrwi_catalog_path = output_dir / "DGRWI_RealWorldObjects.xml"

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
                baseline_version = extract_baseline_version(metadata["sourceXmi"])
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


def parse_argparse_help(script_path: Path, python_exe: str) -> list[FieldSpec]:
    cmd = [python_exe, str(script_path), "--help"]
    proc = subprocess.run(
        cmd,
        cwd=str(WORKSPACE_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=25,
    )
    output = (proc.stdout or "") + "\n" + (proc.stderr or "")
    required_flags = parse_required_flags_from_usage(output)
    fields: list[FieldSpec] = []

    for line in output.splitlines():
        match = OPTION_RE.match(line)
        if not match:
            continue
        cli_flag = match.group("long")
        if cli_flag == "--help":
            continue

        raw_help = (match.group("help") or "").strip()
        default = parse_default_from_help(raw_help)
        required = cli_flag in required_flags or "required" in raw_help.lower()
        metavar = match.group("metavar")
        field_type = "boolean" if metavar is None else "string"

        fields.append(
            FieldSpec(
                name=cli_flag[2:].replace("-", "_"),
                cliFlag=cli_flag,
                type=field_type,
                required=required,
                default=default,
                help=raw_help,
            )
        )
    return fields


def merge_fields(auto_fields: list[FieldSpec], overrides: list[dict[str, Any]]) -> list[FieldSpec]:
    by_flag: dict[str, FieldSpec] = {f.cliFlag: f for f in auto_fields}

    for item in overrides:
        cli_flag = str(item.get("cliFlag", "")).strip()
        if not cli_flag:
            continue
        merged = FieldSpec(
            name=item.get("name", cli_flag[2:].replace("-", "_")),
            cliFlag=cli_flag,
            type=infer_type(item),
            required=bool(item.get("required", False)),
            default=item.get("default"),
            help=str(item.get("help", "")),
        )
        by_flag[cli_flag] = merged

    return sorted(by_flag.values(), key=lambda f: f.cliFlag)


def build_script_spec(script_path: Path, overrides_map: dict[str, list[dict[str, Any]]]) -> ScriptSpec:
    settings = load_settings()
    cfg = execution_config(settings)
    auto_fields = parse_argparse_help(script_path, cfg["pythonExe"])
    merged = merge_fields(auto_fields, overrides_map.get(script_path.name, []))
    return ScriptSpec(scriptName=script_path.name, scriptPath=str(script_path), fields=merged)


def build_command(
    request: JobStartRequest,
    spec: ScriptSpec,
    aoi_temp_file: Path | None,
    db_settings: dict[str, Any],
) -> list[str]:
    cfg = execution_config(db_settings)
    command = [cfg["pythonExe"], spec.scriptPath]
    fields_by_name = {f.name: f for f in spec.fields}

    for key, value in request.params.items():
        if key not in fields_by_name:
            continue
        field = fields_by_name[key]
        if field.type == "boolean":
            if bool(value):
                command.append(field.cliFlag)
            continue
        if value is None:
            continue
        text = str(value).strip()
        if text == "":
            continue
        command.extend([field.cliFlag, text])

    if request.aoiGeoJson and request.aoiParamFlag and aoi_temp_file is not None:
        command.extend([request.aoiParamFlag.strip(), str(aoi_temp_file)])

    destination_type = str(db_settings.get("destinationType", "geopackage"))
    gpkg_binding_mode = str(db_settings.get("gpkgBindingMode", "direct"))
    artifacts_dir = cfg["outputDir"]
    gpkg_target = str((db_settings.get("geopackage") or {}).get("path", ""))
    tmp_dir = cfg["tmpDir"]

    has_flag = {f.cliFlag for f in spec.fields}
    if "--python" in has_flag and "--python" not in command:
        command.extend(["--python", cfg["pythonExe"]])

    if "--tmp-dir" in has_flag and "--tmp-dir" not in command and tmp_dir:
        command.extend(["--tmp-dir", tmp_dir])

    if "--output-dir" in has_flag and "--output-dir" not in command:
        command.extend(["--output-dir", artifacts_dir])

    if "--output-gpkg" in has_flag and "--output-gpkg" not in command:
        if destination_type == "geopackage" and gpkg_target.strip() and gpkg_binding_mode == "direct":
            command.extend(["--output-gpkg", gpkg_target])
        else:
            command.extend(["--output-gpkg", str((Path(artifacts_dir) / "DGIF_central_staging.gpkg").resolve())])

    if (
        destination_type == "geopackage"
        and gpkg_binding_mode == "direct"
        and "--skip-schema" in has_flag
        and "--skip-schema" not in command
        and gpkg_target.strip()
        and Path(gpkg_target).expanduser().exists()
    ):
        # Keep previously ingested datasets when writing repeatedly to one central DB.
        command.append("--skip-schema")

    for arg in request.extraArgs:
        token = str(arg).strip()
        if token:
            command.append(token)

    command, _verbose_mode = with_verbose_flags(
        command,
        has_flag,
        cfg["logLevel"],
    )

    return command


def describe_db_settings(db_settings: dict[str, Any]) -> str:
    destination_type = str(db_settings.get("destinationType", "geopackage"))
    write_mode = str(db_settings.get("writeMode", "replace"))
    gpkg_binding_mode = str(db_settings.get("gpkgBindingMode", "direct"))

    if destination_type == "duckdb":
        target = str((db_settings.get("duckdb") or {}).get("path", "")).strip()
    elif destination_type == "postgresql":
        pg = db_settings.get("postgresql") or {}
        target = (
            f"{pg.get('host', '127.0.0.1')}:{pg.get('port', 5432)} "
            f"/{pg.get('database', 'dgim')} schema={pg.get('schema', 'swissdgif')}"
        )
    else:
        target = str((db_settings.get("geopackage") or {}).get("path", "")).strip()

    if not target:
        target = "<unset>"

    return (
        f"destinationType={destination_type} "
        f"writeMode={write_mode} "
        f"gpkgBindingMode={gpkg_binding_mode} "
        f"target={target}"
    )


def collect_candidate_artifacts(command: list[str], started_at: float) -> list[str]:
    candidates: set[Path] = set()

    for token in command:
        if any(token.lower().endswith(ext) for ext in [".gpkg", ".csv", ".xml", ".ili", ".qgz", ".xtf", ".txt", ".geojson"]):
            p = Path(token)
            if not p.is_absolute():
                p = WORKSPACE_ROOT / p
            candidates.add(p)

    output_dir = WORKSPACE_ROOT / "output"
    if output_dir.exists():
        for p in output_dir.rglob("*"):
            if p.is_file() and p.stat().st_mtime >= started_at:
                candidates.add(p)

    existing = [p for p in candidates if p.exists() and p.is_file()]
    return sorted({str(p.resolve()) for p in existing})


def append_log(line: str) -> None:
    text = line.rstrip("\n")
    with JOB.lock:
        JOB.logs.append(text)
    print(text, flush=True)


def append_debug(line: str) -> None:
    append_log(f"[debug] {line}")


def set_process_paused(
    process: subprocess.Popen[str], paused: bool
) -> tuple[bool, str]:
    action = "Suspend-Process" if paused else "Resume-Process"
    try:
        subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                f"{action} -Id {process.pid} -ErrorAction Stop",
            ],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        return True, ""
    except subprocess.CalledProcessError as exc:
        message = (exc.stderr or exc.stdout or str(exc)).strip()
        return False, message


def run_job(
    job_id: str,
    command: list[str],
    script_name: str,
    started_at: float,
    aoi_file: Path | None,
    db_settings: dict[str, Any],
) -> None:
    try:
        append_debug(f"Launching script: {script_name}")
        append_debug(f"Working directory: {WORKSPACE_ROOT}")
        append_debug(f"Command: {' '.join(command)}")

        proc = subprocess.Popen(
            command,
            cwd=str(WORKSPACE_ROOT),
            env=build_process_env(db_settings),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        with JOB.lock:
            JOB.process = proc

        assert proc.stdout is not None
        for line in proc.stdout:
            append_log(line)

        return_code = proc.wait()
        finished_at = time.time()
        elapsed = finished_at - started_at
        artifacts = collect_candidate_artifacts(command, started_at)
        append_debug(
            f"Script completed with return code {return_code} in {elapsed:.2f}s"
        )
        append_debug(f"Detected artifacts: {len(artifacts)}")
        for artifact in artifacts:
            append_debug(f"artifact: {artifact}")

        ingest_ok = True
        ingest_logs: list[str] = []
        if return_code == 0:
            try:
                settings = load_settings()
                append_debug(f"DB ingest settings: {describe_db_settings(settings)}")
                ingest_result = ingest_artifacts(
                    settings=settings,
                    run_id=job_id,
                    script_name=script_name,
                    status="done",
                    started_at=started_at,
                    finished_at=finished_at,
                    return_code=return_code,
                    artifacts=artifacts,
                )
                ingest_ok = ingest_result.ok
                ingest_logs = ingest_result.logs
                append_debug(
                    f"DB ingest completed: ok={ingest_ok} messages={len(ingest_logs)}"
                )
            except Exception as ingest_exc:
                ingest_ok = False
                ingest_logs = [f"[db-target-error] {ingest_exc}"]
                ingest_logs.extend(
                    [
                        "[db-target-error-traceback]",
                        traceback.format_exc().rstrip("\n"),
                    ]
                )

        with JOB.lock:
            if JOB.id == job_id:
                if return_code == 0 and ingest_ok:
                    JOB.status = "done"
                else:
                    JOB.status = "error"
                JOB.return_code = return_code
                JOB.finished_at = finished_at
                JOB.artifacts = artifacts
                JOB.logs.extend(ingest_logs)
                JOB.process = None
                JOB.paused = False
    except Exception as exc:  # pragma: no cover
        with JOB.lock:
            if JOB.id == job_id:
                JOB.status = "error"
                JOB.return_code = -1
                JOB.finished_at = time.time()
                JOB.logs.append(f"[backend-error] {exc}")
                JOB.logs.append("[backend-error-traceback]")
                JOB.logs.append(traceback.format_exc().rstrip("\n"))
                JOB.process = None
                JOB.paused = False
    finally:
        if aoi_file is not None and aoi_file.exists():
            try:
                aoi_file.unlink()
            except OSError:
                pass


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/")
def index() -> FileResponse:
    dist_index = WEBAPP_DIST_DIR / "index.html"
    if dist_index.exists():
        return FileResponse(str(dist_index))
    index_path = STATIC_DIR / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="Frontend not found")
    return FileResponse(str(index_path))


@app.get("/settings")
def settings_page() -> FileResponse:
    settings_path = STATIC_DIR / "settings.html"
    if not settings_path.exists():
        raise HTTPException(status_code=404, detail="Settings UI not found")
    return FileResponse(str(settings_path))


@app.get("/docs")
def docs_home() -> RedirectResponse:
    return RedirectResponse(url="/docs/")


def _redirect_docs_alias(path: str) -> RedirectResponse:
    return RedirectResponse(url=f"/docs{path}")


@app.get("/principles")
@app.get("/principles/{path:path}")
def docs_alias_principles(path: str = "") -> RedirectResponse:
    suffix = f"/{path}" if path else ""
    return _redirect_docs_alias(f"/principles{suffix}")


@app.get("/how-to")
@app.get("/how-to/{path:path}")
def docs_alias_how_to(path: str = "") -> RedirectResponse:
    suffix = f"/{path}" if path else ""
    return _redirect_docs_alias(f"/how-to{suffix}")


@app.get("/webapp")
@app.get("/webapp/{path:path}")
def docs_alias_webapp(path: str = "") -> RedirectResponse:
    suffix = f"/{path}" if path else ""
    return _redirect_docs_alias(f"/webapp{suffix}")


@app.get("/backend-api")
@app.get("/backend-api/{path:path}")
def docs_alias_backend_api(path: str = "") -> RedirectResponse:
    suffix = f"/{path}" if path else ""
    return _redirect_docs_alias(f"/backend-api{suffix}")


@app.get("/scripts")
@app.get("/scripts/{path:path}")
def docs_alias_scripts(path: str = "") -> RedirectResponse:
    suffix = f"/{path}" if path else ""
    return _redirect_docs_alias(f"/scripts{suffix}")


@app.get("/docs/")
def docs_home_slash() -> FileResponse:
    built_docs = DOCS_SITE_DIR / "index.html"
    if built_docs.exists():
        return FileResponse(str(built_docs))
    fallback = STATIC_DIR / "docs.html"
    if fallback.exists():
        return FileResponse(str(fallback))
    raise HTTPException(status_code=404, detail="Documentation not found")


@app.get("/docs/{path:path}")
def docs_assets(path: str) -> FileResponse:
    served = _serve_docs_site(path)
    if served is not None:
        return served
    raise HTTPException(
        status_code=404,
        detail="Documentation asset not found",
    )


@app.get("/assets/{path:path}")
def docs_root_assets(path: str) -> FileResponse:
    served = _serve_docs_site(f"assets/{path}")
    if served is not None:
        return served
    raise HTTPException(status_code=404, detail="Documentation asset not found")


@app.get("/javascripts/{path:path}")
def docs_root_javascripts(path: str) -> FileResponse:
    served = _serve_docs_site(f"javascripts/{path}")
    if served is not None:
        return served
    raise HTTPException(status_code=404, detail="Documentation asset not found")


@app.get("/api/scripts")
def list_scripts() -> list[dict[str, Any]]:
    scripts = discover_scripts()
    return [{"scriptName": s.name, "scriptPath": str(s)} for s in scripts]


@app.get("/api/scripts/{script_name}")
def get_script_schema(script_name: str) -> dict[str, Any]:
    script_path = SCRIPTS_DIR / script_name
    if not script_path.exists() or not SCRIPT_PATTERN.match(script_path.name):
        raise HTTPException(status_code=404, detail="Script not found")
    overrides_map = load_overrides()
    spec = build_script_spec(script_path, overrides_map)
    return spec.model_dump()


@app.get("/api/jobs/current")
def get_current_job() -> dict[str, Any]:
    with JOB.lock:
        return {
            "summary": JOB.summary().model_dump(),
            "logs": JOB.logs,
        }


@app.get("/api/settings/db")
def get_db_settings() -> dict[str, Any]:
    return load_settings()


@app.post("/api/system/select-folder")
async def select_folder(
    payload: dict[str, Any] | None = Body(default=None),
) -> dict[str, str | None]:
    request_payload = payload or {}
    initial_path = request_payload.get("initialPath")
    selected = await asyncio.to_thread(select_folder_dialog, initial_path)
    return {"path": selected}


@app.post("/api/settings/db")
def update_db_settings(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    saved = save_settings(payload)
    return {"saved": True, "settings": saved}


@app.post("/api/settings/db/validate")
def validate_db_settings(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    settings = sanitize_settings(payload) if payload else load_settings()
    result = validate_destination(settings)
    return result


@app.post("/api/settings/db/postgresql/test")
def test_postgresql_settings(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    settings = sanitize_settings(payload) if payload else load_settings()
    return test_postgresql_connection(settings)


@app.post("/api/baseline/check")
def check_existing_baseline(
    payload: dict[str, Any] | None = Body(default=None),
) -> dict[str, Any]:
    return detect_existing_baseline(payload or {})


@app.get("/{path:path}")
def webapp_assets(path: str) -> FileResponse:
    if path.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not found")
    served = _serve_webapp_dist(path)
    if served is not None:
        return served
    raise HTTPException(status_code=404, detail="Not found")


@app.post("/api/jobs/start")
def start_job(request: JobStartRequest) -> dict[str, Any]:
    script_path = SCRIPTS_DIR / request.scriptName
    if not script_path.exists() or not SCRIPT_PATTERN.match(script_path.name):
        raise HTTPException(status_code=404, detail="Script not found")

    with JOB.lock:
        if JOB.status in {"running", "paused"}:
            raise HTTPException(status_code=409, detail="A job is already running")

    overrides_map = load_overrides()
    spec = build_script_spec(script_path, overrides_map)

    aoi_file: Path | None = None
    if request.aoiGeoJson and request.aoiParamFlag:
        fd, temp_path = tempfile.mkstemp(prefix="aoi_", suffix=".geojson")
        os.close(fd)
        aoi_file = Path(temp_path)
        aoi_file.write_text(json.dumps(request.aoiGeoJson, ensure_ascii=False), encoding="utf-8")

    db_settings = load_settings()
    command = build_command(request, spec, aoi_file, db_settings)

    job_id = str(uuid.uuid4())
    started_at = time.time()

    with JOB.lock:
        JOB.id = job_id
        JOB.status = "running"
        JOB.script_name = request.scriptName
        JOB.command = command
        JOB.started_at = started_at
        JOB.finished_at = None
        JOB.return_code = None
        JOB.logs = [
            f"[backend] Starting: {' '.join(command)}",
            f"[debug] Script: {request.scriptName}",
            f"[debug] Job ID: {job_id}",
            f"[debug] Started at: {started_at}",
            f"[debug] Extra args count: {len(request.extraArgs)}",
            f"[debug] AOI attached: {bool(request.aoiGeoJson and request.aoiParamFlag)}",
        ]
        JOB.artifacts = []
        JOB.paused = False
        JOB.last_request = request.model_dump()

    thread = threading.Thread(
        target=run_job,
        args=(job_id, command, request.scriptName, started_at, aoi_file, db_settings),
        daemon=True,
    )
    thread.start()

    return {"summary": JOB.summary().model_dump()}


@app.post("/api/jobs/stop")
def stop_job() -> dict[str, Any]:
    with JOB.lock:
        if JOB.status not in {"running", "paused"} or JOB.process is None:
            return {"stopped": False, "message": "No running job"}
        JOB.process.terminate()
        JOB.logs.append("[backend] Termination requested")
    return {"stopped": True}


@app.post("/api/jobs/pause")
def pause_job() -> dict[str, Any]:
    with JOB.lock:
        if JOB.status not in {"running", "paused"} or JOB.process is None:
            return {"paused": False, "message": "No running job"}
        should_pause = JOB.status == "running"
        process = JOB.process

    ok, message = set_process_paused(process, should_pause)
    if not ok:
        with JOB.lock:
            JOB.logs.append(f"[backend] Pause/resume failed: {message}")
        raise HTTPException(status_code=500, detail=message)

    with JOB.lock:
        JOB.status = "paused" if should_pause else "running"
        JOB.paused = should_pause
        JOB.logs.append("[backend] Paused" if should_pause else "[backend] Resumed")
        summary = JOB.summary().model_dump()

    return {"paused": should_pause, "summary": summary}


@app.post("/api/jobs/restart")
def restart_job() -> dict[str, Any]:
    with JOB.lock:
        if JOB.status not in {"running", "paused"} or JOB.process is None:
            return {"restarted": False, "message": "No running job"}
        restart_request = JobStartRequest(**dict(JOB.last_request))
        process = JOB.process
        JOB.logs.append("[backend] Restart requested")

    process.terminate()

    timeout = time.time() + 15
    while time.time() < timeout:
        with JOB.lock:
            active = JOB.status in {"running", "paused"} and JOB.process is not None
        if not active:
            break
        time.sleep(0.2)

    return start_job(restart_request) | {"restarted": True}


@app.get("/api/jobs/stream")
def stream_job_events() -> StreamingResponse:
    def event_gen():
        last_index = 0
        while True:
            with JOB.lock:
                summary = JOB.summary().model_dump()
                new_logs = JOB.logs[last_index:]
                last_index = len(JOB.logs)

            payload = JobEvent(summary=JobSummary(**summary), newLogs=new_logs).model_dump()
            yield f"data: {json.dumps(payload)}\n\n"
            time.sleep(1)

    return StreamingResponse(event_gen(), media_type="text/event-stream")


@app.exception_handler(Exception)
def handle_exception(_: Any, exc: Exception) -> JSONResponse:
    traceback.print_exc()
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)},
    )
