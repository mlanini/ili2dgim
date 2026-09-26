#!/usr/bin/env python3
"""
ETL Pipeline: swissTLM3D XTF → DGIF GeoPackage

Orchestrates the full ETL process:
  Phase 1  — Download swissTLM3D XTF archive from data.geo.admin.ch
  Phase 2  — Extract the XTF file from the ZIP
  Phase 2b — Validate the XTF against the INTERLIS model (ilivalidator)
  Phase 3  — Create an empty DGIF GeoPackage (schema import from DGIF_V3.ili)
  Phase 4  — Import XTF into a temporary swissTLM3D GeoPackage (ili2gpkg)
  Phase 5  — Transform and load: Python script reads TLM GPKG, applies
             mapping table, reprojects LV95→WGS84, writes into DGIF GPKG

Prerequisites:
  - Java 8+ (java in PATH)
  - Python 3.12 with GDAL/OGR (QGIS bundled)
  - ili2gpkg 5.3.1 in ressources/ili2gpkg-5.3.1/
  - ilivalidator 1.15.0 in ressources/ilivalidator-1.15.0/
  - DGIF_V3.ili in models/
  - swissTLM3D_ili2_V2_4.ili in models/
  - swissTLM3D_to_DGIF_V3.csv in models/
"""

import argparse
import io
import json
import os
import re
import shutil
import subprocess
import sys
import time
import socket
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

from interlis_tool_paths import (
    describe_configured_interlis_tools,
    resolve_interlis_tool_path,
    should_log,
)


# Ensure Unicode output works on Windows terminals when printing symbols like arrows.
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")


DEFAULT_PROXY_URL = "http://proxy-bvcol.admin.ch:8080"
SWISSTLM3D_BROWSER_COLLECTION_URL = (
    "https://data.geo.admin.ch/browser/index.html#/collections/"
    "ch.swisstopo.swisstlm3d"
)
SWISSTLM3D_DOWNLOAD_PAGE_URL = (
    "https://www.swisstopo.admin.ch/de/landschaftsmodell-swisstlm3d"
)
SWISSTLM3D_CATALOG_PATH = "/ch.swisstopo.swisstlm3d/"
SWISSTLM3D_ZIP_RE = re.compile(
    r"(swisstlm3d_(\d{4}-\d{2})[_\w.-]*\.xtf\.zip)",
    re.IGNORECASE,
)
SWISSTLM3D_RELEASE_RE = re.compile(
    r"swisstlm3d_(\d{4}-\d{2})",
    re.IGNORECASE,
)


def _configure_network_proxy(default_proxy: str = DEFAULT_PROXY_URL) -> None:
    """Configure proxy defaults for Python urllib and Java subprocesses.

    Uses existing env settings when provided; otherwise falls back to the
    corporate proxy supplied in this repository.
    """
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        if not os.environ.get(key):
            os.environ[key] = default_proxy

    proxy_url = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or default_proxy
    parsed = urllib.parse.urlparse(proxy_url)
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

    urllib.request.install_opener(
        urllib.request.build_opener(urllib.request.ProxyHandler(urllib.request.getproxies()))
    )


# ============================================================================
# QGIS / GDAL environment setup (Windows)
# ============================================================================
def _find_qgis_root() -> str | None:
    """Auto-detect QGIS installation directory on Windows."""
    if sys.platform != "win32":
        return None
    base = Path(os.environ.get("PROGRAMFILES", r"C:\Program Files"))
    candidates = sorted(base.glob("QGIS *"), reverse=True)
    return str(candidates[0]) if candidates else None


def _setup_qgis_env(qgis_root: str | None = None) -> None:
    """Prepend QGIS DLL directories to PATH and set GDAL/PROJ env vars.

    When calling the QGIS-bundled Python from outside the QGIS shell, the
    native GDAL DLLs are not on PATH and ``from osgeo import gdal`` fails
    with ``ImportError: DLL load failed``.  This function mirrors what the
    QGIS startup bat files do.
    """
    if qgis_root is None:
        qgis_root = _find_qgis_root()
    if qgis_root is None:
        return  # Not on Windows or no QGIS found — assume GDAL is on PATH
    qgis = Path(qgis_root)
    extra_paths = [
        str(qgis / "bin"),
        str(qgis / "apps" / "gdal" / "bin"),
        str(qgis / "apps" / "Python312"),
        str(qgis / "apps" / "Python312" / "Scripts"),
        str(qgis / "apps" / "Qt5" / "bin"),
    ]
    current = os.environ.get("PATH", "")
    os.environ["PATH"] = os.pathsep.join(extra_paths) + os.pathsep + current
    os.environ["GDAL_DATA"] = str(qgis / "apps" / "gdal" / "share" / "gdal")
    os.environ["PROJ_LIB"] = str(qgis / "share" / "proj")


# Apply QGIS environment before anything else uses GDAL
_setup_qgis_env()
_configure_network_proxy()


# ============================================================================
# ANSI colour helpers (works on Windows Terminal / VS Code / modern consoles)
# ============================================================================
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
GREY = "\033[90m"
RESET = "\033[0m"


def info(msg: str) -> None:
    if should_log("INFO"):
        print(f"{CYAN}[INFO]{RESET} {msg}")


def ok(msg: str) -> None:
    if should_log("INFO"):
        print(f"{GREEN}[OK]{RESET} {msg}")


def warn(msg: str) -> None:
    if should_log("WARNING"):
        print(f"{YELLOW}[WARNING]{RESET} {msg}")


def skip(msg: str) -> None:
    if should_log("INFO"):
        print(f"{YELLOW}[SKIP]{RESET} {msg}")


def error(msg: str) -> None:
    print(f"{RED}[ERROR]{RESET} {msg}", file=sys.stderr)


def banner(title: str) -> None:
    if should_log("INFO"):
        print()
        print(f"{CYAN}================================================================{RESET}")
        print(f"{CYAN}  {title}{RESET}")
        print(f"{CYAN}================================================================{RESET}")


def run_java(args: list[str], label: str) -> int:
    """Run a java command, stream output, return exit code."""
    cmd = ["java"] + args
    info(f"{label}")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        print(f"  {GREY}{line.rstrip()}{RESET}")
    proc.wait()
    return proc.returncode


def file_size_mb(path: str | Path) -> float:
    return round(os.path.getsize(path) / (1024 * 1024), 1)


def discover_latest_tlm_url(seed_url: str, timeout_s: int) -> str | None:
    """Discover the latest swissTLM3D XTF ZIP URL.

    The authoritative source is the STAC API at data.geo.admin.ch. It exposes each
    release as a dated item and the downloadable XTF asset as a direct URL. We use
    the HTML pages only as a fallback when the API is unavailable.
    """
    parsed = urllib.parse.urlparse(seed_url)
    catalog_base = (
        f"{parsed.scheme}://{parsed.netloc}{SWISSTLM3D_CATALOG_PATH}"
        if parsed.scheme and parsed.netloc
        else "https://data.geo.admin.ch/"
    )

    stac_url = "https://data.geo.admin.ch/api/stac/v1/collections/ch.swisstopo.swisstlm3d/items?limit=80"
    try:
        req = urllib.request.Request(stac_url, headers={"User-Agent": "DGIF-ETL/1.0"})
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            payload = json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception:
        payload = None

    if payload and isinstance(payload, dict):
        features = payload.get("features") or []
        best_id = ""
        for feature in features:
            feature_id = str(feature.get("id") or "")
            if not feature_id.startswith("swisstlm3d_"):
                continue
            if feature_id >= best_id:
                best_id = feature_id

        if best_id:
            assets = None
            for feature in features:
                if str(feature.get("id") or "") == best_id:
                    assets = feature.get("assets") or {}
                    break
            if assets:
                xtf_key = f"{best_id}_2056_5728.xtf.zip"
                href = assets.get(xtf_key, {}).get("href")
                if href:
                    return href

    candidates = [
        catalog_base,
        urllib.parse.urljoin(catalog_base, "index.html"),
        SWISSTLM3D_DOWNLOAD_PAGE_URL,
    ]

    best_date = ""
    best_rel_path: str | None = None
    discovered_dates: set[str] = set()

    for url in candidates:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "DGIF-ETL/1.0"})
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                payload = resp.read().decode("utf-8", errors="replace")
        except Exception:
            continue

        for match in SWISSTLM3D_ZIP_RE.finditer(payload):
            filename = match.group(1)
            release_date = match.group(2)
            if release_date >= best_date:
                best_date = release_date
                best_rel_path = f"ch.swisstopo.swisstlm3d/swisstlm3d_{release_date}/{filename}"

        for match in SWISSTLM3D_RELEASE_RE.finditer(payload):
            discovered_dates.add(match.group(1))

    if not best_rel_path and discovered_dates:
        latest_date = max(discovered_dates)
        best_rel_path = (
            f"ch.swisstopo.swisstlm3d/swisstlm3d_{latest_date}/"
            f"swisstlm3d_{latest_date}_2056_5728.xtf.zip"
        )

    if not best_rel_path:
        return None
    return urllib.parse.urljoin("https://data.geo.admin.ch/", best_rel_path)


# ============================================================================
# Main
# ============================================================================
def main() -> int:
    parser = argparse.ArgumentParser(
        description="ETL Pipeline: swissTLM3D XTF → DGIF GeoPackage"
    )
    parser.add_argument(
        "--tlm-url",
        default=SWISSTLM3D_BROWSER_COLLECTION_URL,
        help=(
            "Reference URL for swissTLM3D collection discovery. "
            "The script always resolves the latest current XTF ZIP before download."
        ),
    )
    parser.add_argument(
        "--download-timeout",
        type=int,
        default=300,
        help="Download timeout per attempt in seconds (default: 300)",
    )
    parser.add_argument(
        "--download-retries",
        type=int,
        default=3,
        help="Number of download attempts before failing (default: 3)",
    )
    parser.add_argument(
        "--download-retry-wait",
        type=int,
        default=20,
        help="Wait time in seconds between retry attempts (default: 20)",
    )
    parser.add_argument(
        "--no-auto-discover-tlm-url",
        action="store_true",
        help="Deprecated: latest swissTLM3D URL discovery is always enabled.",
    )
    parser.add_argument(
        "--tmp-dir",
        default="C:/tmp/dgif",
        help="Temporary working directory (default: C:/tmp/dgif)",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download if ZIP already exists",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip XTF validation with ilivalidator",
    )
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Skip extraction if xtf/ directory already exists with .xtf files",
    )
    parser.add_argument(
        "--skip-import",
        action="store_true",
        help="Skip XTF→GeoPackage import (Phase 4) if swisstlm3d_temp.gpkg already exists",
    )
    parser.add_argument(
        "--skip-schema",
        action="store_true",
        help="Skip DGIF schema creation (Phase 3) when output GeoPackage already exists",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Path to Python interpreter with GDAL (default: current interpreter)",
    )
    parser.add_argument(
        "--xtf-dir",
        default=None,
        help="Path to a local directory containing .xtf and .ili files "
             "(e.g. ressources/testdata). When set, download and extraction "
             "are skipped automatically.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for DGIF GeoPackage (default: workspace/output)",
    )
    parser.add_argument(
        "--output-gpkg",
        default=None,
        help="Full output GeoPackage path; overrides output directory default",
    )
    parser.add_argument(
        "--ili-model",
        default=None,
        help="Path to DGIF INTERLIS model (.ili). Defaults to the generated output model or workspace models/DGIF_V3.ili",
    )
    parser.add_argument(
        "--aoi-file",
        default=None,
        help="Optional AOI GeoJSON file for compatibility with the backend ETL runner.",
    )
    parser.add_argument(
        "--aoi-wkt",
        default=None,
        help="Optional AOI WKT for compatibility with the backend ETL runner.",
    )
    parser.add_argument(
        "--target-topics",
        default=None,
        help="Comma-separated DGIF topics to write (e.g. Foundation,Cultural,Transportation)",
    )
    args = parser.parse_args()

    # ========================================================================
    # Configuration
    # ========================================================================
    workspace_root = Path(__file__).resolve().parent.parent
    ili2gpkg_jar = resolve_interlis_tool_path("ili2gpkg")
    ilivalidator_jar = resolve_interlis_tool_path("ili2validator")
    transform_py = workspace_root / "scripts" / "etl_swisstlm3d_transform.py"
    python_exe = args.python

    output_dir = Path(args.output_dir).expanduser() if args.output_dir else (workspace_root / "output")
    dgif_gpkg = output_dir / "DGIF_swissTLM3D.gpkg"
    if args.output_gpkg:
        dgif_gpkg = Path(args.output_gpkg).expanduser()
        output_dir = dgif_gpkg.parent

    if args.ili_model:
        dgif_ili = Path(args.ili_model).expanduser()
    else:
        default_model = workspace_root / "models" / "DGIF_V3.ili"
        output_model = output_dir / "DGIF_V3.ili"
        dgif_ili = output_model if output_model.exists() else default_model

    tlm_ili = workspace_root / "models" / "swissTLM3D_ili2_V2_4.ili"
    mapping_csv = workspace_root / "models" / "swissTLM3D_to_DGIF_V3.csv"

    tmp_dir = Path(args.tmp_dir)
    zip_file = tmp_dir / "swisstlm3d.xtf.zip"
    tlm_gpkg = tmp_dir / "swisstlm3d_temp.gpkg"

    # Model directories (semicolon-separated, as expected by ili2gpkg / ilivalidator).
    # Prefer the explicitly configured DGIF model folder first so generated models in
    # the UI output directory are respected instead of always falling back to models/.
    models_dir = workspace_root / "models"
    dgif_model_dir = f"{dgif_ili.parent};{models_dir};http://models.interlis.ch/;%JAR_DIR"
    # tlm_model_dir is set after Phase 2 (includes the xtf/ directory where
    # the model .ili shipped with the data resides)

    # ========================================================================
    # Banner
    # ========================================================================
    banner("ETL Pipeline: swissTLM3D → DGIF GeoPackage")
    print()
    if should_log("DEBUG"):
        for configured_tool in describe_configured_interlis_tools():
            info(f"Configured tool: {configured_tool}")

    # ========================================================================
    # Prerequisites check
    # ========================================================================
    print(f"{YELLOW}--- Checking prerequisites ---{RESET}")

    # Java
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        java_ver = (result.stderr or result.stdout).strip().split("\n")[0]
        ok(f"Java: {java_ver}")
    except FileNotFoundError:
        error("Java not found in PATH!")
        return 1

    # ili2gpkg
    if ili2gpkg_jar is None or not ili2gpkg_jar.exists():
        error(f"ili2gpkg not found: {ili2gpkg_jar}")
        return 1
    ok(f"ili2gpkg: {ili2gpkg_jar}")

    # ilivalidator
    if not ilivalidator_jar.exists():
        error(f"ilivalidator not found: {ilivalidator_jar}")
        return 1
    ok(f"ilivalidator: {ilivalidator_jar}")

    if args.aoi_file:
        aoi_path = Path(args.aoi_file).expanduser()
        if not aoi_path.exists():
            error(f"AOI file not found: {aoi_path}")
            return 1
        info(f"AOI file: {aoi_path}")
    if args.aoi_wkt:
        info(f"AOI WKT provided: {args.aoi_wkt[:80]}...")

    # Required files
    for f in (dgif_ili, tlm_ili, mapping_csv, transform_py):
        if not f.exists():
            error(f"File not found: {f}")
            return 1
        ok(f"{f.name}")

    # Python + GDAL
    try:
        result = subprocess.run(
            [python_exe, "-c", "from osgeo import gdal; print('GDAL', gdal.VersionInfo())"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        ok(f"Python + {result.stdout.strip()}")
    except Exception as exc:
        error(f"Python/GDAL not available at: {python_exe} ({exc})")
        return 1

    # Temp directory
    tmp_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    ok(f"Temp dir: {tmp_dir}")
    print()

    # ========================================================================
    # Phase 1 — Download
    # ========================================================================
    banner("Phase 1: Download swissTLM3D XTF")

    if args.xtf_dir:
        skip(f"Using local XTF directory: {args.xtf_dir} (--xtf-dir)")
    elif args.skip_download and zip_file.exists():
        skip(f"Using existing: {zip_file}")
    else:
        info("Resolving latest swissTLM3D current XTF URL from catalog...")
        current_download_url = discover_latest_tlm_url(
            seed_url=args.tlm_url,
            timeout_s=max(30, int(args.download_timeout)),
        )
        if not current_download_url:
            error(
                "Unable to resolve latest swissTLM3D current dataset URL from catalog."
            )
            print(
                f"  {GREY}Catalog reference: {SWISSTLM3D_BROWSER_COLLECTION_URL}{RESET}"
            )
            return 1

        if args.no_auto_discover_tlm_url:
            warn("--no-auto-discover-tlm-url is deprecated and ignored.")

        info("Downloading from:")
        print(f"  {GREY}{current_download_url}{RESET}")
        info(f"Destination: {zip_file}")

        t0 = time.perf_counter()
        last_exc: Exception | None = None
        retries = max(1, int(args.download_retries))
        auto_discover_attempted = False
        part_file = zip_file.with_suffix(zip_file.suffix + ".part")
        # Resume a previously interrupted download instead of restarting from
        # scratch. This matters a lot for this ~3.6 GB asset over a slow/flaky
        # corporate proxy, where a single network hiccup used to mean losing
        # all progress.
        if part_file.exists():
            info(
                f"Resuming previous partial download "
                f"({file_size_mb(part_file)} MB already downloaded)"
            )
        for attempt in range(1, retries + 1):
            should_retry = True
            resume_offset = part_file.stat().st_size if part_file.exists() else 0
            try:
                info(f"Download attempt {attempt}/{retries} (timeout={args.download_timeout}s)")
                headers = {"User-Agent": "DGIF-ETL/1.0"}
                if resume_offset:
                    headers["Range"] = f"bytes={resume_offset}-"
                req = urllib.request.Request(current_download_url, headers=headers)
                with urllib.request.urlopen(req, timeout=args.download_timeout) as resp:
                    resumed = resp.status == 206 and resume_offset > 0
                    if resume_offset and not resumed:
                        # Server ignored the Range request (e.g. no Accept-Ranges
                        # support): start over cleanly to avoid a corrupt file.
                        warn("Server does not support resuming; restarting download from scratch.")
                        resume_offset = 0
                    content_length = int(resp.headers.get("Content-Length", 0))
                    total = content_length + resume_offset if resumed else content_length
                    total_mb = total / (1024 * 1024) if total else 0
                    downloaded = resume_offset
                    chunk_size = 1024 * 1024  # 1 MB chunks
                    mode = "ab" if resumed else "wb"
                    with open(str(part_file), mode) as fout:
                        last_reported_pct = -1.0
                        while True:
                            chunk = resp.read(chunk_size)
                            if not chunk:
                                break
                            fout.write(chunk)
                            downloaded += len(chunk)
                            if total:
                                pct = downloaded * 100 / total
                                dl_mb = downloaded / (1024 * 1024)
                                if pct - last_reported_pct >= 5.0 or downloaded == total:
                                    print(
                                        f"  {GREY}[etl-download-progress] {pct:.1f}% "
                                        f"({dl_mb:.1f} / {total_mb:.1f} MB){RESET}",
                                        flush=True,
                                    )
                                    last_reported_pct = pct
                    print()  # newline after progress
                part_file.replace(zip_file)
                last_exc = None
                break
            except urllib.error.HTTPError as exc:
                last_exc = exc

                status = int(getattr(exc, "code", 0) or 0)
                reason = str(getattr(exc, "reason", "")).strip()
                detail = f"HTTP {status}" if status else "HTTP error"
                if reason:
                    detail += f" ({reason})"

                if status == 416:
                    # Range not satisfiable: our partial file is stale/invalid
                    # (e.g. server-side asset changed). Drop it and retry fresh.
                    warn("Resume offset rejected by server (416); discarding partial download.")
                    if part_file.exists():
                        part_file.unlink()
                elif status in (404, 410):
                    if part_file.exists():
                        part_file.unlink()
                    if not auto_discover_attempted:
                        auto_discover_attempted = True
                        info("Auto-discovery: searching latest swissTLM3D release URL...")
                        discovered_url = discover_latest_tlm_url(
                            seed_url=current_download_url,
                            timeout_s=max(30, int(args.download_timeout)),
                        )
                        if discovered_url and discovered_url != current_download_url:
                            warn(
                                "Configured URL returned not found. "
                                "Switching to discovered latest release URL."
                            )
                            print(f"  {GREY}{discovered_url}{RESET}")
                            current_download_url = discovered_url
                            should_retry = True
                        else:
                            should_retry = False
                            error(
                                "Download URL not found on server "
                                f"({detail}) and autodiscovery found no replacement URL."
                            )
                    else:
                        should_retry = False
                        error(
                            "Download URL not found on server "
                            f"({detail}). Latest dataset discovery did not return a valid replacement URL."
                        )
                elif 400 <= status < 500 and status not in (408, 429):
                    should_retry = False
                    error(f"Client-side HTTP error: {detail}.")
                else:
                    warn(f"Download attempt {attempt}/{retries} failed: {detail}")
            except (urllib.error.URLError, socket.timeout, TimeoutError) as exc:
                last_exc = exc
                # Keep the partial file on network errors so the next attempt
                # (or the next full run) can resume instead of starting over.
                warn(f"Download attempt {attempt}/{retries} failed: {exc}")
            except Exception as exc:
                last_exc = exc
                warn(f"Download attempt {attempt}/{retries} failed: {exc}")

            if last_exc is None:
                break
            if not should_retry:
                break
            if attempt < retries:
                info(f"Retrying in {args.download_retry_wait}s...")
                time.sleep(max(0, int(args.download_retry_wait)))

        if last_exc is not None:
            error(f"Download failed after {retries} attempt(s): {last_exc}")

            if isinstance(last_exc, urllib.error.HTTPError):
                status = int(getattr(last_exc, "code", 0) or 0)
                if status in (404, 410):
                    warn(
                        "Resource not found on remote server. "
                        "Latest discovered swissTLM3D URL may have changed during download."
                    )
                    print(
                        f"  {GREY}Tip: retry run; URL is re-discovered from catalog at each execution.{RESET}"
                    )
                elif status in (401, 403, 407):
                    warn("Access/proxy authorization issue detected.")
                    print(f"  {GREY}Tip: check proxy credentials/policies and HTTPS_PROXY/HTTP_PROXY settings.{RESET}")
                else:
                    warn("HTTP download error detected.")
            else:
                warn("Network/proxy issue detected.")

            print(f"  {GREY}You can bypass Phase 1 with local data:{RESET}")
            print(f"  {GREY}1) Use a local XTF directory: --xtf-dir ressources/testdata{RESET}")
            print(f"  {GREY}2) Or pre-download ZIP to {zip_file} and run with --skip-download{RESET}")
            print(f"  {GREY}3) Or increase timeout/retries: --download-timeout 900 --download-retries 5{RESET}")
            if part_file.exists():
                print(
                    f"  {GREY}4) A partial download ({file_size_mb(part_file)} MB) was kept at "
                    f"{part_file}; simply re-run the ETL to resume from there.{RESET}"
                )
            return 1
        elapsed = time.perf_counter() - t0
        ok(f"Downloaded {file_size_mb(zip_file)} MB in {elapsed:.1f}s")
    print()

    # ========================================================================
    # Phase 2 — Extract XTF from ZIP (or use --xtf-dir)
    # ========================================================================
    banner("Phase 2: Extract XTF")

    if args.xtf_dir:
        # Use the user-provided directory directly
        xtf_dir = Path(args.xtf_dir).resolve()
        if not xtf_dir.exists():
            error(f"XTF directory not found: {xtf_dir}")
            return 1
        skip(f"Using local directory: {xtf_dir}")
    else:
        xtf_dir = tmp_dir / "xtf"

        # Check if we can skip extraction
        existing_xtfs = sorted(xtf_dir.rglob("*.xtf")) if xtf_dir.exists() else []
        if args.skip_extract and existing_xtfs:
            skip(f"Using existing {len(existing_xtfs)} XTF file(s) in {xtf_dir}")
        else:
            if xtf_dir.exists():
                shutil.rmtree(xtf_dir)

            info("Extracting ZIP (~28 GB uncompressed, this may take a few minutes)...")
            t0 = time.perf_counter()
            with zipfile.ZipFile(str(zip_file), "r") as zf:
                zf.extractall(str(xtf_dir))
            elapsed = time.perf_counter() - t0
            ok(f"Extraction completed in {elapsed:.1f}s")

    # Find .xtf files
    xtf_files = sorted(xtf_dir.rglob("*.xtf"), key=lambda p: p.name)
    if not xtf_files:
        error("No .xtf files found in archive!")
        return 1

    total_xtf_mb = 0.0
    for xf in xtf_files:
        sz = file_size_mb(xf)
        total_xtf_mb += sz
        ok(f"{xf.name} ({sz} MB)")
    info(f"Found {len(xtf_files)} XTF file(s), total {total_xtf_mb:.0f} MB")

    # Set TLM model directory: xtf/ first (contains the .ili shipped with the
    # data, e.g. swissTLM3D_ili2_V2_4.ili), then the models/ dir (has Units.ili,
    # CoordSys.ili), then the standard online repository and the JAR bundled models.
    tlm_model_dir = (
        f"{xtf_dir};"
        f"{models_dir};"
        f"{workspace_root / 'ressources'};"
        f"http://models.interlis.ch/;"
        f"%JAR_DIR"
    )
    info(f"TLM model dir: {tlm_model_dir}")
    print()

    # ========================================================================
    # Phase 2b — Validate XTF with ilivalidator
    # ========================================================================
    banner("Phase 2b: Validate XTF (ilivalidator)")

    validation_log = tmp_dir / "ilivalidator.log"
    validation_xtf_log = tmp_dir / "ilivalidator_errors.xtf"

    if args.skip_validation:
        skip("Validation skipped (--skip-validation)")
    else:
        warn("This may take a long time for large datasets.")
        validation_ok = True
        for xf in xtf_files:
            validation_log = tmp_dir / f"ilivalidator_{xf.stem}.log"
            validation_xtf_log = tmp_dir / f"ilivalidator_{xf.stem}_errors.xtf"

            validator_args = [
                "-Xmx4096m",
                "-jar", str(ilivalidator_jar),
                "--modeldir", tlm_model_dir,
                "--log", str(validation_log),
                "--xtflog", str(validation_xtf_log),
                "--logtime",
                str(xf),
            ]

            t0 = time.perf_counter()
            rc = run_java(validator_args, f"Validating {xf.name}...")
            elapsed = time.perf_counter() - t0

            if rc == 0:
                ok(f"{xf.name} — validation passed in {elapsed:.1f}s")
            else:
                validation_ok = False
                warn(f"{xf.name} — validation errors (exit code {rc})")
                warn(f"  log:    {validation_log}")
                warn(f"  xtflog: {validation_xtf_log}")

        if not validation_ok:
            info("Continuing with import despite validation errors...")
            print(f"  {GREY}The XTF data is from an official swisstopo source; minor model{RESET}")
            print(f"  {GREY}deviations may exist but do not prevent import.{RESET}")
    print()

    # ========================================================================
    # Phase 3 — Create empty DGIF GeoPackage (schema import)
    # ========================================================================
    banner("Phase 3: Create DGIF GeoPackage schema")
    dgif_schema_log = tmp_dir / "dgif_schemaimport.log"

    if args.skip_schema and dgif_gpkg.exists():
        skip(f"Using existing DGIF GeoPackage: {dgif_gpkg} ({file_size_mb(dgif_gpkg)} MB)")
    else:
        if dgif_gpkg.exists():
            info(f"Removing existing: {dgif_gpkg}")
            dgif_gpkg.unlink()

        dgif_schema_args = [
            "-jar", str(ili2gpkg_jar),
            "--schemaimport",
            "--dbfile", str(dgif_gpkg),
            "--defaultSrsAuth", "EPSG",
            "--defaultSrsCode", "4326",
            "--smart2Inheritance",
            "--nameByTopic",
            "--createGeomIdx",
            "--strokeArcs",
            "--createEnumTabs",
            "--createEnumTxtCol",
            "--beautifyEnumDispName",
            "--createBasketCol",
            "--createTidCol",
            "--createStdCols",
            "--createMetaInfo",
            "--createFk",
            "--createFkIdx",
            "--modeldir", dgif_model_dir,
            "--log", str(dgif_schema_log),
            str(dgif_ili),
        ]

        t0 = time.perf_counter()
        rc = run_java(dgif_schema_args, "Running ili2gpkg --schemaimport for DGIF...")
        if rc != 0:
            error(f"DGIF schema import failed! See: {dgif_schema_log}")
            return 1
        elapsed = time.perf_counter() - t0
        ok(f"DGIF GeoPackage schema created: {file_size_mb(dgif_gpkg)} MB in {elapsed:.1f}s")
    print()

    # ========================================================================
    # Phase 4 — Import XTF into temporary swissTLM3D GeoPackage
    # ========================================================================
    banner("Phase 4: Import XTF into temp GeoPackage")

    if args.skip_import and tlm_gpkg.exists():
        skip(f"Using existing TLM GeoPackage: {tlm_gpkg} ({file_size_mb(tlm_gpkg)} MB)")
    else:
        if tlm_gpkg.exists():
            info(f"Removing existing: {tlm_gpkg}")
            tlm_gpkg.unlink()

        # 4a — Schema import: create the TLM GeoPackage structure from a readable
        #      .ili model. Prefer model shipped with data, fallback to models/.
        tlm_ili_candidates = list(xtf_dir.glob("*.ili")) + [tlm_ili]
        tlm_ili_file = None
        for candidate in tlm_ili_candidates:
            if not candidate.exists() or not candidate.is_file():
                continue
            try:
                with candidate.open("rb") as probe:
                    probe.read(1)
                tlm_ili_file = candidate
                break
            except OSError:
                continue
        if tlm_ili_file is None:
            error(
                "No readable .ili model file found in "
                f"{xtf_dir} or fallback {tlm_ili}"
            )
            return 1
        info(f"TLM model: {tlm_ili_file.name}")

        tlm_schema_log = tmp_dir / "tlm_schemaimport.log"
        tlm_schema_args = [
            "-jar", str(ili2gpkg_jar),
            "--schemaimport",
            "--dbfile", str(tlm_gpkg),
            "--defaultSrsAuth", "EPSG",
            "--defaultSrsCode", "2056",
            "--nameByTopic",
            "--strokeArcs",
            "--createEnumTabs",
            "--createEnumTxtCol",
            "--beautifyEnumDispName",
            "--createBasketCol",
            "--createTidCol",
            "--createStdCols",
            "--modeldir", tlm_model_dir,
            "--log", str(tlm_schema_log),
            str(tlm_ili_file),
        ]

        t0 = time.perf_counter()
        rc = run_java(tlm_schema_args, "ili2gpkg --schemaimport for TLM...")
        if rc != 0:
            error(f"TLM schema import failed! See: {tlm_schema_log}")
            return 1
        elapsed = time.perf_counter() - t0
        ok(f"TLM GeoPackage schema created in {elapsed:.1f}s")

        # 4b — Import each XTF file into the existing schema
        info(f"Importing {len(xtf_files)} XTF file(s) into temp GeoPackage...")
        warn("This may take a long time for large datasets.")
        t0_total = time.perf_counter()

        for i, xf in enumerate(xtf_files, 1):
            info(f"[{i}/{len(xtf_files)}] Importing {xf.name} ({file_size_mb(xf)} MB)...")
            per_file_log = tmp_dir / f"tlm_import_{xf.stem}.log"

            tlm_import_args = [
                "-jar", str(ili2gpkg_jar),
                "--import",
                "--dbfile", str(tlm_gpkg),
                "--disableValidation",
                "--defaultSrsAuth", "EPSG",
                "--defaultSrsCode", "2056",
                "--nameByTopic",
                "--strokeArcs",
                "--createEnumTabs",
                "--createEnumTxtCol",
                "--beautifyEnumDispName",
                "--createBasketCol",
                "--createTidCol",
                "--createStdCols",
                "--modeldir", tlm_model_dir,
                "--log", str(per_file_log),
                str(xf),
            ]

            t0 = time.perf_counter()
            rc = run_java(tlm_import_args, f"ili2gpkg --import {xf.name}")
            elapsed = time.perf_counter() - t0

            if rc != 0:
                error(f"Import of {xf.name} failed! See: {per_file_log}")
                return 1
            ok(f"{xf.name} imported in {elapsed:.1f}s")

        total_elapsed = time.perf_counter() - t0_total
        ok(f"TLM GeoPackage created: {file_size_mb(tlm_gpkg)} MB in {total_elapsed:.1f}s")
    print()

    # ========================================================================
    # Phase 5 — Transform and Load (Python)
    # ========================================================================
    banner("Phase 5: Transform & Load (Python ETL)")

    info("Running etl_swisstlm3d_transform.py...")
    t0 = time.perf_counter()

    proc = subprocess.Popen(
        [
            python_exe,
            str(transform_py),
            "--tlm-gpkg", str(tlm_gpkg),
            "--dgif-gpkg", str(dgif_gpkg),
            "--mapping", str(mapping_csv),
            *([
                "--target-topics",
                args.target_topics,
            ] if args.target_topics else []),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        print(f"  {line.rstrip()}")
    proc.wait()

    if proc.returncode != 0:
        error("Python transform failed!")
        return 1

    elapsed = time.perf_counter() - t0
    final_size = file_size_mb(dgif_gpkg)
    ok(f"Transform completed in {elapsed:.1f}s")
    print()

    # ========================================================================
    # Summary
    # ========================================================================
    banner("ETL Pipeline Complete")
    print()
    print(f"  {GREEN}Output:   {dgif_gpkg} ({final_size} MB){RESET}")
    print(f"  {GREY}TLM temp: {tlm_gpkg} ({file_size_mb(tlm_gpkg)} MB){RESET}")
    print(f"  {GREY}Logs:     {dgif_schema_log}{RESET}")
    for xf in xtf_files:
        print(f"  {GREY}          {tmp_dir / f'tlm_import_{xf.stem}.log'}{RESET}")
    if not args.skip_validation:
        for xf in xtf_files:
            print(f"  {GREY}          {tmp_dir / f'ilivalidator_{xf.stem}.log'}{RESET}")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
