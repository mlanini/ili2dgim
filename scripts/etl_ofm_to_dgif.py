#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL Pipeline: OpenFlightMaps (OFMX) -> DGIF GeoPackage

Orchestrates the full ETL process:
  Phase 1  -- Discover OFMX files (embedded or isolated XML format)
  Phase 2  -- Create empty DGIF GeoPackage (schema import from DGIF_V3.ili)
  Phase 3  -- Transform and load: parse OFMX XML, apply OFM->DGIF mapping,
             insert geometries into DGIF GPKG tables

OFMX data structure (AIXM 4.5 flavour):
  - Aerodrome (Ahp) -> DGIF Aerodrome / Runway (Rwy + Rdn)
  - Designated Point (Dpn) -> DGIF WayPoint
  - Airspace (Ase/Abd) with geometry -> DGIF Airspace
  - Navaid: VOR (Vor), NDB (Ndb), DME (Dme) -> DGIF navaid classes
  - Terrain/Obstacle: partial support (limited obstacle details in OFMX)

Prerequisites:
  - Java 8+ (java in PATH)
  - Python 3.12 with GDAL/OGR (QGIS bundled)
  - ili2gpkg 5.3.1 in ressources/ili2gpkg-5.3.1/
  - DGIF_V3.ili in models/
  - OFMX files (local directory, embedded or isolated format)

Usage:
    python etl_ofm_to_dgif.py --ofmx-dir ressources/ofmx_ls/embedded
    python etl_ofm_to_dgif.py --ofmx-dir ressources/ofmx_ls/embedded --tmp-dir C:/tmp/dgif_ofm
"""

import argparse
import os
import sys

# Set UTF-8 encoding for stdout/stderr on Windows
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
import subprocess
import time
from pathlib import Path

from interlis_tool_paths import (
    describe_configured_interlis_tools,
    resolve_interlis_tool_path,
    should_log,
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
    """Prepend QGIS DLL directories to PATH and set GDAL/PROJ env vars."""
    if qgis_root is None:
        qgis_root = _find_qgis_root()
    if qgis_root is None:
        return
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


_setup_qgis_env()


# ============================================================================
# ANSI colour helpers
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


# ============================================================================
# Discover OFMX files
# ============================================================================
def discover_ofmx_files(ofmx_dir: Path) -> list[Path]:
    """Discover OFMX XML files in a directory.
    
    Looks for:
      - ofmx_ls (embedded format, directory with XML files)
      - ofmx_ls.ofmx (isolated format, single file)
      - *.ofmx (any OFMX file)
    """
    ofmx_files = []

    if not ofmx_dir.is_dir():
        return ofmx_files

    # Isolated format: single .ofmx file
    for f in ofmx_dir.glob("*.ofmx"):
        if f.is_file():
            ofmx_files.append(f)

    # Embedded format: directory listing
    if ofmx_dir.is_dir():
        # Check if ofmx_dir itself or a subdirectory contains ofmx_ls root
        ofmx_root = ofmx_dir / "ofmx_ls"
        if ofmx_root.is_dir():
            ofmx_dir = ofmx_root

        # Look for main XML root file(s)
        for f in ofmx_dir.glob("ofmx_ls"):
            if f.is_file() and not f.suffix:
                ofmx_files.append(f)
                break  # Embedded format typically has one root

    return ofmx_files


# ============================================================================
# Main
# ============================================================================
def main() -> int:
    parser = argparse.ArgumentParser(
        description="ETL Pipeline: OpenFlightMaps OFMX → DGIF GeoPackage"
    )
    parser.add_argument(
        "--ofmx-dir",
        required=True,
        help="Directory containing OFMX files (embedded format in subdirectory "
             "or isolated .ofmx files). E.g. ressources/ofmx_ls/embedded",
    )
    parser.add_argument(
        "--tmp-dir",
        default="C:/tmp/dgif_ofm",
        help="Temporary working directory (default: C:/tmp/dgif_ofm)",
    )
    parser.add_argument(
        "--output-name",
        default="DGIF_OFM.gpkg",
        help="Output DGIF GeoPackage filename (default: DGIF_OFM.gpkg)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for DGIF GeoPackage (default: workspace/output)",
    )
    parser.add_argument(
        "--output-gpkg",
        default=None,
        help="Full output GeoPackage path; overrides --output-dir/--output-name",
    )
    parser.add_argument(
        "--ili-model",
        default=None,
        help="Path to DGIF INTERLIS model (.ili). Defaults to workspace models/DGIF_V3.ili",
    )
    parser.add_argument(
        "--skip-schema",
        action="store_true",
        help="Skip DGIF GeoPackage schema creation if output already exists",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Path to Python interpreter with GDAL (default: current interpreter)",
    )
    parser.add_argument(
        "--target-topics",
        default=None,
        help="Comma-separated DGIF topics to write (e.g. Foundation,Airspace,AeronauticalFacility)",
    )
    args = parser.parse_args()

    # ========================================================================
    # Configuration
    # ========================================================================
    workspace_root = Path(__file__).resolve().parent.parent
    ili2gpkg_jar = resolve_interlis_tool_path("ili2gpkg")
    if args.ili_model:
        dgif_ili = Path(args.ili_model).expanduser()
    else:
        dgif_ili = workspace_root / "models" / "DGIF_V3.ili"
    transform_py = workspace_root / "scripts" / "etl_ofm_transform_v2.py"
    python_exe = args.python

    output_dir = Path(args.output_dir).expanduser() if args.output_dir else (workspace_root / "output")
    dgif_gpkg = output_dir / args.output_name
    if args.output_gpkg:
        dgif_gpkg = Path(args.output_gpkg).expanduser()
        output_dir = dgif_gpkg.parent

    tmp_dir = Path(args.tmp_dir)
    ofmx_dir = Path(args.ofmx_dir)

    # Model directories (semicolon-separated, as expected by ili2gpkg)
    models_dir = workspace_root / "models"
    model_dirs = [str(dgif_ili.parent), str(models_dir)]
    dedup_model_dirs: list[str] = []
    for model_dir in model_dirs:
        if model_dir not in dedup_model_dirs:
            dedup_model_dirs.append(model_dir)
    dgif_model_dir = f"{';'.join(dedup_model_dirs)};http://models.interlis.ch/;%JAR_DIR"

    # ========================================================================
    # Banner
    # ========================================================================
    banner("ETL Pipeline: OpenFlightMaps OFMX → DGIF GeoPackage")
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

    # Required files
    for f in (dgif_ili, transform_py):
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

    # OFMX directory
    if not ofmx_dir.is_dir():
        error(f"OFMX directory not found: {ofmx_dir}")
        return 1
    ok(f"OFMX dir: {ofmx_dir}")

    # Temp directory
    tmp_dir.mkdir(parents=True, exist_ok=True)
    ok(f"Temp dir: {tmp_dir}")
    print()

    # ========================================================================
    # Phase 1 — Discover OFMX files
    # ========================================================================
    banner("Phase 1: Discover OFMX Files")

    ofmx_files = discover_ofmx_files(ofmx_dir)
    if not ofmx_files:
        error(f"No OFMX files found in {ofmx_dir}")
        return 1

    for f in ofmx_files:
        info(f"Found OFMX: {f} ({file_size_mb(f):.1f} MB)")
    print()

    # ========================================================================
    # Phase 2 — Create DGIF GeoPackage (schema only)
    # ========================================================================
    banner("Phase 2: Create DGIF GeoPackage Schema")

    output_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_schema and dgif_gpkg.exists():
        skip(f"Using existing DGIF schema: {dgif_gpkg}")
    else:
        if dgif_gpkg.exists():
            dgif_gpkg.unlink()
            info(f"Removed existing: {dgif_gpkg}")

        info("Generating DGIF GeoPackage schema via ili2gpkg...")
        java_args = [
            "-jar", str(ili2gpkg_jar),
            "--schemaimport",
            "--dbfile", str(dgif_gpkg),
            "--defaultSrsAuth", "EPSG",
            "--defaultSrsCode", "4326",
            "--smart2Inheritance",
            "--nameByTopic",
            "--gpkgMultiGeomPerTable",
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
            "--log", str(tmp_dir / "ili2gpkg_schemaimport.log"),
            str(dgif_ili),
        ]

        returncode = run_java(java_args, "Running ili2gpkg...")
        if returncode != 0:
            error(f"ili2gpkg failed with exit code {returncode}")
            return 1
        ok(f"Created DGIF GeoPackage: {dgif_gpkg} ({file_size_mb(dgif_gpkg):.1f} MB)")
    print()

    # ========================================================================
    # Phase 3 — Transform and Load: OFMX → DGIF
    # ========================================================================
    banner("Phase 3: Transform and Load OFMX → DGIF")

    info("Launching transform script...")
    transform_args = [
        python_exe,
        str(transform_py),
        "--dgif-gpkg", str(dgif_gpkg),
        "--ofmx-files",
        *[str(f) for f in ofmx_files],
        *([
            "--target-topics",
            args.target_topics,
        ] if args.target_topics else []),
    ]

    t0 = time.perf_counter()
    proc = subprocess.Popen(
        transform_args,
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
    elapsed = time.perf_counter() - t0

    if proc.returncode != 0:
        error(f"Transform failed with exit code {proc.returncode}")
        return 1
    ok(f"Transform completed in {elapsed:.1f}s")
    print()

    # ========================================================================
    # Phase 4 -- Validation: Geometry, Topology, Constraints
    # ========================================================================
    banner("Phase 4: Validate DGIF GeoPackage")

    validate_py = workspace_root / "scripts" / "etl_ofm_validate.py"
    if not validate_py.exists():
        warn(f"Validation script not found: {validate_py}")
    else:
        info("Running validation checks...")
        validate_args = [
            python_exe,
            str(validate_py),
            "--dgif-gpkg", str(dgif_gpkg),
        ]

        t0 = time.perf_counter()
        proc = subprocess.Popen(
            validate_args,
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
        elapsed = time.perf_counter() - t0

        if proc.returncode != 0:
            warn(f"Validation script exited with code {proc.returncode}")
        else:
            ok(f"Validation completed in {elapsed:.1f}s")
    print()

    # ========================================================================
    # Complete
    # ========================================================================
    banner("ETL Complete")
    ok(f"Output GeoPackage: {dgif_gpkg}")
    ok(f"Size: {file_size_mb(dgif_gpkg):.1f} MB")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
