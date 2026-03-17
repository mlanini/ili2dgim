#!/usr/bin/env python3
"""
DGIF GeoPackage Generator (ili2gpkg 5.3.1)

Generates a GeoPackage conforming to the DGIWG profile (STD-08-006) from the
INTERLIS 2.4 model DGIF_V3.ili using ili2gpkg 5.3.1.

Inheritance strategy:
  --smart2Inheritance (NewAndSubClass) is used, meaning each concrete class
  gets its own table with all inherited attributes (Entity + FeatureEntity)
  inlined.  This ensures every feature table has its own geometry column.
  The DGIF hierarchy (Entity → FeatureEntity → 600+ concrete classes) is
  "flattened" so that each table is self-contained.

Prerequisites:
  - Java 8+ (java in PATH)
  - ili2gpkg-5.3.1.jar in ressources/ili2gpkg-5.3.1/
  - DGIF_V3.ili in models/
"""

import os
import subprocess
import sys
import time
from pathlib import Path

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
    print(f"{CYAN}[INFO]{RESET} {msg}")


def ok(msg: str) -> None:
    print(f"{GREEN}[OK]{RESET} {msg}")


def error(msg: str) -> None:
    print(f"{RED}[ERROR]{RESET} {msg}", file=sys.stderr)


def banner(title: str) -> None:
    print(f"{CYAN}============================================================{RESET}")
    print(f"{CYAN}  {title}{RESET}")
    print(f"{CYAN}============================================================{RESET}")


def file_size_mb(path: Path) -> float:
    return round(os.path.getsize(path) / (1024 * 1024), 2)


# ============================================================================
# Main
# ============================================================================
def main() -> int:
    # ========================================================================
    # Configuration
    # ========================================================================
    workspace_root = Path(__file__).resolve().parent.parent
    ili2gpkg_jar = workspace_root / "ressources" / "ili2gpkg-5.3.1" / "ili2gpkg-5.3.1.jar"
    ili_model = workspace_root / "models" / "DGIF_V3.ili"
    output_dir = workspace_root / "output"
    models_dir = workspace_root / "models"
    gpkg_file = output_dir / "DGIF_V3.gpkg"
    log_file = output_dir / "ili2gpkg_schemaimport.log"

    # Model directory: models folder (where .ili lives) + standard repository
    model_dir = f"{models_dir};http://models.interlis.ch/;%JAR_DIR"

    # ========================================================================
    # Banner
    # ========================================================================
    banner("DGIF GeoPackage Generator (ili2gpkg 5.3.1)")
    print()

    # ========================================================================
    # Prerequisites check
    # ========================================================================

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
    if not ili2gpkg_jar.exists():
        error(f"ili2gpkg not found: {ili2gpkg_jar}")
        return 1
    ok(f"ili2gpkg: {ili2gpkg_jar}")

    # INTERLIS model
    if not ili_model.exists():
        error(f"INTERLIS model not found: {ili_model}")
        return 1
    ok(f"Model:  {ili_model}")
    print()

    # ========================================================================
    # Remove existing GeoPackage
    # ========================================================================
    if gpkg_file.exists():
        info(f"Removing existing GeoPackage: {gpkg_file}")
        gpkg_file.unlink()

    # ========================================================================
    # Schema import
    # ========================================================================
    info(f"Output: {gpkg_file}")
    info(f"Log:    {log_file}")
    print()

    ili2gpkg_args = [
        "java",
        "-jar", str(ili2gpkg_jar),

        # --- Operation ---
        "--schemaimport",

        # --- Database ---
        "--dbfile", str(gpkg_file),

        # --- SRS: WGS84 (DGIWG profile) ---
        "--defaultSrsAuth", "EPSG",
        "--defaultSrsCode", "4326",

        # --- Inheritance: NewAndSubClass (each concrete class gets all columns) ---
        "--smart2Inheritance",

        # --- Table names: Topic.Class ---
        "--nameByTopic",

        # --- Geometry ---
        "--gpkgMultiGeomPerTable",  # Multi-geometry per table
        "--createGeomIdx",          # Spatial indices (DGIWG requirement)
        "--strokeArcs",             # Arcs → line segments (GeoPackage compat.)

        # --- Enumerations ---
        "--createEnumTabs",         # Lookup tables with enum values
        "--createEnumTxtCol",       # Additional _txt column
        "--beautifyEnumDispName",   # Human-readable display names

        # --- Metadata and traceability ---
        "--createBasketCol",        # T_basket column (topic container)
        "--createTidCol",           # T_Ili_Tid column (Transfer-ID)
        "--createStdCols",          # T_User, T_CreateDate, T_LastChange
        "--createMetaInfo",         # Meta-tables from INTERLIS model

        # --- Referential integrity ---
        "--createFk",               # Foreign key constraints
        "--createFkIdx",            # Indices on FKs

        # --- Model path ---
        "--modeldir", model_dir,

        # --- Logging ---
        "--log", str(log_file),

        # --- Model file ---
        str(ili_model),
    ]

    print(f"{GREY}Command:{RESET}")
    print(f"  {GREY}{' '.join(ili2gpkg_args)}{RESET}")
    print()

    t0 = time.perf_counter()
    proc = subprocess.Popen(
        ili2gpkg_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    for line in proc.stdout:  # type: ignore[union-attr]
        print(line.rstrip())
    proc.wait()
    elapsed = time.perf_counter() - t0

    print()
    banner("Result")

    if proc.returncode == 0:
        size_mb = file_size_mb(gpkg_file)
        ok(f"GeoPackage created in {elapsed:.1f}s")
        info(f"File: {gpkg_file}")
        info(f"Size: {size_mb} MB")
        info(f"Log:  {log_file}")
    else:
        error(f"schemaimport failed (exit code: {proc.returncode})")
        info(f"Check the log: {log_file}")

    print(f"{CYAN}============================================================{RESET}")
    return proc.returncode


if __name__ == "__main__":
    sys.exit(main())
