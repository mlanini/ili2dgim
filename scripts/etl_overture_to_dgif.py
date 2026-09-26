#!/usr/bin/env python3
"""
ETL Pipeline: Overture Maps -> DGIF GeoPackage

Orchestrates the full ETL process:
    Phase 1  - Discover or download Overture data files for AOI
    Phase 2  - Create empty DGIF GeoPackage (schema import from DGIF_V3.ili)
    Phase 3  - Transform and load: apply mapping table, insert into DGIF GPKG

Supported input formats: GeoParquet (.parquet, .geoparquet) and
GeoJSON (.geojson, .json).  Files should be named:
    overture_{theme}_{type}.parquet   (or .geoparquet / .geojson / .json)
    e.g. overture_buildings_building.parquet
         overture_transportation_segment.geojson

Prerequisites:
  - Java 8+ (java in PATH)
  - Python 3.12 with GDAL/OGR (QGIS bundled, Parquet driver required)
  - ili2gpkg 5.3.1 in ressources/ili2gpkg-5.3.1/
  - DGIF_V3.ili in models/
    - Overture_to_DGIF_V3.csv in models/
    - Optional internet access (for direct AOI downloads)

Usage:
    python etl_overture_to_dgif.py --parquet-dir C:/tmp/overture_parquet
    python etl_overture_to_dgif.py --aoi-bbox 5.9,45.8,10.5,47.8 --themes buildings,transportation
    python etl_overture_to_dgif.py --aoi-file C:/tmp/aoi.geojson
"""

import argparse
from collections import deque
import json
import os
import re
import shutil
import subprocess
import sys
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
OVERTURE_DOWNLOAD_DOC_URL = "https://docs.overturemaps.org/getting-data/#download-by-area-of-interest"


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


def warn_overture_download_help(reason: str) -> None:
    warn(
        "Online AOI download unavailable "
        f"({reason}). See Overture guide: {OVERTURE_DOWNLOAD_DOC_URL}"
    )


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
# Overture Maps themes/types — expected file naming convention
# ============================================================================
# Files must be named overture_{theme}_{type}.{ext}
# Supported extensions: .parquet, .geoparquet, .geojson, .json
OVERTURE_THEME_TYPES = [
    ("buildings", "building", "Buildings"),
    ("buildings", "building_part", "Building parts"),
    ("transportation", "segment", "Transportation segments"),
    ("transportation", "connector", "Transportation connectors"),
    ("base", "infrastructure", "Base infrastructure"),
    ("base", "land", "Base land"),
    ("base", "land_cover", "Base land cover"),
    ("base", "land_use", "Base land use"),
    ("base", "water", "Base water"),
    ("places", "place", "Places (POIs)"),
    ("divisions", "division", "Administrative divisions"),
    ("divisions", "division_area", "Administrative areas"),
    ("divisions", "division_boundary", "Administrative boundaries"),
    ("addresses", "address", "Addresses"),
]


def parse_bbox(raw_bbox: str) -> tuple[float, float, float, float]:
    parts = [p.strip() for p in str(raw_bbox).split(",")]
    if len(parts) != 4:
        raise ValueError("AOI bbox must be minx,miny,maxx,maxy")
    minx, miny, maxx, maxy = [float(p) for p in parts]
    if minx >= maxx or miny >= maxy:
        raise ValueError("Invalid AOI bbox extent")
    return minx, miny, maxx, maxy


def _iter_coords(node):
    if isinstance(node, (list, tuple)):
        if node and isinstance(node[0], (int, float)):
            if len(node) >= 2:
                yield float(node[0]), float(node[1])
            return
        for child in node:
            yield from _iter_coords(child)


def bbox_from_geojson(aoi_path: Path) -> tuple[float, float, float, float]:
    payload = json.loads(aoi_path.read_text(encoding="utf-8"))
    geometry = None

    if isinstance(payload, dict) and payload.get("type") == "Feature":
        geometry = payload.get("geometry")
    elif isinstance(payload, dict) and payload.get("type") == "FeatureCollection":
        features = payload.get("features") or []
        if features:
            geometry = {"type": "GeometryCollection", "geometries": [f.get("geometry") for f in features if f.get("geometry")]}
    elif isinstance(payload, dict) and payload.get("type") in {
        "Polygon", "MultiPolygon", "Point", "MultiPoint", "LineString", "MultiLineString", "GeometryCollection"
    }:
        geometry = payload

    if not geometry:
        raise ValueError(f"Invalid AOI GeoJSON in {aoi_path}")

    coords = []
    if geometry.get("type") == "GeometryCollection":
        for geom in geometry.get("geometries") or []:
            coords.extend(list(_iter_coords((geom or {}).get("coordinates"))))
    else:
        coords = list(_iter_coords(geometry.get("coordinates")))

    if not coords:
        raise ValueError(f"No coordinates found in AOI GeoJSON: {aoi_path}")

    xs = [xy[0] for xy in coords]
    ys = [xy[1] for xy in coords]
    minx, maxx = min(xs), max(xs)
    miny, maxy = min(ys), max(ys)
    if minx >= maxx or miny >= maxy:
        raise ValueError("AOI geometry has zero area bounding box")
    return minx, miny, maxx, maxy


def bbox_from_wkt(aoi_wkt: str) -> tuple[float, float, float, float]:
    try:
        from osgeo import ogr
    except Exception as exc:
        raise RuntimeError(
            "GDAL/OGR is required to parse --aoi-wkt. "
            "Use --aoi-file/--aoi-bbox or fix QGIS GDAL runtime."
        ) from exc

    geom = ogr.CreateGeometryFromWkt(aoi_wkt)
    if geom is None:
        raise ValueError("Invalid AOI WKT")
    env = geom.GetEnvelope()  # (minx, maxx, miny, maxy)
    minx, maxx, miny, maxy = env
    if minx >= maxx or miny >= maxy:
        raise ValueError("AOI WKT has zero area bounding box")
    return minx, miny, maxx, maxy


def format_bbox(bbox: tuple[float, float, float, float]) -> str:
    return ",".join(f"{value:.8f}" for value in bbox)


def resolve_aoi_bbox(args) -> tuple[float, float, float, float] | None:
    if args.aoi_bbox:
        return parse_bbox(args.aoi_bbox)
    if args.aoi_file:
        aoi_path = Path(args.aoi_file)
        if not aoi_path.exists():
            raise FileNotFoundError(f"AOI file not found: {aoi_path}")
        return bbox_from_geojson(aoi_path)
    if args.aoi_wkt:
        return bbox_from_wkt(args.aoi_wkt)
    return None


def run_command_streaming(cmd: list[str], label: str) -> int:
    info(label)
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


def _to_duckdb_path(path: Path) -> str:
    return str(path).replace("\\", "/")


def _duckdb_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def check_duckdb_available() -> tuple[bool, str]:
    exe = shutil.which("duckdb")
    if not exe:
        return False, "duckdb executable not found in PATH"
    probe = subprocess.run(
        [exe, "-c", "SELECT 1;"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if probe.returncode != 0:
        details = (probe.stderr or probe.stdout or "").strip()
        return False, details or "duckdb probe failed"
    return True, exe


def download_overture_by_aoi_duckdb(
    bbox: tuple[float, float, float, float],
    parquet_dir: Path,
    themes_filter: set[str] | None = None,
    out_format: str = "geoparquet",
) -> dict[tuple[str, str], Path]:
    """Fallback downloader using DuckDB SQL against Overture S3 parquet."""
    ok_duckdb, duckdb_info = check_duckdb_available()
    if not ok_duckdb:
        raise RuntimeError(
            "DuckDB fallback unavailable: "
            f"{duckdb_info}. Install DuckDB CLI or make it available in PATH."
        )

    duckdb_exe = duckdb_info
    parquet_dir.mkdir(parents=True, exist_ok=True)
    minx, miny, maxx, maxy = bbox

    if out_format == "geojson":
        driver = "GeoJSON"
        ext = ".geojson"
    else:
        driver = "Parquet"
        ext = ".parquet"

    for theme, otype, _ in OVERTURE_THEME_TYPES:
        if themes_filter and theme not in themes_filter:
            continue

        output_file = parquet_dir / f"overture_{theme}_{otype}{ext}"
        if output_file.exists() and output_file.stat().st_size > 0:
            skip(f"Reuse existing AOI download: {output_file.name}")
            continue

        output_literal = _duckdb_sql_literal(_to_duckdb_path(output_file))
        type_literal = _duckdb_sql_literal(otype)
        theme_literal = _duckdb_sql_literal(theme)

        sql = f"""
INSTALL spatial;
LOAD spatial;
INSTALL httpfs;
LOAD httpfs;
SET s3_region='us-west-2';
SET VARIABLE latest=(SELECT latest FROM 'https://stac.overturemaps.org/catalog.json');
COPY (
  SELECT *
  FROM read_parquet(
    's3://overturemaps-us-west-2/release/' || getvariable('latest') || '/theme={theme_literal}/type={type_literal}/*',
    filename=true,
    hive_partitioning=1
  )
  WHERE bbox.xmin <= {maxx}
    AND bbox.xmax >= {minx}
    AND bbox.ymin <= {maxy}
    AND bbox.ymax >= {miny}
) TO '{output_literal}' WITH (FORMAT GDAL, DRIVER '{driver}');
""".strip()

        rc = run_command_streaming(
            [duckdb_exe, "-c", sql],
            f"Downloading Overture {theme}/{otype} for AOI via DuckDB...",
        )
        if rc != 0:
            warn(f"DuckDB download failed for {theme}/{otype}; continuing with other types")
            continue

        if not output_file.exists() or output_file.stat().st_size == 0:
            warn(f"DuckDB produced no output for {theme}/{otype}")

    return discover_parquet_files(parquet_dir, themes_filter)


def check_overturemaps_available(python_exe: str) -> tuple[bool, str]:
    probe = subprocess.run(
        [python_exe, "-m", "overturemaps", "--help"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if probe.returncode == 0:
        return True, ""
    details = (probe.stderr or probe.stdout or "").strip()
    if len(details) > 1200:
        details = details[-1200:]
    return False, details


def download_overture_by_aoi(
    python_exe: str,
    bbox: tuple[float, float, float, float],
    parquet_dir: Path,
    themes_filter: set[str] | None = None,
    out_format: str = "geoparquet",
) -> dict[tuple[str, str], Path]:
    """Download Overture data by AOI using overturemaps, fallback to DuckDB."""
    bbox_str = format_bbox(bbox)
    parquet_dir.mkdir(parents=True, exist_ok=True)

    available, details = check_overturemaps_available(python_exe)
    if not available:
        warn_overture_download_help("overturemaps CLI/module not available")
        warn(
            "overturemaps is not available; trying DuckDB fallback. "
            f"Details: {details or 'module probe failed'}"
        )
        return download_overture_by_aoi_duckdb(
            bbox=bbox,
            parquet_dir=parquet_dir,
            themes_filter=themes_filter,
            out_format=out_format,
        )

    ext = ".parquet" if out_format == "geoparquet" else ".geojson"

    had_failure = False
    for theme, otype, _ in OVERTURE_THEME_TYPES:
        if themes_filter and theme not in themes_filter:
            continue

        output_file = parquet_dir / f"overture_{theme}_{otype}{ext}"
        if output_file.exists() and output_file.stat().st_size > 0:
            skip(f"Reuse existing AOI download: {output_file.name}")
            continue

        cmd = [
            python_exe,
            "-m",
            "overturemaps",
            "download",
            f"--bbox={bbox_str}",
            "-f",
            out_format,
            f"--type={otype}",
            "-o",
            str(output_file),
        ]

        rc = run_command_streaming(cmd, f"Downloading Overture {theme}/{otype} for AOI...")
        if rc != 0:
            had_failure = True
            warn(f"Download failed for {theme}/{otype}; continuing with other types")

    discovered = discover_parquet_files(parquet_dir, themes_filter)
    if discovered:
        return discovered

    if had_failure:
        warn_overture_download_help("overturemaps AOI download returned no usable files")
        warn("No files downloaded with overturemaps; trying DuckDB fallback.")
        return download_overture_by_aoi_duckdb(
            bbox=bbox,
            parquet_dir=parquet_dir,
            themes_filter=themes_filter,
            out_format=out_format,
        )

    return discovered


def discover_parquet_files(
    parquet_dir: Path,
    themes_filter: set[str] | None = None,
) -> dict[tuple[str, str], Path]:
    """Discover Overture data files in a directory.

    Looks for files named overture_{theme}_{type}.{ext} where ext is one of:
    .parquet, .geoparquet, .geojson, .json.

    Also recognises common download naming patterns such as
    ``overture-{release}-{type}-{bbox}.{ext}`` (Overture Maps Explorer)
    and ``{theme}_{type}.{ext}``.
    """
    _SUPPORTED_EXTS = [".parquet", ".geoparquet", ".geojson", ".json"]

    found: dict[tuple[str, str], Path] = {}

    if not parquet_dir.is_dir():
        return found

    for theme, otype, description in OVERTURE_THEME_TYPES:
        if themes_filter and theme not in themes_filter:
            continue

        # Try canonical names first (parquet preferred over geojson)
        candidates = []
        for ext in _SUPPORTED_EXTS:
            candidates.append(parquet_dir / f"overture_{theme}_{otype}{ext}")
            candidates.append(parquet_dir / f"{theme}_{otype}{ext}")
        for candidate in candidates:
            if candidate.exists():
                found[(theme, otype)] = candidate
                break

    # Fallback: scan all supported files and match by type keyword.
    # Handles naming patterns like: overture-{release}-{type}-{bbox}.{ext}
    # collected from the Overture Maps Explorer or other download tools.
    unmatched = [
        (theme, otype)
        for theme, otype, _ in OVERTURE_THEME_TYPES
        if (not themes_filter or theme in themes_filter)
        and (theme, otype) not in found
    ]
    if unmatched:
        all_files = []
        for ext in _SUPPORTED_EXTS:
            all_files.extend(sorted(parquet_dir.glob(f"*{ext}")))
        matched_paths = set(found.values())
        for pf in all_files:
            if pf in matched_paths:
                continue
            name_lower = pf.stem.lower()
            for theme, otype in list(unmatched):
                if (theme, otype) in found:
                    continue
                # Method 1: both theme and type keywords in filename
                if theme in name_lower and otype in name_lower:
                    found[(theme, otype)] = pf
                    break
                # Method 2: type as a distinct segment delimited by hyphens
                # e.g. overture-2026-03-18.0-land_cover-6.899,...
                if re.search(
                    rf'(?:^|[-]){re.escape(otype)}(?:[-.]|$)', name_lower
                ):
                    found[(theme, otype)] = pf
                    break

    return found


# ============================================================================
# Main
# ============================================================================
def main() -> int:
    parser = argparse.ArgumentParser(
        description="ETL Pipeline: Overture Maps -> DGIF GeoPackage"
    )
    parser.add_argument(
        "--parquet-dir",
        required=False,
        help="Directory containing pre-downloaded Overture data files "
             "(.parquet, .geoparquet, .geojson, .json). "
             "E.g. overture_buildings_building.parquet or .geojson. "
             "Optional when --aoi-file or --aoi-bbox is used.",
    )
    parser.add_argument(
        "--aoi-file",
        default=None,
        help="Path to AOI GeoJSON file (Feature/FeatureCollection/Geometry).",
    )
    parser.add_argument(
        "--aoi-wkt",
        default=None,
        help="AOI geometry as WKT (POLYGON/MULTIPOLYGON). Used for download bbox and transform clipping.",
    )
    parser.add_argument(
        "--aoi-bbox",
        default=None,
        help="AOI bbox as minx,miny,maxx,maxy (EPSG:4326).",
    )
    parser.add_argument(
        "--download-dir",
        default=None,
        help="Directory used for automatic Overture AOI downloads (default: <tmp-dir>/overture_download).",
    )
    parser.add_argument(
        "--download-format",
        choices=["geoparquet", "geojson"],
        default="geoparquet",
        help="Format for automatic AOI downloads (default: geoparquet).",
    )
    parser.add_argument(
        "--tmp-dir",
        default="C:/tmp/dgif_overture",
        help="Temporary working directory (default: C:/tmp/dgif_overture)",
    )
    parser.add_argument(
        "--output-name",
        default="DGIF_Overture.gpkg",
        help="Output DGIF GeoPackage filename (default: DGIF_Overture.gpkg)",
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
        "--skip-schema",
        action="store_true",
        help="Skip DGIF GeoPackage schema creation if output already exists",
    )
    parser.add_argument(
        "--themes",
        default=None,
        help="Comma-separated list of themes to process (default: all). "
             "E.g. --themes buildings,transportation,base",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Path to Python interpreter with GDAL (default: current interpreter)",
    )
    parser.add_argument(
        "--target-topics",
        default=None,
        help="Comma-separated DGIF topics to write (e.g. Foundation,Cultural,Transportation)",
    )
    parser.add_argument(
        "--ili-model",
        default=None,
        help="Path to DGIF INTERLIS model (.ili). Defaults to workspace models/DGIF_V3.ili",
    )
    parser.add_argument(
        "--mapping",
        default=None,
        help="Path to Overture mapping CSV. Defaults to models/Overture_to_DGIF_V3.csv.",
    )
    args = parser.parse_args()

    # Filter themes if specified
    themes_filter = None
    if args.themes:
        themes_filter = set(t.strip().lower() for t in args.themes.split(","))

    # ========================================================================
    # Configuration
    # ========================================================================
    workspace_root = Path(__file__).resolve().parent.parent
    ili2gpkg_jar = resolve_interlis_tool_path("ili2gpkg")
    mapping_csv = (
        Path(args.mapping).expanduser()
        if args.mapping
        else (workspace_root / "models" / "Overture_to_DGIF_V3.csv")
    )
    transform_py = workspace_root / "scripts" / "etl_overture_transform.py"
    python_exe = args.python

    output_dir = Path(args.output_dir).expanduser() if args.output_dir else (workspace_root / "output")
    dgif_gpkg = output_dir / args.output_name
    if args.output_gpkg:
        dgif_gpkg = Path(args.output_gpkg).expanduser()
        output_dir = dgif_gpkg.parent

    # Prefer explicit --ili-model; otherwise try output folder first so
    # UI runs that generate DGIF_V3.ili in custom output dirs keep working.
    if args.ili_model:
        dgif_ili = Path(args.ili_model).expanduser()
    else:
        default_model = workspace_root / "models" / "DGIF_V3.ili"
        output_model = output_dir / "DGIF_V3.ili"
        dgif_ili = output_model if output_model.exists() else default_model

    tmp_dir = Path(args.tmp_dir)
    parquet_dir = Path(args.parquet_dir).expanduser() if args.parquet_dir else None
    download_dir = Path(args.download_dir).expanduser() if args.download_dir else (tmp_dir / "overture_download")
    aoi_bbox = resolve_aoi_bbox(args)
    if aoi_bbox is None:
        error("AOI is required. Provide --aoi-file, --aoi-wkt, or --aoi-bbox.")
        return 1

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
    banner("ETL Pipeline: Overture Maps -> DGIF GeoPackage")
    print()
    if should_log("DEBUG"):
        for configured_tool in describe_configured_interlis_tools():
            info(f"Configured tool: {configured_tool}")
    info(f"DGIF model: {dgif_ili}")
    info(f"Mapping CSV: {mapping_csv}")
    info(f"Parquet dir: {parquet_dir if parquet_dir else '(not provided)'}")
    if aoi_bbox:
        info(f"AOI bbox:   {format_bbox(aoi_bbox)}")
    info(f"Themes:      {args.themes or 'all'}")
    print()

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
    for f in (dgif_ili, mapping_csv, transform_py):
        if not f.exists():
            error(f"File not found: {f}")
            return 1
        ok(f"{f.name}")

    # Python + GDAL + Parquet driver
    try:
        result = subprocess.run(
            [python_exe, "-c",
             "from osgeo import gdal, ogr; gdal.UseExceptions(); "
             "drv = ogr.GetDriverByName('Parquet'); "
             "print('GDAL', gdal.VersionInfo(), 'Parquet:', 'YES' if drv else 'NO')"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        ok(f"Python + {result.stdout.strip()}")
        if "Parquet: NO" in result.stdout:
            error("GDAL Parquet driver not available! Requires GDAL compiled with libarrow.")
            return 1
    except Exception as exc:
        error(f"Python/GDAL not available at: {python_exe} ({exc})")
        return 1

    # Directories
    tmp_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    print()

    # ========================================================================
    # Phase 1 - Discover or download Overture data files
    # ========================================================================
    banner("Phase 1: Discover or download Overture data files")

    parquet_files = {}
    if parquet_dir and parquet_dir.is_dir():
        parquet_files = discover_parquet_files(parquet_dir, themes_filter)

    if parquet_files:
        ok(f"Using pre-downloaded data from: {parquet_dir}")
    else:
        if parquet_dir and not parquet_dir.is_dir():
            warn(f"Parquet directory not found: {parquet_dir}")
        if aoi_bbox is not None:
            info(f"No local Overture input found; downloading by AOI into: {download_dir}")
            try:
                parquet_files = download_overture_by_aoi(
                    python_exe=python_exe,
                    bbox=aoi_bbox,
                    parquet_dir=download_dir,
                    themes_filter=themes_filter,
                    out_format=args.download_format,
                )
            except Exception as exc:
                warn_overture_download_help("all online download strategies failed")
                error(str(exc))
                return 1
            parquet_dir = download_dir
        else:
            error("No Overture input files found and no AOI specified for direct download.")
            info("Provide one of:")
            info("  - --parquet-dir C:/tmp/overture_parquet")
            info("  - --aoi-file C:/tmp/aoi.geojson")
            info("  - --aoi-bbox minx,miny,maxx,maxy")
            return 1

    if not parquet_files:
        error(f"No matching data files found in: {parquet_dir}")
        print()
        info("Expected file names (examples):")
        for theme, otype, desc in OVERTURE_THEME_TYPES[:5]:
            print(f"  overture_{theme}_{otype}.parquet    ({desc})")
            print(f"  overture_{theme}_{otype}.geojson    ({desc})")
        print(f"  ...")
        print()
        info("Download Overture data using one of:")
        info("  1. DuckDB CLI: duckdb -c \"COPY (SELECT * FROM read_parquet("
             "'s3://overturemaps-us-west-2/release/2025-05-21.0/"
             "theme=buildings/type=building/*') WHERE bbox.xmin >= 5.9 "
             "AND bbox.xmax <= 10.5 AND bbox.ymin >= 45.8 AND bbox.ymax <= 47.8) "
             "TO 'overture_buildings_building.parquet'\"")
        info("  2. overturemaps-py: overturemaps download --bbox=5.9,45.8,10.5,47.8 "
             "-f geoparquet --type=building -o overture_buildings_building.parquet")
        info("  3. Overture Maps Explorer: https://explore.overturemaps.org")
        return 1

    for (theme, otype), pf in sorted(parquet_files.items()):
        ok(f"{theme}/{otype}: {pf.name} ({file_size_mb(pf)} MB)")

    info(f"Found {len(parquet_files)} data file(s)")
    print()

    # ========================================================================
    # Phase 2 — Create empty DGIF GeoPackage (schema import)
    # ========================================================================
    banner("Phase 2: Create DGIF GeoPackage schema")

    if args.skip_schema and dgif_gpkg.exists():
        skip(f"Using existing DGIF GeoPackage: {dgif_gpkg} ({file_size_mb(dgif_gpkg)} MB)")
    else:
        if dgif_gpkg.exists():
            info(f"Removing existing: {dgif_gpkg}")
            dgif_gpkg.unlink()

        dgif_schema_log = tmp_dir / "dgif_schemaimport.log"
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
    # Phase 3 — Transform and Load (Python)
    # ========================================================================
    banner("Phase 3: Transform & Load (Python ETL)")

    # Build the list of parquet files as arguments
    parquet_args = []
    for (theme, otype), pf in sorted(parquet_files.items()):
        parquet_args.extend(["--parquet", f"{theme}/{otype}={pf}"])

    info("Running etl_overture_transform.py...")
    t0 = time.perf_counter()

    proc = subprocess.Popen(
        [
            python_exe,
            str(transform_py),
            "--dgif-gpkg", str(dgif_gpkg),
            "--mapping", str(mapping_csv),
            "--allow-empty",
            *([
                "--aoi-file",
                args.aoi_file,
            ] if args.aoi_file else []),
            *([
                "--aoi-wkt",
                args.aoi_wkt,
            ] if args.aoi_wkt and not args.aoi_file else []),
            *([
                "--target-topics",
                args.target_topics,
            ] if args.target_topics else []),
        ] + parquet_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    output_tail: deque[str] = deque(maxlen=40)
    assert proc.stdout is not None
    for line in proc.stdout:
        clean_line = line.rstrip()
        output_tail.append(clean_line)
        print(f"  {clean_line}")
    proc.wait()

    if proc.returncode != 0:
        error(f"Python transform failed with exit code {proc.returncode}.")
        if output_tail:
            print("[ERROR] Transform output tail:", file=sys.stderr)
            for tail_line in output_tail:
                print(f"[ERROR]   {tail_line}", file=sys.stderr)
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
    print(f"  {GREEN}Output:  {dgif_gpkg} ({final_size} MB){RESET}")
    print(f"  {GREY}Parquet: {parquet_dir}{RESET}")
    for (theme, otype), pf in sorted(parquet_files.items()):
        print(f"  {GREY}         {pf.name} ({file_size_mb(pf)} MB){RESET}")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
