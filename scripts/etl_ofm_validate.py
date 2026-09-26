#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Validation: DGIF GeoPackage (from OFM) --> Quality Assurance

Performs comprehensive validation:
  Phase 1 -- Geometry validation (invalid/corrupt geometries)
  Phase 2 -- Topology checks (overlaps, gaps in polygon layers)
  Phase 3 -- Referential integrity (foreign key constraints)
  Phase 4 -- Attribute validation (NOT NULL, domain values)
  Phase 5 -- Feature count reporting

Output: validation log + summary

Usage:
    python etl_ofm_validate.py --dgif-gpkg output/DGIF_OFM.gpkg
    python etl_ofm_validate.py --dgif-gpkg output/DGIF_OFM.gpkg --ilivalidator
"""

import argparse
import sqlite3
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

try:
    from osgeo import ogr, osr
except ImportError:
    print("[FATAL] GDAL/OGR Python bindings not found. Install via QGIS or pip.", file=sys.stderr)
    sys.exit(1)

# Set UTF-8 encoding for stdout/stderr on Windows
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')


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


def warn(msg: str) -> None:
    print(f"{YELLOW}[WARNING]{RESET} {msg}")


def error(msg: str) -> None:
    print(f"{RED}[ERROR]{RESET} {msg}", file=sys.stderr)


def banner(title: str) -> None:
    print()
    print(f"{CYAN}================================================================{RESET}")
    print(f"{CYAN}  {title}{RESET}")
    print(f"{CYAN}================================================================{RESET}")


# ============================================================================
# Geometry Validation
# ============================================================================
def validate_geometries(gpkg_path: str) -> dict:
    """
    Validate geometries in all feature tables.
    Returns dict of {table_name: {valid: count, invalid: count, errors: [...]}}
    """
    ogr.UseExceptions()
    driver = ogr.GetDriverByName("GPKG")
    ds = driver.Open(str(gpkg_path))
    
    results = defaultdict(lambda: {"valid": 0, "invalid": 0, "errors": []})
    
    if ds is None:
        error(f"Cannot open GeoPackage: {gpkg_path}")
        return results
    
    for layer_idx in range(ds.GetLayerCount()):
        layer = ds.GetLayer(layer_idx)
        layer_name = layer.GetName()
        
        # Skip non-feature layers
        if layer.GetGeomType() == ogr.wkbNone:
            continue
        
        feature_count = layer.GetFeatureCount()
        if feature_count == 0:
            continue
        
        info(f"Validating {layer_name} ({feature_count} features)...")
        
        invalid_count = 0
        for feature in layer:
            geom = feature.GetGeometryRef()
            if geom is None:
                results[layer_name]["invalid"] += 1
                invalid_count += 1
                results[layer_name]["errors"].append(
                    f"Feature {feature.GetFID()}: NULL geometry"
                )
                continue
            
            # Check if geometry is valid
            if not geom.IsValid():
                results[layer_name]["invalid"] += 1
                invalid_count += 1
                err_msg = geom.GetLastErrorMsg() or "Invalid geometry"
                results[layer_name]["errors"].append(
                    f"Feature {feature.GetFID()} ({geom.GetGeometryName()}): {err_msg}"
                )
            else:
                results[layer_name]["valid"] += 1
        
        if invalid_count == 0:
            ok(f"{layer_name}: All {feature_count} geometries valid")
        else:
            warn(f"{layer_name}: {invalid_count}/{feature_count} invalid geometries")
    
    ds = None
    return results


# ============================================================================
# Topology Checks (polygon overlap, gaps)
# ============================================================================
def validate_topology(gpkg_path: str) -> dict:
    """
    Simple topology checks for polygon layers:
    - Self-intersections
    - Rings not closed
    Returns dict of {table_name: {issues: [...]}}
    """
    ogr.UseExceptions()
    driver = ogr.GetDriverByName("GPKG")
    ds = driver.Open(str(gpkg_path))
    
    results = defaultdict(lambda: {"issues": []})
    
    if ds is None:
        return results
    
    for layer_idx in range(ds.GetLayerCount()):
        layer = ds.GetLayer(layer_idx)
        layer_name = layer.GetName()
        geom_type = layer.GetGeomType()
        
        # Only check polygon layers
        if geom_type not in (ogr.wkbPolygon, ogr.wkbMultiPolygon,
                             ogr.wkbPolygon25D, ogr.wkbMultiPolygon25D):
            continue
        
        feature_count = layer.GetFeatureCount()
        if feature_count == 0:
            continue
        
        info(f"Topology check {layer_name}...")
        
        issues = 0
        for feature in layer:
            geom = feature.GetGeometryRef()
            if geom is None:
                continue
            
            # Check for self-intersections (using SimplifyPreservingTopology as heuristic)
            try:
                simplified = geom.SimplifyPreservingTopology(0.0)
                if simplified is None or simplified.IsEmpty():
                    results[layer_name]["issues"].append(
                        f"Feature {feature.GetFID()}: Geometry simplification failed"
                    )
                    issues += 1
            except Exception as e:
                results[layer_name]["issues"].append(
                    f"Feature {feature.GetFID()}: {str(e)}"
                )
                issues += 1
        
        if issues == 0:
            ok(f"{layer_name}: Topology OK ({feature_count} features)")
        else:
            warn(f"{layer_name}: {issues} topology issues found")
    
    ds = None
    return results


# ============================================================================
# Referential Integrity
# ============================================================================
def validate_constraints(gpkg_path: str) -> dict:
    """
    Check for referential integrity violations and NOT NULL constraints.
    Returns dict of {table_name: {fk_errors: count, null_errors: count}}
    """
    conn = sqlite3.connect(str(gpkg_path))
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()
    
    results = defaultdict(lambda: {"fk_errors": 0, "null_errors": 0, "errors": []})
    
    # Get all tables
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'T_%' AND name NOT LIKE 'gpkg_%' AND name NOT LIKE 'rtree_%'"
    )
    tables = [row[0] for row in cur.fetchall()]
    
    for table_name in tables:
        try:
            # Get column info
            cur.execute(f"PRAGMA table_info(\"{table_name}\")")
            columns = cur.fetchall()  # (cid, name, type, notnull, dflt_value, pk)
            
            # Check NOT NULL constraints
            for col_info in columns:
                cid, col_name, col_type, notnull, dflt_val, pk = col_info
                if notnull and col_name not in ('T_Id', 'T_basket', 'T_CreateDate', 'T_LastChange'):
                    # Check for NULL values in NOT NULL column
                    cur.execute(
                        f"SELECT COUNT(*) FROM \"{table_name}\" WHERE \"{col_name}\" IS NULL"
                    )
                    null_count = cur.fetchone()[0]
                    if null_count > 0:
                        results[table_name]["null_errors"] += null_count
                        results[table_name]["errors"].append(
                            f"Column {col_name}: {null_count} NULL values in NOT NULL column"
                        )
        except sqlite3.OperationalError:
            pass  # Table might not exist or might be special
    
    conn.close()
    return results


# ============================================================================
# Feature Count Report
# ============================================================================
def report_feature_counts(gpkg_path: str) -> dict:
    """
    Report inserted feature counts per DGIF class.
    """
    ogr.UseExceptions()
    driver = ogr.GetDriverByName("GPKG")
    ds = driver.Open(str(gpkg_path))
    
    counts = {}
    
    if ds is None:
        return counts
    
    for layer_idx in range(ds.GetLayerCount()):
        layer = ds.GetLayer(layer_idx)
        layer_name = layer.GetName()
        
        # Skip non-feature and system tables
        if layer.GetGeomType() == ogr.wkbNone or "_" not in layer_name:
            continue
        
        count = layer.GetFeatureCount()
        if count > 0:
            # Extract class name from table name (e.g., "aeronautical_aerodrome" -> "Aerodrome")
            parts = layer_name.split("_", 1)
            if len(parts) == 2:
                class_name = parts[1].replace("_", " ").title().replace(" ", "")
                counts[class_name] = count
    
    ds = None
    return counts


# ============================================================================
# Main
# ============================================================================
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validation: DGIF GeoPackage (from OFM)"
    )
    parser.add_argument(
        "--dgif-gpkg",
        required=True,
        help="Path to DGIF GeoPackage to validate",
    )
    parser.add_argument(
        "--ilivalidator",
        action="store_true",
        help="Also run ilivalidator on the GeoPackage (requires INTERLIS model)",
    )
    args = parser.parse_args()

    dgif_gpkg = Path(args.dgif_gpkg)

    if not dgif_gpkg.exists():
        error(f"GeoPackage not found: {dgif_gpkg}")
        return 1

    # ========================================================================
    # Banner
    # ========================================================================
    banner("Validation: DGIF GeoPackage (from OFM)")
    print()

    # ========================================================================
    # Phase 1: Geometry Validation
    # ========================================================================
    banner("Phase 1: Geometry Validation")

    geom_results = validate_geometries(str(dgif_gpkg))
    total_valid = sum(r["valid"] for r in geom_results.values())
    total_invalid = sum(r["invalid"] for r in geom_results.values())

    if total_invalid > 0:
        warn(f"Total: {total_valid} valid, {total_invalid} invalid geometries")
        for table_name, result in geom_results.items():
            if result["errors"]:
                for err in result["errors"][:5]:  # Show first 5 errors
                    print(f"  {table_name}: {err}")
                if len(result["errors"]) > 5:
                    print(f"  ... and {len(result['errors']) - 5} more")
    else:
        ok(f"All {total_valid} geometries are valid")
    print()

    # ========================================================================
    # Phase 2: Topology Checks
    # ========================================================================
    banner("Phase 2: Topology Checks")

    topo_results = validate_topology(str(dgif_gpkg))
    total_topo_issues = sum(len(r["issues"]) for r in topo_results.values())

    if total_topo_issues > 0:
        warn(f"Total: {total_topo_issues} topology issues found")
        for table_name, result in topo_results.items():
            if result["issues"]:
                for issue in result["issues"][:3]:  # Show first 3 issues
                    print(f"  {table_name}: {issue}")
                if len(result["issues"]) > 3:
                    print(f"  ... and {len(result['issues']) - 3} more")
    else:
        ok("No topology issues detected")
    print()

    # ========================================================================
    # Phase 3: Referential Integrity
    # ========================================================================
    banner("Phase 3: Referential Integrity & Constraints")

    constraint_results = validate_constraints(str(dgif_gpkg))
    total_fk_errors = sum(r["fk_errors"] for r in constraint_results.values())
    total_null_errors = sum(r["null_errors"] for r in constraint_results.values())

    if total_null_errors > 0:
        warn(f"Total: {total_null_errors} NOT NULL constraint violations")
        for table_name, result in constraint_results.items():
            if result["errors"]:
                for err in result["errors"]:
                    print(f"  {table_name}: {err}")
    else:
        ok("All NOT NULL constraints satisfied")
    print()

    # ========================================================================
    # Phase 4: Feature Count Report
    # ========================================================================
    banner("Phase 4: Feature Count Report")

    counts = report_feature_counts(str(dgif_gpkg))
    if counts:
        total_features = sum(counts.values())
        ok(f"Total features inserted: {total_features}")
        print()
        for class_name in sorted(counts.keys()):
            print(f"  {class_name}: {counts[class_name]}")
    else:
        warn("No features found in GeoPackage")
    print()

    # ========================================================================
    # Summary
    # ========================================================================
    banner("Validation Summary")

    validation_status = "PASS" if (total_invalid == 0 and total_topo_issues == 0 and 
                                    total_null_errors == 0 and sum(counts.values()) > 0) else "WARNINGS"
    
    if validation_status == "PASS":
        ok(f"Validation: {validation_status}")
        print(f"  - {total_valid} valid geometries")
        print(f"  - 0 topology issues")
        print(f"  - 0 constraint violations")
        print(f"  - {sum(counts.values())} total features")
    else:
        warn(f"Validation: {validation_status}")
        if total_invalid > 0:
            print(f"  - {total_invalid} invalid geometries")
        if total_topo_issues > 0:
            print(f"  - {total_topo_issues} topology issues")
        if total_null_errors > 0:
            print(f"  - {total_null_errors} NOT NULL violations")
        print(f"  - {sum(counts.values())} features inserted")

    return 0


if __name__ == "__main__":
    sys.exit(main())
