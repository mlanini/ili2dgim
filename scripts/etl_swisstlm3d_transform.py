#!/usr/bin/env python3
"""
ETL Transform & Load: swissTLM3D → DGIF GeoPackage

Reads features from a temporary swissTLM3D GeoPackage (imported via ili2gpkg),
applies the mapping table (swissTLM3D_to_DGIF_V3.csv), reprojects from
LV95 (EPSG:2056) to WGS84 (EPSG:4326), and inserts features into a
DGIF-schema GeoPackage.

Usage:
    python etl_swisstlm3d_transform.py \
        --tlm-gpkg  C:/tmp/geodata/swisstlm3d_temp.gpkg \
        --dgif-gpkg output/DGIF_swissTLM3D.gpkg \
        --mapping   dgiwg_docs/swissTLM3D_to_DGIF_V3.csv
"""

import argparse
import csv
import datetime
import sqlite3
import sys
import uuid
from collections import defaultdict
from pathlib import Path

try:
    from osgeo import ogr, osr, gdal
    gdal.UseExceptions()
except ImportError:
    print("[FATAL] GDAL/OGR Python bindings not found. Install via QGIS or pip.", file=sys.stderr)
    sys.exit(1)

# ============================================================================
# DGIF class → DGIF topic mapping
# Derived from DGIF_BL.ili topic boundaries + class line numbers.
# With --nameByTopic the ili2gpkg table name is "Topic.Class"
# ============================================================================
DGIF_CLASS_TO_TOPIC = {
    # Foundation
    "GeneralLocation": "Foundation",
    # AeronauticalFacility
    "Aerodrome": "AeronauticalFacility",
    "Heliport": "AeronauticalFacility",
    "Runway": "AeronauticalFacility",
    "Taxiway": "AeronauticalFacility",
    # Agricultural
    "AllotmentArea": "Agricultural",
    "Orchard": "Agricultural",
    "Vineyard": "Agricultural",
    # Boundaries
    "AdministrativeDivision": "Boundaries",
    "BoundaryMonument": "Boundaries",
    "ConservationArea": "Boundaries",
    # Cultural (large topic — lines 1082‥2678)
    "AerationBasin": "Cultural",
    "Amenity": "Cultural",
    "AmusementPark": "Cultural",
    "Amphitheatre": "Cultural",
    "ArcheologicalSite": "Cultural",
    "Aerial": "Cultural",
    "Bench": "Cultural",
    "Borehole": "Cultural",
    "BotanicGarden": "Cultural",
    "Bridge": "Transportation",  # line 6621 → Transportation
    "Building": "Cultural",
    "BuildingOverhang": "Cultural",
    "Cable": "Cultural",
    "Cableway": "Cultural",
    "Cairn": "Cultural",
    "CampSite": "Cultural",
    "Cemetery": "Cultural",
    "Checkpoint": "Cultural",
    "CompactSurface": "Cultural",
    "ConstructionZone": "Transportation",  # line 6833 → Transportation
    "Conveyor": "Cultural",
    "CoolingTower": "Cultural",
    "Courtyard": "Cultural",
    "CulturalConservationArea": "Cultural",
    "DisposalSite": "Cultural",
    "EducationalAmenity": "Cultural",
    "ElectricPowerStation": "Cultural",
    "ElectricalPowerGenerator": "Cultural",
    "ExtractionMine": "Cultural",
    "Facility": "Cultural",
    "Fairground": "Cultural",
    "Fence": "Cultural",
    "FiringRange": "Cultural",
    "Flagpole": "Cultural",
    "Fountain": "Cultural",
    "GolfCourse": "Cultural",
    "GolfDrivingRange": "Cultural",
    "Installation": "Cultural",
    "InterestSite": "Cultural",
    "Lookout": "Cultural",
    "Market": "Cultural",
    "MedicalAmenity": "Cultural",
    "Monument": "Cultural",
    "NonBuildingStructure": "Cultural",
    "OverheadObstruction": "Cultural",
    "Park": "Cultural",
    "PicnicSite": "Cultural",
    "PowerSubstation": "Cultural",
    "PublicSquare": "Cultural",
    "Racetrack": "Cultural",
    "Ramp": "Cultural",
    "RecyclingSite": "Cultural",
    "Ruins": "Cultural",
    "SaltEvaporator": "Cultural",
    "SettlingPond": "Cultural",
    "SewageTreatmentPlant": "Cultural",
    "SkiJump": "Cultural",
    "SkiRun": "Cultural",
    "Smokestack": "Cultural",
    "SportsGround": "Cultural",
    "StorageTank": "Cultural",
    "SwimmingPool": "Cultural",
    "Tower": "Cultural",
    "TrainingSite": "Cultural",
    "UndergroundDwelling": "Cultural",
    "VehicleLot": "Cultural",
    "Wall": "Cultural",
    "Waterwork": "Cultural",
    "WindTurbine": "Cultural",
    "Zoo": "Cultural",
    # Elevation
    "GeomorphicExtreme": "Elevation",
    # HydrographicAidsNavigation
    "ShorelineConstruction": "HydrographicAidsNavigation",
    # InlandWater
    "Canal": "Transportation",   # line 6758 → Transportation
    "Dam": "InlandWater",
    "Ditch": "InlandWater",
    "Embankment": "Physiography",  # line 6036
    "InlandWaterbody": "InlandWater",
    "InundatedLand": "InlandWater",
    "Lock": "InlandWater",
    "River": "InlandWater",
    "Spring": "InlandWater",
    "Waterfall": "InlandWater",
    # MilitaryInstallationsDefensiveStructures
    "Fortification": "MilitaryInstallationsDefensiveStructures",
    # OceanEnvironment
    "Harbour": "PortsHarbours",  # line 6493
    "Island": "Physiography",    # line 6315
    # Physiography
    "Glacier": "Physiography",
    "Hill": "Physiography",
    "LandArea": "Physiography",
    "LandMorphologyArea": "Physiography",
    "MountainPass": "Physiography",
    "PermanentSnowIce": "Physiography",
    "RockFormation": "Physiography",
    "SoilSurfaceRegion": "Physiography",
    # Population
    "PopulatedPlace": "Population",
    "Neighbourhood": "Population",
    # Vegetation
    "Forest": "Vegetation",
    "Scrubland": "Vegetation",
    "ShrubLand": "Vegetation",
    "Tree": "Vegetation",
    # Transportation
    "FerryCrossing": "Transportation",
    "LandRoute": "Transportation",
    "LandTransportationWay": "Transportation",
    "Pipeline": "Transportation",
    "Railway": "Transportation",
    "RailwayYard": "Transportation",
    "RoadInterchange": "Transportation",
    "TransportationPlatform": "Transportation",
    "TransportationStation": "Transportation",
    "Tunnel": "Transportation",
    "VehicleBarrier": "Transportation",
    # PortsHarbours
    "Checkpoint_SU004": "Cultural",  # Checkpoint is in Cultural; SU004 alias
    # HydrographicAidsNavigation
    "CaveMouth": "Physiography",  # DB029 not in Hydro — reassign
}

# Fallback: if a class is not found above, try to discover it dynamically
# from the GeoPackage table list.


# ============================================================================
# Mapping row data class
# ============================================================================
class MappingRow:
    """One row from swissTLM3D_to_DGIF_V3.csv"""
    __slots__ = (
        "no", "tlm_topic", "tlm_class", "tlm_attr", "tlm_value",
        "geometry_type", "description",
        "dgif_class", "dgif_code",
        "dgif_attr1", "dgif_attr1_code", "dgif_val1", "dgif_val1_code",
        "dgif_attr2", "dgif_attr2_code", "dgif_val2", "dgif_val2_code",
    )

    def __init__(self, row: list[str]):
        self.no = row[0]
        self.tlm_topic = row[1]
        self.tlm_class = row[2]
        self.tlm_attr = row[3]
        self.tlm_value = row[4]
        self.geometry_type = row[5]
        self.description = row[6]
        self.dgif_class = row[7] if len(row) > 7 else ""
        self.dgif_code = row[8] if len(row) > 8 else ""
        self.dgif_attr1 = row[9] if len(row) > 9 else ""
        self.dgif_attr1_code = row[10] if len(row) > 10 else ""
        self.dgif_val1 = row[11] if len(row) > 11 else ""
        self.dgif_val1_code = row[12] if len(row) > 12 else ""
        self.dgif_attr2 = row[13] if len(row) > 13 else ""
        self.dgif_attr2_code = row[14] if len(row) > 14 else ""
        self.dgif_val2 = row[15] if len(row) > 15 else ""
        self.dgif_val2_code = row[16] if len(row) > 16 else ""

    @property
    def is_mapped(self) -> bool:
        """True if this row has a valid DGIF target (not 'not in DGIF')."""
        return bool(self.dgif_class) and self.description != "not in DGIF"


# ============================================================================
# Load mapping CSV
# ============================================================================
def load_mapping(csv_path: str) -> dict[tuple[str, str], list[MappingRow]]:
    """
    Returns dict keyed by (TLM_class, Objektart_value) → list[MappingRow].
    """
    mapping: dict[tuple[str, str], list[MappingRow]] = defaultdict(list)
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)  # skip header
        for row in reader:
            if not row or not row[0].strip():
                continue
            mr = MappingRow(row)
            if mr.is_mapped:
                mapping[(mr.tlm_class, mr.tlm_value)].append(mr)
    return mapping


# ============================================================================
# Discover TLM source tables in the GeoPackage
# ============================================================================
def discover_tlm_tables(gpkg_path: str) -> dict[str, str]:
    """
    Returns dict of TLM class name → actual GeoPackage table name.
    With --nameByTopic the tables are e.g. 'TLM_AREALE.TLM_FREIZEITAREAL'.
    """
    ds = ogr.Open(gpkg_path, 0)
    if ds is None:
        print(f"[ERROR] Cannot open TLM GeoPackage: {gpkg_path}", file=sys.stderr)
        sys.exit(1)

    tables = {}
    for i in range(ds.GetLayerCount()):
        lyr = ds.GetLayerByIndex(i)
        name = lyr.GetName()
        # Extract class part: "TLM_AREALE.TLM_FREIZEITAREAL" → "TLM_FREIZEITAREAL"
        class_part = name.split(".")[-1] if "." in name else name
        tables[class_part] = name

    ds = None
    return tables


# ============================================================================
# Discover DGIF target tables in the GeoPackage
# ============================================================================
def discover_dgif_tables(gpkg_path: str) -> dict[str, str]:
    """
    Returns dict of DGIF class name → actual GeoPackage table name.
    With --nameByTopic tables are e.g. 'Cultural.Building'.
    """
    ds = ogr.Open(gpkg_path, 0)
    if ds is None:
        print(f"[ERROR] Cannot open DGIF GeoPackage: {gpkg_path}", file=sys.stderr)
        sys.exit(1)

    tables = {}
    for i in range(ds.GetLayerCount()):
        lyr = ds.GetLayerByIndex(i)
        name = lyr.GetName()
        class_part = name.split(".")[-1] if "." in name else name
        tables[class_part] = name

    ds = None
    return tables


# ============================================================================
# Get column names for a table
# ============================================================================
def get_column_names(conn: sqlite3.Connection, table_name: str) -> set[str]:
    """Return set of column names (lowercase) for a table."""
    cursor = conn.execute(f'PRAGMA table_info("{table_name}")')
    return {row[1].lower() for row in cursor.fetchall()}


# ============================================================================
# Coordinate transformer
# ============================================================================
def create_transformer() -> osr.CoordinateTransformation:
    """LV95 (EPSG:2056) → WGS84 (EPSG:4326)"""
    src = osr.SpatialReference()
    src.ImportFromEPSG(2056)
    dst = osr.SpatialReference()
    dst.ImportFromEPSG(4326)
    # Ensure axis order is lon/lat for OGR
    src.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    dst.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    return osr.CoordinateTransformation(src, dst)


# ============================================================================
# Geometry helpers
# ============================================================================
def reproject_geometry(geom: ogr.Geometry, transform: osr.CoordinateTransformation) -> ogr.Geometry:
    """Reproject an OGR geometry, flattening 3D → 2D (DGIF uses Coord2)."""
    if geom is None:
        return None
    clone = geom.Clone()
    clone.FlattenTo2D()
    clone.Transform(transform)
    return clone


def extract_centroid_coord2(geom: ogr.Geometry, transform: osr.CoordinateTransformation) -> ogr.Geometry:
    """For polygon/line features that must map to Point DGIF classes, extract centroid."""
    if geom is None:
        return None
    clone = geom.Clone()
    clone.FlattenTo2D()
    clone.Transform(transform)
    centroid = clone.Centroid()
    return centroid


# ============================================================================
# Insert feature into DGIF table via sqlite3
# ============================================================================
def insert_feature(
    conn: sqlite3.Connection,
    dgif_table: str,
    dgif_columns: set[str],
    feature_data: dict,
    geom_wkb: bytes | None,
):
    """
    Insert a row into the DGIF GeoPackage table.
    feature_data keys are lowercase column names.
    """
    cols = []
    vals = []

    for col, val in feature_data.items():
        col_lower = col.lower()
        if col_lower in dgif_columns:
            cols.append(f'"{col}"')
            vals.append(val)

    if geom_wkb is not None and "geometry" in dgif_columns:
        cols.append('"geometry"')
        vals.append(geom_wkb)

    if not cols:
        return False

    placeholders = ", ".join(["?"] * len(vals))
    sql = f'INSERT INTO "{dgif_table}" ({", ".join(cols)}) VALUES ({placeholders})'
    try:
        conn.execute(sql, vals)
        return True
    except sqlite3.Error as e:
        # Silently skip constraint errors (e.g. unique violations)
        return False


# ============================================================================
# Build GPKG-compatible WKB (little-endian header with SRS ID)
# ============================================================================
def to_gpkg_wkb(geom: ogr.Geometry, srs_id: int = 4326) -> bytes:
    """
    Convert OGR geometry to GeoPackage binary (GP header + WKB).
    See http://www.geopackage.org/spec/#gpb_format
    """
    if geom is None:
        return None

    wkb = geom.ExportToWkb(ogr.wkbNDR)  # little-endian WKB
    envelope = geom.GetEnvelope()  # (minX, maxX, minY, maxY)

    import struct
    # GP header: magic 'GP', version 0, flags, srs_id, envelope
    # flags byte: envelope type 1 (minX,maxX,minY,maxY) = 0b00000010 = 0x02
    # byte order: little-endian (0x01)
    flags = 0x02 | 0x01  # envelope type 1 + little-endian
    header = struct.pack(
        '<2sBBi4d',
        b'GP',          # magic
        0,              # version
        flags,          # flags
        srs_id,         # srs_id
        envelope[0],    # minX
        envelope[1],    # maxX
        envelope[2],    # minY
        envelope[3],    # maxY
    )
    return header + wkb


# ============================================================================
# Main transform
# ============================================================================
def transform(
    tlm_gpkg_path: str,
    dgif_gpkg_path: str,
    mapping_csv_path: str,
):
    print("[INFO] Loading mapping table...")
    mapping = load_mapping(mapping_csv_path)
    print(f"[INFO] Loaded {sum(len(v) for v in mapping.values())} mapping rules for "
          f"{len(mapping)} (class, Objektart) pairs")

    print("[INFO] Discovering TLM tables...")
    tlm_tables = discover_tlm_tables(tlm_gpkg_path)
    print(f"[INFO] Found {len(tlm_tables)} TLM tables")

    print("[INFO] Discovering DGIF tables...")
    dgif_tables = discover_dgif_tables(dgif_gpkg_path)
    print(f"[INFO] Found {len(dgif_tables)} DGIF tables")

    # Coordinate transformer
    transform_ct = create_transformer()

    # Open TLM as OGR read-only
    tlm_ds = ogr.Open(tlm_gpkg_path, 0)
    if tlm_ds is None:
        print("[FATAL] Cannot open TLM GeoPackage", file=sys.stderr)
        sys.exit(1)

    # Open DGIF GeoPackage via sqlite3 for direct insert
    dgif_conn = sqlite3.connect(dgif_gpkg_path)
    dgif_conn.execute("PRAGMA journal_mode=WAL")
    dgif_conn.execute("PRAGMA synchronous=NORMAL")
    dgif_conn.execute("PRAGMA cache_size=-64000")  # 64 MB

    # Cache DGIF table column names
    dgif_col_cache: dict[str, set[str]] = {}

    # Statistics
    stats = defaultdict(int)
    now_iso = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Collect unique TLM classes referenced in mapping
    tlm_classes_needed = set()
    for (tlm_cls, _) in mapping.keys():
        tlm_classes_needed.add(tlm_cls)

    print(f"\n[INFO] Processing {len(tlm_classes_needed)} TLM classes...")
    print("=" * 60)

    for tlm_class_name in sorted(tlm_classes_needed):
        # Find the actual table name in TLM GeoPackage
        if tlm_class_name not in tlm_tables:
            print(f"  [SKIP] TLM class '{tlm_class_name}' not found in GeoPackage")
            stats["tlm_class_not_found"] += 1
            continue

        tlm_table = tlm_tables[tlm_class_name]
        tlm_layer = tlm_ds.GetLayerByName(tlm_table)
        if tlm_layer is None:
            print(f"  [SKIP] Cannot open TLM layer: {tlm_table}")
            stats["tlm_layer_error"] += 1
            continue

        feature_count = tlm_layer.GetFeatureCount()
        print(f"\n  [{tlm_class_name}] ({tlm_table}) — {feature_count} features")

        # Collect all Objektart values mapped for this class
        objektart_map: dict[str, list[MappingRow]] = {}
        for (cls, val), rules in mapping.items():
            if cls == tlm_class_name:
                objektart_map[val] = rules

        # Determine field indices once
        layer_defn = tlm_layer.GetLayerDefn()
        field_names = [layer_defn.GetFieldDefn(i).GetName() for i in range(layer_defn.GetFieldCount())]
        field_names_lower = [n.lower() for n in field_names]

        has_objektart = "objektart" in field_names_lower
        has_name = "name" in field_names_lower
        has_t_ili_tid = "t_ili_tid" in field_names_lower
        has_datum_erstellung = "datum_erstellung" in field_names_lower

        objektart_idx = field_names_lower.index("objektart") if has_objektart else -1
        name_idx = field_names_lower.index("name") if has_name else -1
        t_ili_tid_idx = field_names_lower.index("t_ili_tid") if has_t_ili_tid else -1
        datum_erst_idx = field_names_lower.index("datum_erstellung") if has_datum_erstellung else -1

        class_inserted = 0
        class_skipped = 0
        class_no_match = 0

        # Iterate features
        tlm_layer.ResetReading()
        feat: ogr.Feature
        for feat in tlm_layer:
            # Get Objektart value
            if has_objektart and objektart_idx >= 0:
                objektart_val = feat.GetFieldAsString(objektart_idx)
                # ili2gpkg stores enum values with the topic prefix stripped;
                # try both raw value and stripped
                objektart_val_clean = objektart_val.split(".")[-1] if "." in objektart_val else objektart_val
            else:
                # Classes without Objektart (e.g. TLM_BODENBEDECKUNG sometimes)
                objektart_val = ""
                objektart_val_clean = ""

            # Find mapping rules
            rules = objektart_map.get(objektart_val_clean)
            if rules is None:
                rules = objektart_map.get(objektart_val)
            if rules is None:
                class_no_match += 1
                continue

            # Get source geometry
            src_geom = feat.GetGeometryRef()

            # Get source attributes
            src_name = feat.GetFieldAsString(name_idx) if has_name and name_idx >= 0 else None
            src_tid = feat.GetFieldAsString(t_ili_tid_idx) if has_t_ili_tid and t_ili_tid_idx >= 0 else None
            src_datum = feat.GetFieldAsString(datum_erst_idx) if has_datum_erstellung and datum_erst_idx >= 0 else None

            # Apply each mapping rule (usually 1, but could be multiple)
            for mr in rules:
                dgif_class = mr.dgif_class

                # Resolve DGIF topic
                topic = DGIF_CLASS_TO_TOPIC.get(dgif_class)
                if topic is None:
                    # Try to find in dgif_tables
                    if dgif_class in dgif_tables:
                        # Table exists directly
                        pass
                    else:
                        stats["dgif_class_unknown_topic"] += 1
                        continue

                # Resolve DGIF table name
                if dgif_class in dgif_tables:
                    dgif_table_name = dgif_tables[dgif_class]
                elif topic:
                    candidate = f"{topic}.{dgif_class}"
                    if candidate in [dgif_tables.get(k, "") for k in dgif_tables]:
                        dgif_table_name = candidate
                    else:
                        # Try case-insensitive match
                        found = False
                        for k, v in dgif_tables.items():
                            if k.lower() == dgif_class.lower():
                                dgif_table_name = v
                                found = True
                                break
                        if not found:
                            stats["dgif_table_not_found"] += 1
                            continue
                else:
                    stats["dgif_table_not_found"] += 1
                    continue

                # Get/cache column names
                if dgif_table_name not in dgif_col_cache:
                    dgif_col_cache[dgif_table_name] = get_column_names(dgif_conn, dgif_table_name)

                dgif_cols = dgif_col_cache[dgif_table_name]

                # Determine target geometry type
                # DGIF FeatureEntity.geometry is MANDATORY Coord2 (Point)
                # But many TLM sources are Line or Polygon.
                # We reproject and convert: Polygon/Line → centroid for Point targets,
                # or keep geometry type if the DGIF table supports it.
                # Check if the DGIF table has a geometry column via gpkg_geometry_columns
                target_geom = None
                if src_geom is not None:
                    target_geom = reproject_geometry(src_geom, transform_ct)

                # Build feature data dict
                feature_data = {}

                # T_Id: auto-increment (leave to sqlite)

                # T_Ili_Tid: use source UUID or generate new
                if src_tid:
                    feature_data["T_Ili_Tid"] = src_tid
                else:
                    feature_data["T_Ili_Tid"] = str(uuid.uuid4())

                # uniqueUniversalEntityIdentifier (MANDATORY in Entity)
                if "uniqueuniversalentityidentifier" in dgif_cols:
                    if src_tid:
                        feature_data["uniqueUniversalEntityIdentifier"] = src_tid
                    else:
                        feature_data["uniqueUniversalEntityIdentifier"] = str(uuid.uuid4())

                # beginLifespanVersion (MANDATORY in Entity)
                if "beginlifespanversion" in dgif_cols:
                    if src_datum:
                        feature_data["beginLifespanVersion"] = src_datum
                    else:
                        feature_data["beginLifespanVersion"] = now_iso

                # Map DGIF-specific attributes from CSV
                if mr.dgif_attr1 and mr.dgif_val1:
                    attr_name = mr.dgif_attr1
                    if attr_name.lower() in dgif_cols:
                        feature_data[attr_name] = mr.dgif_val1

                if mr.dgif_attr2 and mr.dgif_val2:
                    attr_name = mr.dgif_attr2
                    if attr_name.lower() in dgif_cols:
                        feature_data[attr_name] = mr.dgif_val2

                # Convert geometry to GPKG binary
                geom_wkb = to_gpkg_wkb(target_geom) if target_geom else None

                # Insert
                ok = insert_feature(dgif_conn, dgif_table_name, dgif_cols, feature_data, geom_wkb)
                if ok:
                    class_inserted += 1
                    stats[f"inserted:{dgif_table_name}"] += 1
                else:
                    class_skipped += 1

        stats["total_inserted"] += class_inserted
        stats["total_skipped"] += class_skipped
        stats["total_no_match"] += class_no_match

        print(f"    → Inserted: {class_inserted}  |  Skipped: {class_skipped}  |  No match: {class_no_match}")

    # Commit
    print("\n[INFO] Committing to DGIF GeoPackage...")
    dgif_conn.commit()

    # Update gpkg_contents extent for populated tables
    print("[INFO] Updating spatial extents...")
    try:
        for dgif_table_name in dgif_col_cache:
            if "geometry" in dgif_col_cache[dgif_table_name]:
                dgif_conn.execute(f"""
                    UPDATE gpkg_contents SET
                        min_x = (SELECT MIN(MbrMinX(geometry)) FROM "{dgif_table_name}" WHERE geometry IS NOT NULL),
                        min_y = (SELECT MIN(MbrMinY(geometry)) FROM "{dgif_table_name}" WHERE geometry IS NOT NULL),
                        max_x = (SELECT MAX(MbrMaxX(geometry)) FROM "{dgif_table_name}" WHERE geometry IS NOT NULL),
                        max_y = (SELECT MAX(MbrMaxY(geometry)) FROM "{dgif_table_name}" WHERE geometry IS NOT NULL)
                    WHERE table_name = ?
                """, (dgif_table_name,))
        dgif_conn.commit()
    except sqlite3.Error:
        pass  # MbrMinX may not be available without SpatiaLite

    # Clean up
    dgif_conn.close()
    tlm_ds = None

    # Report
    print("\n" + "=" * 60)
    print("  ETL Transform Summary")
    print("=" * 60)
    print(f"  Total features inserted : {stats['total_inserted']}")
    print(f"  Total features skipped  : {stats['total_skipped']}")
    print(f"  No Objektart match      : {stats['total_no_match']}")
    print(f"  TLM classes not found   : {stats.get('tlm_class_not_found', 0)}")
    print(f"  DGIF tables not found   : {stats.get('dgif_table_not_found', 0)}")
    print(f"  DGIF unknown topic      : {stats.get('dgif_class_unknown_topic', 0)}")

    print("\n  Features per DGIF table:")
    for k, v in sorted(stats.items()):
        if k.startswith("inserted:"):
            table = k.split(":", 1)[1]
            print(f"    {table:<45} {v:>8}")

    print("=" * 60)
    return 0 if stats["total_inserted"] > 0 else 1


# ============================================================================
# CLI
# ============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="ETL Transform: swissTLM3D GeoPackage → DGIF GeoPackage"
    )
    parser.add_argument("--tlm-gpkg", required=True, help="Path to temporary swissTLM3D GeoPackage")
    parser.add_argument("--dgif-gpkg", required=True, help="Path to target DGIF GeoPackage")
    parser.add_argument("--mapping", required=True, help="Path to swissTLM3D_to_DGIF_V3.csv")
    args = parser.parse_args()

    # Validate paths
    for label, path in [("TLM GPKG", args.tlm_gpkg), ("Mapping CSV", args.mapping)]:
        if not Path(path).exists():
            print(f"[FATAL] {label} not found: {path}", file=sys.stderr)
            sys.exit(1)

    if not Path(args.dgif_gpkg).exists():
        print(f"[FATAL] DGIF GPKG not found: {args.dgif_gpkg}", file=sys.stderr)
        sys.exit(1)

    rc = transform(args.tlm_gpkg, args.dgif_gpkg, args.mapping)
    sys.exit(rc)


if __name__ == "__main__":
    main()
