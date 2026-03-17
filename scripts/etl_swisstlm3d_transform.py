#!/usr/bin/env python3
"""
ETL Transform & Load: swissTLM3D → DGIF GeoPackage

Reads features from a temporary swissTLM3D GeoPackage (imported via ili2gpkg),
applies the mapping table (swissTLM3D_to_DGIF_V3.csv), reprojects from
LV95 (EPSG:2056) to WGS84 (EPSG:4326), and inserts features into a
DGIF-schema GeoPackage.

The DGIF GeoPackage uses --smart2Inheritance, so each concrete class table
(e.g. cultural_building) contains ALL inherited columns from Entity and
FeatureEntity (including ageometry, beginlifespanversion, etc.).
This means a single INSERT per feature into the concrete table is sufficient.

Usage:
    python etl_swisstlm3d_transform.py \\
        --tlm-gpkg  C:/tmp/dgif/swisstlm3d_temp.gpkg \\
        --dgif-gpkg output/DGIF_swissTLM3D.gpkg \\
        --mapping   models/swissTLM3D_to_DGIF_V3.csv
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
# Derived from DGIF_V3.ili topic boundaries + class line numbers.
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
    Uses the ili2db metadata table T_ILI2DB_CLASSNAME to resolve
    INTERLIS qualified names (e.g. 'swissTLM3D_ili2_V2_4.TLM_STRASSEN.TLM_STRASSE')
    to the actual SQL table name (e.g. 'tlm_strassen_tlm_strasse').
    The returned key is the short class name (e.g. 'TLM_STRASSE').
    """
    import sqlite3

    conn = sqlite3.connect(gpkg_path)
    cur = conn.cursor()

    # Build set of actual feature table names for filtering
    cur.execute("SELECT table_name FROM gpkg_contents WHERE data_type='features'")
    feature_tables = {row[0] for row in cur.fetchall()}

    # Use ili2db metadata to get the INTERLIS class name → SQL table mapping
    tables = {}
    cur.execute("SELECT iliname, sqlname FROM T_ILI2DB_CLASSNAME")
    for iliname, sqlname in cur.fetchall():
        if sqlname not in feature_tables:
            continue
        # iliname: 'swissTLM3D_ili2_V2_4.TLM_STRASSEN.TLM_STRASSE'
        # Extract last part as class name: 'TLM_STRASSE'
        parts = iliname.split(".")
        if len(parts) >= 3:
            class_name = parts[-1]  # e.g. 'TLM_STRASSE'
            tables[class_name] = sqlname

    conn.close()
    return tables


# ============================================================================
# Discover DGIF target tables in the GeoPackage
# ============================================================================
def discover_dgif_tables(gpkg_path: str) -> dict[str, str]:
    """
    Returns dict of DGIF class name → actual GeoPackage table name.
    Uses the ili2db metadata table T_ILI2DB_CLASSNAME to resolve
    INTERLIS qualified names (e.g. 'DGIF_V3.Cultural.Building')
    to the actual SQL table name (e.g. 'cultural_building').
    The returned key is the short class name (e.g. 'Building').
    """
    import sqlite3

    conn = sqlite3.connect(gpkg_path)
    cur = conn.cursor()

    # Build set of actual table names for filtering (features + attributes)
    cur.execute("SELECT table_name FROM gpkg_contents WHERE data_type IN ('features','attributes')")
    feature_tables = {row[0] for row in cur.fetchall()}

    # Use ili2db metadata to get the INTERLIS class name → SQL table mapping
    tables = {}
    cur.execute("SELECT iliname, sqlname FROM T_ILI2DB_CLASSNAME")
    for iliname, sqlname in cur.fetchall():
        if sqlname not in feature_tables:
            continue
        # iliname: 'DGIF_V3.Cultural.Building'
        # Extract last part as class name: 'Building'
        parts = iliname.split(".")
        if len(parts) >= 3:
            class_name = parts[-1]  # e.g. 'Building'
            tables[class_name] = sqlname

    conn.close()
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
# Build ili2db class metadata from DGIF GeoPackage
# ============================================================================
def build_class_metadata(conn: sqlite3.Connection) -> dict:
    """
    Build metadata dict for DGIF classes from ili2db system tables.
    Returns dict keyed by short class name (e.g. 'Building') with:
      - iliname: fully qualified INTERLIS name (e.g. 'DGIF_V3.Cultural.Building')
      - sqlname: SQL table name (e.g. 'cultural_building')
      - topic:   INTERLIS topic name (e.g. 'Cultural')
      - columns: set of column names (lowercase)
      - notnull_defaults: dict of lowercase col name -> default value for
        domain-specific NOT NULL columns (excludes T_Id, T_basket, T_LastChange,
        T_CreateDate, T_User which are always provided)
    """
    cur = conn.cursor()

    # Get all class name mappings
    cur.execute("SELECT iliname, sqlname FROM T_ILI2DB_CLASSNAME")
    classname_rows = cur.fetchall()

    # Get all table names from gpkg_contents (features + attributes)
    cur.execute("SELECT table_name FROM gpkg_contents WHERE data_type IN ('features','attributes')")
    all_tables = {row[0] for row in cur.fetchall()}

    # Columns always provided by the ETL code
    always_provided = {"t_id", "t_basket", "t_lastchange", "t_createdate", "t_user"}

    meta = {}
    for iliname, sqlname in classname_rows:
        if sqlname not in all_tables:
            continue
        parts = iliname.split(".")
        if len(parts) >= 3:
            class_name = parts[-1]   # e.g. 'Building'
            topic_name = parts[-2]   # e.g. 'Cultural'
            # Get column info (name, type, notnull, default)
            col_cur = conn.execute(f'PRAGMA table_info("{sqlname}")')
            columns = set()
            notnull_defaults = {}
            for row in col_cur.fetchall():
                col_name = row[1].lower()
                col_type = (row[2] or "").upper()
                is_notnull = bool(row[3])
                columns.add(col_name)
                if is_notnull and col_name not in always_provided:
                    # Determine a sensible default based on column type
                    if "INT" in col_type:
                        notnull_defaults[col_name] = 0
                    elif "DOUBLE" in col_type or "REAL" in col_type or "FLOAT" in col_type:
                        notnull_defaults[col_name] = 0.0
                    elif "BOOL" in col_type:
                        notnull_defaults[col_name] = False
                    else:
                        # TEXT / VARCHAR — use 'unknown'
                        notnull_defaults[col_name] = "unknown"
            meta[class_name] = {
                "iliname": iliname,
                "sqlname": sqlname,
                "topic": topic_name,
                "columns": columns,
                "notnull_defaults": notnull_defaults,
            }
    return meta


# ============================================================================
# Ensure dataset and baskets exist
# ============================================================================
def ensure_baskets(conn: sqlite3.Connection, topics_needed: set[str]) -> dict[str, int]:
    """
    Create a dataset and one basket per DGIF topic.
    Returns dict of topic_iliname -> basket T_Id.
    """
    cur = conn.cursor()

    # Check for existing dataset
    cur.execute("SELECT T_Id FROM T_ILI2DB_DATASET LIMIT 1")
    row = cur.fetchone()
    if row:
        dataset_id = row[0]
    else:
        cur.execute("INSERT INTO T_ILI2DB_DATASET (datasetName) VALUES (?)", ("swissTLM3D_import",))
        dataset_id = cur.lastrowid

    # Create baskets for each topic
    basket_map = {}
    for topic_ili in sorted(topics_needed):
        # topic_ili e.g. 'DGIF_V3.Cultural'
        cur.execute("SELECT T_Id FROM T_ILI2DB_BASKET WHERE topic=?", (topic_ili,))
        row = cur.fetchone()
        if row:
            basket_map[topic_ili] = row[0]
        else:
            basket_tid = str(uuid.uuid4())
            cur.execute(
                "INSERT INTO T_ILI2DB_BASKET (dataset, topic, T_Ili_Tid, attachmentKey) VALUES (?,?,?,?)",
                (dataset_id, topic_ili, basket_tid, "swissTLM3D_import")
            )
            basket_map[topic_ili] = cur.lastrowid

    conn.commit()
    return basket_map


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

    # Open DGIF GeoPackage via sqlite3
    dgif_conn = sqlite3.connect(dgif_gpkg_path)
    dgif_conn.execute("PRAGMA journal_mode=WAL")
    dgif_conn.execute("PRAGMA synchronous=NORMAL")
    dgif_conn.execute("PRAGMA cache_size=-64000")  # 64 MB
    dgif_conn.execute("PRAGMA foreign_keys=OFF")    # defer FK checks for performance

    # Drop rtree triggers that reference ST_IsEmpty / ST_MinX etc.
    # These SpatiaLite functions are not available in plain sqlite3.
    # We will rebuild the rtree index after all inserts.
    rtree_triggers = dgif_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='trigger' "
        "AND (sql LIKE '%ST_IsEmpty%' OR sql LIKE '%ST_MinX%')"
    ).fetchall()
    if rtree_triggers:
        print(f"[INFO] Dropping {len(rtree_triggers)} rtree triggers (SpatiaLite not available)...")
        for (tname,) in rtree_triggers:
            dgif_conn.execute(f'DROP TRIGGER IF EXISTS "{tname}"')
        dgif_conn.commit()
        print(f"[INFO]   Dropped: {[t[0] for t in rtree_triggers]}")

    print("[INFO] Building DGIF class metadata from ili2db tables...")
    class_meta = build_class_metadata(dgif_conn)
    print(f"[INFO] Found {len(class_meta)} DGIF classes")

    # Coordinate transformer
    transform_ct = create_transformer()

    # Open TLM as OGR read-only
    tlm_ds = ogr.Open(tlm_gpkg_path, 0)
    if tlm_ds is None:
        print("[FATAL] Cannot open TLM GeoPackage", file=sys.stderr)
        sys.exit(1)

    # Collect which DGIF topics are needed for baskets
    topics_needed = set()
    for (_, _), rules in mapping.items():
        for mr in rules:
            if mr.dgif_class in class_meta:
                meta = class_meta[mr.dgif_class]
                topics_needed.add(f"DGIF_V3.{meta['topic']}")

    print(f"[INFO] Creating baskets for {len(topics_needed)} topics...")
    basket_map = ensure_baskets(dgif_conn, topics_needed)
    print(f"[INFO] Baskets: {basket_map}")

    # T_Id counter — start at 1 (tables are empty after schemaimport)
    next_tid = 1

    # Statistics
    stats = defaultdict(int)
    now_iso = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Track spatial extent for gpkg_contents update (no SpatiaLite needed)
    extent_min_x = float("inf")
    extent_min_y = float("inf")
    extent_max_x = float("-inf")
    extent_max_y = float("-inf")

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
        print(f"\n  [{tlm_class_name}] ({tlm_table}) -- {feature_count} features")

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

                # Resolve DGIF class metadata
                if dgif_class not in class_meta:
                    stats["dgif_class_not_found"] += 1
                    continue

                meta = class_meta[dgif_class]
                dgif_table_name = meta["sqlname"]
                dgif_cols = meta["columns"]
                dgif_iliname = meta["iliname"]   # e.g. 'DGIF_V3.Cultural.Building'
                dgif_topic = meta["topic"]       # e.g. 'Cultural'

                # Resolve basket
                topic_key = f"DGIF_V3.{dgif_topic}"
                basket_id = basket_map.get(topic_key)
                if basket_id is None:
                    stats["dgif_basket_not_found"] += 1
                    continue

                # Assign T_Id for this feature
                tid = next_tid
                next_tid += 1

                # Generate identifiers
                ili_tid = src_tid if src_tid else str(uuid.uuid4())
                entity_uuid = ili_tid
                begin_date = src_datum if src_datum else now_iso

                # --- Geometry ---
                # With --smart2Inheritance each concrete class has its own
                # ageometry column (MANDATORY Coord2 = Point).
                # For Line/Polygon sources, extract centroid.
                geom_wkb = None
                if src_geom is not None:
                    geom_type = src_geom.GetGeometryType()
                    flat_type = ogr.GT_Flatten(geom_type)
                    if flat_type == ogr.wkbPoint:
                        target_geom = reproject_geometry(src_geom, transform_ct)
                    else:
                        # Polygon, Line, Multi* -> centroid
                        target_geom = extract_centroid_coord2(src_geom, transform_ct)
                    if target_geom is not None:
                        geom_wkb = to_gpkg_wkb(target_geom, srs_id=4326)
                        # Track extent
                        px = target_geom.GetX()
                        py = target_geom.GetY()
                        if px < extent_min_x:
                            extent_min_x = px
                        if px > extent_max_x:
                            extent_max_x = px
                        if py < extent_min_y:
                            extent_min_y = py
                        if py > extent_max_y:
                            extent_max_y = py

                # --- Single-table insert (--smart2Inheritance) ---
                # With --smart2Inheritance, each concrete class table contains
                # ALL inherited columns (Entity + FeatureEntity + domain attrs).
                # No separate foundation_entity / foundation_featureentity insert.
                has_geom_col = "ageometry" in dgif_cols

                # If the table has ageometry (FeatureEntity subclass) and no
                # geometry was extracted, use a default point (0,0)
                if has_geom_col and geom_wkb is None:
                    default_pt = ogr.Geometry(ogr.wkbPoint)
                    default_pt.AddPoint(0.0, 0.0)
                    geom_wkb = to_gpkg_wkb(default_pt, srs_id=4326)

                insert_cols = [
                    "T_Id", "T_Ili_Tid", "T_basket",
                    "beginlifespanversion", "uniqueuniversalentityidentifier",
                    "T_LastChange", "T_CreateDate", "T_User",
                ]
                insert_vals: list = [
                    tid, ili_tid, basket_id,
                    begin_date, entity_uuid,
                    now_iso, now_iso, "etl_swisstlm3d",
                ]

                # Add geometry column only for FeatureEntity subclasses
                if has_geom_col:
                    insert_cols.insert(3, "ageometry")
                    insert_vals.insert(3, geom_wkb)

                # Map DGIF-specific attributes from CSV
                if mr.dgif_attr1 and mr.dgif_val1:
                    attr_lower = mr.dgif_attr1.lower()
                    if attr_lower in dgif_cols:
                        insert_cols.append(mr.dgif_attr1)
                        insert_vals.append(mr.dgif_val1)

                if mr.dgif_attr2 and mr.dgif_val2:
                    attr_lower = mr.dgif_attr2.lower()
                    if attr_lower in dgif_cols:
                        insert_cols.append(mr.dgif_attr2)
                        insert_vals.append(mr.dgif_val2)

                # Fill remaining NOT NULL columns with defaults
                notnull_defs = meta.get("notnull_defaults", {})
                already_set = {c.lower() for c in insert_cols}
                for nn_col, nn_default in notnull_defs.items():
                    if nn_col not in already_set:
                        insert_cols.append(nn_col)
                        insert_vals.append(nn_default)

                col_str = ", ".join(f'"{c}"' for c in insert_cols)
                placeholders = ", ".join(["?"] * len(insert_vals))
                sql = f'INSERT INTO "{dgif_table_name}" ({col_str}) VALUES ({placeholders})'

                try:
                    dgif_conn.execute(sql, insert_vals)
                    class_inserted += 1
                    stats[f"inserted:{dgif_table_name}"] += 1
                except sqlite3.Error as e:
                    if stats["insert_error"] < 5:
                        print(f"  [DEBUG] Insert error: {e}", file=sys.stderr)
                        print(f"  [DEBUG]   table={dgif_table_name}", file=sys.stderr)
                    stats["insert_error"] += 1
                    class_skipped += 1

        stats["total_inserted"] += class_inserted
        stats["total_skipped"] += class_skipped
        stats["total_no_match"] += class_no_match

        print(f"    -> Inserted: {class_inserted}  |  Skipped: {class_skipped}  |  No match: {class_no_match}")

    # Commit
    print("\n[INFO] Committing to DGIF GeoPackage...")
    dgif_conn.commit()

    # Update gpkg_contents extent for all tables that received inserts
    print("[INFO] Updating spatial extents...")
    if extent_min_x < float("inf"):
        try:
            # Collect table names that had features inserted
            inserted_tables = [
                k.split(":", 1)[1] for k in stats if k.startswith("inserted:")
            ]
            updated_count = 0
            for tbl in inserted_tables:
                dgif_conn.execute(
                    "UPDATE gpkg_contents SET min_x=?, min_y=?, max_x=?, max_y=? "
                    "WHERE table_name=?",
                    (extent_min_x, extent_min_y, extent_max_x, extent_max_y, tbl)
                )
                updated_count += dgif_conn.execute(
                    "SELECT changes()"
                ).fetchone()[0]
            dgif_conn.commit()
            print(f"[INFO]   Extent: ({extent_min_x:.6f}, {extent_min_y:.6f}) - "
                  f"({extent_max_x:.6f}, {extent_max_y:.6f})")
            print(f"[INFO]   Updated {updated_count} gpkg_contents rows")
        except sqlite3.Error as e:
            print(f"[WARN] Could not update extents: {e}", file=sys.stderr)
    else:
        print("[INFO]   No geometry inserted, skipping extent update.")

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
    print(f"  DGIF class not found    : {stats.get('dgif_class_not_found', 0)}")
    print(f"  DGIF basket not found   : {stats.get('dgif_basket_not_found', 0)}")
    print(f"  Insert errors           : {stats.get('insert_error', 0)}")

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
