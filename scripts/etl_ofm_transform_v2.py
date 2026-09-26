#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL Transform: OFMX XML → DGIF GeoPackage (insert features)

Phase 4/5 of OFM pipeline: parse OFMX XML directly, extract features,
and insert into DGIF_V3.ili GeoPackage tables with proper basket/dataset
metadata.

OFMX format: AIXM 4.5 flavor with tags like Ahp, Rwy, Vor, Ndb, Dme, Dpn, Ase, Abd

Coordinate format: geoLat="47.20891953N", geoLong="009.65979003E" (with N/S/E/W suffixes)
"""

import argparse
import sqlite3
import struct
import sys
import uuid
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path
from typing import Optional


# ============================================================================
# Logging helpers
# ============================================================================
RESET = "\033[0m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
GREY = "\033[90m"

def error(msg: str):
    print(f"[{RED}ERROR{RESET}] {msg}")

def warn(msg: str):
    print(f"[{YELLOW}WARNING{RESET}] {msg}")

def info(msg: str):
    print(f"[{GREY}INFO{RESET}] {msg}")

def ok(msg: str):
    print(f"[{GREEN}OK{RESET}] {msg}")

def debug(msg: str):
    print(f"[DEBUG] {msg}")


# ============================================================================
# XML parsing helpers
# ============================================================================
def strip_ns(tag: str) -> str:
    """Remove XML namespace from tag."""
    return tag.split("}")[-1] if "}" in tag else tag


def get_xml_text(element: ET.Element, child_tag: str, default: str = "") -> str:
    """Extract text from child element, handling namespaces."""
    if element is None:
        return default
    for child in element:
        if strip_ns(child.tag) == child_tag:
            return child.text or default
    return default


def get_xml_attr(element: ET.Element, attr_name: str, default: str = "") -> str:
    """Get attribute value, handling namespaces."""
    if element is None:
        return default
    for key, value in element.attrib.items():
        if strip_ns(key) == attr_name or key.endswith(attr_name):
            return value
    return default


# ============================================================================
# Coordinate & Geometry Helpers
# ============================================================================
def parse_lat_long(lat_str: str, long_str: str) -> Optional[tuple[float, float]]:
    """Parse latitude/longitude strings to (lon, lat) tuple.
    
    Handles formats like:
      - 47.20891953N (with direction suffix)
      - 009.65979003E (with direction suffix)
      - 47.20891953 (without suffix)
    """
    try:
        # Parse latitude (remove N/S suffix, negate if S)
        lat_str = lat_str.strip() if lat_str else ""
        lat_sign = 1
        if lat_str.endswith(('N', 'n')):
            lat_str = lat_str[:-1]
        elif lat_str.endswith(('S', 's')):
            lat_sign = -1
            lat_str = lat_str[:-1]
        lat = float(lat_str) * lat_sign
        
        # Parse longitude (remove E/W suffix, negate if W)
        long_str = long_str.strip() if long_str else ""
        lon_sign = 1
        if long_str.endswith(('E', 'e')):
            long_str = long_str[:-1]
        elif long_str.endswith(('W', 'w')):
            lon_sign = -1
            long_str = long_str[:-1]
        lon = float(long_str) * lon_sign
        
        # Validate ranges
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return (lon, lat)
    except (ValueError, TypeError, AttributeError):
        pass
    return None


def point_to_gpkg_wkb(lon: float, lat: float, srs_id: int = 4326) -> bytes:
    """Convert (lon, lat) to GeoPackage binary WKB (POINT)."""
    wkb = struct.pack('<BI2d', 1, 1, lon, lat)  # WKB POINT little-endian
    
    # GP header: magic 'GP', version 0, flags, srs_id, envelope
    flags = 0x02 | 0x01  # envelope type 1 + little-endian
    header = struct.pack(
        '<2sBBi4d',
        b'GP',
        0,
        flags,
        srs_id,
        lon, lon, lat, lat,  # envelope: minX, maxX, minY, maxY
    )
    return header + wkb


def polygon_to_gpkg_wkb(coords: list[tuple[float, float]], srs_id: int = 4326) -> Optional[bytes]:
    """Convert list of (lon, lat) tuples to GeoPackage binary WKB (POLYGON)."""
    if len(coords) < 3:
        return None
    
    # Close polygon if not already closed
    if coords[0] != coords[-1]:
        coords = coords + [coords[0]]
    
    # WKB POLYGON: type (3, little-endian), ring count, ring...
    wkb = struct.pack('<BI', 3, 1)  # type=3 (POLYGON), 1 ring
    wkb += struct.pack('<I', len(coords))  # ring point count
    for lon, lat in coords:
        wkb += struct.pack('<2d', lon, lat)
    
    # Bounding box
    xs = [c[0] for c in coords]
    ys = [c[1] for c in coords]
    min_x, max_x = min(xs), max(xs)
    min_y, max_y = min(ys), max(ys)
    
    # GP header
    flags = 0x02 | 0x01  # envelope type 1 + little-endian
    header = struct.pack(
        '<2sBBi4d',
        b'GP',
        0,
        flags,
        srs_id,
        min_x, max_x, min_y, max_y,
    )
    return header + wkb


# ============================================================================
# Database helpers
# ============================================================================
def discover_dgif_tables(gpkg_path: str) -> dict[str, str]:
    """Returns dict of DGIF class name → SQL table name."""
    conn = sqlite3.connect(gpkg_path)
    cur = conn.cursor()

    cur.execute("SELECT table_name FROM gpkg_contents WHERE data_type IN ('features','attributes')")
    feature_tables = {row[0] for row in cur.fetchall()}

    tables = {}
    cur.execute("SELECT iliname, sqlname FROM T_ILI2DB_CLASSNAME")
    for iliname, sqlname in cur.fetchall():
        if sqlname not in feature_tables:
            continue
        parts = iliname.split(".")
        if len(parts) >= 3:
            class_name = parts[-1]
            tables[class_name] = sqlname

    conn.close()
    return tables


def get_or_create_dataset(conn: sqlite3.Connection, dataset_name: str = "OFMX_import") -> int:
    """Get or create a dataset entry, return dataset T_Id."""
    cur = conn.cursor()
    
    cur.execute("SELECT T_Id FROM T_ILI2DB_DATASET LIMIT 1")
    row = cur.fetchone()
    if row:
        return row[0]
    
    cur.execute("INSERT INTO T_ILI2DB_DATASET (datasetName) VALUES (?)", (dataset_name,))
    conn.commit()
    return cur.lastrowid


def get_or_create_basket(conn: sqlite3.Connection, topic: str, dataset_id: int,
                         attachment_key: str = "OFM_import") -> int:
    """Get or create a basket for a topic, return basket T_Id."""
    cur = conn.cursor()
    
    cur.execute(
        "SELECT T_Id FROM T_ILI2DB_BASKET WHERE topic = ? AND dataset = ?",
        (topic, dataset_id)
    )
    row = cur.fetchone()
    if row:
        return row[0]
    
    til_id = str(uuid.uuid4()).replace("-", "")
    cur.execute(
        "INSERT INTO T_ILI2DB_BASKET (dataset, topic, T_Ili_Tid, attachmentKey) VALUES (?, ?, ?, ?)",
        (dataset_id, topic, til_id, attachment_key)
    )
    conn.commit()
    return cur.lastrowid


# ============================================================================
# Feature parsers (return geometry only)
# ============================================================================
def parse_aerodrome_geom(elem: ET.Element) -> Optional[bytes]:
    """Extract geometry from Ahp element."""
    lat_str = get_xml_text(elem, "geoLat")
    lon_str = get_xml_text(elem, "geoLong")
    
    coords = parse_lat_long(lat_str, lon_str)
    if not coords:
        return None
    
    lon, lat = coords
    return point_to_gpkg_wkb(lon, lat)


def parse_runway_geom(elem: ET.Element) -> Optional[bytes]:
    """Extract geometry from Rwy element."""
    lat_str = get_xml_text(elem, "geoLat")
    lon_str = get_xml_text(elem, "geoLong")
    
    coords = parse_lat_long(lat_str, lon_str)
    if not coords:
        return None
    
    lon, lat = coords
    return point_to_gpkg_wkb(lon, lat)


def parse_waypoint_geom(elem: ET.Element) -> Optional[bytes]:
    """Extract geometry from Dpn element."""
    lat_str = get_xml_text(elem, "geoLat")
    lon_str = get_xml_text(elem, "geoLong")
    
    coords = parse_lat_long(lat_str, lon_str)
    if not coords:
        return None
    
    lon, lat = coords
    return point_to_gpkg_wkb(lon, lat)


def parse_vor_geom(elem: ET.Element) -> Optional[bytes]:
    """Extract geometry from Vor element."""
    lat_str = get_xml_text(elem, "geoLat")
    lon_str = get_xml_text(elem, "geoLong")
    
    coords = parse_lat_long(lat_str, lon_str)
    if not coords:
        return None
    
    lon, lat = coords
    return point_to_gpkg_wkb(lon, lat)


def parse_ndb_geom(elem: ET.Element) -> Optional[bytes]:
    """Extract geometry from Ndb element."""
    lat_str = get_xml_text(elem, "geoLat")
    lon_str = get_xml_text(elem, "geoLong")
    
    coords = parse_lat_long(lat_str, lon_str)
    if not coords:
        return None
    
    lon, lat = coords
    return point_to_gpkg_wkb(lon, lat)


def parse_dme_geom(elem: ET.Element) -> Optional[bytes]:
    """Extract geometry from Dme element."""
    lat_str = get_xml_text(elem, "geoLat")
    lon_str = get_xml_text(elem, "geoLong")
    
    coords = parse_lat_long(lat_str, lon_str)
    if not coords:
        return None
    
    lon, lat = coords
    return point_to_gpkg_wkb(lon, lat)


def parse_airspace_geom(elem: ET.Element) -> Optional[bytes]:
    """Extract geometry from Ase element (via Abd and Avx)."""
    # Airspace geometry comes from Avx (Airspace Vertex) points
    # We'll create a simple polygon from the first few Avx points
    # In reality, this would need more sophisticated handling
    
    coords = []
    for child in elem:
        if strip_ns(child.tag) == "Avx":
            lat_str = get_xml_text(child, "geoLat")
            lon_str = get_xml_text(child, "geoLong")
            parsed = parse_lat_long(lat_str, lon_str)
            if parsed:
                coords.append(parsed)
    
    if len(coords) < 3:
        return None
    
    return polygon_to_gpkg_wkb(coords)


# ============================================================================
# Main ETL logic
# ============================================================================
def main() -> int:
    parser = argparse.ArgumentParser(description="Transform OFMX XML to DGIF GeoPackage")
    parser.add_argument("--dgif-gpkg", required=True, help="Path to DGIF GeoPackage")
    parser.add_argument("--ofmx-files", nargs="+", required=True, help="OFMX file paths")
    parser.add_argument(
        "--target-topics",
        default=None,
        help="Comma-separated DGIF topic names to write (e.g. Foundation,Airspace,AeronauticalFacility)",
    )
    args = parser.parse_args()

    dgif_gpkg = Path(args.dgif_gpkg)
    ofmx_files = [Path(f) for f in args.ofmx_files if f.strip()]

    if not dgif_gpkg.exists():
        error(f"DGIF GeoPackage not found: {dgif_gpkg}")
        return 1

    # Connect to DGIF GeoPackage
    conn = sqlite3.connect(str(dgif_gpkg))
    conn.execute("PRAGMA foreign_keys = OFF")

    # Discover DGIF tables
    dgif_tables = discover_dgif_tables(str(dgif_gpkg))
    info(f"Discovered {len(dgif_tables)} DGIF classes")

    selected_topics = None
    if args.target_topics:
        selected_topics = {
            token.strip()
            for token in args.target_topics.split(",")
            if token.strip()
        }
        if not selected_topics:
            selected_topics = None

    def topic_enabled(topic_name: str) -> bool:
        return selected_topics is None or topic_name in selected_topics

    # Create or get dataset and baskets
    dataset_id = get_or_create_dataset(conn, "OFMX_import")

    basket_aero = get_or_create_basket(conn, "DGIF_V3.AeronauticalFacility", dataset_id) if topic_enabled("AeronauticalFacility") else None
    basket_navaids = get_or_create_basket(conn, "DGIF_V3.AeronauticalAidsNavigation", dataset_id) if topic_enabled("AeronauticalAidsNavigation") else None
    basket_airspace = get_or_create_basket(conn, "DGIF_V3.Airspace", dataset_id) if topic_enabled("Airspace") else None
    basket_foundation = get_or_create_basket(conn, "DGIF_V3.Foundation", dataset_id) if topic_enabled("Foundation") else None

    # Parse OFMX and insert features
    counts = defaultdict(int)

    for ofmx_file in ofmx_files:
        if not ofmx_file.exists():
            warn(f"OFMX file not found: {ofmx_file}")
            continue

        info(f"Parsing OFMX: {ofmx_file}")

        # Parse entire XML tree
        try:
            tree = ET.parse(str(ofmx_file))
            root = tree.getroot()
        except ET.ParseError as e:
            error(f"XML parse error: {e}")
            return 1

        # Single pass: iterate all root-level elements
        for elem in root:
            tag = strip_ns(elem.tag)
            t_id = int(uuid.uuid4().int % (2**31))

            if tag == "Ahp":
                geom = parse_aerodrome_geom(elem)
                if geom and "Aerodrome" in dgif_tables and basket_aero is not None:
                    table = dgif_tables["Aerodrome"]
                    try:
                        conn.execute(
                            f"INSERT INTO {table} (T_Id, T_basket, ageometry) VALUES (?, ?, ?)",
                            (t_id, basket_aero, geom)
                        )
                        counts["Aerodrome"] += 1
                    except sqlite3.DatabaseError as e:
                        debug(f"Ahp insert: {e}")

            elif tag == "Rwy":
                geom = parse_runway_geom(elem)
                if geom and "Runway" in dgif_tables and basket_aero is not None:
                    table = dgif_tables["Runway"]
                    try:
                        conn.execute(
                            f"INSERT INTO {table} (T_Id, T_basket, ageometry) VALUES (?, ?, ?)",
                            (t_id, basket_aero, geom)
                        )
                        counts["Runway"] += 1
                    except sqlite3.DatabaseError as e:
                        debug(f"Rwy insert: {e}")

            elif tag == "Dpn":
                geom = parse_waypoint_geom(elem)
                if geom and "WayPoint" in dgif_tables and basket_foundation is not None:
                    table = dgif_tables["WayPoint"]
                    try:
                        conn.execute(
                            f"INSERT INTO {table} (T_Id, T_basket, ageometry) VALUES (?, ?, ?)",
                            (t_id, basket_foundation, geom)
                        )
                        counts["WayPoint"] += 1
                    except sqlite3.DatabaseError as e:
                        debug(f"Dpn insert: {e}")

            elif tag == "Vor":
                geom = parse_vor_geom(elem)
                if geom and "VhfOmniRadioBeacon" in dgif_tables and basket_navaids is not None:
                    table = dgif_tables["VhfOmniRadioBeacon"]
                    try:
                        conn.execute(
                            f"INSERT INTO {table} (T_Id, T_basket, ageometry) VALUES (?, ?, ?)",
                            (t_id, basket_navaids, geom)
                        )
                        counts["VhfOmniRadioBeacon"] += 1
                    except sqlite3.DatabaseError as e:
                        debug(f"Vor insert: {e}")

            elif tag == "Ndb":
                geom = parse_ndb_geom(elem)
                if geom and "NonDirectionalRadioBeacon" in dgif_tables and basket_navaids is not None:
                    table = dgif_tables["NonDirectionalRadioBeacon"]
                    try:
                        conn.execute(
                            f"INSERT INTO {table} (T_Id, T_basket, ageometry) VALUES (?, ?, ?)",
                            (t_id, basket_navaids, geom)
                        )
                        counts["NonDirectionalRadioBeacon"] += 1
                    except sqlite3.DatabaseError as e:
                        debug(f"Ndb insert: {e}")

            elif tag == "Dme":
                geom = parse_dme_geom(elem)
                if geom and "DistanceMeasuringEquipment" in dgif_tables and basket_navaids is not None:
                    table = dgif_tables["DistanceMeasuringEquipment"]
                    try:
                        conn.execute(
                            f"INSERT INTO {table} (T_Id, T_basket, ageometry) VALUES (?, ?, ?)",
                            (t_id, basket_navaids, geom)
                        )
                        counts["DistanceMeasuringEquipment"] += 1
                    except sqlite3.DatabaseError as e:
                        debug(f"Dme insert: {e}")

            elif tag == "Ase":
                geom = parse_airspace_geom(elem)
                if geom and "Airspace" in dgif_tables and basket_airspace is not None:
                    table = dgif_tables["Airspace"]
                    try:
                        conn.execute(
                            f"INSERT INTO {table} (T_Id, T_basket, ageometry) VALUES (?, ?, ?)",
                            (t_id, basket_airspace, geom)
                        )
                        counts["Airspace"] += 1
                    except sqlite3.DatabaseError as e:
                        debug(f"Ase insert: {e}")

    # Commit and close
    conn.commit()
    conn.execute("PRAGMA foreign_keys = ON")
    conn.close()

    # Report results
    print()
    ok("Transform completed successfully")
    print()
    print("Inserted features:")
    if not any(counts.values()):
        warn("No features were inserted (check OFMX file format and mapping)")
    else:
        for entity_type, count in sorted(counts.items(), key=lambda x: -x[1]):
            if count > 0:
                print(f"  {entity_type}: {count}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
