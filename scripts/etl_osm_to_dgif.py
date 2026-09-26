#!/usr/bin/env python3
"""
ETL Pipeline: OpenStreetMap via Overpass API -> DGIF GeoPackage

Downloads OSM data for the requested AOI from a selectable Overpass API
provider, applies the OSM_to_DGIF_V3.csv mapping table, and inserts the
result into a DGIF GeoPackage generated from the current INTERLIS model.

This implementation is intentionally conservative: it focuses on nodes and
ways returned by Overpass and maps the rows that can be matched directly from
the OSM mapping table. That keeps the pipeline usable from the web app while
remaining compatible with the existing DGIF model output.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import struct
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import defaultdict
from pathlib import Path


def _find_qgis_root() -> str | None:
    if sys.platform != "win32":
        return None
    base = Path(os.environ.get("PROGRAMFILES", r"C:\Program Files"))
    candidates = sorted(base.glob("QGIS *"), reverse=True)
    return str(candidates[0]) if candidates else None


def _setup_qgis_env(qgis_root: str | None = None) -> None:
    if qgis_root is None:
        qgis_root = _find_qgis_root()
    if qgis_root is None:
        return
    qgis = Path(qgis_root)
    extra_paths = [
        str(qgis / "bin"),
        str(qgis / "apps" / "gdal" / "bin"),
        str(qgis / "apps" / "sqlite" / "bin"),
        str(qgis / "apps" / "Python312"),
        str(qgis / "apps" / "Python312" / "DLLs"),
        str(qgis / "apps" / "Python312" / "Scripts"),
        str(qgis / "apps" / "Qt5" / "bin"),
    ]
    current = os.environ.get("PATH", "")
    os.environ["PATH"] = os.pathsep.join(extra_paths) + os.pathsep + current
    os.environ["GDAL_DATA"] = str(qgis / "apps" / "gdal" / "share" / "gdal")
    os.environ["PROJ_LIB"] = str(qgis / "share" / "proj")


_setup_qgis_env()

try:
    from osgeo import ogr, gdal
    gdal.UseExceptions()
except Exception as exc:
    print(f"[FATAL] GDAL/OGR Python bindings not available: {exc}", file=sys.stderr)
    raise SystemExit(1)

from interlis_tool_paths import (
    describe_configured_interlis_tools,
    resolve_interlis_tool_path,
    should_log,
)

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
GREY = "\033[90m"
RESET = "\033[0m"

OVERPASS_ENDPOINTS = {
    "overpassTurbo": "https://overpass-api.de/api/interpreter",
    "swissOverpass": "https://overpass.osm.ch/api/interpreter",
}


def _sql_text(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sql_value(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return "X'" + bytes(value).hex() + "'"
    return _sql_text(str(value))


def _row_get(row: dict[str, object], key: str, default=None):
    if key in row:
        return row[key]
    lower = key.lower()
    for existing_key, value in row.items():
        if str(existing_key).lower() == lower:
            return value
    return default


def execute_sql_query(ds, sql: str) -> list[dict[str, object]]:
    layer = ds.ExecuteSQL(sql, dialect="SQLITE")
    if layer is None:
        return []
    try:
        rows: list[dict[str, object]] = []
        layer_defn = layer.GetLayerDefn()
        field_names = [layer_defn.GetFieldDefn(i).GetName() for i in range(layer_defn.GetFieldCount())]
        feat = layer.GetNextFeature()
        while feat is not None:
            row = {name: feat.GetField(name) for name in field_names}
            rows.append(row)
            feat = layer.GetNextFeature()
        return rows
    finally:
        ds.ReleaseResultSet(layer)


def execute_sql(ds, sql: str) -> None:
    layer = ds.ExecuteSQL(sql, dialect="SQLITE")
    if layer is not None:
        ds.ReleaseResultSet(layer)


def info(msg: str) -> None:
    if should_log("INFO"):
        print(f"{CYAN}[INFO]{RESET} {msg}")


def ok(msg: str) -> None:
    if should_log("INFO"):
        print(f"{GREEN}[OK]{RESET} {msg}")


def warn(msg: str) -> None:
    if should_log("WARNING"):
        print(f"{YELLOW}[WARNING]{RESET} {msg}")


def error(msg: str) -> None:
    print(f"{RED}[ERROR]{RESET} {msg}", file=sys.stderr)


def banner(title: str) -> None:
    if should_log("INFO"):
        print()
        print(f"{CYAN}================================================================{RESET}")
        print(f"{CYAN}  {title}{RESET}")
        print(f"{CYAN}================================================================{RESET}")


def run_java(args: list[str], label: str) -> int:
    cmd = ["java"] + args
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


def file_size_mb(path: str | Path) -> float:
    return round(os.path.getsize(path) / (1024 * 1024), 1)


def parse_bbox(raw_bbox: str) -> tuple[float, float, float, float]:
    parts = [part.strip() for part in str(raw_bbox).split(",")]
    if len(parts) != 4:
        raise ValueError("AOI bbox must be minx,miny,maxx,maxy")
    minx, miny, maxx, maxy = [float(part) for part in parts]
    if minx >= maxx or miny >= maxy:
        raise ValueError("Invalid AOI bbox extent")
    return minx, miny, maxx, maxy


def bbox_from_geojson(aoi_path: Path) -> tuple[float, float, float, float]:
    payload = json.loads(aoi_path.read_text(encoding="utf-8"))
    geometry = None
    if isinstance(payload, dict) and payload.get("type") == "Feature":
        geometry = payload.get("geometry")
    elif isinstance(payload, dict) and payload.get("type") == "FeatureCollection":
        features = payload.get("features") or []
        if features:
            geometry = {"type": "GeometryCollection", "geometries": [f.get("geometry") for f in features if f.get("geometry")]}
    elif isinstance(payload, dict) and payload.get("type") in {"Polygon", "MultiPolygon", "Point", "MultiPoint", "LineString", "MultiLineString", "GeometryCollection"}:
        geometry = payload

    if not geometry:
        raise ValueError(f"Invalid AOI GeoJSON in {aoi_path}")

    coords: list[tuple[float, float]] = []

    def iter_coords(node):
        if isinstance(node, (list, tuple)):
            if node and isinstance(node[0], (int, float)):
                if len(node) >= 2:
                    coords.append((float(node[0]), float(node[1])))
                return
            for child in node:
                iter_coords(child)

    if geometry.get("type") == "GeometryCollection":
        for geom in geometry.get("geometries") or []:
            iter_coords((geom or {}).get("coordinates"))
    else:
        iter_coords(geometry.get("coordinates"))

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
    geom = ogr.CreateGeometryFromWkt(aoi_wkt)
    if geom is None:
        raise ValueError("Invalid AOI WKT")
    env = geom.GetEnvelope()
    minx, maxx, miny, maxy = env
    if minx >= maxx or miny >= maxy:
        raise ValueError("AOI WKT has zero area bounding box")
    return minx, miny, maxx, maxy


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


def load_aoi_geometry(aoi_file: str | None, aoi_wkt: str | None) -> ogr.Geometry | None:
    if aoi_file:
        ds = ogr.Open(aoi_file)
        if ds is None:
            raise ValueError(f"Cannot open AOI file: {aoi_file}")
        lyr = ds.GetLayer(0)
        if lyr is None:
            raise ValueError(f"AOI file has no readable layer: {aoi_file}")
        lyr.ResetReading()
        feat = lyr.GetNextFeature()
        while feat is not None:
            geom = feat.GetGeometryRef()
            if geom is not None:
                out = geom.Clone()
                out.FlattenTo2D()
                ds = None
                return out
            feat = lyr.GetNextFeature()
        ds = None
        raise ValueError(f"AOI file has no geometry features: {aoi_file}")

    if aoi_wkt:
        geom = ogr.CreateGeometryFromWkt(aoi_wkt)
        if geom is None:
            raise ValueError("Invalid AOI WKT")
        geom.FlattenTo2D()
        return geom

    return None


def format_bbox(bbox: tuple[float, float, float, float]) -> str:
    return ",".join(f"{value:.8f}" for value in bbox)


def to_gpkg_wkb(geom: ogr.Geometry, srs_id: int = 4326) -> bytes | None:
    if geom is None:
        return None
    wkb = geom.ExportToWkb(ogr.wkbNDR)
    envelope = geom.GetEnvelope()
    flags = 0x02 | 0x01
    header = struct.pack(
        "<2sBBi4d",
        b"GP",
        0,
        flags,
        srs_id,
        envelope[0],
        envelope[1],
        envelope[2],
        envelope[3],
    )
    return header + wkb


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


class MappingRow:
    __slots__ = (
        "no",
        "osm_feature_class",
        "osm_key",
        "osm_value",
        "description",
        "mapping_description",
        "dgif_class",
        "dgif_code",
        "dgif_attr1",
        "dgif_attr1_code",
        "dgif_val1",
        "dgif_val1_code",
        "dgif_attr2",
        "dgif_attr2_code",
        "dgif_val2",
        "dgif_val2_code",
    )

    def __init__(self, row: list[str]):
        row = row + [""] * (16 - len(row))
        self.no = row[0].strip()
        self.osm_feature_class = row[1].strip()
        self.osm_key = row[2].strip()
        self.osm_value = row[3].strip()
        self.description = row[4].strip()
        self.mapping_description = row[5].strip()
        self.dgif_class = row[6].strip()
        self.dgif_code = row[7].strip()
        self.dgif_attr1 = row[8].strip()
        self.dgif_attr1_code = row[9].strip()
        self.dgif_val1 = row[10].strip()
        self.dgif_val1_code = row[11].strip()
        self.dgif_attr2 = row[12].strip()
        self.dgif_attr2_code = row[13].strip()
        self.dgif_val2 = row[14].strip()
        self.dgif_val2_code = row[15].strip()

    @property
    def is_mapped(self) -> bool:
        return bool(self.dgif_class) and self.mapping_description != "not in DGIF"


def load_mapping(csv_path: str) -> dict[tuple[str, str], list[MappingRow]]:
    mapping: dict[tuple[str, str], list[MappingRow]] = defaultdict(list)
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        next(reader, None)
        for row in reader:
            if not row or not row[0].strip():
                continue
            mr = MappingRow(row)
            if mr.is_mapped:
                mapping[(mr.osm_feature_class.lower(), mr.osm_key.lower())].append(mr)
    return mapping


def value_matches(expected: str, actual: str) -> bool:
    expected = expected.strip().lower()
    actual = actual.strip().lower()
    if not expected:
        return True
    if expected == actual:
        return True
    if "," in expected:
        options = [part.strip() for part in expected.split(",") if part.strip()]
        if actual in options:
            return True
    if expected == 'user-defined or "yes"' and actual in {"yes", "user-defined"}:
        return True
    return False


def get_element_geometry(element: dict) -> tuple[ogr.Geometry | None, str]:
    element_type = str(element.get("type") or "").lower()
    if element_type == "node":
        lat = element.get("lat")
        lon = element.get("lon")
        if lat is None or lon is None:
            return None, "Point"
        geom = ogr.Geometry(ogr.wkbPoint)
        geom.AddPoint(float(lon), float(lat))
        return geom, "Point"

    coords = element.get("geometry") or []
    if not isinstance(coords, list) or not coords:
        return None, "Line"

    points = [(float(item["lon"]), float(item["lat"])) for item in coords if isinstance(item, dict) and "lon" in item and "lat" in item]
    if len(points) < 2:
        return None, "Line"

    is_closed = len(points) >= 4 and points[0] == points[-1]
    if is_closed:
        ring = ogr.Geometry(ogr.wkbLinearRing)
        for lon, lat in points:
            ring.AddPoint(lon, lat)
        polygon = ogr.Geometry(ogr.wkbPolygon)
        polygon.AddGeometry(ring)
        return polygon, "Polygon"

    line = ogr.Geometry(ogr.wkbLineString)
    for lon, lat in points:
        line.AddPoint(lon, lat)
    return line, "Line"


def query_overpass(endpoint: str, bbox: tuple[float, float, float, float], timeout_s: int, user_agent: str) -> list[dict]:
    minx, miny, maxx, maxy = bbox
    query = f"""
[out:json][timeout:{timeout_s}];
(
  node({miny:.8f},{minx:.8f},{maxy:.8f},{maxx:.8f});
  way({miny:.8f},{minx:.8f},{maxy:.8f},{maxx:.8f});
  relation({miny:.8f},{minx:.8f},{maxy:.8f},{maxx:.8f});
);
out body geom;
""".strip()
    data = urllib.parse.urlencode({"data": query}).encode("utf-8")
    headers = {
        "User-Agent": user_agent,
        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
        "Accept": "application/json",
    }

    def _attempt(url: str) -> list[dict]:
        request = urllib.request.Request(url, data=data, headers=headers)
        with urllib.request.urlopen(request, timeout=timeout_s + 60) as response:
            payload = json.loads(response.read().decode("utf-8", errors="replace"))
        elements = payload.get("elements") or []
        return [element for element in elements if isinstance(element, dict)]

    def _build_http_fallback(url: str) -> str | None:
        if not url.lower().startswith("https://"):
            return None
        return "http://" + url.split("://", 1)[1]

    try:
        return _attempt(endpoint)
    except urllib.error.HTTPError as exc:
        fallback_endpoint = _build_http_fallback(endpoint)
        retryable_proxy_codes = {407, 502, 503, 504}
        if fallback_endpoint is None or int(getattr(exc, "code", 0) or 0) not in retryable_proxy_codes:
            raise
        warn(
            "Overpass HTTPS request failed through proxy "
            f"(HTTP {exc.code}: {exc.reason}). Retrying via HTTP endpoint: {fallback_endpoint}"
        )
        return _attempt(fallback_endpoint)
    except urllib.error.URLError as exc:
        error_text = str(exc)
        fallback_endpoint = _build_http_fallback(endpoint)
        https_tunnel_blocked = (
            fallback_endpoint is not None
            and ("407" in error_text or "tunnel" in error_text.lower())
        )
        if not https_tunnel_blocked:
            raise

        warn(
            "Proxy rejected HTTPS tunnel to Overpass. "
            f"Retrying via HTTP endpoint: {fallback_endpoint}"
        )
        return _attempt(fallback_endpoint)


def build_class_metadata(ds) -> dict:
    classname_rows = execute_sql_query(ds, "SELECT iliname, sqlname FROM T_ILI2DB_CLASSNAME")

    all_table_rows = execute_sql_query(ds, "SELECT table_name FROM gpkg_contents WHERE data_type IN ('features','attributes')")
    all_tables = {str(_row_get(row, "table_name") or "") for row in all_table_rows}

    always_provided = {"t_id", "t_basket", "t_lastchange", "t_createdate", "t_user"}

    geom_rows = execute_sql_query(ds, "SELECT table_name, geometry_type_name FROM gpkg_geometry_columns")
    table_geom_type = {
        str(_row_get(row, "table_name") or ""): str(_row_get(row, "geometry_type_name") or "").upper()
        for row in geom_rows
    }

    meta = {}
    for row in classname_rows:
        iliname = str(_row_get(row, "iliname") or "")
        sqlname = str(_row_get(row, "sqlname") or "")
        if sqlname not in all_tables:
            continue
        parts = iliname.split(".")
        if len(parts) >= 3:
            class_name = parts[-1]
            topic_name = parts[-2]
            col_rows = execute_sql_query(ds, f"PRAGMA table_info({_sql_ident(sqlname)})")
            columns = set()
            notnull_defaults = {}
            for col_row in col_rows:
                col_name = str(_row_get(col_row, "name") or "").lower()
                col_type = str(_row_get(col_row, "type") or "").upper()
                is_notnull = bool(_row_get(col_row, "notnull") or False)
                if not col_name:
                    continue
                columns.add(col_name)
                if is_notnull and col_name not in always_provided:
                    if "INT" in col_type:
                        notnull_defaults[col_name] = 0
                    elif "DOUBLE" in col_type or "REAL" in col_type or "FLOAT" in col_type:
                        notnull_defaults[col_name] = 0.0
                    elif "BOOL" in col_type:
                        notnull_defaults[col_name] = False
                    else:
                        notnull_defaults[col_name] = "unknown"
            meta[class_name] = {
                "iliname": iliname,
                "sqlname": sqlname,
                "topic": topic_name,
                "columns": columns,
                "notnull_defaults": notnull_defaults,
                "geom_type": table_geom_type.get(sqlname, ""),
            }
    return meta


def ensure_baskets(ds, topics_needed: set[str]) -> dict[str, int]:
    dataset_rows = execute_sql_query(ds, "SELECT T_Id AS t_id FROM T_ILI2DB_DATASET LIMIT 1")
    if dataset_rows:
        dataset_id = int(_row_get(dataset_rows[0], "t_id") or 0)
    else:
        execute_sql(ds, "INSERT INTO T_ILI2DB_DATASET (datasetName) VALUES ('osm_import')")
        id_rows = execute_sql_query(ds, "SELECT last_insert_rowid() AS row_id")
        dataset_id = int(_row_get(id_rows[0], "row_id") or 0)

    basket_map = {}
    for topic_ili in sorted(topics_needed):
        basket_rows = execute_sql_query(
            ds,
            f"SELECT T_Id AS t_id FROM T_ILI2DB_BASKET WHERE topic={_sql_text(topic_ili)} LIMIT 1",
        )
        if basket_rows:
            basket_map[topic_ili] = int(_row_get(basket_rows[0], "t_id") or 0)
        else:
            basket_tid = str(uuid.uuid4())
            execute_sql(
                ds,
                (
                    "INSERT INTO T_ILI2DB_BASKET (dataset, topic, T_Ili_Tid, attachmentKey) "
                    f"VALUES ({dataset_id}, {_sql_text(topic_ili)}, {_sql_text(basket_tid)}, 'osm_import')"
                ),
            )
            id_rows = execute_sql_query(ds, "SELECT last_insert_rowid() AS row_id")
            basket_map[topic_ili] = int(_row_get(id_rows[0], "row_id") or 0)

    return basket_map


def next_available_tid(ds) -> int:
    max_tid = 0
    table_rows = execute_sql_query(ds, "SELECT name FROM sqlite_master WHERE type='table'")
    for table_row in table_rows:
        table_name = str(_row_get(table_row, "name") or "")
        if not table_name:
            continue
        cols = execute_sql_query(ds, f"PRAGMA table_info({_sql_ident(table_name)})")
        if not any(str(_row_get(col, "name") or "").lower() == "t_id" for col in cols):
            continue
        max_rows = execute_sql_query(ds, f"SELECT MAX({_sql_ident('T_Id')}) AS max_tid FROM {_sql_ident(table_name)}")
        if not max_rows:
            continue
        row_value = _row_get(max_rows[0], "max_tid")
        if row_value is None:
            continue
        try:
            max_tid = max(max_tid, int(row_value))
        except (TypeError, ValueError):
            continue
    return max_tid + 1


def update_gpkg_extents(ds, extents: dict[str, list[float]]) -> None:
    for table_name, extent in extents.items():
        minx, miny, maxx, maxy = extent
        execute_sql(
            ds,
            (
                "UPDATE gpkg_contents SET "
                f"min_x={_sql_value(minx)}, min_y={_sql_value(miny)}, "
                f"max_x={_sql_value(maxx)}, max_y={_sql_value(maxy)} "
                f"WHERE table_name={_sql_text(table_name)}"
            ),
        )


def populate_foundation_metadata(ds, basket_tid: int, start_tid: int) -> int:
    now_iso = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    user = "etl_osm"
    tid = start_tid
    table_columns_cache: dict[str, set[str]] = {}

    def _base(extra: dict | None = None) -> dict:
        nonlocal tid
        row = {
            "T_Id": tid,
            "T_basket": basket_tid,
            "T_Ili_Tid": str(uuid.uuid4()),
            "T_LastChange": now_iso,
            "T_CreateDate": now_iso,
            "T_User": user,
        }
        if extra:
            row.update(extra)
        tid += 1
        return row

    def _entity(extra: dict | None = None) -> dict:
        row = _base({
            "beginlifespanversion": now_iso,
            "uniqueuniversalentityidentifier": str(uuid.uuid4()),
        })
        if extra:
            row.update(extra)
        return row

    def _insert(table: str, row: dict) -> int:
        table_cols = table_columns_cache.get(table)
        if table_cols is None:
            col_rows = execute_sql_query(ds, f"PRAGMA table_info({_sql_ident(table)})")
            table_cols = {str(_row_get(col, "name") or "").lower() for col in col_rows}
            table_columns_cache[table] = table_cols

        filtered_row = {key: value for key, value in row.items() if key.lower() in table_cols}
        cols = ", ".join(_sql_ident(key) for key in filtered_row.keys())
        values = ", ".join(_sql_value(value) for value in filtered_row.values())
        execute_sql(
            ds,
            f"INSERT INTO {_sql_ident(table)} ({cols}) VALUES ({values})",
        )
        return row["T_Id"]

    info("Populating Foundation metadata (OpenStreetMap -> DGIF) ...")
    src_tid = _insert("foundation_sourceinfo", _base({
        "datasetcitation": "OpenStreetMap contributors - https://www.openstreetmap.org",
        "sourcedescription": (
            "OpenStreetMap is a collaborative open map database queried via Overpass API. "
            "This ETL downloads a selected AOI and maps supported features into DGIF."
        ),
        "sourceidentifier": "openstreetmap.org",
        "typeofsource": "vectorDataset",
        "resourcecontentorigin": "OpenStreetMap contributors",
        "scaledenominator": 0,
        "sourcecurrencydatetime": dt.datetime.now().strftime("%Y"),
    }))
    ok(f"foundation_sourceinfo          T_Id={src_tid}")

    org_tid = _insert("foundation_organisation", _entity({
        "organisationdescription": "OpenStreetMap contributors",
        "organisationtype": "nonGovernmentalOrganisation",
        "homegeopoliticalentity": "global",
        "organisationreach": "global",
        "branding": "OpenStreetMap",
    }))
    ok(f"foundation_organisation         T_Id={org_tid}")

    contact_tid = _insert("foundation_contactinfo", _base({
        "addresscountry": "",
        "addresscity": "",
        "addressdeliverypoint": "",
        "addresspostalcode": "",
        "addressadministrativearea": "",
        "addresselectronicmail": "",
        "telephonevoice": "",
        "onlineresourcelinkage": "https://www.openstreetmap.org",
    }))
    ok(f"foundation_contactinfo          T_Id={contact_tid}")

    orgunit_tid = _insert("foundation_organisationalunit", _entity({
        "contactinfo": "OpenStreetMap contributors; web: https://www.openstreetmap.org",
        "mainorganisation": org_tid,
    }))
    ok(f"foundation_organisationalunit   T_Id={orgunit_tid}")

    restr_tid = _insert("foundation_restrictioninfo", _base({
        "commercialcopyrightnotice": "© OpenStreetMap contributors",
        "commercialdistribrestrict": "ODbL",
    }))
    ok(f"foundation_restrictioninfo      T_Id={restr_tid}")

    hcoord_tid = _insert("foundation_horizcoordmetadata", _base({
        "geodeticdatum": "worldGeodeticSystem1984",
        "horizaccuracycategory": "Varies by source contribution",
    }))
    ok(f"foundation_horizcoordmetadata   T_Id={hcoord_tid}")

    fmeta_tid = _insert("foundation_featuremetadata", _base({
        "delineationknown": 1,
        "delineationknown_txt": "true",
        "existencecertaintycat": "definite",
        "surveycoveragecategory": "incomplete",
        "dataqualitystatement": "OSM quality varies by contributor, geography, and tagging practice.",
    }))
    ok(f"foundation_featuremetadata      T_Id={fmeta_tid}")

    fameta_tid = _insert("foundation_featureattmetadata", _base({
        "currencydatetime": dt.datetime.now().strftime("%Y"),
        "dataqualitystatement": "Attribute quality varies by OSM tagging completeness.",
    }))
    ok(f"foundation_featureattmetadata   T_Id={fameta_tid}")

    name_tid = _insert("foundation_namespecification", _base({
        "aname": "OpenStreetMap",
        "nametype": "official",
        "nameusedescription": "OpenStreetMap contributor database",
        "referencename": 1,
        "referencename_txt": "true",
    }))
    ok(f"foundation_namespecification    T_Id={name_tid}")

    return tid


def resolve_overpass_endpoint(provider: str, overpass_url: str | None) -> str:
    provider = (provider or "").strip() or "overpassTurbo"
    if provider == "custom":
        if not overpass_url:
            raise ValueError("Custom Overpass provider selected but no URL was supplied.")
        return overpass_url.strip()
    return OVERPASS_ENDPOINTS.get(provider, OVERPASS_ENDPOINTS["overpassTurbo"])


def transform(
    dgif_gpkg_path: str,
    mapping_csv_path: str,
    provider: str,
    overpass_url: str,
    bbox: tuple[float, float, float, float],
    target_topics: set[str] | None = None,
    allow_empty: bool = False,
    aoi_geom: ogr.Geometry | None = None,
    overpass_timeout: int = 900,
    user_agent: str = "DGIF-OSM-ETL/1.0",
):
    info("Loading mapping table...")
    mapping = load_mapping(mapping_csv_path)
    info(f"Loaded {sum(len(v) for v in mapping.values())} mapping rules for {len(mapping)} (feature_class, key) combinations")

    endpoint = resolve_overpass_endpoint(provider, overpass_url)
    info(f"Using Overpass endpoint: {endpoint}")
    info(f"AOI bbox: {format_bbox(bbox)}")
    elements = query_overpass(endpoint, bbox, overpass_timeout, user_agent)
    info(f"Overpass returned {len(elements)} elements")

    dgif_ds = gdal.OpenEx(dgif_gpkg_path, gdal.OF_VECTOR | gdal.OF_UPDATE)
    if dgif_ds is None:
        raise RuntimeError(f"Cannot open DGIF GeoPackage for update: {dgif_gpkg_path}")

    execute_sql(dgif_ds, "PRAGMA journal_mode=WAL")
    execute_sql(dgif_ds, "PRAGMA synchronous=NORMAL")
    execute_sql(dgif_ds, "PRAGMA cache_size=-64000")
    execute_sql(dgif_ds, "PRAGMA foreign_keys=OFF")
    execute_sql(dgif_ds, "BEGIN")

    rtree_triggers = execute_sql_query(
        dgif_ds,
        "SELECT name FROM sqlite_master WHERE type='trigger' AND (sql LIKE '%ST_IsEmpty%' OR sql LIKE '%ST_MinX%')",
    )
    if rtree_triggers:
        info(f"Dropping {len(rtree_triggers)} rtree triggers...")
        for trigger_row in rtree_triggers:
            tname = str(_row_get(trigger_row, "name") or "")
            if not tname:
                continue
            execute_sql(dgif_ds, f"DROP TRIGGER IF EXISTS {_sql_ident(tname)}")

    info("Building DGIF class metadata from ili2db tables...")
    class_meta = build_class_metadata(dgif_ds)
    info(f"Found {len(class_meta)} DGIF classes")

    topics_needed = set()
    for rules in mapping.values():
        for mr in rules:
            if mr.dgif_class in class_meta:
                topic_name = str(class_meta[mr.dgif_class]["topic"])
                if target_topics is None or topic_name in target_topics:
                    topics_needed.add(f"DGIF_V3.{topic_name}")
    if target_topics is None or "Foundation" in target_topics:
        topics_needed.add("DGIF_V3.Foundation")

    info(f"Creating baskets for {len(topics_needed)} topics...")
    basket_map = ensure_baskets(dgif_ds, topics_needed)
    next_tid = next_available_tid(dgif_ds)
    now_iso = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    stats = defaultdict(int)
    table_extents: dict[str, list[float]] = {}

    print(f"\n[INFO] Processing {len(elements)} OSM elements...")
    print("=" * 60)

    for element in elements:
        tags = element.get("tags") or {}
        if not isinstance(tags, dict) or not tags:
            stats["no_tags"] += 1
            continue

        src_geom, geom_kind = get_element_geometry(element)
        if src_geom is not None and aoi_geom is not None:
            try:
                if not src_geom.Intersects(aoi_geom):
                    stats["aoi_filtered_out"] += 1
                    continue
            except Exception:
                stats["aoi_filtered_out"] += 1
                continue

        feature_class_candidates = {f"{key.lower().strip()}_{geom_kind.lower()}" for key in tags.keys()}
        matching_rules: list[tuple[MappingRow, str, str]] = []
        for key, value in tags.items():
            key_norm = str(key).strip().lower()
            value_norm = str(value).strip()
            lookup_key = (f"{key_norm}_{geom_kind.lower()}", key_norm)
            for row in mapping.get(lookup_key, []):
                if value_matches(row.osm_value, value_norm):
                    matching_rules.append((row, key_norm, value_norm))

        if not matching_rules:
            stats["no_match"] += 1
            continue

        element_id = f"{element.get('type', 'element')}/{element.get('id', uuid.uuid4())}"

        for mr, matched_key, matched_value in matching_rules:
            if mr.dgif_class not in class_meta:
                stats["dgif_class_not_found"] += 1
                continue

            meta = class_meta[mr.dgif_class]
            dgif_table_name = meta["sqlname"]
            dgif_cols = meta["columns"]
            dgif_topic = meta["topic"]

            if target_topics is not None and dgif_topic not in target_topics:
                stats["topic_filtered_out"] += 1
                continue

            topic_key = f"DGIF_V3.{dgif_topic}"
            basket_id = basket_map.get(topic_key)
            if basket_id is None:
                stats["dgif_basket_not_found"] += 1
                continue

            dgif_geom_type = str(meta.get("geom_type", "")).upper()
            geom_to_write = None
            if src_geom is not None:
                geom_to_write = src_geom.Clone()
                geom_to_write.FlattenTo2D()
                src_flat = ogr.GT_Flatten(geom_to_write.GetGeometryType())
                if dgif_geom_type.startswith("POINT") and src_flat != ogr.wkbPoint:
                    geom_to_write = geom_to_write.Centroid()
                elif dgif_geom_type.startswith("LINE") and src_flat == ogr.wkbPolygon:
                    boundary = geom_to_write.Boundary()
                    geom_to_write = boundary if boundary is not None else geom_to_write
                elif dgif_geom_type.startswith("POLYGON") and src_flat not in {ogr.wkbPolygon, ogr.wkbMultiPolygon}:
                    stats["geometry_type_mismatch"] += 1
                    continue

            tid = next_tid
            next_tid += 1
            ili_tid = element_id
            entity_uuid = str(uuid.uuid4())

            insert_cols = [
                "T_Id",
                "T_Ili_Tid",
                "T_basket",
                "beginlifespanversion",
                "uniqueuniversalentityidentifier",
                "T_LastChange",
                "T_CreateDate",
                "T_User",
            ]
            insert_vals: list = [
                tid,
                ili_tid,
                basket_id,
                now_iso,
                entity_uuid,
                now_iso,
                now_iso,
                "etl_osm",
            ]

            if geom_to_write is not None and "ageometry" in dgif_cols:
                insert_cols.insert(3, "ageometry")
                insert_vals.insert(3, to_gpkg_wkb(geom_to_write, srs_id=4326))

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

            notnull_defs = meta.get("notnull_defaults", {})
            already_set = {column.lower() for column in insert_cols}
            for nn_col, nn_default in notnull_defs.items():
                if nn_col not in already_set:
                    insert_cols.append(nn_col)
                    insert_vals.append(nn_default)

            col_str = ", ".join(_sql_ident(column) for column in insert_cols)
            values_sql = ", ".join(_sql_value(value) for value in insert_vals)
            sql = f"INSERT INTO {_sql_ident(dgif_table_name)} ({col_str}) VALUES ({values_sql})"

            try:
                execute_sql(dgif_ds, sql)
                stats["inserted"] += 1

                if geom_to_write is not None:
                    env = geom_to_write.GetEnvelope()
                    if dgif_table_name not in table_extents:
                        table_extents[dgif_table_name] = [env[0], env[2], env[1], env[3]]
                    else:
                        te = table_extents[dgif_table_name]
                        if env[0] < te[0]:
                            te[0] = env[0]
                        if env[2] < te[1]:
                            te[1] = env[2]
                        if env[1] > te[2]:
                            te[2] = env[1]
                        if env[3] > te[3]:
                            te[3] = env[3]
            except RuntimeError as exc:
                stats["insert_error"] += 1
                if stats["insert_error"] <= 5:
                    print(f"  [DEBUG] Insert error for {dgif_table_name}: {exc}", file=sys.stderr)

    info("Committing to DGIF GeoPackage...")
    execute_sql(dgif_ds, "COMMIT")

    if (target_topics is None or "Foundation" in target_topics) and stats["inserted"] > 0:
        foundation_basket = basket_map.get("DGIF_V3.Foundation")
        if foundation_basket is not None:
            execute_sql(dgif_ds, "BEGIN")
            next_tid = populate_foundation_metadata(dgif_ds, foundation_basket, next_tid)
            execute_sql(dgif_ds, "COMMIT")
    elif target_topics is None or "Foundation" in target_topics:
        warn("Skipping Foundation metadata because no source features were inserted.")

    if table_extents:
        info("Updating spatial extents...")
        execute_sql(dgif_ds, "BEGIN")
        update_gpkg_extents(dgif_ds, table_extents)
        execute_sql(dgif_ds, "COMMIT")

    dgif_ds = None

    print()
    print("[INFO] OSM ETL summary")
    print(f"  Inserted features     : {stats['inserted']:,}")
    print(f"  No tag match          : {stats['no_match']:,}")
    print(f"  Geometry mismatches   : {stats['geometry_type_mismatch']:,}")
    print(f"  AOI filtered out      : {stats['aoi_filtered_out']:,}")
    print(f"  Insert errors         : {stats['insert_error']:,}")

    if stats["inserted"] == 0 and not allow_empty:
        raise RuntimeError("No OSM features were inserted into the DGIF GeoPackage")


def main() -> int:
    parser = argparse.ArgumentParser(description="ETL Pipeline: OpenStreetMap via Overpass API -> DGIF GeoPackage")
    parser.add_argument("--provider", default="overpassTurbo", choices=["overpassTurbo", "swissOverpass", "custom"], help="Overpass provider preset")
    parser.add_argument("--overpass-url", default=None, help="Custom Overpass API endpoint (required if --provider custom)")
    parser.add_argument("--aoi-file", default=None, help="Path to AOI GeoJSON file")
    parser.add_argument("--aoi-wkt", default=None, help="AOI geometry as WKT")
    parser.add_argument("--aoi-bbox", default=None, help="AOI bbox as minx,miny,maxx,maxy")
    parser.add_argument("--tmp-dir", default="C:/tmp/dgif_osm", help="Temporary working directory")
    parser.add_argument("--output-name", default="DGIF_OSM.gpkg", help="Output DGIF GeoPackage filename")
    parser.add_argument("--output-dir", default=None, help="Output directory for DGIF GeoPackage")
    parser.add_argument("--output-gpkg", default=None, help="Full output GeoPackage path")
    parser.add_argument("--skip-schema", action="store_true", help="Skip DGIF schema creation if output already exists")
    parser.add_argument("--mapping", required=True, help="Path to OSM_to_DGIF_V3.csv")
    parser.add_argument("--target-topics", default=None, help="Comma-separated DGIF topics to write")
    parser.add_argument("--ili-model", default=None, help="Path to DGIF INTERLIS model (.ili)")
    parser.add_argument("--overpass-timeout", type=int, default=900, help="Overpass query timeout in seconds")
    parser.add_argument("--user-agent", default="DGIF-OSM-ETL/1.0", help="User-Agent for Overpass HTTP requests")
    args = parser.parse_args()

    bbox = resolve_aoi_bbox(args)
    if bbox is None:
        error("AOI is required. Provide --aoi-file, --aoi-wkt, or --aoi-bbox.")
        return 1

    aoi_geom = load_aoi_geometry(args.aoi_file, args.aoi_wkt)

    workspace_root = Path(__file__).resolve().parent.parent
    ili2gpkg_jar = resolve_interlis_tool_path("ili2gpkg")
    mapping_csv = Path(args.mapping).expanduser()
    if not mapping_csv.is_absolute():
        mapping_csv = (workspace_root / mapping_csv).resolve()
    if args.ili_model:
        dgif_ili = Path(args.ili_model).expanduser()
    else:
        default_model = workspace_root / "models" / "DGIF_V3.ili"
        dgif_ili = default_model

    output_dir = Path(args.output_dir).expanduser() if args.output_dir else (workspace_root / "output")
    dgif_gpkg = output_dir / args.output_name
    if args.output_gpkg:
        dgif_gpkg = Path(args.output_gpkg).expanduser()
        output_dir = dgif_gpkg.parent

    models_dir = workspace_root / "models"
    model_dirs = [str(dgif_ili.parent), str(models_dir)]
    dedup_model_dirs: list[str] = []
    for model_dir in model_dirs:
        if model_dir not in dedup_model_dirs:
            dedup_model_dirs.append(model_dir)
    dgif_model_dir = f"{';'.join(dedup_model_dirs)};http://models.interlis.ch/;%JAR_DIR"

    banner("ETL Pipeline: OpenStreetMap via Overpass API -> DGIF GeoPackage")
    print()
    if should_log("DEBUG"):
        for configured_tool in describe_configured_interlis_tools():
            info(f"Configured tool: {configured_tool}")
    info(f"DGIF model: {dgif_ili}")
    info(f"Mapping CSV: {mapping_csv}")
    info(f"Output path: {dgif_gpkg}")
    info(f"Provider:    {args.provider}")
    print()

    print(f"{YELLOW}--- Checking prerequisites ---{RESET}")
    try:
        result = subprocess.run(["java", "-version"], capture_output=True, text=True, encoding="utf-8", errors="replace")
        java_ver = (result.stderr or result.stdout).strip().split("\n")[0]
        ok(f"Java: {java_ver}")
    except FileNotFoundError:
        error("Java not found in PATH!")
        return 1

    if ili2gpkg_jar is None or not ili2gpkg_jar.exists():
        error(f"ili2gpkg not found: {ili2gpkg_jar}")
        return 1
    ok(f"ili2gpkg: {ili2gpkg_jar}")

    for f in (dgif_ili, mapping_csv):
        if not f.exists():
            error(f"File not found: {f}")
            return 1
        ok(f.name)

    try:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "from osgeo import gdal, ogr; gdal.UseExceptions(); print('GDAL', gdal.VersionInfo(), 'GeoJSON:', 'YES' if ogr.GetDriverByName('GeoJSON') else 'NO')",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        ok(f"Python + {result.stdout.strip()}")
    except Exception as exc:
        error(f"Python/GDAL not available: {exc}")
        return 1

    tmp_dir = Path(args.tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    banner("Phase 1: Create DGIF GeoPackage schema")
    if args.skip_schema and dgif_gpkg.exists():
        info(f"Using existing DGIF GeoPackage: {dgif_gpkg} ({file_size_mb(dgif_gpkg)} MB)")
    else:
        if dgif_gpkg.exists():
            info(f"Removing existing: {dgif_gpkg}")
            dgif_gpkg.unlink()

        dgif_schema_log = tmp_dir / "dgif_schemaimport.log"
        dgif_schema_args = [
            "-jar",
            str(ili2gpkg_jar),
            "--schemaimport",
            "--dbfile",
            str(dgif_gpkg),
            "--defaultSrsAuth",
            "EPSG",
            "--defaultSrsCode",
            "4326",
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
            "--modeldir",
            dgif_model_dir,
            "--log",
            str(dgif_schema_log),
            str(dgif_ili),
        ]
        rc = run_java(dgif_schema_args, "Running ili2gpkg --schemaimport for DGIF...")
        if rc != 0:
            error(f"DGIF schema import failed! See: {dgif_schema_log}")
            return 1
        ok(f"DGIF GeoPackage schema created: {file_size_mb(dgif_gpkg)} MB")

    banner("Phase 2: Transform and Load (OSM ETL)")
    selected_topics = None
    if args.target_topics:
        selected_topics = {topic.strip() for topic in args.target_topics.split(",") if topic.strip()}

    try:
        transform(
            dgif_gpkg_path=str(dgif_gpkg),
            mapping_csv_path=str(mapping_csv),
            provider=args.provider,
            overpass_url=args.overpass_url or "",
            bbox=bbox,
            target_topics=selected_topics,
            allow_empty=False,
            aoi_geom=aoi_geom,
            overpass_timeout=args.overpass_timeout,
            user_agent=args.user_agent,
        )
    except urllib.error.HTTPError as exc:
        error(f"Overpass HTTP error: {exc.code} {exc.reason}")
        return 1
    except urllib.error.URLError as exc:
        error(f"Overpass request failed: {exc}")
        return 1
    except Exception as exc:
        error(str(exc))
        return 1

    info(f"Output written to {dgif_gpkg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())