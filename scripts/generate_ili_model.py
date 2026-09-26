#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generate INTERLIS 2.4 model (.ili) from DGIF_BL_2025-1.xmi.

Mapping rules (UML → INTERLIS 2.4 / eCH-0031 V2.1.0):
  - DGIM package                → MODEL DGIF_V3
  - DGIM thematic sub-packages  → TOPIC
  - uml:Class                   → CLASS (with OID)
  - ownedAttribute              → ATTRIBUTE with cardinality
  - uml:Generalization          → EXTENDS
  - uml:Association             → ASSOCIATION
  - uml:Enumeration (local)     → Inline enumeration DOMAIN
  - uml:DataType (Foundation)   → STRUCTURE
  - AttributeDataTypes          → mapped to INTERLIS base types

Output: output/DGIF_V3.ili
"""

import xml.etree.ElementTree as ET
import os
import sys
import re
import argparse
import subprocess
from collections import OrderedDict, defaultdict

from interlis_tool_paths import (
    describe_configured_interlis_tools,
    resolve_interlis_tool_path,
)

# ── Configuration ──────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
XMI_PATH = os.path.join(BASE_DIR, "ressources", "DGIF_BL_2025-1.xmi")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "DGIF_V3.ili")

XMI_NS_CANDIDATES = (
    "http://schema.omg.org/spec/XMI/2.1",
    "http://www.omg.org/spec/XMI/20110701",
)
UML_NS = "http://www.omg.org/spec/UML/20110701"

XMI_VERSION_PATTERN = re.compile(r"(\d{4}-\d+)")

# ── Helpers ────────────────────────────────────────────────────────────────

def xmi_attr(elem, local_name, default=""):
    for ns in XMI_NS_CANDIDATES:
        value = elem.get(f"{{{ns}}}{local_name}")
        if value:
            return value
    for key, value in elem.attrib.items():
        if key == local_name or key.endswith(f":{local_name}") or key.endswith(f"}}{local_name}"):
            return value
    return default

def xmi_type(elem):
    return xmi_attr(elem, "type", "")

def xmi_id(elem):
    return xmi_attr(elem, "id", "")

def xmi_idref(elem):
    return xmi_attr(elem, "idref", "")

def local_tag(elem):
    t = elem.tag
    return t.split("}")[-1] if "}" in t else t


def extract_baseline_version_from_xmi_path(xmi_path):
    """Extract baseline version (e.g. 2026-1) from XMI filename/path."""
    file_name = os.path.basename(str(xmi_path))
    match = XMI_VERSION_PATTERN.search(file_name)
    if match:
        return match.group(1)
    return "2025-1"


def sanitize_name(name):
    """Make a name safe for INTERLIS identifiers."""
    if not name:
        return "Unnamed"
    # Replace spaces and special chars
    name = re.sub(r'[^A-Za-z0-9_]', '_', name)
    # Ensure starts with letter
    if name and not name[0].isalpha():
        name = "C_" + name
    return name


def ili_cardinality(lower_val, upper_val):
    """Convert UML cardinality to INTERLIS [min..max] notation."""
    lo = normalize_cardinality_lower(lower_val, default="0")
    hi = normalize_cardinality_upper(upper_val, default="1")
    return f"[{lo}..{hi}]"


def normalize_cardinality_upper(value, default="*"):
    """Normalize UML upper bounds to INTERLIS-compatible values."""
    raw = str(value).strip() if value is not None else ""
    if not raw:
        return default
    if raw == "-1":
        return "*"
    return raw


def normalize_cardinality_lower(value, default="0"):
    """Normalize UML lower bounds; keep only non-negative integers."""
    raw = str(value).strip() if value is not None else ""
    if not raw:
        return default
    if raw.isdigit():
        return raw
    return default


# ── Global ID→Name map ────────────────────────────────────────────────────

def build_id_name_map(root):
    """Build a global map of xmi:id → element name for resolving references."""
    id_map = {}
    for elem in root.iter():
        eid = xmi_id(elem)
        ename = elem.get("name", "")
        if eid and ename:
            id_map[eid] = ename
    return id_map


def build_id_elem_map(root):
    """Build a global map of xmi:id → element for resolving references."""
    id_map = {}
    for elem in root.iter():
        eid = xmi_id(elem)
        if eid:
            id_map[eid] = elem
    return id_map


# ── Navigation ─────────────────────────────────────────────────────────────

def find_package_by_name(parent, name):
    for elem in parent:
        lt = local_tag(elem)
        if lt == "packagedElement":
            if xmi_type(elem) == "uml:Package" and elem.get("name", "") == name:
                return elem
    return None


def find_model_element(root):
    for child in root:
        lt = local_tag(child)
        if lt == "Model" or (lt == "packagedElement" and xmi_type(child) == "uml:Model"):
            return child
    return root


def navigate_packages(root, path_names):
    """Navigate through nested packages."""
    current = find_model_element(root)
    for start_index in range(len(path_names)):
        candidate = current
        matched = True
        for name in path_names[start_index:]:
            found = find_package_by_name(candidate, name)
            if found is None:
                matched = False
                break
            candidate = found
        if matched:
            return candidate
    print(f"  WARNING: Package path not found: {'/'.join(path_names)}")
    return None


def get_child_packages(parent):
    """Get all direct child packages."""
    pkgs = []
    for elem in parent:
        if local_tag(elem) == "packagedElement" and xmi_type(elem) == "uml:Package":
            pkgs.append(elem)
    return pkgs


def get_child_elements_by_type(parent, uml_type_str):
    """Get all direct child packagedElements of a given xmi:type."""
    results = []
    for elem in parent:
        if local_tag(elem) == "packagedElement" and xmi_type(elem) == uml_type_str:
            results.append(elem)
    return results


def collect_elements_recursive(parent, uml_type_str):
    """Recursively collect all packagedElements of a given type."""
    results = []
    for elem in parent:
        lt = local_tag(elem)
        if lt == "packagedElement":
            if xmi_type(elem) == uml_type_str:
                results.append(elem)
            # Recurse into sub-packages
            if xmi_type(elem) == "uml:Package":
                results.extend(collect_elements_recursive(elem, uml_type_str))
    return results


# ── DataType mapping ───────────────────────────────────────────────────────

# Map from known Foundation DataType names → INTERLIS type
INTERLIS_TYPE_MAP = {
    # Basic primitives
    "Boolean": "BOOLEAN",
    "Text": "TEXT*255",
    "Real": "0.000 .. 999999999.999",
    "Integer": "-2000000000 .. 2000000000",
    "Count": "0 .. 2000000000",
    "Index": "0 .. 2000000000",
    "IntegerNonNegative": "0 .. 2000000000",
    "RealNonNegative": "0.000 .. 999999999.999",
    # Geometry – WGS84 (EPSG:4326)
    "GmPoint": "Coord2",
    "GmCurve": "Line",
    "GmSurface": "Surface",
    "GmMultiPoint": "Coord2",
    "GmMultiCurve": "Line",
    "GmMultiSurface": "Surface",
    "GmSolid": "Coord3",
    "GmMultiSolid": "Coord3",
    # Geometry meta wrapper → placeholder; actual type resolved per-class
    # from geometry_GEO OCL constraints
    "GeometryInfoMeta": "GeometryPlaceholder",
    # Intervals
    "RealInterval": "0.000 .. 999999999.999",
    "IntegerInterval": "-2000000000 .. 2000000000",
    "MeasureInterval": "0.000 .. 999999999.999",
    "CountInterval": "0 .. 2000000000",
    "RealNonNegInterval": "0.000 .. 999999999.999",
    # Constrained reals
    "RealNonNeg359": "0.000 .. 359.000",
    "RealNonNeg359r9": "0.000 .. 359.900",
    "RealNonNeg360": "0.000 .. 360.000",
    "Realm180to180": "-180.000 .. 180.000",
    "RealNonNeg0r00to4r99": "0.000 .. 4.990",
    "RealNonNegInterval100": "0.000 .. 100.000",
    "RealNonNegInterval360": "0.000 .. 360.000",
    # Constrained integers
    "Integer0to359": "0 .. 359",
    "IntegerNonNeg0to120": "0 .. 120",
    # Meta types → STRUCTURE refs
    "BooleanMeta": "BOOLEAN",
    "CharacterStringMeta": "TEXT*255",
    "MeasureMeta": "0.000 .. 999999999.999",
    "RealMeta": "0.000 .. 999999999.999",
    "IntegerMeta": "-2000000000 .. 2000000000",
    "IntegerIntervalMeta": "-2000000000 .. 2000000000",
    "MeasureIntervalMeta": "0.000 .. 999999999.999",
    "HydroTypeMeta": "TEXT*255",
    "CI_CitationMeta": "TEXT*1024",
    "CI_ContactMeta": "TEXT*1024",
    "CI_ResponsiblePartyMeta": "TEXT*1024",
    # ISO types
    "CI_Citation": "TEXT*1024",
    "CI_Contact": "TEXT*1024",
    "CI_ResponsibleParty": "TEXT*1024",
    # Enumeration
    "Enumeration": "TEXT*255",
    # Key types
    "KeyNonLex5": "TEXT*5",
    "KeyNonLex18": "TEXT*18",
    "KeyNonLex24": "TEXT*24",
    "KeyNonLex80": "TEXT*80",
    "KeyNonLex254": "TEXT*254",
    # StrucText types → TEXT
    "TextNonLex60": "TEXT*60",
}

# Pattern for *StrucText → TEXT*255
STRUCTEXT_PATTERN = re.compile(r'^.*StrucText$')
# Pattern for *Meta → simplified
META_PATTERN = re.compile(r'^(.+)Meta$')
# Pattern for *Reason → skip
REASON_PATTERN = re.compile(r'^(.+)Reason$')
# Pattern for *Union → skip
UNION_PATTERN = re.compile(r'^(.+)Union$')

# ── Geometry attribute-name mapping ────────────────────────────────────────
# The XMI references ISO 19107 geometry types (GM_Point, GM_Curve, …) via
# external IDs that are NOT included in the file.  We resolve them by the
# well-known attribute name used in the DGIF Foundation classes.
GEOMETRY_ATTR_NAME_MAP = {
    "geometry":             "GeometryPlaceholder",  # FeatureEntity.geometry  (GM_Object → resolved per-class via geometry_GEO constraint)
    "pointGeometry":        "Coord2",          # PointGeometryInfo       (GM_Point)
    "curveGeometry":        "Line",            # CurveGeometryInfo       (GM_Curve)
    "surfaceGeometry":      "Surface",         # SurfaceGeometryInfo     (GM_Surface)
    "multiPointGeometry":   "Coord2",          # MultiPointGeometryInfo  (GM_MultiPoint)
    "multiCurveGeometry":   "Line",            # MultiCurveGeometryInfo  (GM_MultiCurve)
    "multiSurfaceGeometry": "Surface",         # MultiSurfaceGeometryInfo(GM_MultiSurface)
    "solidGeometry":        "Coord3",          # SolidGeometryInfo       (GM_Solid → 3-D fallback)
    "multiSolidGeometry":   "Coord3",          # MultiSolidGeometryInfo  (GM_MultiSolid → 3-D fallback)
}

# ── Geometry constraint extraction ─────────────────────────────────────────
# Each concrete FeatureEntity subclass in the XMI carries an ownedRule named
# "geometry_GEO" with an OCL body such as:
#   inv: geometry->forAll(g|g.oclIsKindOf(PointGeometryInfo))
#   inv: geometry->forAll(g|(g.oclIsKindOf(CurveGeometryInfo) or g.oclIsKindOf(SurfaceGeometryInfo)))
# We parse these to determine the correct INTERLIS geometry type per class.

# Map from GeometryInfo class name (in OCL) → INTERLIS geometry type
_OCL_GEOM_MAP = {
    "PointGeometryInfo":        "Coord2",
    "CurveGeometryInfo":        "Line",
    "SurfaceGeometryInfo":      "Surface",
    "MultiPointGeometryInfo":   "Coord2",
    "MultiCurveGeometryInfo":   "Line",
    "MultiSurfaceGeometryInfo": "Surface",
    "SolidGeometryInfo":        "Coord3",
    "MultiSolidGeometryInfo":   "Coord3",
}

# Priority order for choosing a single INTERLIS type when the OCL constraint
# allows multiple geometry types.  Higher index → higher priority.
_GEOM_PRIORITY = {"Coord2": 0, "Coord3": 1, "Line": 2, "Surface": 3}

_OCL_KINDOF_RE = re.compile(r'oclIsKindOf\((\w+)\)')


def build_geometry_type_map(root, id_name_map):
    """Extract geometry_GEO constraints from XMI and build a map:
        class_name (sanitized) → INTERLIS geometry type string.

    For classes with multiple allowed geometry types (e.g. Point or Surface),
    we pick the one with the highest _GEOM_PRIORITY (Surface > Line > Coord2).
    This gives concrete classes the most expressive geometry column.
    """
    geom_map = {}  # sanitized class name → INTERLIS type

    def _extract_constraint_body(constraint_elem):
        """Return OCL text for geometry_GEO constraints across XMI variants."""
        # EA/DGIF 2026-1 format: <constraint ... description="...inv: ..."/>
        desc = constraint_elem.get("description", "")
        if desc:
            return desc
        # EA/DGIF 2025-1 format: <ownedRule><specification body="..."/></ownedRule>
        spec = None
        for child in constraint_elem:
            if local_tag(child) == "specification":
                spec = child
                break
        if spec is not None:
            return spec.get("body", "")
        return ""

    # Iterate classes directly so we always know the owning class, regardless
    # of whether constraints are encoded as uml:ownedRule or uml:constraint.
    for class_elem in root.iter():
        if local_tag(class_elem) not in ("packagedElement", "element") or xmi_type(class_elem) != "uml:Class":
            continue

        class_raw = class_elem.get("name", "")
        if not class_raw:
            continue
        class_safe = sanitize_name(class_raw)

        class_ili_types = set()
        for child in class_elem.iter():
            if child is class_elem:
                continue
            lt = local_tag(child)
            if lt not in ("ownedRule", "constraint"):
                continue
            if child.get("name", "") != "geometry_GEO":
                continue

            body = _extract_constraint_body(child)
            if not body:
                continue

            for match_name in _OCL_KINDOF_RE.findall(body):
                geom_type = _OCL_GEOM_MAP.get(match_name)
                if geom_type:
                    class_ili_types.add(geom_type)

        if not class_ili_types:
            continue

        best = max(class_ili_types, key=lambda t: _GEOM_PRIORITY.get(t, -1))
        geom_map[class_safe] = best

    return geom_map

# ── Angle attribute-name pattern ───────────────────────────────────────────
# Attributes whose name contains "Angle" represent angular measurements in
# degrees.  The DGIF conceptual model defines them as Units.Angle_Degree with
# range 0.00..360.00.  "AngleAcc" attributes represent accuracy of an angle
# (e.g. ILS glide-path angle accuracy) — same unit, same range.
# We match by regex so new attributes are automatically handled.
ANGLE_ATTR_PATTERN = re.compile(r'[Aa]ngle', re.IGNORECASE)
ANGLE_ILI_TYPE = "0.000 .. 360.000 [Units.Angle_Degree]"


def resolve_interlis_type(type_id, id_name_map, local_enums, id_elem_map,
                          attr_name=None):
    """
    Resolve a UML type reference to an INTERLIS type string.
    Returns (type_str, is_enum_ref, enum_name_or_none)
    """
    type_name = id_name_map.get(type_id, "")
    
    if not type_name:
        # Type ID not resolved – check if it's a known geometry attribute name
        if attr_name and attr_name in GEOMETRY_ATTR_NAME_MAP:
            return (GEOMETRY_ATTR_NAME_MAP[attr_name], False, None)
        # Check if it's an Angle attribute by name pattern
        if attr_name and ANGLE_ATTR_PATTERN.search(attr_name):
            return (ANGLE_ILI_TYPE, False, None)
        return ("TEXT*255", False, None)
    
    # 0) Angle attributes: if the attribute name matches the Angle pattern,
    #    override whatever the DataType says (typically Real/RealNonNegative)
    #    with the proper Units.Angle_Degree range.
    if attr_name and ANGLE_ATTR_PATTERN.search(attr_name):
        return (ANGLE_ILI_TYPE, False, None)

    # 0b) Geometry attributes: the 2026-1 XMI may use different external
    #     type names/IDs for ISO geometry classes. Use stable DGIF attribute
    #     names as the source of truth for INTERLIS geometry domains.
    if attr_name and attr_name in GEOMETRY_ATTR_NAME_MAP:
        return (GEOMETRY_ATTR_NAME_MAP[attr_name], False, None)
    
    # 1) Check direct mapping
    if type_name in INTERLIS_TYPE_MAP:
        return (INTERLIS_TYPE_MAP[type_name], False, None)
    
    # 2) Check if it's a StrucText pattern
    if STRUCTEXT_PATTERN.match(type_name):
        return ("TEXT*255", False, None)
    
    # 3) Check if it's a Meta wrapper
    meta_match = META_PATTERN.match(type_name)
    if meta_match:
        base_name = meta_match.group(1)
        if base_name in INTERLIS_TYPE_MAP:
            return (INTERLIS_TYPE_MAP[base_name], False, None)
        return ("TEXT*255", False, None)
    
    # 4) Check if Reason / Union → skip
    if REASON_PATTERN.match(type_name) or UNION_PATTERN.match(type_name):
        return (None, False, None)  # skip attribute
    
    # 5) Check if it's a local enumeration (defined in same package)
    if type_id in local_enums:
        enum_elem = local_enums[type_id]
        return (None, True, enum_elem)
    
    # 6) Check if the element in id_elem_map is an Enumeration
    elem = id_elem_map.get(type_id)
    if elem is not None:
        if xmi_type(elem) == "uml:Enumeration":
            return (None, True, elem)
        # Foundation Enumeration (e.g., intervalClosureType, valueNilReason)
        if xmi_type(elem) == "uml:DataType":
            # It's a complex DataType → treat as STRUCTURE reference
            return (f"TEXT*255 !! STRUCTURE {sanitize_name(type_name)}", False, None)
    
    # 7) Default: TEXT
    return ("TEXT*255", False, None)


# ── Extract model components ───────────────────────────────────────────────

def extract_class_info(cls_elem, id_name_map, id_elem_map, local_enums):
    """Extract class information: name, attributes, generalization, constraints."""
    info = {
        "name": sanitize_name(cls_elem.get("name", "Unnamed")),
        "raw_name": cls_elem.get("name", "Unnamed"),
        "id": xmi_id(cls_elem),
        "attributes": [],
        "generalization": None,
        "constraints": [],
        "assoc_attrs": [],  # attributes that are association ends
    }
    
    for child in cls_elem:
        lt = local_tag(child)
        
        if lt == "ownedAttribute":
            attr_name = child.get("name", "")
            if not attr_name:
                continue
            
            # Check if it's an association end (has association attribute)
            assoc_id = child.get("association", "")
            aggregation = child.get("aggregation", "")
            
            # Get type reference
            type_elem = child.find("type")
            type_id = ""
            if type_elem is not None:
                type_id = xmi_idref(type_elem)
            
            # Get cardinality
            lower_elem = child.find("lowerValue")
            upper_elem = child.find("upperValue")
            lower_val = lower_elem.get("value", "1") if lower_elem is not None else "1"
            upper_val = upper_elem.get("value", "1") if upper_elem is not None else "1"
            
            attr_info = {
                "name": sanitize_name(attr_name),
                "raw_name": attr_name,
                "type_id": type_id,
                "lower": lower_val,
                "upper": upper_val,
                "association": assoc_id,
                "aggregation": aggregation,
            }
            
            if assoc_id:
                info["assoc_attrs"].append(attr_info)
            else:
                info["attributes"].append(attr_info)
        
        elif lt == "generalization" and xmi_type(child) == "uml:Generalization":
            general_id = child.get("general", "")
            if general_id:
                info["generalization"] = general_id
        
        elif lt == "ownedRule":
            constraint_name = child.get("name", "")
            spec_elem = child.find("specification")
            body = ""
            if spec_elem is not None:
                body = spec_elem.get("body", "")
            if constraint_name:
                info["constraints"].append({
                    "name": constraint_name,
                    "body": body,
                })
    
    return info


def extract_association_info(assoc_elem, id_name_map, id_elem_map):
    """Extract association info: name, ends with roles and cardinalities."""
    info = {
        "name": sanitize_name(assoc_elem.get("name", "")),
        "raw_name": assoc_elem.get("name", ""),
        "id": xmi_id(assoc_elem),
        "ends": [],
    }
    
    # Collect all memberEnd references
    member_end_ids = []
    for child in assoc_elem:
        lt = local_tag(child)
        if lt == "memberEnd":
            ref = xmi_idref(child)
            if ref:
                member_end_ids.append(ref)
    
    # Collect ownedEnd elements
    for child in assoc_elem:
        lt = local_tag(child)
        if lt == "ownedEnd" and xmi_type(child) == "uml:Property":
            end_name = child.get("name", "")
            aggregation = child.get("aggregation", "")
            
            type_elem = child.find("type")
            type_id = ""
            if type_elem is not None:
                type_id = xmi_idref(type_elem)
            
            lower_elem = child.find("lowerValue")
            upper_elem = child.find("upperValue")
            lower_val = lower_elem.get("value", "1") if lower_elem is not None else "1"
            upper_val = upper_elem.get("value", "1") if upper_elem is not None else "1"
            
            type_name = id_name_map.get(type_id, "Unknown")
            
            info["ends"].append({
                "role": sanitize_name(end_name) if end_name else sanitize_name(type_name),
                "raw_role": end_name,
                "class_id": type_id,
                "class_name": sanitize_name(type_name),
                "lower": lower_val,
                "upper": upper_val,
                "aggregation": aggregation,
            })
    
    return info


def extract_enumeration_literals(enum_elem):
    """Extract enumeration literal names from a uml:Enumeration element."""
    literals = []
    for child in enum_elem:
        lt = local_tag(child)
        if lt == "ownedLiteral":
            name = child.get("name", "")
            if name:
                literals.append(sanitize_name(name))
    return literals


def topological_sort_classes(class_infos, local_class_names, id_name_map):
    """
    Sort class_info dicts so that:
      1) parent classes come before children (EXTENDS)  — hard constraint
      2) referenced classes come before referencing classes (REFERENCE TO)  — soft
    Only considers dependencies within local_class_names (same topic).
    External dependencies are ignored for ordering purposes.

    When REFERENCE TO creates cycles, EXTENDS ordering is still honoured and
    a best-effort heuristic minimizes unresolved intra-topic forward refs.
    """
    name_to_info = {}
    for ci in class_infos:
        name_to_info[ci["name"]] = ci

    # ── Build two dependency sets: hard (EXTENDS) and soft (REFERENCE TO) ──
    extends_deps = {ci["name"]: set() for ci in class_infos}
    ref_deps     = {ci["name"]: set() for ci in class_infos}

    for ci in class_infos:
        cname = ci["name"]

        # 1) EXTENDS dependency (hard)
        parent_name = ci.get("_parent_safe", "")
        if parent_name and parent_name in local_class_names:
            extends_deps[cname].add(parent_name)

        # 2) REFERENCE TO dependencies (soft)
        for assoc_attr in ci.get("assoc_attrs", []):
            target_id = assoc_attr.get("type_id", "")
            if target_id:
                raw = id_name_map.get(target_id, "")
                target_safe = sanitize_name(raw) if raw else ""
                if target_safe and target_safe in local_class_names and target_safe != cname:
                    ref_deps[cname].add(target_safe)

    # ── Hard-topological + soft-reference heuristic ──
    # We enforce EXTENDS strictly. Among classes currently eligible
    # (hard in-degree == 0), we pick the one with the fewest unresolved
    # local REFERENCE TO dependencies to reduce commented forward refs.
    hard_in_degree = {ci["name"]: len(extends_deps[ci["name"]]) for ci in class_infos}
    hard_children = {ci["name"]: [] for ci in class_infos}
    for cname, deps in extends_deps.items():
        for dep in deps:
            if dep in hard_children:
                hard_children[dep].append(cname)

    placed = set()
    sorted_names = []
    remaining = set(name_to_info.keys())

    while remaining:
        ready = [n for n in remaining if hard_in_degree.get(n, 0) == 0]

        if not ready:
            # Defensive fallback for malformed/cyclic EXTENDS graphs.
            node = sorted(remaining)[0]
        else:
            def _rank(n):
                unresolved_soft = len([d for d in ref_deps.get(n, set()) if d not in placed])
                total_soft = len(ref_deps.get(n, set()))
                return (unresolved_soft, total_soft, n)

            node = min(ready, key=_rank)

        sorted_names.append(node)
        placed.add(node)
        remaining.remove(node)

        for ch in hard_children.get(node, []):
            if ch in hard_in_degree and hard_in_degree[ch] > 0:
                hard_in_degree[ch] -= 1

    return [name_to_info[n] for n in sorted_names if n in name_to_info]


def collect_inherited_attr_names(cls_info, all_class_infos_map, id_name_map):
    """Recursively collect all attribute names (attributes + assoc_attrs)
    inherited from parent classes.  Returns a set of sanitized attr names."""
    inherited = set()
    gen_id = cls_info.get("generalization", "")
    if not gen_id:
        return inherited
    parent_raw = id_name_map.get(gen_id, "")
    if not parent_raw:
        return inherited
    parent_safe = sanitize_name(parent_raw)
    parent_ci = all_class_infos_map.get(parent_safe)
    if parent_ci is None:
        return inherited
    # Direct attributes of parent
    for a in parent_ci.get("attributes", []):
        inherited.add(a["name"])
    for a in parent_ci.get("assoc_attrs", []):
        inherited.add(a["name"])
    # Recurse up
    inherited |= collect_inherited_attr_names(parent_ci, all_class_infos_map, id_name_map)
    return inherited


def ancestor_has_geometry(cls_info, all_class_infos_map, id_name_map, geom_type_map):
    """Check whether any ancestor class in the inheritance chain already has
    a geometry attribute emitted (i.e. is present in geom_type_map).
    Returns the ancestor's geometry type string if found, else None."""
    gen_id = cls_info.get("generalization", "")
    if not gen_id:
        return None
    parent_raw = id_name_map.get(gen_id, "")
    if not parent_raw:
        return None
    parent_safe = sanitize_name(parent_raw)
    # Check if parent itself has a geometry entry
    if parent_safe in geom_type_map:
        return geom_type_map[parent_safe]
    # Recurse up
    parent_ci = all_class_infos_map.get(parent_safe)
    if parent_ci is None:
        return None
    return ancestor_has_geometry(parent_ci, all_class_infos_map, id_name_map, geom_type_map)


def topological_sort_topics(topic_deps):
    """
    Sort topic names topologically so that each topic is emitted after
    all topics it DEPENDS ON.  Where circular dependencies exist the
    algorithm breaks cycles by deferring the back-edge (the dependency
    that would create a cycle).  Returns:
      ordered   – list of topic names in emission order
      effective – dict  topic → set of deps that are satisfied
                  (i.e. the target topic appears *before* this one)
    """
    from collections import deque

    all_topics = set(topic_deps.keys())

    # Build in-degree (only counting edges whose target exists)
    in_degree = {t: 0 for t in all_topics}
    fwd = {t: set() for t in all_topics}   # t -> dependents
    for t, deps in topic_deps.items():
        for d in deps:
            if d in all_topics:
                in_degree[t] += 1
                fwd[d].add(t)

    queue = deque(sorted(t for t in all_topics if in_degree[t] == 0))
    ordered = []
    while queue:
        node = queue.popleft()
        ordered.append(node)
        for ch in sorted(fwd[node]):
            in_degree[ch] -= 1
            if in_degree[ch] == 0:
                queue.append(ch)

    # Remaining nodes are in cycles – add them (cycle-break fallback)
    remaining = [t for t in sorted(all_topics) if t not in set(ordered)]
    ordered.extend(remaining)

    # Compute effective DEPENDS ON: only deps already emitted earlier
    position = {t: i for i, t in enumerate(ordered)}
    effective = {}
    for t in ordered:
        effective[t] = {d for d in topic_deps.get(t, set())
                        if d in position and position[d] < position[t]}
    return ordered, effective


# ── INTERLIS writer ────────────────────────────────────────────────────────

class IliWriter:
    def __init__(self):
        self.lines = []
        self.indent = 0
    
    def write(self, text=""):
        prefix = "  " * self.indent
        self.lines.append(prefix + text)
    
    def blank(self):
        self.lines.append("")
    
    def inc(self):
        self.indent += 1
    
    def dec(self):
        self.indent = max(0, self.indent - 1)
    
    def get_text(self):
        return "\n".join(self.lines) + "\n"


def write_ili_header(w, baseline_version, source_xmi_name):
    w.write("INTERLIS 2.4;")
    w.blank()
    w.write(f"/** DGIF Baseline {baseline_version} - Defence Geospatial Information Model (DGIM)")
    w.write(f" *  Auto-generated from {source_xmi_name}")
    w.write(" *  INTERLIS 2.4 / eCH-0031 V2.1.0")
    w.write(" */")
    w.blank()


def write_model_header(w, model_name, baseline_version):
    w.write(f"MODEL {model_name} (en)")
    w.write(f'  AT "https://www.dgiwg.org/dgif"')
    w.write(f'  VERSION "{baseline_version}" =')
    w.blank()
    w.inc()
    w.write("IMPORTS Units;")
    w.blank()
    # DOMAIN – WGS84 geometry types (EPSG:4326)
    w.write("DOMAIN")
    w.blank()
    w.inc()
    w.write("Coord2 = COORD -180.000 .. 180.000 [Units.Angle_Degree], -90.000 .. 90.000 [Units.Angle_Degree],")
    w.write("  ROTATION 2 -> 1;")
    w.blank()
    w.write("Coord3 = COORD -180.000 .. 180.000 [Units.Angle_Degree], -90.000 .. 90.000 [Units.Angle_Degree],")
    w.write("  -10000.000 .. 100000.000 [INTERLIS.m],")
    w.write("  ROTATION 2 -> 1;")
    w.blank()
    w.write("Line = POLYLINE WITH (STRAIGHTS, ARCS) VERTEX Coord2;")
    w.blank()
    w.write("Surface = SURFACE WITH (STRAIGHTS, ARCS) VERTEX Coord2 WITHOUT OVERLAPS > 0.001;")
    w.blank()
    w.dec()
    w.blank()


def write_model_footer(w, model_name):
    w.dec()
    w.write(f"END {model_name}.")
    w.blank()


def write_topic_header(w, topic_name, depends_on=None, model_name="DGIF_V3"):
    # INTERLIS 2.4 grammar: DEPENDS ON goes INSIDE the topic body (after '=')
    w.write(f"TOPIC {topic_name} =")
    w.inc()
    if depends_on:
        # TopicRef = [ Model-Name '.' ] Topic-Name
        deps = ", ".join(f"{model_name}.{d}" for d in sorted(depends_on))
        w.write(f"DEPENDS ON {deps};")
    w.blank()


def write_topic_footer(w, topic_name):
    w.dec()
    w.write(f"END {topic_name};")
    w.blank()


def write_class(w, cls_info, id_name_map, id_elem_map, local_enums, 
                all_class_names, class_to_topic=None, current_topic=None,
                effective_deps=None, model_name="DGIF_V3",
                inherited_attr_names=None, emitted_class_names=None,
                geom_type_map=None, all_class_infos_map=None):
    """Write a single CLASS definition.
    
    effective_deps:      set of topic names declared in DEPENDS ON
    inherited_attr_names: set of attr names inherited from parent chain;
                          if an attr name appears here it is emitted
                          with the (EXTENDED) qualifier.
    emitted_class_names:  set of class names already emitted in this topic;
                          REFERENCE TO targeting a class not yet emitted
                          (intra-topic forward ref) is commented out.
    geom_type_map:        dict class_name → INTERLIS geometry type, built from
                          geometry_GEO OCL constraints in the XMI.
    all_class_infos_map:  dict class_name → cls_info for ancestor lookups.
    """
    if geom_type_map is None:
        geom_type_map = {}
    if all_class_infos_map is None:
        all_class_infos_map = {}
    if inherited_attr_names is None:
        inherited_attr_names = set()
    if emitted_class_names is None:
        emitted_class_names = set()
    name = cls_info["name"]
    
    # Look up the per-class geometry type from the geometry_GEO OCL constraints
    class_geom_type = geom_type_map.get(name, None)
    
    # Check whether an ancestor already defines the geometry attribute.
    # If so, this class inherits it and must NOT re-declare it (INTERLIS
    # does not allow re-declaration unless the new type EXTENDS the old one,
    # which is not the case for Coord2 / Line / Surface).
    ancestor_geom = ancestor_has_geometry(
        cls_info, all_class_infos_map, id_name_map, geom_type_map)
    if ancestor_geom is not None:
        # An ancestor already provides geometry; suppress for this class.
        class_geom_type = None
    
    # Determine EXTENDS
    extends_str = ""
    if cls_info["generalization"]:
        parent_name = id_name_map.get(cls_info["generalization"], "")
        if parent_name:
            parent_safe = sanitize_name(parent_name)
            # Qualify with Model.Topic.Class if parent is in a different topic (§3.5.3)
            if class_to_topic and current_topic:
                parent_topic = class_to_topic.get(parent_safe, "")
                if parent_topic and parent_topic != current_topic:
                    parent_safe = f"{model_name}.{parent_topic}.{parent_safe}"
            extends_str = f" EXTENDS {parent_safe}"
    
    w.write(f"CLASS {name}{extends_str} =")
    w.inc()
    
    # Write attributes
    geometry_emitted = False
    for attr in cls_info["attributes"]:
        attr_name = attr["name"]
        type_id = attr["type_id"]
        lower = attr["lower"]
        upper = attr["upper"]
        
        # Check if this overrides an inherited attribute
        is_extended = attr_name in inherited_attr_names
        extended_tag = " (EXTENDED)" if is_extended else ""
        
        # Resolve type
        ili_type, is_enum, enum_elem = resolve_interlis_type(
            type_id, id_name_map, local_enums, id_elem_map,
            attr_name=attr.get("raw_name", attr_name))
        
        if ili_type is None and not is_enum:
            continue  # skip Reason/Union types
        
        # ── Per-class geometry override ────────────────────────────────
        # The base FeatureEntity.geometry resolves to "GeometryPlaceholder".
        # We skip it in the base class and emit it per-class with the correct
        # type derived from the geometry_GEO OCL constraint.
        if ili_type == "GeometryPlaceholder":
            # Don't emit the generic placeholder in the base class.
            # The concrete subclass will provide the correct type.
            continue
        
        # If this is a geometry attr inherited from FeatureEntity and
        # this concrete class has a specific geometry type, emit it
        # with the correct type (overriding the inherited placeholder).
        if attr_name == "geometry" and is_extended and class_geom_type:
            w.write(f"geometry : MANDATORY {class_geom_type};")
            geometry_emitted = True
            continue
        
        # Determine MANDATORY
        mandatory = " MANDATORY" if lower != "0" and not is_extended else ""
        
        # Flattened model: multi-valued attributes (BAG OF) are collapsed to
        # single-valued.  This avoids ili2gpkg issues with BAG OF on a flat
        # GeoPackage schema and simplifies downstream ETL processing.
        if is_enum and enum_elem is not None:
            literals = extract_enumeration_literals(enum_elem)
            if literals:
                enum_inline = "(" + ", ".join(literals) + ")"
                w.write(f"{attr_name}{extended_tag} :{mandatory} {enum_inline};")
            else:
                w.write(f"{attr_name}{extended_tag} :{mandatory} TEXT*255;")
        else:
            type_str = ili_type if ili_type else "TEXT*255"
            if "!!" in type_str:
                base_type, inline_comment = type_str.split("!!", 1)
                w.write(
                    f"{attr_name}{extended_tag} :{mandatory} {base_type.strip()};"
                    f" !! {inline_comment.strip()}"
                )
            else:
                w.write(f"{attr_name}{extended_tag} :{mandatory} {type_str};")
    
    # ── Emit geometry attribute for concrete FeatureEntity subclasses ──
    # If this class has a geometry_GEO constraint and we haven't already
    # emitted geometry above, emit it now.  The "geometry" attribute is
    # inherited from FeatureEntity (which no longer defines it in the .ili)
    # so each concrete class provides its own typed geometry attribute.
    if class_geom_type and not geometry_emitted:
        w.write(f"geometry : MANDATORY {class_geom_type};")
    
    # Write association-end attributes as references
    for assoc_attr in cls_info["assoc_attrs"]:
        attr_name = assoc_attr["name"]
        type_id = assoc_attr["type_id"]
        target_name = id_name_map.get(type_id, "")
        
        # If this REFERENCE TO attribute name collides with an inherited one,
        # skip it entirely – INTERLIS will inherit the parent's definition.
        # We cannot safely emit (EXTENDED) because the new target class may
        # not extend the parent's target class as INTERLIS requires.
        if attr_name in inherited_attr_names:
            continue
        
        if target_name:
            target_safe = sanitize_name(target_name)
            is_cross_topic = False  # track whether (EXTERNAL) is needed
            # Qualify with Model.Topic.Class if target is in a different topic (§3.5.3)
            if class_to_topic and current_topic:
                target_topic = class_to_topic.get(target_safe, "")
                if target_topic and target_topic != current_topic:
                    is_cross_topic = True
                    if effective_deps and target_topic in effective_deps:
                        # Target topic is already defined -> qualify
                        target_safe = f"{model_name}.{target_topic}.{target_safe}"
                    else:
                        # Target topic not yet defined -> cannot reference
                        # Emit as INTERLIS comment (forward ref not allowed)
                        w.write(f"!! {attr_name} : REFERENCE TO (EXTERNAL) {model_name}.{target_topic}.{target_safe};  !! forward reference - topic {target_topic} not yet defined")
                        continue
                elif target_topic == current_topic:
                    # Intra-topic: check if target class is already emitted
                    if target_safe not in emitted_class_names and target_safe != name:
                        w.write(f"!! {attr_name} : REFERENCE TO {target_safe};  !! forward reference - class {target_safe} not yet defined in this topic")
                        continue
            # INTERLIS 2.4 §2.6.3: cross-topic REFERENCE TO requires (EXTERNAL)
            ext_kw = " (EXTERNAL)" if is_cross_topic else ""
            w.write(f"{attr_name} : REFERENCE TO{ext_kw} {target_safe};")
    
    w.dec()
    w.write(f"END {name};")
    w.blank()


def write_association(w, assoc_info, id_name_map, all_class_names,
                      class_to_topic=None, current_topic=None,
                      effective_deps=None, emitted_topics=None,
                      model_name="DGIF_V3"):
    """Write an ASSOCIATION definition in INTERLIS 2.4 syntax.
    
    Correct INTERLIS 2.4 RoleDef syntax (§3.7.1):
      RoleDef = Role-Name ('--' | '-<>' | '-<#>') [ Cardinality ] ClassRef ';'
      Cardinality = '{' ( '*' | PosNumber [ '..' ( PosNumber | '*' ) ] ) '}'
    
    Aggregation strength symbols (§3.7.2):
      '--'   = Association (plain)
      '-<>'  = Aggregation   (role pointing to the Whole)
      '-<#>' = Composition   (role pointing to the Whole)
    """
    if not assoc_info["name"] or len(assoc_info["ends"]) < 2:
        return
    
    name = assoc_info["name"]
    effective_deps = effective_deps or set()
    emitted_topics = emitted_topics or set()
    
    # ── Pre-check: can all referenced classes be resolved? ──
    role_lines = []
    comment_out = False
    comment_reason = ""
    
    for end in assoc_info["ends"]:
        role = end["role"]
        class_name = end["class_name"]
        lower = end["lower"]
        upper = end["upper"]
        
        # INTERLIS 2.4 association cardinality uses {min..max}
        lo = normalize_cardinality_lower(lower, default="0")
        hi = normalize_cardinality_upper(upper, default="*")
        
        # Relationship strength symbol (§3.7.2)
        # aggregation on the ownedEnd marks the role leading to the Whole
        if end["aggregation"] == "composite":
            rel_symbol = "-<#>"
            # §3.7.2: Composition – a part object may belong to at most one
            # whole, so the max cardinality of the composite-side role must be 1
            if hi != "1" and hi != "0":
                hi = "1"
                if lo not in ("0", "1"):
                    lo = "0"
        elif end["aggregation"] == "shared":
            rel_symbol = "-<>"
        else:
            rel_symbol = "--"
        
        card = f"{{{lo}..{hi}}}"
        
        # Determine qualified class name for cross-topic references (§3.5.3)
        qualified_name = class_name
        is_cross_topic_role = False
        if class_to_topic and current_topic:
            target_topic = class_to_topic.get(class_name, "")
            if target_topic and target_topic != current_topic:
                is_cross_topic_role = True
                if target_topic in effective_deps or target_topic in emitted_topics:
                    qualified_name = f"{model_name}.{target_topic}.{class_name}"
                else:
                    # Target topic not accessible → comment out entire association
                    comment_out = True
                    comment_reason = (f"forward reference – class {class_name} "
                                      f"is in topic {target_topic} which is not yet available")
            elif not target_topic and class_name not in all_class_names:
                comment_out = True
                comment_reason = f"class {class_name} not found in model"
        
        # EXTERNAL property for cross-topic roles (§3.7.5)
        ext_kw = " (EXTERNAL)" if is_cross_topic_role else ""
        role_lines.append(f"{role}{ext_kw} {rel_symbol} {card} {qualified_name};")
    
    # ── Write association ──
    prefix = "!! " if comment_out else ""
    suffix = f"  !! {comment_reason}" if comment_out else ""
    
    w.write(f"{prefix}ASSOCIATION {name} ={suffix}")
    w.inc()
    for line in role_lines:
        w.write(f"{prefix}{line}")
    w.dec()
    w.write(f"{prefix}END {name};")
    w.blank()


# ── Main logic ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate DGIF INTERLIS model (.ili) from DGIF XMI"
    )
    parser.add_argument(
        "--xmi-path",
        default=XMI_PATH,
        help=f"Path to DGIF XMI file (default: {XMI_PATH})",
    )
    parser.add_argument(
        "--output-file",
        default=OUTPUT_FILE,
        help=f"Output INTERLIS file path (default: {OUTPUT_FILE})",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional output directory; if set, writes DGIF_V3.ili in this folder",
    )
    args = parser.parse_args()

    for configured_tool in describe_configured_interlis_tools():
        print(f"Configured tool: {configured_tool}")

    xmi_path = args.xmi_path
    output_file = args.output_file
    if args.output_dir:
        output_file = os.path.join(args.output_dir, "DGIF_V3.ili")

    print("=" * 70)
    print("DGIF XMI -> INTERLIS 2.4 Model Generator")
    print("=" * 70)
    
    # Parse XMI
    print(f"\nParsing XMI: {xmi_path}")
    tree = ET.parse(xmi_path)
    root = tree.getroot()
    print("XMI parsed successfully.")
    
    # Build global maps
    print("Building global ID->Name map...")
    id_name_map = build_id_name_map(root)
    print(f"  {len(id_name_map)} elements mapped.")
    
    print("Building global ID->Element map...")
    id_elem_map = build_id_elem_map(root)
    print(f"  {len(id_elem_map)} elements mapped.")
    
    # Build per-class geometry type map from geometry_GEO OCL constraints
    print("Extracting geometry_GEO constraints...")
    geom_type_map = build_geometry_type_map(root, id_name_map)
    geom_counts = {}
    for gt in geom_type_map.values():
        geom_counts[gt] = geom_counts.get(gt, 0) + 1
    print(f"  {len(geom_type_map)} classes with geometry constraints: {geom_counts}")
    
    # Navigate to DGIM
    dgim = navigate_packages(root, ["DGIF", "DGIM"])
    if dgim is None:
        print("ERROR: DGIM package not found!")
        sys.exit(1)
    print("DGIM package found.")
    
    # Get DGIM thematic sub-packages (first level)
    thematic_packages = get_child_packages(dgim)
    print(f"Found {len(thematic_packages)} thematic sub-packages:")
    for pkg in thematic_packages:
        print(f"  - {pkg.get('name', '?')}")
    
    baseline_version = extract_baseline_version_from_xmi_path(xmi_path)
    source_xmi_name = os.path.basename(xmi_path)

    # Start writing INTERLIS
    w = IliWriter()
    write_ili_header(w, baseline_version, source_xmi_name)
    write_model_header(w, "DGIF_V3", baseline_version)
    
    # Collect all class names across all topics for cross-references
    # Also build class_to_topic mapping
    all_class_names = set()
    class_to_topic = {}  # class_name → topic_name
    for pkg in thematic_packages:
        topic_name = sanitize_name(pkg.get("name", "Unnamed"))
        for cls in collect_elements_recursive(pkg, "uml:Class"):
            cname = sanitize_name(cls.get("name", ""))
            all_class_names.add(cname)
            class_to_topic[cname] = topic_name
    print(f"\nTotal classes across all topics: {len(all_class_names)}")
    
    # ── Pre-compute full cross-topic dependencies for every TOPIC ──────
    # We need this BEFORE ordering so we can do a topological sort of topics.
    pkg_by_name = {}  # topic_name → pkg element
    topic_full_deps = {}  # topic_name → set of all other topics it depends on
    topic_extends_deps = {}  # topic_name → set of topics needed for EXTENDS only
    topic_ref_deps = {}  # topic_name → set of topics needed for REFERENCE TO only
    topic_class_infos = {}  # topic_name → list of class_info dicts
    topic_local_names = {}  # topic_name → set of local class names
    topic_enums = {}  # topic_name → local_enums dict
    topic_assocs = {}  # topic_name → list of association elements
    
    for pkg in thematic_packages:
        topic_name = sanitize_name(pkg.get("name", "Unnamed"))
        pkg_by_name[topic_name] = pkg
        
        classes = collect_elements_recursive(pkg, "uml:Class")
        associations = collect_elements_recursive(pkg, "uml:Association")
        enumerations = collect_elements_recursive(pkg, "uml:Enumeration")
        topic_assocs[topic_name] = associations
        
        if not classes and not associations:
            topic_full_deps[topic_name] = set()
            topic_class_infos[topic_name] = []
            topic_local_names[topic_name] = set()
            topic_enums[topic_name] = {}
            continue
        
        # Build local enum map
        local_enums = {}
        for enum in enumerations:
            eid = xmi_id(enum)
            if eid:
                local_enums[eid] = enum
        topic_enums[topic_name] = local_enums
        
        # Build class infos
        local_class_names = set()
        class_infos = []
        for cls in classes:
            cls_info = extract_class_info(cls, id_name_map, id_elem_map, local_enums)
            local_class_names.add(cls_info["name"])
            if cls_info["generalization"]:
                parent_name = id_name_map.get(cls_info["generalization"], "")
                cls_info["_parent_safe"] = sanitize_name(parent_name) if parent_name else ""
            else:
                cls_info["_parent_safe"] = ""
            class_infos.append(cls_info)
        
        # Topological sort within this topic
        class_infos = topological_sort_classes(class_infos, local_class_names, id_name_map)
        topic_class_infos[topic_name] = class_infos
        topic_local_names[topic_name] = local_class_names
        
        # Compute deps separately: EXTENDS (structural) vs REFERENCE TO (weak)
        extends_deps = set()
        ref_deps = set()
        for ci in class_infos:
            parent_safe = ci.get("_parent_safe", "")
            if parent_safe and parent_safe not in local_class_names:
                pt = class_to_topic.get(parent_safe, "")
                if pt and pt != topic_name:
                    extends_deps.add(pt)
            for assoc_attr in ci.get("assoc_attrs", []):
                tid = assoc_attr.get("type_id", "")
                if tid:
                    raw = id_name_map.get(tid, "")
                    ts = sanitize_name(raw) if raw else ""
                    if ts and ts not in local_class_names:
                        tt = class_to_topic.get(ts, "")
                        if tt and tt != topic_name:
                            ref_deps.add(tt)
        topic_extends_deps[topic_name] = extends_deps
        topic_ref_deps[topic_name] = ref_deps
        topic_full_deps[topic_name] = extends_deps | ref_deps
    
    # ── Topological sort of TOPICs ─────────────────────────────────────
    # Use ONLY the EXTENDS dependencies (structural) for ordering.
    # EXTENDS deps are mandatory – a topic that extends a class from
    # another topic MUST come after that topic.
    # REFERENCE TO deps are weak – if the target topic is not yet
    # emitted we simply drop the REFERENCE TO attribute (no forward ref).
    ordered_topic_names, _ = topological_sort_topics(topic_extends_deps)
    
    # Build effective DEPENDS ON for each topic:
    #   = extends_deps  (always satisfied by construction of the order)
    #   + ref_deps that point to topics already emitted before this one
    position = {t: i for i, t in enumerate(ordered_topic_names)}
    effective_deps_map = {}
    for t in ordered_topic_names:
        eff = set(topic_extends_deps.get(t, set()))  # EXTENDS always in
        for rd in topic_ref_deps.get(t, set()):
            if rd in position and position[rd] < position[t]:
                eff.add(rd)  # REFERENCE TO only if already emitted
        effective_deps_map[t] = eff
    
    print(f"\nTopic emission order (topological):")
    for tn in ordered_topic_names:
        eff = sorted(effective_deps_map.get(tn, set()))
        full = sorted(topic_full_deps.get(tn, set()))
        fwd = sorted(set(full) - set(eff))
        line = f"  {tn}"
        if eff:
            line += f"  DEPENDS ON {eff}"
        if fwd:
            line += f"  (forward-ref: {fwd})"
        print(line)
    
    # ── Emit TOPICs in topological order ───────────────────────────────
    total_classes = 0
    total_assocs = 0
    emitted_topics = set()  # track already-emitted topics for association cross-refs
    
    # Build global class_info map for inherited-attribute lookups
    all_class_infos_map = {}  # class_name → cls_info
    for tn in ordered_topic_names:
        for ci in topic_class_infos.get(tn, []):
            all_class_infos_map[ci["name"]] = ci
    
    for topic_name in ordered_topic_names:
        class_infos = topic_class_infos.get(topic_name, [])
        associations = topic_assocs.get(topic_name, [])
        local_enums = topic_enums.get(topic_name, {})
        
        if not class_infos and not associations:
            continue
        
        eff_deps = effective_deps_map.get(topic_name, set())
        
        print(f"\nTOPIC {topic_name}: {len(class_infos)} classes, "
              f"{len(associations)} associations"
              f"{f', DEPENDS ON {sorted(eff_deps)}' if eff_deps else ''}")
        
        write_topic_header(w, topic_name, eff_deps if eff_deps else None)
        
        # Write classes (topologically sorted)
        emitted_class_names = set()
        for cls_info in class_infos:
            inherited = collect_inherited_attr_names(
                cls_info, all_class_infos_map, id_name_map)
            write_class(w, cls_info, id_name_map, id_elem_map, local_enums,
                       all_class_names, class_to_topic, topic_name,
                       effective_deps=eff_deps, model_name="DGIF_V3",
                       inherited_attr_names=inherited,
                       emitted_class_names=emitted_class_names,
                       geom_type_map=geom_type_map,
                       all_class_infos_map=all_class_infos_map)
            emitted_class_names.add(cls_info["name"])
            total_classes += 1
        
        # Write associations
        for assoc in associations:
            assoc_info = extract_association_info(assoc, id_name_map, id_elem_map)
            if assoc_info["name"] and len(assoc_info["ends"]) >= 2:
                write_association(w, assoc_info, id_name_map, all_class_names,
                                  class_to_topic=class_to_topic,
                                  current_topic=topic_name,
                                  effective_deps=eff_deps,
                                  emitted_topics=emitted_topics,
                                  model_name="DGIF_V3")
                total_assocs += 1
        
        write_topic_footer(w, topic_name)
        emitted_topics.add(topic_name)
    
    write_model_footer(w, "DGIF_V3")
    
    # Write output
    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(w.get_text())
    
    print(f"\n{'=' * 70}")
    print(f"INTERLIS model generated: {output_file}")
    print(f"  Topics: {len([p for p in thematic_packages if collect_elements_recursive(p, 'uml:Class')])}")
    print(f"  Classes: {total_classes}")
    print(f"  Associations: {total_assocs}")
    print(f"{'=' * 70}")

    ili2c_jar = resolve_interlis_tool_path("ili2c")
    if ili2c_jar is not None and ili2c_jar.exists():
        model_dir = os.path.dirname(output_file) or os.path.join(BASE_DIR, "models")
        print("\nValidating generated model with ili2c...")
        result = subprocess.run(
            [
                "java",
                "-jar",
                str(ili2c_jar),
                "--modeldir",
                model_dir,
                output_file,
            ],
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if result.stdout.strip():
            print(result.stdout.strip())
        if result.returncode != 0:
            if result.stderr.strip():
                print(result.stderr.strip())
            print(f"WARNING: ili2c validation failed, but the model file was generated successfully: {output_file}")
        else:
            print(f"ili2c validation OK: {ili2c_jar}")


if __name__ == "__main__":
    main()
