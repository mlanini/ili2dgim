#!/usr/bin/env python3
"""
Generate QGIS portrayal (.qml) style files for DGIF GeoPackage layers.

Symbology is inspired by the Swiss Landeskarte / military topographic line
map at 1:50'000 (LK50 / TLM50).  The LK50 uses a well-known thematic color
code that has been standard in Swiss and NATO military cartography since the
mid-20th century:

  - BLACK        — cultural features, buildings, text, infrastructure
  - RED / BROWN  — roads, significant transport routes
  - BLUE         — hydrography (rivers, lakes, canals, springs)
  - GREEN        — vegetation (forest, scrubland, vineyards, orchards)
  - BROWN/SIENNA — relief, contours, rock formations, physiography
  - GREY         — built-up areas, vehicle lots, compact surfaces
  - MAGENTA      — boundaries, administrative divisions
  - OLIVE        — military installations, firing ranges

Each populated DGIF table receives its own .qml file saved alongside the
GeoPackage in ``output/styles/<table_name>.qml``.  A master
``output/styles/DGIF_swissTLM3D.qml`` layer-definition file is also written
so that QGIS can load all layers at once with the correct styling.

Usage:
    python scripts/generate_portrayal.py [--gpkg output/DGIF_swissTLM3D.gpkg]

Dependencies: Python 3.10+ standard library only (no QGIS/PyQGIS needed).
"""

import argparse
import os
import sqlite3
import textwrap
from pathlib import Path

# ============================================================================
# TLM50 / Landeskarte color palette  (R, G, B, A)
# ============================================================================
# Derived from the Swiss National Map 1:50'000 specification.
# Colors follow NATO STANAG 3676 / MIL-STD-2500 symbology conventions
# adapted to Swiss national mapping tradition.
# ============================================================================

COLORS = {
    # Cultural / Buildings — black/dark grey
    "building_fill":       (40, 40, 40, 200),
    "building_stroke":     (0, 0, 0, 255),
    "cultural_fill":       (80, 80, 80, 160),
    "cultural_stroke":     (50, 50, 50, 255),
    "cultural_point":      (30, 30, 30, 255),

    # Transportation — red/brown (Verkehrsrot)
    "road_stroke":         (205, 50, 50, 255),      # Strasse rot
    "road_casing":         (140, 30, 30, 255),
    "railway_stroke":      (50, 50, 50, 255),        # Bahn schwarz
    "railway_cross":       (255, 255, 255, 255),      # white ticks
    "cableway_stroke":     (80, 80, 80, 255),
    "transport_point":     (180, 40, 40, 255),
    "transport_fill":      (220, 180, 160, 140),

    # Hydrography — blue (Gewässerblau)
    "water_fill":          (166, 206, 240, 200),      # Seefläche
    "water_stroke":        (56, 120, 190, 255),       # Flusslinie
    "water_point":         (40, 100, 180, 255),

    # Vegetation — green (Vegetationsgrün)
    "forest_fill":         (180, 215, 160, 190),      # Waldfläche
    "forest_stroke":       (90, 140, 70, 255),
    "scrub_fill":          (200, 225, 180, 170),
    "vineyard_fill":       (210, 230, 170, 170),
    "vegetation_point":    (60, 130, 50, 255),

    # Physiography / Relief — brown/sienna (Reliefbraun)
    "rock_fill":           (215, 200, 175, 180),
    "rock_stroke":         (160, 130, 90, 255),
    "terrain_fill":        (230, 215, 190, 150),
    "terrain_stroke":      (170, 140, 100, 255),
    "terrain_point":       (140, 110, 70, 255),

    # Boundaries — magenta/violet
    "boundary_fill":       (220, 180, 220, 80),
    "boundary_stroke":     (180, 50, 180, 255),

    # Military — olive green
    "military_fill":       (150, 160, 100, 140),
    "military_stroke":     (100, 110, 60, 255),

    # Population — warm grey / rose
    "populated_fill":      (240, 220, 210, 140),
    "populated_stroke":    (180, 140, 120, 255),

    # Agricultural — light green/yellow-green
    "agricultural_fill":   (220, 235, 170, 170),
    "agricultural_stroke": (140, 170, 80, 255),

    # Elevation — brown point symbols
    "elevation_point":     (140, 100, 60, 255),

    # Default fallback
    "default_fill":        (200, 200, 200, 120),
    "default_stroke":      (128, 128, 128, 255),
    "default_point":       (100, 100, 100, 255),
}


def rgba(key: str) -> str:
    """Return 'r,g,b,a' string for a color key."""
    return ",".join(str(c) for c in COLORS[key])


def hex_rgb(key: str) -> str:
    """Return '#rrggbb' hex string for a color key."""
    r, g, b, _ = COLORS[key]
    return f"#{r:02x}{g:02x}{b:02x}"


# ============================================================================
# Per-table symbology configuration
# ============================================================================
# Each entry maps a DGIF GeoPackage table name to a symbology dict:
#   geom_type:  POINT | LINESTRING | POLYGON (overridden from GPKG if available)
#   renderer:   "single" | "categorized"
#   fill:       color key for polygon fill
#   stroke:     color key for outline / line
#   width:      stroke width in mm
#   point:      color key for point symbol
#   size:       point symbol size in mm
#   symbol:     point marker name (circle, square, triangle, cross, star, diamond)
#   pattern:    dash pattern for lines (e.g. "5;2" = 5mm dash, 2mm gap)
#   label_field: field name to use for labeling (optional)
#   label_size:  label font size in points
#   categorize_field: field for categorized renderer
#   categories: dict of {value: {fill/stroke/...overrides}}
# ============================================================================

STYLES = {
    # ── Cultural ────────────────────────────────────────────────────────
    "cultural_building": {
        "fill": "building_fill", "stroke": "building_stroke", "width": 0.3,
    },
    "cultural_wall": {
        "stroke": "cultural_stroke", "width": 0.5,
        "fill": "cultural_fill",
    },
    "cultural_monument": {
        "point": "cultural_point", "size": 3.0, "symbol": "square",
    },
    "cultural_cableway": {
        "stroke": "cableway_stroke", "width": 0.5, "pattern": "3;2",
    },
    "cultural_vehiclelot": {
        "fill": "cultural_fill", "stroke": "cultural_stroke", "width": 0.2,
    },
    "cultural_sportsground": {
        "fill": "cultural_fill", "stroke": "cultural_stroke", "width": 0.2,
    },
    "cultural_waterwork": {
        "point": "water_point", "size": 3.5, "symbol": "circle",
    },
    "cultural_publicsquare": {
        "stroke": "cultural_stroke", "width": 0.6,
        "fill": "cultural_fill",
    },
    "cultural_tower": {
        "fill": "cultural_fill", "stroke": "building_stroke", "width": 0.4,
    },
    "cultural_storagetank": {
        "fill": "cultural_fill", "stroke": "building_stroke", "width": 0.3,
    },
    "cultural_cemetery": {
        "fill": "cultural_fill", "stroke": "cultural_stroke", "width": 0.3,
        # LK50: cemetery has cross-hatch pattern, we approximate with fill
    },
    "cultural_cable": {
        "stroke": "cableway_stroke", "width": 0.4, "pattern": "2;1.5",
    },
    "cultural_firingrange": {
        "stroke": "military_stroke", "width": 0.6, "pattern": "6;3",
        "fill": "military_fill",
    },
    "cultural_archeologicalsite": {
        "fill": "terrain_fill", "stroke": "terrain_stroke", "width": 0.4,
    },
    "cultural_aerial": {
        "point": "cultural_point", "size": 3.0, "symbol": "triangle",
    },

    # ── Transportation ──────────────────────────────────────────────────
    "transportation_landtransportationway": {
        "stroke": "road_stroke", "width": 0.8,
        "label_field": "waysignificance", "label_size": 7,
    },
    "transportation_railway": {
        "stroke": "railway_stroke", "width": 0.7, "pattern": "4;1;1;1",
    },
    "transportation_vehiclebarrier": {
        "point": "transport_point", "size": 2.5, "symbol": "cross",
    },
    "transportation_transportationstation": {
        "point": "transport_point", "size": 4.0, "symbol": "square",
    },
    "transportation_transportationplatform": {
        "fill": "transport_fill", "stroke": "road_stroke", "width": 0.3,
    },
    "transportation_pipeline": {
        "stroke": "cultural_stroke", "width": 0.4, "pattern": "5;2;1;2",
    },

    # ── Inland Water ────────────────────────────────────────────────────
    "inlandwater_river": {
        "stroke": "water_stroke", "width": 0.7,
        "fill": "water_fill",
    },
    "inlandwater_ditch": {
        "stroke": "water_stroke", "width": 0.4, "pattern": "3;1.5",
    },
    "inlandwater_inlandwaterbody": {
        "fill": "water_fill", "stroke": "water_stroke", "width": 0.3,
    },
    "inlandwater_dam": {
        "fill": "cultural_fill", "stroke": "building_stroke", "width": 0.5,
    },
    "inlandwater_waterfall": {
        "point": "water_point", "size": 3.5, "symbol": "triangle",
    },

    # ── Vegetation ──────────────────────────────────────────────────────
    "vegetation_forest": {
        "fill": "forest_fill", "stroke": "forest_stroke", "width": 0.3,
    },
    "vegetation_scrubland": {
        "fill": "scrub_fill", "stroke": "forest_stroke", "width": 0.2,
    },
    "vegetation_shrubland": {
        "fill": "scrub_fill", "stroke": "forest_stroke", "width": 0.2,
    },

    # ── Agricultural ────────────────────────────────────────────────────
    "agricultural_vineyard": {
        "fill": "vineyard_fill", "stroke": "agricultural_stroke", "width": 0.2,
    },

    # ── Physiography ────────────────────────────────────────────────────
    "physiography_rockformation": {
        "fill": "rock_fill", "stroke": "rock_stroke", "width": 0.3,
    },
    "physiography_soilsurfaceregion": {
        "fill": "terrain_fill", "stroke": "terrain_stroke", "width": 0.2,
    },
    "physiography_landmorphologyarea": {
        "fill": "terrain_fill", "stroke": "terrain_stroke", "width": 0.3,
    },
    "physiography_hill": {
        "point": "terrain_point", "size": 4.0, "symbol": "triangle",
    },
    "physiography_landarea": {
        "fill": "terrain_fill", "stroke": "terrain_stroke", "width": 0.2,
    },

    # ── Boundaries ──────────────────────────────────────────────────────
    "boundaries_administrativedivision": {
        "fill": "boundary_fill", "stroke": "boundary_stroke", "width": 0.8,
        "pattern": "6;3;2;3",
    },

    # ── Population ──────────────────────────────────────────────────────
    "population_populatedplace": {
        "fill": "populated_fill", "stroke": "populated_stroke", "width": 0.4,
        "label_field": "aname", "label_size": 9,
    },

    # ── Elevation ───────────────────────────────────────────────────────
    "elevation_geomorphicextreme": {
        "point": "elevation_point", "size": 3.5, "symbol": "triangle",
    },
}


# ============================================================================
# QML XML generation helpers
# ============================================================================

_QML_HEADER = """\
<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version="3.40" styleCategories="Symbology|Labeling|Rendering"
      simplifyLocal="1" simplifyDrawingTol="1" simplifyMaxScale="1"
      simplifyDrawingHints="1" simplifyAlgorithm="0"
      labelsEnabled="{labels_enabled}">
"""

_QML_FOOTER = """\
  <blendMode>0</blendMode>
  <featureBlendMode>0</featureBlendMode>
  <layerOpacity>1</layerOpacity>
</qgis>
"""


def _svg_marker_name(name: str) -> str:
    """Map our short marker name to QGIS simple marker shape."""
    mapping = {
        "circle": "circle",
        "square": "square",
        "triangle": "triangle",
        "cross": "cross",
        "star": "star",
        "diamond": "diamond",
        "pentagon": "pentagon",
    }
    return mapping.get(name, "circle")


def _build_point_symbol(style: dict, geom_type: str) -> str:
    """Build a simple marker symbol XML block."""
    color_key = style.get("point", "default_point")
    size = style.get("size", 2.5)
    marker = _svg_marker_name(style.get("symbol", "circle"))
    stroke_key = style.get("stroke", color_key)

    return textwrap.dedent(f"""\
        <symbol name="0" type="marker" clip_to_extent="1" force_rhr="0" alpha="1">
          <data_defined_properties>
            <Option type="Map"><Option name="name" value="" type="QString"/>
            <Option name="properties" type="Map"/><Option name="type" value="collection" type="QString"/></Option>
          </data_defined_properties>
          <layer class="SimpleMarker" pass="0" locked="0" enabled="1">
            <Option type="Map">
              <Option name="color" value="{rgba(color_key)}" type="QString"/>
              <Option name="name" value="{marker}" type="QString"/>
              <Option name="outline_color" value="{rgba(stroke_key)}" type="QString"/>
              <Option name="outline_width" value="0.3" type="QString"/>
              <Option name="outline_width_unit" value="MM" type="QString"/>
              <Option name="size" value="{size}" type="QString"/>
              <Option name="size_unit" value="MM" type="QString"/>
              <Option name="angle" value="0" type="QString"/>
              <Option name="horizontal_anchor_point" value="1" type="QString"/>
              <Option name="vertical_anchor_point" value="1" type="QString"/>
              <Option name="offset" value="0,0" type="QString"/>
              <Option name="offset_unit" value="MM" type="QString"/>
            </Option>
          </layer>
        </symbol>
    """)


def _build_line_symbol(style: dict, geom_type: str) -> str:
    """Build a simple line symbol XML block."""
    color_key = style.get("stroke", "default_stroke")
    width = style.get("width", 0.5)
    pattern = style.get("pattern", "")

    custom_dash = ""
    use_custom = "0"
    if pattern:
        use_custom = "1"
        custom_dash = f'<Option name="customdash" value="{pattern}" type="QString"/>'

    return textwrap.dedent(f"""\
        <symbol name="0" type="line" clip_to_extent="1" force_rhr="0" alpha="1">
          <data_defined_properties>
            <Option type="Map"><Option name="name" value="" type="QString"/>
            <Option name="properties" type="Map"/><Option name="type" value="collection" type="QString"/></Option>
          </data_defined_properties>
          <layer class="SimpleLine" pass="0" locked="0" enabled="1">
            <Option type="Map">
              <Option name="line_color" value="{rgba(color_key)}" type="QString"/>
              <Option name="line_width" value="{width}" type="QString"/>
              <Option name="line_width_unit" value="MM" type="QString"/>
              <Option name="capstyle" value="round" type="QString"/>
              <Option name="joinstyle" value="round" type="QString"/>
              <Option name="use_custom_dash" value="{use_custom}" type="QString"/>
              {custom_dash}
              <Option name="customdash_unit" value="MM" type="QString"/>
              <Option name="line_style" value="solid" type="QString"/>
              <Option name="offset" value="0" type="QString"/>
              <Option name="offset_unit" value="MM" type="QString"/>
            </Option>
          </layer>
        </symbol>
    """)


def _build_fill_symbol(style: dict, geom_type: str) -> str:
    """Build a simple fill symbol XML block."""
    fill_key = style.get("fill", "default_fill")
    stroke_key = style.get("stroke", "default_stroke")
    width = style.get("width", 0.3)
    pattern = style.get("pattern", "")

    border_style = "solid"
    custom_dash = ""
    use_custom = "0"
    if pattern:
        use_custom = "1"
        custom_dash = (
            f'<Option name="customdash" value="{pattern}" type="QString"/>\n'
            f'              <Option name="customdash_unit" value="MM" type="QString"/>'
        )

    return textwrap.dedent(f"""\
        <symbol name="0" type="fill" clip_to_extent="1" force_rhr="0" alpha="1">
          <data_defined_properties>
            <Option type="Map"><Option name="name" value="" type="QString"/>
            <Option name="properties" type="Map"/><Option name="type" value="collection" type="QString"/></Option>
          </data_defined_properties>
          <layer class="SimpleFill" pass="0" locked="0" enabled="1">
            <Option type="Map">
              <Option name="color" value="{rgba(fill_key)}" type="QString"/>
              <Option name="style" value="solid" type="QString"/>
              <Option name="outline_color" value="{rgba(stroke_key)}" type="QString"/>
              <Option name="outline_width" value="{width}" type="QString"/>
              <Option name="outline_width_unit" value="MM" type="QString"/>
              <Option name="outline_style" value="{border_style}" type="QString"/>
              <Option name="border_width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
              <Option name="use_custom_dash" value="{use_custom}" type="QString"/>
              {custom_dash}
              <Option name="joinstyle" value="round" type="QString"/>
              <Option name="offset" value="0,0" type="QString"/>
              <Option name="offset_unit" value="MM" type="QString"/>
            </Option>
          </layer>
        </symbol>
    """)


def _build_labeling(style: dict, table_name: str) -> str:
    """Build labeling XML block if label_field is specified."""
    label_field = style.get("label_field")
    if not label_field:
        return ""
    label_size = style.get("label_size", 8)

    return textwrap.dedent(f"""\
  <labeling type="simple">
    <settings calloutType="simple">
      <text-style fieldName="{label_field}" fontSize="{label_size}"
                  fontFamily="Noto Sans" fontWeight="50"
                  textColor="0,0,0,255" textOpacity="1"
                  namedStyle="Regular" isExpression="0"
                  multilineHeight="1" blendMode="0"
                  previewBkgrdColor="255,255,255,255"
                  allowHtml="0" forcedBold="0" forcedItalic="0"
                  capitalization="0" useSubstitutions="0">
        <text-buffer bufferSize="0.8" bufferSizeUnits="MM"
                     bufferColor="255,255,255,200" bufferDraw="1"
                     bufferBlendMode="0" bufferNoFill="0"
                     bufferOpacity="0.8" bufferJoinStyle="round"/>
      </text-style>
      <placement priority="5" maxCurvedCharAngleIn="25"
                 maxCurvedCharAngleOut="-25"
                 dist="1.5" distUnits="MM"
                 placement="2" placementFlags="10"
                 repeatDistance="150" repeatDistanceUnits="MM"
                 overrunDistance="0" overrunDistanceUnit="MM"
                 centroidWhole="0" centroidInside="0"
                 offsetType="0" geometryGenerator=""
                 geometryGeneratorType="PointGeometry"
                 geometryGeneratorEnabled="0"
                 labelOffsetMapUnitScale="3x:0,0,0,0,0,0"
                 xOffset="0" yOffset="0" offsetUnits="MM"
                 rotationAngle="0" rotationUnit="AngleDegrees"
                 preserveRotation="1" overlapHandling="PreventOverlap"
                 allowDegraded="0" lineAnchorType="0"
                 lineAnchorPercent="0.5"
                 lineAnchorClipping="0"/>
      <rendering scaleMin="0" scaleMax="100000"
                 scaleVisibility="1" limitNumLabels="0"
                 obstacle="1" obstacleFactor="1"
                 obstacleType="1" zIndex="0"
                 drawUnplaced="0" unplacedVisibility="0"
                 fontMinPixelSize="3" fontMaxPixelSize="10000"
                 fontLimitPixelSize="0" minFeatureSize="0"
                 mergeLines="1" upsidedownLabels="0"
                 displayAll="0"/>
    </settings>
  </labeling>
""")


def _determine_geom_category(geom_type: str) -> str:
    """Determine if geometry is point, line, or polygon."""
    gt = geom_type.upper()
    if "POINT" in gt:
        return "point"
    elif "LINE" in gt or "CURVE" in gt:
        return "line"
    elif "POLYGON" in gt or "SURFACE" in gt:
        return "polygon"
    else:
        # GEOMETRY — ambiguous; check style hints
        return "polygon"  # default for mixed


def generate_qml(table_name: str, geom_type: str, style: dict) -> str:
    """Generate a complete QML XML string for one layer."""
    geom_cat = _determine_geom_category(geom_type)

    # Override geom_cat if style explicitly declares point (has "point" key
    # but no "fill" key)
    if "point" in style and "fill" not in style:
        geom_cat = "point"
    elif "fill" in style and "point" not in style:
        if geom_cat == "point":
            geom_cat = "polygon"

    has_labels = "label_field" in style
    labels_enabled = "1" if has_labels else "0"

    parts = []
    parts.append(_QML_HEADER.format(labels_enabled=labels_enabled))

    # Renderer
    parts.append('  <renderer-v2 type="singleSymbol" symbollevels="0" enableorderby="0">')
    parts.append("    <symbols>")

    if geom_cat == "point":
        parts.append(_build_point_symbol(style, geom_type))
    elif geom_cat == "line":
        parts.append(_build_line_symbol(style, geom_type))
    else:
        parts.append(_build_fill_symbol(style, geom_type))

    parts.append("    </symbols>")
    parts.append("  </renderer-v2>")

    # Labeling
    if has_labels:
        parts.append(_build_labeling(style, table_name))

    parts.append(_QML_FOOTER)
    return "\n".join(parts)


def generate_default_qml(table_name: str, geom_type: str) -> str:
    """Generate a fallback QML for tables not in the STYLES dict."""
    geom_cat = _determine_geom_category(geom_type)
    if geom_cat == "point":
        style = {"point": "default_point", "size": 2.0, "symbol": "circle"}
    elif geom_cat == "line":
        style = {"stroke": "default_stroke", "width": 0.4}
    else:
        style = {"fill": "default_fill", "stroke": "default_stroke", "width": 0.2}
    return generate_qml(table_name, geom_type, style)


# ============================================================================
# Railway special symbol (black line with white perpendicular ticks)
# ============================================================================

def _generate_railway_qml(table_name: str, geom_type: str, style: dict) -> str:
    """Generate a railway-specific QML with hashed line ticks (LK50 style)."""
    has_labels = "label_field" in style
    labels_enabled = "1" if has_labels else "0"
    stroke_key = style.get("stroke", "railway_stroke")
    width = style.get("width", 0.7)

    parts = []
    parts.append(_QML_HEADER.format(labels_enabled=labels_enabled))
    parts.append('  <renderer-v2 type="singleSymbol" symbollevels="0" enableorderby="0">')
    parts.append("    <symbols>")
    parts.append(textwrap.dedent(f"""\
        <symbol name="0" type="line" clip_to_extent="1" force_rhr="0" alpha="1">
          <data_defined_properties>
            <Option type="Map"><Option name="name" value="" type="QString"/>
            <Option name="properties" type="Map"/><Option name="type" value="collection" type="QString"/></Option>
          </data_defined_properties>
          <!-- Base line (black) -->
          <layer class="SimpleLine" pass="0" locked="0" enabled="1">
            <Option type="Map">
              <Option name="line_color" value="{rgba(stroke_key)}" type="QString"/>
              <Option name="line_width" value="{width}" type="QString"/>
              <Option name="line_width_unit" value="MM" type="QString"/>
              <Option name="capstyle" value="flat" type="QString"/>
              <Option name="joinstyle" value="miter" type="QString"/>
              <Option name="line_style" value="solid" type="QString"/>
              <Option name="use_custom_dash" value="0" type="QString"/>
              <Option name="offset" value="0" type="QString"/>
              <Option name="offset_unit" value="MM" type="QString"/>
            </Option>
          </layer>
          <!-- Perpendicular ticks (white, LK50 Bahn symbol) -->
          <layer class="HashLine" pass="0" locked="0" enabled="1">
            <Option type="Map">
              <Option name="hash_length" value="2" type="QString"/>
              <Option name="hash_length_unit" value="MM" type="QString"/>
              <Option name="interval" value="4" type="QString"/>
              <Option name="interval_unit" value="MM" type="QString"/>
              <Option name="offset" value="0" type="QString"/>
              <Option name="offset_unit" value="MM" type="QString"/>
              <Option name="rotate" value="1" type="QString"/>
            </Option>
            <symbol name="@0@1" type="line" clip_to_extent="1" force_rhr="0" alpha="1">
              <data_defined_properties>
                <Option type="Map"><Option name="name" value="" type="QString"/>
                <Option name="properties" type="Map"/><Option name="type" value="collection" type="QString"/></Option>
              </data_defined_properties>
              <layer class="SimpleLine" pass="0" locked="0" enabled="1">
                <Option type="Map">
                  <Option name="line_color" value="255,255,255,255" type="QString"/>
                  <Option name="line_width" value="0.4" type="QString"/>
                  <Option name="line_width_unit" value="MM" type="QString"/>
                  <Option name="capstyle" value="flat" type="QString"/>
                  <Option name="joinstyle" value="miter" type="QString"/>
                  <Option name="line_style" value="solid" type="QString"/>
                  <Option name="use_custom_dash" value="0" type="QString"/>
                  <Option name="offset" value="0" type="QString"/>
                  <Option name="offset_unit" value="MM" type="QString"/>
                </Option>
              </layer>
            </symbol>
          </layer>
        </symbol>
    """))
    parts.append("    </symbols>")
    parts.append("  </renderer-v2>")
    if has_labels:
        parts.append(_build_labeling(style, table_name))
    parts.append(_QML_FOOTER)
    return "\n".join(parts)


# ============================================================================
# Road categorized renderer (by waysignificance)
# ============================================================================

_ROAD_CATEGORIES = {
    # waysignificance → (color_rgba, width, label)
    "motorway":     ("205,50,50,255",   1.4, "Autobahn"),
    "primary":      ("205,60,60,255",   1.0, "Hauptstrasse"),
    "secondary":    ("205,80,80,255",   0.8, "Nebenstrasse"),
    "local":        ("180,100,80,255",  0.5, "Gemeindestrasse"),
    "track":        ("150,120,100,255", 0.4, "Feldweg"),
    "path":         ("140,130,120,255", 0.3, "Fussweg"),
    "pedestrian":   ("140,130,120,255", 0.3, "Fussgaengerzone"),
}


def _generate_road_categorized_qml(table_name: str, geom_type: str, style: dict) -> str:
    """Generate a categorized road QML based on waysignificance (LK50 style)."""
    has_labels = "label_field" in style
    labels_enabled = "1" if has_labels else "0"

    parts = []
    parts.append(_QML_HEADER.format(labels_enabled=labels_enabled))
    parts.append(f'  <renderer-v2 type="categorizedSymbol" attr="waysignificance"'
                 f' symbollevels="0" enableorderby="0">')
    parts.append("    <categories>")

    idx = 0
    for val, (color, width, label) in _ROAD_CATEGORIES.items():
        parts.append(f'      <category value="{val}" symbol="{idx}" label="{label}" render="true"/>')
        idx += 1
    # Default for unknown
    parts.append(f'      <category value="" symbol="{idx}" label="Other" render="true"/>')
    parts.append("    </categories>")

    parts.append("    <symbols>")
    idx = 0
    for val, (color, width, label) in _ROAD_CATEGORIES.items():
        parts.append(textwrap.dedent(f"""\
            <symbol name="{idx}" type="line" clip_to_extent="1" force_rhr="0" alpha="1">
              <data_defined_properties>
                <Option type="Map"><Option name="name" value="" type="QString"/>
                <Option name="properties" type="Map"/><Option name="type" value="collection" type="QString"/></Option>
              </data_defined_properties>
              <layer class="SimpleLine" pass="0" locked="0" enabled="1">
                <Option type="Map">
                  <Option name="line_color" value="{color}" type="QString"/>
                  <Option name="line_width" value="{width}" type="QString"/>
                  <Option name="line_width_unit" value="MM" type="QString"/>
                  <Option name="capstyle" value="round" type="QString"/>
                  <Option name="joinstyle" value="round" type="QString"/>
                  <Option name="line_style" value="solid" type="QString"/>
                  <Option name="use_custom_dash" value="0" type="QString"/>
                  <Option name="offset" value="0" type="QString"/>
                  <Option name="offset_unit" value="MM" type="QString"/>
                </Option>
              </layer>
            </symbol>
        """))
        idx += 1

    # Default symbol (grey)
    parts.append(textwrap.dedent(f"""\
        <symbol name="{idx}" type="line" clip_to_extent="1" force_rhr="0" alpha="1">
          <data_defined_properties>
            <Option type="Map"><Option name="name" value="" type="QString"/>
            <Option name="properties" type="Map"/><Option name="type" value="collection" type="QString"/></Option>
          </data_defined_properties>
          <layer class="SimpleLine" pass="0" locked="0" enabled="1">
            <Option type="Map">
              <Option name="line_color" value="{rgba('road_stroke')}" type="QString"/>
              <Option name="line_width" value="0.6" type="QString"/>
              <Option name="line_width_unit" value="MM" type="QString"/>
              <Option name="capstyle" value="round" type="QString"/>
              <Option name="joinstyle" value="round" type="QString"/>
              <Option name="line_style" value="solid" type="QString"/>
              <Option name="use_custom_dash" value="0" type="QString"/>
              <Option name="offset" value="0" type="QString"/>
              <Option name="offset_unit" value="MM" type="QString"/>
            </Option>
          </layer>
        </symbol>
    """))

    parts.append("    </symbols>")
    parts.append("  </renderer-v2>")

    if has_labels:
        parts.append(_build_labeling(style, table_name))
    parts.append(_QML_FOOTER)
    return "\n".join(parts)


# ============================================================================
# Layer order (drawing order: bottom → top)
# ============================================================================

LAYER_ORDER = [
    # 1. Base surfaces (bottom)
    "physiography_soilsurfaceregion",
    "physiography_landarea",
    "physiography_landmorphologyarea",
    "physiography_rockformation",
    # 2. Water surfaces
    "inlandwater_inlandwaterbody",
    # 3. Vegetation
    "vegetation_forest",
    "vegetation_scrubland",
    "vegetation_shrubland",
    "agricultural_vineyard",
    # 4. Built-up areas
    "population_populatedplace",
    "boundaries_administrativedivision",
    # 5. Cultural surfaces
    "cultural_vehiclelot",
    "cultural_sportsground",
    "cultural_cemetery",
    "cultural_archeologicalsite",
    "cultural_firingrange",
    "cultural_publicsquare",
    "cultural_storagetank",
    # 6. Buildings
    "cultural_building",
    "cultural_tower",
    # 7. Water lines
    "inlandwater_river",
    "inlandwater_ditch",
    "inlandwater_dam",
    # 8. Transportation lines
    "transportation_landtransportationway",
    "transportation_railway",
    "transportation_pipeline",
    "cultural_cableway",
    "cultural_cable",
    "cultural_wall",
    # 9. Point symbols (top)
    "transportation_transportationplatform",
    "transportation_transportationstation",
    "transportation_vehiclebarrier",
    "cultural_monument",
    "cultural_waterwork",
    "cultural_aerial",
    "inlandwater_waterfall",
    "physiography_hill",
]


# ============================================================================
# QGIS Project file (.qgs) generation — lightweight layer-list
# ============================================================================

def _generate_qlr(gpkg_path: str, populated_tables: dict[str, str],
                  styles_dir: str) -> str:
    """Generate a QGIS Layer Definition (.qlr) XML for loading all layers."""
    gpkg_abs = os.path.abspath(gpkg_path).replace("\\", "/")
    styles_abs = os.path.abspath(styles_dir).replace("\\", "/")

    parts = []
    parts.append('<!DOCTYPE qgis-layer-definition>')
    parts.append('<qlr>')
    parts.append('  <layer-tree-group name="DGIF swissTLM3D (TLM50)" expanded="1">')

    # Order layers: use LAYER_ORDER for defined tables, then append remaining
    ordered = []
    for t in LAYER_ORDER:
        if t in populated_tables:
            ordered.append(t)
    for t in sorted(populated_tables.keys()):
        if t not in ordered:
            ordered.append(t)

    for table_name in ordered:
        geom_type = populated_tables[table_name]
        layer_name = table_name.replace("_", " ").title()
        geom_cat = _determine_geom_category(geom_type)
        qgis_geom_type = {"point": "Point", "line": "Line", "polygon": "Polygon"}.get(
            geom_cat, "Polygon"
        )
        parts.append(
            f'    <layer-tree-layer name="{layer_name}" '
            f'providerKey="ogr" '
            f'source="{gpkg_abs}|layername={table_name}" '
            f'id="{table_name}_001">'
        )
        parts.append(f'      <customproperties/>')
        parts.append(f'    </layer-tree-layer>')

    parts.append('  </layer-tree-group>')

    # Map layers
    parts.append('  <maplayers>')
    for table_name in ordered:
        geom_type = populated_tables[table_name]
        layer_name = table_name.replace("_", " ").title()
        geom_cat = _determine_geom_category(geom_type)
        qgis_geom_type = {"point": "Point", "line": "Line", "polygon": "Polygon"}.get(
            geom_cat, "Polygon"
        )
        wkb_type = {"point": "Point", "line": "LineString", "polygon": "Polygon"}.get(
            geom_cat, "Unknown"
        )
        qml_file = f"{styles_abs}/{table_name}.qml"

        parts.append(
            f'    <maplayer type="vector" geometry="{qgis_geom_type}" '
            f'id="{table_name}_001">'
        )
        parts.append(f'      <layername>{layer_name}</layername>')
        parts.append(
            f'      <datasource>{gpkg_abs}|layername={table_name}</datasource>'
        )
        parts.append(f'      <provider encoding="UTF-8">ogr</provider>')
        parts.append(f'      <srs><spatialrefsys>'
                     f'<authid>EPSG:4326</authid>'
                     f'</spatialrefsys></srs>')
        if os.path.isfile(qml_file.replace("/", os.sep)):
            parts.append(f'      <styleUrl>{qml_file}</styleUrl>')
        parts.append(f'    </maplayer>')

    parts.append('  </maplayers>')
    parts.append('</qlr>')
    return "\n".join(parts)


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate TLM50-style QGIS portrayal (.qml) for DGIF GeoPackage"
    )
    parser.add_argument(
        "--gpkg", default="output/DGIF_swissTLM3D.gpkg",
        help="Path to the populated DGIF GeoPackage (default: output/DGIF_swissTLM3D.gpkg)"
    )
    parser.add_argument(
        "--output-dir", default=None,
        help="Output directory for .qml files (default: output/styles/)"
    )
    args = parser.parse_args()

    gpkg_path = args.gpkg
    if not os.path.isfile(gpkg_path):
        print(f"[ERROR] GeoPackage not found: {gpkg_path}", file=__import__("sys").stderr)
        __import__("sys").exit(1)

    styles_dir = args.output_dir or os.path.join(os.path.dirname(gpkg_path), "styles")
    os.makedirs(styles_dir, exist_ok=True)

    # Discover populated tables
    print(f"[INFO] Reading GeoPackage: {gpkg_path}")
    conn = sqlite3.connect(gpkg_path)
    cur = conn.cursor()

    cur.execute("SELECT table_name, geometry_type_name FROM gpkg_geometry_columns")
    all_geom_tables = {r[0]: r[1] for r in cur.fetchall()}

    populated = {}
    for table_name, geom_type in all_geom_tables.items():
        try:
            cnt = cur.execute(f'SELECT COUNT(*) FROM [{table_name}]').fetchone()[0]
            if cnt > 0:
                populated[table_name] = geom_type
        except sqlite3.Error:
            pass

    conn.close()

    print(f"[INFO] Found {len(populated)} populated feature tables")
    print(f"[INFO] Output directory: {styles_dir}")
    print()

    # Generate QML files
    generated = 0
    for table_name, geom_type in sorted(populated.items()):
        style = STYLES.get(table_name)

        # Special renderers
        if table_name == "transportation_railway":
            qml_content = _generate_railway_qml(table_name, geom_type,
                                                 style or STYLES["transportation_railway"])
        elif table_name == "transportation_landtransportationway":
            qml_content = _generate_road_categorized_qml(
                table_name, geom_type,
                style or STYLES["transportation_landtransportationway"]
            )
        elif style:
            qml_content = generate_qml(table_name, geom_type, style)
        else:
            qml_content = generate_default_qml(table_name, geom_type)

        qml_path = os.path.join(styles_dir, f"{table_name}.qml")
        with open(qml_path, "w", encoding="utf-8") as f:
            f.write(qml_content)

        status = "styled" if style else "default"
        print(f"  [{status:7s}] {table_name:45s} {geom_type:20s} -> {table_name}.qml")
        generated += 1

    # Generate QLR (layer definition file)
    qlr_content = _generate_qlr(gpkg_path, populated, styles_dir)
    qlr_path = os.path.join(styles_dir, "DGIF_swissTLM3D.qlr")
    with open(qlr_path, "w", encoding="utf-8") as f:
        f.write(qlr_content)

    print()
    print(f"[INFO] Generated {generated} QML style files")
    print(f"[INFO] Generated layer definition: {qlr_path}")
    print()
    print("[INFO] To use in QGIS:")
    print(f"  1. Open QGIS and drag-and-drop '{qlr_path}' into the canvas")
    print(f"  2. Or: Layer > Add Layer > Add Vector Layer > {gpkg_path}")
    print(f"     Then right-click each layer > Properties > Symbology > Load Style")
    print(f"     and select the corresponding .qml file from {styles_dir}/")
    print()
    print("[INFO] Color palette legend (TLM50 / Landeskarte 1:50'000):")
    print("  BLACK/DARK GREY  - Buildings, infrastructure, cultural features")
    print("  RED              - Roads, main transport routes")
    print("  BLACK + TICKS    - Railways (Bahn)")
    print("  BLUE             - Rivers, lakes, water features")
    print("  GREEN            - Forest, vegetation, scrubland")
    print("  BROWN/SIENNA     - Rock formations, terrain, relief")
    print("  MAGENTA          - Administrative boundaries")
    print("  OLIVE            - Military installations, firing ranges")
    print("  YELLOW-GREEN     - Agricultural areas (vineyards)")


if __name__ == "__main__":
    main()
