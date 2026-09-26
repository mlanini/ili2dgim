import { useEffect, useRef, useState } from "react";
import type { Feature, FeatureCollection, Point, Polygon, Position } from "geojson";
import * as turf from "@turf/turf";
import maplibregl from "maplibre-gl";
import MapboxDraw from "@mapbox/mapbox-gl-draw";
import "maplibre-gl/dist/maplibre-gl.css";
import "@mapbox/mapbox-gl-draw/dist/mapbox-gl-draw.css";

type AoiMode = "rectangle" | "polygon" | "circle";

type Props = {
  mode: AoiMode;
  radiusKm: number;
  value: Feature<Polygon> | null;
  onChange: (feature: Feature<Polygon> | null) => void;
};

const AOI_SOURCE = "aoi-source";
const AOI_FILL = "aoi-fill";
const AOI_LINE = "aoi-line";
const BASEMAP_SOURCE = "basemap-raster";
const BASEMAP_LAYER = "basemap-raster-layer";

const OFFLINE_STYLE: maplibregl.StyleSpecification = {
  version: 8,
  sources: {},
  layers: [
    {
      id: "background",
      type: "background",
      paint: {
        "background-color": "#f3f4f6"
      }
    }
  ]
};

const BASEMAP_TILE_URLS = [
  "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
  "https://a.tile.openstreetmap.org/{z}/{x}/{y}.png",
  "https://b.tile.openstreetmap.org/{z}/{x}/{y}.png",
  "https://c.tile.openstreetmap.org/{z}/{x}/{y}.png"
];

function toRectPolygon(a: Position, b: Position): Feature<Polygon> {
  const [x1, y1] = a;
  const [x2, y2] = b;
  const minX = Math.min(x1, x2);
  const maxX = Math.max(x1, x2);
  const minY = Math.min(y1, y2);
  const maxY = Math.max(y1, y2);

  return {
    type: "Feature",
    properties: { mode: "rectangle" },
    geometry: {
      type: "Polygon",
      coordinates: [[[minX, minY], [maxX, minY], [maxX, maxY], [minX, maxY], [minX, minY]]]
    }
  };
}

function featureCollectionFrom(feature: Feature<Polygon> | null): FeatureCollection {
  if (!feature) {
    return { type: "FeatureCollection", features: [] };
  }
  return { type: "FeatureCollection", features: [feature] };
}

// mapbox-gl-draw@1.5.0's bundled default theme uses raw number arrays as
// branches of a "case" expression for `line-dasharray` (e.g. [0.2, 2]).
// MapLibre GL's stricter style validator (unlike older mapbox-gl) rejects
// this, requiring array literals to be wrapped as ["literal", [...]]. This
// caused "Expression name must be a string, but found number instead"
// console errors for the gl-draw-lines(.hot/.cold) layers. We patch just
// that one style entry before handing the theme to MapboxDraw.
const DRAW_STYLES = (
  MapboxDraw as unknown as { lib: { theme: Array<Record<string, unknown>> } }
).lib.theme.map((style) => {
  if (style.id !== "gl-draw-lines") {
    return style;
  }
  return {
    ...style,
    paint: {
      ...(style.paint as Record<string, unknown>),
      "line-dasharray": [
        "case",
        ["==", ["get", "active"], "true"],
        ["literal", [0.2, 2]],
        ["literal", [2, 0]]
      ]
    }
  };
});

export default function AoiMap({ mode, radiusKm, value, onChange }: Props) {
  const mapContainerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const drawRef = useRef<MapboxDraw | null>(null);
  const firstCornerRef = useRef<Position | null>(null);
  const [circleCenter, setCircleCenter] = useState<Position | null>(null);
  const [basemapWarning, setBasemapWarning] = useState<string>("");

  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current) {
      return;
    }

    const map = new maplibregl.Map({
      container: mapContainerRef.current,
      style: OFFLINE_STYLE,
      center: [8.2275, 46.8182],
      zoom: 7
    });

    map.addControl(new maplibregl.NavigationControl({ showCompass: true }), "top-right");

    const draw = new MapboxDraw({
      displayControlsDefault: false,
      controls: {
        polygon: true,
        trash: true
      },
      styles: DRAW_STYLES as never
    });

    map.on("load", () => {
      setBasemapWarning("");

      if (!map.getSource(BASEMAP_SOURCE)) {
        map.addSource(BASEMAP_SOURCE, {
          type: "raster",
          tiles: BASEMAP_TILE_URLS,
          tileSize: 256,
          attribution: "OpenStreetMap"
        });
      }

      if (!map.getLayer(BASEMAP_LAYER)) {
        map.addLayer({
          id: BASEMAP_LAYER,
          type: "raster",
          source: BASEMAP_SOURCE
        });
      }

      map.addControl(draw as never, "top-left");

      map.addSource(AOI_SOURCE, {
        type: "geojson",
        data: featureCollectionFrom(value)
      });

      map.addLayer({
        id: AOI_FILL,
        type: "fill",
        source: AOI_SOURCE,
        paint: {
          "fill-color": "#0f766e",
          "fill-opacity": 0.18
        }
      });

      map.addLayer({
        id: AOI_LINE,
        type: "line",
        source: AOI_SOURCE,
        paint: {
          "line-color": "#0f766e",
          "line-width": 2.5
        }
      });
    });

    map.on("error", (evt) => {
      const sourceId = (evt as { sourceId?: string }).sourceId;
      if (sourceId === BASEMAP_SOURCE) {
        setBasemapWarning("Basemap web non disponibile (rete/proxy). Disegno AOI comunque disponibile.");
      }
    });

    map.on("draw.create", () => {
      if (mode !== "polygon") {
        return;
      }
      const data = draw.getAll();
      const poly = data.features.find((f) => f.geometry.type === "Polygon") as Feature<Polygon> | undefined;
      if (poly) {
        onChange(poly);
      }
    });

    map.on("draw.update", () => {
      if (mode !== "polygon") {
        return;
      }
      const data = draw.getAll();
      const poly = data.features.find((f) => f.geometry.type === "Polygon") as Feature<Polygon> | undefined;
      if (poly) {
        onChange(poly);
      }
    });

    map.on("click", (event) => {
      const pos: Position = [event.lngLat.lng, event.lngLat.lat];

      if (mode === "rectangle") {
        if (!firstCornerRef.current) {
          firstCornerRef.current = pos;
          return;
        }
        const poly = toRectPolygon(firstCornerRef.current, pos);
        firstCornerRef.current = null;
        onChange(poly);
      }

      if (mode === "circle") {
        setCircleCenter(pos);
      }
    });

    mapRef.current = map;
    drawRef.current = draw;

    return () => {
      map.remove();
      mapRef.current = null;
      drawRef.current = null;
    };
  }, [mode, onChange, value]);

  useEffect(() => {
    const map = mapRef.current;
    const draw = drawRef.current;
    if (!map || !draw) {
      return;
    }

    if (mode === "polygon") {
      draw.changeMode("draw_polygon");
    } else {
      draw.changeMode("simple_select");
    }
    if (mode !== "rectangle") {
      firstCornerRef.current = null;
    }
  }, [mode]);

  useEffect(() => {
    if (!circleCenter || mode !== "circle") {
      return;
    }
    const circle = turf.circle(circleCenter as [number, number], radiusKm, {
      steps: 64,
      units: "kilometers"
    }) as Feature<Polygon>;
    circle.properties = { mode: "circle", radiusKm };
    onChange(circle);
  }, [circleCenter, mode, onChange, radiusKm]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !map.isStyleLoaded()) {
      return;
    }
    const source = map.getSource(AOI_SOURCE) as maplibregl.GeoJSONSource | undefined;
    if (source) {
      source.setData(featureCollectionFrom(value));
    }
  }, [value]);

  return (
    <div style={{ position: "relative" }}>
      <div ref={mapContainerRef} style={{ width: "100%", height: 400, borderRadius: 12 }} />
      {basemapWarning && (
        <div
          style={{
            position: "absolute",
            left: 12,
            bottom: 12,
            background: "rgba(255,255,255,0.92)",
            border: "1px solid #d4d4d8",
            borderRadius: 8,
            padding: "6px 10px",
            fontSize: 12,
            color: "#3f3f46",
            maxWidth: "75%"
          }}
        >
          {basemapWarning}
        </div>
      )}
    </div>
  );
}
