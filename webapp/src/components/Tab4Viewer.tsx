import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { FeatureCollection } from "geojson";
import maplibregl from "maplibre-gl";
import {
  Box,
  Card,
  CardContent,
  Stack,
  Typography,
  Alert,
  Button,
  TextField,
  MenuItem,
  CircularProgress,
  Chip
} from "@mui/material";
import InfoRounded from "@mui/icons-material/InfoRounded";
import "maplibre-gl/dist/maplibre-gl.css";
import { Api, getApiBase } from "../api";

type Tab4ViewerProps = {
  outputPath: string;
  outputFolder: string;
};

const OUTPUT_SOURCE = "output-source";
const OUTPUT_FILL = "output-fill";
const OUTPUT_LINE = "output-line";
const OUTPUT_POINT = "output-point";
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

function formatBytes(value: number): string {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  if (value < 1024 * 1024 * 1024) return `${(value / (1024 * 1024)).toFixed(1)} MB`;
  return `${(value / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

function fallbackEmptyCollection(): FeatureCollection {
  return { type: "FeatureCollection", features: [] };
}

export default function Tab4Viewer({ outputPath, outputFolder }: Tab4ViewerProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const [basemapWarning, setBasemapWarning] = useState("");

  const [files, setFiles] = useState<Array<{ name: string; path: string; size: number; modifiedAt: string; format: string }>>([]);
  const [selectedPath, setSelectedPath] = useState("");
  const [layers, setLayers] = useState<Array<{ name: string; geometryType: string; featureCount: number; extent: [number, number, number, number] | null; srs: string }>>([]);
  const [selectedLayer, setSelectedLayer] = useState("");
  const [geojson, setGeojson] = useState<FeatureCollection>(fallbackEmptyCollection());
  const [stats, setStats] = useState({
    totalFeatures: 0,
    featuresReturned: 0,
    truncated: false,
    bbox: null as [number, number, number, number] | null,
    driver: ""
  });
  const [loadingFiles, setLoadingFiles] = useState(false);
  const [loadingLayer, setLoadingLayer] = useState(false);
  const [error, setError] = useState("");

  const selectedFile = useMemo(
    () => files.find((item) => item.path === selectedPath) ?? null,
    [files, selectedPath]
  );

  const refreshFiles = useCallback(async () => {
    setLoadingFiles(true);
    setError("");
    try {
      const result = await Api.viewer.listFiles(outputFolder || undefined);
      setFiles(result.files);

      const preferred = outputPath && result.files.some((item) => item.path === outputPath)
        ? outputPath
        : result.files[0]?.path || "";
      setSelectedPath(preferred);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(`Unable to load output datasets: ${message}`);
      setFiles([]);
      setSelectedPath("");
    } finally {
      setLoadingFiles(false);
    }
  }, [outputFolder, outputPath]);

  useEffect(() => {
    void refreshFiles();
  }, [refreshFiles]);

  useEffect(() => {
    if (!selectedPath) {
      setLayers([]);
      setSelectedLayer("");
      setGeojson(fallbackEmptyCollection());
      setStats({ totalFeatures: 0, featuresReturned: 0, truncated: false, bbox: null, driver: "" });
      return;
    }

    let cancelled = false;
    const loadLayers = async () => {
      setError("");
      try {
        const result = await Api.viewer.listLayers(selectedPath);
        if (cancelled) return;
        setLayers(result.layers);
        setSelectedLayer(result.layers[0]?.name || "");
      } catch (err) {
        if (cancelled) return;
        const message = err instanceof Error ? err.message : String(err);
        setError(`Unable to inspect dataset layers: ${message}`);
        setLayers([]);
        setSelectedLayer("");
      }
    };
    void loadLayers();
    return () => {
      cancelled = true;
    };
  }, [selectedPath]);

  useEffect(() => {
    if (!selectedPath || !selectedLayer) {
      setGeojson(fallbackEmptyCollection());
      setStats({ totalFeatures: 0, featuresReturned: 0, truncated: false, bbox: null, driver: "" });
      return;
    }

    let cancelled = false;
    const loadLayer = async () => {
      setLoadingLayer(true);
      setError("");
      try {
        const result = await Api.viewer.getLayerGeoJson(selectedPath, selectedLayer, 2500);
        if (cancelled) return;
        setGeojson(result.geojson);
        setStats({
          totalFeatures: result.totalFeatures,
          featuresReturned: result.featuresReturned,
          truncated: result.truncated,
          bbox: result.bbox,
          driver: result.driver
        });
      } catch (err) {
        if (cancelled) return;
        const message = err instanceof Error ? err.message : String(err);
        setError(`Unable to load map preview: ${message}`);
        setGeojson(fallbackEmptyCollection());
      } finally {
        if (!cancelled) {
          setLoadingLayer(false);
        }
      }
    };

    void loadLayer();
    return () => {
      cancelled = true;
    };
  }, [selectedPath, selectedLayer]);

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: OFFLINE_STYLE,
      center: [8.2275, 46.8182],
      zoom: 7
    });

    map.addControl(new maplibregl.NavigationControl({ showCompass: true }), "top-right");

    map.on("load", () => {
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

      map.addSource(OUTPUT_SOURCE, {
        type: "geojson",
        data: fallbackEmptyCollection()
      });

      map.addLayer({
        id: OUTPUT_FILL,
        type: "fill",
        source: OUTPUT_SOURCE,
        paint: {
          "fill-color": "#0f766e",
          "fill-opacity": 0.22
        },
        filter: ["==", ["geometry-type"], "Polygon"]
      });

      map.addLayer({
        id: OUTPUT_LINE,
        type: "line",
        source: OUTPUT_SOURCE,
        paint: {
          "line-color": "#115e59",
          "line-width": 2
        },
        filter: ["==", ["geometry-type"], "LineString"]
      });

      map.addLayer({
        id: OUTPUT_POINT,
        type: "circle",
        source: OUTPUT_SOURCE,
        paint: {
          "circle-radius": 4,
          "circle-color": "#0f766e",
          "circle-stroke-width": 1,
          "circle-stroke-color": "#ffffff"
        },
        filter: ["==", ["geometry-type"], "Point"]
      });
    });

    map.on("error", (evt) => {
      const sourceId = (evt as { sourceId?: string }).sourceId;
      if (sourceId === BASEMAP_SOURCE) {
        setBasemapWarning("Basemap web non disponibile (rete/proxy). Preview dati comunque disponibile.");
      }
    });

    mapRef.current = map;

    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !map.isStyleLoaded()) return;
    const source = map.getSource(OUTPUT_SOURCE) as maplibregl.GeoJSONSource | undefined;
    if (!source) return;

    source.setData(geojson as never);

    const bounds = stats.bbox;
    if (bounds) {
      map.fitBounds(
        [
          [bounds[0], bounds[1]],
          [bounds[2], bounds[3]]
        ],
        { padding: 32, maxZoom: 15 }
      );
    }
  }, [geojson, stats.bbox]);

  const downloadUrl = selectedFile
    ? `${getApiBase()}/api/files/download?path=${encodeURIComponent(selectedFile.path)}`
    : "";

  return (
    <Box>
      <Stack spacing={2}>
        {!selectedPath && (
          <Alert severity="info" icon={<InfoRounded />}>
            Esegui ETL oppure seleziona una cartella output con dataset supportati per visualizzare i geodati.
          </Alert>
        )}

        {error && <Alert severity="error">{error}</Alert>}

        {basemapWarning && <Alert severity="warning">{basemapWarning}</Alert>}

        <Card sx={{ boxShadow: "var(--panel-shadow)" }}>
          <CardContent>
            <Stack spacing={2} sx={{ height: "100%" }}>
              <div>
                <Typography variant="h6" sx={{ fontWeight: 700, mb: 1 }}>
                  Viewer Geodati Output
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Seleziona un dataset generato (GPKG, DuckDB, GeoJSON), poi un layer per la preview mappa.
                </Typography>
              </div>

              <Stack direction={{ xs: "column", md: "row" }} spacing={2}>
                <TextField
                  select
                  fullWidth
                  size="small"
                  label="Dataset output"
                  value={selectedPath}
                  onChange={(event) => setSelectedPath(event.target.value)}
                  disabled={loadingFiles || files.length === 0}
                >
                  {files.map((item) => (
                    <MenuItem key={item.path} value={item.path}>
                      {item.name} ({item.format.toUpperCase()})
                    </MenuItem>
                  ))}
                </TextField>

                <TextField
                  select
                  fullWidth
                  size="small"
                  label="Layer"
                  value={selectedLayer}
                  onChange={(event) => setSelectedLayer(event.target.value)}
                  disabled={!selectedPath || layers.length === 0}
                >
                  {layers.map((layer) => (
                    <MenuItem key={layer.name} value={layer.name}>
                      {layer.name} ({layer.geometryType})
                    </MenuItem>
                  ))}
                </TextField>
              </Stack>

              <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap" useFlexGap>
                <Button variant="outlined" size="small" onClick={() => void refreshFiles()} disabled={loadingFiles}>
                  Refresh Datasets
                </Button>
                {selectedFile && (
                  <Button variant="outlined" size="small" component="a" href={downloadUrl}>
                    Download File
                  </Button>
                )}
                {loadingFiles || loadingLayer ? <CircularProgress size={18} /> : null}
                {stats.driver ? <Chip size="small" label={`Driver: ${stats.driver}`} /> : null}
                {stats.featuresReturned > 0 ? (
                  <Chip
                    size="small"
                    color={stats.truncated ? "warning" : "default"}
                    label={`Features: ${stats.featuresReturned}/${stats.totalFeatures}`}
                  />
                ) : null}
              </Stack>

              {selectedFile && (
                <Box sx={{ mb: 1 }}>
                  <Typography variant="caption" color="text.secondary" sx={{ display: "block" }}>
                    {selectedFile.path}
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    Size: {formatBytes(selectedFile.size)} • Updated: {new Date(selectedFile.modifiedAt).toLocaleString()}
                  </Typography>
                </Box>
              )}

              <Box
                ref={containerRef}
                sx={{
                  flex: 1,
                  minHeight: 500,
                  border: "1px solid",
                  borderColor: "divider",
                  borderRadius: 1,
                  overflow: "hidden",
                  bgcolor: "background.paper"
                }}
              />
            </Stack>
          </CardContent>
        </Card>
      </Stack>
    </Box>
  );
}
