import { useEffect, useMemo, useState, type ChangeEvent } from "react";
import * as turf from "@turf/turf";
import type { Feature, Polygon } from "geojson";
import {
  Box,
  Card,
  CardContent,
  Stack,
  Typography,
  Button,
  Alert,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  TextField,
  CircularProgress,
  Divider,
  Grid,
  Checkbox,
  FormGroup,
  FormControlLabel
} from "@mui/material";
import PlayArrowRounded from "@mui/icons-material/PlayArrowRounded";
import StopRounded from "@mui/icons-material/StopRounded";
import { Api } from "../api";
import AoiMap from "./AoiMap";
import ExecutionConsole from "./ExecutionConsole";
import type { ETL, Mappings } from "../types";

type Tab3ETLProps = {
  state: ETL;
  onUpdate: (etl: ETL) => void;
  mappings: Mappings;
  genericOutputFolder: string;
  baselineIliModel: string | null;
};

type AoiMode = "rectangle" | "polygon";

type OsmOverpassProvider = NonNullable<ETL["osmOverpassProvider"]>;

const OVERPASS_PROVIDER_LABELS: Record<OsmOverpassProvider, string> = {
  overpassTurbo: "Overpass Turbo / FOSSGIS",
  swissOverpass: "Swiss Overpass API",
  custom: "Custom endpoint"
};

const OVERPASS_PROVIDER_ENDPOINTS: Record<Exclude<OsmOverpassProvider, "custom">, string> = {
  overpassTurbo: "https://overpass-api.de/api/interpreter",
  swissOverpass: "https://overpass.osm.ch/api/interpreter"
};

const SWITZERLAND_BBOX: [number, number, number, number] = [5.96, 45.82, 10.5, 47.82];
const LARGE_AOI_KM2 = 4000;

function toOutputFileName(source: string, outputFormat: "gpkg" | "duckdb"): string {
  const slug = source.toLowerCase();
  return outputFormat === "duckdb" ? `DGIF_${slug}.duckdb` : `DGIF_${slug}.gpkg`;
}

function resolveOverpassEndpoint(
  provider: OsmOverpassProvider,
  customUrl: string
): string {
  if (provider === "custom") {
    return customUrl.trim();
  }
  return OVERPASS_PROVIDER_ENDPOINTS[provider];
}

function joinPath(folder: string, fileName: string): string {
  const trimmed = folder.trim().replace(/[\\/]+$/, "");
  if (!trimmed) {
    return "";
  }
  return `${trimmed}/${fileName}`;
}

const SOURCE_DATA_LABELS: Record<ETL["sourceData"], string> = {
  swissTLM3D: "swissTLM3D",
  osm: "OpenStreetMap",
  overture: "Overture Maps",
  ofm: "OFM"
};

const SOURCE_ORDER: ETL["sourceData"][] = ["swissTLM3D", "osm", "overture", "ofm"];

function inferSourceType(mappingKey: string, mappingPath: string): ETL["sourceData"] | null {
  const key = String(mappingKey || "").toLowerCase();
  const path = String(mappingPath || "").toLowerCase().replace(/\\/g, "/");

  if (key.includes("swisstlm3d") || path.includes("swisstlm3d_to_dgif_v3.csv")) {
    return "swissTLM3D";
  }
  if (key.includes("overture") || path.includes("overture_to_dgif_v3.csv")) {
    return "overture";
  }
  if (
    key === "osm" ||
    key.includes("openstreetmap") ||
    path.includes("osm_to_dgif_v3.csv")
  ) {
    return "osm";
  }
  if (key === "ofm" || key.includes("ofmx") || path.includes("ofm_to_dgif")) {
    return "ofm";
  }
  return null;
}

function collectAvailableSourceTypes(mappingTables: Record<string, string>): ETL["sourceData"][] {
  const found = new Set<ETL["sourceData"]>();
  // OFM ETL does not depend on Tab 2 mapping generation.
  found.add("ofm");
  Object.entries(mappingTables || {}).forEach(([key, value]) => {
    const inferred = inferSourceType(key, value);
    if (inferred) {
      found.add(inferred);
    }
  });
  return SOURCE_ORDER.filter((source) => found.has(source));
}

function arraysEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) {
    return false;
  }
  return a.every((value, index) => value === b[index]);
}

function resolveMappingPathForSource(
  mappingTables: Record<string, string>,
  source: ETL["sourceData"]
): string | undefined {
  for (const [key, value] of Object.entries(mappingTables || {})) {
    if (!value) {
      continue;
    }
    const inferred = inferSourceType(key, value);
    if (inferred === source) {
      return value;
    }
  }
  return undefined;
}

function formatWktCoord(value: number): string {
  const rounded = Number(value.toFixed(8));
  return Number.isInteger(rounded) ? String(rounded) : String(rounded);
}

function closeRing(ring: number[][]): number[][] {
  if (ring.length === 0) {
    return ring;
  }
  const first = ring[0];
  const last = ring[ring.length - 1];
  if (first[0] === last[0] && first[1] === last[1]) {
    return ring;
  }
  return [...ring, [first[0], first[1]]];
}

function featureToWkt(feature: Feature<Polygon> | null): string {
  if (!feature) {
    return "";
  }
  const rings = feature.geometry.coordinates
    .map((ring: number[][]) => closeRing(ring))
    .map(
      (ring: number[][]) =>
        `(${ring.map((c: number[]) => `${formatWktCoord(c[0])} ${formatWktCoord(c[1])}`).join(", ")})`
    )
    .join(", ");
  return `POLYGON (${rings})`;
}

function parsePolygonWkt(input: string): Feature<Polygon> {
  const text = input.trim();
  const match = text.match(/^POLYGON\s*\(\((.*)\)\)\s*$/i);
  if (!match) {
    throw new Error("WKT must be a POLYGON geometry.");
  }
  const ringsRaw = match[1].split(/\)\s*,\s*\(/);
  const rings = ringsRaw.map((ringRaw) => {
    const coords = ringRaw
      .split(",")
      .map((pair) => pair.trim())
      .filter(Boolean)
      .map((pair) => {
        const parts = pair.split(/\s+/).filter(Boolean);
        if (parts.length < 2) {
          throw new Error("Invalid WKT coordinate pair.");
        }
        const x = Number(parts[0]);
        const y = Number(parts[1]);
        if (!Number.isFinite(x) || !Number.isFinite(y)) {
          throw new Error("Invalid numeric values in WKT.");
        }
        return [x, y] as [number, number];
      });
    if (coords.length < 4) {
      throw new Error("Polygon ring requires at least 4 coordinates.");
    }
    const closed = closeRing(coords as number[][]) as [number, number][];
    return closed;
  });

  return {
    type: "Feature",
    properties: { mode: "wkt" },
    geometry: {
      type: "Polygon",
      coordinates: rings
    }
  };
}

export default function Tab3ETL({
  state,
  onUpdate,
  mappings,
  genericOutputFolder,
  baselineIliModel
}: Tab3ETLProps) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>("");
  const [success, setSuccess] = useState<string>("");
  const [aoiMode, setAoiMode] = useState<AoiMode>("rectangle");
  const [logs, setLogs] = useState<string[]>([]);
  const [progress, setProgress] = useState(0);
  const [running, setRunning] = useState(false);
  const [currentPhase, setCurrentPhase] = useState("idle");
  const [availableTopics, setAvailableTopics] = useState<string[]>([]);
  const [topicsLoading, setTopicsLoading] = useState(false);
  const [wktError, setWktError] = useState<string>("");

  const swissBounds = useMemo(() => turf.bboxPolygon(SWITZERLAND_BBOX), []);
  const availableSourceTypes = useMemo(
    () => collectAvailableSourceTypes(mappings.mappingTables),
    [mappings.mappingTables]
  );
  const aoiAreaKm2 = state.aoiFeature ? turf.area(state.aoiFeature) / 1_000_000 : 0;
  const isLargeAoi = state.aoiFeature ? aoiAreaKm2 > LARGE_AOI_KM2 : false;
  const isInsideSwitzerland = state.aoiFeature
    ? turf.booleanWithin(state.aoiFeature, swissBounds)
    : null;

  const appendLog = (message: string) => {
    const timestamp = new Date().toLocaleTimeString();
    setLogs((prev: string[]) => [...prev, `[ui ${timestamp}] ${message}`]);
  };

  const startRuntimePolling = () => {
    let offset = 0;
    let polling = false;
    return window.setInterval(async () => {
      if (polling) return;
      polling = true;
      try {
        const status = await Api.runtime.getStatus(offset);
        if (status.newLogs.length > 0) {
          setLogs((prev: string[]) => [...prev, ...status.newLogs]);
        }
        offset = status.nextOffset;
        setProgress(status.progress);
        setCurrentPhase(status.currentPhase || "running");
      } catch {
        // Keep UI responsive if runtime endpoint is unavailable.
      } finally {
        polling = false;
      }
    }, 500);
  };

  const activeSourceOptions = availableSourceTypes;
  const canChooseSource = activeSourceOptions.length > 0;
  const resolvedSource = state.sourceData;
  const sourceAvailable = availableSourceTypes.includes(resolvedSource);
  const needsOvertureInput = resolvedSource === "overture";
  const needsOverpassInput = resolvedSource === "osm";
  const overtureParquetDir = String(state.overtureParquetDir || "").trim();
  const osmOverpassProvider = state.osmOverpassProvider || "overpassTurbo";
  const osmOverpassUrl = String(state.osmOverpassUrl || "").trim();
  const osmOverpassEndpoint = resolveOverpassEndpoint(osmOverpassProvider, osmOverpassUrl);
  const hasOvertureInput =
    !needsOvertureInput || overtureParquetDir.length > 0 || state.aoiFeature !== null;
  const hasOverpassInput =
    !needsOverpassInput ||
    osmOverpassProvider !== "custom" ||
    osmOverpassUrl.length > 0;
  const postgisRequested = state.outputFormat === "postgis";
  const genericFolderReady = genericOutputFolder.trim() !== "";

  const canRun =
    sourceAvailable &&
    state.aoiFeature !== null &&
    state.outputPath !== "" &&
    state.targetTopics.length > 0 &&
    hasOvertureInput &&
    hasOverpassInput &&
    genericFolderReady &&
    !postgisRequested &&
    !loading &&
    !running;
  const selectedMappingPath = resolveMappingPathForSource(
    mappings.mappingTables,
    resolvedSource
  );

  useEffect(() => {
    if (state.aoiFeature && !state.aoiWkt.trim()) {
      onUpdate({ ...state, aoiWkt: featureToWkt(state.aoiFeature) });
    }
  }, [onUpdate, state, state.aoiFeature, state.aoiWkt]);

  const applyWktToAoi = (): Feature<Polygon> | null => {
    const text = state.aoiWkt.trim();
    if (!text) {
      setWktError("");
      if (state.aoiFeature !== null) {
        onUpdate({ ...state, aoiFeature: null, aoiWkt: "" });
      }
      return null;
    }
    try {
      const parsed = parsePolygonWkt(text);
      const normalized = featureToWkt(parsed);
      setWktError("");
      if (normalized !== state.aoiWkt || state.aoiFeature === null) {
        onUpdate({ ...state, aoiFeature: parsed, aoiWkt: normalized });
      }
      return parsed;
    } catch (err) {
      const message = err instanceof Error ? err.message : "Invalid AOI WKT.";
      setWktError(message);
      return state.aoiFeature;
    }
  };

  useEffect(() => {
    let cancelled = false;
    const loadTopics = async () => {
      setTopicsLoading(true);
      try {
        const result = await Api.etl.getTopics({
          iliModelPath: baselineIliModel ?? undefined,
          modelOutputFolder: genericOutputFolder || undefined
        });
        if (cancelled) {
          return;
        }
        const topics = result.topics.filter((topic) => topic !== "DM01AVCH24LV95D");
        setAvailableTopics(topics);
        const selected = state.targetTopics.filter((topic) => topics.includes(topic));
        const nextSelection = selected.length > 0 ? selected : topics;
        if (!arraysEqual(nextSelection, state.targetTopics)) {
          onUpdate({ ...state, targetTopics: nextSelection });
        }
      } catch {
        if (!cancelled) {
          setAvailableTopics([]);
        }
      } finally {
        if (!cancelled) {
          setTopicsLoading(false);
        }
      }
    };
    loadTopics();
    return () => {
      cancelled = true;
    };
  }, [baselineIliModel, genericOutputFolder]);

  useEffect(() => {
    if (!canChooseSource) {
      return;
    }

    if (!activeSourceOptions.includes(state.sourceData)) {
      onUpdate({ ...state, sourceData: activeSourceOptions[0] });
    }
  }, [activeSourceOptions, canChooseSource, onUpdate, state]);

  useEffect(() => {
    if (postgisRequested) {
      return;
    }
    if (!genericOutputFolder.trim()) {
      if (state.outputPath !== "") {
        onUpdate({ ...state, outputPath: "" });
      }
      return;
    }
    const nextPath = joinPath(
      genericOutputFolder,
      toOutputFileName(
        resolvedSource,
        state.outputFormat === "duckdb" ? "duckdb" : "gpkg"
      )
    );
    if (nextPath && nextPath !== state.outputPath) {
      onUpdate({ ...state, outputPath: nextPath });
    }
  }, [
    genericOutputFolder,
    onUpdate,
    postgisRequested,
    resolvedSource,
    state,
    state.outputFormat,
    state.outputPath
  ]);

  const handleStartETL = async () => {
    const effectiveFeature = applyWktToAoi() ?? state.aoiFeature;

    if (effectiveFeature === null) {
      setError("Define an AOI before running ETL.");
      return;
    }
    if (postgisRequested) {
      setError("PostGIS output is not yet implemented in Tab 0 connection settings.");
      return;
    }
    if (!sourceAvailable) {
      setError("Generate mappings first for the selected ETL source in Tab 2.");
      return;
    }
    if (state.targetTopics.length === 0) {
      setError("Select at least one destination TOPIC.");
      return;
    }
    if (needsOvertureInput && !overtureParquetDir && state.aoiFeature === null) {
      setError("Set Overture Input Folder or define an AOI for direct Overture download.");
      return;
    }
    if (needsOverpassInput && osmOverpassProvider === "custom" && !osmOverpassEndpoint) {
      setError("Set a custom Overpass endpoint for OSM ETL.");
      return;
    }

    setLoading(true);
    setError("");
    setSuccess("");
    setLogs([]);
    setRunning(true);
    setProgress(0);
    setCurrentPhase("preparing request");

    appendLog(`Starting ETL for source: ${resolvedSource}`);
    appendLog(`Output target: ${state.outputPath}`);
    if (isLargeAoi) {
      appendLog(`AOI area ${aoiAreaKm2.toFixed(1)} km2 (> ${LARGE_AOI_KM2} km2). Runtime may be long.`);
    }
    const runtimeTicker = startRuntimePolling();

    try {
      const result = await Api.etl.start({
        sourceTypes: [resolvedSource],
        aoiGeoJson: effectiveFeature,
        aoiWkt: state.aoiWkt,
        mappingPath: selectedMappingPath,
        outputFormat: state.outputFormat,
        outputPath: state.outputPath,
        iliModelPath: baselineIliModel || undefined,
        genericOutputFolder: genericOutputFolder || undefined,
        parquetDir: needsOvertureInput ? overtureParquetDir : undefined,
        osmOverpassProvider: needsOverpassInput ? osmOverpassProvider : undefined,
        osmOverpassUrl: needsOverpassInput ? osmOverpassEndpoint : undefined,
        targetTopics: state.targetTopics
      });
      setProgress(100);
      setCurrentPhase("completed");
      appendLog(`ETL output produced: ${result.outputFile}`);
      appendLog("ETL execution completed.");

      setSuccess(`ETL completed successfully. Output: ${result.outputFile}`);
      onUpdate({ ...state, outputPath: result.outputFile });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setCurrentPhase("failed");
      appendLog(`ERROR: ${message}`);
    } finally {
      window.clearInterval(runtimeTicker);
      setLoading(false);
      setRunning(false);
    }
  };

  const handleStopETL = async () => {
    try {
      await Api.etl.stop();
      appendLog("Stop request sent to backend.");
      setRunning(false);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      appendLog(`ERROR: ${message}`);
    }
  };

  return (
    <Box>
      <Stack spacing={2}>
        {error && <Alert severity="error">{error}</Alert>}
        {success && <Alert severity="success">{success}</Alert>}

        <Card sx={{ boxShadow: "var(--panel-shadow)" }}>
          <CardContent>
            <Stack spacing={2}>
              <div>
                <Typography variant="h6" sx={{ fontWeight: 700, mb: 1 }}>
                  Data Source & Output Configuration
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Draw AOI, auto-detect if it is fully in Switzerland, then configure output destination.
                </Typography>
              </div>

              <Divider />

              {!canChooseSource && (
                <Alert severity="info">
                  Generate at least one mapping table in Tab 2 before selecting the source data.
                </Alert>
              )}

              {state.aoiFeature && isInsideSwitzerland === true && (
                <Alert severity="success">
                  AOI is fully in Switzerland.
                </Alert>
              )}

              {state.aoiFeature && isInsideSwitzerland === false && (
                <Alert severity="info">
                  AOI extends outside Switzerland.
                </Alert>
              )}

              {state.aoiFeature && isLargeAoi && (
                <Alert severity="warning">
                  Large AOI: {aoiAreaKm2.toFixed(1)} km2 &gt; {LARGE_AOI_KM2} km2. Processing times may be long.
                </Alert>
              )}

              <Grid container spacing={2}>
                <Grid item xs={12} sm={6}>
                  <FormControl fullWidth size="small">
                    <InputLabel>Source Data</InputLabel>
                    <Select
                      value={state.sourceData}
                      label="Source Data"
                      onChange={(e: { target: { value: string } }) => onUpdate({ ...state, sourceData: e.target.value as ETL["sourceData"] })}
                      disabled={!canChooseSource}
                      renderValue={(value: unknown) => SOURCE_DATA_LABELS[String(value) as ETL["sourceData"]] ?? String(value)}
                    >
                      {activeSourceOptions.map((source: ETL["sourceData"]) => (
                        <MenuItem key={source} value={source}>
                          {SOURCE_DATA_LABELS[source]}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>

                {needsOvertureInput && (
                  <Grid item xs={12} sm={6}>
                    <TextField
                      fullWidth
                      size="small"
                      label="Overture Input Folder (optional)"
                      placeholder="C:/tmp/overture_parquet"
                      value={state.overtureParquetDir || ""}
                      onChange={(e: ChangeEvent<HTMLInputElement>) =>
                        onUpdate({ ...state, overtureParquetDir: e.target.value })
                      }
                    />
                  </Grid>
                )}

                {needsOverpassInput && (
                  <>
                    <Grid item xs={12} sm={6}>
                      <FormControl fullWidth size="small">
                        <InputLabel>Overpass Provider</InputLabel>
                        <Select
                          value={osmOverpassProvider}
                          label="Overpass Provider"
                          onChange={(e: { target: { value: string } }) =>
                            onUpdate({
                              ...state,
                              osmOverpassProvider: e.target.value as OsmOverpassProvider
                            })
                          }
                        >
                          {(Object.keys(OVERPASS_PROVIDER_LABELS) as OsmOverpassProvider[]).map((provider) => (
                            <MenuItem key={provider} value={provider}>
                              {OVERPASS_PROVIDER_LABELS[provider]}
                            </MenuItem>
                          ))}
                        </Select>
                      </FormControl>
                    </Grid>

                    {osmOverpassProvider === "custom" && (
                      <Grid item xs={12} sm={6}>
                        <TextField
                          fullWidth
                          size="small"
                          label="Custom Overpass URL"
                          placeholder="https://example.org/api/interpreter"
                          value={osmOverpassUrl}
                          onChange={(e: ChangeEvent<HTMLInputElement>) =>
                            onUpdate({ ...state, osmOverpassUrl: e.target.value })
                          }
                        />
                      </Grid>
                    )}
                  </>
                )}

                <Grid item xs={12} sm={6}>
                  <FormControl fullWidth size="small">
                    <InputLabel>Output Format</InputLabel>
                    <Select
                      value={state.outputFormat}
                      label="Output Format"
                      onChange={(e: { target: { value: string } }) => onUpdate({ ...state, outputFormat: e.target.value as ETL["outputFormat"] })}
                    >
                      <MenuItem value="gpkg">GeoPackage (.gpkg)</MenuItem>
                      <MenuItem value="postgis">PostGIS Database</MenuItem>
                      <MenuItem value="duckdb">DuckDB Database</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>

                <Grid item xs={12}>
                  {postgisRequested ? (
                    <Alert severity="info">
                      PostGIS output is not yet implemented in Tab 0 connection settings.
                    </Alert>
                  ) : (
                    <Alert severity="info">
                      Output path is auto-derived from Generic Output Folder: {state.outputPath || "(not set)"}
                    </Alert>
                  )}
                </Grid>

                <Grid item xs={12}>
                  <Stack spacing={1}>
                    <Stack direction="row" spacing={1}>
                      <Button
                        size="small"
                        variant="outlined"
                        onClick={() => onUpdate({ ...state, targetTopics: availableTopics })}
                        disabled={topicsLoading || availableTopics.length === 0}
                      >
                        Select All Topics
                      </Button>
                      <Button
                        size="small"
                        variant="outlined"
                        onClick={() => onUpdate({ ...state, targetTopics: [] })}
                        disabled={topicsLoading || state.targetTopics.length === 0}
                      >
                        Clear Topics
                      </Button>
                    </Stack>
                    {topicsLoading ? (
                      <Typography variant="body2" color="text.secondary">
                        Loading destination model topics...
                      </Typography>
                    ) : (
                      <FormGroup row>
                        {availableTopics.map((topic: string) => (
                          <FormControlLabel
                            key={topic}
                            control={
                              <Checkbox
                                checked={state.targetTopics.includes(topic)}
                                onChange={(event: ChangeEvent<HTMLInputElement>) => {
                                  const nextTopics = event.target.checked
                                    ? [...state.targetTopics, topic]
                                    : state.targetTopics.filter((item) => item !== topic);
                                  onUpdate({ ...state, targetTopics: nextTopics });
                                }}
                              />
                            }
                            label={topic}
                          />
                        ))}
                      </FormGroup>
                    )}
                    {state.targetTopics.length === 0 && !topicsLoading && (
                      <Alert severity="warning">
                        No destination TOPIC selected. ETL start is disabled.
                      </Alert>
                    )}
                  </Stack>
                </Grid>

                {!sourceAvailable && (
                  <Grid item xs={12}>
                    <Alert severity="warning">
                      Mapping table for source {resolvedSource} is missing. Generate it in Tab 2 first.
                    </Alert>
                  </Grid>
                )}

                {!genericOutputFolder.trim() && !postgisRequested && (
                  <Grid item xs={12}>
                    <Alert severity="warning">
                      Generic Output Folder in Tab 0 is empty. Set it before running ETL with GPKG/DuckDB.
                    </Alert>
                  </Grid>
                )}

                {needsOvertureInput && !hasOvertureInput && (
                  <Grid item xs={12}>
                    <Alert severity="warning">
                      Overture ETL needs either a local input folder or an AOI (for direct download).
                    </Alert>
                  </Grid>
                )}

                {needsOverpassInput && !hasOverpassInput && (
                  <Grid item xs={12}>
                    <Alert severity="warning">
                      Select an Overpass provider or enter a custom Overpass URL.
                    </Alert>
                  </Grid>
                )}
              </Grid>
            </Stack>
          </CardContent>
        </Card>

        <Card sx={{ boxShadow: "var(--panel-shadow)" }}>
          <CardContent>
            <Stack spacing={2}>
              <div>
                <Typography variant="h6" sx={{ fontWeight: 700, mb: 1 }}>
                  Area of Interest
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Draw a rectangle or polygon in the generic map viewer to define extraction area.
                </Typography>
              </div>

              <Stack direction="row" spacing={1} sx={{ mb: 2 }}>
                <Button
                  variant={aoiMode === "rectangle" ? "contained" : "outlined"}
                  size="small"
                  onClick={() => setAoiMode("rectangle")}
                >
                  Rectangle
                </Button>
                <Button
                  variant={aoiMode === "polygon" ? "contained" : "outlined"}
                  size="small"
                  onClick={() => setAoiMode("polygon")}
                >
                  Polygon
                </Button>
              </Stack>

              <AoiMap
                mode={aoiMode}
                radiusKm={10}
                value={state.aoiFeature}
                onChange={(feature) => {
                  const syncedWkt = featureToWkt(feature);
                  setWktError("");
                  onUpdate({ ...state, aoiFeature: feature, aoiWkt: syncedWkt });
                }}
              />

              <TextField
                fullWidth
                multiline
                minRows={3}
                size="small"
                label="AOI WKT (POLYGON)"
                value={state.aoiWkt}
                onChange={(e: ChangeEvent<HTMLInputElement>) => {
                  setWktError("");
                  onUpdate({ ...state, aoiWkt: e.target.value });
                }}
                onBlur={() => {
                  applyWktToAoi();
                }}
                error={Boolean(wktError)}
                helperText={wktError || "Synchronized with map AOI. Edit WKT and leave field to update map."}
              />
            </Stack>
          </CardContent>
        </Card>

        <Card sx={{ boxShadow: "var(--panel-shadow)" }}>
          <CardContent>
            <Stack spacing={2}>
              <div>
                <Typography variant="h6" sx={{ fontWeight: 700, mb: 1 }}>
                  Execution
                </Typography>
              </div>

              <Stack direction="row" spacing={1}>
                <Button
                  variant="contained"
                  startIcon={<PlayArrowRounded />}
                  onClick={handleStartETL}
                  disabled={!canRun}
                >
                  {loading ? <CircularProgress size={20} /> : "Start ETL"}
                </Button>

                <Button
                  variant="outlined"
                  color="warning"
                  startIcon={<StopRounded />}
                  onClick={handleStopETL}
                  disabled={!running}
                >
                  Stop
                </Button>
              </Stack>

              {(loading || logs.length > 0) && (
                <ExecutionConsole
                  progress={progress}
                  logs={logs}
                  running={loading}
                  currentPhase={currentPhase}
                />
              )}
            </Stack>
          </CardContent>
        </Card>
      </Stack>
    </Box>
  );
}
