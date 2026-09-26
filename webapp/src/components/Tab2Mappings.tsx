import { useState } from "react";
import {
  Box,
  Card,
  CardContent,
  Stack,
  Typography,
  Button,
  Alert,
  Checkbox,
  FormControlLabel,
  Divider,
  CircularProgress
} from "@mui/material";
import DownloadRounded from "@mui/icons-material/DownloadRounded";
import { Api } from "../api";
import ExecutionConsole from "./ExecutionConsole";
import type { Mappings } from "../types";

type Tab2MappingsProps = {
  state: Mappings;
  onUpdate: (mappings: Mappings) => void;
  baselineActive: boolean;
  baselineIliModel: string | null;
  modelOutputFolder: string;
};

export default function Tab2Mappings({
  state,
  onUpdate,
  baselineActive,
  baselineIliModel,
  modelOutputFolder
}: Tab2MappingsProps) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>("");
  const [success, setSuccess] = useState<string>("");
  const [progress, setProgress] = useState(0);
  const [logs, setLogs] = useState<string[]>([]);
  const [currentPhase, setCurrentPhase] = useState("idle");

  const appendLog = (message: string) => {
    const timestamp = new Date().toLocaleTimeString();
    setLogs((prev: string[]) => [...prev, `[ui ${timestamp}] ${message}`]);
  };

  const toFileUrl = (path: string | null) => {
    if (!path) return "";
    const normalized = String(path).replace(/\\/g, "/");
    if (normalized.startsWith("file://")) return normalized;
    return `file:///${encodeURI(normalized)}`;
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

  const anySelected = state.selectedMappings.swissTLM3D || 
                      state.selectedMappings.osm || 
                      state.selectedMappings.overture;

  const handleToggleMapping = (source: "swissTLM3D" | "osm" | "overture") => {
    onUpdate({
      ...state,
      selectedMappings: {
        ...state.selectedMappings,
        [source]: !state.selectedMappings[source]
      }
    });
  };

  const handleGenerateMappings = async () => {
    if (!anySelected) {
      setError("Select at least one source data type");
      return;
    }

    setLoading(true);
    setError("");
    setSuccess("");
    setLogs([]);
    setProgress(0);
    setCurrentPhase("preparing request");
    const runtimeTicker = startRuntimePolling();

    try {
      const sourceTypes = Object.entries(state.selectedMappings)
        .filter(([_, selected]) => selected)
        .map(([name]) => name);

      appendLog(`Generating mapping tables for: ${sourceTypes.join(", ")}`);
      appendLog("Submitting generation request to backend.");

      const result = await Api.mappings.generate(sourceTypes, {
        iliModelPath: baselineIliModel || undefined,
        modelOutputFolder: modelOutputFolder || undefined
      });
      setProgress(100);
      setCurrentPhase("completed");
      
      const mergedMappingTables = {
        ...state.mappingTables,
        ...result.mappingTables
      };

      onUpdate({
        ...state,
        mappingTables: mergedMappingTables
      });

      Object.entries(result.mappingTables).forEach(([source, tablePath]) => {
        appendLog(`Generated ${source} mapping: ${tablePath}`);
      });
      appendLog("Mapping generation completed.");

      setSuccess(
        `Mapping tables generated for ${Object.keys(result.mappingTables).length} source(s). ` +
        `Stored mappings: ${Object.keys(mergedMappingTables).length}.`
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setCurrentPhase("failed");
      appendLog(`ERROR: ${message}`);
    } finally {
      window.clearInterval(runtimeTicker);
      setLoading(false);
    }
  };

  const handleDownloadMappings = async () => {
    try {
      const blob = await Api.mappings.download(state.mappingTables);
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "mapping-tables.zip";
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
    } catch (err) {
      setError(String(err));
    }
  };

  return (
    <Box>
      <Stack spacing={2}>
        {!baselineActive && (
          <Alert severity="info">
            Load a baseline XMI file in the previous tab to enable mapping configuration.
          </Alert>
        )}

        {error && <Alert severity="error">{error}</Alert>}
        {success && <Alert severity="success">{success}</Alert>}

        <Card sx={{ boxShadow: "var(--panel-shadow)" }}>
          <CardContent>
            <Stack spacing={2}>
              <div>
                <Typography variant="h6" sx={{ fontWeight: 700, mb: 1 }}>
                  Select Source Data Types
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Choose which source data types require mapping tables to be generated.
                </Typography>
              </div>

              <Divider />

              <Stack spacing={1.5}>
                <FormControlLabel
                  control={
                    <Checkbox
                      checked={state.selectedMappings.swissTLM3D}
                      onChange={() => handleToggleMapping("swissTLM3D")}
                      disabled={!baselineActive}
                    />
                  }
                  label={
                    <Stack spacing={0.3}>
                      <Typography variant="body2" sx={{ fontWeight: 600 }}>
                        swissTLM3D
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        Swiss Geospatial Landscape Model (swissTLM3D)
                      </Typography>
                    </Stack>
                  }
                />

                <FormControlLabel
                  control={
                    <Checkbox
                      checked={state.selectedMappings.osm}
                      onChange={() => handleToggleMapping("osm")}
                      disabled={!baselineActive}
                    />
                  }
                  label={
                    <Stack spacing={0.3}>
                      <Typography variant="body2" sx={{ fontWeight: 600 }}>
                        OpenStreetMap
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        Open Street Map (OSM) data
                      </Typography>
                    </Stack>
                  }
                />

                <FormControlLabel
                  control={
                    <Checkbox
                      checked={state.selectedMappings.overture}
                      onChange={() => handleToggleMapping("overture")}
                      disabled={!baselineActive}
                    />
                  }
                  label={
                    <Stack spacing={0.3}>
                      <Typography variant="body2" sx={{ fontWeight: 600 }}>
                        Overture Maps
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        Overture Maps data
                      </Typography>
                    </Stack>
                  }
                />
              </Stack>

              <Divider />

              <Stack direction="row" spacing={1}>
                <Button
                  variant="contained"
                  onClick={handleGenerateMappings}
                  disabled={!baselineActive || !anySelected || loading}
                >
                  {loading ? <CircularProgress size={20} /> : "Generate Mapping Tables"}
                </Button>
              </Stack>
            </Stack>
          </CardContent>
        </Card>

        {(loading || logs.length > 0) && (
          <ExecutionConsole
            progress={progress}
            logs={logs}
            running={loading}
            currentPhase={currentPhase}
          />
        )}

        {Object.keys(state.mappingTables).length > 0 && (
          <Card sx={{ boxShadow: "var(--panel-shadow)" }}>
            <CardContent>
              <Stack spacing={2}>
                <Typography variant="h6" sx={{ fontWeight: 700 }}>
                  Generated Mapping Tables
                </Typography>

                <Stack spacing={1}>
                  {Object.entries(state.mappingTables).map(([source, table]) => (
                    <Box
                      key={source}
                      sx={{
                        p: 1.5,
                        border: "1px solid",
                        borderColor: "divider",
                        borderRadius: 1,
                        bgcolor: "action.hover"
                      }}
                    >
                      <Stack direction="row" justifyContent="space-between" alignItems="center">
                        <Stack spacing={0.3}>
                          <Typography variant="body2" sx={{ fontWeight: 600 }}>
                            {source}
                          </Typography>
                          <Typography variant="caption" className="mono" color="text.secondary">
                            <a
                              href={toFileUrl(table)}
                              target="_blank"
                              rel="noreferrer"
                              style={{ color: "inherit", textDecoration: "underline" }}
                            >
                              {table}
                            </a>
                          </Typography>
                        </Stack>
                      </Stack>
                    </Box>
                  ))}
                </Stack>

                <Button
                  variant="contained"
                  startIcon={<DownloadRounded />}
                  onClick={handleDownloadMappings}
                  fullWidth
                >
                  Download All Mapping Tables
                </Button>
              </Stack>
            </CardContent>
          </Card>
        )}
      </Stack>
    </Box>
  );
}
