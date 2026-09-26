import { useState } from "react";
import {
  Box,
  Card,
  CardContent,
  Stack,
  Typography,
  Button,
  Alert,
  CircularProgress,
  Divider
} from "@mui/material";
import CloudUploadRounded from "@mui/icons-material/CloudUploadRounded";
import CheckCircleRounded from "@mui/icons-material/CheckCircleRounded";
import { Api } from "../api";
import ExecutionConsole from "./ExecutionConsole";
import type { Baseline, Settings } from "../types";

type Tab1BaselineProps = {
  state: Baseline;
  onUpdate: (baseline: Baseline) => void;
  commonSettings: Settings;
};

export default function Tab1Baseline({ state, onUpdate, commonSettings }: Tab1BaselineProps) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>("");
  const [progress, setProgress] = useState(0);
  const [logs, setLogs] = useState<string[]>([]);
  const [currentPhase, setCurrentPhase] = useState("idle");

  const appendLog = (message: string) => {
    const timestamp = new Date().toLocaleTimeString();
    setLogs((prev: string[]) => [...prev, `[ui ${timestamp}] ${message}`]);
  };

  const toFileUrl = (path: string | null) => {
    if (!path) {
      return "";
    }

    const normalized = path.replace(/\\/g, "/");
    if (/^file:\/\//i.test(normalized)) {
      return normalized;
    }

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

  const handleFileUpload = async (file: File) => {
    if (!file.name.endsWith(".xmi")) {
      setError("Please upload a valid XMI file (.xmi)");
      return;
    }

    setLoading(true);
    setError("");
    setLogs([]);
    setProgress(0);
    setCurrentPhase("preparing request");
    appendLog(`Baseline upload started: ${file.name}`);
    const runtimeTicker = startRuntimePolling();

    try {
      appendLog("Applying Tab 0 folders to baseline execution payload.");
      const result = await Api.baseline.upload(
        file,
        {
          modelFormat: "xmi",
          xmiInputFolder: commonSettings.xmiInputFolder || undefined,
          modelOutputFolder: commonSettings.modelOutputFolder || undefined
        }
      );
      setProgress(100);
      setCurrentPhase("completed");
      appendLog(`INTERLIS model: ${result.iliModel || "not returned"}`);
      appendLog(`DGFCD catalogs: ${result.dgfcdCatalogs || "not returned"}`);
      appendLog(`DGRWI catalogs: ${result.dgrwiCatalogs || "not returned"}`);
      appendLog("Baseline extraction completed.");

      onUpdate({
        ...state,
        xmiFile: file,
        baselineActive: true,
        interlis24Model: result.iliModel,
        dgfcdCatalogs: result.dgfcdCatalogs,
        dgrwiCatalogs: result.dgrwiCatalogs,
        baselineVersion: result.baselineVersion ?? null,
        baselineLabel: result.baselineLabel ?? null
      });
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

  const handleReset = () => {
    onUpdate({
      xmiFile: null,
      baselineActive: false,
      interlis24Model: null,
      dgfcdCatalogs: null,
      dgrwiCatalogs: null,
      baselineVersion: null,
      baselineLabel: null
    });
    setError("");
    setProgress(0);
    setCurrentPhase("idle");
    setLogs([]);
  };

  return (
    <Box>
      <Stack spacing={2}>
        {error && <Alert severity="error">{error}</Alert>}

        <Card sx={{ boxShadow: "var(--panel-shadow)" }}>
          <CardContent>
            <Stack spacing={2}>
              <div>
                <Typography variant="h6" sx={{ fontWeight: 700, mb: 1 }}>
                  Load XMI Baseline
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Upload an XMI model file to extract the INTERLIS 2.4 model and DGFCD/DGRWI catalogs.
                  This step is optional and establishes the active baseline for mapping configuration.
                </Typography>
              </div>

              {!state.baselineActive ? (
                <Box
                  sx={{
                    border: "2px dashed",
                    borderColor: "primary.main",
                    borderRadius: 2,
                    p: 4,
                    textAlign: "center",
                    cursor: "pointer",
                    transition: "background-color 0.2s",
                    "&:hover": {
                      backgroundColor: "action.hover"
                    }
                  }}
                  component="label"
                >
                  <input
                    type="file"
                    accept=".xmi"
                    onChange={(e) => {
                      const file = e.currentTarget.files?.[0];
                      if (file) handleFileUpload(file);
                    }}
                    style={{ display: "none" }}
                    disabled={loading}
                  />
                  <Stack alignItems="center" spacing={1}>
                    {loading ? (
                      <>
                        <CircularProgress size={40} />
                        <Typography variant="body2">Processing XMI file...</Typography>
                      </>
                    ) : (
                      <>
                        <CloudUploadRounded sx={{ fontSize: 48, color: "primary.main" }} />
                        <Typography variant="body1" sx={{ fontWeight: 500 }}>
                          Click to upload XMI file
                        </Typography>
                        <Typography variant="caption" color="text.secondary">
                          or drag and drop your .xmi file here
                        </Typography>
                      </>
                    )}
                  </Stack>
                </Box>
              ) : (
                <Stack spacing={2}>
                  <Box
                    sx={{
                      bgcolor: "success.lighter",
                      border: "1px solid",
                      borderColor: "success.light",
                      borderRadius: 2,
                      p: 2
                    }}
                  >
                    <Stack direction="row" spacing={1.5} alignItems="flex-start">
                      <CheckCircleRounded sx={{ color: "success.main", mt: 0.3 }} />
                      <Stack spacing={0.5} sx={{ minWidth: 0 }}>
                        <Typography variant="body2" sx={{ fontWeight: 600 }}>
                          Baseline loaded successfully
                        </Typography>
                        <Typography variant="caption" color="text.secondary">
                          {state.xmiFile?.name}
                        </Typography>
                      </Stack>
                    </Stack>
                  </Box>

                  {(state.baselineLabel || state.baselineVersion) && (
                    <Box
                      sx={{
                        border: "1px solid",
                        borderColor: "divider",
                        borderRadius: 2,
                        p: 2,
                        bgcolor: "background.paper"
                      }}
                    >
                      <Stack spacing={0.5}>
                        <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
                          Baseline details
                        </Typography>
                        {state.baselineLabel && (
                          <Typography variant="body2" color="text.secondary">
                            Label: {state.baselineLabel}
                          </Typography>
                        )}
                        {state.baselineVersion && (
                          <Typography variant="body2" color="text.secondary">
                            Version: {state.baselineVersion}
                          </Typography>
                        )}
                      </Stack>
                    </Box>
                  )}

                  <Divider />

                  <div>
                    <Typography variant="subtitle2" sx={{ fontWeight: 600, mb: 1.5 }}>
                      Extracted Artifacts
                    </Typography>
                    <Stack spacing={1}>
                      <Box sx={{ display: "flex", gap: 1, alignItems: "center" }}>
                        <Typography variant="body2" sx={{ minWidth: 180 }}>
                          INTERLIS 2.4 Model:
                        </Typography>
                        {state.interlis24Model ? (
                          <Typography
                            component="a"
                            variant="caption"
                            className="mono"
                            href={toFileUrl(state.interlis24Model)}
                            target="_blank"
                            rel="noreferrer"
                            sx={{ flex: 1, color: "primary.main", textDecoration: "underline" }}
                          >
                            {state.interlis24Model}
                          </Typography>
                        ) : (
                          <Typography variant="caption" className="mono" sx={{ flex: 1, color: "text.secondary" }}>
                            —
                          </Typography>
                        )}
                      </Box>
                      <Box sx={{ display: "flex", gap: 1, alignItems: "center" }}>
                        <Typography variant="body2" sx={{ minWidth: 180 }}>
                          DGFCD Catalogs:
                        </Typography>
                        {state.dgfcdCatalogs ? (
                          <Typography
                            component="a"
                            variant="caption"
                            className="mono"
                            href={toFileUrl(state.dgfcdCatalogs)}
                            target="_blank"
                            rel="noreferrer"
                            sx={{ flex: 1, color: "primary.main", textDecoration: "underline" }}
                          >
                            {state.dgfcdCatalogs}
                          </Typography>
                        ) : (
                          <Typography variant="caption" className="mono" sx={{ flex: 1, color: "text.secondary" }}>
                            —
                          </Typography>
                        )}
                      </Box>
                      <Box sx={{ display: "flex", gap: 1, alignItems: "center" }}>
                        <Typography variant="body2" sx={{ minWidth: 180 }}>
                          DGRWI Catalogs:
                        </Typography>
                        {state.dgrwiCatalogs ? (
                          <Typography
                            component="a"
                            variant="caption"
                            className="mono"
                            href={toFileUrl(state.dgrwiCatalogs)}
                            target="_blank"
                            rel="noreferrer"
                            sx={{ flex: 1, color: "primary.main", textDecoration: "underline" }}
                          >
                            {state.dgrwiCatalogs}
                          </Typography>
                        ) : (
                          <Typography variant="caption" className="mono" sx={{ flex: 1, color: "text.secondary" }}>
                            —
                          </Typography>
                        )}
                      </Box>
                    </Stack>
                  </div>

                  <Divider />

                  <Button
                    variant="outlined"
                    color="error"
                    onClick={handleReset}
                  >
                    Reset & Upload New Baseline
                  </Button>
                </Stack>
              )}

              {(loading || logs.length > 0) && (
                <>
                  <Divider />
                  <ExecutionConsole
                    progress={progress}
                    logs={logs}
                    running={loading}
                    currentPhase={currentPhase}
                  />
                </>
              )}
            </Stack>
          </CardContent>
        </Card>
      </Stack>
    </Box>
  );
}
