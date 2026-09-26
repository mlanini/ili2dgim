import { useEffect, useRef, useState, type ChangeEvent } from "react";
import {
  Box,
  Card,
  CardContent,
  Stack,
  TextField,
  Typography,
  Button,
  Grid,
  InputAdornment
} from "@mui/material";
import FolderOpenRounded from "@mui/icons-material/FolderOpenRounded";
import { Api } from "../api";
import type { InterlisToolPaths, Settings } from "../types";

const POSTGRES_SSL_OPTIONS: Settings["postgresql"]["sslmode"][] = [
  "disable",
  "allow",
  "prefer",
  "require",
  "verify-ca",
  "verify-full"
];

type Tab0SettingsProps = {
  state: Settings;
  onUpdate: (settings: Settings) => void;
  backendReady: boolean;
};

const INTERLIS_TOOL_FIELDS: Array<{
  key: keyof InterlisToolPaths;
  label: string;
  placeholder: string;
}> = [
  {
    key: "ili2c",
    label: "ili2c Path",
    placeholder: "C:/path/to/ili2c.jar"
  },
  {
    key: "ili2validator",
    label: "ili2validator Path",
    placeholder: "C:/path/to/ili2validator.jar"
  },
  {
    key: "ili2gpkg",
    label: "ili2gpkg Path",
    placeholder: "C:/path/to/ili2gpkg.jar"
  },
  {
    key: "ili2duckdb",
    label: "ili2duckdb Path",
    placeholder: "C:/path/to/ili2duckdb.jar"
  },
  {
    key: "ili2pg",
    label: "ili2pg Path",
    placeholder: "C:/path/to/ili2pg.jar"
  }
];

export default function Tab0Settings({ state, onUpdate, backendReady }: Tab0SettingsProps) {
  const [syncState, setSyncState] = useState<"idle" | "saving" | "saved" | "error">("idle");
  const [syncMessage, setSyncMessage] = useState("");
  const [testState, setTestState] = useState<"idle" | "testing" | "success" | "error">("idle");
  const [testMessage, setTestMessage] = useState("");
  const [jsonMessage, setJsonMessage] = useState("");
  const importInputRef = useRef<HTMLInputElement | null>(null);

  const handleChange = (key: keyof Settings, value: unknown) => {
    onUpdate({
      ...state,
      [key]: value
    });
  };

  const handleToolPathChange = (key: keyof InterlisToolPaths, value: string) => {
    handleChange("toolPaths", {
      ...state.toolPaths,
      [key]: value
    });
  };

  const tab0ToolPaths = {
    ili2c: state.toolPaths.ili2c,
    ili2validator: state.toolPaths.ili2validator,
    ili2gpkg: state.toolPaths.ili2gpkg,
    ili2duckdb: state.toolPaths.ili2duckdb,
    ili2pg: state.toolPaths.ili2pg
  };

  const tab0SettingsExport = {
    ...state,
    toolPaths: tab0ToolPaths
  };

  const exportSettingsAsJson = () => {
    const blob = new Blob([JSON.stringify(tab0SettingsExport, null, 2)], { type: "application/json" });
    const url = window.URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = "tab0-settings.json";
    anchor.click();
    window.URL.revokeObjectURL(url);
    setJsonMessage("Tab 0 settings exported as JSON.");
  };

  const importSettingsFromJson = async (file: File) => {
    const parsed = JSON.parse(await file.text()) as unknown;
    if (!parsed || typeof parsed !== "object") {
      throw new Error("The JSON file must contain an object.");
    }

    const candidate = parsed as Record<string, unknown>;
    const imported = candidate.settings && typeof candidate.settings === "object"
      ? candidate.settings as Partial<Settings>
      : candidate.tab0 && typeof candidate.tab0 === "object"
        ? candidate.tab0 as Partial<Settings>
        : candidate as Partial<Settings>;

    onUpdate({
      ...state,
      ...imported,
      toolPaths: {
        ...state.toolPaths,
        ...(imported.toolPaths ?? {}),
        ili2imd: state.toolPaths.ili2imd
      },
      commonParams: {
        ...state.commonParams,
        ...(imported.commonParams ?? {})
      },
      postgresql: {
        ...state.postgresql,
        ...(imported.postgresql ?? {})
      }
    });
    setJsonMessage("Tab 0 settings imported from JSON.");
  };

  const handleFolderBrowse = async (key: "xmiInputFolder" | "modelOutputFolder") => {
    try {
      const selected = await Api.system.selectFolder(state[key] || undefined);
      if (selected.path) {
        handleChange(key, selected.path);
      }
    } catch (error) {
      console.error(`Failed to select folder for ${key}`, error);
    }
  };

  useEffect(() => {
    if (!backendReady) {
      return;
    }

    const timeoutId = window.setTimeout(() => {
      const persistExecutionSettings = async () => {
        try {
          setSyncState("saving");
          setSyncMessage("Saving central execution settings...");
          const currentDbSettings = await Api.settings.getDb();
          const payload = {
            ...currentDbSettings,
            artifactsDir: state.modelOutputFolder || currentDbSettings.artifactsDir || "",
            execution: {
              ...(currentDbSettings.execution ?? {}),
              proxyUrl: state.commonParams.proxyUrl || "",
              logLevel: state.commonParams.logLevel || "INFO",
              interlisTools: {
                ili2c: state.toolPaths.ili2c,
                ili2validator: state.toolPaths.ili2validator,
                ili2gpkg: state.toolPaths.ili2gpkg,
                ili2duckdb: state.toolPaths.ili2duckdb,
                ili2pg: state.toolPaths.ili2pg
              }
            },
            postgresql: {
              ...(currentDbSettings.postgresql ?? {}),
              host: state.postgresql.host,
              port: Number(state.postgresql.port) || 5432,
              database: state.postgresql.database,
              schema: state.postgresql.schema,
              user: state.postgresql.user,
              passwordEnvVar: state.postgresql.passwordEnvVar,
              sslmode: state.postgresql.sslmode
            }
          };
          await Api.settings.saveDb(payload);
          setSyncState("saved");
          setSyncMessage("Central execution settings are up to date.");
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          setSyncState("error");
          setSyncMessage(`Central settings sync failed: ${message}`);
        }
      };

      void persistExecutionSettings();
    }, 450);

    return () => {
      window.clearTimeout(timeoutId);
    };
  }, [backendReady, state.commonParams.proxyUrl, state.commonParams.logLevel, state.toolPaths, state.postgresql]);

  const handlePostgresqlConnectionTest = async () => {
    try {
      setTestState("testing");
      setTestMessage("Testing PostGIS connection...");
      const currentDbSettings = await Api.settings.getDb();
      const payload = {
        ...currentDbSettings,
        execution: {
          ...(currentDbSettings.execution ?? {}),
          proxyUrl: state.commonParams.proxyUrl || "",
          logLevel: state.commonParams.logLevel || "INFO",
          interlisTools: {
            ...(currentDbSettings.execution?.interlisTools ?? {}),
            ili2c: state.toolPaths.ili2c,
            ili2validator: state.toolPaths.ili2validator,
            ili2gpkg: state.toolPaths.ili2gpkg,
            ili2duckdb: state.toolPaths.ili2duckdb,
            ili2pg: state.toolPaths.ili2pg,
            ili2imd: state.toolPaths.ili2imd
          }
        },
        postgresql: {
          ...(currentDbSettings.postgresql ?? {}),
          host: state.postgresql.host,
          port: Number(state.postgresql.port) || 5432,
          database: state.postgresql.database,
          schema: state.postgresql.schema,
          user: state.postgresql.user,
          passwordEnvVar: state.postgresql.passwordEnvVar,
          sslmode: state.postgresql.sslmode
        }
      };
      const result = await Api.settings.testPostgresql(payload);
      setTestState("success");
      setTestMessage(result.message);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      setTestState("error");
      setTestMessage(`PostGIS connection test failed: ${message}`);
    }
  };

  return (
    <Box>
      <Stack spacing={2}>
        <Card sx={{ boxShadow: "var(--panel-shadow)" }}>
          <CardContent>
            <Stack spacing={2}>
              <div>
                <Typography variant="h6" sx={{ fontWeight: 700, mb: 1 }}>
                  Model Settings
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Configure baseline format and shared output folders for baseline and mappings.
                </Typography>
              </div>

              <Grid container spacing={2}>
                <Grid item xs={12} sm={6}>
                  <TextField
                    label="Model Format"
                    value={state.modelFormat}
                    onChange={(e: { target: { value: string } }) => handleChange("modelFormat", e.target.value)}
                    select
                    fullWidth
                    size="small"
                    SelectProps={{
                      native: true
                    }}
                  >
                    <option value="ili">INTERLIS 2.4 (.ili)</option>
                    <option value="xmi">XMI (.xmi)</option>
                  </TextField>
                </Grid>

                <Grid item xs={12} sm={6}>
                  <TextField
                    label="XMI Input Folder"
                    value={state.xmiInputFolder}
                    onChange={(e: { target: { value: string } }) => handleChange("xmiInputFolder", e.target.value)}
                    placeholder="/path/to/xmi"
                    fullWidth
                    size="small"
                    InputProps={{
                      endAdornment: (
                        <InputAdornment position="end">
                          <Button size="small" startIcon={<FolderOpenRounded />} onClick={() => void handleFolderBrowse("xmiInputFolder")}>
                            Browse
                          </Button>
                        </InputAdornment>
                      )
                    }}
                  />
                </Grid>

                <Grid item xs={12} sm={6}>
                  <TextField
                    label="Generic Output Folder"
                    value={state.modelOutputFolder}
                    onChange={(e: { target: { value: string } }) => handleChange("modelOutputFolder", e.target.value)}
                    placeholder="/path/to/output"
                    helperText="Single shared folder for model, catalogs, Tab 2 mappings and Tab 3 ETL outputs."
                    fullWidth
                    size="small"
                    InputProps={{
                      endAdornment: (
                        <InputAdornment position="end">
                          <Button size="small" startIcon={<FolderOpenRounded />} onClick={() => void handleFolderBrowse("modelOutputFolder")}>
                            Browse
                          </Button>
                        </InputAdornment>
                      )
                    }}
                  />
                </Grid>
              </Grid>
            </Stack>
          </CardContent>
        </Card>

        <Card sx={{ boxShadow: "var(--panel-shadow)" }}>
          <CardContent>
            <Stack spacing={2}>
              <div>
                <Typography variant="h6" sx={{ fontWeight: 700, mb: 1 }}>
                  PostGIS Connection
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Connection parameters stored centrally and reused by ETL output mode.
                </Typography>
              </div>

              <Grid container spacing={2}>
                <Grid item xs={12} sm={6}>
                  <TextField
                    label="Host"
                    value={state.postgresql.host}
                    onChange={(e: { target: { value: string } }) => handleChange("postgresql", {
                      ...state.postgresql,
                      host: e.target.value
                    })}
                    placeholder="127.0.0.1"
                    fullWidth
                    size="small"
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <TextField
                    label="Port"
                    value={state.postgresql.port}
                    onChange={(e: { target: { value: string } }) => handleChange("postgresql", {
                      ...state.postgresql,
                      port: Number(e.target.value) || 5432
                    })}
                    placeholder="5432"
                    type="number"
                    fullWidth
                    size="small"
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <TextField
                    label="Database"
                    value={state.postgresql.database}
                    onChange={(e: { target: { value: string } }) => handleChange("postgresql", {
                      ...state.postgresql,
                      database: e.target.value
                    })}
                    placeholder="dgim"
                    fullWidth
                    size="small"
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <TextField
                    label="Schema"
                    value={state.postgresql.schema}
                    onChange={(e: { target: { value: string } }) => handleChange("postgresql", {
                      ...state.postgresql,
                      schema: e.target.value
                    })}
                    placeholder="swissdgif"
                    fullWidth
                    size="small"
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <TextField
                    label="User"
                    value={state.postgresql.user}
                    onChange={(e: { target: { value: string } }) => handleChange("postgresql", {
                      ...state.postgresql,
                      user: e.target.value
                    })}
                    placeholder="postgres"
                    fullWidth
                    size="small"
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <TextField
                    label="Password Env Var"
                    value={state.postgresql.passwordEnvVar}
                    onChange={(e: { target: { value: string } }) => handleChange("postgresql", {
                      ...state.postgresql,
                      passwordEnvVar: e.target.value
                    })}
                    placeholder="DGIM_DB_PASSWORD"
                    fullWidth
                    size="small"
                    helperText="Secret is read from this environment variable at runtime."
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <TextField
                    label="SSL Mode"
                    value={state.postgresql.sslmode}
                    onChange={(e: { target: { value: string } }) => handleChange("postgresql", {
                      ...state.postgresql,
                      sslmode: e.target.value as Settings["postgresql"]["sslmode"]
                    })}
                    select
                    fullWidth
                    size="small"
                    SelectProps={{ native: true }}
                  >
                    {POSTGRES_SSL_OPTIONS.map((mode) => (
                      <option key={mode} value={mode}>{mode}</option>
                    ))}
                  </TextField>
                </Grid>
              </Grid>

              <Stack spacing={1}>
                <Button
                  variant="outlined"
                  onClick={() => void handlePostgresqlConnectionTest()}
                  disabled={!backendReady || testState === "testing"}
                  sx={{ alignSelf: "flex-start" }}
                >
                  {testState === "testing" ? "Testing..." : "Test PostGIS Connection"}
                </Button>
                <Typography
                  variant="caption"
                  color={testState === "error" ? "error.main" : testState === "success" ? "success.main" : "text.secondary"}
                >
                  {testMessage || "Checks the current PostGIS parameters directly; Proxy URL is not required for this test."}
                </Typography>
              </Stack>
            </Stack>
          </CardContent>
        </Card>

        <Card sx={{ boxShadow: "var(--panel-shadow)" }}>
          <CardContent>
            <Stack spacing={2}>
              <div>
                <Typography variant="h6" sx={{ fontWeight: 700, mb: 1 }}>
                  INTERLIS Tool Origins
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Central paths injected into scripts as execution variables.
                </Typography>
              </div>

              <Grid container spacing={2}>
                {INTERLIS_TOOL_FIELDS.map((field) => (
                  <Grid key={field.key} item xs={12} sm={6}>
                    <TextField
                      label={field.label}
                      value={state.toolPaths[field.key]}
                      onChange={(e: { target: { value: string } }) => handleToolPathChange(field.key, e.target.value)}
                      placeholder={field.placeholder}
                      fullWidth
                      size="small"
                    />
                  </Grid>
                ))}
              </Grid>
            </Stack>
          </CardContent>
        </Card>

        <Card sx={{ boxShadow: "var(--panel-shadow)" }}>
          <CardContent>
            <Stack spacing={2}>
              <div>
                <Typography variant="h6" sx={{ fontWeight: 700, mb: 1 }}>
                  Common Parameters
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Global settings applied to all scripts.
                </Typography>
              </div>

              <TextField
                label="Proxy URL (optional)"
                value={state.commonParams.proxyUrl || ""}
                onChange={(e: { target: { value: string } }) => handleChange("commonParams", {
                  ...state.commonParams,
                  proxyUrl: e.target.value
                })}
                placeholder="http://proxy.example.com:8080"
                fullWidth
                size="small"
              />

              <TextField
                label="Log Level"
                value={state.commonParams.logLevel || "INFO"}
                onChange={(e: { target: { value: string } }) => handleChange("commonParams", {
                  ...state.commonParams,
                  logLevel: e.target.value
                })}
                select
                fullWidth
                size="small"
                SelectProps={{
                  native: true
                }}
              >
                <option value="DEBUG">DEBUG</option>
                <option value="INFO">INFO</option>
                <option value="WARNING">WARNING</option>
                <option value="ERROR">ERROR</option>
              </TextField>

              <Typography
                variant="caption"
                color={syncState === "error" ? "error.main" : "text.secondary"}
              >
                {syncMessage || "Execution settings are synced to backend /api/settings/db."}
              </Typography>
            </Stack>
          </CardContent>
        </Card>

        <Card sx={{ boxShadow: "var(--panel-shadow)" }}>
          <CardContent>
            <Stack spacing={2}>
              <div>
                <Typography variant="h6" sx={{ fontWeight: 700, mb: 1 }}>
                  JSON Transfer
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Export or re-import Tab 0 settings as JSON.
                </Typography>
              </div>

              <Stack direction={{ xs: "column", sm: "row" }} spacing={1}>
                <Button variant="outlined" onClick={exportSettingsAsJson}>
                  Export JSON
                </Button>
                <Button variant="contained" onClick={() => importInputRef.current?.click()}>
                  Import JSON
                </Button>
              </Stack>

              <input
                ref={importInputRef}
                type="file"
                accept="application/json,.json"
                hidden
                onChange={async (event: ChangeEvent<HTMLInputElement>) => {
                  const file = event.currentTarget.files?.[0];
                  event.currentTarget.value = "";
                  if (!file) {
                    return;
                  }

                  try {
                    await importSettingsFromJson(file);
                  } catch (error) {
                    const message = error instanceof Error ? error.message : String(error);
                    setJsonMessage(`Import failed: ${message}`);
                  }
                }}
              />

              <Typography variant="caption" color="text.secondary">
                {jsonMessage || "The file is applied immediately and synchronized when backend sync is available."}
              </Typography>
            </Stack>
          </CardContent>
        </Card>
      </Stack>
    </Box>
  );
}
