import { useState, useEffect } from "react";
import {
  Box,
  Tabs,
  Tab,
  Stack,
  Typography,
  Alert
} from "@mui/material";

import { Api, getApiBase } from "./api";
import Tab0Settings from "./components/Tab0Settings";
import Tab1Baseline from "./components/Tab1Baseline";
import Tab2Mappings from "./components/Tab2Mappings";
import Tab3ETL from "./components/Tab3ETL";
import Tab4Viewer from "./components/Tab4Viewer";
import type { AppState, InterlisToolPaths, Settings } from "./types";

const API_BASE = getApiBase();

const initialToolPaths: InterlisToolPaths = {
  ili2c: "",
  ili2validator: "",
  ili2gpkg: "",
  ili2duckdb: "",
  ili2pg: "",
  ili2imd: ""
};

const initialPostgresqlSettings: Settings["postgresql"] = {
  host: "127.0.0.1",
  port: 5432,
  database: "dgim",
  schema: "swissdgif",
  user: "postgres",
  passwordEnvVar: "DGIM_DB_PASSWORD",
  sslmode: "prefer"
};

// Initial state template
const initialState: AppState = {
  // Tab 0: Settings
  settings: {
    modelFormat: "xmi",
    xmiInputFolder: "",
    modelOutputFolder: "",
    toolPaths: initialToolPaths,
    commonParams: {
      proxyUrl: "http://prp01.adb.intra.admin.ch:8080",
      logLevel: "INFO"
    },
    postgresql: initialPostgresqlSettings
  },
  
  // Tab 1: Baseline
  baseline: {
    xmiFile: null,
    baselineActive: false,
    interlis24Model: null,
    dgfcdCatalogs: null,
    dgrwiCatalogs: null,
    baselineVersion: null,
    baselineLabel: null
  },
  
  // Tab 2: Mappings
  mappings: {
    baselineModel: null,
    selectedMappings: {
      swissTLM3D: false,
      osm: false,
      overture: false
    },
    mappingTables: {}
  },
  
  // Tab 3: ETL
  etl: {
    sourceData: "swissTLM3D",
    aoiFeature: null,
    aoiWkt: "",
    outputFormat: "gpkg",
    outputPath: "",
    targetTopics: [],
    overtureParquetDir: "",
    osmOverpassProvider: "overpassTurbo",
    osmOverpassUrl: ""
  }
};

function mergeSettings(base: Settings, incoming?: Partial<Settings>): Settings {
  const legacyCatalogs = (incoming as { catalogsOutputFolder?: string } | undefined)
    ?.catalogsOutputFolder;
  const normalizedIncoming = {
    ...incoming,
    modelOutputFolder: incoming?.modelOutputFolder || legacyCatalogs || ""
  };

  const mergedPostgresql = {
    ...base.postgresql,
    ...(incoming?.postgresql ?? {})
  };
  const sslmode = mergedPostgresql.sslmode;
  const normalizedSslmode: Settings["postgresql"]["sslmode"] =
    sslmode === "disable" ||
    sslmode === "allow" ||
    sslmode === "prefer" ||
    sslmode === "require" ||
    sslmode === "verify-ca" ||
    sslmode === "verify-full"
      ? sslmode
      : base.postgresql.sslmode;

  return {
    ...base,
    ...normalizedIncoming,
    toolPaths: {
      ...base.toolPaths,
      ...(incoming?.toolPaths ?? {})
    },
    commonParams: {
      ...base.commonParams,
      ...(incoming?.commonParams ?? {})
    },
    postgresql: {
      ...mergedPostgresql,
      port: Number(mergedPostgresql.port) || base.postgresql.port,
      sslmode: normalizedSslmode
    }
  };
}

function mapDbSettingsToUiSettings(current: Settings, dbSettings: unknown): Settings {
  if (!dbSettings || typeof dbSettings !== "object") {
    return current;
  }

  const dbRecord = dbSettings as {
    artifactsDir?: string;
    execution?: {
      proxyUrl?: string;
      logLevel?: string;
      interlisTools?: Partial<InterlisToolPaths>;
    };
    postgresql?: Partial<Settings["postgresql"]>;
  };
  const execution = dbRecord.execution ?? {};
  const interlisTools = execution.interlisTools ?? {};
  const pg = dbRecord.postgresql ?? {};
  const artifactsDir =
    typeof dbRecord.artifactsDir === "string" && dbRecord.artifactsDir.trim()
      ? dbRecord.artifactsDir
      : "";

  return mergeSettings(current, {
    modelOutputFolder: artifactsDir || current.modelOutputFolder,
    commonParams: {
      ...current.commonParams,
      proxyUrl: typeof execution.proxyUrl === "string" && execution.proxyUrl.trim()
        ? execution.proxyUrl
        : (current.commonParams.proxyUrl ?? "http://prp01.adb.intra.admin.ch:8080"),
      logLevel: typeof execution.logLevel === "string"
        ? execution.logLevel
        : (current.commonParams.logLevel ?? "INFO")
    },
    toolPaths: {
      ...current.toolPaths,
      ili2c: typeof interlisTools.ili2c === "string" ? interlisTools.ili2c : current.toolPaths.ili2c,
      ili2validator: typeof interlisTools.ili2validator === "string" ? interlisTools.ili2validator : current.toolPaths.ili2validator,
      ili2gpkg: typeof interlisTools.ili2gpkg === "string" ? interlisTools.ili2gpkg : current.toolPaths.ili2gpkg,
      ili2duckdb: typeof interlisTools.ili2duckdb === "string" ? interlisTools.ili2duckdb : current.toolPaths.ili2duckdb,
      ili2pg: typeof interlisTools.ili2pg === "string" ? interlisTools.ili2pg : current.toolPaths.ili2pg
    },
    postgresql: {
      ...current.postgresql,
      host: typeof pg.host === "string" ? pg.host : current.postgresql.host,
      port: typeof pg.port === "number" ? pg.port : current.postgresql.port,
      database: typeof pg.database === "string" ? pg.database : current.postgresql.database,
      schema: typeof pg.schema === "string" ? pg.schema : current.postgresql.schema,
      user: typeof pg.user === "string" ? pg.user : current.postgresql.user,
      passwordEnvVar: typeof pg.passwordEnvVar === "string" ? pg.passwordEnvVar : current.postgresql.passwordEnvVar,
      sslmode: typeof pg.sslmode === "string" ? pg.sslmode as Settings["postgresql"]["sslmode"] : current.postgresql.sslmode
    }
  });
}

function loadState(): AppState {
  try {
    const saved = localStorage.getItem("ili2dgim_state");
    if (!saved) {
      return initialState;
    }
    const parsed = JSON.parse(saved) as Partial<AppState>;
    const baseline = (parsed.baseline ?? {}) as Partial<AppState["baseline"]>;
    const etl = (parsed.etl ?? {}) as Partial<AppState["etl"]>;
    return {
      ...initialState,
      ...parsed,
      settings: mergeSettings(initialState.settings, parsed.settings),
      etl: {
        ...initialState.etl,
        ...etl,
        targetTopics: Array.isArray(etl.targetTopics)
          ? etl.targetTopics.filter((topic): topic is string => typeof topic === "string")
          : []
      },
      baseline: {
        ...initialState.baseline,
        ...baseline,
        // File objects are not serializable; keep only verified persisted paths.
        xmiFile: null,
        interlis24Model: typeof baseline.interlis24Model === "string" ? baseline.interlis24Model : null,
        dgfcdCatalogs: typeof baseline.dgfcdCatalogs === "string" ? baseline.dgfcdCatalogs : null,
        dgrwiCatalogs: typeof baseline.dgrwiCatalogs === "string" ? baseline.dgrwiCatalogs : null,
        baselineVersion: typeof baseline.baselineVersion === "string" ? baseline.baselineVersion : null,
        baselineLabel: typeof baseline.baselineLabel === "string" ? baseline.baselineLabel : null,
        baselineActive: Boolean(baseline.baselineActive)
      }
    };
  } catch {
    return initialState;
  }
}

function saveState(state: AppState) {
  localStorage.setItem("ili2dgim_state", JSON.stringify(state));
}

function formatBaselineLabel(label: string | null, version: string | null): string {
  const base = label && label.trim() ? label.trim() : "DGIF V3 Baseline";
  if (!version) {
    return base;
  }
  return base.includes(version) ? base : `${base} ${version}`;
}

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`tab-panel-${index}`}
      aria-labelledby={`tab-${index}`}
      {...other}
    >
      {value === index && <Box sx={{ pt: 3 }}>{children}</Box>}
    </div>
  );
}

export default function App() {
  const [currentTab, setCurrentTab] = useState<number>(1);
  const [state, setState] = useState<AppState>(initialState);
  const [error, setError] = useState<string>("");
  const [settingsReady, setSettingsReady] = useState(false);

  // Load state on mount
  useEffect(() => {
    setState(loadState());
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function loadCentralSettings() {
      try {
        const dbSettings = await Api.settings.getDb();
        if (cancelled) {
          return;
        }
        setState((prev) => ({
          ...prev,
          settings: mapDbSettingsToUiSettings(prev.settings, dbSettings)
        }));
      } catch (err) {
        if (!cancelled) {
          const message = err instanceof Error ? err.message : String(err);
          setError(`Unable to load central settings: ${message}`);
        }
      } finally {
        if (!cancelled) {
          setSettingsReady(true);
        }
      }
    }

    void loadCentralSettings();

    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function verifyBaseline() {
      try {
        const result = await Api.baseline.check({
          modelOutputFolder: state.settings.modelOutputFolder || undefined
        });
        if (cancelled) {
          return;
        }

        setState((prev) => ({
          ...prev,
          baseline: {
            ...prev.baseline,
            baselineActive: result.baselineActive,
            interlis24Model: result.iliModel,
            dgfcdCatalogs: result.dgfcdCatalogs,
            dgrwiCatalogs: result.dgrwiCatalogs,
            baselineVersion: result.baselineVersion,
            baselineLabel: result.baselineLabel,
            xmiFile: result.baselineActive ? prev.baseline.xmiFile : null
          }
        }));
      } catch {
        // Keep local persisted state if baseline check endpoint is unavailable.
      }
    }

    void verifyBaseline();

    return () => {
      cancelled = true;
    };
  }, [
    settingsReady,
    state.settings.modelOutputFolder
  ]);

  // Save state whenever it changes
  useEffect(() => {
    saveState(state);
  }, [state]);

  // Determine which tabs are enabled based on completion status
  const tabsEnabled = [
    true, // Tab 0: always enabled
    true, // Tab 1: always enabled
    state.baseline.baselineActive, // Tab 2: enabled if baseline loaded
    state.mappings.selectedMappings.swissTLM3D || 
    state.mappings.selectedMappings.osm || 
    state.mappings.selectedMappings.overture, // Tab 3: enabled if mappings done
    state.etl.outputPath !== "" // Tab 4: enabled if ETL done
  ];

  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    if (tabsEnabled[newValue]) {
      setCurrentTab(newValue);
      setError("");
    }
  };

  const updateState = (updates: Partial<AppState>) => {
    setState((prev) => ({ ...prev, ...updates }));
  };

  return (
    <Box sx={{ p: { xs: 2, md: 4 }, minHeight: "100vh", bgcolor: "background.default" }}>
      {/* Header */}
      <Stack spacing={2} sx={{ mb: 4 }}>
        <Typography variant="h3" sx={{ fontWeight: 700, letterSpacing: -1 }}>
          ili2dgim Runner
        </Typography>
        <Typography variant="body1" sx={{ maxWidth: 950 }}>
          Data processing pipeline with guided step-by-step configuration.
        </Typography>
        <Typography
          variant="body2"
          sx={{
            fontWeight: 600,
            color: state.baseline.baselineActive ? "success.main" : "text.secondary"
          }}
        >
          Active Baseline: {state.baseline.baselineActive
            ? formatBaselineLabel(
              state.baseline.baselineLabel,
              state.baseline.baselineVersion
            )
            : "NO"}
        </Typography>
      </Stack>

      {error && <Alert severity="error">{error}</Alert>}

      {/* Tab Navigation */}
      <Box sx={{ borderBottom: 1, borderColor: "divider", mb: 2 }}>
        <Tabs value={currentTab} onChange={handleTabChange} aria-label="Pipeline tabs">
          <Tab label="Settings" id="tab-0" aria-controls="tab-panel-0" />
          <Tab label="Baseline" id="tab-1" aria-controls="tab-panel-1" />
          <Tab 
            label="Mappings" 
            id="tab-2" 
            aria-controls="tab-panel-2"
            disabled={!tabsEnabled[2]}
          />
          <Tab 
            label="ETL" 
            id="tab-3" 
            aria-controls="tab-panel-3"
            disabled={!tabsEnabled[3]}
          />
          <Tab 
            label="Viewer" 
            id="tab-4" 
            aria-controls="tab-panel-4"
            disabled={!tabsEnabled[4]}
          />
        </Tabs>
      </Box>

      {/* Tab Panels */}
      <TabPanel value={currentTab} index={0}>
        <Tab0Settings
          state={state.settings}
          onUpdate={(settings) => updateState({ settings })}
          backendReady={settingsReady}
        />
      </TabPanel>

      <TabPanel value={currentTab} index={1}>
        <Tab1Baseline 
          state={state.baseline} 
          onUpdate={(baseline) => updateState({ baseline })}
          commonSettings={state.settings}
        />
      </TabPanel>

      <TabPanel value={currentTab} index={2}>
        <Tab2Mappings 
          state={state.mappings}
          onUpdate={(mappings) => updateState({ mappings })}
          baselineActive={state.baseline.baselineActive}
          baselineIliModel={state.baseline.interlis24Model}
          modelOutputFolder={state.settings.modelOutputFolder}
        />
      </TabPanel>

      <TabPanel value={currentTab} index={3}>
        <Tab3ETL 
          state={state.etl}
          onUpdate={(etl) => updateState({ etl })}
          mappings={state.mappings}
          genericOutputFolder={state.settings.modelOutputFolder}
          baselineIliModel={state.baseline.interlis24Model}
        />
      </TabPanel>

      <TabPanel value={currentTab} index={4}>
        <Tab4Viewer 
          outputPath={state.etl.outputPath}
          outputFolder={state.settings.modelOutputFolder}
        />
      </TabPanel>
    </Box>
  );
}
