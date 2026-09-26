import type { InterlisToolPaths } from "./types";

type ViewerFeatureCollection = {
  type: "FeatureCollection";
  features: Array<{
    type: "Feature";
    geometry: unknown;
    properties: Record<string, unknown>;
  }>;
};

export type FieldSpec = {
  name: string;
  cliFlag: string;
  type: "string" | "number" | "boolean";
  required: boolean;
  default?: string | number | boolean | null;
  help: string;
};

export type ScriptSummary = {
  scriptName: string;
  scriptPath: string;
};

export type ScriptSpec = {
  scriptName: string;
  scriptPath: string;
  fields: FieldSpec[];
};

export type JobSummary = {
  id: string;
  status: "idle" | "running" | "done" | "error";
  scriptName: string | null;
  command: string[];
  startedAt: number | null;
  finishedAt: number | null;
  returnCode: number | null;
  artifacts: string[];
};

export type JobEvent = {
  summary: JobSummary;
  newLogs: string[];
};

export type FolderSelection = {
  path: string | null;
};

export type BaselineUploadOptions = {
  modelFormat?: "ili" | "xmi";
  xmiInputFolder?: string;
  modelOutputFolder?: string;
};

export type BaselineUploadResult = {
  iliModel: string | null;
  dgfcdCatalogs: string | null;
  dgrwiCatalogs: string | null;
  baselineVersion?: string | null;
  baselineLabel?: string | null;
};

export type BaselineCheckResult = {
  baselineActive: boolean;
  iliModel: string | null;
  dgfcdCatalogs: string | null;
  dgrwiCatalogs: string | null;
  baselineVersion: string | null;
  baselineLabel: string;
  modelOutputFolder: string;
  missing: string[];
};

export type RuntimeStatus = {
  label: string;
  currentPhase: string;
  running: boolean;
  totalSteps: number;
  completedSteps: number;
  progress: number;
  newLogs: string[];
  nextOffset: number;
};

export type EtlTopicsResult = {
  iliModel: string;
  topics: string[];
};

export type MappingsGenerateOptions = {
  iliModelPath?: string;
  modelOutputFolder?: string;
};

export type DbSettings = {
  destinationType?: string;
  writeMode?: string;
  gpkgBindingMode?: string;
  tablePrefix?: string;
  artifactsDir?: string;
  execution?: {
    pythonExe?: string;
    tmpDir?: string;
    proxyUrl?: string;
    logLevel?: string;
    interlisTools?: Partial<InterlisToolPaths>;
    envVars?: Record<string, string>;
  };
  geopackage?: {
    path?: string;
  };
  duckdb?: {
    path?: string;
  };
  postgresql?: {
    host?: string;
    port?: number;
    database?: string;
    schema?: string;
    user?: string;
    passwordEnvVar?: string;
    sslmode?: string;
  };
};

export type DbConnectionTestResult = {
  ok: boolean;
  message: string;
};

export type ViewerFileEntry = {
  name: string;
  path: string;
  size: number;
  modifiedAt: string;
  format: string;
};

export type ViewerFilesResult = {
  folder: string;
  files: ViewerFileEntry[];
};

export type ViewerLayerEntry = {
  name: string;
  geometryType: string;
  featureCount: number;
  extent: [number, number, number, number] | null;
  srs: string;
};

export type ViewerLayersResult = {
  path: string;
  driver: string;
  layers: ViewerLayerEntry[];
};

export type ViewerLayerGeoJsonResult = {
  path: string;
  layerName: string;
  driver: string;
  totalFeatures: number;
  featuresReturned: number;
  truncated: boolean;
  bbox: [number, number, number, number] | null;
  geojson: ViewerFeatureCollection;
};

const API_BASE = import.meta.env.VITE_API_BASE ?? "http://127.0.0.1:8000";

async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}`);
  }
  return res.json() as Promise<T>;
}

async function apiPost<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: body ? JSON.stringify(body) : undefined
  });
  if (!res.ok) {
    const message = await res.text();
    throw new Error(message || `${res.status} ${res.statusText}`);
  }
  return res.json() as Promise<T>;
}

export function getApiBase(): string {
  return API_BASE;
}

export const Api = {
  // Legacy script API
  listScripts: () => apiGet<ScriptSummary[]>("/api/scripts"),
  getScript: (scriptName: string) => apiGet<ScriptSpec>(`/api/scripts/${scriptName}`),
  getCurrentJob: () => apiGet<{ summary: JobSummary; logs: string[] }>("/api/jobs/current"),
  startJob: (payload: unknown) => apiPost<{ summary: JobSummary }>("/api/jobs/start", payload),
  stopJob: () => apiPost<{ stopped: boolean; message?: string }>("/api/jobs/stop"),

  // Baseline API
  baseline: {
    upload: async (file: File, options: BaselineUploadOptions): Promise<BaselineUploadResult> => {
      const formData = new FormData();
      formData.append("file", file);
      if (options.modelFormat) {
        formData.append("modelFormat", options.modelFormat);
      }
      if (options.xmiInputFolder) {
        formData.append("xmiInputFolder", options.xmiInputFolder);
      }
      if (options.modelOutputFolder) {
        formData.append("modelOutputFolder", options.modelOutputFolder);
      }
      const res = await fetch(`${API_BASE}/api/baseline/upload`, {
        method: "POST",
        body: formData
      });
      if (!res.ok) throw new Error(await res.text());
      return res.json() as Promise<BaselineUploadResult>;
    },
    check: (payload: {
      modelOutputFolder?: string;
      outputFolder?: string;
    }) => apiPost<BaselineCheckResult>("/api/baseline/check", payload)
  },

  // Mappings API
  mappings: {
    generate: (sourceTypes: string[], options?: MappingsGenerateOptions) =>
      apiPost<{ mappingTables: Record<string, string> }>("/api/mappings/generate", {
        sourceTypes,
        iliModelPath: options?.iliModelPath,
        modelOutputFolder: options?.modelOutputFolder
      }),
    download: (tables: Record<string, string>) =>
      fetch(`${API_BASE}/api/mappings/download`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tables })
      }).then((res) => {
        if (!res.ok) throw new Error("Download failed");
        return res.blob();
      })
  },

  // ETL API
  etl: {
    start: (payload: unknown) =>
      apiPost<{ outputFile: string }>("/api/etl/start", payload),
    stop: () =>
      apiPost<{ stopped: boolean }>("/api/etl/stop"),
    getTopics: (payload: { iliModelPath?: string; modelOutputFolder?: string }) =>
      apiPost<EtlTopicsResult>("/api/etl/topics", payload)
  },

  system: {
    selectFolder: (initialPath?: string) =>
      apiPost<FolderSelection>("/api/system/select-folder", { initialPath })
  },

  runtime: {
    getStatus: (offset: number) =>
      apiGet<RuntimeStatus>(`/api/runtime/status?offset=${offset}`)
  },

  viewer: {
    listFiles: (outputFolder?: string) => {
      const qs = new URLSearchParams();
      if (outputFolder && outputFolder.trim()) {
        qs.set("outputFolder", outputFolder.trim());
      }
      const suffix = qs.toString();
      return apiGet<ViewerFilesResult>(`/api/viewer/files${suffix ? `?${suffix}` : ""}`);
    },
    listLayers: (path: string) => {
      const qs = new URLSearchParams({ path });
      return apiGet<ViewerLayersResult>(`/api/viewer/layers?${qs.toString()}`);
    },
    getLayerGeoJson: (path: string, layer?: string, limit = 2000) => {
      const qs = new URLSearchParams({ path, limit: String(limit) });
      if (layer && layer.trim()) {
        qs.set("layer", layer.trim());
      }
      return apiGet<ViewerLayerGeoJsonResult>(`/api/viewer/layer-geojson?${qs.toString()}`);
    }
  },

  settings: {
    getDb: () => apiGet<DbSettings>("/api/settings/db"),
    saveDb: (payload: DbSettings) =>
      apiPost<{ saved: boolean; settings: DbSettings }>("/api/settings/db", payload),
    testPostgresql: (payload: DbSettings) =>
      apiPost<DbConnectionTestResult>("/api/settings/db/postgresql/test", payload)
  }
};
