import type { Feature, Polygon } from "geojson";

export type InterlisToolPaths = {
  ili2c: string;
  ili2validator: string;
  ili2gpkg: string;
  ili2duckdb: string;
  ili2pg: string;
  ili2imd: string;
};

export type PostgresqlSettings = {
  host: string;
  port: number;
  database: string;
  schema: string;
  user: string;
  passwordEnvVar: string;
  sslmode: "disable" | "allow" | "prefer" | "require" | "verify-ca" | "verify-full";
};

// Tab 0: Settings
export type Settings = {
  modelFormat: "ili" | "xmi";
  xmiInputFolder: string;
  modelOutputFolder: string;
  toolPaths: InterlisToolPaths;
  commonParams: Record<string, string>;
  postgresql: PostgresqlSettings;
};

// Tab 1: Baseline
export type Baseline = {
  xmiFile: File | null;
  baselineActive: boolean;
  interlis24Model: string | null;
  dgfcdCatalogs: string | null;
  dgrwiCatalogs: string | null;
  baselineVersion: string | null;
  baselineLabel: string | null;
};

// Tab 2: Mappings
export type Mappings = {
  baselineModel: string | null;
  selectedMappings: {
    swissTLM3D: boolean;
    osm: boolean;
    overture: boolean;
  };
  mappingTables: Record<string, string>;
};

// Tab 3: ETL
export type ETL = {
  sourceData: "swissTLM3D" | "osm" | "overture" | "ofm";
  aoiFeature: Feature<Polygon> | null;
  aoiWkt: string;
  outputFormat: "gpkg" | "postgis" | "duckdb";
  outputPath: string;
  targetTopics: string[];
  overtureParquetDir?: string;
  osmOverpassProvider?: "overpassTurbo" | "swissOverpass" | "custom";
  osmOverpassUrl?: string;
};

// Complete app state
export type AppState = {
  settings: Settings;
  baseline: Baseline;
  mappings: Mappings;
  etl: ETL;
};

// Job status for tracking script execution
export type JobStatus = "idle" | "running" | "done" | "error";

export type JobSummary = {
  id: string;
  status: JobStatus;
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
