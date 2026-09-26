const destinationTypeEl = document.getElementById("destinationType");
const writeModeEl = document.getElementById("writeMode");
const gpkgBindingModeEl = document.getElementById("gpkgBindingMode");
const tablePrefixEl = document.getElementById("tablePrefix");
const artifactsDirEl = document.getElementById("artifactsDir");
const pythonExeEl = document.getElementById("pythonExe");
const tmpDirEl = document.getElementById("tmpDir");
const proxyUrlEl = document.getElementById("proxyUrl");
const logLevelEl = document.getElementById("logLevel");
const ili2cPathEl = document.getElementById("ili2cPath");
const ili2validatorPathEl = document.getElementById("ili2validatorPath");
const ili2gpkgPathEl = document.getElementById("ili2gpkgPath");
const ili2duckdbPathEl = document.getElementById("ili2duckdbPath");
const ili2pgPathEl = document.getElementById("ili2pgPath");
const envVarsEl = document.getElementById("envVars");

const gpkgPathEl = document.getElementById("gpkgPath");
const duckdbPathEl = document.getElementById("duckdbPath");
const pgHostEl = document.getElementById("pgHost");
const pgPortEl = document.getElementById("pgPort");
const pgDatabaseEl = document.getElementById("pgDatabase");
const pgSchemaEl = document.getElementById("pgSchema");
const pgUserEl = document.getElementById("pgUser");
const pgSslmodeEl = document.getElementById("pgSslmode");
const pgPasswordEnvVarEl = document.getElementById("pgPasswordEnvVar");

const geoBoxEl = document.getElementById("geoBox");
const duckBoxEl = document.getElementById("duckBox");
const pgBoxEl = document.getElementById("pgBox");
const statusBoxEl = document.getElementById("statusBox");

const saveBtn = document.getElementById("saveBtn");
const validateBtn = document.getElementById("validateBtn");
const pgTestBtn = document.getElementById("pgTestBtn");

function showStatus(message, isError = false) {
  statusBoxEl.textContent = message;
  statusBoxEl.style.color = isError ? "#b91c1c" : "#065f46";
}

function renderDestinationSections() {
  const dest = destinationTypeEl.value;
  geoBoxEl.style.display = dest === "geopackage" ? "block" : "none";
  duckBoxEl.style.display = dest === "duckdb" ? "block" : "none";
  pgBoxEl.style.display = dest === "postgresql" ? "block" : "none";
}

function parseEnvVarsText(raw) {
  const result = {};
  for (const line of String(raw || "").split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }
    const eqIndex = trimmed.indexOf("=");
    if (eqIndex <= 0) {
      continue;
    }
    const key = trimmed.slice(0, eqIndex).trim();
    const value = trimmed.slice(eqIndex + 1).trim();
    if (key) {
      result[key] = value;
    }
  }
  return result;
}

function envVarsToText(envVars) {
  if (!envVars || typeof envVars !== "object") {
    return "";
  }
  return Object.entries(envVars)
    .map(([k, v]) => `${k}=${v ?? ""}`)
    .join("\n");
}

function collectPayload() {
  return {
    destinationType: destinationTypeEl.value,
    writeMode: writeModeEl.value,
    gpkgBindingMode: gpkgBindingModeEl.value,
    tablePrefix: tablePrefixEl.value,
    artifactsDir: artifactsDirEl.value,
    execution: {
      pythonExe: pythonExeEl.value,
      tmpDir: tmpDirEl.value,
      proxyUrl: proxyUrlEl.value,
      logLevel: logLevelEl.value,
      interlisTools: {
        ili2c: ili2cPathEl.value,
        ili2validator: ili2validatorPathEl.value,
        ili2gpkg: ili2gpkgPathEl.value,
        ili2duckdb: ili2duckdbPathEl.value,
        ili2pg: ili2pgPathEl.value
      },
      envVars: parseEnvVarsText(envVarsEl.value)
    },
    geopackage: {
      path: gpkgPathEl.value
    },
    duckdb: {
      path: duckdbPathEl.value
    },
    postgresql: {
      host: pgHostEl.value,
      port: Number(pgPortEl.value || 5432),
      database: pgDatabaseEl.value,
      schema: pgSchemaEl.value,
      user: pgUserEl.value,
      passwordEnvVar: pgPasswordEnvVarEl.value,
      sslmode: pgSslmodeEl.value
    }
  };
}

function applySettings(s) {
  destinationTypeEl.value = s.destinationType || "geopackage";
  writeModeEl.value = s.writeMode || "replace";
  gpkgBindingModeEl.value = s.gpkgBindingMode || "direct";
  tablePrefixEl.value = s.tablePrefix || "dgim_";
  artifactsDirEl.value = s.artifactsDir || "";

  const execCfg = s.execution || {};
  const interlisTools = execCfg.interlisTools || {};
  pythonExeEl.value = execCfg.pythonExe || "";
  tmpDirEl.value = execCfg.tmpDir || "";
  proxyUrlEl.value = execCfg.proxyUrl || "";
  logLevelEl.value = execCfg.logLevel || "INFO";
  ili2cPathEl.value = interlisTools.ili2c || "";
  ili2validatorPathEl.value = interlisTools.ili2validator || "";
  ili2gpkgPathEl.value = interlisTools.ili2gpkg || "";
  ili2duckdbPathEl.value = interlisTools.ili2duckdb || "";
  ili2pgPathEl.value = interlisTools.ili2pg || "";
  envVarsEl.value = envVarsToText(execCfg.envVars);

  gpkgPathEl.value = (s.geopackage && s.geopackage.path) || "";
  duckdbPathEl.value = (s.duckdb && s.duckdb.path) || "";
  pgHostEl.value = (s.postgresql && s.postgresql.host) || "127.0.0.1";
  pgPortEl.value = (s.postgresql && s.postgresql.port) || 5432;
  pgDatabaseEl.value = (s.postgresql && s.postgresql.database) || "dgim";
  pgSchemaEl.value = (s.postgresql && s.postgresql.schema) || "swissdgif";
  pgUserEl.value = (s.postgresql && s.postgresql.user) || "postgres";
  pgPasswordEnvVarEl.value = (s.postgresql && s.postgresql.passwordEnvVar) || "DGIM_DB_PASSWORD";
  pgSslmodeEl.value = (s.postgresql && s.postgresql.sslmode) || "prefer";

  renderDestinationSections();
}

async function apiGet(path) {
  const res = await fetch(path);
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return res.json();
}

async function apiPost(path, payload) {
  const res = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return res.json();
}

saveBtn.addEventListener("click", async () => {
  try {
    const payload = collectPayload();
    const res = await apiPost("/api/settings/db", payload);
    applySettings(res.settings);
    showStatus("Settings saved.");
  } catch (err) {
    showStatus(String(err), true);
  }
});

validateBtn.addEventListener("click", async () => {
  try {
    const payload = collectPayload();
    const res = await apiPost("/api/settings/db/validate", payload);
    showStatus(res.message || "Validation successful.");
  } catch (err) {
    showStatus(String(err), true);
  }
});

if (pgTestBtn) {
  pgTestBtn.addEventListener("click", async () => {
    try {
      const payload = collectPayload();
      const res = await apiPost("/api/settings/db/postgresql/test", payload);
      showStatus(res.message || "PostGIS connection test successful.");
    } catch (err) {
      showStatus(String(err), true);
    }
  });
}

destinationTypeEl.addEventListener("change", renderDestinationSections);

(async () => {
  try {
    const settings = await apiGet("/api/settings/db");
    applySettings(settings);
    showStatus("Settings loaded.");
  } catch (err) {
    showStatus(String(err), true);
  }
})();
