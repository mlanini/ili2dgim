const apiBase = "";

const scriptSelect = document.getElementById("scriptSelect");
const pipelineStepsWrap = document.getElementById("pipelineSteps");
const pipelineSelectEl = document.getElementById("pipelineSelect");
const productSelectEl = document.getElementById("productSelect");
const productHintEl = document.getElementById("productHint");
const runProductBtn = document.getElementById("runProductBtn");
const paramsWrap = document.getElementById("params");
const extraArgsEl = document.getElementById("extraArgs");
const aoiFlagEl = document.getElementById("aoiFlag");
const runBtn = document.getElementById("runBtn");
const stopBtn = document.getElementById("stopBtn");
const pauseBtn = document.getElementById("pauseBtn");
const restartBtn = document.getElementById("restartBtn");
const logsEl = document.getElementById("logs");
const downloadsEl = document.getElementById("downloads");
const jobStatusEl = document.getElementById("jobStatus");
const progressLabelEl = document.getElementById("progressLabel");
const progressFillEl = document.getElementById("progressFill");
const progressMetaEl = document.getElementById("progressMeta");
const aoiModeEl = document.getElementById("aoiMode");
const radiusKmEl = document.getElementById("radiusKm");
const mapEl = document.getElementById("map");

let scripts = [];
let selectedSpec = null;
let paramsState = {};
let logsState = [];
let aoiFeature = null;
let pipelineProgress = null;

const PRODUCT_FLOWS = {
  swisstlm3d: {
    buildScript: "build_swisstlm3d_dgif_v3.py",
    etlScript: "etl_swisstlm3d_to_dgif.py",
    description: "Build model/catalogs + swissTLM3D mapping + ETL"
  },
  ofm: {
    buildScript: null,
    etlScript: "etl_ofm_to_dgif.py",
    description: "Build model/catalogs + OpenFlightMaps ETL"
  },
  osm: {
    buildScript: "build_osm_dgif_v3.py",
    etlScript: null,
    description: "Build model/catalogs + OSM mapping table (no dedicated ETL script)"
  },
  overture: {
    buildScript: "build_overture_dgif_v3.py",
    etlScript: "etl_overture_to_dgif.py",
    description: "Build model/catalogs + Overture mapping + ETL"
  }
};

const MANAGEMENT_STEPS = [
  { id: 1, label: "Extract source catalogs", scriptName: "extract_dgfcd_dgrwi_catalogs.py" },
  { id: 2, label: "Generate INTERLIS model", scriptName: "generate_ili_model.py" },
  { id: 3, label: "Generate target GeoPackage schema", scriptName: "generate_gpkg.py" }
];

const AOI_SOURCE = "aoi-source";
const WEB_MERCATOR_LIMIT = 20037508.342789244;
const WMTS_TILE_SIZE = 256;
const WMTS_MIN_ZOOM = 6;
const WMTS_MAX_ZOOM = 18;
const DEFAULT_CENTER_LON_LAT = { lon: 8.2275, lat: 46.8182 };
const GEOADMIN_TILE_URL =
  "https://wmts.geo.admin.ch/1.0.0/ch.swisstopo.pixelkarte-farbe/default/current/3857/{z}/{x}/{y}.jpeg";

let mapState = null;

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function lonLatToMercator(lon, lat) {
  const safeLat = clamp(lat, -85.05112878, 85.05112878);
  const x = (lon * WEB_MERCATOR_LIMIT) / 180;
  const y =
    Math.log(Math.tan(((90 + safeLat) * Math.PI) / 360)) / (Math.PI / 180);
  return {
    x,
    y: (y * WEB_MERCATOR_LIMIT) / 180
  };
}

function mercatorToLonLat(x, y) {
  const lon = (x / WEB_MERCATOR_LIMIT) * 180;
  const lat =
    (180 / Math.PI) *
    (2 * Math.atan(Math.exp((y / WEB_MERCATOR_LIMIT) * Math.PI)) - Math.PI / 2);
  return { lon, lat };
}

function worldSize(zoom) {
  return WMTS_TILE_SIZE * 2 ** zoom;
}

function mercatorToWorldPx(x, y, zoom) {
  const size = worldSize(zoom);
  return {
    x: ((x + WEB_MERCATOR_LIMIT) / (2 * WEB_MERCATOR_LIMIT)) * size,
    y: ((WEB_MERCATOR_LIMIT - y) / (2 * WEB_MERCATOR_LIMIT)) * size
  };
}

function worldPxToMercator(px, py, zoom) {
  const size = worldSize(zoom);
  return {
    x: (px / size) * (2 * WEB_MERCATOR_LIMIT) - WEB_MERCATOR_LIMIT,
    y: WEB_MERCATOR_LIMIT - (py / size) * (2 * WEB_MERCATOR_LIMIT)
  };
}

function mercatorToViewPx(x, y) {
  if (!mapState) return { x: 0, y: 0 };
  const centerWorld = mercatorToWorldPx(mapState.center.x, mapState.center.y, mapState.zoom);
  const pointWorld = mercatorToWorldPx(x, y, mapState.zoom);
  return {
    x: pointWorld.x - centerWorld.x + mapState.width / 2,
    y: pointWorld.y - centerWorld.y + mapState.height / 2
  };
}

function viewPxToMercator(px, py) {
  if (!mapState) return { x: 0, y: 0 };
  const centerWorld = mercatorToWorldPx(mapState.center.x, mapState.center.y, mapState.zoom);
  return worldPxToMercator(
    centerWorld.x + (px - mapState.width / 2),
    centerWorld.y + (py - mapState.height / 2),
    mapState.zoom
  );
}

function polygonFromRect(rect) {
  const x1 = Math.min(rect.x1, rect.x2);
  const x2 = Math.max(rect.x1, rect.x2);
  const y1 = Math.min(rect.y1, rect.y2);
  const y2 = Math.max(rect.y1, rect.y2);
  return [
    [x1, y1],
    [x2, y1],
    [x2, y2],
    [x1, y2],
    [x1, y1]
  ];
}

function polygonFromCircle(center, radiusMeters, steps = 64) {
  const out = [];
  for (let i = 0; i <= steps; i += 1) {
    const t = (i / steps) * Math.PI * 2;
    out.push([
      center.x + Math.cos(t) * radiusMeters,
      center.y + Math.sin(t) * radiusMeters
    ]);
  }
  return out;
}

function mercatorRingToGeoJsonRing(ring) {
  return ring.map(([x, y]) => {
    const ll = mercatorToLonLat(x, y);
    return [Number(ll.lon.toFixed(8)), Number(ll.lat.toFixed(8))];
  });
}

function setAoiFromMercatorPolygon(ring, mode) {
  aoiFeature = {
    type: "Feature",
    properties: {
      source: AOI_SOURCE,
      mode: mode || "custom",
      viewerCrs: "EPSG:3857",
      geometryCrs: "EPSG:4326"
    },
    geometry: {
      type: "Polygon",
      coordinates: [mercatorRingToGeoJsonRing(ring)]
    }
  };
}

function updateAoiInfo() {
  if (!mapState || !mapState.statusEl) return;
  if (!aoiFeature) {
    mapState.statusEl.textContent = "AOI not set";
    return;
  }
  const coords = (aoiFeature.geometry && aoiFeature.geometry.coordinates && aoiFeature.geometry.coordinates[0]) || [];
  mapState.statusEl.textContent = `AOI set (${coords.length} vertices) - ${aoiFeature.properties.mode}`;
}

function updateRadiusUi() {
  if (!radiusKmEl || !aoiModeEl) return;
  radiusKmEl.disabled = aoiModeEl.value !== "circle";
}

function clearAoi() {
  aoiFeature = null;
  if (mapState) {
    mapState.draw.rect = null;
    mapState.draw.tempRect = null;
    mapState.draw.polygon = [];
    mapState.draw.circle = null;
  }
  updateAoiInfo();
  renderMap();
}

function drawSvgPolygon(points, className) {
  if (!mapState || !mapState.svgOverlay || points.length === 0) return;
  const poly = document.createElementNS("http://www.w3.org/2000/svg", "polygon");
  poly.setAttribute("class", className);
  poly.setAttribute(
    "points",
    points
      .map(([mx, my]) => {
        const p = mercatorToViewPx(mx, my);
        return `${p.x},${p.y}`;
      })
      .join(" ")
  );
  mapState.svgOverlay.appendChild(poly);
}

function drawSvgPolyline(points, className) {
  if (!mapState || !mapState.svgOverlay || points.length === 0) return;
  const line = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
  line.setAttribute("class", className);
  line.setAttribute(
    "points",
    points
      .map(([mx, my]) => {
        const p = mercatorToViewPx(mx, my);
        return `${p.x},${p.y}`;
      })
      .join(" ")
  );
  mapState.svgOverlay.appendChild(line);
}

function renderTiles() {
  if (!mapState || !mapState.tileLayer) return;
  const tileLayer = mapState.tileLayer;
  tileLayer.innerHTML = "";
  const zoom = mapState.zoom;
  const size = worldSize(zoom);
  const centerWorld = mercatorToWorldPx(mapState.center.x, mapState.center.y, zoom);

  const left = centerWorld.x - mapState.width / 2;
  const top = centerWorld.y - mapState.height / 2;
  const right = centerWorld.x + mapState.width / 2;
  const bottom = centerWorld.y + mapState.height / 2;

  const minTx = Math.floor(left / WMTS_TILE_SIZE);
  const maxTx = Math.floor(right / WMTS_TILE_SIZE);
  const minTy = Math.floor(top / WMTS_TILE_SIZE);
  const maxTy = Math.floor(bottom / WMTS_TILE_SIZE);
  const tileCount = 2 ** zoom;

  for (let ty = minTy; ty <= maxTy; ty += 1) {
    if (ty < 0 || ty >= tileCount) continue;
    for (let tx = minTx; tx <= maxTx; tx += 1) {
      const wrappedTx = ((tx % tileCount) + tileCount) % tileCount;
      const img = document.createElement("img");
      img.className = "aoi-tile";
      img.alt = "swisstopo tile";
      img.draggable = false;
      img.src = GEOADMIN_TILE_URL
        .replace("{z}", String(zoom))
        .replace("{x}", String(wrappedTx))
        .replace("{y}", String(ty));

      img.style.left = `${tx * WMTS_TILE_SIZE - left}px`;
      img.style.top = `${ty * WMTS_TILE_SIZE - top}px`;
      tileLayer.appendChild(img);
    }
  }

  mapState.scaleEl.textContent = `EPSG:3857 | z${zoom}`;
  mapState.coordEl.textContent = `x ${mapState.center.x.toFixed(0)}  y ${mapState.center.y.toFixed(0)}`;
}

function renderOverlay() {
  if (!mapState || !mapState.svgOverlay) return;
  mapState.svgOverlay.innerHTML = "";

  if (mapState.draw.rect) {
    drawSvgPolygon(polygonFromRect(mapState.draw.rect), "aoi-shape");
  }
  if (mapState.draw.tempRect) {
    drawSvgPolygon(polygonFromRect(mapState.draw.tempRect), "aoi-shape-temp");
  }
  if (mapState.draw.circle) {
    drawSvgPolygon(
      polygonFromCircle(mapState.draw.circle.center, mapState.draw.circle.radiusMeters),
      "aoi-shape"
    );
  }
  if (mapState.draw.polygon.length >= 3) {
    const closed = [...mapState.draw.polygon, mapState.draw.polygon[0]];
    drawSvgPolygon(closed, "aoi-shape");
  } else if (mapState.draw.polygon.length > 0) {
    drawSvgPolyline(mapState.draw.polygon, "aoi-line-temp");
  }
}

function renderMap() {
  renderTiles();
  renderOverlay();
  updateAoiInfo();
}

function setZoom(nextZoom, anchorPx = null) {
  if (!mapState) return;
  const z = clamp(nextZoom, WMTS_MIN_ZOOM, WMTS_MAX_ZOOM);
  if (z === mapState.zoom) return;

  if (anchorPx) {
    const before = viewPxToMercator(anchorPx.x, anchorPx.y);
    mapState.zoom = z;
    const after = viewPxToMercator(anchorPx.x, anchorPx.y);
    mapState.center.x += before.x - after.x;
    mapState.center.y += before.y - after.y;
  } else {
    mapState.zoom = z;
  }

  mapState.center.x = clamp(
    mapState.center.x,
    -WEB_MERCATOR_LIMIT,
    WEB_MERCATOR_LIMIT
  );
  mapState.center.y = clamp(
    mapState.center.y,
    -WEB_MERCATOR_LIMIT,
    WEB_MERCATOR_LIMIT
  );

  renderMap();
}

function createMapUi() {
  mapEl.innerHTML = "";

  const root = document.createElement("div");
  root.className = "aoi-map-root";

  const toolbar = document.createElement("div");
  toolbar.className = "aoi-toolbar";
  toolbar.innerHTML = `
    <button type="button" id="aoiZoomIn" class="secondary">+</button>
    <button type="button" id="aoiZoomOut" class="secondary">-</button>
    <button type="button" id="aoiReset" class="secondary">Reset CH</button>
    <button type="button" id="aoiClear" class="secondary">Clear AOI</button>
    <a href="https://docs.geo.admin.ch/" target="_blank" rel="noreferrer">docs.geo.admin.ch</a>
  `;

  const viewport = document.createElement("div");
  viewport.className = "aoi-viewport";
  const tileLayer = document.createElement("div");
  tileLayer.className = "aoi-tile-layer";
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("class", "aoi-overlay");
  viewport.appendChild(tileLayer);
  viewport.appendChild(svg);

  const footer = document.createElement("div");
  footer.className = "aoi-footer";
  footer.innerHTML = `
    <span id="aoiStatus">AOI not set</span>
    <span id="aoiScale">EPSG:3857</span>
    <span id="aoiCoord"></span>
  `;

  root.appendChild(toolbar);
  root.appendChild(viewport);
  root.appendChild(footer);
  mapEl.appendChild(root);

  return {
    viewport,
    tileLayer,
    svgOverlay: svg,
    statusEl: footer.querySelector("#aoiStatus"),
    scaleEl: footer.querySelector("#aoiScale"),
    coordEl: footer.querySelector("#aoiCoord"),
    zoomInBtn: toolbar.querySelector("#aoiZoomIn"),
    zoomOutBtn: toolbar.querySelector("#aoiZoomOut"),
    resetBtn: toolbar.querySelector("#aoiReset"),
    clearBtn: toolbar.querySelector("#aoiClear")
  };
}

function applyCurrentDrawAsAoi() {
  if (!mapState) return;
  if (mapState.draw.rect) {
    setAoiFromMercatorPolygon(polygonFromRect(mapState.draw.rect), "rectangle");
  } else if (mapState.draw.circle) {
    setAoiFromMercatorPolygon(
      polygonFromCircle(mapState.draw.circle.center, mapState.draw.circle.radiusMeters),
      "circle"
    );
  } else if (mapState.draw.polygon.length >= 3) {
    const ring = [...mapState.draw.polygon, mapState.draw.polygon[0]];
    setAoiFromMercatorPolygon(ring, "polygon");
  }
  updateAoiInfo();
}

function panMap(deltaPxX, deltaPxY) {
  if (!mapState) return;
  const centerWorld = mercatorToWorldPx(mapState.center.x, mapState.center.y, mapState.zoom);
  const next = worldPxToMercator(centerWorld.x - deltaPxX, centerWorld.y - deltaPxY, mapState.zoom);
  mapState.center.x = clamp(next.x, -WEB_MERCATOR_LIMIT, WEB_MERCATOR_LIMIT);
  mapState.center.y = clamp(next.y, -WEB_MERCATOR_LIMIT, WEB_MERCATOR_LIMIT);
  renderMap();
}

function initAoiMap() {
  if (!mapEl || !aoiModeEl || !radiusKmEl) {
    return;
  }

  const center = lonLatToMercator(DEFAULT_CENTER_LON_LAT.lon, DEFAULT_CENTER_LON_LAT.lat);
  const ui = createMapUi();
  const rect = ui.viewport.getBoundingClientRect();

  mapState = {
    center,
    zoom: 8,
    width: Math.max(320, rect.width || 640),
    height: Math.max(260, rect.height || 360),
    tileLayer: ui.tileLayer,
    svgOverlay: ui.svgOverlay,
    statusEl: ui.statusEl,
    scaleEl: ui.scaleEl,
    coordEl: ui.coordEl,
    draw: {
      rect: null,
      tempRect: null,
      polygon: [],
      circle: null
    },
    pan: {
      active: false,
      startX: 0,
      startY: 0
    },
    dragRect: {
      active: false,
      start: null
    }
  };

  ui.zoomInBtn.addEventListener("click", () => setZoom(mapState.zoom + 1));
  ui.zoomOutBtn.addEventListener("click", () => setZoom(mapState.zoom - 1));
  ui.resetBtn.addEventListener("click", () => {
    mapState.center = lonLatToMercator(DEFAULT_CENTER_LON_LAT.lon, DEFAULT_CENTER_LON_LAT.lat);
    mapState.zoom = 8;
    renderMap();
  });
  ui.clearBtn.addEventListener("click", clearAoi);

  ui.viewport.addEventListener("wheel", (event) => {
    event.preventDefault();
    const rect2 = ui.viewport.getBoundingClientRect();
    const anchor = {
      x: event.clientX - rect2.left,
      y: event.clientY - rect2.top
    };
    setZoom(mapState.zoom + (event.deltaY < 0 ? 1 : -1), anchor);
  });

  ui.viewport.addEventListener("contextmenu", (event) => event.preventDefault());

  ui.viewport.addEventListener("mousedown", (event) => {
    const rect2 = ui.viewport.getBoundingClientRect();
    const px = event.clientX - rect2.left;
    const py = event.clientY - rect2.top;

    if (event.button === 2) {
      mapState.pan.active = true;
      mapState.pan.startX = event.clientX;
      mapState.pan.startY = event.clientY;
      return;
    }

    const mode = aoiModeEl.value;
    if (mode === "rectangle") {
      const start = viewPxToMercator(px, py);
      mapState.dragRect.active = true;
      mapState.dragRect.start = start;
      mapState.draw.tempRect = {
        x1: start.x,
        y1: start.y,
        x2: start.x,
        y2: start.y
      };
      renderOverlay();
    }
  });

  ui.viewport.addEventListener("mousemove", (event) => {
    const rect2 = ui.viewport.getBoundingClientRect();
    const px = event.clientX - rect2.left;
    const py = event.clientY - rect2.top;

    if (mapState.pan.active) {
      panMap(event.clientX - mapState.pan.startX, event.clientY - mapState.pan.startY);
      mapState.pan.startX = event.clientX;
      mapState.pan.startY = event.clientY;
      return;
    }

    if (mapState.dragRect.active && mapState.draw.tempRect) {
      const curr = viewPxToMercator(px, py);
      mapState.draw.tempRect.x2 = curr.x;
      mapState.draw.tempRect.y2 = curr.y;
      renderOverlay();
    }
  });

  ui.viewport.addEventListener("mouseup", (event) => {
    if (mapState.pan.active && event.button === 2) {
      mapState.pan.active = false;
      return;
    }

    if (mapState.dragRect.active && mapState.draw.tempRect) {
      mapState.dragRect.active = false;
      mapState.draw.rect = mapState.draw.tempRect;
      mapState.draw.tempRect = null;
      mapState.draw.polygon = [];
      mapState.draw.circle = null;
      applyCurrentDrawAsAoi();
      renderMap();
    }
  });

  ui.viewport.addEventListener("mouseleave", () => {
    mapState.pan.active = false;
    if (mapState.dragRect.active && mapState.draw.tempRect) {
      mapState.dragRect.active = false;
      mapState.draw.tempRect = null;
      renderOverlay();
    }
  });

  ui.viewport.addEventListener("click", (event) => {
    if (event.button !== 0) return;
    const mode = aoiModeEl.value;
    if (mode === "rectangle") return;

    const rect2 = ui.viewport.getBoundingClientRect();
    const px = event.clientX - rect2.left;
    const py = event.clientY - rect2.top;
    const point = viewPxToMercator(px, py);

    if (mode === "circle") {
      const radiusKm = Number.parseFloat(radiusKmEl.value || "0");
      const radiusMeters = Math.max(100, Number.isFinite(radiusKm) ? radiusKm * 1000 : 1000);
      mapState.draw.circle = { center: point, radiusMeters };
      mapState.draw.rect = null;
      mapState.draw.polygon = [];
      applyCurrentDrawAsAoi();
      renderMap();
      return;
    }

    if (mode === "polygon") {
      mapState.draw.rect = null;
      mapState.draw.circle = null;
      mapState.draw.polygon.push([point.x, point.y]);
      renderOverlay();
      if (mapState.draw.polygon.length >= 3) {
        applyCurrentDrawAsAoi();
      }
    }
  });

  ui.viewport.addEventListener("dblclick", (event) => {
    const mode = aoiModeEl.value;
    if (mode !== "polygon") return;
    event.preventDefault();
    if (mapState.draw.polygon.length >= 3) {
      applyCurrentDrawAsAoi();
      renderMap();
    }
  });

  window.addEventListener("resize", () => {
    if (!mapState) return;
    const nextRect = ui.viewport.getBoundingClientRect();
    mapState.width = Math.max(320, nextRect.width || 640);
    mapState.height = Math.max(260, nextRect.height || 360);
    renderMap();
  });

  aoiModeEl.disabled = false;
  updateRadiusUi();
  aoiModeEl.addEventListener("change", () => {
    updateRadiusUi();
    if (aoiModeEl.value !== "polygon") {
      mapState.draw.polygon = [];
      renderOverlay();
    }
  });
  radiusKmEl.addEventListener("input", () => {
    if (aoiModeEl.value === "circle" && mapState.draw.circle) {
      const radiusKm = Number.parseFloat(radiusKmEl.value || "0");
      mapState.draw.circle.radiusMeters = Math.max(
        100,
        Number.isFinite(radiusKm) ? radiusKm * 1000 : 1000
      );
      applyCurrentDrawAsAoi();
      renderMap();
    }
  });

  renderMap();
}

function setStatus(status) {
  jobStatusEl.textContent = String(status || "idle").toUpperCase();
  jobStatusEl.className = `pill ${status || "idle"}`;
  updateJobControls(status || "idle");
}

function updateJobControls(status) {
  const active = status === "running" || status === "paused";
  runBtn.disabled = active || !canRun();
  stopBtn.disabled = !active;
  pauseBtn.disabled = !active;
  restartBtn.disabled = !active;
  pauseBtn.textContent = status === "paused" ? "Resume" : "Pause";
}

function splitArgs(raw) {
  return raw
    .split(" ")
    .map((x) => x.trim())
    .filter(Boolean);
}

function renderLogs() {
  logsEl.textContent = logsState.length ? logsState.join("\n") : "No logs available";
  logsEl.scrollTop = logsEl.scrollHeight;
}

function renderDownloads(artifacts) {
  downloadsEl.innerHTML = "";
  (artifacts || []).forEach((p) => {
    const a = document.createElement("a");
    a.href = `${apiBase}/api/files/download?path=${encodeURIComponent(p)}`;
    a.target = "_blank";
    a.textContent = p.split(/[/\\]/).pop();
    downloadsEl.appendChild(a);
  });
}

function makeInput(field) {
  const wrap = document.createElement("div");
  const label = document.createElement("label");
  label.textContent = `${field.cliFlag}${field.required ? " *" : ""}`;
  wrap.appendChild(label);

  if (field.type === "boolean") {
    const sel = document.createElement("select");
    sel.innerHTML = `<option value="false">OFF</option><option value="true">ON</option>`;
    sel.value = paramsState[field.name] ? "true" : "false";
    sel.addEventListener("change", () => {
      paramsState[field.name] = sel.value === "true";
    });
    wrap.appendChild(sel);
  } else {
    const input = document.createElement("input");
    input.value = paramsState[field.name] || "";
    input.placeholder = field.help || "";
    input.addEventListener("input", () => {
      paramsState[field.name] = input.value;
    });
    wrap.appendChild(input);
  }

  return wrap;
}

function renderParams(spec) {
  paramsWrap.innerHTML = "";
  paramsState = {};
  (spec.fields || []).forEach((field) => {
    if (field.type === "boolean") {
      paramsState[field.name] = field.default === true;
    } else {
      paramsState[field.name] = field.default != null ? String(field.default) : "";
    }
    paramsWrap.appendChild(makeInput(field));
  });
}

async function apiGet(path) {
  const res = await fetch(`${apiBase}${path}`);
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return res.json();
}

async function apiPost(path, payload) {
  const res = await fetch(`${apiBase}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: payload ? JSON.stringify(payload) : undefined
  });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return res.json();
}

function canRun() {
  if (!selectedSpec) return false;
  return (selectedSpec.fields || [])
    .filter((f) => f.required && f.type !== "boolean")
    .every((f) => String(paramsState[f.name] || "").trim().length > 0);
}

function updateRunBtn() {
  const status = (jobStatusEl.textContent || "idle").toLowerCase();
  if (status === "running" || status === "paused") {
    runBtn.disabled = true;
    return;
  }
  runBtn.disabled = !canRun();
}

function setSelectedScript(scriptName) {
  const found = scripts.find((s) => s.scriptName === scriptName);
  if (!found) {
    alert(`Script not available in this UI: ${scriptName}`);
    return false;
  }
  scriptSelect.value = found.scriptName;
  selectedSpec = found;
  renderParams(selectedSpec);
  updateRunBtn();
  renderPipeline();
  return true;
}

async function startCurrentJob(clearLogs = true) {
  if (!selectedSpec) return;
  if (clearLogs) {
    logsState = [];
    renderLogs();
  }
  const payload = {
    scriptName: selectedSpec.scriptName,
    params: paramsState,
    extraArgs: splitArgs(extraArgsEl.value),
    aoiGeoJson: aoiFeature,
    aoiParamFlag: aoiFlagEl.value.trim() || null
  };
  return apiPost("/api/jobs/start", payload);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function appendLogLine(line) {
  logsState.push(line);
  renderLogs();
}

function stripAnsi(value) {
  return String(value || "").replace(/\x1b\[[0-9;]*m/g, "");
}

function formatElapsed(seconds) {
  const total = Math.max(0, Math.floor(seconds || 0));
  const mm = Math.floor(total / 60);
  const ss = total % 60;
  return `${String(mm).padStart(2, "0")}:${String(ss).padStart(2, "0")}`;
}

function extractPhaseProgress(logs) {
  let current = 0;
  let maxSeen = 0;
  let title = "";
  const phaseRe = /\bPhase\s+(\d+)\s*[:\-—]?\s*(.*)$/i;

  for (const rawLine of logs || []) {
    const line = stripAnsi(rawLine);
    const m = line.match(phaseRe);
    if (!m) continue;
    const n = Number.parseInt(m[1], 10);
    if (!Number.isFinite(n) || n <= 0) continue;
    current = n;
    if (n > maxSeen) maxSeen = n;
    if (m[2] && m[2].trim()) {
      title = m[2].trim();
    }
  }

  return { current, maxSeen, title };
}

function setProgressUi(opts) {
  const {
    percent = 0,
    label = "Idle",
    meta = "",
    indeterminate = false,
    done = false,
    error = false
  } = opts || {};

  const bounded = Math.max(0, Math.min(100, Number(percent) || 0));
  progressLabelEl.textContent = label;
  progressMetaEl.textContent = meta;
  progressFillEl.classList.toggle("indeterminate", Boolean(indeterminate));
  progressFillEl.classList.toggle("done", Boolean(done));
  progressFillEl.classList.toggle("error", Boolean(error));
  progressFillEl.style.width = indeterminate ? "35%" : `${bounded.toFixed(1)}%`;

  const track = progressFillEl.parentElement;
  if (track) {
    track.setAttribute("aria-valuenow", String(Math.round(indeterminate ? 0 : bounded)));
  }
}

function updateProgress(summary) {
  const status = summary && summary.status ? summary.status : "idle";
  const startedAt = summary && summary.startedAt ? Number(summary.startedAt) : 0;
  const elapsed = startedAt > 0 ? Date.now() / 1000 - startedAt : 0;
  const phase = extractPhaseProgress(logsState);

  if (status === "idle") {
    pipelineProgress = null;
    setProgressUi({ percent: 0, label: "Idle", meta: "No active job" });
    return;
  }

  if (status === "done") {
    if (pipelineProgress && pipelineProgress.total > 0) {
      const lastStep = pipelineProgress.currentStepName || summary.scriptName || "pipeline";
      setProgressUi({
        percent: 100,
        label: "Pipeline completed",
        meta: `${pipelineProgress.total}/${pipelineProgress.total} steps done. Last: ${lastStep}`,
        done: true
      });
      pipelineProgress = null;
      return;
    }
    setProgressUi({
      percent: 100,
      label: `Completed ${summary.scriptName || "script"}`,
      meta: `Elapsed ${formatElapsed(elapsed)}`,
      done: true
    });
    return;
  }

  if (status === "error") {
    if (pipelineProgress && pipelineProgress.total > 0) {
      const failingStep = Math.min((pipelineProgress.stepIndex || 0) + 1, pipelineProgress.total);
      setProgressUi({
        percent: ((pipelineProgress.stepIndex || 0) / pipelineProgress.total) * 100,
        label: "Pipeline failed",
        meta: `Failed at step ${failingStep}: ${pipelineProgress.currentStepName || summary.scriptName || "unknown"}`,
        error: true
      });
      pipelineProgress = null;
      return;
    }
    setProgressUi({
      percent: 100,
      label: `Failed ${summary.scriptName || "script"}`,
      meta: `Elapsed ${formatElapsed(elapsed)}`,
      error: true
    });
    return;
  }

  if (status === "paused") {
    setProgressUi({
      label: `Paused ${summary.scriptName || "script"}`,
      meta: `Elapsed ${formatElapsed(elapsed)} (process paused)`
    });
    return;
  }

  if (pipelineProgress && pipelineProgress.total > 0) {
    const total = pipelineProgress.total;
    const stepIndex = pipelineProgress.stepIndex || 0;
    const sub = phase.maxSeen > 0 && phase.current > 0
      ? Math.max(0, Math.min(1, phase.current / phase.maxSeen))
      : 0.2;
    const pct = Math.max(0, Math.min(99, ((stepIndex + sub) / total) * 100));
    const stepNo = Math.min(stepIndex + 1, total);
    const phaseText = phase.current > 0
      ? `Phase ${phase.current}${phase.title ? ` - ${phase.title}` : ""}`
      : "running";

    setProgressUi({
      percent: pct,
      label: `Pipeline step ${stepNo}/${total}: ${pipelineProgress.currentStepName || summary.scriptName || "running"}`,
      meta: `${phaseText} | Elapsed ${formatElapsed(elapsed)}`
    });
    return;
  }

  if (phase.maxSeen > 0 && phase.current > 0) {
    const pct = Math.max(5, Math.min(99, (phase.current / phase.maxSeen) * 100));
    setProgressUi({
      percent: pct,
      label: `Running ${summary.scriptName || "script"}`,
      meta: `Phase ${phase.current}/${phase.maxSeen}${phase.title ? ` - ${phase.title}` : ""} | Elapsed ${formatElapsed(elapsed)}`
    });
    return;
  }

  setProgressUi({
    label: `Running ${summary.scriptName || "script"}`,
    meta: `Elapsed ${formatElapsed(elapsed)}`,
    indeterminate: true
  });
}

async function waitForJobCompletion(jobId) {
  while (true) {
    const resp = await apiGet("/api/jobs/current");
    const summary = resp.summary || {};
    setStatus(summary.status);
    renderDownloads(summary.artifacts || []);
    logsState = resp.logs || [];
    renderLogs();
    updateProgress(summary);

    if (summary.id !== jobId) {
      await sleep(1000);
      continue;
    }

    if (summary.status === "done") {
      return;
    }
    if (summary.status === "error") {
      throw new Error(`Pipeline failed at ${summary.scriptName || "unknown step"}`);
    }
    await sleep(1000);
  }
}

function currentProductFlow() {
  const key = productSelectEl.value;
  return PRODUCT_FLOWS[key] || PRODUCT_FLOWS.swisstlm3d;
}

function getFlowSteps() {
  if (pipelineSelectEl.value === "management") {
    return MANAGEMENT_STEPS;
  }

  const flow = currentProductFlow();
  return [
    { id: 1, label: "Build product mapping", scriptName: flow.buildScript },
    { id: 2, label: "ETL product ingest", scriptName: flow.etlScript }
  ];
}

function getTerminalScriptForCurrentPipeline() {
  if (pipelineSelectEl.value === "management") {
    const steps = getFlowSteps();
    return steps.length > 0 ? steps[steps.length - 1].scriptName : null;
  }
  const flow = currentProductFlow();
  return flow.etlScript || flow.buildScript;
}

function updateProductUi() {
  const isManagement = pipelineSelectEl.value === "management";
  productSelectEl.disabled = isManagement;

  if (isManagement) {
    productHintEl.textContent = "Management Pipeline: shared model and schema generation tasks.";
  } else {
    const flow = currentProductFlow();
    productHintEl.textContent = flow.description;
  }

  const terminalScript = getTerminalScriptForCurrentPipeline();
  if (terminalScript) {
    setSelectedScript(terminalScript);
  }
}

async function runImplicitPipeline() {
  const terminalScript = getTerminalScriptForCurrentPipeline();
  if (!terminalScript) {
    throw new Error("No executable script configured for selected product.");
  }

  const flowScripts = getFlowSteps().map((step) => step.scriptName).filter(Boolean);
  const availableScripts = new Set(scripts.map((s) => s.scriptName));
  const missing = flowScripts.filter((name) => !availableScripts.has(name));
  if (missing.length > 0) {
    throw new Error(`Missing scripts for selected product: ${missing.join(", ")}`);
  }

  if (!selectedSpec || selectedSpec.scriptName !== terminalScript) {
    setSelectedScript(terminalScript);
  }

  const terminalSnapshot = {
    params: { ...paramsState },
    extraArgs: extraArgsEl.value,
    aoiFlag: aoiFlagEl.value
  };

  if (!canRun()) {
    throw new Error("Fill required parameters for the selected product ETL first.");
  }

  const running = await apiGet("/api/jobs/current");
  if (running.summary && running.summary.status === "running") {
    throw new Error("Another job is already running.");
  }

  logsState = [];
  pipelineProgress = {
    total: flowScripts.length,
    stepIndex: 0,
    currentStepName: ""
  };
  appendLogLine("============================================================");
  appendLogLine(
    pipelineSelectEl.value === "management"
      ? "[pipeline] Management pipeline selected"
      : `[pipeline] Product selected: ${productSelectEl.value}`
  );

  for (let i = 0; i < flowScripts.length; i += 1) {
    const scriptName = flowScripts[i];
    pipelineProgress.stepIndex = i;
    pipelineProgress.currentStepName = scriptName;
    const isTerminal = scriptName === terminalScript;
    setSelectedScript(scriptName);

    if (isTerminal) {
      for (const field of selectedSpec.fields || []) {
        if (Object.prototype.hasOwnProperty.call(terminalSnapshot.params, field.name)) {
          paramsState[field.name] = terminalSnapshot.params[field.name];
        }
      }
      extraArgsEl.value = terminalSnapshot.extraArgs;
      aoiFlagEl.value = terminalSnapshot.aoiFlag;
      updateRunBtn();
      if (!canRun()) {
        throw new Error(`Missing required parameters for ${scriptName}.`);
      }
    }

    appendLogLine("------------------------------------------------------------");
    appendLogLine(`[pipeline] START ${scriptName}`);
    const startResp = await startCurrentJob(false);
    const jobId = startResp && startResp.summary ? startResp.summary.id : "";
    if (!jobId) {
      throw new Error(`Unable to start step ${scriptName}`);
    }
    await waitForJobCompletion(jobId);
    pipelineProgress.stepIndex = i + 1;
    appendLogLine(`[pipeline] DONE  ${scriptName}`);
  }

  appendLogLine("============================================================");
  appendLogLine("[pipeline] Selected pipeline completed successfully.");
}

function renderPipeline() {
  pipelineStepsWrap.innerHTML = "";
  getFlowSteps().forEach((step) => {
    const scriptName = step.scriptName;
    const available = !!scriptName && scripts.some((s) => s.scriptName === scriptName);
    const active = !!scriptName && selectedSpec && selectedSpec.scriptName === scriptName;

    const row = document.createElement("div");
    row.className = `pipeline-step${active ? " active" : ""}`;

    const head = document.createElement("div");
    head.className = "pipeline-head";
    head.innerHTML = `<strong>Step ${step.id}</strong><span class="small">${step.label}</span>`;
    row.appendChild(head);

    const code = document.createElement("code");
    code.textContent = scriptName || "not required for selected product";
    row.appendChild(code);

    const actions = document.createElement("div");
    actions.className = "pipeline-actions";

    const openBtn = document.createElement("button");
    openBtn.textContent = active ? "Selected" : "Open";
    openBtn.className = active ? "" : "secondary";
    openBtn.disabled = !available;
    openBtn.addEventListener("click", () => {
      if (scriptName) {
        setSelectedScript(scriptName);
      }
    });

    const runStepBtn = document.createElement("button");
    runStepBtn.textContent = "Run";
    runStepBtn.disabled = !available;
    runStepBtn.addEventListener("click", async () => {
      try {
        const ok = scriptName ? setSelectedScript(scriptName) : false;
        if (!ok) return;
        await startCurrentJob();
      } catch (err) {
        alert(String(err));
      }
    });

    actions.appendChild(openBtn);
    actions.appendChild(runStepBtn);
    row.appendChild(actions);

    pipelineStepsWrap.appendChild(row);
  });
}

async function loadScripts() {
  const list = await apiGet("/api/scripts");
  scripts = await Promise.all(list.map((x) => apiGet(`/api/scripts/${x.scriptName}`)));

  scriptSelect.innerHTML = "";
  scripts.forEach((spec) => {
    const opt = document.createElement("option");
    opt.value = spec.scriptName;
    opt.textContent = spec.scriptName;
    scriptSelect.appendChild(opt);
  });

  if (scripts.length > 0) {
    selectedSpec = scripts[0];
    renderParams(selectedSpec);
    updateRunBtn();
    renderPipeline();
  }
}

async function loadCurrentJob() {
  const resp = await apiGet("/api/jobs/current");
  setStatus(resp.summary.status);
  renderDownloads(resp.summary.artifacts || []);
  logsState = resp.logs || [];
  renderLogs();
  updateProgress(resp.summary || {});
}

scriptSelect.addEventListener("change", () => {
  selectedSpec = scripts.find((s) => s.scriptName === scriptSelect.value) || null;
  if (selectedSpec) {
    renderParams(selectedSpec);
  }
  updateRunBtn();
  renderPipeline();
});

productSelectEl.addEventListener("change", () => {
  updateProductUi();
});

pipelineSelectEl.addEventListener("change", () => {
  updateProductUi();
  renderPipeline();
});

runBtn.addEventListener("click", async () => {
  try {
    pipelineProgress = null;
    await startCurrentJob();
  } catch (err) {
    alert(String(err));
  }
});

runProductBtn.addEventListener("click", async () => {
  try {
    runProductBtn.disabled = true;
    await runImplicitPipeline();
  } catch (err) {
    alert(String(err));
  } finally {
    runProductBtn.disabled = false;
  }
});

stopBtn.addEventListener("click", async () => {
  try {
    await apiPost("/api/jobs/stop", {});
    updateProgress({ status: "error", scriptName: "stopped" });
  } catch (err) {
    alert(String(err));
  }
});

pauseBtn.addEventListener("click", async () => {
  try {
    const resp = await apiPost("/api/jobs/pause", {});
    if (resp && resp.summary && resp.summary.status) {
      setStatus(resp.summary.status);
      updateProgress(resp.summary);
    }
  } catch (err) {
    alert(String(err));
  }
});

restartBtn.addEventListener("click", async () => {
  try {
    pipelineProgress = null;
    logsState = [];
    renderLogs();
    const resp = await apiPost("/api/jobs/restart", {});
    if (resp && resp.summary && resp.summary.status) {
      setStatus(resp.summary.status);
      renderDownloads(resp.summary.artifacts || []);
      updateProgress(resp.summary);
    }
  } catch (err) {
    alert(String(err));
  }
});

const evt = new EventSource("/api/jobs/stream");
evt.onmessage = (event) => {
  const payload = JSON.parse(event.data);
  setStatus(payload.summary.status);
  if (payload.newLogs && payload.newLogs.length > 0) {
    logsState = logsState.concat(payload.newLogs);
    renderLogs();
  }
  updateProgress(payload.summary || {});
  renderDownloads(payload.summary.artifacts || []);
};

updateJobControls("idle");

(async () => {
  try {
    initAoiMap();
    await loadScripts();
    updateProductUi();
    await loadCurrentJob();
  } catch (err) {
    alert(String(err));
  }
})();
