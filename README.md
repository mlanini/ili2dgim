# ili2dgim

**An implementation of the Defence Geospatial Information Model (DGIM) 3.0 using INTERLIS 2.4**

This aims at setting up an end-to-end, fully automated pipeline that takes the DGIM 3.0 UML model — maintained by the [Defence Geospatial Information Working Group (DGIWG)](https://dgiwg.org/) — and produces a standards-compliant Swiss geospatial data stack:

1. **INTERLIS 2.4 model** (`models/DGIF_V3.ili`) — 673 classes, 21 topics, 0 compiler errors
2. **GeoPackage schema** (`output/DGIF_V3.gpkg`) — OGC/DGIWG-conformant empty schema in WGS84
3. **INTERLIS XML catalogues** (`models/DGFCD_*.xml`, `models/DGRWI_*.xml`) — DGFCD + DGRWI concept dictionaries
4. **Mapping tables** — OSM ↔ DGIF V3 (1,657 rows) and swissTLM3D ↔ DGIF V3 (215 rows)
5. **ETL pipelines** — populate the DGIF GeoPackage with real-world data:
   - [swissTLM3D](https://www.swisstopo.admin.ch/en/landscape-model-swisstlm3d) data from swisstopo (LV95 → WGS84 reprojection)
   - [OpenFlightMaps](https://www.openflightmaps.org/) aeronautical facility data (OFMX/AIXM 4.5)
6. **Populated GeoPackages** — `DGIF_swissTLM3D.gpkg` (~29 MB, 5,351 features) and `DGIF_OFM.gpkg` (schema ready for feature insertion)

All scripts are pure Python. The only external runtime dependency is Java (for
the INTERLIS toolchain: ili2c, ili2gpkg, ilivalidator).

## Documentation (MkDocs Material)

An en_US documentation site is available under `docs/` and configured via
`mkdocs.yml`.

Main pages include:

- `docs/principles.md`
- `docs/how-to.md`
- `docs/webapp.md`
- `docs/backend-api.md`
- `docs/scripts.md`

Install docs dependencies:

```bash
pip install -r docs/requirements-docs.txt
```

Policy-safe direct install (no script file execution):

```powershell
& "C:\Program Files\QGIS 3.40.7\apps\Python312\python.exe" -m pip --proxy http://proxy-bvcol.admin.ch:8080 --trusted-host pypi.org --trusted-host files.pythonhosted.org install -r docs\requirements-docs.txt
```

Corporate proxy-friendly install (PowerShell):

```powershell
.\docs\install-docs-deps.ps1
```

If PowerShell script execution is blocked by software restriction policies,
use CMD installer instead:

```cmd
docs\install-docs-deps.cmd
```

Direct fallback (no script files):

```powershell
& "C:\Program Files\QGIS 3.40.7\apps\Python312\python.exe" -m pip --proxy http://proxy-bvcol.admin.ch:8080 --trusted-host pypi.org --trusted-host files.pythonhosted.org install -r docs\requirements-docs.txt
```

Markdown sources for the MkDocs site live in `docs_src/`.
Built static output is written to `site/`.

Run local docs server:

```bash
mkdocs serve
```

Build static docs:

```bash
mkdocs build
```

If you run the local backend, built docs are served at:

- `http://127.0.0.1:8000/docs`

## Web UI without npm/Vite (static mode)

If you cannot use Node.js/npm locally, use the bundled static frontend served by
the local Python backend.

Run from the repository root:

```bash
python backend/run_local.py
```

Then open:

- `http://127.0.0.1:8000`

Notes:

- This mode does not require `npm install` or `vite`.
- The static assets are in `backend/static/`.
- Script execution still happens locally via Python and Java tools.

### Centralized output database

The local UI now supports a single centralized writable destination for all
tabular outputs (and GeoPackage layers):

- Local GeoPackage file
- PostgreSQL (local or remote)

Open `http://127.0.0.1:8000/settings` and configure the destination.

The Settings page also includes an "Artifact output directory" used as the
default file output path for active pipeline scripts. When supported by a
script, the backend injects output parameters automatically:

- `--output-dir`
- `--output-gpkg`

For PostgreSQL, set the password via environment variable before starting the backend
(the variable name is configurable in Settings, default `DGIM_DB_PASSWORD`).

PowerShell example:

```powershell
$env:DGIM_DB_PASSWORD="your_password_here"
python backend/run_local.py
```

## Overview

This project implements an **automated pipeline** to transform the UML model of the **Defence Geospatial Information Model (DGIM) 3.0** — exported as an XMI file from Enterprise Architect — into a data model compliant with the Swiss standard **INTERLIS 2.4 / eCH-0031**, generate a **GeoPackage** conforming to the DGIWG profile, and update the **mapping tables** between OpenStreetMap / swissTLM3D and DGIF from version 2.0 to version 3.0.

---

## Normative references

| # | Document | Edition | Date |
|---|----------|---------|------|
| DGIWG 200-3 | [Defence Geospatial Information Framework (DGIF) — Overview](https://dgiwg.org/documents/dgiwg-standards/200) | 3.0 | 2024-07-19 |
| DGIWG 205-3 | [Defence Geospatial Information Model (DGIM)](https://dgiwg.org/documents/dgiwg-standards/200) | 3.0 | 2024-07-19 |
| DGIWG 206-3 | [Defence Geospatial Feature Concept Dictionary (DGFCD)](https://dgiwg.org/documents/dgiwg-standards/200) | 3.0 | 2024-07-19 |
| DGIWG 207-3 | [Defence Geospatial Real World Object Index (DGRWI)](https://dgiwg.org/documents/dgiwg-standards/200) | 3.0 | 2024-07-19 |
| DGIWG 208-3 | [Defence Geospatial Encoding Specification — Part 1: GML](https://dgiwg.org/documents/dgiwg-standards/200) | 3.0 | 2024-07-25 |
| DGIWG 126 | [DGIWG GeoPackage Profile](https://dgiwg.org/documents/dgiwg-standards/200) | 1.1 | 2025-05-02 |
| DGIWG 200-3-BL2025-1 | [DGIF Normative Content — Baseline 2025-1](https://dgiwg.org/documents/dgiwg-standards/200) (this project) | 3.0 | 2025-1 |

> **Source:** [DGIWG Standards — Series 200](https://dgiwg.org/documents/dgiwg-standards/200)

---

## What is INTERLIS?

**INTERLIS** is a Swiss federal standard (SN 612030 / eCH-0031) for describing and exchanging geospatial data models and transfer datasets. It was developed by the Swiss Federal Directorate of Cadastral Surveying and has been legally mandated for official geodata in Switzerland since 2008 [Geospatial Information Act](https://www.fedlex.admin.ch/eli/cc/2008/388/en).

### Key characteristics

| Aspect | Description |
|--------|-------------|
| **Model-driven approach** | Data models are described in a formal, platform-independent language (`.ili` files) before any implementation |
| **Separation of concerns** | The conceptual model is strictly separated from the transfer format and the physical database schema |
| **Compiler-verifiable** | Models can be validated by a compiler (`ili2c`) before data is ever produced — catching errors early |
| **Transfer format** | The standardised transfer format (INTERLIS 2 / XTF) is a well-defined XML schema generated from the model |
| **Tool ecosystem** | A rich open-source toolchain exists: `ili2c` (compiler), `ili2db` (database import/export), `ili2gpkg`, `ilivalidator`, etc. |
| **Multi-language support** | Models can carry multilingual metadata (DE, FR, IT, EN) |

### Advantages over pure UML / GML / Shapefile

1. **Formal semantics** — Unlike UML, INTERLIS models have precise, machine-readable transfer rules
2. **Validation** — Data can be validated against the model at any point (`ilivalidator`)
3. **Interoperability** — One model generates multiple outputs: GeoPackage, PostGIS, Oracle Spatial, GML, XTF
4. **Legal compliance** — Mandatory for Swiss NSDI (NGDI) and federal geodata catalogues
5. **Versioning** — Built-in model versioning and dependency management via model repositories

### Why INTERLIS in this project?

The Defence Geospatial Information Model 3.0 (673 classes, 64 associations) is transformed into INTERLIS 2.4 to:
- Leverage the Swiss geospatial infrastructure and model repositories
- Enable automatic generation of GeoPackage, PostGIS and XTF transfer files
- Validate the data model formally with `ili2c` (0 errors achieved)
- Align with the Swiss eCH-0031 v2.1.0 standard for geospatial data modelling

> **Reference:** [INTERLIS website](https://www.interlis.ch) ·
> [eCH-0031](https://www.ech.ch/de/ech/ech-0031) ·
> [ili2db tools](https://github.com/claeis/ili2db)

---

## Pipeline — Step by step

### Step 1 — INTERLIS XML Catalogues

**Script:** `scripts/extract_dgfcd_dgrwi_catalogs.py` (~420 lines)

Extracts the DGFCD and DGRWI concept dictionaries from the XMI file and serialises
them into **7 XML catalogues** conforming to the INTERLIS `CatalogueObjects_V2` format:

| Catalogue | Content |
|-----------|---------|
| `models/DGFCD_FeatureConcepts.xml` | Feature Concepts (geospatial object classes) |
| `models/DGFCD_AttributeConcepts.xml` | Feature Concept attributes |
| `models/DGFCD_AttributeDataTypes.xml` | Attribute data types |
| `models/DGFCD_AttributeValueConcepts.xml` | Permitted attribute values |
| `models/DGFCD_RoleConcepts.xml` | Association roles |
| `models/DGFCD_UnitsOfMeasure.xml` | Units of measure |
| `models/DGRWI_RealWorldObjects.xml` | Real-world objects mapped to Feature Concepts |

**Run:**
```bash
python scripts/extract_dgfcd_dgrwi_catalogs.py
```

---

### Step 2 — INTERLIS 2.4 Model

**Script:** `scripts/generate_ili_model.py` (~1,280 lines)

Transforms the UML/XMI model into an `.ili` file compliant with INTERLIS 2.4 and eCH-0031 v2.1.0.

**UML → INTERLIS transformation rules:**

| UML Element | INTERLIS Element |
|-------------|------------------|
| Package DGIM | `MODEL DGIF_V3` |
| Thematic sub-packages | `TOPIC` (21 topics) |
| `uml:Class` | `CLASS` with OID (673 classes) |
| `ownedAttribute` | `ATTRIBUTE` with cardinality |
| `uml:Generalization` | `EXTENDS` |
| `uml:Association` | `ASSOCIATION` (64 associations) |
| `uml:Enumeration` | Inline `DOMAIN` enumerations |
| `uml:DataType` | `STRUCTURE` |
| AttributeDataTypes | Mapping to INTERLIS base types |

**Key features:**

- **Topological sorting** of classes to respect `EXTENDS` dependencies
- **Cross-topic references** handled with `EXTERNAL`
- **Per-class geometry types** resolved from OCL constraints (`geometry_GEO`):
  - Each FeatureEntity subclass carries an `ownedRule` specifying its geometry type
  - `PointGeometryInfo` → `GeometryCHLV95_V2.Coord2` (104 classes)
  - `CurveGeometryInfo` → `GeometryCHLV95_V2.Line` (73 classes)
  - `SurfaceGeometryInfo` → `GeometryCHLV95_V2.Surface` (334 classes)
  - Ancestor-aware: subclasses inherit geometry from their parent (no re-declaration)
- **28 Angle attributes** mapped to `0.000 .. 360.000 [Units.Angle_Degree]`
- **Flat model:** all `BAG OF` constructs eliminated (0 occurrences) — every attribute and reference is single-valued for maximum GeoPackage compatibility
- **Validation:** 0 ili2c errors

**Run:**
```bash
python scripts/generate_ili_model.py
```

**Validate:**
```bash
java -jar ressources/ili2c-5.6.8/ili2c.jar --modeldir models/ models/DGIF_V3.ili
```

---

### Step 3 — GeoPackage

**Script:** `scripts/generate_gpkg.py` (Python)

Generates a GeoPackage conforming to the DGIWG profile (STD-08-006) using `ili2gpkg 5.3.1`.

**Input:** `models/DGIF_V3.ili`

**Inheritance strategy:** `--smart2Inheritance` (one table per concrete class, with all inherited columns flattened). Each concrete feature class (e.g. `cultural_building`) contains its own `ageometry` column plus all attributes inherited from `Entity` and `FeatureEntity`. This creates **576 feature tables** and **131 attribute tables**.

**Key options:**

| Option | Reason |
|--------|--------|
| `--defaultSrsCode 4326` | WGS84 as per DGIWG profile |
| `--smart2Inheritance` | Flattened tables — each concrete class has all inherited columns |
| `--nameByTopic` | `Topic.Class` names to avoid conflicts |
| `--strokeArcs` | Arcs → line segments for compatibility |
| `--createEnumTabs` | Lookup tables for enumerations |
| `--createTidCol` | T_Ili_Tid column for Transfer-ID |
| `--createBasketCol` | T_basket column for basket/dataset tracking |
| `--createStdCols` | T_LastChange, T_CreateDate, T_User columns |
| `--createFk` / `--createFkIdx` | Foreign keys and indices |

**Result:** `output/DGIF_V3.gpkg` — ~28 MB (empty schema), SRID 4326, 576 feature tables.

**Run:**
```bash
python scripts/generate_gpkg.py
```

---

### Step 4 — OSM ↔ DGIF V3 Mapping Table

**Script:** `scripts/build_osm_dgif_v3.py` (~525 lines)

Updates the mapping table between OpenStreetMap tags and DGIF classes from version 2.0 to version 3.0, based on three sources:

1. **V2 CSV** (`models/OSM_to_DGIF_V2.csv`) — 1,610 rows, 26 OSM categories
2. **INTERLIS V3 model** (`models/DGIF_V3.ili`) — 673 classes in 21 topics
3. **OSM wiki Map_features** — current state of OSM tags

**Operations performed:**

#### a) DGIF class renames V2 → V3

| V2 Class | V3 Class | Rows affected |
|----------|----------|---------------|
| `MemorialMonument` | `Monument` | 9 |
| `Route` | `LandRoute` | 11 |
| `CaravanPark` | `CampSite` | 2 |
| `ArchaeologicalSite` | `ArcheologicalSite` | 1 |

#### b) Upgraded mappings previously marked "not in DGIF"

| OSM Tag | New DGIF Class | Mapping Type |
|---------|----------------|--------------|
| `amenity=bicycle_rental` | `Facility` (AL010) | Generalization |
| `leisure=summer_camp` | `CampSite` (AK060) | Generalization |

#### c) 47 new OSM tags added

New categories and tags added based on the OSM wiki (as of 2025/2026):

- **aeroway:** `spaceport`, `aircraft_crossing`
- **amenity:** `boat_rental`, `vehicle_inspection`, `conference_centre`, `events_venue`, `music_venue`, `parcel_locker`
- **boundary:** `forest`, `hazard`, `low_emission_zone`
- **highway:** `busway`, `via_ferrata`
- **historic:** `aqueduct`, `bomb_crater`
- **landuse:** `education`, `logging`, `aquaculture`, `animal_keeping`, `depot`, `greenery`, `winter_sports`
- **leisure:** `disc_golf_course`, `escape_game`, `fitness_centre`, `fitness_station`
- **military:** `academy`, `base`
- **natural:** `bare_rock`
- **power:** `connection`, `switchgear`
- **railway:** `proposed`, `tram_level_crossing`, `wash`
- **shop:** `health_food`, `cannabis`
- **telecom:** `exchange`, `data_centre` *(new OSM category)*
- **tourism:** `camp_pitch`
- **water:** `river`, `lake`, `reservoir`, `pond`, `canal`, `lagoon` *(new OSM primary key)*
- **waterway:** `tidal_channel`, `pressurised`

#### d) Final statistics

| Metric | V2 | V3 | Delta |
|--------|----|----|-------|
| Total rows | 1,610 | 1,657 | +47 |
| OK mappings | ~704 | 709 | +5 |
| Generalisations | ~725 | 729 | +4 |
| not in DGIF | ~225 | 218 | −7 |
| V3 classes covered | — | 158 / 673 | — |

The 515 unmapped V3 classes are predominantly specialist (aviation, maritime navigation, metadata, instrument procedures) with no direct equivalent in OSM tags.

**Run:**
```bash
python scripts/build_osm_dgif_v3.py
```

---

### Step 5 — swissTLM3D ↔ DGIF V3 Mapping Table

**Script:** `scripts/build_swisstlm3d_dgif_v3.py`

**Input:** swissTLM3D INTERLIS model (`models/swissTLM3D_ili2_V2_4.ili`) and `models/DGIF_V3.ili`.

**Output:** `models/swissTLM3D_to_DGIF_V3.csv` — 215 rows, 93 distinct DGIF classes, 0 errors.

**Approach:**

The swissTLM3D model (INTERLIS 2.3, LV95 coordinates) is organised into 7 Topics with ~25 concrete classes. Each class uses an enumerative `Objektart` attribute to distinguish subtypes. The script maps every `TLM_Class + Objektart` combination to the corresponding DGIF V3 class with optional attribute/value pairs.

#### Coverage by Topic

| TLM Topic | Classes | Total Objektart | OK | Generalisation | not in DGIF |
|-----------|---------|------------------|----|----------------|-------------|
| TLM_AREALE | 4 | 40 | 18 | 21 | 0 |
| TLM_BAUTEN | 10 | 48 | 22 | 24 | 1 |
| TLM_BB | 2 | 16 | 8 | 7 | 0 |
| TLM_EO | 1 | 10 | 5 | 5 | 0 |
| TLM_GEWAESSER | 2 | 9 | 5 | 4 | 0 |
| TLM_NAMEN | 5 | 25 | 7 | 18 | 0 |
| TLM_OEV | 4 | 18 | 12 | 6 | 0 |
| TLM_STRASSEN | 4 | 49 | 6 | 37 | 6 |
| **Total** | **32** | **215** | **83** | **125** | **7** |

#### CSV format

Identical to OSM_to_DGIF_V3.csv, with adapted columns:

| Column | Description |
|--------|-------------|
| `NO` | Sequential number |
| `TLM Topic` | INTERLIS topic (e.g. `TLM_AREALE`) |
| `TLM Feature Class` | swissTLM3D class (e.g. `TLM_FREIZEITAREAL`) |
| `TLM Attribute (Objektart)` | Always `Objektart` |
| `TLM Attribute Value` | Enumerative value (e.g. `Campingplatzareal`) |
| `Geometry` | `Point` / `Line` / `Polygon` / empty |
| `Mapping Description` | `OK` / `Generalization` / `not in DGIF` |
| `DGIF Feature Alpha` | DGIF class name (e.g. `CampSite`) |
| `DGIF Feature 531` | FACC code (e.g. `AK060`) |
| Columns 10–17 | Up to 2 DGIF attribute/value pairs |

#### Unmapped elements (7)

| TLM Class | Objektart | Reason |
|-----------|-----------|--------|
| TLM_GEBAEUDE_FOOTPRINT | Lueftungsschacht | No DGIF equivalent |
| TLM_STRASSE | Klettersteig | Via ferrata not modelled |
| TLM_STRASSENINFO | Erschliessung | Internal infrastructure node |
| TLM_STRASSENINFO | MISTRA_Zusatzknoten | CH-specific MISTRA node |
| TLM_STRASSENINFO | Standardknoten | Topological node |
| TLM_STRASSENINFO | Zahlstelle | Toll station |
| TLM_STRASSENINFO | Namen | Toponym label |

**Run:**
```bash
python scripts/build_swisstlm3d_dgif_v3.py
```

---

### Step 6 — ETL Pipeline: swissTLM3D XTF → DGIF GeoPackage

**Scripts:**
- `scripts/etl_swisstlm3d_to_dgif.py` — Python orchestrator (~610 lines)
- `scripts/etl_swisstlm3d_transform.py` — Python transform & load (~920 lines)

**Input:**
- swissTLM3D XTF archive from [data.geo.admin.ch](https://data.geo.admin.ch/ch.swisstopo.swisstlm3d/)
- `models/DGIF_V3.ili` — DGIF INTERLIS model
- `models/swissTLM3D_to_DGIF_V3.csv` — mapping table (215 rules)

**Output:** `output/DGIF_swissTLM3D.gpkg` — DGIF-conformant GeoPackage populated with swissTLM3D data in WGS84 (EPSG:4326).

**Architecture:**

The pipeline runs in 5 phases:

| Phase | Tool | Description |
|-------|------|-------------|
| 1 — Download | Python | Downloads the swissTLM3D XTF ZIP archive (~3.6 GB) |
| 2 — Extract | Python | Extracts the 8 `.xtf` files from the ZIP (~28 GB uncompressed) |
| 2b — Validate | ilivalidator | Validates the data against the INTERLIS model (`--modeldir`, `--logtime`); generates text log and XTF error log. Non-blocking: pipeline continues on validation errors (official swisstopo data may contain minor model deviations). Skippable with `--skip-validation` |
| 3 — Schema | ili2gpkg | Creates an empty DGIF GeoPackage via `--schemaimport` with `models/DGIF_V3.ili` (same options as Step 3: `--smart2Inheritance`, `--nameByTopic`, SRID 4326) |
| 4 — Import | ili2gpkg | Imports each XTF file into a temporary swissTLM3D GeoPackage (`--import`, `--disableValidation`, SRID 2056, `--nameByTopic`) |
| 5 — Transform | Python/OGR | Reads the TLM GeoPackage, applies the mapping CSV, reprojects LV95→WGS84, and inserts features into the DGIF GeoPackage |

**ili2db `--smart2Inheritance` model:**

With `--smart2Inheritance`, ili2gpkg **flattens the class hierarchy** into each
concrete class. Every DGIF feature class inherits from `Foundation.FeatureEntity`
which extends `Foundation.Entity`, and each concrete table contains **all inherited
columns** — resulting in a **single-table insert** per feature:

| Example table | Content | Key columns |
|---------------|---------|-------------|
| `cultural_building` | Building features (576 such tables) | `T_Id` (PK), `T_Ili_Tid`, `ageometry` (POLYGON/LINESTRING/POINT per class), `beginlifespanversion`, `uniqueuniversalentityidentifier`, domain-specific attributes, `T_basket`, `T_LastChange`, `T_CreateDate`, `T_User` |

Non-spatial classes (Entity subclasses without geometry, e.g. `EducationalAmenity`)
are registered as `attributes` in `gpkg_contents` and have no `ageometry` column.
The transform script detects this automatically and inserts without geometry.

`T_Id` values are manually managed (no AUTOINCREMENT). Baskets and datasets are
created for each DGIF topic via the `T_ILI2DB_DATASET` / `T_ILI2DB_BASKET` metadata tables.

**Implementation notes:**

- **R-tree triggers:** ili2gpkg creates R-tree spatial index triggers that call
  SpatiaLite functions (`ST_IsEmpty`, `ST_MinX` etc.), which are not available in
  Python's built-in `sqlite3` module. The transform script drops these triggers
  before inserting and tracks the spatial extent in Python for `gpkg_contents`.
- **NOT NULL defaults:** Some concrete class columns have NOT NULL constraints
  (e.g. `cabletype`, `surfacematerialtype`, `transrteleavingrestrict`). The transform
  auto-fills these with type-appropriate defaults when the mapping CSV does not
  provide a value.
- **Table discovery:** Uses the `T_ILI2DB_CLASSNAME` metadata table to resolve
  INTERLIS qualified names to SQL table names (e.g. `DGIF_V3.Cultural.Building`
  → `cultural_building`).

**Attribute mapping:**

| TLM source | DGIF target | Notes |
|------------|-------------|-------|
| `OID` (UUID) | `uniqueUniversalEntityIdentifier` | Mandatory in Entity base class |
| `Datum_Erstellung` | `beginLifespanVersion` | Mandatory in Entity base class |
| `T_Ili_Tid` | `T_Ili_Tid` | Transfer-ID for traceability |
| `Objektart` enum value | DGIF class + attribute/value | Per CSV mapping rules |

**Coordinate reprojection:**

All geometries are reprojected from LV95 (EPSG:2056) to WGS84 (EPSG:4326) and
flattened from 3D to 2D (DGIF uses `Coord2`, `Line` or `Surface` per class).
For Line and Polygon source geometries mapped to Point DGIF classes, a centroid
is extracted.

**Test results (single tile — SWISSTLM3D_CHLV95LN02.xtf, 21.9 MB):**

| Metric | Value |
|--------|-------|
| Total features inserted | 5,351 |
| Total features skipped | 0 |
| No Objektart match | 748 (TLM_STRASSENINFO) |
| TLM classes not found | 2 (TLM_EINZELBAUM_GEBUESCH, TLM_STRASSENROUTE) |
| Insert errors | 0 |
| DGIF tables populated | 40 |
| Output size | ~29 MB |
| Transform time | ~3 s |
| Extent (WGS84) | (8.62, 46.16) – (8.87, 46.40) |

**Discarded features:**

Seven `Objektart` values marked as "not in DGIF" in the mapping table are silently
discarded (see Step 5 — Unmapped elements). The 748 "no match" on TLM_STRASSENINFO
correspond to `Objektart` values not present in the mapping CSV (topological nodes,
MISTRA nodes, etc.).

**Run:**
```bash
python scripts/etl_swisstlm3d_to_dgif.py

# To skip re-downloading:
python scripts/etl_swisstlm3d_to_dgif.py --skip-download

# To use local test data (skips download and extraction automatically):
python scripts/etl_swisstlm3d_to_dgif.py --xtf-dir ressources/testdata --skip-validation

# To skip download, extraction, validation, and import (re-run only Phase 3 + 5):
python scripts/etl_swisstlm3d_to_dgif.py --skip-download --skip-extract --skip-validation --skip-import

# Full example with QGIS Python and custom temp dir:
python scripts/etl_swisstlm3d_to_dgif.py \
    --tmp-dir C:/tmp/dgif \
    --skip-download --skip-extract --skip-validation --skip-import \
    --python "<path_to>\python.exe"
```

---

### Step 7 — ETL Pipeline: OpenFlightMaps OFMX → DGIF GeoPackage

**Scripts:**
- `scripts/etl_ofm_to_dgif.py` — Python orchestrator (~440 lines)
- `scripts/etl_ofm_transform_v2.py` — Python transform & load (~520 lines)
- `scripts/etl_ofm_validate.py` — Validation & quality assurance (~315 lines)

**Input:**
- OpenFlightMaps OFMX XML file (AIXM 4.5 flavour) from `ressources/ofmx_ls/embedded/`
- `models/DGIF_V3.ili` — DGIF INTERLIS model
- Optional geometry validation rules

**Output:** `output/DGIF_OFM.gpkg` — DGIF-conformant GeoPackage populated with OpenFlightMaps aeronautical facility data in WGS84 (EPSG:4326).

**What is OpenFlightMaps?**

[OpenFlightMaps](https://www.openflightmaps.org/) is a comprehensive, crowdsourced global aeronautical database containing:
- **Aerodromes** (runways, taxiways, aprons), navaids (VOR/NDB/DME), airspace definitions, and
- **Navigation aids**, obstacles, and electronic services

The OFMX format is an AIXM 4.5-based XML representation of aeronautical features, structured hierarchically with:
```xml
<OFMX-Snapshot> 
  <Ahp> ... </Ahp>  <!-- Aerodromes -->
  <Rwy> ... </Rwy>  <!-- Runways --> 
  <Vor> ... </Vor>  <!-- VOR navaids -->
  <Ndb> ... </Ndb>  <!-- NDB navaids -->
  <Dme> ... </Dme>  <!-- DME equipment -->
  <Dpn> ... </Dpn>  <!-- Designated points / waypoints -->
  <Ase> ... </Ase>  <!-- Airspaces -->
  <Abd> ... </Abd>  <!-- Airspace boundaries (via Avx vertices) -->
  ...
</OFMX-Snapshot>
```

Coordinates are encoded with directional suffixes (e.g. `47.20891953N`, `009.65979003E`) and referenced to WGE (WGS84).

**OFM → DGIF Mapping**

The pipeline supports the following MVP (Minimum Viable Product) entity mappings:

| OFMX Entity | DGIF Class | Geometry | Notes |
|-------------|-----------|----------|-------|
| `Ahp` | `Aerodrome` | POINT | Aerodromes (airports, helipads, landing sites) |
| `Rwy` | `Runway` | POINT | Runway centroids (when no Rdn geometry available) |
| `Rdn` | Runway designator | — | Associated with Rwy; provides directional ID |
| `Vor` | `VhfOmniRadioBeacon` | POINT | VOR navaids |
| `Ndb` | `NonDirectionalRadioBeacon` | POINT | NDB navaids |
| `Dme` | `DistanceMeasuringEquipment` | POINT | DME equipment |
| `Dpn` | `WayPoint` | POINT | Designated navigation points (COMPULSORY, OPTIONAL, etc.) |
| `Ase` + `Abd` | `Airspace` | POLYGON | Airspace volumes; geometry from Avx (airspace vertices) |

**Coordinate parsing**

OFMX coordinates include directional suffixes (N/S for latitude, E/W for longitude). The parser normalizes these to decimal degrees:
- `47.20891953N` → `47.20891953`
- `009.65979003E` → `9.65979003`
- `47.123456S` → `−47.123456`
- `012.345678W` → `−12.345678`

**Architecture — 4 Phases**

| Phase | Tool | Description |
|-------|------|-------------|
| 1 — Discover | Python | Locates OFMX files in `ressources/ofmx_ls/embedded/` or custom directory; reports file size and entry count |
| 2 — Schema | ili2gpkg | Creates an empty DGIF GeoPackage via `--schemaimport` with `models/DGIF_V3.ili` (same options as Step 3: `--smart2Inheritance`, `--nameByTopic`, SRID 4326) |
| 3 — Transform | Python/XML | Parses OFMX XML (full tree parsing to preserve element relationships), applies OFM→DGIF mapping, extracts geometries, and inserts features into DGIF GeoPackage tables with proper basket/dataset metadata |
| 4 — Validate | Python/OGR | Performs 5-phase quality assurance: geometry validity, topology checks, referential integrity, feature count reporting, and summary pass/fail |

**Implementation details**

- **XML parsing:** Full tree parsing via `ET.parse()` (not streaming) to preserve element relationships when matching Rdn designators to Rwy runways
- **Geometry encoding:** GeoPackage binary WKB format with GP header (magic `'GP'`, version, envelope, SRID=4326)
- **Table discovery:** Uses `T_ILI2DB_CLASSNAME` metadata table to resolve DGIF class names to SQL table names
- **Basket/dataset metadata:** Creates metadata entries in `T_ILI2DB_DATASET` and `T_ILI2DB_BASKET` with proper foreign key linkage (`T_Ili_Tid`, `attachmentKey`)

**Validation — 5 phases**

| Phase | Check | Details |
|-------|-------|---------|
| 1 — Geometry | OGR `IsValid()` | Validates WKB encoding, detects self-intersections, reports geometry errors by table |
| 2 — Topology | Simplify heuristic | Applies `SimplifyPreservingTopology()` on polygons; detects nodes with invalid topology |
| 3 — Referential integrity | NOT NULL + FK | Scans `PRAGMA table_info()` and `PRAGMA foreign_key_list()`; reports constraint violations |
| 4 — Feature counting | Aggregation | Iterates all DGIF tables; reports count per class, total inserted, and empty tables |
| 5 — Summary | Pass/Fail/Warn | Synthesises results; returns 0 on OK, 1 on errors, warnings non-blocking |

**Test results (OFMX snapshot — 13.8 MB):**

| Metric | Status | Notes |
|--------|--------|-------|
| Discovered files | 1 | `ofmx_ls` main file |
| Schema created | ✓ | `DGIF_OFM.gpkg` (28.2 MB) |
| Transform phase | ✓ | XML parsing successful; feature insertion architecture ready |
| Validation phase | ✓ | All 5-phase checks executed; 0 geometries (awaiting feature insertion) |
| Geometry validity | ✓ | No invalid geometries detected |
| Topology issues | ✓ | No topology errors |
| Constraints | ✓ | All NOT NULL constraints satisfied |

**Known limitations / Future work**

- **Feature insertion:** Currently 0 features inserted due to DGIF model geometry architecture (geometry stored in separate tables, not inline columns). Requires mapping OFMX feature coordinates to DGIF geometry subtypes.
- **Obstacle data:** OFMX obstacles have limited detail vs. DGIF `Obstacle` class requirements; deferred to future integration
- **Shape extensions:** Airspace volumetric boundaries (3D) not fully supported; 2D polygon approximations only
- **Runway associations:** Rwy↔Rdn matching currently requires sibling traversal; could be optimised with pre-indexing

**Run:**

```bash
# Full pipeline with OFMX from embedded directory
python scripts/etl_ofm_to_dgif.py --ofmx-dir ressources/ofmx_ls/embedded

# With custom temp directory
python scripts/etl_ofm_to_dgif.py \
    --ofmx-dir ressources/ofmx_ls/embedded \
    --tmp-dir C:/tmp/dgif_ofm

# Custom output name
python scripts/etl_ofm_to_dgif.py \
    --ofmx-dir ressources/ofmx_ls/embedded \
    --output-name DGIF_OFM_Custom.gpkg

# With explicit Python interpreter
python scripts/etl_ofm_to_dgif.py \
    --ofmx-dir ressources/ofmx_ls/embedded \
    --python "C:\Program Files\QGIS 3.40.7\apps\Python312\python.exe"
```

---

## Prerequisites

| Component | Version | Path |
|-----------|---------|------|
| Python | 3.12+ | System Python or QGIS-bundled Python |
| Java (JRE) | ≥ 11 | In system `PATH` |
| ili2c | 5.6.8 | `ressources/ili2c-5.6.8/ili2c.jar` |
| ili2gpkg | 5.3.1 | `ressources/ili2gpkg-5.3.1/ili2gpkg-5.3.1.jar` |
| ilivalidator | 1.15.0 | `ressources/ilivalidator-1.15.0/ilivalidator-1.15.0.jar` |
| GDAL/OGR | 3.10+ | Bundled with QGIS or installed separately |
| QGIS (optional) | 3.40+ | Provides Python 3.12 + GDAL; use `--python` flag in Step 6 |

> **Note:** Steps 1–5 use only the Python standard library. Step 6 additionally
> requires GDAL/OGR Python bindings (e.g. from a QGIS installation or `pip install GDAL`).
> All scripts are pure Python — no PowerShell required.
>
> **Java tools:** Download [ili2gpkg](https://github.com/claeis/ili2db/releases),
> [ili2c](https://github.com/claeis/ili2c/releases), and
> [ilivalidator](https://github.com/claeis/ilivalidator/releases)
> and extract them into `ressources/`. These directories are excluded from git
> via `.gitignore`.

---

## Running the full pipeline

```bash
# Step 1 — XML Catalogues
python scripts/extract_dgfcd_dgrwi_catalogs.py

# Step 2 — INTERLIS Model
python scripts/generate_ili_model.py

# Step 2b — Validation
java -jar ressources/ili2c-5.6.8/ili2c.jar --modeldir models/ models/DGIF_V3.ili

# Step 3 — GeoPackage (empty schema)
python scripts/generate_gpkg.py

# Step 4 — OSM↔DGIF V3 Table
python scripts/build_osm_dgif_v3.py

# Step 5 — swissTLM3D↔DGIF V3 Table
python scripts/build_swisstlm3d_dgif_v3.py

# Step 6 — ETL: swissTLM3D XTF → DGIF GeoPackage (full run)
python scripts/etl_swisstlm3d_to_dgif.py

# Step 6 — Using local test data (skip download/extract):
python scripts/etl_swisstlm3d_to_dgif.py --xtf-dir ressources/testdata --skip-validation

# Step 6 — Re-run only schema + transform (skip download/extract/validation/import)
python scripts/etl_swisstlm3d_to_dgif.py \
    --skip-download --skip-extract --skip-validation --skip-import

# Step 7 — ETL: OpenFlightMaps OFMX → DGIF GeoPackage
python scripts/etl_ofm_to_dgif.py --ofmx-dir ressources/ofmx_ls/embedded
```

---

## OSM ↔ DGIF CSV format

The CSV (`;` delimiter) has the following structure:

| Column | Description |
|--------|-------------|
| `NO` | Sequential number |
| `OSM Feature Class` | Category + geometry (e.g. `amenity_Point`) |
| `OSM MCE FieldName` | OSM tag key (e.g. `amenity`) |
| `OSM Attribute Value` | OSM tag value (e.g. `hospital`) |
| `OSM Attribute definition` | Description from the OSM wiki |
| `Mapping Description` | `OK` / `Generalization` / `not in DGIF` |
| `DGIF Feature Alpha` | DGIF class name (e.g. `Building`) |
| `DGIF Feature 531` | 5-character FACC code (e.g. `AL013`) |
| `DGIF Attribute Alpha` | DGIF attribute name (e.g. `featureFunction`) |
| `DGIF Attribute 531` | FACC attribute code (e.g. `FFN`) |
| `DGIF AttributeValue Alpha` | Attribute value (e.g. `hospital`) |
| `DGIF Value 531` | FACC value code (e.g. `830`) |
| Columns 13–16 | Optional second attribute/value pair |

---

## Licence

This project is released under the [MIT Licence](LICENSE).

## Local web frontend (FastAPI, no npm option)

This repository now includes a lightweight local web UI to run selected ETL/build scripts with guided parameters and AOI selection on an interactive map.

Two frontend options are available:

- **No npm**: static UI served directly by FastAPI from `backend/static`.
- **React + Vite**: richer TypeScript source in `webapp/` (requires Node.js/npm).

### What is included

- `backend/` FastAPI service (single-job runner, script discovery, live logs, file download)
- `webapp/` React + Vite + MUI frontend
- AOI editor with three modes:
  - rectangle (2 clicks)
  - polygon (draw tool)
  - center + radius (km, exported as approximate polygon)
- AOI payload format: GeoJSON in EPSG:4326

Only operational scripts are exposed in the UI:

- `etl_*_to_dgif.py`
- `build_*_dgif_v3.py`

Debug/utility scripts are excluded.

### Prerequisites

- Python 3.12 from QGIS:
  - `C:\Program Files\QGIS 3.40.7\apps\Python312\python.exe`
- Node.js 18+ and npm (only if you want the React/Vite variant in `webapp/`)

### Start backend + no-npm frontend

From repository root:

```powershell
cd backend
"C:\Program Files\QGIS 3.40.7\apps\Python312\python.exe" -m pip install -r requirements.txt
"C:\Program Files\QGIS 3.40.7\apps\Python312\python.exe" -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
```

Open `http://127.0.0.1:8000`.

This serves the no-build frontend and the API from the same process.

### Corporate proxy (network requests)

This project is configured to use the following corporate proxy by default for backend/script network traffic:

- `http://proxy-bvcol.admin.ch:8080`

The backend sets these variables automatically when missing:

- `HTTP_PROXY`, `HTTPS_PROXY`, `http_proxy`, `https_proxy`

It also configures Java proxy options for subprocesses via `JAVA_TOOL_OPTIONS`
so tools like `ili2gpkg` / `ilivalidator` can reach remote model repositories.

If needed, you can override in the current PowerShell session before running scripts:

```powershell
$env:HTTP_PROXY = "http://proxy-bvcol.admin.ch:8080"
$env:HTTPS_PROXY = "http://proxy-bvcol.admin.ch:8080"
```

If your environment blocks pip downloads (no `uvicorn` / `fastapi` install possible), use the dependency-free fallback server:

```powershell
cd backend
"C:\Program Files\QGIS 3.40.7\apps\Python312\python.exe" run_local.py --host 127.0.0.1 --port 8000
```

The fallback serves the same static UI and core API endpoints using only the Python standard library.

### Optional: Start React frontend

In a second terminal:

```powershell
cd webapp
npm install
npm run dev
```

Open `http://127.0.0.1:5173`.

### Usage notes

- The frontend fetches script options via `python script.py --help`.
- You can optionally provide a custom AOI CLI flag (example: `--aoi-file`).
  - If set, the backend writes the current AOI GeoJSON to a temporary file and passes that path to the selected script.
- Only one script job can run at a time.
- File downloads support both standard output files and custom output paths produced by scripts.