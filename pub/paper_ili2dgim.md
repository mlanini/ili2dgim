# From UML to GeoPackage: A Model-Driven Pipeline for Implementing the Defence Geospatial Information Model 3.0 with INTERLIS 2.4

**Authors:** *Michael Lanini*

**Abstract.**
The Defence Geospatial Information Framework (DGIF) 3.0, maintained by the Defence Geospatial Information Working Group (DGIWG), defines a comprehensive conceptual schema for military and civilian geospatial interoperability — comprising 673 feature classes, 64 associations, and a rich concept dictionary. Despite its normative completeness, the framework lacks a reference implementation that bridges the UML conceptual model with a readily usable, physically instantiated geodatabase. This paper presents *ili2dgim*, a fully automated, model-driven pipeline that transforms the DGIF 3.0 UML model — exported as XMI from Enterprise Architect — into an INTERLIS 2.4 data model compliant with the Swiss federal standard eCH-0031, generates an OGC GeoPackage conforming to the DGIWG profile, extracts the DGFCD and DGRWI concept dictionaries as INTERLIS XML catalogues, and populates the schema with real-world data from heterogeneous sources (swissTLM3D, Overture Maps, OpenStreetMap) through semantic mapping tables and ETL pipelines. The resulting artefacts — a compiler-verified INTERLIS model (0 errors), a 576-table GeoPackage schema in WGS 84, and populated datasets with over 5,000 features — demonstrate that the Swiss INTERLIS ecosystem can serve as an effective implementation platform for NATO/DGIWG standards, enabling formal validation, multi-format output generation, and cross-source geodata harmonisation. We discuss the transformation rules, inheritance flattening strategies, geometry resolution from OCL constraints, and cross-domain semantic mapping methodology, and evaluate the pipeline against completeness, correctness, and interoperability criteria.

**Keywords:** DGIF 3.0, DGIM, INTERLIS 2.4, GeoPackage, model-driven architecture, geospatial interoperability, ETL, DGIWG, eCH-0031, swissTLM3D, Overture Maps, OpenStreetMap

---

## 1. Introduction

### 1.1 Motivation and Problem Statement

Geospatial data interoperability remains one of the most persistent challenges in defence and civilian spatial data infrastructures. NATO and its partner nations exchange vast quantities of geographic information for situational awareness, mission planning, logistics, and humanitarian operations. The Defence Geospatial Information Working Group (DGIWG) has developed a comprehensive suite of standards — the Defence Geospatial Information Framework (DGIF) 3.0 — to harmonise this exchange. At its core lies the Defence Geospatial Information Model (DGIM), a UML conceptual schema defining 673 feature classes organised into 21 thematic domains, from transportation infrastructure to hydrography, cultural features to aeronautical information.

However, a conceptual model alone does not produce interoperable data. The gap between a UML class diagram maintained in Enterprise Architect and a populated geodatabase that can be loaded into a GIS, validated against a formal schema, and exchanged between heterogeneous systems, is substantial. Existing DGIF encoding specifications focus primarily on GML (Geography Markup Language), which, while standards-compliant, presents practical limitations: GML files are verbose, tooling is limited in operational GIS environments, and the encoding rules require significant manual effort to maintain as the model evolves.

This paper addresses the question: *Can the DGIF 3.0 conceptual model be automatically transformed into a standards-compliant, physically instantiated geodatabase using a model-driven approach — and can heterogeneous real-world data sources be harmonised into this schema through automated ETL pipelines?*

### 1.2 The Interoperability Challenge in Defence Geospatial Data

Military geospatial interoperability operates at multiple levels. At the *syntactic* level, data must conform to agreed formats (GML, GeoPackage, Shapefile). At the *semantic* level, the meaning of features and attributes must be shared — a "road" in one dataset must correspond to a "road" in another, with compatible attribute domains. At the *schematic* level, the structure of the data — class hierarchies, associations, cardinalities — must be mutually understood.

DGIWG addresses these levels through a layered architecture:

- **DGIM** (DGIWG 205-3): the conceptual schema — UML classes, attributes, associations, and constraints.
- **DGFCD** (DGIWG 206-3): the Defence Geospatial Feature Concept Dictionary — a controlled vocabulary of feature types, attributes, and permitted values, identified by FACC (Feature and Attribute Coding Catalogue) codes.
- **DGRWI** (DGIWG 207-3): the Defence Geospatial Real World Object Index — mapping real-world entities to DGFCD concepts.
- **Encoding Specifications** (DGIWG 208-3): rules for serialising the model into GML, with the DGIWG GeoPackage Profile (DGIWG 126) providing an alternative vector tile format.

The challenge lies in *implementing* this architecture end-to-end: from the normative UML model to a physical database schema that can ingest, validate, and serve data from diverse national sources.

### 1.3 Research Questions

This paper addresses three research questions:

1. **RQ1 — Model Transformation:** How can the DGIF 3.0 UML model (673 classes, 64 associations, OCL constraints) be automatically and correctly transformed into an INTERLIS 2.4 data model that compiles without errors?

2. **RQ2 — Schema Generation:** Can the resulting INTERLIS model be used to generate a GeoPackage conforming to the DGIWG profile, with an inheritance strategy suitable for operational GIS use?

3. **RQ3 — Data Harmonisation:** Can heterogeneous geodata sources (swissTLM3D, Overture Maps, OpenStreetMap) be semantically mapped to the DGIF schema and loaded into the generated GeoPackage through automated ETL pipelines, producing cross-source consistent results?

### 1.4 Contributions

This paper makes the following contributions:

1. **A complete UML-to-INTERLIS transformation pipeline** that parses XMI, resolves class hierarchies and OCL geometry constraints, performs topological sorting, and produces a 7,782-line INTERLIS 2.4 model with 0 compiler errors.

2. **An automated GeoPackage generation workflow** using the INTERLIS toolchain (ili2gpkg) with smart inheritance flattening, producing a 576-table schema conforming to DGIWG STD-08-006.

3. **Cross-domain semantic mapping tables** between three heterogeneous data sources and the DGIF schema: OSM (1,657 rules), swissTLM3D (215 rules), and Overture Maps — with a formal methodology for convergence analysis.

4. **ETL pipelines** that transform and load real-world data (5,351+ features) into the DGIF GeoPackage, handling coordinate reprojection, geometry type coercion, and attribute domain validation.

5. **An open-source reference implementation** (*ili2dgim*) demonstrating the viability of the Swiss INTERLIS ecosystem as an implementation platform for NATO/DGIWG geospatial standards.

### 1.5 Paper Structure

The remainder of this paper is organised as follows. Section 2 provides background on the DGIF framework, INTERLIS, and related work. Section 3 describes the overall pipeline architecture. Section 4 details the UML-to-INTERLIS model transformation. Section 5 covers concept dictionary extraction. Section 6 presents GeoPackage schema generation. Section 7 discusses semantic mapping methodology. Section 8 describes the ETL pipelines. Section 9 evaluates results. Section 10 discusses implications and limitations. Section 11 concludes with future work.

---

## 2. Background and Related Work

### 2.1 Defence Geospatial Information Framework (DGIF) 3.0

The DGIF is a suite of standards published by DGIWG (Defence Geospatial Information Working Group), the NATO-affiliated body responsible for geospatial standardisation. Version 3.0, released in July 2024 [1], represents a major restructuring from previous editions. The framework comprises:

- **DGIWG 200-3** — Overview and architecture document.
- **DGIWG 205-3** — The DGIM: a UML conceptual model with 673 classes, 64 associations, and 21 thematic topics.
- **DGIWG 206-3** — The DGFCD: a concept dictionary with FACC-coded feature types, attribute types, data types, attribute values, role concepts, and units of measure.
- **DGIWG 207-3** — The DGRWI: a mapping between real-world objects and feature concepts.
- **DGIWG 208-3** — GML encoding specification.
- **DGIWG 126** — GeoPackage Profile v1.1 (2025), defining how DGIF data should be stored in OGC GeoPackage format.
- **DGIWG 200-3-BL2025-1** — Normative Content Baseline 2025-1, the specific version of the UML model used in this work.

The DGIM is structured as a hierarchy rooted in two base classes: `Entity` (carrying lifecycle metadata, external references, and unique identifiers) and `FeatureEntity` (extending `Entity` with geometry). The 21 topics span domains including Foundation, Transportation, Cultural, Hydrography, Physiography, Military, Aeronautical Information, and Maritime Navigation.

### 2.2 The DGIM UML Model: Structure, Scope, and Normative Content

The DGIF Baseline 2025-1 UML model is maintained in Sparx Systems Enterprise Architect and exported as an XMI 2.1 file (`DGIF_BL_2025-1.xmi`, approximately 8 MB). Key structural characteristics include:

- **673 concrete classes** distributed across 21 thematic packages (topics).
- **64 explicit associations** with named roles and cardinalities.
- **OCL constraints** on `FeatureEntity` subclasses: each concrete feature class carries an `ownedRule` named `geometry_GEO` whose body specifies the permitted geometry type via `oclIsKindOf()` — e.g., `inv: geometry->forAll(g|g.oclIsKindOf(PointGeometryInfo))`.
- **Deep inheritance hierarchies**: some classes are 3–4 levels deep (e.g., `Building` extends `Structure` extends `FeatureEntity` extends `Entity`).
- **Cross-package references**: classes in one topic frequently reference classes in another (e.g., a `Transportation.Railway` may reference a `Foundation.ContactInfo`).
- **Enumerations and data types**: approximately 60 data types defined in the Foundation package, plus numerous enumerations for attribute domains.

The geometry type distribution across the 673 classes is: 104 Point classes, 73 Curve/Line classes, 334 Surface/Polygon classes, and 162 classes without geometry (non-spatial entities or metadata classes).

### 2.3 INTERLIS 2.4 and the Swiss NSDI Standards Ecosystem

INTERLIS is a Swiss federal standard (SN 612030) for describing and exchanging geospatial data models and datasets [2]. Legally mandated for official geodata in Switzerland since the Geoinformation Act of 2007 (SR 510.62), it is governed by the eCH-0031 standard (currently v2.1.0) [3].

**Key characteristics of INTERLIS:**

- **Model-driven approach:** Data models are described in a formal, platform-independent language (`.ili` files) before any physical implementation.
- **Separation of concerns:** Conceptual model ↔ transfer format ↔ physical schema are strictly separated.
- **Compiler-verifiable:** The INTERLIS compiler `ili2c` validates model syntax and semantics before data production.
- **Rich type system:** Support for structured types, enumerations with hierarchies, units of measure, coordinate reference systems, polylines with arcs, surfaces with topology rules.
- **Multi-target generation:** A single `.ili` model generates GeoPackage, PostGIS, Oracle Spatial, GML, and INTERLIS XTF transfer files via the `ili2db` toolchain.
- **Validation:** The `ilivalidator` tool verifies data conformance against the model at any stage.

The INTERLIS toolchain is open-source and comprises:

| Tool | Purpose | Version used |
|------|---------|-------------|
| `ili2c` | Model compiler and validator | 5.6.8 |
| `ili2gpkg` | GeoPackage schema/data import/export | 5.3.1 |
| `ilivalidator` | Data validation against INTERLIS models | 1.15.0 |

The relevance of INTERLIS to this work lies in its unique combination of formal model semantics, automatic schema generation, and built-in validation — properties that align well with the DGIF's need for rigorous, machine-verifiable implementations.

### 2.4 OGC GeoPackage and the DGIWG Profile

OGC GeoPackage is an open, standards-based, platform-independent format for storing geospatial data in a SQLite database [4]. It supports vector features, tile matrices, attributes, and extensions. The DGIWG GeoPackage Profile (DGIWG 126, v1.1) [5] constrains the general OGC specification for defence use, mandating:

- WGS 84 (EPSG:4326) as the default spatial reference system.
- Specific metadata tables and content types.
- Feature table naming conventions.
- Geometry encoding in GeoPackage binary (GP header + WKB).

The `ili2gpkg` tool, part of the `ili2db` suite, generates GeoPackage databases directly from INTERLIS models, including spatial indexes, foreign keys, metadata tables, and enumeration lookup tables — making it an ideal bridge between the INTERLIS conceptual model and the DGIWG GeoPackage physical implementation.

### 2.5 Related Approaches to Geospatial Schema Transformation

Several approaches exist for transforming UML geospatial models into physical implementations:

**ShapeChange** [6] is a Java tool developed by interactive instruments GmbH that transforms UML models (in Enterprise Architect format) into GML application schemas, JSON Schema, SQL DDL, and other targets. It is widely used in the INSPIRE community and supports ISO 19109 application schemas. However, ShapeChange does not target INTERLIS and lacks native support for the DGIM's OCL geometry constraints.

**INSPIRE** (Infrastructure for Spatial Information in the European Community) [7] uses a similar model-driven approach with UML schemas transformed to GML application schemas. The INSPIRE approach shares many goals with our work but operates within the European SDI context and relies on GML rather than GeoPackage for data exchange.

**HALE Studio** [8] provides a graphical environment for schema-to-schema mapping and transformation, supporting GML, GeoPackage, Shapefile, and database formats. While powerful for interactive mapping, it does not operate at the model level (UML → formal data description language) as our pipeline does.

**Our approach differs** in several key aspects: (1) it targets INTERLIS 2.4 as the intermediate formal language, enabling compiler verification and multi-target generation; (2) it resolves geometry types from OCL constraints in the UML model; (3) it produces a flat (non-hierarchical) GeoPackage schema through smart inheritance flattening; and (4) it integrates end-to-end from UML parsing to populated geodatabases with cross-source harmonisation.

---

## 3. Architecture of the Transformation Pipeline

### 3.1 End-to-End Overview

The *ili2dgim* pipeline transforms the DGIF 3.0 UML model into a populated, standards-compliant geospatial database through six automated stages:

```
                 ┌─────────────────────────────────────────────────────────────┐
                 │                    DGIF BL 2025-1 (XMI)                     │
                 └──────────────┬────────────────────────┬─────────────────────┘
                                │                        │
                       ┌────────▼─────────┐      ┌───────▼────────┐
                       │  Step 1: Extract │      │  Step 2: Gen.  │
                       │  DGFCD / DGRWI   │      │  INTERLIS .ili │
                       │  XML Catalogues  │      │  Model         │
                       └────────┬─────────┘      └───────┬────────┘
                                │                        │
                       7 XML files               DGIF_V3.ili (7782 lines)
                                │                        │
                                │               ┌────────▼────────┐
                                │               │  Step 3: Gen.   │
                                │               │  GeoPackage     │
                                │               │  (ili2gpkg)     │
                                │               └────────┬────────┘
                                │                        │
                                │               DGIF_V3.gpkg (576 tables)
                                │                        │
                 ┌──────────────┼────────────────────────┼────────────────┐
                 │              │                        │                │
        ┌────────▼──────┐ ┌────▼──────────┐ ┌──────────▼──────┐           │
        │ Step 4: OSM   │ │ Step 5: TLM3D │ │ Step 6a: ETL    │           │
        │ Mapping Table │ │ Mapping Table │ │ swissTLM3D      │           │
        │ (1657 rows)   │ │ (215 rows)    │ │ → DGIF GPKG     │           │
        └───────────────┘ └───────────────┘ └──────────┬──────┘           │
                                                       │          ┌───────▼──────┐
                                                       │          │ Step 6b: ETL │
                                                       │          │ Overture     │
                                                       │          │ → DGIF GPKG  │
                                                       │          └──────┬───────┘
                                                       │                 │
                                            ┌──────────▼─────────────────▼──┐
                                            │  DGIF GeoPackage (populated)  │
                                            │  WGS84 · 40+ tables · 5351+   │
                                            │  features per source          │
                                            └───────────────────────────────┘
```

### 3.2 Design Principles

The pipeline is governed by four design principles:

1. **Full automation:** Every step from XMI parsing to populated GeoPackage runs without manual intervention. A single command sequence reproduces the entire output.

2. **Standards compliance:** The output conforms to three intersecting standards: INTERLIS 2.4 / eCH-0031 (data model), DGIWG GeoPackage Profile (physical schema), and DGIF 3.0 / Baseline 2025-1 (conceptual schema).

3. **Formal verification:** The INTERLIS model is compiled with `ili2c` (0 errors required), and data can be validated with `ilivalidator` at any stage.

4. **Reproducibility:** All scripts are pure Python (standard library for Steps 1–5; GDAL/OGR for Step 6). The only external runtime dependency is Java for the INTERLIS toolchain.

### 3.3 Technology Stack

| Layer | Technology | Role |
|-------|-----------|------|
| UML model source | Enterprise Architect XMI 2.1 | Input — normative DGIF UML model |
| Model transformation | Python 3.12, `xml.etree.ElementTree` | XMI parsing, model generation |
| Formal model language | INTERLIS 2.4 | Intermediate representation — compiler-verified |
| Model compilation | `ili2c` 5.6.8 (Java) | Syntax and semantic validation |
| Schema generation | `ili2gpkg` 5.3.1 (Java) | GeoPackage DDL from INTERLIS |
| Data validation | `ilivalidator` 1.15.0 (Java) | INTERLIS XTF conformance checking |
| ETL — geometry | GDAL/OGR 3.10+ (Python bindings) | Coordinate reprojection, geometry coercion |
| ETL — storage | `sqlite3` (Python stdlib) | Direct GeoPackage/SQLite insertion |
| Visualisation | QGIS 3.40+ | GeoPackage rendering and verification |

---

## 4. UML-to-INTERLIS Model Transformation

### 4.1 XMI Parsing and UML Element Extraction

The transformation begins with parsing the DGIF Baseline 2025-1 XMI file (`DGIF_BL_2025-1.xmi`), an XML Metadata Interchange document conforming to XMI 2.1, exported from Sparx Systems Enterprise Architect.

The parser, implemented in `generate_ili_model.py` (approximately 1,280 lines of Python), uses the `xml.etree.ElementTree` API and extracts the following UML elements:

- **Packages:** Navigated recursively from the root model to identify thematic sub-packages (DGIM topics). Each `packagedElement` with `xmi:type="uml:Package"` becomes an INTERLIS `TOPIC`.
- **Classes:** `packagedElement` elements with `xmi:type="uml:Class"` within topic packages. Each class is assigned an OID based on INTERLIS conventions.
- **Attributes:** `ownedAttribute` elements within classes, with their type references resolved against a global `xmi:id → name` map. Cardinalities are extracted from `lowerValue`/`upperValue` specifications.
- **Generalisations:** `generalization` elements linking subclasses to superclasses, resolving `general` references to establish EXTENDS relationships.
- **Associations:** `packagedElement` elements with `xmi:type="uml:Association"`, with `memberEnd` and `ownedEnd` references resolved to identify participating classes and roles.
- **Enumerations:** Classes with `xmi:type="uml:Enumeration"`, whose `ownedLiteral` elements become INTERLIS enumeration values.
- **Data types:** Classes with `xmi:type="uml:DataType"` in the Foundation package, mapped to INTERLIS `STRUCTURE` definitions or base type equivalents.
- **OCL constraints:** `ownedRule` elements containing geometry specifications (see Section 4.4).

A critical pre-processing step builds a global ID-to-name resolution map encompassing all `xmi:id` values in the document, enabling cross-reference resolution throughout the transformation.

### 4.2 Transformation Rules: UML → INTERLIS 2.4 Mapping

The core of the transformation is a rule-based mapping from UML elements to INTERLIS 2.4 constructs:

| UML Element | INTERLIS 2.4 Construct | Notes |
|---|---|---|
| Package `DGIM` (root) | `MODEL DGIF_V3 (en) AT "https://www.dgiwg.org/dgif" VERSION "2025-1"` | Single model with English locale |
| Thematic sub-packages | `TOPIC <name> =` | 21 topics generated |
| `uml:Class` | `CLASS <name> (FINAL) = OID TEXT*36;` | UUID-based OID |
| `uml:Generalization` | `EXTENDS <superclass>` | Within same topic |
| `ownedAttribute` | `<name> : [MANDATORY] <type>;` | Cardinality-aware |
| `uml:Association` | `ASSOCIATION <name> =` | With role names and cardinalities |
| `uml:Enumeration` | Inline `(value1, value2, ...)` | Flat enumeration |
| `uml:DataType` (Foundation) | `STRUCTURE` or INTERLIS base type | Via `INTERLIS_TYPE_MAP` (≈60 entries) |
| Multi-valued attribute (`*`) | Single-valued attribute | BAG OF eliminated |

**Type mapping** is governed by the `INTERLIS_TYPE_MAP` dictionary, which maps approximately 60 UML data type names to their INTERLIS equivalents. Examples:

| UML Data Type | INTERLIS Type |
|---|---|
| `CharacterString` | `TEXT*255` |
| `Integer` | `0 .. 999999999` |
| `Real` | `0.000 .. 99999999.999` |
| `Boolean` | `BOOLEAN` |
| `Date` | `FORMAT INTERLIS.XMLDate` |
| `DateTime` | `FORMAT INTERLIS.XMLDateTime` |
| `URI` | `INTERLIS.URI` |
| `Measure` | `0.000 .. 99999999.999` |

Attributes whose name contains the substring "Angle" receive the specialised type `0.000 .. 360.000 [Units.Angle_Degree]`, affecting 28 attributes across the model.

### 4.3 Handling Class Hierarchies: Topological Sorting and Cross-Topic References

The DGIM's deep class hierarchies and cross-topic references present two ordering challenges for INTERLIS generation:

**Intra-topic class ordering.** Within each TOPIC, classes must be ordered such that superclasses appear before their subclasses (for `EXTENDS` declarations) and referenced classes appear before referencing classes (for `REFERENCE TO` declarations). The pipeline uses **Kahn's algorithm** for topological sorting with two dependency types:

- *Hard dependencies* (EXTENDS): the superclass must precede the subclass. Violation is a compilation error.
- *Soft dependencies* (REFERENCE TO): the referenced class should precede the referencing class. Violation causes a forward reference, handled by emitting the reference as an INTERLIS comment (`!!`).

When the combined hard + soft dependency graph contains cycles (which can occur with mutual references), the algorithm falls back to EXTENDS-only ordering, and soft dependencies that create forward references are emitted as comments.

**Inter-topic ordering.** Topics are topologically sorted based on their EXTENDS dependencies. Cross-topic `REFERENCE TO` declarations are marked with the `(EXTERNAL)` qualifier per INTERLIS §2.6.3, and the referenced topic is listed in the `DEPENDS ON` clause — but only if the target topic has already been emitted.

### 4.4 Geometry Resolution from OCL Constraints

A distinguishing feature of the DGIM is that geometry types are not declared as UML attributes with explicit spatial types. Instead, each concrete `FeatureEntity` subclass carries an `ownedRule` named `geometry_GEO` containing an OCL (Object Constraint Language) invariant of the form:

```
inv: geometry->forAll(g | g.oclIsKindOf(PointGeometryInfo))
```

The pipeline resolves these constraints through the following process:

1. **Extraction:** The function `build_geometry_type_map()` scans all `ownedRule` elements in the XMI and applies the regex `oclIsKindOf\((\w+)\)` to extract the geometry info class name.

2. **Mapping:** Each geometry info class is mapped to an INTERLIS geometry type:

   | OCL Geometry Info | INTERLIS Type | Feature Count |
   |---|---|---|
   | `PointGeometryInfo` | `DGIF_V3.Coord2` | 104 classes |
   | `CurveGeometryInfo` | `DGIF_V3.Line` | 73 classes |
   | `SurfaceGeometryInfo` | `DGIF_V3.Surface` | 334 classes |

3. **Priority resolution:** When an OCL constraint specifies multiple permitted geometry types (e.g., both Point and Surface), the type with the highest priority is selected: `Surface > Line > Coord3 > Coord2`.

4. **Inheritance handling:** The function `ancestor_has_geometry()` traverses the class hierarchy upward to determine whether a superclass already declares a geometry attribute. If so, the subclass does not re-declare it, preventing compilation errors from duplicate attribute definitions.

5. **Emission:** The base class `FeatureEntity` emits a placeholder `GeometryPlaceholder` that is filtered from the output. Each concrete subclass declares its own `geometry : MANDATORY <type>` attribute based on the resolved OCL constraint.

### 4.5 Flat Model Strategy: Eliminating BAG OF Constructs

INTERLIS supports multi-valued attributes through `BAG OF` and `LIST OF` constructs. However, these structures pose significant challenges for GeoPackage generation: each `BAG OF` attribute requires a separate table with a foreign key back to the parent, complicating both the schema and downstream ETL queries.

The pipeline adopts a **flat model strategy**: all attributes with upper cardinality `*` (unbounded) are collapsed to single-valued attributes. This eliminates all `BAG OF` constructs from the generated model (0 occurrences in the output), producing a schema where every attribute maps to a single column in the corresponding GeoPackage table.

This design decision trades completeness for practicality: while a multi-valued attribute theoretically loses information when flattened, the DGIF's operational use cases (feature exchange, terrain analysis, mission planning) overwhelmingly involve single-valued attribute instances. The mapping tables (Section 7) further validate this assumption — no mapping rule requires multi-valued attribute assignment.

### 4.6 Compiler Validation

The generated INTERLIS model (`DGIF_V3.ili`, 7,782 lines) is validated by the INTERLIS compiler `ili2c` 5.6.8:

```
java -jar ili2c.jar --modeldir models/ models/DGIF_V3.ili
```

The model compiles with **0 errors**, confirming syntactic and semantic correctness of all type references, EXTENDS declarations, ASSOCIATION definitions, and DOMAIN specifications. The model declares:

- **1 MODEL** (`DGIF_V3`) with English locale
- **21 TOPICs**
- **673 CLASSes** with `OID TEXT*36`
- **64 ASSOCIATIONs**
- **Global DOMAINs**: `Coord2` (WGS 84 2D), `Coord3` (WGS 84 3D), `Line` (polyline with straights and arcs), `Surface` (surface without overlaps)
- **IMPORTS**: `Units` (from the INTERLIS standard library)

---

## 5. Concept Dictionary Extraction (DGFCD and DGRWI)

### 5.1 Feature Concepts, Attribute Concepts, and Value Domains

The DGFCD (Defence Geospatial Feature Concept Dictionary) is a controlled vocabulary that provides unique FACC (Feature and Attribute Coding Catalogue) codes for every feature type, attribute, and attribute value in the DGIM. The DGRWI (Defence Geospatial Real World Object Index) maps real-world objects to their corresponding DGFCD feature concepts.

The extraction script (`extract_dgfcd_dgrwi_catalogs.py`, approximately 420 lines) navigates the XMI package hierarchy to locate six DGFCD sub-packages and the DGRWI package:

| DGFCD Package | Content | Output File |
|---|---|---|
| FeatureConcepts | Geospatial object classes | `DGFCD_FeatureConcepts.xml` |
| AttributeConcepts | Feature concept attributes | `DGFCD_AttributeConcepts.xml` |
| AttributeDataTypes | Attribute data types | `DGFCD_AttributeDataTypes.xml` |
| AttributeValueConcepts | Permitted attribute values | `DGFCD_AttributeValueConcepts.xml` |
| RoleConcepts | Association roles | `DGFCD_RoleConcepts.xml` |
| UnitsOfMeasure | Units of measurement | `DGFCD_UnitsOfMeasure.xml` |
| DGRWI | Real-world objects → feature concepts | `DGRWI_RealWorldObjects.xml` |

### 5.2 Serialisation to INTERLIS CatalogueObjects_V2 XML

Each catalogue is serialised as an INTERLIS 2.4 transfer file conforming to the `CatalogueObjects_V2` schema. The XML structure follows the INTERLIS XTF (XML Transfer Format) conventions:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<TRANSFER xmlns="http://www.interlis.ch/INTERLIS2.4">
  <HEADERSECTION SENDER="ili2dgim" VERSION="2.4">
    <MODELS><MODEL NAME="DGFCD_Catalogues"/></MODELS>
  </HEADERSECTION>
  <DATASECTION>
    <DGFCD_Catalogues.FeatureConcepts BID="b1">
      <FeatureConcept TID="b1.1">
        <name>Building</name>
        <code>AL013</code>
        <definition>A relatively permanent structure...</definition>
      </FeatureConcept>
      <!-- ... -->
    </DGFCD_Catalogues.FeatureConcepts>
  </DATASECTION>
</TRANSFER>
```

Each entry receives a unique TID (Transfer Identifier) in the format `<basket_id>.<sequential_number>`, enabling cross-referencing between catalogues (e.g., an AttributeConcept references its parent FeatureConcept via TID).

### 5.3 Role of Catalogues in Schema Validation and ETL

The extracted catalogues serve multiple purposes in the pipeline:

1. **ETL attribute validation:** The `DGFCD_AttributeValueConcepts.xml` catalogue defines the permitted values for each DGIF attribute. ETL pipelines (Section 8) use these values to verify that mapped attribute values are within the DGFCD domain.

2. **Mapping table construction:** When building semantic mappings (Section 7), the FACC codes from `DGFCD_FeatureConcepts.xml` provide the authoritative identifier for each DGIF class, enabling unambiguous source-to-target correspondence.

3. **Documentation:** The catalogues serve as machine-readable reference documentation for the DGIF schema, complementing the `.ili` model file.

---

## 6. GeoPackage Generation and Schema Design

### 6.1 From INTERLIS to GeoPackage via ili2gpkg

The `generate_gpkg.py` script (approximately 214 lines) invokes `ili2gpkg 5.3.1` to perform a `--schemaimport` operation: reading the INTERLIS model and generating a GeoPackage database with the corresponding table structure, constraints, indexes, and metadata.

The invocation passes the following key options:

```
java -jar ili2gpkg-5.3.1.jar --schemaimport \
    --dbfile output/DGIF_V3.gpkg \
    --modeldir "models/;http://models.interlis.ch/;%JAR_DIR" \
    --models DGIF_V3 \
    --smart2Inheritance \
    --nameByTopic \
    --defaultSrsAuth EPSG --defaultSrsCode 4326 \
    --strokeArcs \
    --createEnumTabs --createEnumTxtCol --beautifyEnumDispName \
    --createBasketCol --createTidCol --createStdCols --createMetaInfo \
    --createFk --createFkIdx \
    --createGeomIdx --gpkgMultiGeomPerTable
```

### 6.2 Inheritance Flattening with `--smart2Inheritance`

The `--smart2Inheritance` option (formally "NewAndSubClass" strategy) is critical for operational usability. It instructs `ili2gpkg` to **flatten the class hierarchy** such that each concrete (non-abstract) class receives its own table containing *all* inherited columns from its ancestor chain.

For example, the class `Cultural.Building` inherits from `Foundation.FeatureEntity` which extends `Foundation.Entity`. With smart inheritance, the `cultural_building` table contains:

| Column | Origin | Type |
|---|---|---|
| `T_Id` | ili2gpkg internal | INTEGER PRIMARY KEY |
| `T_Ili_Tid` | ili2gpkg Transfer-ID | TEXT |
| `T_basket` | ili2gpkg basket reference | INTEGER |
| `T_LastChange`, `T_CreateDate`, `T_User` | `--createStdCols` | TIMESTAMP / TEXT |
| `beginlifespanversion` | Entity | TEXT |
| `endlifespanversion` | Entity | TEXT |
| `externalreferences` | Entity | TEXT |
| `uniqueuniversalentityidentifier` | Entity | TEXT |
| `ageometry` | FeatureEntity (concrete) | POLYGON |
| `buildingcondition` | Building | TEXT |
| `buildingfunction` | Building | TEXT |
| `featurefunction` | Building | TEXT |
| `heightabovegroundlevel` | Building | REAL |
| ... | ... | ... |

This flattening produces a **single-table-per-class** layout with no JOINs required for feature retrieval — ideal for GIS visualisation and operational queries. The trade-off is schema size: the resulting GeoPackage contains **576 feature tables** and **131 attribute/structure tables**, totalling approximately 28 MB for the empty schema.

Note that the geometry column is named `ageometry` rather than `geometry`. This is an ili2gpkg convention to avoid conflicts with GeoPackage reserved column names.

### 6.3 Schema Characteristics

The generated GeoPackage has the following characteristics:

| Property | Value |
|---|---|
| File size (empty schema) | ~28 MB |
| Spatial Reference System | WGS 84 (EPSG:4326) |
| Feature tables | 576 |
| Attribute tables | 131 |
| Total tables (incl. metadata) | ~750 |
| Geometry types | POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON |
| Spatial indexes | R-tree per geometry column |
| Enumeration tables | Lookup tables for all INTERLIS enumerations |
| Metadata tables | `T_ILI2DB_CLASSNAME`, `T_ILI2DB_ATTRNAME`, `T_ILI2DB_BASKET`, `T_ILI2DB_DATASET`, `T_ILI2DB_MODEL`, etc. |

### 6.4 Conformance to the DGIWG GeoPackage Profile

The generated GeoPackage conforms to the DGIWG GeoPackage Profile (DGIWG 126, v1.1) through:

- **CRS:** WGS 84 (EPSG:4326) as mandated by the profile.
- **Content type:** Vector features registered in `gpkg_contents` with `data_type = 'features'` (spatial) or `'attributes'` (non-spatial).
- **Geometry encoding:** Standard GeoPackage binary format (GP header + Well-Known Binary).
- **Metadata:** The `gpkg_extensions`, `gpkg_geometry_columns`, and `gpkg_spatial_ref_sys` tables are populated per OGC specification.
- **Naming:** Table names follow `topic_class` convention (lowercase, underscore-separated) via `--nameByTopic`.

---

## 7. Cross-Domain Semantic Mapping

### 7.1 Mapping Methodology

A core contribution of this work is the systematic semantic mapping between heterogeneous geospatial data sources and the DGIF V3 schema. The mapping methodology follows a structured approach:

1. **Source schema analysis:** For each data source, the thematic domains, feature classes, attributes, and value domains are inventoried.

2. **DGFCD concept matching:** Each source feature class is matched to the most appropriate DGFCD Feature Concept, using the feature definition, FACC code, and attribute structure as matching criteria.

3. **Attribute alignment:** Source attributes are mapped to DGIF attributes, with value transformations where the source and target domains differ.

4. **Mapping classification:** Each mapping rule is classified as:
   - **OK** — direct semantic correspondence (e.g., OSM `amenity=hospital` → DGIF `Building` with `featureFunction=hospital`).
   - **Generalisation** — the source concept is more specific than the DGIF target (e.g., OSM `amenity=bicycle_rental` → DGIF `Facility`).
   - **not in DGIF** — no suitable DGIF class exists for the source concept.

5. **Cross-source convergence:** When multiple sources describe the same real-world entity type, the mappings must converge on identical DGIF attributes and values (see Section 7.5).

The mappings are stored as semicolon-delimited CSV files with a standardised column structure supporting up to two DGIF attribute/value pairs per rule.

### 7.2 OpenStreetMap ↔ DGIF V3

The OSM mapping table (`OSM_to_DGIF_V3.csv`) was produced by updating the existing V2 mapping (1,610 rows) to align with the V3 model changes. The update script (`build_osm_dgif_v3.py`, 526 lines) performs:

1. **Class renames V2 → V3:** Four classes were renamed between model versions:

   | V2 Name | V3 Name | Affected Rows |
   |---|---|---|
   | `MemorialMonument` | `Monument` | 9 |
   | `Route` | `LandRoute` | 11 |
   | `CaravanPark` | `CampSite` | 2 |
   | `ArchaeologicalSite` | `ArcheologicalSite` | 1 |

2. **Upgraded mappings:** Seven entries previously marked "not in DGIF" received new V3 mappings (e.g., `amenity=bicycle_rental` → `Facility`).

3. **47 new OSM tags:** Tags from 16 OSM categories (aeroway, amenity, boundary, highway, historic, landuse, leisure, military, natural, power, railway, shop, telecom, tourism, water, waterway) were added with corresponding DGIF mappings.

**Final statistics:**

| Metric | Value |
|---|---|
| Total mapping rules | 1,657 |
| OK (direct) mappings | 709 |
| Generalisation mappings | 729 |
| not in DGIF | 218 |
| DGIF classes covered | 158 / 673 (23.5%) |

The 515 unmapped DGIF classes are predominantly specialist domains (aviation procedures, maritime navigation aids, military-specific features) with no direct OSM equivalent.

### 7.3 swissTLM3D ↔ DGIF V3

The Swiss Topographic Landscape Model 3D (swissTLM3D) is the national topographic dataset of Switzerland, maintained by swisstopo [9]. It is modelled in INTERLIS 2.3 and uses LV95 (EPSG:2056) coordinates. The model comprises 7 thematic topics with approximately 32 concrete classes, each using an enumerative `Objektart` attribute to distinguish subtypes.

The mapping table (`swissTLM3D_to_DGIF_V3.csv`, 215 rows) was produced by `build_swisstlm3d_dgif_v3.py`, mapping every `(TLM_Class, Objektart)` combination to a DGIF V3 class:

| Metric | Value |
|---|---|
| Total mapping rules | 215 |
| OK (direct) mappings | 83 |
| Generalisation mappings | 125 |
| not in DGIF | 7 |
| DGIF classes covered | 93 / 673 (13.8%) |
| TLM Topics covered | 8 / 8 (100%) |

The seven unmapped elements are Swiss-specific infrastructure nodes (MISTRA nodes, topological nodes, toll stations) and a ventilation shaft sub-type with no DGIF equivalent.

### 7.4 Overture Maps ↔ DGIF V3

Overture Maps Foundation provides openly licensed, global geospatial data, structured in themes (transportation, places, buildings, etc.) with a schema based on GeoJSON/GeoParquet [10]. The mapping uses a 4-tuple key `(theme, type, subtype, class)` with a cascading fallback strategy:

1. Exact match: `(theme, type, subtype, class)`
2. Ignore subtype: `(theme, type, "", class)`
3. Ignore class: `(theme, type, subtype, "")`
4. Generic: `(theme, type, "", "")`

This fallback mechanism accommodates Overture's evolving schema, where new subtypes and classes are introduced between releases.

### 7.5 Cross-Source Semantic Alignment and Convergence Analysis

A critical quality criterion for the harmonisation framework is **cross-source convergence**: when different data sources describe the same real-world entity type, the resulting DGIF features must carry identical attribute values.

The harmonisation guide (documented in a separate reference document) defines convergence rules for key feature types. For example:

**Railways:**

| Source | Source Value | DGIF Attribute | DGIF Value |
|---|---|---|---|
| swissTLM3D | `Normalspur` | `railwayClass` | `mainLine` |
| Overture | `standard_gauge` | `railwayClass` | `mainLine` |
| swissTLM3D | `Schmalspur` | `railwayClass` | `branchLine` |
| Overture | `narrow_gauge` | `railwayClass` | `branchLine` |

**Roads:**

| Source | Source Value | DGIF Attribute | DGIF Value |
|---|---|---|---|
| swissTLM3D | `Autobahn` | `waySignificance` | `primaryWay` |
| Overture | `motorway` | `waySignificance` | `primaryWay` |
| swissTLM3D | `Autostrasse` | `waySignificance` | `primaryWay` |
| Overture | `trunk` | `waySignificance` | `primaryWay` |

Cross-source consistency can be verified with SQL queries against the populated GeoPackage:

```sql
SELECT railwayclass, railwayuse, COUNT(*)
FROM transportation_railway
GROUP BY railwayclass, railwayuse;
```

---

## 8. ETL Pipelines for Heterogeneous Geodata Harmonisation

### 8.1 Pipeline Architecture

Each data source follows a five-phase pipeline:

| Phase | Tool | Description |
|---|---|---|
| 1 — Acquire | Python | Download / locate source data |
| 2 — Validate | ilivalidator | Validate source against INTERLIS model (optional, non-blocking) |
| 3 — Schema | ili2gpkg | Create empty DGIF GeoPackage (`--schemaimport`) |
| 4 — Import | ili2gpkg / OGR | Import source data into temporary workspace |
| 5 — Transform | Python / OGR / sqlite3 | Apply mapping CSV, reproject, insert into DGIF GeoPackage |

Phase 5 is the core of the ETL, where semantic transformation, geometry coercion, and attribute mapping occur. The architecture is consistent across sources, with source-specific adapters for reading and key resolution.

### 8.2 swissTLM3D Pipeline

The swissTLM3D ETL (`etl_swisstlm3d_to_dgif.py` orchestrator + `etl_swisstlm3d_transform.py` transform, approximately 1,900 lines combined) processes data from the Swiss national topographic model.

**Input processing:**
- Source data is a ZIP archive containing 8 INTERLIS XTF files (~3.6 GB compressed, ~28 GB decompressed).
- Each XTF is imported into a temporary GeoPackage using `ili2gpkg --import` with LV95 (EPSG:2056) coordinates.

**Mapping CSV loading:**
The function `load_mapping()` reads the semicolon-delimited CSV and builds a dictionary keyed by `(TLM_class, Objektart_value)`, where each entry contains the DGIF target class and up to two attribute/value pairs. Rows marked "not in DGIF" are excluded.

**Table discovery:**
The `discover_dgif_tables()` function queries the ili2gpkg metadata table `T_ILI2DB_CLASSNAME` to resolve INTERLIS qualified names (e.g., `DGIF_V3.Cultural.Building`) to SQL table names (e.g., `cultural_building`). This indirection is necessary because `ili2gpkg` applies naming conventions (lowercase, topic prefix) that are not trivially predictable from the INTERLIS model alone.

**Feature transformation loop:**
For each TLM feature:

1. **Read** the feature from the temporary GeoPackage via OGR.
2. **Resolve** the `Objektart` enumeration value to select the appropriate mapping rule.
3. **Reproject** geometry from LV95 (EPSG:2056) to WGS 84 (EPSG:4326) using OGR's `CoordinateTransformation`.
4. **Flatten** 3D geometries to 2D (the DGIF model uses `Coord2`, `Line`, and `Surface` — all 2D types).
5. **Coerce** geometry type if necessary: if the source geometry is a polygon but the target DGIF class expects a point, extract the centroid.
6. **Encode** the geometry as GeoPackage binary (GP header + WKB) using a custom `to_gpkg_wkb()` function.
7. **Map** base attributes: `T_Id` (sequential), `T_Ili_Tid` (from source or generated UUID), `T_basket` (resolved per DGIF topic), `beginlifespanversion` (from `Datum_Erstellung`, normalised to ISO 8601), `uniqueuniversalentityidentifier`.
8. **Map** domain attributes: up to two attribute/value pairs from the CSV mapping rule.
9. **Fill** NOT NULL defaults: columns with NOT NULL constraints that are not populated by the mapping receive type-appropriate defaults (0 for INTEGER, `"unknown"` for TEXT).
10. **Insert** into the DGIF GeoPackage via `sqlite3` parameterised query.

**Performance optimisations:**
- SQLite is configured with WAL journal mode, `synchronous=NORMAL`, and a 64 MB page cache.
- Foreign key checks are disabled during bulk insertion.
- R-tree spatial index triggers (created by ili2gpkg and dependent on SpatiaLite functions) are dropped before insertion and rebuilt afterward.
- Spatial extents are tracked in Python and written to `gpkg_contents` post-insertion.

**Foundation metadata:**
The pipeline inserts 9 Foundation-topic records (SourceInfo, Organisation, ContactInfo, etc.) with static metadata referencing geocat.ch and swisstopo, providing provenance documentation for the dataset.

### 8.3 Overture Maps Pipeline

The Overture Maps ETL (`etl_overture_to_dgif.py` + `etl_overture_transform.py`, approximately 1,162 lines for the transform) follows the same five-phase architecture but adapts to the Overture data model:

**Key differences from swissTLM3D:**

| Aspect | swissTLM3D | Overture Maps |
|---|---|---|
| Source format | INTERLIS XTF (→ GeoPackage) | GeoParquet / GeoJSON |
| Mapping key | `(TLM_class, Objektart)` | `(theme, type, subtype, class)` |
| CRS handling | LV95 → WGS 84 reprojection | Already WGS 84, no reprojection |
| Mapping fallback | Exact match only | 4-level cascade (see Section 7.4) |
| Nested attributes | N/A | `names.primary`, `categories.primary` via `_extract_nested_primary()` |
| Feature ID | swissTLM3D `OID` | Overture `id` or generated UUID |
| Provenance | geocat.ch / swisstopo | Overture Maps Foundation |

**Handling Overture nested structures:**
Overture features contain nested JSON-like structures (e.g., `names.primary`, `categories.primary`) that GDAL may expose as Python dictionaries, JSON strings, or lists depending on the source format. The `_extract_nested_primary()` function handles all three representations transparently.

### 8.4 Geometry Handling: Type Coercion, 3D Flattening, Centroid Extraction

Geometry transformations are a critical aspect of the ETL, as source geometries do not always match the DGIF target type:

| Source Geometry | Target DGIF Type | Strategy |
|---|---|---|
| POINT | POINT | Direct (reproject if needed) |
| LINESTRING | LINESTRING | Direct (reproject if needed) |
| POLYGON | POLYGON/SURFACE | Direct (reproject if needed) |
| LINESTRING | POINT | Centroid extraction |
| POLYGON | POINT | Centroid extraction |
| POINT Z | POINT | Flatten to 2D (`FlattenTo2D()`) |
| LINESTRING Z | LINESTRING | Flatten to 2D |
| POLYGON Z | POLYGON | Flatten to 2D |
| POINT | POLYGON | Skip (no sensible upcast) |

The `reproject_geometry()` function chains these operations: clone → flatten to 2D → coordinate transformation → type coercion (centroid if needed).

### 8.5 Implementation Challenges

Several technical challenges were encountered during ETL implementation:

**R-tree trigger incompatibility.** `ili2gpkg` creates SQLite R-tree spatial index triggers that invoke SpatiaLite functions (`ST_IsEmpty`, `ST_MinX`, `ST_MaxX`, `ST_MinY`, `ST_MaxY`). These functions are not available in Python's built-in `sqlite3` module (which uses a vanilla SQLite without SpatiaLite). The solution: drop all R-tree triggers before bulk insertion, track spatial extents in Python, and manually rebuild R-tree indexes after insertion.

**NOT NULL constraint defaults.** Some DGIF classes have columns with NOT NULL constraints for attributes not covered by the mapping (e.g., `cabletype`, `surfacematerialtype`). The transform script auto-detects these constraints via `PRAGMA table_info()` and fills them with type-appropriate defaults when the CSV mapping does not provide a value.

**Table name resolution.** The mapping between INTERLIS qualified names and SQL table names is not trivial (e.g., `DGIF_V3.Cultural.Building` → `cultural_building`). The pipeline resolves this through the `T_ILI2DB_CLASSNAME` metadata table populated by `ili2gpkg`, which provides an authoritative INTERLIS-to-SQL name mapping.

**Basket and dataset management.** ili2gpkg's data model requires each inserted feature to reference a *basket* (a container corresponding to an INTERLIS TOPIC instance) and a *dataset*. The pipeline creates one dataset per source and one basket per DGIF topic, linking features correctly via `T_basket` foreign keys to the `T_ILI2DB_BASKET` metadata table.

**Geometry column naming.** The DGIF INTERLIS model declares the attribute as `geometry`, but ili2gpkg renames it to `ageometry` to avoid conflicts with GeoPackage reserved column names. All ETL code uses `ageometry` for geometry insertion.

---

## 9. Evaluation and Results

### 9.1 Model Completeness

The generated INTERLIS model covers the full scope of the DGIF 3.0 Baseline 2025-1:

| Metric | DGIM UML Model | INTERLIS Output | Coverage |
|---|---|---|---|
| Classes | 673 | 673 | 100% |
| Topics | 21 | 21 | 100% |
| Associations | 64 | 64 | 100% |
| Geometry-bearing classes | 511 | 511 | 100% |
| Non-spatial classes | 162 | 162 | 100% |
| Compiler errors | — | 0 | N/A |

All UML elements are represented in the INTERLIS output. The transformation preserves semantic fidelity: class names, attribute names, cardinalities, inheritance hierarchies, and associations are maintained. Geometry types resolved from OCL constraints match the DGIM's intent (verified by manual review of a 10% sample of classes).

### 9.2 Mapping Coverage and Gap Analysis

**Across data sources:**

| Source | Total Rules | OK | Generalisation | not in DGIF | DGIF Classes Covered |
|---|---|---|---|---|---|
| OSM | 1,657 | 709 (42.8%) | 729 (44.0%) | 218 (13.2%) | 158 (23.5%) |
| swissTLM3D | 215 | 83 (38.6%) | 125 (58.1%) | 7 (3.3%) | 93 (13.8%) |
| Overture | var. | var. | var. | var. | var. |

The predominance of "Generalisation" mappings (44–58%) reflects a fundamental difference in granularity: the DGIF schema is finer-grained than typical civilian data sources. For instance, the DGIF distinguishes between `Monument`, `Memorial`, and `HistoricSite`, while OSM uses a single `historic=monument` tag for all three.

**Gap analysis:** The 515 DGIF classes not covered by OSM are concentrated in:
- Aeronautical Information (instrument procedures, navaids): ~120 classes
- Maritime Navigation (buoys, lights, channels): ~80 classes
- Military-specific features (obstacles, installations): ~60 classes
- Foundation/metadata classes: ~50 classes
- Specialist physiography (bathymetry, ice features): ~40 classes

These gaps are expected: civilian data sources do not capture military, aeronautical, or maritime information at the level of detail defined by DGIWG.

### 9.3 ETL Performance

**swissTLM3D single-tile test (SWISSTLM3D_CHLV95LN02.xtf, 21.9 MB):**

| Metric | Value |
|---|---|
| Total features inserted | 5,351 |
| Features skipped (not in DGIF) | 0 (filtered by CSV) |
| No Objektart match | 748 (TLM_STRASSENINFO nodes) |
| TLM classes not found in GeoPackage | 2 |
| Insert errors | 0 |
| DGIF tables populated | 40 |
| Output GeoPackage size | ~29 MB |
| Transform time (Phase 5 only) | ~3 seconds |
| WGS 84 extent | (8.62°, 46.16°) – (8.87°, 46.40°) |

The zero insert errors and 100% success rate on matched features demonstrate the robustness of the mapping and transformation logic. The 748 unmatched TLM_STRASSENINFO entries correspond to topological nodes and Swiss-specific infrastructure identifiers (MISTRA nodes) that have no DGIF equivalent — a known and documented gap.

### 9.4 Cross-Source Consistency Validation

Cross-source consistency was verified using SQL queries against populated GeoPackages from both swissTLM3D and Overture Maps sources:

```sql
-- Verify railway class convergence
SELECT railwayclass, COUNT(*) as n
FROM transportation_railway
GROUP BY railwayclass;
```

Both sources produce features with identical `railwayclass` values (`mainLine`, `branchLine`) for semantically equivalent input features, confirming the convergence of the mapping methodology.

Additional validation queries verify:

- **Date format consistency:** All `beginlifespanversion` values conform to ISO 8601 (`YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SSZ`).
- **Domain value conformance:** All attribute values belong to the DGFCD-defined domain.
- **Geometry type correctness:** All `ageometry` values in each table match the expected geometry type registered in `gpkg_geometry_columns`.
- **Referential integrity:** All `T_basket` values reference valid entries in `T_ILI2DB_BASKET`.

### 9.5 Comparison with Existing DGIF Implementations

To our knowledge, this is the first publicly documented implementation of the DGIF 3.0 model as a GeoPackage database generated from an intermediate formal data description language (INTERLIS). The existing DGIF encoding specification (DGIWG 208-3) targets GML, which:

- Produces verbose XML files ill-suited for direct GIS visualisation.
- Requires specialised GML parsers and does not support random-access queries.
- Lacks the spatial indexing and SQLite portability of GeoPackage.

The INTERLIS-based approach offers several advantages: (1) the model is compiler-verified before any data is produced; (2) the same `.ili` model can generate GeoPackage, PostGIS, Oracle Spatial, and GML outputs via `ili2db`; (3) data validation at any stage via `ilivalidator`; and (4) the GeoPackage output is directly usable in QGIS, ArcGIS, and other standard GIS tools.

---

## 10. Discussion

### 10.1 Benefits of the Model-Driven INTERLIS Approach

The central thesis validated by this work is that the INTERLIS ecosystem provides an effective "implementation bridge" between the DGIF's conceptual UML model and operationally usable geospatial databases. Key benefits include:

**Formal verification.** The INTERLIS compiler catches structural errors — undefined type references, invalid cardinalities, circular dependencies — before any data is produced. This is particularly valuable for a model of DGIF's scale (673 classes), where manual review is impractical.

**Multi-target generation.** The single `DGIF_V3.ili` model can generate GeoPackage (this work), PostGIS, Oracle Spatial, and INTERLIS XTF outputs without modification. This eliminates the need for parallel maintenance of encoding specifications for different target platforms.

**Standardised validation.** The `ilivalidator` tool can validate both the model and any data produced against it, providing a formal conformance testing mechanism that goes beyond schema validation (e.g., checking value domains, cardinality constraints, and geometric topology rules).

**Swiss NSDI integration.** As INTERLIS is the legally mandated format for Swiss geodata, the DGIF INTERLIS model integrates natively with the Swiss National Spatial Data Infrastructure (NGDI), model repositories, and cadastral data exchange workflows.

### 10.2 Limitations

**Semantic loss from BAG OF flattening.** The decision to eliminate all multi-valued attributes (BAG OF) simplifies the schema but prevents representing features with genuinely multi-valued attributes (e.g., a feature with multiple external references). In practice, this limitation has not manifested in the ETL pipelines, but it may become relevant for richer data sources.

**Forward reference limitations.** The INTERLIS language requires classes to be declared before they are referenced. For cyclic cross-topic references, the pipeline emits forward references as comments (`!!`), which means these associations are present in the model documentation but not enforced by the compiler.

**Mapping subjectivity.** The "Generalisation" mapping category (44–58% of rules) inherently involves subjective judgement about the best-fit DGIF class for a source concept. While guidelines are documented, different domain experts might produce different generalisation mappings.

**NOT NULL default values.** The ETL pipeline fills NOT NULL columns with type-appropriate defaults (0 for INTEGER, "unknown" for TEXT) when no mapping value is available. These synthetic values are indistinguishable from real data unless explicitly flagged — a potential data quality concern.

**Scope of evaluation.** The ETL pipeline has been evaluated on a single swissTLM3D tile (21.9 MB, 5,351 features). Full-scale evaluation on the complete swissTLM3D dataset (~28 GB) would provide more robust performance metrics and potentially uncover edge cases not present in the test tile.

### 10.3 Lessons Learned: Bridging NATO/DGIWG Standards and Swiss NSDI

Several practical lessons emerged from this implementation:

1. **Geometry column naming conventions differ** between INTERLIS/ili2gpkg and OGC GeoPackage expectations. The automatic renaming of `geometry` to `ageometry` required attention throughout the ETL.

2. **Metadata table awareness is essential.** The ili2gpkg metadata tables (`T_ILI2DB_CLASSNAME`, `T_ILI2DB_ATTRNAME`, `T_ILI2DB_BASKET`) are the authoritative source for resolving names and relationships. Hardcoding table or column names is fragile and error-prone.

3. **SpatiaLite dependency management.** The incompatibility between ili2gpkg's SpatiaLite-dependent R-tree triggers and Python's vanilla SQLite required workaround code for trigger management and manual index rebuilding.

4. **Coordinate reference system discipline.** The DGIF mandates WGS 84, while Swiss national data uses LV95. Every geometry in the pipeline passes through explicit reprojection — no implicit CRS assumptions.

5. **Model evolution tracking.** Between DGIF V2 and V3, four classes were renamed and several were restructured. Automated update scripts (Section 7.2) proved valuable for maintaining mapping tables across model versions.

### 10.4 Generalisability to Other Defence or National SDI Contexts

While this implementation targets the Swiss NSDI, the approach is generalisable:

- **Other INTERLIS-using countries** (e.g., Austria, Liechtenstein) can directly reuse the DGIF_V3.ili model.
- **Non-INTERLIS environments** can still benefit from the mapping methodology and ETL architecture. The ili2gpkg GeoPackage output is universally readable.
- **Other national datasets** (e.g., OS MasterMap UK, ATKIS Germany, BD TOPO France) could be mapped to DGIF using the same CSV-based methodology with source-adapted ETL scripts.
- **INSPIRE alignment** is a natural extension: the DGIF and INSPIRE share many thematic domains, and a DGIF-to-INSPIRE mapping layer could leverage the existing catalogues.

---

## 11. Conclusion and Future Work

### 11.1 Summary of Contributions

This paper has presented *ili2dgim*, an end-to-end, fully automated pipeline that bridges the gap between the DGIF 3.0 UML conceptual model and operationally usable geospatial databases. The pipeline:

1. **Transforms** the DGIM UML model (673 classes, 64 associations) into a compiler-verified INTERLIS 2.4 model (7,782 lines, 0 errors) with geometry types resolved from OCL constraints.

2. **Generates** a GeoPackage schema (576 feature tables, WGS 84) conforming to the DGIWG profile, using smart inheritance flattening for operational usability.

3. **Extracts** the DGFCD and DGRWI concept dictionaries as 7 INTERLIS XML catalogue files, enabling machine-readable access to the DGIF vocabulary.

4. **Maps** three heterogeneous data sources (OSM: 1,657 rules; swissTLM3D: 215 rules; Overture Maps) to the DGIF schema through a systematic methodology with documented cross-source convergence rules.

5. **Populates** the DGIF GeoPackage with 5,351+ real-world features via automated ETL pipelines, handling coordinate reprojection, geometry coercion, and attribute domain validation.

The work demonstrates that the Swiss INTERLIS ecosystem — with its formal model verification, multi-target generation, and standardised validation — provides an effective implementation platform for NATO/DGIWG geospatial standards.

### 11.2 Towards Broader Data Model Integration

The natural next step is to deepen the integration with additional open and community-driven geospatial data models. While national topographic datasets are valuable, the greatest potential for scalable DGIF population lies in globally available, continuously updated data ecosystems:

- **OpenStreetMap (OSM):** The current 1,657-rule mapping covers 23.5% of DGIF classes. Expanding coverage to specialist OSM tagging schemas — such as `seamark:*` for maritime features, `aeroway:*` for aeronautical infrastructure, and `military:*` for defence-relevant objects — could significantly increase the proportion of DGIF classes addressable from OSM alone. Furthermore, integrating OSM's rich attribute vocabulary (e.g., `maxspeed`, `lanes`, `surface`) into the mapping would improve attribute completeness beyond the current two-attribute-per-rule limit.

- **Overture Maps Foundation:** As Overture's schema evolves (new themes, subtypes, and attribute structures are added with each quarterly release), the mapping must track these changes systematically. The 4-level cascading fallback mechanism (Section 7.4) provides resilience, but a schema-diff tool that automatically detects new Overture categories and proposes candidate DGIF mappings would accelerate maintenance.

- **Additional open data models:** Google Open Buildings, Microsoft Building Footprints, ESA WorldCover (land use/land cover), and OpenAddresses represent complementary data sources that could fill specific DGIF domains (buildings, land cover, addresses) with high-quality, globally consistent data. Each would require a source-specific mapping CSV and ETL adapter following the established pipeline architecture.

- **INSPIRE data models:** The INSPIRE Directive defines harmonised data models for 34 spatial data themes across Europe. A systematic DGIF-to-INSPIRE mapping layer could leverage the existing DGFCD catalogues and enable bidirectional data exchange between NATO/DGIWG and European SDI contexts.

The overarching goal is to evolve *ili2dgim* from a pipeline targeting individual data sources into a **multi-source harmonisation framework** where any geospatial dataset with a documented schema can be mapped to DGIF through a standardised CSV-based methodology and pluggable ETL adapters.

### 11.3 Continuous Synchronisation with Evolving Source Data and DGIM Versions

The pipeline's automation enables continuous synchronisation:

- When DGIWG publishes a new DGIF baseline, the XMI file is replaced and the pipeline re-run, producing an updated INTERLIS model and GeoPackage schema automatically.
- When source data is updated (e.g., new swissTLM3D quarterly release), only the ETL phases need re-execution.
- Mapping tables can be versioned and diffed between DGIF releases, with automated update scripts (as demonstrated for the V2→V3 transition) minimising manual effort.

Future work will explore CI/CD integration (automated pipeline execution on model or data changes) and model repository publication (hosting the `DGIF_V3.ili` model on the Swiss INTERLIS model repository at `models.interlis.ch`).

---

## References

[1] DGIWG, "Defence Geospatial Information Framework (DGIF) — Overview," DGIWG 200-3, Edition 3.0, July 2024. [Online]. Available: https://dgiwg.org/documents/dgiwg-standards/200

[2] Swiss Association for Standardization (SNV), "INTERLIS 2 — A Data Description Language for Geo-Data," SN 612030, 2006.

[3] eCH, "eCH-0031 INTERLIS 2 — Geo-Datenmodellierung," Version 2.1.0, 2024. [Online]. Available: https://www.ech.ch/de/ech/ech-0031

[4] Open Geospatial Consortium, "OGC GeoPackage Encoding Standard," OGC 12-128r17, Version 1.3.1, 2021. [Online]. Available: https://www.geopackage.org/

[5] DGIWG, "DGIWG GeoPackage Profile," DGIWG 126, Edition 1.1, May 2025. [Online]. Available: https://dgiwg.org/documents/dgiwg-standards/200

[6] interactive instruments GmbH, "ShapeChange — Processing Application Schemas," 2024. [Online]. Available: https://shapechange.net/

[7] European Commission, "INSPIRE — Infrastructure for Spatial Information in the European Community," Directive 2007/2/EC, 2007. [Online]. Available: https://inspire.ec.europa.eu/

[8] wetransform GmbH, "HALE Studio — The Humboldt Alignment Editor," 2024. [Online]. Available: https://www.wetransform.to/products/halestudio/

[9] Federal Office of Topography swisstopo, "swissTLM3D — Swiss Topographic Landscape Model," 2024. [Online]. Available: https://www.swisstopo.admin.ch/en/landscape-model-swisstlm3d

[10] Overture Maps Foundation, "Overture Maps — Open Map Data," 2024. [Online]. Available: https://overturemaps.org/

[11] DGIWG, "Defence Geospatial Information Model (DGIM)," DGIWG 205-3, Edition 3.0, July 2024.

[12] DGIWG, "Defence Geospatial Feature Concept Dictionary (DGFCD)," DGIWG 206-3, Edition 3.0, July 2024.

[13] DGIWG, "Defence Geospatial Real World Object Index (DGRWI)," DGIWG 207-3, Edition 3.0, July 2024.

[14] DGIWG, "Defence Geospatial Encoding Specification — Part 1: GML," DGIWG 208-3, Edition 3.0, July 2024.

[15] C. Eisenhut, "ili2db — INTERLIS to Database," GitHub, 2024. [Online]. Available: https://github.com/claeis/ili2db

[16] C. Eisenhut, "ili2c — INTERLIS Compiler," GitHub, 2024. [Online]. Available: https://github.com/claeis/ili2c

[17] C. Eisenhut, "ilivalidator — INTERLIS Data Validator," GitHub, 2024. [Online]. Available: https://github.com/claeis/ilivalidator

[18] Swiss Confederation, "Federal Act on Geoinformation (Geoinformation Act, GeoIA)," SR 510.62, 2007. [Online]. Available: https://www.fedlex.admin.ch/eli/cc/2008/388/en

[19] S. Keller and M. Salvini, "INTERLIS — A Standard for Geo-Data Modelling and Exchange in Switzerland," in *Proceedings of the ISPRS Workshop on Spatial Data Quality*, 2003.

[20] ISO/TC 211, "ISO 19109:2015 — Geographic information — Rules for application schema," International Organization for Standardization, 2015.

---

## Appendix A — INTERLIS 2.4 Model Excerpt

```interlis
INTERLIS 2.4;

MODEL DGIF_V3 (en)
  AT "https://www.dgiwg.org/dgif"
  VERSION "2025-1" =

  IMPORTS Units;

  DOMAIN
    Coord2 = COORD -180.000 .. 180.000 [Units.Angle_Degree],
                    -90.000 .. 90.000 [Units.Angle_Degree],
                    ROTATION 2 -> 1;

    Coord3 = COORD -180.000 .. 180.000 [Units.Angle_Degree],
                    -90.000 .. 90.000 [Units.Angle_Degree],
                    -10000.000 .. 100000.000 [INTERLIS.m],
                    ROTATION 2 -> 1;

    Line = POLYLINE WITH (STRAIGHTS, ARCS)
           VERTEX Coord2;

    Surface = SURFACE WITH (STRAIGHTS, ARCS)
              VERTEX Coord2
              WITHOUT OVERLAPS > 0.001;

  TOPIC Foundation =

    CLASS Entity (ABSTRACT) =
      OID TEXT*36;
      beginLifespanVersion : MANDATORY FORMAT INTERLIS.XMLDate;
      endLifespanVersion : FORMAT INTERLIS.XMLDate;
      externalReferences : TEXT*1024;
      specifiedDomainValues : TEXT*4096;
      uniqueUniversalEntityIdentifier : MANDATORY TEXT*36;
    END Entity;

    CLASS FeatureEntity (ABSTRACT) EXTENDS Entity =
      !! geometry placeholder — concrete classes declare typed geometry
    END FeatureEntity;

    !! ... additional Foundation classes ...

  END Foundation;

  TOPIC Cultural =
    DEPENDS ON Foundation;

    CLASS Building EXTENDS Foundation.FeatureEntity =
      geometry : MANDATORY DGIF_V3.Surface;
      buildingCondition : TEXT*255;
      buildingFunction : TEXT*255;
      featureFunction : TEXT*255;
      heightAboveGroundLevel : 0.000 .. 99999999.999;
      !! ... additional attributes ...
    END Building;

    !! ... additional Cultural classes ...

  END Cultural;

  !! ... 19 additional TOPICs ...

END DGIF_V3.
```

---

## Appendix B — UML → INTERLIS Transformation Rule Table

| # | UML Source Element | INTERLIS Target | Rule |
|---|---|---|---|
| R1 | Root package `DGIM` | `MODEL DGIF_V3 (en) VERSION "2025-1"` | One model per XMI |
| R2 | Sub-package | `TOPIC <name>` | Package name preserved |
| R3 | `uml:Class` | `CLASS <name> (FINAL) = OID TEXT*36;` | UUID OID, FINAL by default |
| R4 | `uml:Class` (abstract) | `CLASS <name> (ABSTRACT) =` | isAbstract flag |
| R5 | `uml:Generalization` | `EXTENDS <superclass>` | Same-topic only |
| R6 | Cross-topic generalization | `EXTENDS <Topic>.<Class>` | With `DEPENDS ON` |
| R7 | `ownedAttribute` [1..1] | `<name> : MANDATORY <type>;` | MANDATORY for lower=1 |
| R8 | `ownedAttribute` [0..1] | `<name> : <type>;` | Optional |
| R9 | `ownedAttribute` [0..*] | `<name> : <type>;` | BAG OF eliminated, single value |
| R10 | `uml:Enumeration` | `<name> : (val1, val2, ...);` | Inline enumeration |
| R11 | `uml:DataType` → `INTERLIS_TYPE_MAP` | Base INTERLIS type | Dictionary lookup |
| R12 | `uml:DataType` → Foundation STRUCTURE | `STRUCTURE <name>` | Complex type |
| R13 | `uml:Association` | `ASSOCIATION <name> =` | With role cardinalities |
| R14 | OCL `oclIsKindOf(PointGeometryInfo)` | `geometry : MANDATORY Coord2;` | Per-class resolution |
| R15 | OCL `oclIsKindOf(CurveGeometryInfo)` | `geometry : MANDATORY Line;` | Per-class resolution |
| R16 | OCL `oclIsKindOf(SurfaceGeometryInfo)` | `geometry : MANDATORY Surface;` | Per-class resolution |
| R17 | Attribute name contains "Angle" | `0.000 .. 360.000 [Units.Angle_Degree]` | Unit-annotated |
| R18 | Cross-topic REFERENCE TO | `<role> (EXTERNAL) -- {0..1} <Topic>.<Class>;` | EXTERNAL qualifier |
| R19 | Forward reference (unresolvable) | `!! <reference text>` | INTERLIS comment |

---

## Appendix C — Mapping CSV Schema and Sample Rows

**Column structure (swissTLM3D example):**

| Col | Name | Description |
|---|---|---|
| 0 | NO | Sequential number |
| 1 | TLM Topic | INTERLIS topic (`TLM_AREALE`, `TLM_BAUTEN`, ...) |
| 2 | TLM Feature Class | swissTLM3D class (`TLM_FREIZEITAREAL`, ...) |
| 3 | TLM Attribute (Objektart) | Always `Objektart` |
| 4 | TLM Attribute Value | Enum value (`Campingplatzareal`, ...) |
| 5 | Geometry | `Point` / `Line` / `Polygon` / empty |
| 6 | Mapping Description | `OK` / `Generalization` / `not in DGIF` |
| 7 | DGIF Feature Alpha | DGIF class name (`CampSite`, ...) |
| 8 | DGIF Feature 531 | FACC code (`AK060`, ...) |
| 9 | DGIF Attribute Alpha | 1st DGIF attribute (`campSiteType`, ...) |
| 10 | DGIF Attribute 531 | 1st attribute FACC code |
| 11 | DGIF AttributeValue Alpha | 1st attribute value |
| 12 | DGIF Value 531 | 1st value FACC code |
| 13–16 | *(same as 9–12)* | 2nd attribute/value pair (optional) |

**Sample rows:**

```
1;TLM_AREALE;TLM_FREIZEITAREAL;Objektart;Campingplatzareal;Polygon;OK;CampSite;AK060;;;;;;;;
2;TLM_AREALE;TLM_FREIZEITAREAL;Objektart;Golfplatzareal;Polygon;OK;GolfCourse;AK100;;;;;;;;
3;TLM_BAUTEN;TLM_GEBAEUDE_FOOTPRINT;Objektart;Gebaeude;Polygon;OK;Building;AL013;;;;;;;;
```

---

## Appendix D — SQL Validation Queries for Cross-Source Consistency

```sql
-- D.1 Verify railway class convergence across data sources
SELECT railwayclass, railwayuse, COUNT(*) as feature_count
FROM transportation_railway
GROUP BY railwayclass, railwayuse
ORDER BY feature_count DESC;

-- D.2 Verify road significance convergence
SELECT waysignificance, COUNT(*) as feature_count
FROM transportation_landtransportationway
GROUP BY waysignificance
ORDER BY feature_count DESC;

-- D.3 Check ISO 8601 date format compliance
SELECT T_Ili_Tid, beginlifespanversion
FROM transportation_railway
WHERE beginlifespanversion NOT LIKE '____-__-__%'
LIMIT 20;

-- D.4 Detect out-of-domain attribute values
SELECT DISTINCT waysignificance
FROM transportation_landtransportationway
WHERE waysignificance NOT IN (
    'primaryWay', 'secondaryWay', 'tertiaryWay',
    'localWay', 'minorWay', 'intraUrbanAccessWay',
    'intraUrbanInterconnectionWay', 'noInformation'
);

-- D.5 Verify geometry type correctness per table
SELECT gc.table_name, gc.geometry_type_name,
       COUNT(*) as feature_count
FROM gpkg_geometry_columns gc
JOIN gpkg_contents c ON gc.table_name = c.table_name
WHERE c.data_type = 'features'
GROUP BY gc.table_name, gc.geometry_type_name;

-- D.6 Verify referential integrity of basket references
SELECT r.T_basket, COUNT(*) as orphan_count
FROM transportation_railway r
LEFT JOIN T_ILI2DB_BASKET b ON r.T_basket = b.T_Id
WHERE b.T_Id IS NULL
GROUP BY r.T_basket;

-- D.7 Feature count per DGIF topic
SELECT b.topic, COUNT(*) as total_features
FROM transportation_railway r
JOIN T_ILI2DB_BASKET b ON r.T_basket = b.T_Id
GROUP BY b.topic
ORDER BY total_features DESC;
```

---

*Manuscript prepared: April 2026*
