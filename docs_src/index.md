# ili2dgim Documentation

This home page gives a simple entry point into a complex standards stack. If you are new to DGIF, read the next section first and only then move into the deeper technical pages.

This documentation describes three areas of the project:

- The ili2dgim principles and modeling approach.
- The web application and backend orchestration API.
- The Python automation scripts used to build model artifacts and run ETL pipelines.

## DGIF foundation for newcomers

**DGIF is not one document.** It is a family of DGIWG standards that work together:

1. **Overview**: explains the whole framework and conformance logic.
2. **DGIM**: the conceptual UML model of feature classes, attributes, and relationships.
3. **DGFCD**: the controlled vocabulary for feature concepts, attributes, values, roles, and units.
4. **DGRWI**: the index that connects real-world things to DGFCD concepts.
5. **Encoding specifications**: rules for exchanging DGIF data, including GML.
6. **GeoPackage profile**: practical storage rules when you want a database-style delivery format.

In simple terms: **DGIM tells you what the world looks like, DGFCD tells you which names and values are allowed, and the encoding/profile documents tell you how to package that information for exchange.**

!!! note

    Use current editions. The DGIWG standards catalog can list several revisions of the same family. For this project, start with the DGIF 3.0 documents published in July 2024 and the GeoPackage Profile 1.4 Ed.1.1.1 published in March 2026. Older DGIF generations and older GeoPackage profile revisions should be treated as legacy references unless you are maintaining an older implementation.

### Official DGIWG entry point

Browse the official catalog here: [DGIWG Standards](https://dgiwg.org/documents/dgiwg-standards).

The links below point both to the catalog origin page and to the current PDF file published by DGIWG.

| Document | Why it matters | Links |
| --- | --- | --- |
| DGIWG 200: DGIF Overview | Big picture, scope, and conformance classes. | [Origin page](https://dgiwg.org/documents/dgiwg-standards/200) and [PDF](https://portal.dgiwg.org/files/68061) |
| DGIWG 205: DGIM | Conceptual model of classes, inheritance, and semantics. | [Origin page](https://dgiwg.org/documents/dgiwg-standards/205) and [PDF](https://portal.dgiwg.org/files/68062) |
| DGIWG 206: DGFCD | Official dictionary of feature concepts, attributes, values, roles, and units. | [Origin page](https://dgiwg.org/documents/dgiwg-standards/206) and [PDF](https://portal.dgiwg.org/files/68063) |
| DGIWG 207: DGRWI | Index linking real-world objects to DGFCD concepts. | [Origin page](https://dgiwg.org/documents/dgiwg-standards/207) and [PDF](https://portal.dgiwg.org/files/68064) |
| DGIWG 208: DGIF Encoding Specification - Part 1: GML | Normative exchange encoding for DGIF data. | [Origin page](https://dgiwg.org/documents/dgiwg-standards/208) and [PDF](https://portal.dgiwg.org/files/68065) |
| DGIWG 126: GeoPackage Profile 1.4 Ed.1.1.1 | Current DGIWG GeoPackage profile for operational database delivery. | [Origin page](https://dgiwg.org/documents/dgiwg-standards/126) and [PDF](https://portal.dgiwg.org/files/76055) |

**Where ili2dgim fits:** this project takes DGIF conceptual content and turns it into a usable implementation stack based on INTERLIS, GeoPackage, mapping tables, and ETL pipelines.

## What ili2dgim delivers

The project converts DGIM/DGIF UML content into an operational INTERLIS-based delivery stack:

1. INTERLIS model generation (`DGIF_V3.ili`).
2. GeoPackage schema generation (DGIF-aligned target DB).
3. Mapping table generation for OSM, swissTLM3D, and Overture.
4. ETL pipelines that ingest source data and populate DGIF target layers.

## Documentation map

- Start with [Principles](principles.md) for conceptual and architectural foundations.
- Continue with [Webapp Overview](webapp.md) for frontend/backend operator workflows.
- Use [Backend API](backend-api.md) as endpoint reference for integrations.
- Use [Scripts](scripts.md) as command and option reference for pipeline automation.

## Quick start for docs (MkDocs Material)

Install docs dependencies:

```bash
pip install -r docs/requirements-docs.txt
```

Then run locally from repository root:

```bash
mkdocs serve
```

Build static docs:

```bash
mkdocs build
```