# ili2dgim Dokumentation

Diese Startseite ist ein einfacher Einstieg in einen komplexen Normenstapel. Wenn DGIF fuer Sie neu ist, lesen Sie zuerst diese Einfuehrung und gehen Sie danach in die technischen Detailseiten.

Diese Dokumentation beschreibt drei Projektbereiche:

- Die ili2dgim-Prinzipien und den Modellierungsansatz.
- Die Web-Anwendung und die Backend-Orchestrierungs-API.
- Die Python-Automationsskripte zum Erzeugen von Modellartefakten und zum Ausfuehren von ETL-Pipelines.

## DGIF-Grundlagen fuer Einsteiger

**DGIF ist nicht nur ein Dokument.** Es ist eine Familie von DGIWG-Standards, die zusammenarbeiten:

1. **Overview**: erklaert das Gesamtbild und die Konformitaetslogik.
2. **DGIM**: das konzeptionelle UML-Modell mit Klassen, Attributen und Beziehungen.
3. **DGFCD**: das kontrollierte Vokabular fuer Konzepte, Attribute, Werte, Rollen und Einheiten.
4. **DGRWI**: der Index, der reale Objekte mit DGFCD-Konzepten verbindet.
5. **Encoding Specifications**: Regeln fuer den Austausch von DGIF-Daten, einschliesslich GML.
6. **GeoPackage-Profil**: praktische Speicherregeln fuer eine datenbankartige Lieferform.

Kurz gesagt: **DGIM beschreibt die Welt, DGFCD beschreibt die erlaubten Namen und Werte, und die Encoding-/Profil-Dokumente beschreiben, wie diese Informationen fuer den Austausch verpackt werden.**

!!! note

    Verwenden Sie aktuelle Ausgaben. Fuer dieses Projekt sollten Sie mit den DGIF-3.0-Dokumenten von Juli 2024 und mit dem GeoPackage Profile 1.4 Ed.1.1.1 von Maerz 2026 beginnen. Aeltere Revisionen sind nur Legacy-Referenzen.

### Offizieller DGIWG-Einstieg

Offizieller Katalog: [DGIWG Standards](https://dgiwg.org/documents/dgiwg-standards).

| Dokument | Warum es wichtig ist | Links |
| --- | --- | --- |
| DGIWG 200: DGIF Overview | Gesamtbild, Geltungsbereich und Konformitaet. | [Herkunftsseite](https://dgiwg.org/documents/dgiwg-standards/200) und [PDF](https://portal.dgiwg.org/files/68061) |
| DGIWG 205: DGIM | Konzeptionelles Modell der Klassen, Vererbung und Semantik. | [Herkunftsseite](https://dgiwg.org/documents/dgiwg-standards/205) und [PDF](https://portal.dgiwg.org/files/68062) |
| DGIWG 206: DGFCD | Offizielles Woerterbuch fuer Konzepte, Attribute, Werte, Rollen und Einheiten. | [Herkunftsseite](https://dgiwg.org/documents/dgiwg-standards/206) und [PDF](https://portal.dgiwg.org/files/68063) |
| DGIWG 207: DGRWI | Index fuer die Verbindung zwischen realen Objekten und Konzepten. | [Herkunftsseite](https://dgiwg.org/documents/dgiwg-standards/207) und [PDF](https://portal.dgiwg.org/files/68064) |
| DGIWG 208: DGIF Encoding Specification - Part 1: GML | Normative Kodierung fuer DGIF-Austausch. | [Herkunftsseite](https://dgiwg.org/documents/dgiwg-standards/208) und [PDF](https://portal.dgiwg.org/files/68065) |
| DGIWG 126: GeoPackage Profile 1.4 Ed.1.1.1 | Aktuelles GeoPackage-Profil fuer operative Datenbanklieferung. | [Herkunftsseite](https://dgiwg.org/documents/dgiwg-standards/126) und [PDF](https://portal.dgiwg.org/files/76055) |

**Rolle von ili2dgim:** Dieses Projekt macht aus konzeptionellem DGIF-Inhalt einen nutzbaren Implementierungsstapel mit INTERLIS, GeoPackage, Mapping-Tabellen und ETL-Pipelines.

## Was ili2dgim liefert

1. Erzeugung des INTERLIS-Modells (`DGIF_V3.ili`).
2. Erzeugung des GeoPackage-Schemas.
3. Erzeugung von Mapping-Tabellen fuer OSM, swissTLM3D und Overture.
4. ETL-Pipelines, die Quelldaten in DGIF-Ziellayer laden.

## Dokumentationskarte

- Beginnen Sie mit [Principles](principles.md) fuer die konzeptionellen und architektonischen Grundlagen.
- Fahren Sie mit [Webapp Overview](webapp.md) fuer die Operator-Workflows fort.
- Verwenden Sie [Backend API](backend-api.md) als Integrationsreferenz.
- Verwenden Sie [Scripts](scripts.md) als Referenz fuer Befehle und Optionen.

## Schnellstart

```bash
pip install -r docs/requirements-docs.txt
mkdocs serve
mkdocs build
```

Technische Seiten ohne Uebersetzung fallen automatisch auf die englische Version zurueck.