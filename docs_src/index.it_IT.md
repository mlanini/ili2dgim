# Documentazione ili2dgim

Questa pagina iniziale offre un ingresso semplice in un insieme di standard complesso. Se DGIF e nuovo per te, leggi prima questa introduzione e solo dopo passa alle pagine tecniche piu dettagliate.

Questa documentazione descrive tre aree del progetto:

- I principi di ili2dgim e l'approccio di modellazione.
- L'applicazione web e l'API di orchestrazione del backend.
- Gli script Python che costruiscono gli artefatti del modello ed eseguono le pipeline ETL.

## Fondamenti DGIF per chi inizia

**DGIF non e un singolo documento.** E una famiglia di standard DGIWG che lavorano insieme:

1. **Overview**: spiega il quadro generale e la logica di conformita.
2. **DGIM**: il modello UML concettuale con classi, attributi e relazioni.
3. **DGFCD**: il vocabolario controllato di concetti, attributi, valori, ruoli e unita.
4. **DGRWI**: l'indice che collega gli oggetti del mondo reale ai concetti DGFCD.
5. **Specifiche di encoding**: le regole per scambiare dati DGIF, compreso il GML.
6. **Profilo GeoPackage**: le regole pratiche di memorizzazione quando serve una consegna in forma di database.

In breve: **DGIM descrive com'e fatto il mondo, DGFCD dice quali nomi e valori sono ammessi, e i documenti di encoding/profilo spiegano come impacchettare queste informazioni per lo scambio.**

!!! note

    Usa le edizioni correnti. Per questo progetto conviene partire dai documenti DGIF 3.0 pubblicati a luglio 2024 e dal GeoPackage Profile 1.4 Ed.1.1.1 pubblicato a marzo 2026. Le revisioni piu vecchie vanno trattate come riferimenti legacy.

### Punto di ingresso ufficiale DGIWG

Catalogo ufficiale: [DGIWG Standards](https://dgiwg.org/documents/dgiwg-standards).

| Documento | Perche conta | Link |
| --- | --- | --- |
| DGIWG 200: DGIF Overview | Quadro generale, perimetro e conformita. | [Pagina origine](https://dgiwg.org/documents/dgiwg-standards/200) e [PDF](https://portal.dgiwg.org/files/68061) |
| DGIWG 205: DGIM | Modello concettuale di classi, ereditarieta e semantica. | [Pagina origine](https://dgiwg.org/documents/dgiwg-standards/205) e [PDF](https://portal.dgiwg.org/files/68062) |
| DGIWG 206: DGFCD | Dizionario ufficiale di concetti, attributi, valori, ruoli e unita. | [Pagina origine](https://dgiwg.org/documents/dgiwg-standards/206) e [PDF](https://portal.dgiwg.org/files/68063) |
| DGIWG 207: DGRWI | Indice che collega oggetti reali e concetti del dizionario. | [Pagina origine](https://dgiwg.org/documents/dgiwg-standards/207) e [PDF](https://portal.dgiwg.org/files/68064) |
| DGIWG 208: DGIF Encoding Specification - Part 1: GML | Encoding normativo per lo scambio DGIF. | [Pagina origine](https://dgiwg.org/documents/dgiwg-standards/208) e [PDF](https://portal.dgiwg.org/files/68065) |
| DGIWG 126: GeoPackage Profile 1.4 Ed.1.1.1 | Profilo GeoPackage corrente per consegne operative su database. | [Pagina origine](https://dgiwg.org/documents/dgiwg-standards/126) e [PDF](https://portal.dgiwg.org/files/76055) |

**Dove si colloca ili2dgim:** questo progetto trasforma il contenuto concettuale DGIF in uno stack implementativo utilizzabile basato su INTERLIS, GeoPackage, tabelle di mapping e pipeline ETL.

## Che cosa produce ili2dgim

1. Generazione del modello INTERLIS (`DGIF_V3.ili`).
2. Generazione dello schema GeoPackage.
3. Generazione delle tabelle di mapping per OSM, swissTLM3D e Overture.
4. Pipeline ETL che caricano i dati sorgente nei layer target DGIF.

## Mappa della documentazione

- Inizia da [Principles](principles.md) per le basi concettuali e architetturali.
- Prosegui con [Webapp Overview](webapp.md) per i flussi operativi frontend/backend.
- Usa [Backend API](backend-api.md) come riferimento per le integrazioni.
- Usa [Scripts](scripts.md) come riferimento per comandi e opzioni.

## Avvio rapido

```bash
pip install -r docs/requirements-docs.txt
mkdocs serve
mkdocs build
```

Le pagine tecniche non ancora tradotte ricadono automaticamente sulla versione inglese.