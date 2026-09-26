# Documentation ili2dgim

Cette page d'accueil donne une entree simple dans un ensemble de normes complexe. Si DGIF est nouveau pour vous, lisez d'abord cette introduction, puis passez aux pages techniques.

Cette documentation couvre trois domaines du projet :

- Les principes ili2dgim et l'approche de modelisation.
- L'application web et l'API d'orchestration du backend.
- Les scripts Python qui construisent les artefacts de modele et executent les pipelines ETL.

## Fondation DGIF pour debutants

**DGIF n'est pas un seul document.** C'est une famille de normes DGIWG qui travaillent ensemble :

1. **Overview** : explique le cadre general et la conformite.
2. **DGIM** : le modele UML conceptuel des classes, attributs et relations.
3. **DGFCD** : le vocabulaire controle pour les concepts, attributs, valeurs, roles et unites.
4. **DGRWI** : l'index qui relie les objets du monde reel aux concepts DGFCD.
5. **Specifications d'encodage** : les regles d'echange des donnees DGIF, y compris le GML.
6. **Profil GeoPackage** : les regles pratiques de stockage pour une livraison de type base de donnees.

En bref : **DGIM decrit le monde, DGFCD decrit les noms et valeurs autorises, et les documents d'encodage/profil expliquent comment emballer ces informations pour l'echange.**

!!! note

    Utilisez les editions courantes. Pour ce projet, commencez par les documents DGIF 3.0 publies en juillet 2024 et par le profil GeoPackage 1.4 Ed.1.1.1 publie en mars 2026. Les anciennes generations DGIF et les anciennes revisions du profil GeoPackage doivent etre considerees comme des references historiques.

### Point d'entree officiel DGIWG

Catalogue officiel : [DGIWG Standards](https://dgiwg.org/documents/dgiwg-standards).

| Document | Pourquoi c'est important | Liens |
| --- | --- | --- |
| DGIWG 200: DGIF Overview | Vue d'ensemble, perimetre et conformite. | [Page d'origine](https://dgiwg.org/documents/dgiwg-standards/200) et [PDF](https://portal.dgiwg.org/files/68061) |
| DGIWG 205: DGIM | Modele conceptuel des classes, de l'heritage et de la semantique. | [Page d'origine](https://dgiwg.org/documents/dgiwg-standards/205) et [PDF](https://portal.dgiwg.org/files/68062) |
| DGIWG 206: DGFCD | Dictionnaire officiel des concepts, attributs, valeurs, roles et unites. | [Page d'origine](https://dgiwg.org/documents/dgiwg-standards/206) et [PDF](https://portal.dgiwg.org/files/68063) |
| DGIWG 207: DGRWI | Index reliant les objets du monde reel aux concepts. | [Page d'origine](https://dgiwg.org/documents/dgiwg-standards/207) et [PDF](https://portal.dgiwg.org/files/68064) |
| DGIWG 208: DGIF Encoding Specification - Part 1: GML | Encodage normatif pour l'echange DGIF. | [Page d'origine](https://dgiwg.org/documents/dgiwg-standards/208) et [PDF](https://portal.dgiwg.org/files/68065) |
| DGIWG 126: GeoPackage Profile 1.4 Ed.1.1.1 | Profil GeoPackage actuel pour une livraison operationnelle. | [Page d'origine](https://dgiwg.org/documents/dgiwg-standards/126) et [PDF](https://portal.dgiwg.org/files/76055) |

**Place d'ili2dgim :** ce projet transforme le contenu conceptuel DGIF en une chaine d'implementation basee sur INTERLIS, GeoPackage, tables de correspondance et pipelines ETL.

## Ce que livre ili2dgim

1. Generation du modele INTERLIS (`DGIF_V3.ili`).
2. Generation du schema GeoPackage.
3. Generation des tables de correspondance pour OSM, swissTLM3D et Overture.
4. Pipelines ETL qui chargent les donnees source dans les couches DGIF.

## Carte de la documentation

- Commencez par [Principles](principles.md) pour les bases conceptuelles et architecturales.
- Continuez avec [Webapp Overview](webapp.md) pour les flux operateur frontend/backend.
- Utilisez [Backend API](backend-api.md) comme reference d'integration.
- Utilisez [Scripts](scripts.md) comme reference de commandes et d'options.

## Demarrage rapide

```bash
pip install -r docs/requirements-docs.txt
mkdocs serve
mkdocs build
```

Les pages techniques non encore traduites retombent automatiquement sur la version anglaise.