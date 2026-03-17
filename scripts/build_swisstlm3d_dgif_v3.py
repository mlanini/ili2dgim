#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_swisstlm3d_dgif_v3.py
============================
Generates the mapping table swissTLM3D → DGIF V3.0.

For every swissTLM3D class + Objektart value, finds the best matching
DGIF 3.0 class (with optional attribute/value) and writes a CSV with the
same structure as OSM_to_DGIF_V3.csv.

Input:
  - models/swissTLM3D_ili2_V2_4.ili        (swissTLM3D INTERLIS model)
  - models/DGIF_V3.ili                     (DGIF 3.0 INTERLIS model)

Output:
  - models/swissTLM3D_to_DGIF_V3.csv
"""

import csv
import re
import os
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
ILI_DGIF  = BASE / "models" / "DGIF_V3.ili"
ILI_TLM   = BASE / "models" / "swissTLM3D_ili2_V2_4.ili"
CSV_OUT   = BASE / "models" / "swissTLM3D_to_DGIF_V3.csv"

# ── Extract DGIF V3 classes ─────────────────────────────────────────────────
def extract_dgif_classes(ili_path: Path) -> set:
    classes = set()
    pat = re.compile(r'^\s*CLASS\s+(\w+)\s')
    with open(ili_path, encoding="utf-8") as f:
        for line in f:
            m = pat.match(line)
            if m:
                classes.add(m.group(1))
    return classes

# ── Geometry type from swissTLM3D attribute ─────────────────────────────────
def geom_type(shape_type: str) -> str:
    if "HKoord" in shape_type:
        return "Point"
    elif "D_POLYLINE" in shape_type:
        return "Line"
    elif "D_SURFACE" in shape_type:
        return "Polygon"
    return ""

# ══════════════════════════════════════════════════════════════════════════════
# MASTER MAPPING TABLE
# ══════════════════════════════════════════════════════════════════════════════
# Structure:
#   (TLM_Topic, TLM_Class, TLM_Objektart,
#    Mapping_Description,
#    DGIF_Feature_Alpha, DGIF_Feature_531,
#    DGIF_Attr_Alpha, DGIF_Attr_531, DGIF_AttrVal_Alpha, DGIF_Val_531,
#    DGIF_Attr2_Alpha, DGIF_Attr2_531, DGIF_AttrVal2_Alpha, DGIF_Val2_531,
#    Geometry_Type)
#
# Mapping_Description: OK = exact match, Generalization = approximate,
#                      not in DGIF = no equivalent

MAPPING = [
    # ══════════════════════════════════════════════════════════════════════════
    # TLM_AREALE
    # ══════════════════════════════════════════════════════════════════════════

    # ── TLM_FREIZEITAREAL ──
    ("TLM_AREALE", "TLM_FREIZEITAREAL", "Campingplatzareal",
     "OK", "CampSite", "AK060", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_FREIZEITAREAL", "Freizeitanlagenareal",
     "Generalization", "AmusementPark", "AK030", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_FREIZEITAREAL", "Golfplatzareal",
     "OK", "GolfCourse", "AK100", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_FREIZEITAREAL", "Pferderennbahnareal",
     "OK", "Racetrack", "AK130", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_FREIZEITAREAL", "Schwimmbadareal",
     "OK", "SwimmingPool", "AK170", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_FREIZEITAREAL", "Sportplatzareal",
     "OK", "SportsGround", "AK040", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_FREIZEITAREAL", "Standplatzareal",
     "Generalization", "CampSite", "AK060", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_FREIZEITAREAL", "Zooareal",
     "OK", "Zoo", "AK180", "", "", "", "", "", "", "", "", "Polygon"),

    # ── TLM_NUTZUNGSAREAL ──
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Abwasserreinigungsareal",
     "OK", "SewageTreatmentPlant", "AB000", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Baumschule",
     "Generalization", "Orchard", "EA050", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Deponieareal",
     "OK", "DisposalSite", "AB010", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Friedhof",
     "OK", "Cemetery", "AL030", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Historisches_Areal",
     "Generalization", "ArcheologicalSite", "AL012", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Abbauareal",
     "OK", "ExtractionMine", "AA010", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Kraftwerkareal",
     "OK", "ElectricPowerStation", "AD010", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Messeareal",
     "Generalization", "Fairground", "AK010", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Obstanlage",
     "OK", "Orchard", "EA050", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Oeffentliches_Parkareal",
     "OK", "Park", "AK120", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Reben",
     "OK", "Vineyard", "EA060", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Schrebergartenareal",
     "Generalization", "AllotmentArea", "AJ110", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Schul_und_Hochschulareal",
     "Generalization", "EducationalAmenity", "AL019", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Spitalareal",
     "Generalization", "Building", "AL013",
     "featureFunction", "FFN", "hospital", "830", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Wald_nicht_bestockt",
     "Generalization", "Forest", "EC015", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Antennenareal",
     "Generalization", "Aerial", "AT010", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Kehrichtverbrennungsareal",
     "Generalization", "DisposalSite", "AB010", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Klosterareal",
     "Generalization", "Building", "AL013",
     "featureFunction", "FFN", "religiousActivities", "930", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Massnahmenvollzugsanstaltsareal",
     "Generalization", "Building", "AL013",
     "featureFunction", "FFN", "lawEnforcement", "810", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Unterwerkareal",
     "OK", "PowerSubstation", "AD030", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_NUTZUNGSAREAL", "Truppenuebungsplatz",
     "OK", "TrainingSite", "FA015", "", "", "", "", "", "", "", "", "Polygon"),

    # ── TLM_VERKEHRSAREAL ──
    ("TLM_AREALE", "TLM_VERKEHRSAREAL", "Flugfeldareal",
     "Generalization", "Aerodrome", "GB001", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_VERKEHRSAREAL", "Flughafenareal",
     "OK", "Aerodrome", "GB001", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_VERKEHRSAREAL", "Flugplatzareal",
     "OK", "Aerodrome", "GB001", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_VERKEHRSAREAL", "Heliport",
     "OK", "Heliport", "GB055", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_VERKEHRSAREAL", "Oeffentliches_Parkplatzareal",
     "OK", "VehicleLot", "AQ140", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_VERKEHRSAREAL", "Privates_Fahrareal",
     "Generalization", "VehicleLot", "AQ140", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_VERKEHRSAREAL", "Privates_Parkplatzareal",
     "Generalization", "VehicleLot", "AQ140", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_VERKEHRSAREAL", "Rastplatzareal",
     "Generalization", "Facility", "AL010", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_VERKEHRSAREAL", "Verkehrsflaeche",
     "Generalization", "LandTransportationWay", "AP030", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_AREALE", "TLM_VERKEHRSAREAL", "Gleisareal",
     "Generalization", "RailwayYard", "AN060", "", "", "", "", "", "", "", "", "Polygon"),

    # ── TLM_SCHUTZGEBIET ──
    ("TLM_AREALE", "TLM_SCHUTZGEBIET", "Nationalpark",
     "OK", "ConservationArea", "FA003", "", "", "", "", "", "", "", "", "Polygon"),

    # ══════════════════════════════════════════════════════════════════════════
    # TLM_BAUTEN
    # ══════════════════════════════════════════════════════════════════════════

    # ── TLM_GEBAEUDE_FOOTPRINT ──
    ("TLM_BAUTEN", "TLM_GEBAEUDE_FOOTPRINT", "Gebaeude",
     "OK", "Building", "AL013", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_GEBAEUDE_FOOTPRINT", "Historische_Baute",
     "Generalization", "Building", "AL013",
     "historicSignificance", "HSS", "historic", "2", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_GEBAEUDE_FOOTPRINT", "Kapelle",
     "Generalization", "Building", "AL013",
     "featureFunction", "FFN", "religiousActivities", "930", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_GEBAEUDE_FOOTPRINT", "Sakraler_Turm",
     "Generalization", "Tower", "AL241",
     "featureFunction", "FFN", "religiousActivities", "930", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_GEBAEUDE_FOOTPRINT", "Sakrales_Gebaeude",
     "Generalization", "Building", "AL013",
     "featureFunction", "FFN", "religiousActivities", "930", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_GEBAEUDE_FOOTPRINT", "Hochhaus",
     "Generalization", "Building", "AL013", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_GEBAEUDE_FOOTPRINT", "Hochkamin",
     "OK", "Smokestack", "AF030", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_GEBAEUDE_FOOTPRINT", "Turm",
     "OK", "Tower", "AL241", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_GEBAEUDE_FOOTPRINT", "Kuehlturm",
     "OK", "CoolingTower", "AF040", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_GEBAEUDE_FOOTPRINT", "Lagertank",
     "OK", "StorageTank", "AM070", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_GEBAEUDE_FOOTPRINT", "Lueftungsschacht",
     "not in DGIF", "", "", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_GEBAEUDE_FOOTPRINT", "Offenes_Gebaeude",
     "Generalization", "Building", "AL013", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_GEBAEUDE_FOOTPRINT", "Treibhaus",
     "Generalization", "Building", "AL013",
     "featureFunction", "FFN", "agriculture", "001", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_GEBAEUDE_FOOTPRINT", "Im_Bau",
     "Generalization", "ConstructionZone", "AL023", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_GEBAEUDE_FOOTPRINT", "Flugdach",
     "Generalization", "Building", "AL013", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_GEBAEUDE_FOOTPRINT", "Unterirdisches_Gebaeude",
     "Generalization", "UndergroundDwelling", "AL065", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_GEBAEUDE_FOOTPRINT", "Verbindungsbruecke",
     "Generalization", "Bridge", "AQ040", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_GEBAEUDE_FOOTPRINT", "Mauer_gross",
     "OK", "Wall", "AL260", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_GEBAEUDE_FOOTPRINT", "Mauer_gross_gedeckt",
     "Generalization", "Wall", "AL260", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_GEBAEUDE_FOOTPRINT", "Einhausung",
     "Generalization", "Building", "AL013", "", "", "", "", "", "", "", "", "Polygon"),

    # ── TLM_MAUER ──
    ("TLM_BAUTEN", "TLM_MAUER", "Mauer",
     "OK", "Wall", "AL260", "", "", "", "", "", "", "", "", "Line"),

    # ── TLM_SPORTBAUTE_LIN ──
    ("TLM_BAUTEN", "TLM_SPORTBAUTE_LIN", "Bobbahn",
     "Generalization", "Racetrack", "AK130", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_BAUTEN", "TLM_SPORTBAUTE_LIN", "Laufbahn",
     "Generalization", "Racetrack", "AK130", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_BAUTEN", "TLM_SPORTBAUTE_LIN", "Pferderennbahn",
     "OK", "Racetrack", "AK130", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_BAUTEN", "TLM_SPORTBAUTE_LIN", "Rodelbahn",
     "Generalization", "Racetrack", "AK130", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_BAUTEN", "TLM_SPORTBAUTE_LIN", "Scheibenstand",
     "OK", "FiringRange", "FA015", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_BAUTEN", "TLM_SPORTBAUTE_LIN", "Skisprungschanze",
     "OK", "SkiJump", "AK150", "", "", "", "", "", "", "", "", "Line"),

    # ── TLM_SPORTBAUTE_PLY ──
    ("TLM_BAUTEN", "TLM_SPORTBAUTE_PLY", "Sportplatz",
     "OK", "SportsGround", "AK040", "", "", "", "", "", "", "", "", "Polygon"),

    # ── TLM_STAUBAUTE ──
    ("TLM_BAUTEN", "TLM_STAUBAUTE", "Staumauer",
     "OK", "Dam", "BI020", "damType", "DWT", "gravityConcrete", "2", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_STAUBAUTE", "Staudamm",
     "OK", "Dam", "BI020", "damType", "DWT", "earthFill", "1", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_STAUBAUTE", "Wasserbecken",
     "Generalization", "InlandWaterbody", "BH082", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_STAUBAUTE", "Wehr",
     "OK", "Dam", "BI020", "damType", "DWT", "weir", "5", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_STAUBAUTE", "Schutzdamm",
     "Generalization", "Embankment", "DB090", "", "", "", "", "", "", "", "", "Polygon"),

    # ── TLM_VERBAUUNG ──
    ("TLM_BAUTEN", "TLM_VERBAUUNG", "Gewaesserverbauung",
     "Generalization", "ShorelineConstruction", "BB081", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_BAUTEN", "TLM_VERBAUUNG", "Schutzverbauung",
     "Generalization", "Fortification", "AH055", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_BAUTEN", "TLM_VERBAUUNG", "Trockenmauer",
     "OK", "Wall", "AL260", "", "", "", "", "", "", "", "", "Line"),

    # ── TLM_VERKEHRSBAUTE_LIN ──
    ("TLM_BAUTEN", "TLM_VERKEHRSBAUTE_LIN", "Hafensteg",
     "Generalization", "ShorelineConstruction", "BB081", "", "", "", "", "", "", "", "", "Line"),

    # ── TLM_VERKEHRSBAUTE_PLY ──
    ("TLM_BAUTEN", "TLM_VERKEHRSBAUTE_PLY", "Graspiste",
     "Generalization", "Runway", "GB045", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_VERKEHRSBAUTE_PLY", "Hartbelagpiste",
     "OK", "Runway", "GB045", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_VERKEHRSBAUTE_PLY", "Perron",
     "Generalization", "TransportationPlatform", "AQ125", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_VERKEHRSBAUTE_PLY", "Rollfeld_Gras",
     "Generalization", "Taxiway", "GB050", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_VERKEHRSBAUTE_PLY", "Rollfeld_Hartbelag",
     "OK", "Taxiway", "GB050", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BAUTEN", "TLM_VERKEHRSBAUTE_PLY", "Schleuse",
     "OK", "Lock", "BI030", "", "", "", "", "", "", "", "", "Polygon"),

    # ── TLM_LEITUNG ──
    ("TLM_BAUTEN", "TLM_LEITUNG", "Stromleitung",
     "OK", "Cable", "AT005", "", "", "", "", "", "", "", "", "Line"),

    # ── TLM_VERSORGUNGSBAUTE_PKT ──
    ("TLM_BAUTEN", "TLM_VERSORGUNGSBAUTE_PKT", "Antenne_gross",
     "OK", "Aerial", "AT010", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_BAUTEN", "TLM_VERSORGUNGSBAUTE_PKT", "Windturbine",
     "OK", "WindTurbine", "AJ051", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_BAUTEN", "TLM_VERSORGUNGSBAUTE_PKT", "Antenne_klein",
     "Generalization", "Aerial", "AT010", "", "", "", "", "", "", "", "", "Point"),

    # ══════════════════════════════════════════════════════════════════════════
    # TLM_BB (Bodenbedeckung)
    # ══════════════════════════════════════════════════════════════════════════
    ("TLM_BB", "TLM_BODENBEDECKUNG", "Fels",
     "OK", "RockFormation", "DB160", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BB", "TLM_BODENBEDECKUNG", "Feuchtgebiet",
     "Generalization", "InundatedLand", "ED020", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BB", "TLM_BODENBEDECKUNG", "Fliessgewaesser",
     "Generalization", "River", "BH140", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BB", "TLM_BODENBEDECKUNG", "Gebueschwald",
     "Generalization", "ShrubLand", "EB020", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BB", "TLM_BODENBEDECKUNG", "Gletscher",
     "OK", "Glacier", "BJ030", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BB", "TLM_BODENBEDECKUNG", "Lockergestein",
     "Generalization", "SoilSurfaceRegion", "DA010", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BB", "TLM_BODENBEDECKUNG", "Stehende_Gewaesser",
     "OK", "InlandWaterbody", "BH082", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BB", "TLM_BODENBEDECKUNG", "Wald",
     "OK", "Forest", "EC015", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BB", "TLM_BODENBEDECKUNG", "Wald_offen",
     "Generalization", "Forest", "EC015", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BB", "TLM_BODENBEDECKUNG", "Fels_locker",
     "Generalization", "RockFormation", "DB160", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BB", "TLM_BODENBEDECKUNG", "Felsbloecke",
     "Generalization", "RockFormation", "DB160", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BB", "TLM_BODENBEDECKUNG", "Felsbloecke_locker",
     "Generalization", "RockFormation", "DB160", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BB", "TLM_BODENBEDECKUNG", "Lockergestein_locker",
     "Generalization", "SoilSurfaceRegion", "DA010", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BB", "TLM_BODENBEDECKUNG", "Gehoelzflaeche",
     "Generalization", "Scrubland", "EB010", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_BB", "TLM_BODENBEDECKUNG", "Schneefeld_Toteis",
     "OK", "PermanentSnowIce", "BJ040", "", "", "", "", "", "", "", "", "Polygon"),

    # ── TLM_EINZELBAUM_GEBUESCH ──
    ("TLM_BB", "TLM_EINZELBAUM_GEBUESCH", "Einzelbaum",
     "OK", "Tree", "EC005", "", "", "", "", "", "", "", "", "Point"),

    # ══════════════════════════════════════════════════════════════════════════
    # TLM_EO (Einzelobjekte)
    # ══════════════════════════════════════════════════════════════════════════
    ("TLM_EO", "TLM_EINZELOBJEKT", "Bildstock",
     "Generalization", "Monument", "AL130",
     "structureShape", "SSC", "cross", "98", "", "", "", "", "Point"),
    ("TLM_EO", "TLM_EINZELOBJEKT", "Brunnen",
     "OK", "Fountain", "BI030", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_EO", "TLM_EINZELOBJEKT", "Denkmal",
     "OK", "Monument", "AL130", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_EO", "TLM_EINZELOBJEKT", "Gipfelkreuz",
     "Generalization", "Monument", "AL130",
     "structureShape", "SSC", "cross", "98", "", "", "", "", "Point"),
    ("TLM_EO", "TLM_EINZELOBJEKT", "Grotte_Hoehle",
     "OK", "CaveMouth", "DB029", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_EO", "TLM_EINZELOBJEKT", "Quelle",
     "OK", "Spring", "BH170", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_EO", "TLM_EINZELOBJEKT", "Triangulationspyramide",
     "Generalization", "BoundaryMonument", "AL025", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_EO", "TLM_EINZELOBJEKT", "Wasserfall",
     "OK", "Waterfall", "BH180", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_EO", "TLM_EINZELOBJEKT", "Wasserversorgung",
     "Generalization", "Waterwork", "AJ031", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_EO", "TLM_EINZELOBJEKT", "Landesgrenzstein",
     "OK", "BoundaryMonument", "AL025", "", "", "", "", "", "", "", "", "Point"),

    # ══════════════════════════════════════════════════════════════════════════
    # TLM_GEWAESSER
    # ══════════════════════════════════════════════════════════════════════════
    ("TLM_GEWAESSER", "TLM_FLIESSGEWAESSER", "Bisse_Suone",
     "Generalization", "Canal", "BH020", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_GEWAESSER", "TLM_FLIESSGEWAESSER", "Druckleitung_einfach",
     "OK", "Pipeline", "AQ113", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_GEWAESSER", "TLM_FLIESSGEWAESSER", "Druckleitung_mehrfach",
     "OK", "Pipeline", "AQ113", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_GEWAESSER", "TLM_FLIESSGEWAESSER", "Druckstollen",
     "Generalization", "Tunnel", "AQ130", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_GEWAESSER", "TLM_FLIESSGEWAESSER", "Fliessgewaesser",
     "OK", "River", "BH140", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_GEWAESSER", "TLM_FLIESSGEWAESSER", "Seeachse",
     "Generalization", "InlandWaterbody", "BH082", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_GEWAESSER", "TLM_FLIESSGEWAESSER", "Trockenrinne",
     "Generalization", "Ditch", "BH030", "", "", "", "", "", "", "", "", "Line"),

    ("TLM_GEWAESSER", "TLM_STEHENDES_GEWAESSER", "See",
     "OK", "InlandWaterbody", "BH082",
     "inlandWaterType", "IWT", "lake", "1", "", "", "", "", "Line"),
    ("TLM_GEWAESSER", "TLM_STEHENDES_GEWAESSER", "Seeinsel",
     "OK", "Island", "BA030", "", "", "", "", "", "", "", "", "Line"),

    # ══════════════════════════════════════════════════════════════════════════
    # TLM_NAMEN
    # ══════════════════════════════════════════════════════════════════════════
    ("TLM_NAMEN", "TLM_FLURNAME", "Flurname_swisstopo",
     "Generalization", "GeneralLocation", "ZD040", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_NAMEN", "TLM_FLURNAME", "Lokalname_swisstopo",
     "Generalization", "GeneralLocation", "ZD040", "", "", "", "", "", "", "", "", "Point"),

    ("TLM_NAMEN", "TLM_GEBIETSNAME", "Gebiet",
     "Generalization", "AdministrativeDivision", "FA001", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_NAMEN", "TLM_GEBIETSNAME", "Grossregion",
     "Generalization", "AdministrativeDivision", "FA001", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_NAMEN", "TLM_GEBIETSNAME", "Landschaftsname",
     "Generalization", "LandArea", "DA020", "", "", "", "", "", "", "", "", "Polygon"),

    ("TLM_NAMEN", "TLM_GELAENDENAME", "Gletscher",
     "OK", "Glacier", "BJ030", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_NAMEN", "TLM_GELAENDENAME", "Graben",
     "Generalization", "LandMorphologyArea", "DB001",
     "landMorphology", "LND", "valley", "52", "", "", "", "", "Polygon"),
    ("TLM_NAMEN", "TLM_GELAENDENAME", "Grat",
     "Generalization", "LandMorphologyArea", "DB001",
     "landMorphology", "LND", "ridge", "42", "", "", "", "", "Polygon"),
    ("TLM_NAMEN", "TLM_GELAENDENAME", "Haupttal",
     "Generalization", "LandMorphologyArea", "DB001",
     "landMorphology", "LND", "valley", "52", "", "", "", "", "Polygon"),
    ("TLM_NAMEN", "TLM_GELAENDENAME", "Huegelzug",
     "Generalization", "Hill", "DB070", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_NAMEN", "TLM_GELAENDENAME", "Massiv",
     "Generalization", "LandMorphologyArea", "DB001",
     "landMorphology", "LND", "mountain", "62", "", "", "", "", "Polygon"),
    ("TLM_NAMEN", "TLM_GELAENDENAME", "Seeteil",
     "Generalization", "InlandWaterbody", "BH082", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_NAMEN", "TLM_GELAENDENAME", "Tal",
     "OK", "LandMorphologyArea", "DB001",
     "landMorphology", "LND", "valley", "52", "", "", "", "", "Polygon"),

    ("TLM_NAMEN", "TLM_NAME_PKT", "Alpiner_Gipfel",
     "Generalization", "GeomorphicExtreme", "DB080",
     "featureType", "FTY", "peak", "1", "", "", "", "", "Point"),
    ("TLM_NAMEN", "TLM_NAME_PKT", "Felskopf",
     "Generalization", "GeomorphicExtreme", "DB080",
     "featureType", "FTY", "peak", "1", "", "", "", "", "Point"),
    ("TLM_NAMEN", "TLM_NAME_PKT", "Gipfel",
     "OK", "GeomorphicExtreme", "DB080",
     "featureType", "FTY", "peak", "1", "", "", "", "", "Point"),
    ("TLM_NAMEN", "TLM_NAME_PKT", "Hauptgipfel",
     "OK", "GeomorphicExtreme", "DB080",
     "featureType", "FTY", "peak", "1", "", "", "", "", "Point"),
    ("TLM_NAMEN", "TLM_NAME_PKT", "Haupthuegel",
     "Generalization", "Hill", "DB070", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_NAMEN", "TLM_NAME_PKT", "Huegel",
     "OK", "Hill", "DB070", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_NAMEN", "TLM_NAME_PKT", "Pass",
     "OK", "MountainPass", "DB050", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_NAMEN", "TLM_NAME_PKT", "Strassenpass",
     "Generalization", "MountainPass", "DB050", "", "", "", "", "", "", "", "", "Point"),

    ("TLM_NAMEN", "TLM_SIEDLUNGSNAME", "Ort",
     "OK", "PopulatedPlace", "AL105", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_NAMEN", "TLM_SIEDLUNGSNAME", "Ortsteil",
     "Generalization", "PopulatedPlace", "AL105", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_NAMEN", "TLM_SIEDLUNGSNAME", "Quartier",
     "Generalization", "Neighbourhood", "AL106", "", "", "", "", "", "", "", "", "Polygon"),
    ("TLM_NAMEN", "TLM_SIEDLUNGSNAME", "Quartierteil",
     "Generalization", "Neighbourhood", "AL106", "", "", "", "", "", "", "", "", "Polygon"),

    # ══════════════════════════════════════════════════════════════════════════
    # TLM_OEV (Öffentlicher Verkehr)
    # ══════════════════════════════════════════════════════════════════════════

    # ── TLM_EISENBAHN ──
    ("TLM_OEV", "TLM_EISENBAHN", "Kleinbahn",
     "Generalization", "Railway", "AN010",
     "gaugeConfiguration", "GAW", "narrowGauge", "2", "", "", "", "", "Line"),
    ("TLM_OEV", "TLM_EISENBAHN", "Normalspur",
     "OK", "Railway", "AN010",
     "gaugeConfiguration", "GAW", "standardGauge", "1", "", "", "", "", "Line"),
    ("TLM_OEV", "TLM_EISENBAHN", "Schmalspur",
     "OK", "Railway", "AN010",
     "gaugeConfiguration", "GAW", "narrowGauge", "2", "", "", "", "", "Line"),
    ("TLM_OEV", "TLM_EISENBAHN", "Schmalspur_mit_Normalspur",
     "Generalization", "Railway", "AN010",
     "gaugeConfiguration", "GAW", "mixedGauge", "4", "", "", "", "", "Line"),

    # ── TLM_HALTESTELLE ──
    ("TLM_OEV", "TLM_HALTESTELLE", "Haltestelle_Bahn",
     "OK", "TransportationStation", "AQ125",
     "meansTransportation", "TME", "railway", "3", "", "", "", "", "Point"),
    ("TLM_OEV", "TLM_HALTESTELLE", "Haltestelle_Bus",
     "Generalization", "TransportationStation", "AQ125",
     "meansTransportation", "TME", "bus", "5", "", "", "", "", "Point"),
    ("TLM_OEV", "TLM_HALTESTELLE", "Haltestelle_Schiff",
     "OK", "Harbour", "BB005", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_OEV", "TLM_HALTESTELLE", "Terminal",
     "OK", "TransportationStation", "AQ125", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_OEV", "TLM_HALTESTELLE", "Uebrige_Bahnen",
     "Generalization", "TransportationStation", "AQ125",
     "meansTransportation", "TME", "cableCar", "14", "", "", "", "", "Point"),

    # ── TLM_SCHIFFFAHRT ──
    ("TLM_OEV", "TLM_SCHIFFFAHRT", "Autofaehre",
     "OK", "FerryCrossing", "AQ070",
     "meansTransportation", "TME", "vehicleFerry", "10", "", "", "", "", "Line"),
    ("TLM_OEV", "TLM_SCHIFFFAHRT", "Personenfaehre",
     "OK", "FerryCrossing", "AQ070",
     "meansTransportation", "TME", "passengerFerry", "12", "", "", "", "", "Line"),

    # ── TLM_UEBRIGE_BAHN ──
    ("TLM_OEV", "TLM_UEBRIGE_BAHN", "Foerderband",
     "OK", "Conveyor", "AF060", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_OEV", "TLM_UEBRIGE_BAHN", "Gondelbahn",
     "OK", "Cableway", "AT041",
     "cablewayType", "CAT", "gondolaLift", "6", "", "", "", "", "Line"),
    ("TLM_OEV", "TLM_UEBRIGE_BAHN", "Luftseilbahn",
     "OK", "Cableway", "AT041",
     "cablewayType", "CAT", "aerialTramway", "5", "", "", "", "", "Line"),
    ("TLM_OEV", "TLM_UEBRIGE_BAHN", "Sesselbahn",
     "OK", "Cableway", "AT041",
     "cablewayType", "CAT", "chairLift", "2", "", "", "", "", "Line"),
    ("TLM_OEV", "TLM_UEBRIGE_BAHN", "Skilift",
     "OK", "Cableway", "AT041",
     "cablewayType", "CAT", "skiTow", "3", "", "", "", "", "Line"),
    ("TLM_OEV", "TLM_UEBRIGE_BAHN", "Transportseil",
     "Generalization", "Cableway", "AT041", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_OEV", "TLM_UEBRIGE_BAHN", "Lift",
     "Generalization", "Cableway", "AT041",
     "cablewayType", "CAT", "teeBarLift", "7", "", "", "", "", "Line"),

    # ══════════════════════════════════════════════════════════════════════════
    # TLM_STRASSEN
    # ══════════════════════════════════════════════════════════════════════════

    # ── TLM_AUS_EINFAHRT ──
    ("TLM_STRASSEN", "TLM_AUS_EINFAHRT", "Verzweigung",
     "OK", "RoadInterchange", "AQ111", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_STRASSEN", "TLM_AUS_EINFAHRT", "Ausfahrt",
     "Generalization", "RoadInterchange", "AQ111", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_STRASSEN", "TLM_AUS_EINFAHRT", "Einfahrt",
     "Generalization", "RoadInterchange", "AQ111", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_STRASSEN", "TLM_AUS_EINFAHRT", "Ein_und_Ausfahrt",
     "Generalization", "RoadInterchange", "AQ111", "", "", "", "", "", "", "", "", "Point"),

    # ── TLM_STRASSE ──
    ("TLM_STRASSEN", "TLM_STRASSE", "Ausfahrt",
     "Generalization", "LandTransportationWay", "AP030", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Autobahn",
     "OK", "LandTransportationWay", "AP030",
     "roadCharacteristics", "RCH", "motorway", "1", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Autostrasse",
     "Generalization", "LandTransportationWay", "AP030",
     "roadCharacteristics", "RCH", "motorway", "1", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Autozug",
     "Generalization", "FerryCrossing", "AQ070", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Dienstzufahrt",
     "Generalization", "LandTransportationWay", "AP030", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Einfahrt",
     "Generalization", "LandTransportationWay", "AP030", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Faehre",
     "OK", "FerryCrossing", "AQ070", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Klettersteig",
     "not in DGIF", "", "", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Markierte_Spur",
     "Generalization", "LandTransportationWay", "AP030", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Platz",
     "OK", "PublicSquare", "AL170", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Raststaette",
     "Generalization", "Facility", "AL010", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Strasse_3m",
     "Generalization", "LandTransportationWay", "AP030", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Strasse_4m",
     "Generalization", "LandTransportationWay", "AP030", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Strasse_6m",
     "Generalization", "LandTransportationWay", "AP030", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Strasse_8m",
     "Generalization", "LandTransportationWay", "AP030", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Strasse_10m",
     "OK", "LandTransportationWay", "AP030", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Verbindung",
     "Generalization", "LandTransportationWay", "AP030", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Weg_1m",
     "Generalization", "LandTransportationWay", "AP030", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Weg_2m",
     "Generalization", "LandTransportationWay", "AP030", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Wegfragment_1m",
     "Generalization", "LandTransportationWay", "AP030", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Wegfragment_2m",
     "Generalization", "LandTransportationWay", "AP030", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Zufahrt",
     "Generalization", "LandTransportationWay", "AP030", "", "", "", "", "", "", "", "", "Line"),
    ("TLM_STRASSEN", "TLM_STRASSE", "Provisorium",
     "Generalization", "LandTransportationWay", "AP030", "", "", "", "", "", "", "", "", "Line"),

    # ── TLM_STRASSENINFO ──
    ("TLM_STRASSEN", "TLM_STRASSENINFO", "Durchfahrtssperre",
     "Generalization", "VehicleBarrier", "AP040", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_STRASSEN", "TLM_STRASSENINFO", "Erschliessung",
     "not in DGIF", "", "", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_STRASSEN", "TLM_STRASSENINFO", "Loop_Junction",
     "Generalization", "RoadInterchange", "AQ111", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_STRASSEN", "TLM_STRASSENINFO", "MISTRA_Zusatzknoten",
     "not in DGIF", "", "", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_STRASSEN", "TLM_STRASSENINFO", "Raststaette",
     "Generalization", "Facility", "AL010", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_STRASSEN", "TLM_STRASSENINFO", "Standardknoten",
     "not in DGIF", "", "", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_STRASSEN", "TLM_STRASSENINFO", "Verladestation",
     "Generalization", "TransportationStation", "AQ125", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_STRASSEN", "TLM_STRASSENINFO", "Zahlstelle",
     "not in DGIF", "", "", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_STRASSEN", "TLM_STRASSENINFO", "Zollamt_24h_24h",
     "Generalization", "Checkpoint", "SU004", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_STRASSEN", "TLM_STRASSENINFO", "Zollamt_24h_eingeschraenkt",
     "Generalization", "Checkpoint", "SU004", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_STRASSEN", "TLM_STRASSENINFO", "Zollamt_eingeschraenkt",
     "Generalization", "Checkpoint", "SU004", "", "", "", "", "", "", "", "", "Point"),
    ("TLM_STRASSEN", "TLM_STRASSENINFO", "Namen",
     "not in DGIF", "", "", "", "", "", "", "", "", "", "", "Point"),

    # ── TLM_STRASSENROUTE ──
    ("TLM_STRASSEN", "TLM_STRASSENROUTE", "Europastrasse",
     "Generalization", "LandRoute", "AP031", "", "", "", "", "", "", "", "", ""),
    ("TLM_STRASSEN", "TLM_STRASSENROUTE", "Hauptstrasse_A",
     "Generalization", "LandRoute", "AP031", "", "", "", "", "", "", "", "", ""),
    ("TLM_STRASSEN", "TLM_STRASSENROUTE", "Hauptstrasse_B",
     "Generalization", "LandRoute", "AP031", "", "", "", "", "", "", "", "", ""),
    ("TLM_STRASSEN", "TLM_STRASSENROUTE", "Hauptstrasse_C",
     "Generalization", "LandRoute", "AP031", "", "", "", "", "", "", "", "", ""),
    ("TLM_STRASSEN", "TLM_STRASSENROUTE", "Hauptstrasse_swisstopo_gelb",
     "Generalization", "LandRoute", "AP031", "", "", "", "", "", "", "", "", ""),
    ("TLM_STRASSEN", "TLM_STRASSENROUTE", "Hauptstrasse_swisstopo_rot",
     "Generalization", "LandRoute", "AP031", "", "", "", "", "", "", "", "", ""),
    ("TLM_STRASSEN", "TLM_STRASSENROUTE", "HLS_Bund",
     "Generalization", "LandRoute", "AP031", "", "", "", "", "", "", "", "", ""),
    ("TLM_STRASSEN", "TLM_STRASSENROUTE", "HLS_Kanton",
     "Generalization", "LandRoute", "AP031", "", "", "", "", "", "", "", "", ""),
    ("TLM_STRASSEN", "TLM_STRASSENROUTE", "Kantonsstrasse",
     "Generalization", "LandRoute", "AP031", "", "", "", "", "", "", "", "", ""),
    ("TLM_STRASSEN", "TLM_STRASSENROUTE", "Nationalstrasse",
     "Generalization", "LandRoute", "AP031", "", "", "", "", "", "", "", "", ""),
    ("TLM_STRASSEN", "TLM_STRASSENROUTE", "Nebenstrasse",
     "Generalization", "LandRoute", "AP031", "", "", "", "", "", "", "", "", ""),
]


# ── CSV header (same structure as OSM_to_DGIF) ──────────────────────────────
HEADER = (
    "NO;TLM Topic;TLM Feature Class;TLM Attribute (Objektart);TLM Attribute Value;"
    "Geometry;Mapping Description;"
    "DGIF Feature Alpha;DGIF Feature 531;"
    "DGIF Attribute Alpha;DGIF Attribute 531;DGIF AttributeValue Alpha;DGIF Value 531;"
    "DGIF Attribute Alpha;DGIF Attribute 531;DGIF AttributeValue Alpha;DGIF Value 531;"
)


def build_csv():
    # Validate DGIF classes
    dgif_classes = extract_dgif_classes(ILI_DGIF)
    print(f"[INFO] DGIF V3 classes: {len(dgif_classes)}")
    print(f"[INFO] Mapping entries: {len(MAPPING)}")

    issues = []
    rows = []

    for i, m in enumerate(MAPPING, 1):
        (topic, tlm_class, objektart,
         mapping_desc, dgif_alpha, dgif_531,
         attr1, attr1_531, attrval1, val1_531,
         attr2, attr2_531, attrval2, val2_531,
         geom) = m

        # Validate DGIF class
        if dgif_alpha and dgif_alpha not in dgif_classes:
            issues.append(f"Row {i}: DGIF class '{dgif_alpha}' NOT in V3 model!")

        row = [
            str(i), topic, tlm_class, "Objektart", objektart, geom,
            mapping_desc, dgif_alpha, dgif_531,
            attr1, attr1_531, attrval1, val1_531,
            attr2, attr2_531, attrval2, val2_531,
        ]
        rows.append(row)

    # Write CSV
    with open(CSV_OUT, "w", encoding="utf-8-sig", newline="") as f:
        f.write(HEADER + "\n")
        for row in rows:
            f.write(";".join(row) + "\n")

    print(f"\n[INFO] CSV written: {CSV_OUT}")
    print(f"[INFO] Total rows: {len(rows)}")

    if issues:
        print(f"\n--- VALIDATION ISSUES ({len(issues)}) ---")
        for iss in issues:
            print(f"  • {iss}")

    # Statistics
    ok = sum(1 for m in MAPPING if m[3] == "OK")
    gen = sum(1 for m in MAPPING if m[3] == "Generalization")
    nid = sum(1 for m in MAPPING if m[3] == "not in DGIF")
    print(f"\n--- MAPPING STATISTICS ---")
    print(f"  OK             : {ok}")
    print(f"  Generalization : {gen}")
    print(f"  not in DGIF    : {nid}")
    print(f"  Total          : {len(MAPPING)}")

    # Mapped DGIF classes
    mapped = {m[4] for m in MAPPING if m[4]}
    print(f"\n  Distinct DGIF classes used: {len(mapped)}")


if __name__ == "__main__":
    build_csv()
