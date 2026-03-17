#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_osm_dgif_v3.py
=====================
Reads the V2 CSV mapping table (OSM_to_DGIF_V2.csv), the DGIF 3.0 INTERLIS
model (DGIF_V3.ili), compares class names, and produces an updated V3 CSV.

Steps:
  1. Extract all class names from DGIF_V3.ili (V3).
  2. Parse V2 CSV -> list of mapping rows.
  3. For each V2 row with a DGIF Feature Alpha name, check it still exists
     in V3.  Identify renamed / removed classes.
  4. Add new OSM tags from the wiki that were NOT in V2 but can be mapped
     to newly available V3 classes.
  5. Write OSM_to_DGIF_V3.csv.

Author: Automated DGIF pipeline
"""

import csv
import re
import os
import sys
from pathlib import Path

# ── paths ────────────────────────────────────────────────────────────────────
BASE = Path(__file__).resolve().parent.parent
ILI_FILE  = BASE / "models" / "DGIF_V3.ili"
CSV_V2    = BASE / "models" / "OSM_to_DGIF_V2.csv"
CSV_V3    = BASE / "models" / "OSM_to_DGIF_V3.csv"

# ── 1.  Extract V3 classes from .ili ─────────────────────────────────────────
def extract_v3_classes(ili_path: Path) -> set:
    """Return set of class names found in the .ili file."""
    classes = set()
    pattern = re.compile(r'^\s*CLASS\s+(\w+)\s')
    with open(ili_path, encoding="utf-8") as f:
        for line in f:
            m = pattern.match(line)
            if m:
                classes.add(m.group(1))
    return classes

# ── 2.  Read V2 CSV ─────────────────────────────────────────────────────────
def read_v2_csv(csv_path: Path):
    """Return (header_line, rows) using csv.reader to handle quoting correctly."""
    # Try utf-8-sig first, fall back to latin-1
    enc_used = "latin-1"
    for enc in ("utf-8-sig", "latin-1"):
        try:
            with open(csv_path, encoding=enc) as f:
                _ = f.read()
            enc_used = enc
            break
        except UnicodeDecodeError:
            continue

    with open(csv_path, encoding=enc_used, newline="") as f:
        reader = csv.reader(f, delimiter=";", quotechar='"')
        all_rows = list(reader)

    header = ";".join(all_rows[0])
    rows = all_rows[1:]
    return header, rows

# ── 3.  V2 ↔ V3 class name mapping  ─────────────────────────────────────────
# Manually curated rename / consolidation table for classes that changed name
# between DGIF 2.0 and 3.0.  Built by comparing the V2 CSV Feature Alpha names
# with the 673 V3 class list.
# Format:  "V2_name" : "V3_name"   (or None if removed in V3)
RENAME_MAP = {
    # V3 renamed MemorialMonument → Monument
    "MemorialMonument": "Monument",
    # V3 removed CaravanPark → closest match is CampSite
    "CaravanPark": "CampSite",
    # V3 removed Route (generic) → map to LandRoute
    "Route": "LandRoute",
    # V2 used British spelling 'ArchaeologicalSite'; V3 model uses 'ArcheologicalSite'
    "ArchaeologicalSite": "ArcheologicalSite",
}

# ── New OSM tags that exist on the OSM wiki (as of 2025) but were missing
#    from V2 and can now be mapped to V3 classes.
# Each entry: (OSM_Feature_Class, OSM_Key, OSM_Value, definition,
#              Mapping_Description, DGIF_Feature_Alpha, DGIF_Feature_531,
#              attr_alpha, attr_531, attrval_alpha, attrval_531,
#              attr2_alpha, attr2_531, attrval2_alpha, attrval2_531)
NEW_MAPPINGS = [
    # ── aeroway ──
    ("aeroway_Point",  "aeroway", "spaceport",
     "A spaceport or cosmodrome", "Generalization",
     "Aerodrome", "GB001", "", "", "", "", "", "", "", ""),
    ("aeroway_Point",  "aeroway", "terminal",
     "An airport passenger building", "Generalization",
     "Building", "AL013", "featureFunction", "FFN",
     "airTransportation", "481", "", "", "", ""),
    ("aeroway_Point",  "aeroway", "aircraft_crossing",
     "A point where traffic is impacted by crossing aircraft", "not in DGIF",
     "", "", "", "", "", "", "", "", "", ""),

    # ── amenity – new / missing from V2 ──
    ("amenity_Point", "amenity", "charging_station",
     "Charging facility for electric vehicles", "Generalization",
     "MotorVehicleStation", "AQ141", "", "", "", "", "", "", "", ""),
    ("amenity_Point", "amenity", "bicycle_rental",
     "Rent a bicycle", "Generalization",
     "Facility", "AL010", "featureFunction", "FFN",
     "rentalOfGoods", "480", "", "", "", ""),
    ("amenity_Point", "amenity", "boat_rental",
     "Rent a Boat", "Generalization",
     "Facility", "AL010", "featureFunction", "FFN",
     "rentalOfGoods", "480", "", "", "", ""),
    ("amenity_Point", "amenity", "car_rental",
     "Rent a car", "Generalization",
     "Facility", "AL010", "featureFunction", "FFN",
     "rentalOfGoods", "480", "", "", "", ""),
    ("amenity_Point", "amenity", "car_wash",
     "Wash a car", "Generalization",
     "Facility", "AL010", "featureFunction", "FFN",
     "vehicleMaintenance", "486", "", "", "", ""),
    ("amenity_Point", "amenity", "vehicle_inspection",
     "Government vehicle inspection", "Generalization",
     "Facility", "AL010", "featureFunction", "FFN",
     "vehicleMaintenance", "486", "", "", "", ""),
    ("amenity_Point", "amenity", "conference_centre",
     "A large building used to hold a convention", "Generalization",
     "Building", "AL013", "featureFunction", "FFN",
     "conferenceCenter", "594", "", "", "", ""),
    ("amenity_Point", "amenity", "events_venue",
     "A building specifically used for organising events", "Generalization",
     "Building", "AL013", "featureFunction", "FFN",
     "culturalArtsEntertainment", "890", "", "", "", ""),
    ("amenity_Point", "amenity", "gambling",
     "A place for gambling", "Generalization",
     "Building", "AL013", "featureFunction", "FFN",
     "sportsAmusementRecreation", "900", "", "", "", ""),
    ("amenity_Point", "amenity", "music_venue",
     "An indoor place to hear contemporary live music", "Generalization",
     "Building", "AL013", "featureFunction", "FFN",
     "culturalArtsEntertainment", "890", "", "", "", ""),
    ("amenity_Point", "amenity", "planetarium",
     "A planetarium", "Generalization",
     "Building", "AL013", "featureFunction", "FFN",
     "culturalArtsEntertainment", "890", "", "", "", ""),
    ("amenity_Point", "amenity", "food_court",
     "An area with several restaurant food counters", "Generalization",
     "Building", "AL013", "featureFunction", "FFN",
     "restaurant", "572", "", "", "", ""),
    ("amenity_Point", "amenity", "ice_cream",
     "Ice cream shop or ice cream parlour", "Generalization",
     "Building", "AL013", "featureFunction", "FFN",
     "restaurant", "572", "", "", "", ""),
    ("amenity_Point", "amenity", "parcel_locker",
     "Machine for picking up and sending parcels", "not in DGIF",
     "", "", "", "", "", "", "", "", "", ""),

    # ── boundary – new types ──
    ("boundary_Polygon", "boundary", "forest",
     "A delimited forest given defined boundaries", "Generalization",
     "Forest", "EC015", "", "", "", "", "", "", "", ""),
    ("boundary_Polygon", "boundary", "hazard",
     "A designated hazardous area", "not in DGIF",
     "", "", "", "", "", "", "", "", "", ""),
    ("boundary_Polygon", "boundary", "low_emission_zone",
     "Low emission zone restricting polluting vehicles", "not in DGIF",
     "", "", "", "", "", "", "", "", "", ""),
    ("boundary_Polygon", "boundary", "maritime",
     "Maritime boundaries (baseline, contiguous zone, EEZ)", "Generalization",
     "AdministrativeBoundary", "FA000", "", "", "", "", "", "", "", ""),

    # ── emergency – new ──
    ("emergency_Point", "emergency", "defibrillator",
     "An AED defibrillator", "not in DGIF",
     "", "", "", "", "", "", "", "", "", ""),
    ("emergency_Point", "emergency", "ambulance_station",
     "An ambulance station", "Generalization",
     "Building", "AL013", "featureFunction", "FFN",
     "emergencyMedicalServices", "836", "", "", "", ""),
    ("emergency_Point", "emergency", "assembly_point",
     "A designated safe place for gathering during emergency", "not in DGIF",
     "", "", "", "", "", "", "", "", "", ""),

    # ── highway – new / missing ──
    ("highway_Line", "highway", "busway",
     "A dedicated roadway for bus rapid transit systems", "Generalization",
     "LandTransportationWay", "AP030", "", "", "", "", "", "", "", ""),
    ("highway_Line", "highway", "via_ferrata",
     "A route equipped with fixed cables, stemples, ladders", "not in DGIF",
     "", "", "", "", "", "", "", "", "", ""),

    # ── historic – new from wiki ──
    ("historic_Point", "historic", "aircraft",
     "A decommissioned aircraft", "not in DGIF",
     "", "", "", "", "", "", "", "", "", ""),
    ("historic_Point", "historic", "aqueduct",
     "A historic structure to convey water", "Generalization",
     "Aqueduct", "BH010", "", "", "", "", "", "", "", ""),
    ("historic_Point", "historic", "battlefield",
     "The site of a battle", "Generalization",
     "ArcheologicalSite", "AL012", "", "", "", "", "", "", "", ""),
    ("historic_Point", "historic", "bomb_crater",
     "A bomb crater", "not in DGIF",
     "", "", "", "", "", "", "", "", "", ""),
    ("historic_Point", "historic", "locomotive",
     "A decommissioned locomotive", "not in DGIF",
     "", "", "", "", "", "", "", "", "", ""),

    # ── landuse – new from wiki ──
    ("landuse_Polygon", "landuse", "education",
     "An area predominantly used for educational purposes", "Generalization",
     "BuiltUpArea", "AL020", "", "", "", "", "", "", "", ""),
    ("landuse_Polygon", "landuse", "logging",
     "An area where trees have been cut down", "Generalization",
     "Forest", "EC015", "", "", "", "", "", "", "", ""),
    ("landuse_Polygon", "landuse", "aquaculture",
     "Farming of freshwater and saltwater organisms", "Generalization",
     "AquacultureFacility", "BH051", "", "", "", "", "", "", "", ""),
    ("landuse_Polygon", "landuse", "animal_keeping",
     "An area used to keep animals", "Generalization",
     "Farm", "AJ060", "", "", "", "", "", "", "", ""),
    ("landuse_Polygon", "landuse", "plant_nursery",
     "Planting plants maintaining for new plant production", "Generalization",
     "Orchard", "EA050", "", "", "", "", "", "", "", ""),
    ("landuse_Polygon", "landuse", "depot",
     "A depot for vehicles (trains, buses, trams)", "Generalization",
     "Facility", "AL010", "featureFunction", "FFN",
     "warehousingStorage", "530", "", "", "", ""),
    ("landuse_Polygon", "landuse", "greenery",
     "Any area covered with landscaping/decorative greenery", "Generalization",
     "Park", "AK120", "", "", "", "", "", "", "", ""),
    ("landuse_Polygon", "landuse", "winter_sports",
     "An area dedicated to winter sports", "Generalization",
     "SkiJumpSite", "AK155", "", "", "", "", "", "", "", ""),

    # ── leisure – new from wiki ──
    ("leisure_Point", "leisure", "disc_golf_course",
     "A place to play disc golf", "Generalization",
     "SportsGround", "AK040", "", "", "", "", "", "", "", ""),
    ("leisure_Point", "leisure", "escape_game",
     "A physical adventure game (escape room)", "not in DGIF",
     "", "", "", "", "", "", "", "", "", ""),
    ("leisure_Point", "leisure", "fitness_centre",
     "Fitness centre, health club or gym", "Generalization",
     "Building", "AL013", "featureFunction", "FFN",
     "sportsCentre", "912", "", "", "", ""),
    ("leisure_Point", "leisure", "fitness_station",
     "An outdoor facility for fitness exercises", "Generalization",
     "SportsGround", "AK040", "", "", "", "", "", "", "", ""),
    ("leisure_Point", "leisure", "hackerspace",
     "A place where people with common tech interests meet", "not in DGIF",
     "", "", "", "", "", "", "", "", "", ""),
    ("leisure_Polygon", "leisure", "summer_camp",
     "A place for supervised summer camps", "Generalization",
     "CampSite", "AK060", "", "", "", "", "", "", "", ""),

    # ── man_made – new from wiki ──
    ("man_made_Point", "man_made", "communications_tower",
     "A huge tower for transmitting radio", "OK",
     "CommunicationTower", "AT080", "", "", "", "", "", "", "", ""),
    ("man_made_Point", "man_made", "monitoring_station",
     "A station that monitors something", "not in DGIF",
     "", "", "", "", "", "", "", "", "", ""),
    ("man_made_Point", "man_made", "offshore_platform",
     "Offshore platform or oil rig", "OK",
     "OffshorePlatform", "BD115", "", "", "", "", "", "", "", ""),
    ("man_made_Point", "man_made", "pumping_station",
     "A pumping station for fluids", "OK",
     "PumpingStation", "AQ116", "", "", "", "", "", "", "", ""),

    # ── military – new ──
    ("military_Point", "military", "academy",
     "A training establishment for military service members", "Generalization",
     "MilitaryInstallation", "SU001", "", "", "", "", "", "", "", ""),
    ("military_Polygon", "military", "base",
     "A military base facility", "OK",
     "MilitaryInstallation", "SU001", "", "", "", "", "", "", "", ""),

    # ── natural – new ──
    ("natural_Point", "natural", "bare_rock",
     "An area with sparse vegetation where bedrock is visible", "OK",
     "RockFormation", "DB160", "", "", "", "", "", "", "", ""),

    # ── power – new from wiki ──
    ("power_Point", "power", "connection",
     "A free-standing electrical connection", "not in DGIF",
     "", "", "", "", "", "", "", "", "", ""),
    ("power_Point", "power", "switchgear",
     "A switchgear with busbar assemblies", "Generalization",
     "PowerSubstation", "AD030", "", "", "", "", "", "", "", ""),

    # ── railway – new from wiki ──
    ("railway_Line", "railway", "proposed",
     "Railway being proposed", "not in DGIF",
     "", "", "", "", "", "", "", "", "", ""),
    ("railway_Point", "railway", "tram_level_crossing",
     "A point where trams and roads cross", "Generalization",
     "Crossing", "AQ062", "meansTransportation", "TME",
     "tramway", "14", "", "", "", ""),
    ("railway_Point", "railway", "wash",
     "A railroad carriage wash", "not in DGIF",
     "", "", "", "", "", "", "", "", "", ""),

    # ── shop – new from wiki ──
    ("shop_Point", "shop", "health_food",
     "A health food shop selling wholefoods, vitamins", "Generalization",
     "Building", "AL013", "featureFunction", "FFN",
     "grocery", "476", "", "", "", ""),
    ("shop_Point", "shop", "cannabis",
     "A shop legally selling non-medical cannabis products", "Generalization",
     "Building", "AL013", "featureFunction", "FFN",
     "specializedStore", "464", "", "", "", ""),

    # ── telecom – new category ──
    ("telecom_Point", "telecom", "exchange",
     "A telephone exchange building", "Generalization",
     "Building", "AL013", "featureFunction", "FFN",
     "telecommunications", "610", "", "", "", ""),
    ("telecom_Point", "telecom", "data_centre",
     "A data centre", "Generalization",
     "Building", "AL013", "featureFunction", "FFN",
     "telecommunications", "610", "", "", "", ""),

    # ── tourism – new from wiki ──
    ("tourism_Point", "tourism", "camp_pitch",
     "A tent or caravan pitch location within a campsite", "Generalization",
     "CampSite", "AK060", "", "", "", "", "", "", "", ""),

    # ── water – new primary key ──
    ("water_Polygon", "water", "river",
     "The water covered area of a river", "OK",
     "InlandWaterbody", "BH082", "inlandWaterType", "IWT",
     "river", "2", "", "", "", ""),
    ("water_Polygon", "water", "lake",
     "A natural or semi-natural body of still water", "OK",
     "InlandWaterbody", "BH082", "inlandWaterType", "IWT",
     "lake", "1", "", "", "", ""),
    ("water_Polygon", "water", "reservoir",
     "An artificial lake for storing water", "OK",
     "InlandWaterbody", "BH082", "inlandWaterType", "IWT",
     "reservoir", "4", "", "", "", ""),
    ("water_Polygon", "water", "pond",
     "A small body of standing water", "OK",
     "InlandWaterbody", "BH082", "inlandWaterType", "IWT",
     "pond", "3", "", "", "", ""),
    ("water_Polygon", "water", "canal",
     "The area covered by the water of a canal", "OK",
     "Canal", "BH020", "", "", "", "", "", "", "", ""),
    ("water_Polygon", "water", "lagoon",
     "A body of shallow sea water separated by a barrier", "OK",
     "InlandWaterbody", "BH082", "inlandWaterType", "IWT",
     "lagoon", "5", "", "", "", ""),

    # ── waterway – new from wiki ──
    ("waterway_Line", "waterway", "tidal_channel",
     "A natural intertidal waterway in mangroves or salt marshes", "Generalization",
     "River", "BH140", "", "", "", "", "", "", "", ""),
    ("waterway_Line", "waterway", "pressurised",
     "An artificial waterway where water flows in a closed space", "Generalization",
     "Pipeline", "AQ113", "", "", "", "", "", "", "", ""),
    ("waterway_Point", "waterway", "waterfall",
     "A waterfall", "OK",
     "Waterfall", "BH180", "", "", "", "", "", "", "", ""),
]


def build_v3_csv():
    """Main routine."""
    # 1. Load V3 classes
    v3_classes = extract_v3_classes(ILI_FILE)
    print(f"[INFO] V3 model classes found: {len(v3_classes)}")

    # Build case-insensitive lookup
    v3_lower = {c.lower(): c for c in v3_classes}

    # 2. Read V2 CSV
    header, v2_rows = read_v2_csv(CSV_V2)
    print(f"[INFO] V2 CSV rows: {len(v2_rows)}")

    # 3. Index NEW_MAPPINGS by (feature_class, key, value) for quick lookup
    nm_index = {}
    for nm in NEW_MAPPINGS:
        key = (nm[0].strip(), nm[1].strip(), nm[2].strip())
        nm_index[key] = nm

    # 4. For every V2 row, validate / fix DGIF Feature Alpha
    issues = []
    fixed_rows = []
    new_no = 0
    updated_keys = set()  # track which NEW_MAPPINGS got merged into V2 rows

    for row in v2_rows:
        new_no += 1
        # Ensure at least 16 fields
        while len(row) < 16:
            row.append("")

        osm_fc  = row[1].strip()
        osm_key = row[2].strip()
        osm_val = row[3].strip()
        dgif_alpha = row[6].strip()

        # Check if this V2 row is "not in DGIF" but we have a new mapping for it
        nm_key = (osm_fc, osm_key, osm_val)
        if row[5].strip() == "not in DGIF" and nm_key in nm_index:
            nm = nm_index[nm_key]
            if nm[4].strip() != "not in DGIF":  # we have a real mapping now
                issues.append(
                    f"Row {new_no}: UPGRADED '{osm_fc}/{osm_key}={osm_val}' "
                    f"from 'not in DGIF' -> '{nm[4]}' ({nm[5]})"
                )
                row[4] = nm[3]   # definition
                row[5] = nm[4]   # Mapping Description
                row[6] = nm[5]   # DGIF Feature Alpha
                row[7] = nm[6]   # DGIF Feature 531
                row[8] = nm[7]   # attr alpha
                row[9] = nm[8]   # attr 531
                row[10] = nm[9]  # attrval alpha
                row[11] = nm[10] # attrval 531
                row[12] = nm[11] # attr2 alpha
                row[13] = nm[12] # attr2 531
                row[14] = nm[13] # attrval2 alpha
                row[15] = nm[14] # attrval2 531
                dgif_alpha = nm[5]
                updated_keys.add(nm_key)

        # Also check if V2 already has this mapping (OK/Generalization) and
        # NEW_MAPPINGS would be a duplicate
        if nm_key in nm_index:
            updated_keys.add(nm_key)

        if dgif_alpha == "":
            # "not in DGIF" – keep as-is
            row[0] = str(new_no)
            fixed_rows.append(row)
            continue

        # Check rename map first
        if dgif_alpha in RENAME_MAP:
            old = dgif_alpha
            dgif_alpha = RENAME_MAP[dgif_alpha]
            if dgif_alpha is None:
                issues.append(f"Row {new_no}: class '{old}' removed in V3 -> set 'not in DGIF'")
                row[5] = "not in DGIF"
                row[6] = ""
                row[7] = ""
                for i in range(8, 16):
                    row[i] = ""
                row[0] = str(new_no)
                fixed_rows.append(row)
                continue
            else:
                issues.append(f"Row {new_no}: renamed '{old}' -> '{dgif_alpha}'")
                row[6] = dgif_alpha

        # Verify class exists in V3
        if dgif_alpha in v3_classes:
            pass  # OK
        elif dgif_alpha.lower() in v3_lower:
            correct = v3_lower[dgif_alpha.lower()]
            issues.append(f"Row {new_no}: case fix '{dgif_alpha}' -> '{correct}'")
            row[6] = correct
            dgif_alpha = correct
        else:
            # Class not in V3 – flag it
            issues.append(f"Row {new_no}: V2 class '{dgif_alpha}' NOT found in V3 model!")

        row[0] = str(new_no)
        fixed_rows.append(row)

    # 5. Add truly new OSM mappings (not already in V2)
    new_added = 0
    for nm in NEW_MAPPINGS:
        nm_key = (nm[0].strip(), nm[1].strip(), nm[2].strip())
        if nm_key in updated_keys:
            continue  # already merged or was already present
        new_no += 1
        new_added += 1
        row = [str(new_no)] + list(nm)
        while len(row) < 16:
            row.append("")
        fixed_rows.append(row)

    # 6. Write V3 CSV
    with open(CSV_V3, "w", encoding="utf-8-sig", newline="") as f:
        # Write header
        f.write(header + "\n")
        for row in fixed_rows:
            f.write(";".join(row) + "\n")

    print(f"\n[INFO] V3 CSV written: {CSV_V3}")
    print(f"[INFO] Total rows: {len(fixed_rows)} (V2 base: {len(v2_rows)}, truly new: {new_added})")
    print(f"[INFO] Issues / changes: {len(issues)}")

    # Print issues report
    if issues:
        print("\n--- DELTA REPORT (V2 → V3) ---")
        for iss in issues:
            print(f"  • {iss}")

    # 7. Summary statistics
    not_in_dgif = sum(1 for r in fixed_rows if r[5].strip() == "not in DGIF")
    ok_count    = sum(1 for r in fixed_rows if r[5].strip() == "OK")
    gen_count   = sum(1 for r in fixed_rows if r[5].strip() == "Generalization")
    print(f"\n--- MAPPING STATISTICS ---")
    print(f"  OK             : {ok_count}")
    print(f"  Generalization : {gen_count}")
    print(f"  not in DGIF    : {not_in_dgif}")
    print(f"  Total          : {len(fixed_rows)}")

    # 8. List V3 classes NOT referenced by any mapping
    mapped_classes = set()
    for r in fixed_rows:
        c = r[6].strip() if len(r) > 6 else ""
        if c:
            mapped_classes.add(c)
    unmapped = v3_classes - mapped_classes
    if unmapped:
        print(f"\n--- V3 CLASSES NOT MAPPED TO ANY OSM TAG ({len(unmapped)}) ---")
        for c in sorted(unmapped):
            print(f"  • {c}")


if __name__ == "__main__":
    build_v3_csv()
