#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Extract DGFCD and DGRWI concepts from DGIF_BL_2025-1.xmi
and generate INTERLIS 2.4 XML catalogs (CatalogueObjects_V2 format).

Output:
  - output/DGFCD_FeatureConcepts.xml
  - output/DGFCD_AttributeConcepts.xml
  - output/DGFCD_AttributeDataTypes.xml
  - output/DGFCD_AttributeValueConcepts.xml
  - output/DGFCD_RoleConcepts.xml
  - output/DGFCD_UnitsOfMeasure.xml
  - output/DGRWI_RealWorldObjects.xml
"""

import xml.etree.ElementTree as ET
import os
import sys
from collections import OrderedDict

# ── Configuration ──────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
XMI_PATH = os.path.join(BASE_DIR, "ressources", "DGIF_BL_2025-1.xmi")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

# XMI namespaces
NS = {
    "xmi": "http://www.omg.org/spec/XMI/20110701",
    "uml": "http://www.omg.org/spec/UML/20110701",
}

# INTERLIS catalog header info
INTERLIS_VERSION = "2.4"
CATALOG_MODEL = "CatalogueObjects_V2"
SENDER = "DGIF_XMI_Extractor"


def parse_xmi(path):
    """Parse the XMI file and return the root element."""
    print(f"Parsing XMI file: {path}")
    tree = ET.parse(path)
    root = tree.getroot()
    print(f"XMI parsed successfully.")
    return root


def find_package_by_name(parent, name):
    """Find a direct child packagedElement of type uml:Package with given name."""
    for elem in parent:
        tag = elem.tag
        # Handle namespaced and non-namespaced tags
        local_tag = tag.split("}")[-1] if "}" in tag else tag
        if local_tag == "packagedElement":
            xmi_type = elem.get("{http://www.omg.org/spec/XMI/20110701}type", "")
            elem_name = elem.get("name", "")
            if xmi_type == "uml:Package" and elem_name == name:
                return elem
    return None


def find_all_packages_recursive(root, path_names):
    """Navigate through nested packages by name list."""
    current = root
    # First find uml:Model
    for child in root:
        local_tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        if local_tag == "Model" or (local_tag == "packagedElement" and 
            child.get("{http://www.omg.org/spec/XMI/20110701}type", "") == "uml:Model"):
            current = child
            break
    
    for name in path_names:
        found = find_package_by_name(current, name)
        if found is None:
            print(f"  WARNING: Package '{name}' not found under current element")
            return None
        current = found
    return current


def extract_classes(package_elem):
    """Extract all uml:Class elements from a package (direct children only)."""
    classes = []
    if package_elem is None:
        return classes
    for elem in package_elem:
        local_tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        if local_tag == "packagedElement":
            xmi_type = elem.get("{http://www.omg.org/spec/XMI/20110701}type", "")
            if xmi_type == "uml:Class":
                name = elem.get("name", "")
                xmi_id = elem.get("{http://www.omg.org/spec/XMI/20110701}id", "")
                classes.append({"name": name, "xmi_id": xmi_id, "element": elem})
    return classes


def extract_attribute_concepts(package_elem):
    """Extract AttributeConcepts: class name + datatype reference."""
    concepts = []
    if package_elem is None:
        return concepts
    for elem in package_elem:
        local_tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        if local_tag == "packagedElement":
            xmi_type = elem.get("{http://www.omg.org/spec/XMI/20110701}type", "")
            if xmi_type == "uml:Class":
                name = elem.get("name", "")
                xmi_id = elem.get("{http://www.omg.org/spec/XMI/20110701}id", "")
                # Find datatype reference
                datatype_ref = ""
                for attr in elem:
                    attr_tag = attr.tag.split("}")[-1] if "}" in attr.tag else attr.tag
                    if attr_tag == "ownedAttribute" and attr.get("name") == "datatype":
                        type_elem = attr.find("type")
                        if type_elem is not None:
                            datatype_ref = type_elem.get("{http://www.omg.org/spec/XMI/20110701}idref", "")
                        else:
                            # type might be referenced via xmi:idref on a child
                            for sub in attr:
                                sub_tag = sub.tag.split("}")[-1] if "}" in sub.tag else sub.tag
                                if sub_tag == "type":
                                    datatype_ref = sub.get("{http://www.omg.org/spec/XMI/20110701}idref", 
                                                          sub.get("xmi:idref", ""))
                concepts.append({
                    "name": name,
                    "xmi_id": xmi_id,
                    "datatype_ref": datatype_ref
                })
    return concepts


def extract_attribute_value_concepts(package_elem):
    """Extract AttributeValueConcepts: sub-packages containing enumeration values."""
    avc_list = []
    if package_elem is None:
        return avc_list
    for elem in package_elem:
        local_tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        if local_tag == "packagedElement":
            xmi_type = elem.get("{http://www.omg.org/spec/XMI/20110701}type", "")
            if xmi_type == "uml:Package":
                pkg_name = elem.get("name", "")
                values = []
                for sub in elem:
                    sub_tag = sub.tag.split("}")[-1] if "}" in sub.tag else sub.tag
                    if sub_tag == "packagedElement":
                        sub_type = sub.get("{http://www.omg.org/spec/XMI/20110701}type", "")
                        if sub_type == "uml:Class":
                            values.append(sub.get("name", ""))
                avc_list.append({
                    "attribute_name": pkg_name,
                    "values": values
                })
    return avc_list


def extract_dgrwi(package_elem):
    """Extract DGRWI: RWO classes with their Dependency targets."""
    rwos = []
    if package_elem is None:
        return rwos
    # Build a map: client_id -> list of supplier_ids from Dependency elements
    dep_map = {}
    for elem in package_elem:
        local_tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        if local_tag == "packagedElement":
            xmi_type = elem.get("{http://www.omg.org/spec/XMI/20110701}type", "")
            if xmi_type == "uml:Dependency":
                client = elem.get("client", "")
                supplier = elem.get("supplier", "")
                if client:
                    dep_map.setdefault(client, []).append(supplier)

    # Extract classes with their dependencies
    for elem in package_elem:
        local_tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        if local_tag == "packagedElement":
            xmi_type = elem.get("{http://www.omg.org/spec/XMI/20110701}type", "")
            if xmi_type == "uml:Class":
                name = elem.get("name", "")
                xmi_id = elem.get("{http://www.omg.org/spec/XMI/20110701}id", "")
                suppliers = dep_map.get(xmi_id, [])
                rwos.append({
                    "name": name,
                    "xmi_id": xmi_id,
                    "feature_refs": suppliers
                })
    return rwos


def build_id_name_map(root):
    """Build a map from xmi:id -> name for all packagedElement in the XMI."""
    id_map = {}
    for elem in root.iter():
        local_tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        if local_tag == "packagedElement":
            xmi_id = elem.get("{http://www.omg.org/spec/XMI/20110701}id", "")
            name = elem.get("name", "")
            if xmi_id and name:
                id_map[xmi_id] = name
    return id_map


# ── INTERLIS XML Catalog Writers ───────────────────────────────────────────

def xml_header(model_name, topic_name):
    """Create the INTERLIS XML transfer header."""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<!-- INTERLIS {INTERLIS_VERSION} XML Catalog - {topic_name} -->\n'
        f'<!-- Generated from DGIF_BL_2025-1.xmi -->\n'
        f'<TRANSFER xmlns="http://www.interlis.ch/INTERLIS2.4">\n'
        f'  <HEADERSECTION SENDER="{SENDER}" VERSION="{INTERLIS_VERSION}">\n'
        f'    <MODELS>\n'
        f'      <MODEL NAME="{model_name}" VERSION="2025-1" URI="https://dgiwg.org/dgif"/>\n'
        f'    </MODELS>\n'
        f'  </HEADERSECTION>\n'
        f'  <DATASECTION>\n'
    )


def xml_footer():
    return (
        '  </DATASECTION>\n'
        '</TRANSFER>\n'
    )


def write_simple_catalog(filepath, model_name, topic_name, basket_id, entries, entry_tag="Entry"):
    """Write a simple catalog with Code/Name entries."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(xml_header(model_name, topic_name))
        f.write(f'    <{model_name}.{topic_name} BID="{basket_id}">\n')
        for i, entry in enumerate(entries, 1):
            tid = f"{basket_id}.{i}"
            name = entry if isinstance(entry, str) else entry.get("name", "")
            f.write(f'      <{model_name}.{topic_name}.{entry_tag} TID="{tid}">\n')
            f.write(f'        <Code>{name}</Code>\n')
            f.write(f'        <Name>{name}</Name>\n')
            f.write(f'      </{model_name}.{topic_name}.{entry_tag}>\n')
        f.write(f'    </{model_name}.{topic_name}>\n')
        f.write(xml_footer())
    print(f"  Written: {filepath} ({len(entries)} entries)")


def write_attribute_concepts_catalog(filepath, model_name, concepts, id_map):
    """Write AttributeConcepts catalog with datatype info."""
    topic = "DGFCD_AttributeConcepts"
    basket_id = "DGFCD.AttributeConcepts"
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(xml_header(model_name, topic))
        f.write(f'    <{model_name}.{topic} BID="{basket_id}">\n')
        for i, c in enumerate(concepts, 1):
            tid = f"{basket_id}.{i}"
            dt_name = id_map.get(c["datatype_ref"], c["datatype_ref"])
            f.write(f'      <{model_name}.{topic}.AttributeConcept TID="{tid}">\n')
            f.write(f'        <Code>{c["name"]}</Code>\n')
            f.write(f'        <Name>{c["name"]}</Name>\n')
            f.write(f'        <DataType>{dt_name}</DataType>\n')
            f.write(f'      </{model_name}.{topic}.AttributeConcept>\n')
        f.write(f'    </{model_name}.{topic}>\n')
        f.write(xml_footer())
    print(f"  Written: {filepath} ({len(concepts)} entries)")


def write_attribute_value_concepts_catalog(filepath, model_name, avc_list):
    """Write AttributeValueConcepts catalog with enumeration values."""
    topic = "DGFCD_AttributeValueConcepts"
    basket_id = "DGFCD.AttributeValueConcepts"
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(xml_header(model_name, topic))
        f.write(f'    <{model_name}.{topic} BID="{basket_id}">\n')
        entry_num = 0
        for avc in avc_list:
            for val in avc["values"]:
                entry_num += 1
                tid = f"{basket_id}.{entry_num}"
                f.write(f'      <{model_name}.{topic}.AttributeValue TID="{tid}">\n')
                f.write(f'        <AttributeCode>{avc["attribute_name"]}</AttributeCode>\n')
                f.write(f'        <Code>{val}</Code>\n')
                f.write(f'        <Name>{val}</Name>\n')
                f.write(f'      </{model_name}.{topic}.AttributeValue>\n')
        f.write(f'    </{model_name}.{topic}>\n')
        f.write(xml_footer())
    print(f"  Written: {filepath} ({entry_num} entries)")


def write_dgrwi_catalog(filepath, model_name, rwos, id_map):
    """Write DGRWI catalog with feature references."""
    topic = "DGRWI_RealWorldObjects"
    basket_id = "DGRWI.RealWorldObjects"
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(xml_header(model_name, topic))
        f.write(f'    <{model_name}.{topic} BID="{basket_id}">\n')
        for i, rwo in enumerate(rwos, 1):
            tid = f"{basket_id}.{i}"
            f.write(f'      <{model_name}.{topic}.RealWorldObject TID="{tid}">\n')
            f.write(f'        <Code>{rwo["name"]}</Code>\n')
            f.write(f'        <Name>{rwo["name"]}</Name>\n')
            if rwo["feature_refs"]:
                refs = ", ".join(id_map.get(r, r) for r in rwo["feature_refs"])
                f.write(f'        <FeatureTypeRefs>{refs}</FeatureTypeRefs>\n')
            f.write(f'      </{model_name}.{topic}.RealWorldObject>\n')
        f.write(f'    </{model_name}.{topic}>\n')
        f.write(xml_footer())
    print(f"  Written: {filepath} ({len(rwos)} entries)")


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    if not os.path.exists(XMI_PATH):
        print(f"ERROR: XMI file not found: {XMI_PATH}")
        sys.exit(1)

    root = parse_xmi(XMI_PATH)
    
    # Build global ID → name map
    print("Building ID→Name map...")
    id_map = build_id_name_map(root)
    print(f"  Mapped {len(id_map)} elements.")

    model_name = "DGIF"

    # ── DGFCD ──────────────────────────────────────────────────────────────
    print("\n═══ DGFCD Extraction ═══")

    # 1. FeatureConcepts
    print("\n─ FeatureConcepts ─")
    pkg = find_all_packages_recursive(root, ["DGIF", "DGFCD", "FeatureConcepts"])
    fc = extract_classes(pkg)
    print(f"  Found {len(fc)} feature concepts")
    write_simple_catalog(
        os.path.join(OUTPUT_DIR, "DGFCD_FeatureConcepts.xml"),
        model_name, "DGFCD_FeatureConcepts", "DGFCD.FeatureConcepts",
        fc, entry_tag="FeatureConcept"
    )

    # 2. AttributeConcepts
    print("\n─ AttributeConcepts ─")
    pkg = find_all_packages_recursive(root, ["DGIF", "DGFCD", "AttributeConcepts"])
    ac = extract_attribute_concepts(pkg)
    print(f"  Found {len(ac)} attribute concepts")
    write_attribute_concepts_catalog(
        os.path.join(OUTPUT_DIR, "DGFCD_AttributeConcepts.xml"),
        model_name, ac, id_map
    )

    # 3. AttributeDataTypes
    print("\n─ AttributeDataTypes ─")
    pkg = find_all_packages_recursive(root, ["DGIF", "DGFCD", "AttributeDataTypes"])
    adt = extract_classes(pkg)
    print(f"  Found {len(adt)} attribute data types")
    write_simple_catalog(
        os.path.join(OUTPUT_DIR, "DGFCD_AttributeDataTypes.xml"),
        model_name, "DGFCD_AttributeDataTypes", "DGFCD.AttributeDataTypes",
        adt, entry_tag="DataType"
    )

    # 4. AttributeValueConcepts
    print("\n─ AttributeValueConcepts ─")
    pkg = find_all_packages_recursive(root, ["DGIF", "DGFCD", "AttributeValueConcepts"])
    avc = extract_attribute_value_concepts(pkg)
    total_vals = sum(len(a["values"]) for a in avc)
    print(f"  Found {len(avc)} attribute enumerations with {total_vals} total values")
    write_attribute_value_concepts_catalog(
        os.path.join(OUTPUT_DIR, "DGFCD_AttributeValueConcepts.xml"),
        model_name, avc
    )

    # 5. RoleConcepts
    print("\n─ RoleConcepts ─")
    pkg = find_all_packages_recursive(root, ["DGIF", "DGFCD", "RoleConcepts"])
    rc = extract_classes(pkg)
    print(f"  Found {len(rc)} role concepts")
    write_simple_catalog(
        os.path.join(OUTPUT_DIR, "DGFCD_RoleConcepts.xml"),
        model_name, "DGFCD_RoleConcepts", "DGFCD.RoleConcepts",
        rc, entry_tag="RoleConcept"
    )

    # 6. UnitsOfMeasure
    print("\n─ UnitsOfMeasure ─")
    pkg = find_all_packages_recursive(root, ["DGIF", "DGFCD", "UnitsOfMeasure"])
    uom = extract_classes(pkg)
    print(f"  Found {len(uom)} units of measure")
    write_simple_catalog(
        os.path.join(OUTPUT_DIR, "DGFCD_UnitsOfMeasure.xml"),
        model_name, "DGFCD_UnitsOfMeasure", "DGFCD.UnitsOfMeasure",
        uom, entry_tag="UnitOfMeasure"
    )

    # ── DGRWI ──────────────────────────────────────────────────────────────
    print("\n═══ DGRWI Extraction ═══")
    pkg = find_all_packages_recursive(root, ["DGIF", "DGRWI"])
    rwos = extract_dgrwi(pkg)
    print(f"  Found {len(rwos)} real-world objects")
    write_dgrwi_catalog(
        os.path.join(OUTPUT_DIR, "DGRWI_RealWorldObjects.xml"),
        model_name, rwos, id_map
    )

    # ── Summary ────────────────────────────────────────────────────────────
    print("\n═══ Summary ═══")
    print(f"  DGFCD FeatureConcepts:       {len(fc)}")
    print(f"  DGFCD AttributeConcepts:     {len(ac)}")
    print(f"  DGFCD AttributeDataTypes:    {len(adt)}")
    print(f"  DGFCD AttributeValueConcepts:{len(avc)} enums, {total_vals} values")
    print(f"  DGFCD RoleConcepts:          {len(rc)}")
    print(f"  DGFCD UnitsOfMeasure:        {len(uom)}")
    print(f"  DGRWI RealWorldObjects:      {len(rwos)}")
    print(f"\nAll catalogs written to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
