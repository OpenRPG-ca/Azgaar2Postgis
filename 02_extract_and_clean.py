import xml.etree.ElementTree as ET
from svgpathtools import parse_path
import geojson
from shapely.geometry import Polygon, mapping, LineString
from shapely.ops import unary_union
from tqdm import tqdm
import re
import os
import json
import sys
import csv
import re
from typing import Optional
import logging

# ===========================
# Logging Configuration
# ===========================
logging.basicConfig(
    filename="data-loader.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ===========================
# Constants and Configurations
# ===========================

# Paths to input and output files
HOME_DIR = "/srv/data-loader"
DATA_DIR = os.path.join(HOME_DIR, "data")
SVG_FILE = os.path.join(DATA_DIR, "openheim.svg")
LAND_OUTPUT_FILE = os.path.join(DATA_DIR, "openheim_land_cleaned.geojson")
RIVERS_OUTPUT_FILE = os.path.join(DATA_DIR, "openheim_rivers_cleaned.geojson")
SVG_NAMESPACE = "{http://www.w3.org/2000/svg}"
XLINK_NAMESPACE = "{http://www.w3.org/1999/xlink}"
SVG_HEIGHT = 2000  # Set to your SVG's height

FILES_TO_CLEAN = [
    os.path.join(DATA_DIR, "/srv/data-loader/data/cells.geojson"),
    os.path.join(DATA_DIR, "/srv/data-loader/data/markers.geojson"),
    # os.path.join(DATA_DIR, "/srv/data-loader/data/rivers.geojson"), commented out beacause i am using svg file for river
    os.path.join(DATA_DIR, "/srv/data-loader/data/routes.geojson"),
    LAND_OUTPUT_FILE,
    RIVERS_OUTPUT_FILE,
]

# ===========================
# Utility Functions
# ===========================


def svg_path_to_coords(d_str):
    path = parse_path(d_str)
    coords = []
    for seg in path:
        if hasattr(seg, "start"):
            coords.append((seg.start.real, seg.start.imag))
        if hasattr(seg, "end"):
            coords.append((seg.end.real, seg.end.imag))
    return coords


def deduplicate(coords):
    if not coords:
        return coords
    deduped = [coords[0]]
    for pt in coords[1:]:
        if pt != deduped[-1]:
            deduped.append(pt)
    return deduped


def ensure_closed(coords):
    if coords and coords[0] != coords[-1]:
        coords.append(coords[0])
    return coords


def flip_y(coords):
    return [(x, SVG_HEIGHT - y) for x, y in coords]


def flip_y_coords_in_feature(feature):
    geom = feature.get("geometry")
    if not geom or "coordinates" not in geom:
        return

    coords = geom["coordinates"]
    geom_type = geom.get("type")

    if geom_type == "Point":
        x, y = coords
        geom["coordinates"] = (x, SVG_HEIGHT - y)
    elif geom_type == "LineString":
        geom["coordinates"] = flip_y(coords)
    elif geom_type == "Polygon":
        geom["coordinates"] = [flip_y(ring) for ring in coords]
    elif geom_type == "MultiLineString":
        geom["coordinates"] = [flip_y(line) for line in coords]
    elif geom_type == "MultiPolygon":
        geom["coordinates"] = [[flip_y(ring) for ring in poly] for poly in coords]


def strip_river_prefix_and_make_int(feature_id):
    if isinstance(feature_id, str) and feature_id.startswith("river"):
        number_part = feature_id.replace("river", "")
        if number_part.isdigit():
            return int(number_part)
    return feature_id


def strip_marker_prefix_and_make_int(feature_id):
    """
    If feature_id is a string starting with 'marker' followed by digits,
    returns the integer part. Otherwise, returns feature_id unchanged.
    """
    if isinstance(feature_id, str) and feature_id.startswith("marker"):
        number_part = feature_id.replace("marker", "")
        if number_part.isdigit():
            return int(number_part)
    return feature_id


def clean_id(val):
    if isinstance(val, str):
        m = re.search(r"(\d+)$", val)
        if m:
            return int(m.group(1))
    if isinstance(val, int):
        return val
    return None


# ===========================
# Core Data Extraction Functions
# ===========================


def extract_river_paths(root):
    river_features = []
    for path_elem in root.findall(f".//{SVG_NAMESPACE}path"):
        path_id = path_elem.attrib.get("id")
        d = path_elem.attrib.get("d")
        if not d or not path_id or not path_id.startswith("river"):
            continue
        coords = svg_path_to_coords(d)
        coords = deduplicate(coords)
        coords = flip_y(coords)
        if len(coords) > 1:
            line = LineString(coords)
            river_id = strip_river_prefix_and_make_int(path_id)
            river_features.append(
                geojson.Feature(
                    geometry=mapping(line), properties={"id": river_id, "type": "river"}
                )
            )
    logger.info(f"Extracted {len(river_features)} river features from SVG.")
    return river_features


def extract_land_and_freshwater(root):
    # Find land mask and referenced feature IDs
    land_mask = root.find(f".//{SVG_NAMESPACE}mask[@id='land']")
    land_ids = []
    if land_mask is not None:
        for use in land_mask.findall(f"{SVG_NAMESPACE}use"):
            href = use.attrib.get(f"{XLINK_NAMESPACE}href")
            if href and use.attrib.get("fill") == "white":
                land_ids.append(href.lstrip("#"))

    # Find freshwater lake IDs from <g id="freshwater">/<use>
    freshwater_root = root.find(f".//{SVG_NAMESPACE}g[@id='freshwater']")
    freshwater_ids = []
    if freshwater_root is not None:
        for use in freshwater_root.findall(f".//{SVG_NAMESPACE}use"):
            href = use.attrib.get(f"{XLINK_NAMESPACE}href")
            if href:
                freshwater_ids.append(href.lstrip("#"))

    land_polys = []
    freshwater_polys = []
    for path_elem in root.findall(f".//{SVG_NAMESPACE}path"):
        path_id = path_elem.attrib.get("id")
        d = path_elem.attrib.get("d")
        if not d:
            continue
        coords = svg_path_to_coords(d)
        coords = deduplicate(coords)
        coords = ensure_closed(coords)
        coords = flip_y(coords)
        if len(coords) > 3:
            poly = Polygon(coords)
            if not poly.is_valid:
                poly = poly.buffer(0)
            if path_id in land_ids:
                land_polys.append((path_id, poly))
            elif path_id in freshwater_ids:
                freshwater_polys.append((path_id, poly))
    logger.info(
        f"Extracted {len(land_polys)} land polygons and {len(freshwater_polys)} freshwater polygons."
    )
    return land_polys, freshwater_polys


def make_land_features(land_polys, freshwater_polys):
    all_freshwater = (
        unary_union([poly for _, poly in freshwater_polys])
        if freshwater_polys
        else None
    )
    land_features = []
    for land_id, land_poly in tqdm(land_polys, desc="Processing land polygons"):
        if all_freshwater:
            land_with_holes = land_poly.difference(all_freshwater)
        else:
            land_with_holes = land_poly
        if land_with_holes.geom_type == "Polygon":
            geoms = [land_with_holes]
        elif land_with_holes.geom_type == "MultiPolygon":
            geoms = list(land_with_holes.geoms)
        else:
            continue
        for geom in geoms:
            land_id_clean = strip_river_prefix_and_make_int(land_id)
            land_features.append(
                geojson.Feature(
                    geometry=mapping(geom),
                    properties={"id": land_id_clean, "type": "land"},
                )
            )
    logger.info(f"Created {len(land_features)} land features (with freshwater holes).")
    return land_features


# ===========================
# Cleaning & Validation
# ===========================


def clean_file(infile):
    if not os.path.exists(infile):
        logger.warning(f"File does not exist and will be skipped: {infile}")
        return

    with open(infile, "r") as f:
        data = json.load(f)

    changed = False
    for feat in data.get("features", []):
        old_id = feat["properties"].get("id")
        if "markers" in infile:
            new_id = strip_marker_prefix_and_make_int(old_id)
        elif "rivers" in infile:
            new_id = strip_river_prefix_and_make_int(old_id)
        else:
            new_id = clean_id(old_id)
        if new_id != old_id:
            feat["properties"]["id"] = new_id
            changed = True

        # Flip Y coordinates for these types
        if any(key in infile for key in ["markers", "routes", "cells"]):
            flip_y_coords_in_feature(feat)
            changed = True

    if changed:
        with open(infile, "w") as f:
            json.dump(data, f, indent=2)
        logger.info(f"Cleaned and flipped (if needed): {infile}")
    else:
        logger.info(f"No change needed: {infile}")


def validate_geojson_file(filepath, allowed_types):
    errors = []
    if not os.path.exists(filepath):
        logger.warning(f"Validation skipped (file not found): {filepath}")
        return
    with open(filepath, "r") as f:
        data = json.load(f)
    for i, feat in enumerate(data.get("features", [])):
        geom_type = feat.get("geometry", {}).get("type")
        if geom_type not in allowed_types:
            errors.append(
                f"{filepath} feature {i} has geometry type '{geom_type}', expected {allowed_types}"
            )
    if errors:
        logger.error(f"VALIDATION ERRORS in {filepath}:")
        for err in errors:
            logger.error("  " + err)
        logger.error("Validation failed.")
        sys.exit(1)
    else:
        logger.info(f"Validation passed: {filepath}")


def clean_markers_csv(input_csv, output_csv):
    """
    Reads a CSV, removes 'marker' from the Id column (leaving only the number), and writes to output_csv.
    """
    with open(input_csv, "r", newline="", encoding="utf-8") as infile, open(
        output_csv, "w", newline="", encoding="utf-8"
    ) as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in reader:
            id_val = row.get("Id")
            if isinstance(id_val, str):
                match = re.match(r"marker\s*(\d+)", id_val.strip())
                if match:
                    row["Id"] = match.group(1)
            writer.writerow(row)
    logger.info(f"Cleaned markers csv: {input_csv} -> {output_csv}")


def extract_km(value: str) -> Optional[float]:
    """Extracts the float value in kilometers from a string like '62 km'."""
    if not value:
        return None
    match = re.search(r"([0-9.]+)\s*km", value)
    return float(match.group(1)) if match else None


def extract_cms(value: str) -> Optional[float]:
    """Extracts the float value in m³/s from a string like '8523 m³/s'."""
    if not value:
        return None
    match = re.search(r"([0-9.]+)\s*m³/s", value)
    return float(match.group(1)) if match else None


def clean_rivers_csv(input_file: str, output_file: str):
    """
    Reads a rivers.csv, cleans Length, Width, Discharge columns (removes units, converts to float),
    and writes the result to a new CSV.
    """
    with open(input_file, "r", newline="", encoding="utf-8") as infile, open(
        output_file, "w", newline="", encoding="utf-8"
    ) as outfile:

        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            if "Length" in row:
                row["Length"] = extract_km(row["Length"])
            if "Width" in row:
                row["Width"] = extract_km(row["Width"])
            if "Discharge" in row:
                row["Discharge"] = extract_cms(row["Discharge"])
            writer.writerow(row)
    logger.info(f"Cleaned rivers csv: {input_file} -> {output_file}")


# ===========================
# Main Script Logic
# ===========================
def main():
    try:
        logger.info(f"Parsing SVG file: {SVG_FILE}")
        with open(SVG_FILE, "r", encoding="utf-8") as f:
            svg_content = f.read()
        root = ET.fromstring(svg_content)

        land_polys, freshwater_polys = extract_land_and_freshwater(root)
        land_features = make_land_features(land_polys, freshwater_polys)

        # Write land GeoJSON
        land_fc = geojson.FeatureCollection(land_features)
        with open(LAND_OUTPUT_FILE, "w") as f:
            geojson.dump(land_fc, f, indent=2)
        logger.info(f"Exported land features to {LAND_OUTPUT_FILE}")

        river_features = extract_river_paths(root)
        with open(RIVERS_OUTPUT_FILE, "w") as rf:
            geojson.dump(geojson.FeatureCollection(river_features), rf, indent=2)
        logger.info(f"Exported rivers to {RIVERS_OUTPUT_FILE}")

        for fname in FILES_TO_CLEAN:
            clean_file(fname)

        validate_geojson_file(LAND_OUTPUT_FILE, {"Polygon", "MultiPolygon"})
        validate_geojson_file(RIVERS_OUTPUT_FILE, {"LineString"})

        clean_rivers_csv(
            os.path.join(DATA_DIR, "rivers.csv"),
            os.path.join(DATA_DIR, "rivers_cleaned.csv"),
        )
        clean_markers_csv(
            os.path.join(DATA_DIR, "markers.csv"),
            os.path.join(DATA_DIR, "markers_cleaned.csv"),
        )

    except Exception as e:
        logger.exception("Fatal error during extract_and_clean execution")
        sys.exit(1)


if __name__ == "__main__":
    main()
