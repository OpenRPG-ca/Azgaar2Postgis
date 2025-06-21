import re
from svgpathtools import parse_path


def flip_y_recursive(coords, SVG_HEIGHT):
    """
    Recursively flip Y coordinates for any depth of nested coordinate arrays,
    as used for Points, LineStrings, Polygons, MultiPolygons, etc.
    """
    if isinstance(coords, (list, tuple)):
        if len(coords) == 2 and all(isinstance(val, (int, float)) for val in coords):
            x, y = coords
            return [x, SVG_HEIGHT - y]
        return [flip_y_recursive(c, SVG_HEIGHT) for c in coords]
    return coords


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


def flip_y(coords, SVG_HEIGHT=1000):
    return [(x, SVG_HEIGHT - y) for x, y in coords]


def flip_y_coords_in_feature(feature, SVG_HEIGHT=1000):
    geom = feature.get("geometry")
    if not geom or "coordinates" not in geom:
        return

    coords = geom["coordinates"]
    geom["coordinates"] = flip_y_recursive(coords, SVG_HEIGHT)


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
