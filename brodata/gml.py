from shapely.geometry import (
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
)


ns = {"gml": "http://www.opengis.net/gml/3.2"}


def parse_coordinates(text):
    """Convert GML coordinates string into list of tuples"""
    return [tuple(map(float, c.split(","))) for c in text.strip().split()]


def parse_poslist(poslist_text, dim=2):
    """Convert a GML posList string to list of coordinate tuples (2D or 3D)."""
    numbers = list(map(float, poslist_text.strip().split()))
    if len(numbers) % dim != 0:
        raise ValueError(f"Number of coordinates not divisible by dimension {dim}")
    return [tuple(numbers[i : i + dim]) for i in range(0, len(numbers), dim)]


def polygon_from_gml(polygon_node, dim=2):
    """Convert GML 3.2 Polygon or PolygonPatch node to Shapely Polygon"""
    exterior_text = polygon_node.findtext(
        ".//gml:exterior/gml:LinearRing/gml:posList", namespaces=ns
    )
    if not exterior_text:
        raise ValueError("Polygon has no exterior")
    exterior = parse_poslist(exterior_text, dim=dim)

    interiors = []
    for inner in polygon_node.findall(
        ".//gml:interior/gml:LinearRing/gml:posList", namespaces=ns
    ):
        interiors.append(parse_poslist(inner.text, dim=dim))

    return Polygon(exterior, interiors)


def multisurface_from_gml(ms_node):
    """Convert GML 3.2 MultiSurface to Shapely MultiPolygon (supports Surface with PolygonPatch)"""
    polygons = []

    for member in ms_node.findall(".//gml:surfaceMember", namespaces=ns):
        # Look for Surface inside surfaceMember
        surface_node = member.find(".//gml:Surface", namespaces=ns)
        if surface_node is None:
            continue  # skip if no surface found

        # Each Surface can have multiple PolygonPatch under patches
        for patch in surface_node.findall(".//gml:PolygonPatch", namespaces=ns):
            polygons.append(polygon_from_gml(patch))

    return MultiPolygon(polygons)


def parse_geometry(node):
    """Parse any GML 3.2 geometry node to Shapely"""
    tag = node.tag.split("}")[-1]
    if tag == "Point":
        pos_text = node.findtext(".//gml:pos", namespaces=ns)
        coords = tuple(map(float, pos_text.split()))
        return Point(coords)
    elif tag == "LineString":
        pos_list = node.findtext(".//gml:posList", namespaces=ns)
        points = [tuple(map(float, p.split())) for p in pos_list.strip().split()]
        return LineString(points)
    elif tag == "Polygon":
        return polygon_from_gml(node)
    elif tag == "MultiSurface":
        return multisurface_from_gml(node)
    elif tag == "MultiPolygon":
        polygons = [
            polygon_from_gml(p)
            for p in node.findall(".//gml:polygonMember/gml:Polygon", namespaces=ns)
        ]
        return MultiPolygon(polygons)
    elif tag == "MultiLineString":
        lines = [
            LineString(
                [
                    tuple(map(float, c.split()))
                    for c in l.findtext(".//gml:posList", namespaces=ns).strip().split()
                ]
            )
            for l in node.findall(
                ".//gml:lineStringMember/gml:LineString", namespaces=ns
            )
        ]
        return MultiLineString(lines)
    elif tag == "MultiPoint":
        points = [
            Point(tuple(map(float, p.findtext(".//gml:pos", namespaces=ns).split())))
            for p in node.findall(".//gml:pointMember/gml:Point", namespaces=ns)
        ]
        return MultiPoint(points)
    else:
        raise NotImplementedError(f"GML type {tag} not supported")
