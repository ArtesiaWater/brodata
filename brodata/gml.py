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


def polygon_from_gml(polygon_node):
    """Convert GML Polygon node to Shapely Polygon"""
    # Exterior
    exterior_text = polygon_node.findtext(
        ".//gml:outerBoundaryIs/gml:LinearRing/gml:coordinates", namespaces=ns
    )
    if not exterior_text:
        exterior_text = polygon_node.findtext(
            ".//gml:exterior/gml:LinearRing/gml:coordinates", namespaces=ns
        )
    exterior = parse_coordinates(exterior_text)

    # Interiors
    interiors = []
    for inner in polygon_node.findall(
        ".//gml:innerBoundaryIs/gml:LinearRing/gml:coordinates", namespaces=ns
    ):
        interiors.append(parse_coordinates(inner.text))

    return Polygon(exterior, interiors)


def multisurface_from_gml(ms_node):
    """Convert GML MultiSurface node to Shapely MultiPolygon"""
    polygons = []
    for member in ms_node.findall(".//gml:surfaceMember", namespaces=ns):
        poly_node = member.find(".//gml:Polygon", namespaces=ns)
        if poly_node is not None:
            polygons.append(polygon_from_gml(poly_node))
    return MultiPolygon(polygons)


def parse_geometry(node):
    """Parse any GML geometry node to Shapely"""
    tag = node.tag.split("}")[-1]
    if tag == "Point":
        coords = parse_coordinates(node.findtext(".//gml:pos", namespaces=ns))
        return Point(coords[0])
    elif tag == "LineString":
        coords = parse_coordinates(node.findtext(".//gml:coordinates", namespaces=ns))
        return LineString(coords)
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
                parse_coordinates(l.findtext(".//gml:coordinates", namespaces=ns))
            )
            for l in node.findall(
                ".//gml:lineStringMember/gml:LineString", namespaces=ns
            )
        ]
        return MultiLineString(lines)
    elif tag == "MultiPoint":
        points = [
            Point(parse_coordinates(p.findtext(".//gml:coordinates", namespaces=ns))[0])
            for p in node.findall(".//gml:pointMember/gml:Point", namespaces=ns)
        ]
        return MultiPoint(points)
    else:
        raise NotImplementedError(f"GML type {tag} not supported")
