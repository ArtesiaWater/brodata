from shapely.geometry import (
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
)


# GML 3.2 namespace
ns = {"gml": "http://www.opengis.net/gml/3.2"}


def parse_poslist(poslist_text, dim=2):
    """Convert GML posList string to list of coordinate tuples (2D or 3D)."""
    numbers = list(map(float, poslist_text.strip().split()))
    if len(numbers) % dim != 0:
        raise ValueError(f"Number of coordinates not divisible by dimension {dim}")
    return [tuple(numbers[i : i + dim]) for i in range(0, len(numbers), dim)]


def polygon_from_gml(polygon_node, dim=2):
    """Convert GML 3.2 Polygon or PolygonPatch node to Shapely Polygon."""
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


def multisurface_from_gml(ms_node, dim=2):
    """Convert GML 3.2 MultiSurface node to Shapely MultiPolygon (supports Surface/PolygonPatch)."""
    polygons = []
    for member in ms_node.findall(".//gml:surfaceMember", namespaces=ns):
        surface_node = member.find(".//gml:Surface", namespaces=ns)
        if surface_node is not None:
            for patch in surface_node.findall(".//gml:PolygonPatch", namespaces=ns):
                polygons.append(polygon_from_gml(patch, dim=dim))
        else:
            poly_node = member.find(".//gml:Polygon", namespaces=ns)
            if poly_node is not None:
                polygons.append(polygon_from_gml(poly_node, dim=dim))
    return MultiPolygon(polygons)


def multigeometry_from_gml(mg_node, dim=2):
    """Convert GML 3.2 MultiGeometry to Shapely GeometryCollection."""
    geometries = []
    for member in mg_node.findall(".//gml:geometryMember", namespaces=ns):
        for child in list(member):
            geometries.append(parse_geometry(child, dim=dim))
    return GeometryCollection(geometries)


def parse_geometry(node, dim=2):
    """Parse any GML 3.2 geometry node to Shapely."""
    tag = node.tag.split("}")[-1]

    if tag == "Point":
        pos_text = node.findtext(".//gml:pos", namespaces=ns)
        coords = tuple(map(float, pos_text.strip().split()))
        return Point(coords)

    elif tag == "LineString":
        pos_list = node.findtext(".//gml:posList", namespaces=ns)
        return LineString(parse_poslist(pos_list, dim=dim))

    elif tag in ("Polygon", "PolygonPatch"):
        return polygon_from_gml(node, dim=dim)

    elif tag == "MultiSurface":
        return multisurface_from_gml(node, dim=dim)

    elif tag == "MultiPolygon":
        polygons = [
            polygon_from_gml(p, dim=dim)
            for p in node.findall(".//gml:polygonMember/gml:Polygon", namespaces=ns)
        ]
        return MultiPolygon(polygons)

    elif tag == "MultiLineString":
        lines = [
            LineString(
                parse_poslist(l.findtext(".//gml:posList", namespaces=ns), dim=dim)
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

    elif tag == "MultiGeometry":
        return multigeometry_from_gml(node, dim=dim)

    else:
        raise NotImplementedError(f"GML type {tag} not supported")
