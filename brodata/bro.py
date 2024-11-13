import logging
import requests
import xml
import types
import pandas as pd
from pyproj import Transformer
import geopandas as gpd

logger = logging.getLogger(__name__)


# %%
def get_characteristics(
    cl,
    tmin=None,
    tmax=None,
    extent=None,
    x=None,
    y=None,
    radius=1000.0,
    epsg=28992,
    to_file=None,
    use_all_corners_of_extent=True,
):
    """
    Haalt de karakteristieken op van een set van registratie objecten, gegeven een
    kenmerkenverzameling (kenset).

    De karakteristieken geven een samenvatting van een object zodat een verdere selectie
    gemaakt kan worden. Het past in een tweetrapsbenadering, waarbij de eerste stap
    bestaat uit het ophalen van de karakteristieken en de 2e stap uit het ophalen van de
    gewenste registratie objecten. Het resultaat van deze operatie is gemaximaliseerd op
    2000.

    Parameters
    ----------
    cl : class
        The brodata class for which characteristics are requested.
    tmin : str or pd.Timestamp, optional
        The minimum registrationPeriod of the requested characteristics. The default is None.
    tmax : str or pd.Timestamp, optional
        The maximum registrationPeriod of the requested characteristics. The default is None.
    extent : list or tuple of 4 floats, optional
        Download the characteristics within extent ([xmin, xmax, ymin, ymax]). The
        default is None.
    x : float, optional
        The x-coordinate of the center of the circle in which the characteristics are
        requested. The default is None.
    y : float, optional
        The y-coordinate of the center of the circle in which the characteristics are
        requested. The default is None.
    radius : float, optional
        The radius in meters of the center of the circle in which the characteristics are
        requested. The default is None.. The default is 1000.0.
    epsg : str, optional
        The coordinate reference system of the specified extent, x or y, and of the
        resulting GeoDataFrame. The default is 28992, which is the Dutch RD-system.
    to_file : str, optional
        When not None, save the characteristics to a file with a name as specified in
        to_file. The defaults None.
    use_all_corners_of_extent : bool, optional
        Because the extent by default is given in epsg 28992, some locations along the
        border of a requested extent will not be returned in the result. To solve this
        issue, when use_all_corners_of_extent is True, all four corners of the extent
        are used to calculate the minimum and maximum lan and lon values. The default is
        True.

    Raises
    ------

        DESCRIPTION.

    Returns
    -------
    gpd.GeoDataFrame
        A GeoDataFrame contraining the characteristics.

    """
    url = f"{cl._rest_url}/characteristics/searches?"
    ns = {"xmlns": cl._xmlns}

    data = {}
    if tmin is not None or tmax is not None:
        data["registrationPeriod"] = {}
        if tmin is not None:
            beginDate = pd.to_datetime(tmin).strftime("%Y-%m-%d")
            data["registrationPeriod"]["beginDate"] = beginDate
        if tmax is not None:
            endDate = pd.to_datetime(tmax).strftime("%Y-%m-%d")
            data["registrationPeriod"]["endDate"] = endDate
    if (x is None or y is None) and extent is None:
        raise (Exception("Please specify either extent or x, y and radius"))

    transformer = Transformer.from_crs(epsg, 4326)
    data["area"] = {}
    if x is not None and y is not None:
        lat, lon = transformer.transform(x, y)
        data["area"]["enclosingCircle"] = {
            "center": {"lat": lat, "lon": lon},
            "radius": radius / 1000,
        }
    if extent is not None:
        lat_ll, lon_ll = transformer.transform(extent[0], extent[2])
        lat_ur, lon_ur = transformer.transform(extent[1], extent[3])
        if use_all_corners_of_extent:
            lat_ul, lon_ul = transformer.transform(extent[0], extent[3])
            lat_lr, lon_lr = transformer.transform(extent[1], extent[2])
            lat_ll = min(lat_ll, lat_lr)
            lon_ll = min(lon_ll, lon_ul)
            lat_ur = max(lat_ul, lat_ur)
            lon_ur = max(lon_lr, lon_ur)

        data["area"]["boundingBox"] = {
            "lowerCorner": {"lat": lat_ll, "lon": lon_ll},
            "upperCorner": {"lat": lat_ur, "lon": lon_ur},
        }
    req = requests.post(url, json=data)
    if req.status_code > 200:
        root = xml.etree.ElementTree.fromstring(req.text)
        XmlFileOrUrl._check_for_rejection(root)
        # if reading of the rejection message failed, raise a more general error
        raise (Exception((f"Retieving data from {url} failed")))

    if to_file is not None:
        with open(to_file, "w") as f:
            f.write(req.text)

    # read results
    tree = xml.etree.ElementTree.fromstring(req.text)

    data = []
    for gmw in tree.findall(f".//xmlns:{cl._char}", ns):
        d = {}
        for key in gmw.attrib:
            d[key.split("}", 1)[1]] = gmw.attrib[key]
        for child in gmw:
            key = child.tag.split("}", 1)[1]
            if len(child) == 0:
                d[key] = child.text
            elif key == "standardizedLocation":
                d["lat"], d["lon"] = XmlFileOrUrl._read_pos(child)
            elif key == "deliveredLocation":
                d["x"], d["y"] = XmlFileOrUrl._read_pos(child)
            elif key.endswith("Date") or key.endswith("Overview"):
                d[key] = child[0].text
            elif key in ["diameterRange", "screenPositionRange"]:
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    d[key] = grandchild.text
            else:
                logger.warning(f"Unknown key: {key}")
        data.append(d)
    df = pd.DataFrame(data)
    if "broId" in df.columns:
        df = df.set_index("broId")
        df = df.sort_index()

    if "x" in df and "y" in df:
        geometry = gpd.points_from_xy(df["x"], df["y"], crs=28992)
        gdf = gpd.GeoDataFrame(df, geometry=geometry)
        if epsg != 28992:
            gdf = gdf.to_crs(epsg)
        return gdf
    elif "lat" in df and "lon" in df:
        geometry = gpd.points_from_xy(df["lon"], df["lat"], crs=4326)
        gdf = gpd.GeoDataFrame(df, geometry=geometry)
        if epsg != 4326:
            gdf = gdf.to_crs(epsg)
        return gdf
    return df


class XmlFileOrUrl:
    """
    A class for parsing and handling XML data from files, URLs, or zipped files.

    Supports fetching XML data from local files or remote URLs. It also handles
    rejection checks and extracts data into object attributes. Data is parsed
    recursively and can be converted to a dictionary.

    Attributes:
        Instance variables are dynamically set based on the XML content.

    Methods:
        __init__(url_or_file, zipfile=None, timeout=5, to_file=None, **kwargs):
            Parses XML from a URL, file, or zipped file, and initializes the object.

        from_bro_id(bro_id, **kwargs):
            Fetches XML data from a REST service based on a given 'bro_id'.

        to_dict():
            Converts instance attributes to a dictionary, excluding methods and
            private attributes.

        _check_for_rejection(tree):
            Checks XML for rejection responses and raises an error if found.

        _read_children_of_children(node, d=None, to_float=None):
            Recursively reads child elements, converting specified ones to float.

        _read_delivered_location(node):
            Extracts geographic location and date information from the XML node.

        _read_pos(node):
            Extracts coordinates (x, y) from a GML-compliant position element.

        _read_date(node):
            Extracts date information from the XML, handling multiple formats.

        _read_time_instant(node):
            Extracts time instant information from a GML-compliant time element.
    """

    def __init__(self, url_or_file, zipfile=None, timeout=5, to_file=None, **kwargs):
        # CSV
        if url_or_file.endswith(".csv"):
            if zipfile is not None:
                self._read_csv(StringIO(zipfile.read(url_or_file)), **kwargs)
            else:
                self._read_csv(url_or_file, **kwargs)
        # XML or URL
        else:
            if zipfile is not None:
                root = xml.etree.ElementTree.fromstring(zipfile.read(url_or_file))
            elif url_or_file.startswith("http"):
                req = requests.get(url_or_file, timeout=timeout)
                if not req.ok:
                    # msg = req.json()["errors"][0]["message"]
                    raise Exception(f"Retrieving data from {url_or_file} failed")
                if to_file is not None:
                    with open(to_file, "w") as f:
                        f.write(req.text)
                root = xml.etree.ElementTree.fromstring(req.text)
                FileOrUrl._check_for_rejection(root)
            else:
                tree = xml.etree.ElementTree.parse(url_or_file)
                root = tree.getroot()

            self._read_contents(root, **kwargs)

    def _read_csv(self, *args, **kwargs):
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not support reading from CSV files."
        )

    @classmethod
    def from_bro_id(cls, bro_id, **kwargs):
        if not hasattr(cls, "_rest_url"):
            raise (NotImplementedError(f"No rest-service defined for {cls.__name__}"))
        return cls(f"{cls._rest_url}/objects/{bro_id}", **kwargs)

    def to_dict(self):
        d = {}
        for attrib in dir(self):
            if attrib.startswith("_"):
                continue
            value = getattr(self, attrib)
            if type(value) == types.MethodType:
                continue
            d[attrib] = value
        return d

    @staticmethod
    def _check_for_rejection(tree):
        ns = {"brocom": "http://www.broservices.nl/xsd/brocommon/3.0"}
        response_type = tree.find("brocom:responseType", ns)
        if response_type.text == "rejection":
            criterionError = tree.find("brocom:criterionError", ns)
            if criterionError is None:
                msg = tree.find("brocom:rejectionReason", ns).text
            else:
                msg = criterionError.find("brocom:specification", ns).text
            raise (ValueError(msg))

    def _read_children_of_children(self, node, d=None, to_float=None, to_int=None):
        if to_float is not None and isinstance(to_float, str):
            to_float = [to_float]
        if to_int is not None and isinstance(to_int, str):
            to_int = [to_int]
        if len(node) == 0:
            key = node.tag.split("}", 1)[1]
            if d is None:
                setattr(
                    self, key, XmlFileOrUrl._parse_text(node, key, to_float, to_int)
                )
            else:
                d[key] = XmlFileOrUrl._parse_text(node, key, to_float, to_int)
        else:
            for child in node:
                self._read_children_of_children(
                    child, d=d, to_float=to_float, to_int=to_int
                )

    @staticmethod
    def _parse_text(node, key, to_float, to_int):
        if to_float is not None and key in to_float:
            return float(node.text)
        if to_int is not None and key in to_int:
            return int(node.text)
        return node.text


    def _read_delivered_location(self, node):
        for child in node:
            key = child.tag.split("}", 1)[1]
            if key == "location":
                x, y = self._read_pos(child)
                setattr(self, "x", x)
                setattr(self, "y", y)
            elif key == "horizontalPositioningDate":
                setattr(self, key, self._read_date(child))
            elif key == "horizontalPositioningMethod":
                setattr(self, key, child.text)
            else:
                logger.warning(f"Unknown key: {key}")

    @staticmethod
    def _read_pos(node):
        ns = {"gml": "http://www.opengis.net/gml/3.2"}
        multipoint = node.find("gml:MultiPoint", ns)
        if multipoint is not None:
            xy = []
            for pointmember in multipoint.findall("gml:pointMember", ns):
                xy.append(XmlFileOrUrl._read_pos(pointmember))
            return xy
        point = node.find("gml:Point", ns)
        if point is None:
            pos = node.find("gml:pos", ns)
        else:
            pos = point.find("gml:pos", ns)
        xy = [float(x) for x in pos.text.split()]
        return xy

    @staticmethod
    def _read_date(node):
        ns = {"brocom": "http://www.broservices.nl/xsd/brocommon/3.0"}
        date = node.find("brocom:date", ns)
        if date is None:
            date = node.find("brocom:yearMonth", ns)
        if date is None:
            date = node.find("brocom:year", ns)
        if date is None:
            voidReason = node.find("brocom:voidReason", ns)
            if voidReason is not None:
                return pd.NaT
        return pd.to_datetime(date.text)

    @staticmethod
    def _read_time_instant(node):
        ns = {"gml": "http://www.opengis.net/gml/3.2"}
        time_instant = node.find("gml:TimeInstant", ns)
        time_position = time_instant.find("gml:timePosition", ns)
        return pd.to_datetime(time_position.text)
