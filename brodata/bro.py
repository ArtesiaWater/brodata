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
    kind, tmin=None, tmax=None, extent=None, x=None, y=None, radius=1000.0, epsg=28992
):
    """
    Haalt de karakteristieken op van een set van registratie objecten, gegeven een kenmerkenverzameling (kenset).

    De karakteristieken geven een samenvatting van een object zodat een verdere selectie gemaakt kan worden. Het past in een tweetrapsbenadering, waarbij de eerste stap bestaat uit het ophalen van de karakteristieken en de 2e stap uit het ophalen van de gewenste registratie objecten. Het resultaat van deze operatie is gemaximaliseerd op 2000. Zie hier en hier voor meer info.

    Parameters
    ----------
    tmin : str or pd.Timestamp, optional
        DESCRIPTION. The default is None.
    tmax : str or pd.Timestamp, optional
        DESCRIPTION. The default is None.
    extent : TYPE, optional
        DESCRIPTION. The default is None.
    x : TYPE, optional
        DESCRIPTION. The default is None.
    y : TYPE, optional
        DESCRIPTION. The default is None.
    radius : TYPE, optional
        DESCRIPTION. The default is 1000.0.
    epsg : TYPE, optional
        DESCRIPTION. The default is 28992.

    Raises
    ------

        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    if kind == "gmw":
        url = "https://publiek.broservices.nl/gm/gmw/v1/characteristics/searches?"
        ns = "{http://www.broservices.nl/xsd/dsgmw/1.1}"
        name = "GMW_C"
    elif kind == "cpt":
        url = "https://publiek.broservices.nl/sr/cpt/v1/characteristics/searches?"
        ns = "{http://www.broservices.nl/xsd/dscpt/1.1}"
        name = "CPT_C"
    elif kind == "bhrgt":
        url = "https://publiek.broservices.nl/sr/bhrgt/v2/characteristics/searches?"
        ns = "{http://www.broservices.nl/xsd/dsbhr-gt/2.1}"
        name = "BHR_GT_C"
    else:
        raise (ValueError("Unknown kind: {kind}"))
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
        lat1, lon1 = transformer.transform(extent[0], extent[2])
        lat2, lon2 = transformer.transform(extent[1], extent[3])
        data["area"]["boundingBox"] = {
            "lowerCorner": {"lat": lat1, "lon": lon1},
            "upperCorner": {"lat": lat2, "lon": lon2},
        }
    req = requests.post(url, json=data)
    if req.status_code > 200:
        logger.error(req.json()["errors"][0]["message"])
        return

    # read results
    tree = xml.etree.ElementTree.fromstring(req.text)

    data = []
    for gmw in tree.findall(f".//{ns}{name}"):
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
    if "x" not in df or "y" not in df:
        return df
    geometry = gpd.points_from_xy(df["x"], df["y"], crs=28992)
    gdf = gpd.GeoDataFrame(df, geometry=geometry)
    if "broId" in gdf.columns:
        gdf = gdf.set_index("broId")
        gdf = gdf.sort_index()
    return gdf


class XmlFileOrUrl:
    def __init__(self, url_or_file, timeout=5, to_file=None, **kwargs):
        if url_or_file.startswith("http"):
            r = requests.get(url_or_file, timeout=timeout, **kwargs)
            if not r.ok:
                # msg = r.json()["errors"][0]["message"]
                raise (Exception((f"Retieving data from {url_or_file} failed")))
            if to_file is not None:
                with open(to_file, "w") as f:
                    f.write(r.text)
            root = xml.etree.ElementTree.fromstring(r.text)
            self._read_contents(root)
        else:
            tree = xml.etree.ElementTree.parse(url_or_file)
            root = tree.getroot()
            self._read_contents(root)

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

    def _read_children_of_children(self, node, d=None):
        if len(node) == 0:
            key = node.tag.split("}", 1)[1]
            if d is None:
                setattr(self, key, node.text)
            else:
                d[key] = node.text
        else:
            for child in node:
                self._read_children_of_children(child, d=d)

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
        xy = [float(x) for x in node.find("gml:pos", ns).text.split()]
        return xy

    @staticmethod
    def _read_date(node):
        ns = {"brocom": "http://www.broservices.nl/xsd/brocommon/3.0"}
        date = node.find("brocom:date", ns)
        return pd.to_datetime(date.text)

    @staticmethod
    def _read_time_instant(node):
        ns = {"gml": "http://www.opengis.net/gml/3.2"}
        time_instant = node.find("gml:TimeInstant", ns)
        time_position = time_instant.find("gml:timePosition", ns)
        return pd.to_datetime(time_position.text)
