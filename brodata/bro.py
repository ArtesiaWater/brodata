import logging
import requests
import xml
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
                d["lat"], d["lon"] = read_point(child)
            elif key == "deliveredLocation":
                d["x"], d["y"] = read_point(child)
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


def read_point(point):
    ns = "{http://www.opengis.net/gml/3.2}"
    xy = [float(x) for x in point.find(f"{ns}pos").text.split()]
    return xy
