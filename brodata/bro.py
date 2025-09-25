import logging
import os
import types
from xml.etree import ElementTree
from abc import ABC, abstractmethod
from io import StringIO
from zipfile import ZipFile

from shapely.geometry import Point
import shapely
import numpy as np
import geopandas as gpd
import pandas as pd
import requests
from pyproj import Transformer

from . import util, gml

logger = logging.getLogger(__name__)


# %%
def _get_bro_ids_of_bronhouder(cl, bronhouder):
    """
    Retrieve list of BRO (Basisregistratie Ondergrond) IDs for a given bronhouder.

    This function sends a GET request to the REST API to fetch the BRO IDs associated
    with the specified bronhouder. If the request is unsuccessful, it logs an error
    message.

    Parameters
    ----------
    bronhouder : str
        The identifier for the bronhouder to retrieve the associated BRO IDs.

    Returns
    -------
    list or None
        A list of BRO IDs if the request is successful. Returns `None` if the request
        fails.
    """
    url = f"{cl._rest_url}/bro-ids?"
    params = dict(bronhouder=bronhouder)
    req = requests.get(url, params=params)
    if req.status_code > 200:
        logger.error(req.json()["errors"][0]["message"])
        return
    bro_ids = req.json()["broIds"]
    return bro_ids


def _get_characteristics(
    cl,
    extent=None,
    tmin=None,
    tmax=None,
    x=None,
    y=None,
    radius=1000.0,
    epsg=28992,
    to_file=None,
    redownload=False,
    use_all_corners_of_extent=True,
    timeout=5,
    zipfile=None,
):
    """
    Get characteristics of a set of registered objects for a given object class.

    The maximum number of objects that can be retrieved is 2000 for a single request.

    Parameters
    ----------
    extent : list or tuple of 4 floats, optional
        Download the characteristics within extent ([xmin, xmax, ymin, ymax]). The
        default is None.
    tmin : str or pd.Timestamp, optional
        The minimum registrationPeriod of the requested characteristics. The default is
        None.
    tmax : str or pd.Timestamp, optional
        The maximum registrationPeriod of the requested characteristics. The default is
        None.
    x : float, optional
        The x-coordinate of the center of the circle in which the characteristics are
        requested. The default is None.
    y : float, optional
        The y-coordinate of the center of the circle in which the characteristics are
        requested. The default is None.
    radius : float, optional
        The radius in meters of the center of the circle in which the characteristics
        are requested. The default is 1000.0.
    epsg : str, optional
        The coordinate reference system of the specified extent, x or y, and of the
        resulting GeoDataFrame. The default is 28992, which is the Dutch RD-system.
    to_file : str, optional
        When not None, save the characteristics to a file with a name as specified in
        to_file. The defaults None.
    redownload : bool, optional
        When the downloaded file exists in to_file, read from this file when redownload
        is False. If redownload is True, download the data again from the BRO-servers.
        The default is False.
    use_all_corners_of_extent : bool, optional
        Because the extent by default is given in epsg 28992, some locations along the
        border of a requested extent will not be returned in the result. To solve this
        issue, when use_all_corners_of_extent is True, all four corners of the extent
        are used to calculate the minimum and maximum lan and lon values. The default is
        True.
    timeout : int or float, optional
        A number indicating how many seconds to wait for the client to make a connection
        and/or send a response. The default is 5.
    zipfile : zipfile.ZipFile, optional
        A zipfile-object. When not None, zipfile is used to read previously downloaded
        data from. The default is None.

    Returns
    -------
    gpd.GeoDataFrame
        A GeoDataFrame contraining the characteristics.

    Notes
    -----
    Haalt de karakteristieken op van een set van registratie objecten, gegeven een
    kenmerkenverzameling (kenset).

    De karakteristieken geven een samenvatting van een object zodat een verdere selectie
    gemaakt kan worden. Het past in een tweetrapsbenadering, waarbij de eerste stap
    bestaat uit het ophalen van de karakteristieken en de 2e stap uit het ophalen van de
    gewenste registratie objecten. Het resultaat van deze operatie is gemaximaliseerd op
    2000.
    """
    if zipfile is None and (
        redownload or to_file is None or not os.path.isfile(to_file)
    ):
        url = f"{cl._rest_url}/characteristics/searches?"

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
        req = requests.post(url, json=data, timeout=timeout)
        if req.status_code > 200:
            root = ElementTree.fromstring(req.text)
            FileOrUrl._check_for_rejection(root)
            # if reading of the rejection message failed, raise a more general error
            raise (Exception((f"Retieving data from {url} failed")))

        if to_file is not None:
            with open(to_file, "w") as f:
                f.write(req.text)

        # read results
        tree = ElementTree.fromstring(req.text)
    else:
        if zipfile is not None:
            with zipfile.open(to_file) as f:
                tree = ElementTree.parse(f).getroot()
        else:
            tree = ElementTree.parse(to_file).getroot()

    ns = {"xmlns": cl._xmlns}
    data = []
    for gmw in tree.findall(f".//xmlns:{cl._char}", ns):
        d = {}
        for key in gmw.attrib:
            d[key.split("}", 1)[1]] = gmw.attrib[key]
        for child in gmw:
            key = util._get_tag(child)
            if len(child) == 0:
                d[key] = child.text
            elif key == "standardizedLocation":
                d[key] = FileOrUrl._read_geometry(child)
            elif key == "deliveredLocation":
                d[key] = FileOrUrl._read_geometry(child)
            elif (
                key.endswith("Date")
                or key.endswith("Overview")
                or key in ["startTime", "endTime"]
            ):
                d[key] = child[0].text
            elif key in ["diameterRange", "screenPositionRange"]:
                for grandchild in child:
                    key = util._get_tag(grandchild)
                    d[key] = grandchild.text
            elif key == "licence":
                for grandchild in child:
                    key2 = grandchild.tag.split("}", 1)[1]
                    for greatgrandchild in grandchild:
                        key3 = greatgrandchild.tag.split("}", 1)[1]
                        if key3 == "identificationLicence":
                            d[key] = greatgrandchild.text
                        else:
                            logger.warning(f"Unknown key: {key2}")
            elif key == "realisedInstallation":
                for grandchild in child:
                    key2 = grandchild.tag.split("}", 1)[1]
                    for greatgrandchild in grandchild:
                        key3 = greatgrandchild.tag.split("}", 1)[1]
                        if key3 == "installationFunction":
                            d[key] = greatgrandchild.text
                        else:
                            logger.warning(f"Unknown key: {key2}")

            else:
                logger.warning(f"Unknown key: {key}")
        data.append(d)

    gdf = objects_to_gdf(data)
    if zipfile is not None and extent is not None:
        gdf = gdf.cx[extent[0] : extent[1], extent[2] : extent[3]]
    return gdf


def _get_data_in_extent(
    bro_cl,
    extent=None,
    timeout=5,
    silent=False,
    to_path=None,
    to_zip=None,
    redownload=False,
    geometry=None,
    to_gdf=True,
    index="broId",
    continue_on_error=False,
):
    """
    Retrieve data within a specified extent from a BRO client.

    Parameters
    ----------
    bro_cl : class
        brodata class.
    extent : str or object, optional
        Spatial extent to query. If a string, interpreted as a zip file path.
    timeout : int, default=5
        Timeout in seconds for data retrieval requests.
    silent : bool, default=False
        If True, disables progress bars and reduces logging output.
    to_path : str, optional
        Directory path to save downloaded files.
    to_zip : str, optional
        Path to a zip file to read from or save data to.
    redownload : bool, default=False
        If True, forces redownload of data even if files exist.
    geometry : str or object, optional
        Geometry specification for the output GeoDataFrame.
    to_gdf : bool, default=True
        If True, converts the output to a GeoDataFrame.
    index : str, default="broId"
        Column name to use as index in the output GeoDataFrame.
    continue_on_error : bool, default=False
        If True, continues processing other items if an error occurs.

    Returns
    -------
    gdf : GeoDataFrame
        GeoDataFrame containing the retrieved data objects, indexed by the specified
        column.

    Raises
    ------
    Exception
        If invalid arguments are provided or data retrieval fails (unless
       continue_on_error is True).

    Notes
    -----
    - If `extent` is a string, it is treated as a zip file path and `to_zip` must not
        be provided.
    - Data can be read from or saved to zip archives or directories, depending on the
        provided arguments.
    - Progress is displayed unless `silent` is True.
    """
    if isinstance(extent, str):
        if to_zip is not None:
            raise (Exception("When extent is a string, do not supply to_zip"))
        to_zip = extent
        extent = None
        redownload = False
    zipfile = None
    _files = None
    if to_zip is not None:
        if not redownload and os.path.isfile(to_zip):
            logger.info(f"Reading data from {to_zip}")
            zipfile = ZipFile(to_zip)
        else:
            if to_path is None:
                to_path = os.path.splitext(to_zip)[0]
            remove_path_again = not os.path.isdir(to_path)
            _files = []

    # get gwm characteristics
    logger.info(f"Getting characteristics in extent: {extent}")
    to_file = None
    if zipfile is not None or to_path is not None:
        to_file = "characteristics.xml"
        if zipfile is None:
            to_file = os.path.join(to_path, to_file)
            if _files is not None:
                _files.append(to_file)
    if to_path is not None and not os.path.isdir(to_path):
        os.makedirs(to_path)

    char = _get_characteristics(
        bro_cl, extent=extent, to_file=to_file, redownload=redownload, zipfile=zipfile
    )

    data = {}
    for bro_id in util.tqdm(char.index, disable=silent):
        if zipfile is not None:
            fname = f"{bro_id}.xml"
            data[bro_id] = bro_cl(fname, zipfile=zipfile)
            continue
        if to_path is not None:
            to_file = os.path.join(to_path, f"{bro_id}.xml")
            if to_zip is not None:
                _files.append(to_file)
            if not redownload and os.path.isfile(to_file):
                data[bro_id] = bro_cl(to_file)
                continue
        kwargs = {"to_file": to_file, "timeout": timeout}
        if continue_on_error:
            try:
                data[bro_id] = bro_cl.from_bro_id(bro_id, **kwargs)
            except Exception as e:
                logger.error(f"Error retrieving {bro_id}: {e}")
        else:
            data[bro_id] = bro_cl.from_bro_id(bro_id, **kwargs)
    if zipfile is not None:
        zipfile.close()
    if zipfile is None and to_zip is not None:
        util._save_data_to_zip(to_zip, _files, remove_path_again, to_path)

    gdf = objects_to_gdf(data, geometry, to_gdf, index)

    return gdf


def objects_to_gdf(
    data,
    geometry=None,
    to_gdf=True,
    index="broId",
    from_crs=None,
    to_crs=28992,
):
    if not to_gdf:
        return data
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = pd.DataFrame([data[key].to_dict() for key in data])

    if index is not None and not df.empty:
        if isinstance(index, str):
            if index in df.columns:
                df = df.set_index(index)
        elif np.all([x in df.columns for x in index]):
            # we assume index is an iterable (list), to form a MultiIndex
            df = df.set_index(index)
    if geometry is None:
        if "deliveredLocation" in df:
            geometry = "deliveredLocation"
            if from_crs is None:
                from_crs = 28992
        elif "standardizedLocation" in df:
            geometry = "standardizedLocation"
            if from_crs is None:
                from_crs = 4258
        else:
            return df
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=from_crs)
    if to_crs is not None and from_crs is not None and to_crs != from_crs:
        gdf = gdf.to_crs(to_crs)
    return gdf


class FileOrUrl(ABC):
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

        _read_date(node):
            Extracts date information from the XML, handling multiple formats.

        _read_time_instant(node):
            Extracts time instant information from a GML-compliant time element.
    """

    def __init__(
        self,
        url_or_file,
        zipfile=None,
        timeout=5,
        to_file=None,
        redownload=True,
        max_retries=2,
        **kwargs,
    ):
        # CSV
        if url_or_file.endswith(".csv"):
            if zipfile is not None:
                self._read_csv(StringIO(zipfile.read(url_or_file)), **kwargs)
            else:
                self._read_csv(url_or_file, **kwargs)
        # XML or URL
        else:
            if zipfile is not None:
                root = ElementTree.fromstring(zipfile.read(url_or_file))
            elif url_or_file.startswith("http"):
                if redownload or to_file is None or not os.path.isfile(to_file):
                    params = {}
                    if "tmin" in kwargs and kwargs["tmin"] is not None:
                        tmin = kwargs.pop("tmin")
                        tmin = pd.to_datetime(tmin).strftime("%Y-%m-%d")
                        params["observationPeriodBeginDate"] = tmin
                    if "tmax" in kwargs and kwargs["tmax"] is not None:
                        tmax = kwargs.pop("tmax")
                        tmax = pd.to_datetime(tmax).strftime("%Y-%m-%d")
                        params["observationPeriodEndDate"] = tmax
                    if max_retries > 1:
                        adapter = requests.adapters.HTTPAdapter(max_retries=max_retries)
                        session = requests.Session()
                        session.mount("https://", adapter)
                        req = session.get(url_or_file, params=params, timeout=timeout)
                    else:
                        req = requests.get(url_or_file, params=params, timeout=timeout)
                    if not req.ok:
                        if req.reason == "Bad Request":
                            root = ElementTree.fromstring(req.text)
                            FileOrUrl._check_for_rejection(root)
                        raise Exception(f"Retrieving data from {url_or_file} failed")
                    if to_file is not None:
                        with open(to_file, "w") as f:
                            f.write(req.text)
                    root = ElementTree.fromstring(req.text)
                    FileOrUrl._check_for_rejection(root)
                else:
                    tree = ElementTree.parse(to_file)
                    root = tree.getroot()
            else:
                tree = ElementTree.parse(url_or_file)
                root = tree.getroot()

            self._read_contents(root, **kwargs)

    def __repr__(self):
        # retrieve properties if they exist
        propdict = {"broId": "broId", "x": "x", "y": "y"}
        props = {}
        for key in propdict:
            if hasattr(self, key):
                props[propdict[key]] = getattr(self, key)
        name = util._format_repr(self, props)
        return name

    @abstractmethod
    def _read_contents(self, tree, **kwargs):
        """Each subclass must overload _read_contents to parse XML result."""

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
            if type(value) is types.MethodType:
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

    @staticmethod
    def _get_tag(node):
        return util._get_tag(node)

    def _warn_unknown_tag(self, tag):
        logger.warning(
            f"Tag {tag} not supported in {self.__class__.__name__} {getattr(self, 'broId', '')}"
        )

    def _raise_assumed_single(self, key):
        raise ValueError(
            f"Assumed there is only one {key} in {self.__class__.__name__} {getattr(self, 'broId', '')}"
        )

    def _read_children_of_children(self, node, d=None, to_float=None, to_int=None):
        if to_float is not None and isinstance(to_float, str):
            to_float = [to_float]
        if to_int is not None and isinstance(to_int, str):
            to_int = [to_int]
        if len(node) == 0:
            key = node.tag.split("}", 1)[1]
            if d is None:
                setattr(self, key, FileOrUrl._parse_text(node, key, to_float, to_int))
            else:
                d[key] = FileOrUrl._parse_text(node, key, to_float, to_int)
        else:
            for child in node:
                self._read_children_of_children(
                    child, d=d, to_float=to_float, to_int=to_int
                )

    @staticmethod
    def _parse_text(node, key, to_float=None, to_int=None):
        if to_float is not None and key in to_float:
            return FileOrUrl._parse_float(node)
        if to_int is not None and key in to_int:
            return int(node.text)
        return node.text

    @staticmethod
    def _parse_float(node):
        if node.text is None:
            return np.nan
        return float(node.text)

    def _read_delivered_location(self, node):
        for child in node:
            key = self._get_tag(child)
            if key == "location":
                setattr(self, "deliveredLocation", self._read_geometry(child))
            elif key == "horizontalPositioningDate":
                setattr(self, key, self._read_date(child))
            elif key == "horizontalPositioningMethod":
                setattr(self, key, child.text)
            else:
                self._warn_unknown_tag(key)

    def _read_standardized_location(self, node):
        for child in node:
            key = self._get_tag(child)
            if key == "location":
                setattr(self, "standardizedLocation", self._read_geometry(child))
            elif key == "coordinateTransformation":
                setattr(self, key, child.text)
            else:
                self._warn_unknown_tag(key)

    def _read_delivered_vertical_position(self, node, d=None):
        for child in node:
            key = self._get_tag(child)
            if key == "verticalPositioningDate":
                value = self._read_date(child)
            elif key == "offset":
                if child.text is None:
                    value = np.nan
                else:
                    value = float(child.text)
            else:
                value = child.text

            if d is None:
                setattr(self, key, value)
            else:
                d[key] = value

    def _read_lifespan(self, node, d=None):
        for child in node:
            key = self._get_tag(child)
            if key in ["startTime", "endTime"]:
                if d is None:
                    setattr(self, key, self._read_date(child))
                else:
                    d[key] = self._read_date(child)
            else:
                self._warn_unknown_tag(key)

    def _read_validity_period(self, node, d=None):
        for child in node:
            key = self._get_tag(child)
            if key == "startValidity":
                if d is None:
                    setattr(self, key, self._read_date(child))
                else:
                    d[key] = self._read_date(child)
            elif key == "endValidity":
                if d is None:
                    setattr(self, key, self._read_date(child))
                else:
                    d[key] = self._read_date(child)
            else:
                self._warn_unknown_tag(key)

    @staticmethod
    def _read_geometry(node):
        assert len(node) == 1
        tag = node[0].tag.split("}")[-1]
        if tag == "pos":
            x, y = tuple(map(float, node[0].text.strip().split()))
            if FileOrUrl._is_epsg_4258(node):
                x, y = y, x
            return Point(x, y)
        geometry = gml.parse_geometry(node[0])
        if FileOrUrl._is_epsg_4258(node[0]):
            geometry = shapely.ops.transform(lambda x, y: (y, x), geometry)
        return geometry

    @staticmethod
    def _is_epsg_4258(node):
        srsName = "urn:ogc:def:crs:EPSG::4258"
        return "srsName" in node.attrib and node.attrib["srsName"] == srsName

    @staticmethod
    def _read_date(node):
        ns = {"brocom": "http://www.broservices.nl/xsd/brocommon/3.0"}
        date = node.find("brocom:date", ns)
        if date is None:
            date = node.find("brocom:yearMonth", ns)
        if date is None:
            date = node.find("brocom:year", ns)
        if date is None:
            return pd.NaT
        return pd.to_datetime(date.text)

    @staticmethod
    def _read_time_instant(node):
        ns = {"gml": "http://www.opengis.net/gml/3.2"}
        time_instant = node.find("gml:TimeInstant", ns)
        time_position = time_instant.find("gml:timePosition", ns)
        return pd.to_datetime(time_position.text)

    def _read_descriptive_borehole_log(self, node):
        d = {}
        to_float = ["upperBoundary", "lowerBoundary"]
        for child in node:
            key = self._get_tag(child)
            if len(child) == 0:
                d[key] = child.text
            elif key == "layer":
                if key not in d:
                    d[key] = []
                layer = {}
                self._read_children_of_children(child, d=layer, to_float=to_float)
                d[key].append(layer)
            else:
                self._warn_unknown_tag(key)
        if "layer" in d:
            d["layer"] = pd.DataFrame(d["layer"])
        return d


def get_bronhouders(index="kvk", **kwargs):
    """
    Get the name, kvk-number and the identifier of bronhouders (data owners).

    Parameters
    ----------
    index : string, optional
        The column to set as the index of the resulting DataFrame. The default is "kvk".
    **kwargs : dict
        Kwargs are passed onto pandas.read_json().

    Returns
    -------
    df : pd.DataFrame
        A Pandas DataFrame, with one row per bronhouder.

    """
    url = "https://bromonitor.nl/api/rapporten/bronhouders"
    df = pd.read_json(url, **kwargs)
    if index is not None:
        df = df.set_index(index)
    return df


def get_brondocumenten_per_bronhouder(index=("kvk", "type"), timeout=5, **kwargs):
    """
    Get the number of documents per bronhouder (data owner).

    Parameters
    ----------
    index : str, tuple or list, optional
        The column(s) to set as the index of the resulting DataFrame. The default is
        "kvk" and "type".
    timeout : int or float, optional
        A number indicating how many seconds to wait for the client to make a connection
        and/or send a response. The default is 5.
    **kwargs : dict
        Kwargs are passed onto pandas.DataFrame().

    Returns
    -------
    df : pd.DataFrame
        A Pandas DataFrame, with one row per combination of bronhouder and data-type.

    """
    url = "https://bromonitor.nl/api/rapporten/brondocumenten-per-bronhouder"
    r = requests.get(url, timeout=timeout)
    if not r.ok:
        raise (Exception("Download of brondocumenten per bronhouder failed"))
    df = pd.DataFrame(r.json()["data"], **kwargs)
    if "key" in df.columns:
        df = pd.concat((pd.DataFrame(list(df["key"])), df.drop(columns="key")), axis=1)
    if index is not None:
        if isinstance(index, tuple):
            index = list(index)
        df = df.set_index(index)
    return df


def get_kvk_df(fn_bronhouder_kvk=None):
    """
    Read manually saved table of KVK and Organisatienaam to DataFrame.

    from https://basisregistratieondergrond.nl/service-contact/formulieren/aangemeld-bro/
    :param fn_bronhouder_kvk: str, filename of the file with bronhouder and kvk numbers
    :return: pandas DataFrame with kvk as index and column 'Organisatienaam' and 'Bronhouder'
    """
    if fn_bronhouder_kvk is None:
        fn_bronhouder_kvk = os.path.join(
            os.path.dirname(__file__), "data", "bronhouder_kvk.txt"
        )

    df_bron_kvk = pd.read_csv(
        fn_bronhouder_kvk,
        sep=";",  # is a dummy value, data will be split later on the last | sign
        dtype=str,
        header=None,
        names=["all_data"],
        skipinitialspace=True,
        comment="#",
    )

    # split column all_data into bronhouder and kvk, using last | sign; both as string type
    # mind that index has string type, as that is format provided in brodata downloads
    df_bron_kvk[["Organisatienaam", "KVK-nummer"]] = (
        df_bron_kvk["all_data"].str.rsplit("|", n=1, expand=True).astype(str)
    )
    df_bron_kvk = df_bron_kvk.drop(columns=["all_data"])

    # add column Bronhouder, value is True when (​B) in kvk
    df_bron_kvk["Bronhouder"] = False

    bronhouder_pattern = r"[(​B)|(B)]"
    df_bron_kvk.loc[
        df_bron_kvk["KVK-nummer"].str.contains(bronhouder_pattern, regex=True),
        "Bronhouder",
    ] = True
    # clean up kvk
    df_bron_kvk["KVK-nummer"] = (
        df_bron_kvk["KVK-nummer"]
        .str.replace(bronhouder_pattern, "", regex=True)
        .str.strip()
    )

    # remove leading and trailing whitespace from all columns
    df_bron_kvk = df_bron_kvk.map(lambda x: x.strip() if isinstance(x, str) else x)

    # make kvk index
    df_bron_kvk.set_index("KVK-nummer", inplace=True)

    return df_bron_kvk
