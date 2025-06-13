import os
from zipfile import ZipFile
import logging
import requests
import json
from tqdm import tqdm
import pandas as pd
import geopandas as gpd
from .gar import GroundwaterAnalysisReport
from .gld import GroundwaterLevelDossier
from .util import _save_data_to_zip, _get_to_file
from . import gmw


base_url = "https://api.pdok.nl/bzk/bro-gminsamenhang-karakteristieken/ogc/v1"

logger = logging.getLogger(__name__)


def conformance():
    url = f"{base_url}/conformance"
    r = requests.get(url, params={"f": "json"})
    if not r.ok:
        raise Exception(f"Retrieving data from {url} failed")
    return r.json()


def collections():
    url = f"{base_url}/collections"
    r = requests.get(url, params={"f": "json"})
    if not r.ok:
        raise Exception(f"Retrieving data from {url} failed")
    return r.json()


def gm_gld_collection():
    url = f"{base_url}/collections/gm_gld"
    r = requests.get(url, params={"f": "json"})
    if not r.ok:
        raise Exception(f"Retrieving data from {url} failed")
    return r.json()


def _gm_items(
    url,
    extent=None,
    crs="http://www.opengis.net/def/crs/EPSG/0/28992",
    limit=1000,
    time_columns=None,
    to_file=None,
    zipfile=None,
    redownload=False,
    **kwargs,
):
    """


    Parameters
    ----------
    extent : list of 4 floats
        The spatial extent ([xmin, xmax, ymin, ymax]) to filter the data..
    crs : string, optional
        The coordinate reference system of the requested extent and the geometries in
        the response. Possible values are:
            http://www.opengis.net/def/crs/OGC/1.3/CRS84
            http://www.opengis.net/def/crs/EPSG/0/28992
            http://www.opengis.net/def/crs/EPSG/0/3857
            http://www.opengis.net/def/crs/EPSG/0/4258
        The default is "http://www.opengis.net/def/crs/EPSG/0/28992".
    limit : TYPE, optional
        Limits the number of items that are presented in the response document. The
        maximum allowed value is 1000. The default is 1000.

    Returns
    -------
    None.

    """
    if zipfile is not None:
        with zipfile.open(to_file) as f:
            json_data = json.load(f)
    elif redownload or to_file is None or not os.path.isfile(to_file):
        params = {"f": "json", "crs": crs, "limit": limit}
        if extent is not None:
            xmin, xmax, ymin, ymax = extent
            bbox = f"{xmin},{ymin},{xmax},{ymax}"
            params["bbox-crs"] = crs
            params["bbox"] = bbox

        for key in kwargs:
            params[key] = kwargs[key]
        r = requests.get(url, params=params)

        if not r.ok:
            detail = r.json()["detail"]
            raise Exception(f"Retrieving data from {url} failed: {detail}")
        if to_file is not None:
            with open(to_file, "w") as f:
                f.write(r.text)
        json_data = r.json()
    else:
        with open(to_file) as f:
            json_data = json.load(f)

    gdf = gpd.GeoDataFrame.from_features(json_data["features"])
    url = _get_next_url(json_data)
    if url is not None:
        gdfs = [gdf]
        while url is not None:
            r = requests.get(url)
            if not r.ok:
                raise Exception(f"Retrieving data from {url} failed")
                json_data = r.json()
                gdfs.append(gpd.GeoDataFrame.from_features(json_data["features"]))
                url = _get_next_url(json_data)
        gdf = pd.concat(gdfs, ignore_index=True)
    if time_columns is None:
        time_columns = gdf.columns[gdf.columns.str.endswith("_time")]
        one_hour = pd.Timedelta(1, "hour")
        for column in time_columns:
            # transform date to dutch winter time
            gdf[column] = (
                pd.to_datetime(gdf[column], utc=True).dt.tz_localize(None) + one_hour
            )
    return gdf


def _get_next_url(json_data):
    links = pd.DataFrame(json_data["links"])
    next_mask = links["rel"] == "next"
    if next_mask.any():
        if next_mask.sum() > 1:
            raise (ValueError("More than 1 'next' page"))
        url = links.loc[next_mask, "href"].iloc[0]
        return url
    else:
        return None


def gar_items(*args, **kwargs):
    url = f"{base_url}/collections/gm_gar/items"

    gdf = _gm_items(url, *args, **kwargs)
    return gdf


def gld_items(*args, tmin=None, tmax=None, **kwargs):
    url = f"{base_url}/collections/gm_gld/items"

    gdf = _gm_items(url, *args, **kwargs)

    return gdf


def gmn_items(*args, **kwargs):
    url = f"{base_url}/collections/gm_gmn/items"

    gdf = _gm_items(url, *args, **kwargs)

    return gdf


def gmn_measuringpoint_items(*args, **kwargs):
    url = f"{base_url}/collections/gm_gmn_measuringpoint/items"

    gdf = _gm_items(url, *args, **kwargs)

    return gdf


def gmn_reference_items(*args, **kwargs):
    url = f"{base_url}/collections/gm_gmn_reference/items"

    gdf = _gm_items(url, *args, **kwargs)

    return gdf


def gmw_items(*args, **kwargs):
    url = f"{base_url}/collections/gm_gmw/items"

    gdf = _gm_items(url, *args, **kwargs)

    return gdf


def gmw_monitoringtube_items(*args, **kwargs):
    url = f"{base_url}/collections/gm_gmw_monitoringtube/items"

    gdf = _gm_items(url, *args, **kwargs)

    return gdf


def get_data_in_extent(
    extent,
    kind="gld",
    silent=False,
    tmin=None,
    tmax=None,
    combine=True,
    to_zip=None,
    to_path=None,
    redownload=False,
    index=None,
):
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

    if to_path is not None and not os.path.isdir(to_path):
        os.makedirs(to_path)

    to_file = _get_to_file("gm_gmw_monitoringtube.json", zipfile, to_path, _files)
    tubes = gmw_monitoringtube_items(
        extent, to_file=to_file, redownload=redownload, zipfile=zipfile
    )
    if kind is None:
        return tubes

    if kind == "gar":
        to_file = _get_to_file("gm_gar.json", zipfile, to_path, _files)
        meas_gdf = gar_items(
            extent, to_file=to_file, redownload=redownload, zipfile=zipfile
        )
        if tmin is not None:
            meas_gdf = meas_gdf[meas_gdf["sampling_date_time"] >= tmin]

        if tmax is not None:
            meas_gdf = meas_gdf[meas_gdf["sampling_date_time"] <= tmax]
        meas_cl = GroundwaterAnalysisReport
    elif kind == "gld":
        to_file = _get_to_file("gm_gld.json", zipfile, to_path, _files)
        meas_gdf = gld_items(
            extent, to_file=to_file, redownload=redownload, zipfile=zipfile
        )
        if tmin is not None:
            meas_gdf = meas_gdf[meas_gdf["research_last_date"] >= tmin]

        if tmax is not None:
            meas_gdf = meas_gdf[meas_gdf["research_first_date"] <= tmax]
        meas_cl = GroundwaterLevelDossier
    else:
        raise (ValueError("kind='{kind}' not supported"))
    measurement_objects = []
    if zipfile is None:
        desc = f"Downloading {kind}-observations"
    else:
        desc = f"Reading {kind}-observations from {to_zip}"
    for url in tqdm(meas_gdf["imbro_xml_url"], disable=silent, desc=desc):
        bro_id = url.rsplit("/", 1)[-1]
        to_file = _get_to_file(f"{bro_id}.xml", zipfile, to_path, _files)
        if zipfile is None:
            measurement_objects.append(meas_cl(url, to_file=to_file))
        else:
            measurement_objects.append(meas_cl(to_file, zipfile=zipfile))

    if zipfile is not None:
        zipfile.close()
    if zipfile is None and to_zip is not None:
        _save_data_to_zip(to_zip, _files, remove_path_again, to_path)

    # only keep tubes with active measurements
    mask = tubes["gm_gmw_monitoringtube_pk"].isin(meas_gdf["gm_gmw_monitoringtube_fk"])
    tubes = tubes[mask]

    if index is None:
        index = ["gmw_bro_id", "tube_number"]
    tubes = tubes.set_index(index)

    obs_df = pd.DataFrame([m_obj.to_dict() for m_obj in measurement_objects])
    if not obs_df.empty:
        obs_df = obs_df.set_index(
            ["groundwaterMonitoringWell", "tubeNumber"]
        ).sort_index()

    if combine and kind in ["gld", "gar"]:
        if kind == "gld":
            idcol = "groundwaterLevelDossier"
        elif kind == "gar":
            idcol = "groundwaterAnalysisReport"
        datcol = gmw._get_data_column(kind)
        logger.info("Adding observations to tube-properties")

        data = {}
        ids = {}
        for index in tubes.index:
            if index not in obs_df.index:
                continue
            data[index] = gmw._combine_observations(
                obs_df.loc[[index], datcol], kind=kind
            )
            ids[index] = list(obs_df.loc[[index], "broId"])
        tubes[datcol] = data
        tubes[idcol] = ids
        return tubes
    else:
        return tubes, obs_df
