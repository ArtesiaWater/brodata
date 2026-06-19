import os
from zipfile import ZipFile
import logging
import requests
import urllib.request
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import MultiPolygon, Polygon
from . import gmw, gld, gar, util


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
    Fetches and parses geospatial features from a GeoJSON endpoint, with optional
    filtering, pagination support, and time column localization.

    Retrieves data from a remote URL, a local file, or within a zip archive. Supports
    bounding box filtering, CRS specification, and conversion of datetime columns to
    Dutch winter time (UTC+1).

    Parameters
    ----------
    url : str
        The base URL to request the GeoJSON data from.
    extent : list, tuple, shapely.geometry.Polygon or shapely.geometry.MultiPolygon, optional
        The spatial extent ([xmin, xmax, ymin, ymax]) or polygon geometry to filter the data.
        When a polygon is provided, its bounding box is used for the spatial query.
    crs : string, optional
        The coordinate reference system of the requested extent and the geometries in
        the response. Possible values are:
            http://www.opengis.net/def/crs/OGC/1.3/CRS84
            http://www.opengis.net/def/crs/EPSG/0/28992
            http://www.opengis.net/def/crs/EPSG/0/3857
            http://www.opengis.net/def/crs/EPSG/0/4258
        The default is "http://www.opengis.net/def/crs/EPSG/0/28992".
    limit : int, optional
        Limits the number of items that are presented in the response document. The
        maximum allowed value is 1000. The default is 1000.
    time_columns : list of str, optional
        Names of columns containing datetime values to convert to Dutch winter time.
        If None, columns ending with '_time' are automatically selected.
    to_file : str, optional
        Path to save the downloaded GeoJSON file. If the file exists and
        `redownload` is False, it will be reused.
    zipfile : ZipFile, optional
        A `zipfile.ZipFile` object from which to read the `to_file` if provided.
    redownload : bool, optional
        If True, forces redownload of the data even if `to_file` exists.
    **kwargs : dict
        Additional query parameters to include in the request.

    Returns
    -------
    gdf : geopandas.GeoDataFrame
        A GeoDataFrame containing the parsed geospatial features.

    """
    if zipfile is not None:
        with zipfile.open(to_file) as f:
            json_data = json.load(f)
    elif redownload or to_file is None or not os.path.isfile(to_file):
        params = {"f": "json", "crs": crs, "limit": limit}
        if extent is not None:
            if isinstance(extent, (Polygon, MultiPolygon)):
                xmin, ymin, xmax, ymax = extent.bounds
            else:
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
    if len(json_data["features"]) == 0:
        msg = "No data found"
        if extent is not None:
            msg = "%s for extent=%s" % (msg, extent)
        msg = "%s on %s" % (msg, url)
        logger.warning(msg)
        return
    gdf = gpd.GeoDataFrame.from_features(json_data["features"], crs=crs)
    url = _get_next_url(json_data)
    if url is not None:
        gdfs = [gdf]
        while url is not None:
            r = requests.get(url)
            if not r.ok:
                raise Exception(f"Retrieving data from {url} failed")
            json_data = r.json()
            gdfs.append(gpd.GeoDataFrame.from_features(json_data["features"], crs=crs))
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
    # Filter results to polygon if polygon extent was provided
    if extent is not None and isinstance(extent, (Polygon, MultiPolygon)):
        gdf = gdf[gdf.intersects(extent)]
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
    tmin=None,
    tmax=None,
    silent=False,
    combine=True,
    index=None,
    as_csv=False,
    status=None,
    observation_type=None,
    qualifier=None,
    to_path=None,
    to_zip=None,
    redownload=False,
    continue_on_error=False,
    sort=True,
    drop_duplicates=True,
    progress_callback=None,
):
    """
    Retrieve metadata and observations within a specified spatial extent.

    This function fetches monitoring well characteristics and groundwater observations
    within the given spatial extent. It can combine the data for specific observation
    types and return either individual dataframes or a combined dataframe.

    Parameters
    ----------
    extent : object
        The spatial extent ([xmin, xmax, ymin, ymax]) to filter the data.
    kind : str, optional
        The type of observations to retrieve. Valid values are {'gld', 'gar'} for
        groundwater level dossier or groundwater analysis report. When kind is None, no
        observations are downloaded. Defaults to 'gld'.
    tmin : str or datetime, optional
        The minimum time for filtering observations. Defaults to None.
    tmax : str or datetime, optional
        The maximum time for filtering observations. Defaults to None.
    silent : bool, optional
        If True, suppresses progress logging. Defaults to False.
    combine : bool, optional
        If True, combines the tube properties, and observations into a single
        dataframe. Defaults to True.
    index : str, optional
        The column to use for indexing in the resulting dataframe. If None, the index
        will be set to a MultiIndex of the columns "gmw_bro_id" and "tube_number".
        Defaults to None.
    as_csv : bool, optional
        If True, the measurement data is requested as CSV files instead of XML files
         (only supported for 'gld'). Defaults to False.
    status : str, optional
        A status string for additional filtering. Possible values are
        "volledigBeoordeeld", "voorlopig" and "onbekend" Only valid if `kind` is 'gld'.
        Defaults to None.
    observation_type : str, optional
        An observation type string for additional filtering. Possible values are
        "reguliereMeting" and "controleMeting". Only valid if `kind` is 'gld'. Defaults
        to None.
    qualifier : str or list of str, optional
        A string or list of strings used to filter the observations. Only valid if
        `kind` is 'gld'. Defaults to None.
    to_path : str, optional
        If not None, save the downloaded files in the directory named to_path. The
        default is None.
    to_zip : str, optional
        If not None, save the downloaded files in a zip-file named to_zip. The default
        is None.
    redownload : bool, optional
        When downloaded files exist in to_path or to_zip, read from these files when
        redownload is False. If redownload is True, download the data again from the
        BRO-servers. The default is False.
    continue_on_error : bool, optional
        If True, continue after an error occurs during downloading or processing of
        individual observation data. Defaults to False.
    sort : bool, optional
        If True, sort the observations. Only used if `kind` is 'gld'. Defaults to True.
    drop_duplicates : bool, optional
        If True, drop duplicate observations based on their timestamp. Only used if
        `kind` is 'gld'. Defaults to True.
    progress_callback : function, optional
        A callback function that takes two arguments (current, total) to report
        progress. If None, no progress reporting is done. Defaults to None.

    Returns
    -------
    gdf : pd.DataFrame
        A dataframe containing tube properties and metadata within the specified extent.

    obs_df : pd.DataFrame, optional
        A dataframe containing the observations for the specified wells. Returned only if
        `combine` is False.
    """

    if isinstance(extent, str):
        if to_zip is not None:
            raise (Exception("When extent is a string, do not supply to_zip"))
        to_zip = extent
        if not os.path.isfile(to_zip):
            raise (FileExistsError(f"The file {to_zip} is not present"))
        extent = None
        redownload = False

    zipfile = None
    _files = None
    if to_zip is not None:
        if not redownload and os.path.isfile(to_zip):
            logger.info("Reading data from %s", to_zip)
            zipfile = ZipFile(to_zip)
        else:
            if to_path is None:
                to_path = os.path.splitext(to_zip)[0]
            remove_path_again = not os.path.isdir(to_path)
            _files = []

    if to_path is not None and not os.path.isdir(to_path):
        os.makedirs(to_path)

    to_file = util._get_to_file("gm_gmw_monitoringtube.json", zipfile, to_path, _files)
    tubes = gmw_monitoringtube_items(
        extent, to_file=to_file, redownload=redownload, zipfile=zipfile
    )

    if tubes is None:
        return tubes

    if index is None:
        index = ["gmw_bro_id", "tube_number"]
    tubes = tubes.set_index(index)

    if kind is None:
        return tubes

    obs_df = get_observations(
        extent,
        tubes=tubes,
        kind=kind,
        tmin=tmin,
        tmax=tmax,
        silent=silent,
        as_csv=as_csv,
        status=status,
        observation_type=observation_type,
        qualifier=qualifier,
        to_path=to_path,
        redownload=redownload,
        continue_on_error=continue_on_error,
        sort=sort,
        drop_duplicates=drop_duplicates,
        progress_callback=progress_callback,
        zipfile=zipfile,
        _files=_files,
    )
    # set the index of obs_df to the same index as tubes, so that they can be easily combined later on
    obs_df = obs_df.reset_index().set_index(["groundwaterMonitoringWell", "tubeNumber"])

    if zipfile is not None:
        zipfile.close()
    if zipfile is None and to_zip is not None:
        util._save_data_to_zip(to_zip, _files, remove_path_again, to_path)

    # only keep tubes with active measurements
    mask = tubes.index.isin(obs_df.index)
    tubes = tubes[mask]

    if combine and kind in ["gld", "gar"]:
        tubes = gmw.add_observations_to_tubes(
            tubes, obs_df, kind=kind, sort=sort, drop_duplicates=drop_duplicates
        )
        return tubes
    else:
        return tubes, obs_df


def get_observations(
    extent,
    tubes=None,
    kind="gld",
    tmin=None,
    tmax=None,
    silent=False,
    as_csv=False,
    status=None,
    observation_type=None,
    qualifier=None,
    to_path=None,
    redownload=False,
    continue_on_error=False,
    sort=True,
    drop_duplicates=True,
    progress_callback=None,
    zipfile=None,
    _files=None,
):
    """
    Retrieve observations for monitoring wells within a specified spatial extent.

    Parameters
    ----------
    extent : object
        The spatial extent ([xmin, xmax, ymin, ymax]) to filter the data.
    tubes : gpd.GeoDataFrame, optional
        A GeoDataFrame containing monitoring tube properties. If provided, the
        observations will be filtered to only include those associated with the
        tubes in this GeoDataFrame. If None, all observations within the extent are
        returned.
    kind : str, optional
        The type of observations to retrieve. Valid values are {'gld', 'gar'} for
        groundwater level dossier or groundwater analysis report. Defaults to 'gld'.
    tmin : str or datetime, optional
        The minimum time for filtering observations. Defaults to None.
    tmax : str or datetime, optional
        The maximum time for filtering observations. Defaults to None.
    silent : bool, optional
        If True, suppresses progress logging. Defaults to False.
    as_csv : bool, optional
        If True, the measurement data is requested as CSV files instead of XML files
        (only supported for 'gld'). Defaults to False.
    status : str, optional
        A status string for additional filtering. Possible values are
        "volledigBeoordeeld", "voorlopig" and "onbekend" Only valid if `kind` is 'gld'.
        Defaults to None.
    observation_type : str, optional
        An observation type string for additional filtering. Possible values are
        "reguliereMeting" and "controleMeting". Only valid if `kind` is 'gld'. Defaults
        to None.
    qualifier : str or list of str, optional
        A string or list of strings used to filter the observations. Only valid if
        `kind` is 'gld'. Defaults to None.
    to_path : str, optional
        If not None, save the downloaded files in the directory named to_path. The
        default is None.
    redownload : bool, optional
        When downloaded files exist in to_path or to_zip, read from these files when
        redownload is False. If redownload is True, download the data again from the
        BRO-servers. The default is False.
    continue_on_error : bool, optional
        If True, continue after an error occurs during downloading or processing of
        individual observation data. Defaults to False.
    sort : bool, optional
        If True, sort the observations. Only used if `kind` is 'gld'. Defaults to True.
    drop_duplicates : bool, optional
        If True, drop duplicate observations based on their timestamp. Only used if
        `kind` is 'gld'. Defaults to True.
    progress_callback : function, optional
        A callback function that takes two arguments (current, total) to report
        progress. If None, no progress reporting is done. Defaults to None.
    zipfile : ZipFile, optional
        A `zipfile.ZipFile` object from which to read the `to_file` if provided.
    _files : list, optional
        A list of file paths to read the `to_file` if provided.

    Returns
    -------
    obs_df : pd.DataFrame
        A dataframe containing the observations for the specified wells, where each row
        corresponds to an individual observation. The index is the BRO-id
        of the observation-objects.

    Raises
    ------
    ValueError
        If the specified `kind` is not supported, or if `as_csv=True` and `kind` is not
        'gld', or if `qualifier` is provided for a kind other than 'gld'.
    """
    if kind == "gar":
        to_file = util._get_to_file("gm_gar.json", zipfile, to_path, _files)
        meas_gdf = gar_items(
            extent, to_file=to_file, redownload=redownload, zipfile=zipfile
        )
        if tmin is not None:
            meas_gdf = meas_gdf[meas_gdf["sampling_date_time"] >= tmin]

        if tmax is not None:
            meas_gdf = meas_gdf[meas_gdf["sampling_date_time"] <= tmax]
        meas_cl = gar.GroundwaterAnalysisReport
    elif kind == "gld":
        to_file = util._get_to_file("gm_gld.json", zipfile, to_path, _files)
        meas_gdf = gld_items(
            extent, to_file=to_file, redownload=redownload, zipfile=zipfile
        )
        if tmin is not None:
            meas_gdf = meas_gdf[meas_gdf["research_last_date"] >= tmin]

        if tmax is not None:
            meas_gdf = meas_gdf[meas_gdf["research_first_date"] <= tmax]
        meas_cl = gld.GroundwaterLevelDossier
    else:
        raise (ValueError(f"kind='{kind}' not supported"))

    if tubes is not None:
        # only download measurements for tubes that are present in the tubes-gdf
        tube_pk = tubes["gm_gmw_monitoringtube_pk"].values
        meas_gdf = meas_gdf[meas_gdf["gm_gmw_monitoringtube_fk"].isin(tube_pk)]

    gld_kwargs = gmw._get_gld_kwargs(
        kind, tmin, tmax, qualifier, status, observation_type, sort, drop_duplicates
    )

    meas_gdf = meas_gdf.set_index("bro_id")
    measurement_objects = []
    if zipfile is None:
        desc = f"Downloading {kind}-observations"
    else:
        desc = f"Reading {kind}-observations from {zipfile.filename}"
    if as_csv and kind != "gld":
        raise (ValueError("as_csv=True is only supported for kind=='gld'"))
    if qualifier is not None and kind != "gld":
        raise (ValueError("A qualifier is only supported for kind=='gld'"))
    datcol = gmw._get_data_column(kind)
    for bro_id in util.tqdm(meas_gdf.index, disable=silent, desc=desc):
        obsdata = gmw._download_observations_for_bro_id(
            bro_id,
            meas_cl,
            as_csv,
            zipfile,
            to_path,
            _files,
            gld_kwargs,
            redownload=redownload,
            continue_on_error=continue_on_error,
        )

        if as_csv:
            meas_dict = {"broId": bro_id, datcol: obsdata}
        else:
            meas_dict = obsdata.to_dict()
        meas_dict["gm_gmw_monitoringtube_fk"] = meas_gdf.at[
            bro_id, "gm_gmw_monitoringtube_fk"
        ]
        measurement_objects.append(meas_dict)

        if progress_callback is not None:
            progress_callback(len(measurement_objects), len(meas_gdf.index))
    if len(measurement_objects) == 0:
        return None
    obs_df = pd.DataFrame(measurement_objects).set_index("broId")

    if as_csv and tubes is not None:
        # obs_df does not contain the gmw-id or the tube-number
        # so we need to add these columns to the dataframe
        tube_pk = tubes.reset_index().set_index("gm_gmw_monitoringtube_pk")
        fk = obs_df["gm_gmw_monitoringtube_fk"].values
        obs_df["groundwaterMonitoringWell"] = tube_pk["gmw_bro_id"].loc[fk].values
        obs_df["tubeNumber"] = tube_pk["tube_number"].loc[fk].values
    return obs_df


def get_kenset_geopackage(to_file=None, layer=None, redownload=False, index="bro_id"):
    """
    Download or read data from a geopackage-file for the whole of the Netherlands.

    Parameters
    ----------
    to_file : str, optional
        Path to save the downloaded GeoPackage file (with the extension `.gpkg`). If the
        file exists and `redownload` is False, it will be reused. The default is None.
    layer : str, optional
        The layer within the geopackage. Possible values are 'gm_gmw',
        'gm_gmw_monitoringtube', 'gm_gld', 'gm_gar', 'gm_gmn', 'gm_gmn_measuringpoint'
        and 'gm_gmn_reference'. The default is None, which read data from the layer
        "gm_gmw".
    redownload : bool, optional
        If True, forces redownload of the data even if `to_file` exists. The default is
        False.
    index : str, optional
        The column to use for indexing in the resulting GeoDataFrame. The default is
        "bro_id".

    Returns
    -------
    gdf : gpd.GeoDataFrame
        A GeoDataFrame containing the resulting objects.

    """
    url = "https://service.pdok.nl/bzk/bro-gminsamenhang-karakteristieken/atom/downloads/brogmkenset.gpkg"
    if to_file is not None:
        if redownload or not os.path.isfile(to_file):
            urllib.request.urlretrieve(url, to_file)
        url = to_file
    gdf = gpd.read_file(url, layer=layer)
    if index in gdf.columns:
        gdf = gdf.set_index(index)
    return gdf
