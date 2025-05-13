import json
import logging
import os
from functools import partial
from zipfile import ZipFile

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

from . import bro, gar, gld
from .frd import FormationResistanceDossier
from .gar import GroundwaterAnalysisReport
from .gld import GroundwaterLevelDossier
from .gmn import GroundwaterMonitoringNetwork
from .util import _save_data_to_zip

logger = logging.getLogger(__name__)


def get_well_code(bro_id):
    """
    Retrieve the well code based on a given BRO-ID and return it as plain text.

    This function sends a GET request to fetch the well code associated with the
    specified BRO-ID. If the request fails, it logs an error message and returns `None`.

    Parameters
    ----------
    bro_id : str
        The BRO-ID for which to retrieve the associated well code.

    Returns
    -------
    well_code : str or None
        The well code as plain text if the request is successful. Returns `None` if
        the request fails.
    """

    url = f"{GroundwaterMonitoringWell._rest_url}/well-code/{bro_id}"
    req = requests.get(url)
    if req.status_code > 200:
        logger.error(req.reason)
        return
    well_code = req.text
    return well_code


class GroundwaterMonitoringWell(bro.FileOrUrl):
    """
    Represents a groundwater monitoring well (GMW) with associated properties.

    This class parses XML data related to a groundwater monitoring well (GMW).
    It extracts details such as location, monitoring tube data, and well history
    and stores these in attributes.

    Attributes
    ----------
    _rest_url : str
        The base URL for the groundwater monitoring well REST API.

    _xmlns : str
        The XML namespace used for parsing the GMW XML data.

    _char : str
        A string used to identify the type of groundwater monitoring well.

    Methods
    -------
    _read_contents(tree)
        Parses the XML tree to extract and store GMW attributes and child elements.

    _read_intermediate_event(node)
        Parses an intermediate event node to extract event details.

    Notes
    -----
    This class extends `bro.XmlFileOrUrl` and is designed to work with GMW XML
    data, either from a file or URL. The XML structure must follow the GMW schema.
    """

    _rest_url = "https://publiek.broservices.nl/gm/gmw/v1"
    _xmlns = "http://www.broservices.nl/xsd/dsgmw/1.1"
    _char = "GMW_C"

    def _read_contents(self, tree):
        ns = {
            "brocom": "http://www.broservices.nl/xsd/brocommon/3.0",
            "xmlns": self._xmlns,
        }

        gmws = tree.findall(".//xmlns:GMW_PO", ns)
        if len(gmws) == 0:
            gmws = tree.findall(".//xmlns:GMW_PPO", ns)
        if len(gmws) == 0:
            raise (ValueError("No gmw found"))
        elif len(gmws) > 1:
            raise (Exception("Only one gmw supported"))
        gmw = gmws[0]

        for key in gmw.attrib:
            setattr(self, key.split("}", 1)[1], gmw.attrib[key])
        for child in gmw:
            key = child.tag.split("}", 1)[1]
            if len(child) == 0:
                setattr(self, key, child.text)
            elif key == "standardizedLocation":
                self._read_standardized_location(child)
            elif key == "deliveredLocation":
                self._read_delivered_location(child)
            elif key == "wellHistory":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if key == "wellConstructionDate":
                        setattr(self, key, self._read_date(grandchild))
                    elif key == "intermediateEvent":
                        if not hasattr(self, key):
                            self.intermediateEvent = []
                        event = self._read_intermediate_event(grandchild)
                        self.intermediateEvent.append(event)
                    else:
                        logger.warning(f"Unknown key: {key}")

            elif key in ["deliveredVerticalPosition", "registrationHistory"]:
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    setattr(self, key, grandchild.text)
            elif key in ["monitoringTube"]:
                if not hasattr(self, key):
                    self.monitoringTube = []
                tube = {}
                self._read_children_of_children(child, tube)
                self.monitoringTube.append(tube)
            else:
                logger.warning(f"Unknown key: {key}")
        if hasattr(self, "monitoringTube"):
            self.monitoringTube = pd.DataFrame(self.monitoringTube)
            tubeNumber = self.monitoringTube["tubeNumber"].astype(int)
            self.monitoringTube["tubeNumber"] = tubeNumber
            self.monitoringTube = self.monitoringTube.set_index("tubeNumber")
        if hasattr(self, "intermediateEvent"):
            self.intermediateEvent = pd.DataFrame(self.intermediateEvent)

    def _read_intermediate_event(self, node):
        d = {}
        for child in node:
            key = child.tag.split("}", 1)[1]
            if key == "eventName":
                d[key] = child.text
            elif key == "eventDate":
                d[key] = self._read_date(child)
            else:
                logger.warning(f"Unknown key: {key}")
        return d


def get_observations(
    bro_ids,
    kind="gld",
    drop_references=True,
    silent=False,
    tmin=None,
    tmax=None,
    as_csv=False,
    tube_number=None,
    qualifier=None,
    to_path=None,
    to_zip=None,
    redownload=False,
    zipfile=None,
    _files=None,
):
    """
    Retrieve groundwater observations for the specified monitoring wells (bro_ids).

    This function fetches groundwater data for monitoring wells based on the provided
    parameters. It supports different types of observations, allows filtering by tube
    number, and can request the data in CSV format for groundwater level observations.

    Parameters
    ----------
    bro_ids : str or list or pd.DataFrame
        The BRO IDs of the monitoring wells for which to retrieve the data. If a
        DataFrame is provided, its index is used as the list of BRO IDs.
    kind : str, optional
        The type of observations to retrieve. Can be one of {'gmn', 'gld', 'gar', 'frd'}.
        Defaults to 'gld' (groundwater level dossier).
    drop_references : bool or list of str, optional
        Specifies whether to drop reference fields in the returned data. Defaults to True,
        in which case 'gmnReferences', 'gldReferences', and 'garReferences' are removed.
    silent : bool, optional
        If True, suppresses progress logging. Defaults to False.
    tmin : str or datetime, optional
        The minimum time filter for the observations. Defaults to None.
    tmax : str or datetime, optional
        The maximum time filter for the observations. Defaults to None.
    as_csv : bool, optional
        If True, requests the observations as CSV files instead of XML-files. Only valid
        if `kind` is 'gld'. Defaults to False.
    tube_number : int, optional
        Filters observations to a specific tube number. Defaults to None.
    qualifier : str or list of str, optional
        A qualifier string for additional filtering. Only valid if `kind` is 'gld'.
        Defaults to None.
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
    zipfile : zipfile.ZipFile, optional
        A zipfile-object. When not None, zipfile is used to read previously downloaded
        data from. The default is None.


    Returns
    -------
    pd.DataFrame
        A DataFrame containing the observations for the specified monitoring wells,
        where each row corresponds to an individual observation.

    Raises
    ------
    Exception
        If `as_csv=True` and `kind` is not 'gld', or if `qualifier` is provided for
        a kind other than 'gld'.
    """
    tubes = []

    if isinstance(bro_ids, str):
        bro_ids = [bro_ids]
        silent = True

    if isinstance(bro_ids, pd.DataFrame):
        bro_ids = bro_ids.index

    if isinstance(drop_references, bool):
        if drop_references:
            drop_references = [
                "gmnReferences",
                "gldReferences",
                "garReferences",
                # "frdReferences",
            ]
        else:
            drop_references = []

    if to_zip is not None:
        if not redownload and os.path.isfile(to_zip):
            raise (NotImplementedError("Redownload=False is not suppported yet"))
            return
        if to_path is None:
            to_path = os.path.splitext(to_zip)[0]
        remove_path_again = not os.path.isdir(to_path)
        if _files is None:
            _files = []

    desc = f"Downloading {kind}-observations"
    if as_csv and kind != "gld":
        raise (Exception("as_csv=True is only supported for kind=='gld'"))
    if qualifier is not None and kind != "gld":
        raise (Exception("A qualifier is only supported for kind=='gld'"))
    to_file = None
    if to_path is not None and not os.path.isdir(to_path):
        os.makedirs(to_path)
    for bro_id in tqdm(np.unique(bro_ids), disable=silent, desc=desc):
        to_rel_file = None
        if zipfile is not None or to_path is not None:
            to_rel_file = f"gmw_relations_{bro_id}.json"
            if zipfile is None:
                to_rel_file = os.path.join(to_path, to_rel_file)
            if _files is not None:
                _files.append(to_rel_file)
        if zipfile is None and (
            redownload or to_rel_file is None or not os.path.isfile(to_rel_file)
        ):
            url = f"https://publiek.broservices.nl/gm/v1/gmw-relations/{bro_id}"
            req = requests.get(url)
            if req.status_code > 200:
                logger.error(req.json()["errors"][0]["message"])
                return
            if to_rel_file is not None:
                with open(to_rel_file, "w") as f:
                    f.write(req.text)
            data = req.json()
        else:
            if zipfile is not None:
                with zipfile.open(to_rel_file) as f:
                    data = json.load(f)
            else:
                with open(to_rel_file) as f:
                    data = json.load(f)
        for tube_ref in data["monitoringTubeReferences"]:
            tube_ref["groundwaterMonitoringWell"] = data["gmwBroId"]
            if tube_number is not None:
                if tube_ref["tubeNumber"] != tube_number:
                    continue
            ref_key = f"{kind}References"
            for ref in tube_ref[ref_key]:
                if zipfile is not None or to_path is not None:
                    if as_csv:
                        to_file = f"{ref['broId']}.csv"
                    else:
                        to_file = f"{ref['broId']}.xml"
                    if zipfile is None:
                        to_file = os.path.join(to_path, to_file)
                    if _files is not None:
                        _files.append(to_file)
                if zipfile is None and (
                    redownload or to_file is None or not os.path.isfile(to_file)
                ):
                    if kind == "gld":
                        if as_csv:
                            df = gld.get_objects_as_csv(
                                ref["broId"], qualifier=qualifier, to_file=to_file
                            )
                        else:
                            df = GroundwaterLevelDossier.from_bro_id(
                                ref["broId"], qualifier=qualifier, to_file=to_file
                            )
                    elif kind == "gar":
                        df = GroundwaterAnalysisReport.from_bro_id(
                            ref["broId"], to_file=to_file
                        )
                    elif kind == "frd":
                        df = FormationResistanceDossier.from_bro_id(
                            ref["broId"], to_file=to_file
                        )
                    elif kind == "gmn":
                        df = GroundwaterMonitoringNetwork.from_bro_id(
                            ref["broId"], to_file=to_file
                        )
                else:
                    if kind == "gld":
                        if as_csv:
                            if zipfile is not None:
                                to_file = zipfile.open(to_file)
                            df = gld.read_gld_csv(
                                to_file,
                                ref["broId"],
                                rapportagetype="compact_met_timestamps",
                                qualifier=qualifier,
                            )
                        else:
                            df = GroundwaterLevelDossier(
                                to_file, qualifier=qualifier, zipfile=zipfile
                            )
                    elif kind == "gar":
                        df = GroundwaterAnalysisReport(to_file, zipfile=zipfile)
                    elif kind == "frd":
                        df = FormationResistanceDossier(to_file, zipfile=zipfile)
                    elif kind == "gmn":
                        df = GroundwaterMonitoringNetwork(to_file, zipfile=zipfile)

                if as_csv:
                    tube_ref["observation"] = df
                    for key in drop_references:
                        if key in tube_ref:
                            tube_ref.pop(key)
                        else:
                            logger.warning(
                                "{} not defined for {}, filter {}".format(
                                    key,
                                    tube_ref["groundwaterMonitoringWell"],
                                    tube_ref["tubeNumber"],
                                )
                            )

                    tube_ref["broId"] = ref["broId"]
                    tubes.append(tube_ref)
                else:
                    tubes.append(df.to_dict())
    if to_zip is not None:
        _save_data_to_zip(to_zip, _files, remove_path_again, to_path)
    return pd.DataFrame(tubes)


def get_tube_observations(gwm_id, tube_number, kind="gld", **kwargs):
    """
    Get the observations of a single groundwater monitoring tube.

    Parameters
    ----------
    gwm_id : TYPE
        The bro_id of the groundwater monitoring well.
    tube_number : int
        The tube number.
    **kwargs : dict
        Kwargs are passed onto get_observations.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the observations.

    """
    df = get_observations(gwm_id, tube_number=tube_number, kind=kind, **kwargs)
    if df.empty:
        return _get_empty_observation_df(kind)
    else:
        data_column = _get_data_column(kind)
        return _combine_observations(df[data_column], kind=kind)


def get_tube_gdf(gmws, index=None):
    """
    Create a GeoDataFrame of tube properties combined with well metadata.

    This function processes a DataFrame of well properties, extracts the relevant
    tube information, and combines them into a GeoDataFrame. The resulting GeoDataFrame
    contains metadata for each monitoring well and its associated tubes, with optional
    spatial information (coordinates) and relevant physical properties.

    Parameters
    ----------
    gmws : list or dict of GroundwaterMonitoringWell, or pd.DataFrame Well and tube data
        in one of the following formats: a list of `GroundwaterMonitoringWell` objects,
        a dictionary of these objects, or a DataFrame with the bro-ids of the
        GroundwaterMonitoringWells as the index and the column monitoringTube containing
        tube properties.
    index : str or list of str, optional
        The column or columns to use for indexing the resulting GeoDataFrame. Defaults
        to ['groundwaterMonitoringWell', 'tubeNumber'] if not provided.

    Returns
    -------
    gdf : gpd.GeoDataFrame
        A GeoDataFrame containing the combined well and tube properties, with the
        specified index and optional geometry (spatial data) if 'x' and 'y' columns are
        present.

    Notes
    -----
    If 'x' and 'y' columns are present, the function creates a GeoDataFrame with point
    geometries based on these coordinates, assuming the EPSG:28992 (Dutch National
    Coordinate System) CRS.
    """
    if isinstance(gmws, list):
        gmws = pd.DataFrame([x.to_dict() for x in gmws])
        if "broId" in gmws.columns:
            gmws = gmws.set_index("broId")
    elif isinstance(gmws, dict):
        gmws = pd.DataFrame([gmws[x].to_dict() for x in gmws])
        if "broId" in gmws.columns:
            gmws = gmws.set_index("broId")
    tubes = []
    for bro_id in gmws.index:
        for tube_number in gmws.loc[bro_id, "monitoringTube"].index:
            # combine properties of well and tube
            tube = pd.concat(
                (
                    gmws.loc[bro_id].drop("monitoringTube"),
                    gmws.loc[bro_id, "monitoringTube"].loc[tube_number],
                )
            )
            tube["groundwaterMonitoringWell"] = bro_id
            tube["tubeNumber"] = tube_number

            tubes.append(tube)

    if index is None:
        index = ["groundwaterMonitoringWell", "tubeNumber"]
    gdf = bro.objects_to_gdf(tubes, index=index)

    gdf = gdf.sort_index()

    # makes sure some columns consist of floats
    columns = [
        "offset",
        "groundLevelPosition",
        "tubeTopDiameter",
        "tubeTopPosition",
        "screenLength",
        "screenTopPosition",
        "screenBottomPosition",
        "plainTubePartLength",
    ]
    columns = [column for column in columns if column in gdf.columns]
    gdf[columns] = gdf[columns].astype(float)
    return gdf


def get_data_in_extent(
    extent,
    kind="gld",
    tmin=None,
    tmax=None,
    combine=False,
    index=None,
    as_csv=False,
    qualifier=None,
    to_zip=None,
    to_path=None,
    redownload=False,
):
    """
    Retrieve metadata and observations within a specified spatial extent.

    This function fetches monitoring well characteristics, groundwater observations,
    and tube properties within the given spatial extent. It can combine the data
    for specific observation types and return either individual dataframes or a
    combined dataframe.

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
    combine : bool, optional
        If True, combines the metadata, tube properties, and observations into a single
        dataframe. Defaults to False.
    index : str, optional
        The column to use for indexing in the resulting dataframe. Defaults to None.
    as_csv : bool, optional
        If True, the measurement data is requested as CSV files instead of XML files
         (only supported for 'gld'). Defaults to False.
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

    Returns
    -------
    gdf : pd.DataFrame
        A dataframe containing tube properties and metadata within the specified extent.

    obs_df : pd.DataFrame, optional
        A dataframe containing the observations for the specified wells. Returned only if
        `combine` is False.

    Raises
    ------
    Exception
        If `as_csv=True` and `kind` is not 'gld', or if other parameters are invalid.
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
    logger.info(f"Getting gmw-characteristics in extent: {extent}")
    to_file = None
    if zipfile is not None or to_path is not None:
        to_file = "gmw_characteristics.xml"
        if zipfile is None:
            to_file = os.path.join(to_path, to_file)
            if _files is not None:
                _files.append(to_file)
    if to_path is not None and not os.path.isdir(to_path):
        os.makedirs(to_path)

    gmw = get_characteristics(
        extent=extent, to_file=to_file, redownload=redownload, zipfile=zipfile
    )

    if kind is None:
        obs_df = pd.DataFrame()
        combine = False
    else:
        # get observations
        logger.info(f"Downloading {kind}-observations")
        obs_df = get_observations(
            gmw,
            kind=kind,
            tmin=tmin,
            tmax=tmax,
            as_csv=as_csv,
            qualifier=qualifier,
            to_path=to_path,
            redownload=redownload,
            zipfile=zipfile,
            _files=_files,
        )

        # only keep wells with observations
        if "groundwaterMonitoringWell" in obs_df.columns:
            gmw = gmw[gmw.index.isin(obs_df["groundwaterMonitoringWell"])]

    logger.info("Downloading tube-properties")

    # get the properties of the monitoringTubes
    gdf = get_tube_gdf_from_characteristics(
        gmw,
        index=index,
        to_path=to_path,
        redownload=redownload,
        zipfile=zipfile,
        _files=_files,
    )

    if zipfile is not None:
        zipfile.close()
    if zipfile is None and to_zip is not None:
        _save_data_to_zip(to_zip, _files, remove_path_again, to_path)

    if not obs_df.empty:
        obs_df = obs_df.set_index(
            ["groundwaterMonitoringWell", "tubeNumber"]
        ).sort_index()

    if combine and kind in ["gld", "gar"]:
        if kind == "gld":
            idcol = "groundwaterLevelDossier"
        elif kind == "gar":
            idcol = "groundwaterAnalysisReport"
        datcol = _get_data_column(kind)

        logger.info("Combining well-properties, tube-properties and observations")

        data = {}
        ids = {}
        for index in gdf.index:
            if index not in obs_df.index:
                continue
            data[index] = _combine_observations(obs_df.loc[[index], datcol], kind=kind)
            ids[index] = list(obs_df.loc[[index], "broId"])
        gdf[datcol] = data
        gdf[idcol] = ids
        return gdf
    else:
        if kind is None:
            return gdf
        else:
            return gdf, obs_df


def _get_data_column(kind):
    if kind == "gld":
        return "observation"
    elif kind == "gar":
        return "laboratoryAnalysis"
    else:
        raise (NotImplementedError(f"Measurement-kind {kind} not supported yet"))


def _get_empty_observation_df(kind):
    if kind == "gld":
        return gld._get_empty_observation_df()
    elif kind == "gar":
        return gar._get_empty_observation_df()
    else:
        raise (NotImplementedError(f"Measurement-kind {kind} not supported yet"))


def _combine_observations(observations, kind):
    obslist = []
    for observation in observations:
        if not isinstance(observation, pd.DataFrame) or observation.empty:
            continue
        obslist.append(observation)
    if len(obslist) == 0:
        return _get_empty_observation_df(kind)
    else:
        return pd.concat(obslist).sort_index()


def get_tube_gdf_from_characteristics(
    characteristics_gdf,
    index=None,
    to_path=None,
    redownload=False,
    zipfile=None,
    _files=None,
):
    """
    Generate a GeoDataFrame of tube properties based on well characteristics.

    This function downloads the GroundwaterMonitoringWell-objects to retreive data about
    the groundwater monitoring tubes, and combined this information in a new
    GeoDataFrame.

    Parameters
    ----------
    characteristics_gdf : gpd.GeoDataFrame
        GeoDataFrame of well characteristics with bro-ids of the
        GroundwaterMonitoringWells as the index, retreived with
        `brodata.gmw.get_characteristics`.
    index : str or list of str, optional
        Column(s) to use as the index for the resulting GeoDataFrame. Defaults
        to ['groundwaterMonitoringWell', 'tubeNumber'] if not provided.

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame of combined well and tube properties
    """
    bids = characteristics_gdf.index.unique()
    gmws = []
    for bid in bids:
        if zipfile is not None:
            fname = f"{bid}.xml"
            gmw = GroundwaterMonitoringWell(fname, zipfile=zipfile)
        else:
            to_file = None
            if to_path is not None:
                to_file = os.path.join(to_path, f"{bid}.xml")
                if _files is not None:
                    _files.append(to_file)
            gmw = GroundwaterMonitoringWell.from_bro_id(
                bid, to_file=to_file, redownload=redownload
            )
        gmws.append(gmw)
    gdf = get_tube_gdf(gmws, index=index)
    return gdf


cl = GroundwaterMonitoringWell

get_bro_ids_of_bronhouder = partial(bro._get_bro_ids_of_bronhouder, cl)
get_bro_ids_of_bronhouder.__doc__ = bro._get_bro_ids_of_bronhouder.__doc__

get_characteristics = partial(bro._get_characteristics, cl)
get_characteristics.__doc__ = bro._get_characteristics.__doc__
