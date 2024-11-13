import logging
import requests
import numpy as np
import pandas as pd
from tqdm import tqdm
import geopandas as gpd

from . import bro, gld
from .gld import GroundwaterLevelDossier
from .gar import GroundwaterAnalysisReport
from .frd import FormationResistanceDossier
from .gmn import GroundwaterMonitoringNetwork

logger = logging.getLogger(__name__)


def get_bro_ids_of_bronhouder(bronhouder):
    """
    Retrieve the list of BRO (Basisregistratie Ondergrond) IDs for a given bronhouder.

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
    url = f"{GroundwaterMonitoringWell._rest_url}/bro-ids?"
    params = dict(bronhouder=bronhouder)
    req = requests.get(url, params=params)
    if req.status_code > 200:
        logger.error(req.json()["errors"][0]["message"])
        return
    bro_ids = req.json()["broIds"]
    return bro_ids


def get_characteristics(**kwargs):
    """
    Get characteristics of Groundwater Monitoring Wells.

    This function fetches the characteristics of groundwater monitoring wells
    using the `bro.get_characteristics` function, specifically for the
    `GroundwaterMonitoringWell` class. It passes the provided keyword arguments
    to the underlying function.

    Parameters
    ----------
    **kwargs
        Additional keyword arguments passed to the `bro.get_characteristics` function.

    Returns
    -------
    gpd.GeoDataFrame
        A GeoDataFrame contraining the characteristics of the groundwater monitoring
        wells.
    """
    return bro.get_characteristics(GroundwaterMonitoringWell, **kwargs)


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


class GroundwaterMonitoringWell(bro.XmlFileOrUrl):
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
                lat, lon = self._read_pos(child.find("brocom:location", ns))
                setattr(self, "lat", lat)
                setattr(self, "lon", lon)
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

    desc = f"Downloading {kind}-observations"
    if as_csv and kind != "gld":
        raise (Exception("as_csv=True is only supported for kind=='gld'"))
    if qualifier is not None and kind != "gld":
        raise (Exception("A qualifier is only supported for kind=='gld'"))
    for bro_id in tqdm(np.unique(bro_ids), disable=silent, desc=desc):
        url = f"https://publiek.broservices.nl/gm/v1/gmw-relations/{bro_id}"
        req = requests.get(url)
        if req.status_code > 200:
            logger.error(req.json()["errors"][0]["message"])
            return
        data = req.json()
        for tube_ref in data["monitoringTubeReferences"]:
            tube_ref["groundwaterMonitoringWell"] = data["gmwBroId"]
            if tube_number is not None:
                if tube_ref["tubeNumber"] != tube_number:
                    continue
            ref_key = f"{kind}References"
            for ref in tube_ref[ref_key]:
                if kind == "gld":
                    # df = gld.get_observations(
                    #    ref["broId"], tmin=tmin, tmax=tmax, as_csv=as_csv
                    # )
                    if as_csv:
                        df = gld.get_objects_as_csv(ref["broId"], qualifier=qualifier)
                    else:
                        df = GroundwaterLevelDossier.from_bro_id(
                            ref["broId"], qualifier=qualifier
                        ).to_dict()
                elif kind == "gar":
                    df = GroundwaterAnalysisReport.from_bro_id(ref["broId"]).to_dict()
                elif kind == "frd":
                    df = FormationResistanceDossier.from_bro_id(ref["broId"]).to_dict()
                elif kind == "gmn":
                    df = GroundwaterMonitoringNetwork.from_bro_id(
                        ref["broId"]
                    ).to_dict()

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
                    tubes.append(df)

    return pd.DataFrame(tubes)


def get_tube_observations(gwm_id, tube_number, **kwargs):
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
    df = get_observations(gwm_id, tube_number=tube_number, **kwargs)
    if df.empty:
        return gld._get_empty_observation_df()
    else:
        assert df.shape[0] == 1
        return df["observation"][0]


def get_tube_gdf(props, index=None):
    """
    Create a GeoDataFrame of tube properties combined with well metadata.

    This function processes a DataFrame of well properties, extracts the relevant
    tube information, and combines them into a GeoDataFrame. The resulting GeoDataFrame
    contains metadata for each monitoring well and its associated tubes, with optional
    spatial information (coordinates) and relevant physical properties.

    Parameters
    ----------
    props : pd.DataFrame
        A DataFrame containing well and tube properties.

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
    tubes = []
    for bro_id in props.index:
        for tube_number in props.loc[bro_id, "monitoringTube"].index:
            # combine properties of well and tube
            tube = pd.concat(
                (
                    props.loc[bro_id].drop("monitoringTube"),
                    props.loc[bro_id, "monitoringTube"].loc[tube_number],
                )
            )
            tube["groundwaterMonitoringWell"] = bro_id
            tube["tubeNumber"] = tube_number

            tubes.append(tube)

    tubes = pd.DataFrame(tubes)
    if index is None:
        index = ["groundwaterMonitoringWell", "tubeNumber"]
    elif isinstance(index, str):
        index = [index]
    if np.all([x in tubes.columns for x in index]):
        tubes = tubes.set_index(index)

    # make a geodataframe
    if "x" in tubes.columns and "y" in tubes.columns:
        geometry = gpd.points_from_xy(tubes["x"], tubes["y"], crs=28992)
    else:
        geometry = None
    gdf = gpd.GeoDataFrame(tubes, geometry=geometry)
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
        groundwater level dossier or groundwater analysis report. Defaults to 'gld'.
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

    logger.info(f"Getting gmw-characteristics in extent: {extent}")
    gmw = get_characteristics(extent=extent)

    logger.info(f"Downloading {kind}-observations")
    obs_df = get_observations(
        gmw, kind=kind, tmin=tmin, tmax=tmax, as_csv=as_csv, qualifier=qualifier
    )

    # only keep wells with observations
    if "groundwaterMonitoringWell" in obs_df.columns:
        gmw = gmw[gmw.index.isin(obs_df["groundwaterMonitoringWell"])]

    logger.info("Downloading tube-properties")
    # get the properties of the monitoringTubes
    props = [GroundwaterMonitoringWell.from_bro_id(bid) for bid in gmw.index.unique()]
    props = pd.DataFrame([x.to_dict() for x in props])
    if "broId" in props.columns:
        props = props.set_index("broId")
    if not obs_df.empty:
        obs_df = obs_df.set_index(
            ["groundwaterMonitoringWell", "tubeNumber"]
        ).sort_index()

    gdf = get_tube_gdf(props, index=index)
    if combine and kind in ["gld", "gar"]:
        if kind == "gld":
            datcol = "observation"
            idcol = "groundwaterLevelDossier"
        elif kind == "gar":
            datcol = "laboratoryAnalysis"
            idcol = "groundwaterAnalysisReport"

        logger.info("Combining well-properties, tube-properties and observations")

        data = {}
        ids = {}
        for index in gdf.index:
            if index not in obs_df.index:
                continue
            dfs = obs_df.loc[[index], datcol]
            data[index] = pd.concat(dfs[~dfs.isna()].values).sort_index()
            ids[index] = list(obs_df.loc[[index], "broId"])
        gdf[datcol] = data
        gdf[idcol] = ids
        return gdf
    else:
        return gdf, obs_df
