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
    """get bro-id's from bronhouder"""
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
    Get characteristics of Groundwater Monitoring Wells (see bro.get_characteristics)
    """
    return bro.get_characteristics(GroundwaterMonitoringWell, **kwargs)


def get_well_code(bro_id):
    """
    Haalt een putcode op, op basis van een BRO-ID. Retourneert de putcode als 'plain text'

    Parameters
    ----------
    bro_id : str
        DESCRIPTION.

    Returns
    -------
    well_code : str
        DESCRIPTION.

    """
    url = f"{GroundwaterMonitoringWell._rest_url}/well-code/{bro_id}"
    req = requests.get(url)
    if req.status_code > 200:
        logger.error(req.reason)
        return
    well_code = req.text
    return well_code


class GroundwaterMonitoringWell(bro.XmlFileOrUrl):
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
):
    """
    Get observations

    Parameters
    ----------
    bro_ids : str
        The bro_ids of the monitoring wells of which we want the tubes.
    kind : str, optional
        The type of observations. Possible values are gmn, gld, gar and frd.
        See the top of this file for the measing of each abbreviation. The default is
        'gld' (groundwater level dossier).


    Returns
    -------
    None.

    """
    tubes = []

    if isinstance(bro_ids, str):
        bro_ids = [bro_ids]
        silent = True

    if isinstance(bro_ids, pd.DataFrame):
        bro_ids = bro_ids.index

    desc = f"Downloading {kind}-observations"
    for bro_id in tqdm(np.unique(bro_ids), disable=silent, desc=desc):
        url = f"https://publiek.broservices.nl/gm/v1/gmw-relations/{bro_id}"
        req = requests.get(url)
        if req.status_code > 200:
            logger.error(req.json()["errors"][0]["message"])
            return
        data = req.json()
        for tube_ref in data["monitoringTubeReferences"]:
            tube_ref["gmwBroId"] = data["gmwBroId"]
            if tube_number is not None:
                if tube_ref["tubeNumber"] != tube_number:
                    continue
            ref_key = f"{kind}References"
            for ref in tube_ref[ref_key]:
                if kind == "gld":
                    # df = gld.get_observations(
                    #    ref["broId"], tmin=tmin, tmax=tmax, as_csv=as_csv
                    # )
                    df = GroundwaterLevelDossier.from_bro_id(ref["broId"]).to_dict()
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
                    if drop_references:
                        for key in [
                            "gmnReferences",
                            "gldReferences",
                            "garReferences",
                            "frdReferences",
                        ]:
                            tube_ref.pop(key)
                    tubes.append(tube_ref)
                else:
                    tubes.append(df)

    return pd.DataFrame(tubes)


def get_tube_observations(gwm_id, tube_number):
    df = get_observations(gwm_id, tube_number=tube_number, as_csv=False)
    if df.empty:
        return gld._get_empty_observation_df()
    else:
        assert df.shape[0] == 1
        return df["observation"][0]


def get_tube_gdf(props, index=None):
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
    qualifier="goedgekeurd",
):
    """
    Get metadata and series within an extent

    Parameters
    ----------
    extent : TYPE
        DESCRIPTION.
    kind : str, optional
        DESCRIPTION. The default is "gld".
    tmin : TYPE, optional
        DESCRIPTION. The default is None.
    tmax : TYPE, optional
        DESCRIPTION. The default is None.
    combine : TYPE, optional
        DESCRIPTION. The default is True.
    index : TYPE, optional
        DESCRIPTION. The default is None.
    qualifier : TYPE, optional
        DESCRIPTION. The default is "goedgekeurd".

    Returns
    -------
    TYPE
        DESCRIPTION.

    """

    logger.info(f"Getting gmw-characteristics in extent: {extent}")
    gmw = get_characteristics(extent=extent)

    logger.info(f"Downloading {kind}-observations")
    obs_df = get_observations(gmw, kind=kind, tmin=tmin, tmax=tmax)

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
