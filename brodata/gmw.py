import logging
import requests
import numpy as np
import pandas as pd
from tqdm import tqdm
import geopandas as gpd

from . import bro, gld

logger = logging.getLogger(__name__)


def get_bro_ids_of_bronhouder(bronhouder):
    """get bro-id's from bronhouder"""
    url = "https://publiek.broservices.nl/gm/gmw/v1/bro-ids?"
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
    return bro.get_characteristics("gmw", **kwargs)


def get_well_code(bro_id):
    """
    Haalt een putcode op, op basis van een BRO-ID. Retourneert de putcode als 'plain text'

    Parameters
    ----------
    bro_id : TYPE
        DESCRIPTION.

    Returns
    -------
    well_code : TYPE
        DESCRIPTION.

    """
    url = f"https://publiek.broservices.nl/gm/gmw/v1/well-code/{bro_id}"
    req = requests.get(url)
    if req.status_code > 200:
        logger.error(req.reason)
        return
    well_code = req.text
    return well_code


def get_gmw(bro_id):
    """
    Haalt een putcode op, op basis van een BRO-ID.

    Retourneert de putcode als 'plain text'

    Parameters
    ----------
    bro_id : str
        Het BRO-ID van de gmw.

    Returns
    -------
    str
        De putcode.

    """
    url = f"https://publiek.broservices.nl/gm/gmw/v1/objects/{bro_id}"
    return GroundwaterMonitoringWell(url)


class GroundwaterMonitoringWell(bro.XmlFileOrUrl):
    def _read_contents(self, tree):
        ns = "{http://www.broservices.nl/xsd/dsgmw/1.1}"

        gmws = tree.findall(f".//{ns}GMW_PO")
        if len(gmws) == 0:
            gmws = tree.findall(f".//{ns}GMW_PPO")
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
                ns = "{http://www.broservices.nl/xsd/brocommon/3.0}"
                lat, lon = self._read_pos(child.find(f"{ns}location"))
                setattr(self, "lat", lat)
                setattr(self, "lon", lon)
            elif key == "deliveredLocation":
                self._read_delivered_location(child)
            elif key == "wellHistory":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if key == "wellConstructionDate":
                        setattr(self, key, self._read_date(grandchild))
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
        The type of observations. Possible values are gml, gld, gar and frd.
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
        for tube in data["monitoringTubeReferences"]:
            tube["gmwBroId"] = data["gmwBroId"]
            if tube_number is not None:
                if tube["tubeNumber"] != tube_number:
                    continue
            ref_key = f"{kind}References"
            for ref in tube[ref_key]:
                df = gld.get_observations(
                    ref["broId"], tmin=tmin, tmax=tmax, as_csv=as_csv
                )
                if as_csv:
                    tube["observation"] = df
                    if drop_references:
                        for key in [
                            "gmnReferences",
                            "gldReferences",
                            "garReferences",
                            "frdReferences",
                        ]:
                            tube.pop(key)
                    tubes.append(tube)
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


def get_tube_gdf(props, obs_df=None, index=None, qualifier="goedgekeurd"):
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
            tube["GroundwaterMonitoringWell"] = bro_id
            tube["tubeNumber"] = tube_number

            if obs_df is not None:
                # add observations
                tube = _add_observation_to_tube(tube, obs_df, (bro_id, tube_number))
                if qualifier is not None:
                    mask = tube["observation"]["qualifier"] == qualifier
                    n_drop = np.sum(~mask)
                    if n_drop > 0:
                        logger.info(
                            f"Dropping {n_drop} measurements of ({bro_id}, {tube_number}) that do not match qualifier {qualifier}"
                        )
                    tube["observation"] = tube["observation"].loc[mask, "value"]
            tubes.append(tube)

    tubes = pd.DataFrame(tubes)
    if index is None:
        index = ["wellCode", "tubeNumber"]
    tubes = tubes.set_index(index)

    # make a geodataframe
    geometry = gpd.points_from_xy(tubes["x"], tubes["y"], crs=28992)
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
    gdf[columns] = gdf[columns].astype(float)
    return gdf


def _add_observation_to_tube(tube, obs_df, name):
    if name in obs_df.index:
        # combine multiple series
        gld = obs_df.loc[name, "broId"]
        if isinstance(gld, str):
            tube["groundwaterLevelDossiers"] = [gld]
        else:
            tube["groundwaterLevelDossiers"] = list(gld)
        tube["observation"] = obs_df.loc[name, "observation"]
        if isinstance(tube["observation"], pd.Series):
            # multiple glds need to be combined
            tube["observation"] = pd.concat(tube["observation"].values).sort_index()
    else:
        tube["groundwaterLevelDossiers"] = []
        tube["observation"] = gld._get_empty_observation_df()
    return tube


def get_data_in_extent(
    extent,
    kind="gld",
    tmin=None,
    tmax=None,
    combine=True,
    index=None,
    qualifier="goedgekeurd",
):
    """
    Get metadata and series within an extent

    Parameters
    ----------
    extent : TYPE
        DESCRIPTION.
    kind : TYPE, optional
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
    if gmw.empty:
        return gmw
    logger.info("Downloading tube-properties")
    # get the properties of the monitoringTubes
    props = [get_gmw(bro_id) for bro_id in gmw.index.unique()]
    props = pd.DataFrame(props).set_index("broId")
    logger.info(f"Downloading {kind}-observations")
    obs_df = get_observations(gmw, kind=kind, tmin=tmin, tmax=tmax)
    obs_df = obs_df.set_index(["GroundwaterMonitoringWell", "tubeNumber"]).sort_index()
    if combine:
        logger.info("Combining well-properties, tube-properties and observations")
        gdf = get_tube_gdf(props, obs_df, index=index, qualifier=qualifier)
        return gdf
    return gmw, props, obs_df
