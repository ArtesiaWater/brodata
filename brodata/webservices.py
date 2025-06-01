import logging

import geopandas as gpd
import numpy as np
import requests
from requests.exceptions import HTTPError
from shapely.geometry import MultiPolygon, Point, Polygon
from tqdm import tqdm

logger = logging.getLogger(__name__)


def get_gdf(kind, extent=None, config=None, index="DINO_NR", **kwargs):
    if config is None:
        config = get_configuration()
    if kind not in config:
        raise (ValueError(f"Unknown kind: {kind}"))
    url = config[kind]["mapserver"]
    layer = 0
    if "layer" in config[kind]:
        layer = config[kind]["layer"]
    gdf = arcrest(url, layer, extent=extent, **kwargs)
    if "greater_than_0" in config[kind]:
        greater_than_0 = config[kind]["greater_than_0"]
        if greater_than_0 in gdf.columns:
            gdf = gdf[gdf[greater_than_0] > 0]
    if not gdf.empty and index in gdf.columns:
        gdf = gdf.set_index(index)
    return gdf


def arcrest(
    url,
    layer,
    extent=None,
    sr=28992,
    f="geojson",
    max_record_count=None,
    timeout=120,
    **kwargs,
):
    """Download data from an arcgis rest FeatureServer.

    Parameters
    ----------
    url : str
        arcrest url.
    layer : str
        layer
    extent : list, tuple or np.array
        extent
    sr : int, optional
        coördinate reference system. The default is 28992 (RD).
    f : str, optional
        output format. Default is geojson
    max_record_count : int, optional
        maximum number of records for request.
    timeout : int, optional
        timeout time of request. Default is 120.

    """
    params = {
        "f": f,
        "outFields": "*",
        "outSR": sr,
        "where": "1=1",
    }
    if extent is not None:
        params["spatialRel"] = "esriSpatialRelIntersects"
        xmin, xmax, ymin, ymax = extent
        params["geometry"] = f"{xmin},{ymin},{xmax},{ymax}"
        params["geometryType"] = "esriGeometryEnvelope"
        params["inSR"] = sr
    props = _get_data(url, {"f": "json"}, timeout=timeout, **kwargs)
    if max_record_count is None:
        max_record_count = props["maxRecordCount"]
    else:
        max_record_count = min(max_record_count, props["maxRecordCount"])

    params["returnIdsOnly"] = True
    url_query = f"{url}/{layer}/query"
    props = _get_data(url_query, params, timeout=timeout, **kwargs)
    params.pop("returnIdsOnly")
    if "objectIds" in props:
        object_ids = props["objectIds"]
        object_id_field_name = props["objectIdFieldName"]
    else:
        object_ids = props["properties"]["objectIds"]
        object_id_field_name = props["properties"]["objectIdFieldName"]
    if object_ids is not None and len(object_ids) > max_record_count:
        # download in batches
        object_ids.sort()
        n_d = int(np.ceil((len(object_ids) / max_record_count)))
        features = []
        for i_d in tqdm(range(n_d)):
            i_min = i_d * max_record_count
            i_max = min(i_min + max_record_count - 1, len(object_ids) - 1)
            where = "{}>={} and {}<={}".format(
                object_id_field_name,
                object_ids[i_min],
                object_id_field_name,
                object_ids[i_max],
            )
            params["where"] = where
            data = _get_data(url_query, params, timeout=timeout, **kwargs)
            features.extend(data["features"])
    else:
        # download all data in one go
        data = _get_data(url_query, params, timeout=timeout, **kwargs)
        features = data["features"]
    if f == "json" or f == "pjson":
        # Interpret the geometry field
        data = []
        for feature in features:
            if "rings" in feature["geometry"]:
                if len(feature["geometry"]) > 1:
                    raise (NotImplementedError("Multiple rings not supported yet"))
                if len(feature["geometry"]["rings"]) == 1:
                    geometry = Polygon(feature["geometry"]["rings"][0])
                else:
                    pols = [Polygon(xy) for xy in feature["geometry"]["rings"]]
                    keep = [0]
                    for i in range(1, len(pols)):
                        if pols[i].within(pols[keep[-1]]):
                            pols[keep[-1]] = pols[keep[-1]].difference(pols[i])
                        else:
                            keep.append(i)
                    if len(keep) == 1:
                        geometry = pols[keep[0]]
                    else:
                        geometry = MultiPolygon([pols[i] for i in keep])
            elif (
                len(feature["geometry"]) == 2
                and "x" in feature["geometry"]
                and "y" in feature["geometry"]
            ):
                geometry = Point(feature["geometry"]["x"], feature["geometry"]["y"])
            else:
                raise (Exception("Not supported yet"))
            feature["attributes"]["geometry"] = geometry
            data.append(feature["attributes"])
        if len(data) == 0:
            # Assigning CRS to a GeoDataFrame without a geometry column is not supported
            gdf = gpd.GeoDataFrame()
        else:
            gdf = gpd.GeoDataFrame(data, crs=sr)
    else:
        # for geojson-data we can transform to GeoDataFrame right away
        if len(features) == 0:
            # Assigning CRS to a GeoDataFrame without a geometry column is not supported
            gdf = gpd.GeoDataFrame()
        else:
            gdf = gpd.GeoDataFrame.from_features(features, crs=sr)

    return gdf


def _get_data(url, params, timeout=5, **kwargs):
    """get data using a request

    Parameters
    ----------
    url : str
        url
    params : dict
        request parameters
    timeout : int, optional
        timeout time of request. Default is 120.

    Returns
    -------
    data

    """
    r = requests.get(url, params=params, timeout=timeout, **kwargs)
    if not r.ok:
        raise (HTTPError(f"Request not successful: {r.url}"))
    data = r.json()
    if "error" in data:
        code = data["error"]["code"]
        message = data["error"]["message"]
        raise (Exception(f"Error code {code}: {message}"))
    return data


services = {
    "lks_abo_rd": "Archeologisch booronderzoek",
    # "lks_aeo_rd": "",
    # "lks_aep_rd": "",
    "lks_bhr_g_rd": "Geologisch booronderzoek (BRO)",
    "lks_bhr_gt_rd": "Geotechnisch booronderzoek (BRO)",
    "lks_bhr_rd": "Bodemkundig booronderzoek (BRO)",
    "lks_cpt_rd": "Geotechnisch sondeeronderzoek (BRO)",
    # "lks_dgm_rd": "",
    # "lks_dgmdiep_rd": "",
    "lks_gbo_rd": "Geologisch booronderzoek",
    # "lks_geok_line_rd": "",
    # "lks_geok_rd": "",
    # "lks_geok_urania_rd": "",
    # "lks_gmd5_rd": "",
    # "lks_gmm_rd": "",
    "lks_gmw_rd": "Grondwatermonitoringput",
    "lks_gso_rd": "Geotechnisch sondeeronderzoek (GDN)",
    # "lks_gtp_rd": "",
    "lks_guf_rd": "Grondwatergebruiksystemen",
    "lks_gwo_rd": "Put met onderzoekgegevens",
    # "lks_mdl_rd": "",
    # "lks_nzs_rd": "",
    "lks_owo_rd": "Oppervlaktewateronderzoek",
    # "lks_rgs_rd": "",
    "lks_sfr_rd": "Bodemkundig wandonderzoek (BRO)",
    # "lks_sgm2_rd": "",
    "lks_vso_rd": "Verticaal elektrisch sondeeronderzoek",
    "lks_wbo_rd": "Geologisch waterbodemonderzoek",
    # "lks_wdm_brh_rd": "",
    # "lks_wdm_ghg_rd": "",
    # "lks_wdm_glg_rd": "",
    # "lks_wdm_gvg_rd": "",
    #'lks_wdm_gxg_rd": "",
    #'lks_wdm_ref_rd": ""
}


def get_configuration(mapserver_url=None):
    config = {}
    if mapserver_url is None:
        mapserver_url = "https://www.broloket.nl/standalone/rest/services"

    # BRO
    bro_mapserver_url = f"{mapserver_url}/uitgifteloket_bro"
    config["Geologisch booronderzoek (BRO)"] = {
        "mapserver": f"{bro_mapserver_url}/lks_bhr_g_rd_v1/MapServer",
        "abbr": "bhrg",
        "rest_url": "https://publiek.broservices.nl/sr/bhrg/v3",
    }
    config["Geotechnisch booronderzoek (BRO)"] = {
        "mapserver": f"{bro_mapserver_url}/lks_bhr_gt_rd_v1/MapServer",
        "abbr": "bhrgt",
        "object": "BHR-GT",
        "rest_url": "https://publiek.broservices.nl/sr/bhrgt/v2",
    }
    config["Bodemkundig booronderzoek (BRO)"] = {
        "mapserver": f"{bro_mapserver_url}/lks_bhr_rd_v1/MapServer",
        "abbr": "bhrp",
        "object": "BHR_O",
        "rest_url": "https://publiek.broservices.nl/sr/bhrp/v2",
    }
    config["Geotechnisch sondeeronderzoek (BRO)"] = {
        "mapserver": f"{bro_mapserver_url}/lks_cpt_rd_v1/MapServer",
        "abbr": "cpt",
        "rest_url": "https://publiek.broservices.nl/sr/cpt/v1",
    }
    config["Bodemkundig wandonderzoek (BRO)"] = {
        "mapserver": f"{bro_mapserver_url}/lks_sfr_rd_v1/MapServer",
        "name_en": "pedological SoilFaceResearch",
        "abbr": "sfr",
    }
    config["Grondwatermonitoringput"] = {
        "mapserver": f"{bro_mapserver_url}/lks_gmw_rd_v1/MapServer",
        "rest_url": "https://publiek.broservices.nl/gm/gmw/v1",
    }
    config["Grondwaterstandonderzoek"] = {
        "reeks": "https://publiek.broservices.nl/gm/gld/v1/seriesAsCsv",
        "rest_url": "https://publiek.broservices.nl/gm/gld/v1",
        "abbr": "gld",
    }
    config["Grondwatersamenstellingsonderzoek"] = {
        "abbr": "gar",
        "rest_url": "https://publiek.broservices.nl/gm/gar/v1",
        "object": "GAR_O",
    }
    config["Grondwatergebruiksystemen"] = {
        "mapserver": f"{bro_mapserver_url}/lks_guf_rd_v1/MapServer",
        "name_nl": "Grondwatergebruiksystemen",
        "name_en": "Groundwater utilisation facility",
        "class": "GroundwaterUtilisationFacility",
        "abbr": "guf",
        "object": "GUF_PO",
        "rest_url": "https://publiek.broservices.nl/gu/guf/v1",
    }

    # DINO
    gdn_mapserver_url = f"{mapserver_url}/uitgifteloket_gdn"
    config["Geologisch booronderzoek"] = {
        "mapserver": f"{gdn_mapserver_url}/lks_gbo_rd_v1/MapServer",
        "download": "https://www.dinoloket.nl/uitgifteloket/api/brh/sampledescription/csv",
    }
    config["Boormonsterprofiel"] = config["Geologisch booronderzoek"].copy()
    config["Boormonsterprofiel"]["greater_than_0"] = "MP_CNT"

    config["Boormonsterfoto"] = config["Geologisch booronderzoek"].copy()
    config["Boormonsterfoto"]["greater_than_0"] = "MF_CNT"

    config["Boorgatmeting"] = config["Geologisch booronderzoek"].copy()
    config["Boorgatmeting"]["greater_than_0"] = "BM_CNT"

    config["Chemische analyse"] = config["Geologisch booronderzoek"].copy()
    config["Chemische analyse"]["greater_than_0"] = "CA_CNT"

    config["Korrelgrootte analyse"] = config["Geologisch booronderzoek"].copy()
    config["Korrelgrootte analyse"]["greater_than_0"] = "KA_CNT"

    config["Geologisch waterbodemonderzoek"] = {
        "mapserver": f"{gdn_mapserver_url}/lks_wbo_rd_v1/MapServer",
        "download": config["Geologisch booronderzoek"]["download"],
    }
    config["Archeologisch booronderzoek"] = {
        "mapserver": f"{gdn_mapserver_url}/lks_abo_rd_v1/MapServer",
        "download": config["Geologisch booronderzoek"]["download"],
    }
    config["Geotechnisch sondeeronderzoek (GDN)"] = {
        "mapserver": f"{gdn_mapserver_url}/lks_gso_rd_v1/MapServer",
    }
    config["Put met onderzoekgegevens"] = {
        "mapserver": f"{gdn_mapserver_url}/lks_gwo_rd_tiled_v1/MapServer",
    }
    config["Grondwatersamenstelling"] = {
        "mapserver": config["Put met onderzoekgegevens"]["mapserver"],
        "table": 2,  # Grondwatersamenstelling
        "greater_than_0": "SA_CNT",
        "download": "https://www.dinoloket.nl/uitgifteloket/api/wo/gwo/qua/report",
    }
    config["Grondwaterstand"] = {
        "mapserver": config["Put met onderzoekgegevens"]["mapserver"],
        "table": 3,  # Grondwaterstand
        "greater_than_0": "ST_CNT",
        "download": "https://www.dinoloket.nl/uitgifteloket/api/wo/gwo/full",
        "details": "https://www.dinoloket.nl/uitgifteloket/api/wo/gwo/details",
    }
    config["Oppervlaktewateronderzoek"] = {
        "mapserver": f"{gdn_mapserver_url}/lks_owo_rd_v1/MapServer"
    }
    config["Verticaal elektrisch sondeeronderzoek"] = {
        "mapserver": f"{gdn_mapserver_url}/lks_vso_rd_v1/MapServer",
        "download": "https://www.dinoloket.nl/uitgifteloket/api/ves/csv",
    }

    return config
