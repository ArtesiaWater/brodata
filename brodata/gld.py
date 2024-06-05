import logging
import requests
from io import StringIO
import pandas as pd
from .webservices import get_gdf
from . import bro

logger = logging.getLogger(__name__)


def get_gld_within_extent(extent, config=None, timeout=5, silent=False):
    kind = "Grondwaterstandonderzoek"
    gdf = get_gdf(
        kind,
        config=config,
        extent=extent,
        timeout=timeout,
        f="json",
    )

    gdf = gdf[gdf["NUMBER_OF_GLD"] > 0]

    bhrp_data = {}
    logger.warning(f"Reading of {kind} not supported yet")
    return gdf, bhrp_data


def get_observations(
    bro_id,
    as_csv=False,
    observatietype="regulier_voorlopig",
    rapportagetype="compact",
    tmin=None,
    tmax=None,
    fname=None,
    **kwargs,
):
    if as_csv:
        # download the observations as csv
        if tmin is not None or tmax is not None:
            raise (Exception("tmin and tmax only supported when as_csv=False"))
        url = f"{GroundwaterLevelDossier._rest_url}/objectsAsCsv/{bro_id}"
        if rapportagetype != "compact":
            raise (Exception("Only rapportagetype compact supported for now"))
        params = {
            "observatietype": observatietype,
            "rapportagetype": rapportagetype,
        }
        req = requests.get(url, params=params)
        if req.status_code > 200:
            logger.error(req.json()["errors"][0]["message"])
            return
        if req.text == "":
            return None
        else:
            df = read_gld_csv(StringIO(req.text), bro_id)
            return df
    else:
        # download the observations as xml
        url = f"{GroundwaterLevelDossier._rest_url}/objects/{bro_id}"
        params = {}
        if tmin is not None:
            tmin = pd.to_datetime(tmin)
            params["observationPeriodBeginDate"] = tmin.strftime("%Y-%m-%d")
        if tmax is not None:
            tmax = pd.to_datetime(tmax)
            params["observationPeriodEndDate"] = tmax.strftime("%Y-%m-%d")
        df = GroundwaterLevelDossier(url, params=params, to_file=fname)
        return df.to_dict()


def read_gld_csv(fname, bro_id, **kwargs):
    names = ["time", "value", "qualifier"]
    df = pd.read_csv(
        fname,
        names=names,
        index_col="time",
        parse_dates=["time"],
        usecols=[0, 1, 2],
    )
    df = process_observations(df, bro_id, **kwargs)
    return df


class GroundwaterLevelDossier(bro.XmlFileOrUrl):
    _rest_url = "https://publiek.broservices.nl/gm/gld/v1"

    def _read_contents(self, tree):
        ns = {
            "ns11": "http://www.broservices.nl/xsd/dsgld/1.0",
            "gldcommon": "http://www.broservices.nl/xsd/gldcommon/1.0",
            "waterml": "http://www.opengis.net/waterml/2.0",
            "swe": "http://www.opengis.net/swe/2.0",
        }
        glds = tree.findall(".//ns11:GLD_O", ns)
        if len(glds) != 1:
            raise (Exception("Only one gld supported"))
        gld = glds[0]
        for key in gld.attrib:
            setattr(self, key.split("}", 1)[1], gld.attrib[key])
        for child in gld:
            key = child.tag.split("}", 1)[1]
            if len(child) == 0:
                setattr(self, key, child.text)
            elif key == "monitoringPoint":
                well = child.find("gldcommon:GroundwaterMonitoringTube", ns)
                gmw_id = well.find("gldcommon:broId", ns).text
                setattr(self, "GroundwaterMonitoringWell", gmw_id)
                tube_nr = int(well.find("gldcommon:tubeNumber", ns).text)
                setattr(self, "tubeNumber", tube_nr)
            elif key in ["registrationHistory"]:
                self._read_children_of_children(child)
            elif key == "groundwaterMonitoringNet":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    setattr(self, key, grandchild[0].text)
            elif key == "observation":
                times = []
                values = []
                qualifiers = []
                for measurement in child.findall(".//waterml:MeasurementTVP", ns):
                    times.append(measurement.find("waterml:time", ns).text)
                    values.append(float(measurement.find("waterml:value", ns).text))
                    metadata = measurement.find("waterml:metadata", ns)
                    TVPMM = metadata.find("waterml:TVPMeasurementMetadata", ns)
                    qualifier = TVPMM.find("waterml:qualifier", ns)
                    value = qualifier.find("swe:Category", ns).find("swe:value", ns)
                    qualifiers.append(value.text)
                observation = pd.DataFrame(
                    {
                        "time": pd.to_datetime(times, utc=True),
                        "value": values,
                        "qualifier": qualifiers,
                    }
                ).set_index("time")
                if not hasattr(self, key):
                    self.observation = []
                self.observation.append(observation)
            else:
                logger.warning(f"Unknown key: {key}")
        if hasattr(self, "observation"):
            self.observation = pd.concat(self.observation)
            self.observation = process_observations(self.observation, self.broId)
        else:
            self.observation = _get_empty_observation_df()


def process_observations(
    df, bro_id, to_wintertime=True, drop_duplicates=True, sort=True
):
    if to_wintertime:
        # remove time zone information by transforming to dutch winter time
        one_hour = pd.Timedelta(1, unit="h")
        df.index = pd.to_datetime(df.index, utc=True).tz_localize(None) + one_hour

    if df.index.has_duplicates and drop_duplicates:
        duplicates = df.index.duplicated(keep="first")
        message = "{} contains {} duplicates (of {}). Keeping only first values."
        logger.warning(message.format(bro_id, duplicates.sum(), len(df.index)))
        df = df[~duplicates]

    if sort:
        # sort DataFrame
        df = df.sort_index()
    return df


def _get_empty_observation_df():
    columns = ["time", "value", "qualifier"]
    return pd.DataFrame(columns=columns).set_index("time")
