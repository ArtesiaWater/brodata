import logging
import requests
from io import StringIO
import pandas as pd
import xml
from .webservices import get_gdf

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
        url = "https://publiek.broservices.nl/gm/gld/v1/objectsAsCsv/{}"
        if rapportagetype != "compact":
            raise (Exception("Only rapportagetype compact supported for now"))
        params = {
            "observatietype": observatietype,
            "rapportagetype": rapportagetype,
        }
        req = requests.get(url.format(bro_id), params=params)
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
        url = "https://publiek.broservices.nl/gm/gld/v1/objects/{}"
        params = {}
        if tmin is not None:
            tmin = pd.to_datetime(tmin)
            params["observationPeriodBeginDate"] = tmin.strftime("%Y-%m-%d")
        if tmax is not None:
            tmax = pd.to_datetime(tmax)
            params["observationPeriodEndDate"] = tmax.strftime("%Y-%m-%d")
        req = requests.get(url.format(bro_id), params=params)
        if req.status_code > 200:
            logger.error(req.json()["errors"][0]["message"])
            return
        if fname is not None:
            with open(fname, "w") as f:
                f.write(req.text)
        df = read_gld(req.text, **kwargs)
        return df


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


def read_gld(fname, file=False, **kwargs):
    ns = {
        "ns11": "http://www.broservices.nl/xsd/dsgld/1.0",
        "gldcommon": "http://www.broservices.nl/xsd/gldcommon/1.0",
        "waterml": "http://www.opengis.net/waterml/2.0",
        "swe": "http://www.opengis.net/swe/2.0",
    }
    if file:
        tree = xml.etree.ElementTree.parse(fname)
    else:
        tree = xml.etree.ElementTree.fromstring(fname)
    glds = tree.findall(".//ns11:GLD_O", ns)
    if len(glds) != 1:
        raise (Exception("Only one gld supported"))
    gld = glds[0]
    d = {}
    for key in gld.attrib:
        d[key.split("}", 1)[1]] = gld.attrib[key]
    observations = []
    for child in gld:
        key = child.tag.split("}", 1)[1]
        if len(child) == 0:
            d[key] = child.text
        elif key == "monitoringPoint":
            well = child.find("gldcommon:GroundwaterMonitoringTube", ns)
            d["GroundwaterMonitoringWell"] = well.find("gldcommon:broId", ns).text
            d["tubeNumber"] = int(well.find("gldcommon:tubeNumber", ns).text)
        elif key in ["registrationHistory"]:
            for grandchild in child:
                key = grandchild.tag.split("}", 1)[1]
                d[key] = grandchild.text
        elif key == "groundwaterMonitoringNet":
            for grandchild in child:
                key = grandchild.tag.split("}", 1)[1]
                d[key] = grandchild[0].text
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
            df = pd.DataFrame(
                {
                    "time": pd.to_datetime(times),
                    "value": values,
                    "qualifier": qualifiers,
                }
            ).set_index("time")
            observations.append(df)

        else:
            logger.warning(f"Unknown key: {key}")
    if len(observations) > 0:
        d["observation"] = pd.concat(observations)
        d["observation"] = process_observations(d["observation"], d["broId"], **kwargs)
    else:
        d["observation"] = _get_empty_observation_df()
    return pd.Series(d)


def process_observations(
    df, bro_id, to_wintertime=True, drop_duplicates=True, sort=True
):
    if to_wintertime:
        # remove time zone information by transforming to dutch winter time
        one_hour = pd.Timedelta(1, unit="H")
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
