import logging
import requests
from io import StringIO
import numpy as np
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


def get_objects_as_csv(
    bro_id,
    rapportagetype="compact_met_timestamps",
    observatietype="regulier_voorlopig",
    to_file=None,
    **kwargs,
):
    """
    Haal een volledig grondwaterstandendossier op op basis van een BRO-ID als CSV (RFC
    4180) bestand. Er kan op rapportage type en observatie type worden gefilterd.

    Parameters
    ----------
    bro_id : string
        The BRO-ID of the Groundwater Level Dossier.
    rapportagetype : string, optional
        Type of report. Possible values are:
            "volledig" (Zo volledig mogelijk),
            "compact" (Compact met leesbare tijdstippen)
            "compact_met_timestamps" (Compact met unix epoch tijdstippen).
        Right now only "compact" and "compact_met_timestamps" are supported. The default
        is "compact_met_timestamps".
    observatietype : string, optional
        Type of observations. Possible values are:
            "regulier_beoordeeld" (observatietype = reguliere meting en mate beoordeling
                = volledig beoordeeld),
            "regulier_voorlopig" (observatietype = reguliere meting en mate beoordeling
                = voorlopig),
            "controle" (observatietype = controle meting)
            "onbekend" (observatietype = reguliere meting en mate beoordeling =
                onbekend).
        When observatietype is None gives all results, seperated by empty lines and a
        line with an explanation. The default is "regulier_voorlopig".
    to_file : string, optional
        DESCRIPTION. The default is None.

    Raises
    ------

        DESCRIPTION.

    Returns
    -------
    df : pd.DataFrame
        DESCRIPTION.

    """
    url = f"{GroundwaterLevelDossier._rest_url}/objectsAsCsv/{bro_id}"
    params = {
        "rapportagetype": rapportagetype,
    }
    if observatietype is not None:
        params["observatietype"] = observatietype
    req = requests.get(url, params=params)
    if req.status_code > 200:
        logger.error(req.json()["errors"][0]["message"])
        return
    if to_file is not None:
        with open(to_file, "w") as f:
            f.write(req.text)
    if rapportagetype not in ["compact", "compact_met_timestamps"]:
        raise (Exception(f"rapportagetype {rapportagetype} is not supported for now"))
    if observatietype is None:
        raise (Exception("observatietype is None is not supported."))
    if req.text == "":
        return None
    else:
        df = read_gld_csv(
            StringIO(req.text), bro_id, rapportagetype=rapportagetype, **kwargs
        )
        return df


def get_series_as_csv(
    bro_id, filter_on_status_quality_control=None, asISO8601=False, to_file=None
):
    """
    Geeft een tabel met als eerste kolom doorlopende tijdstippen (unix epoch) en als
    kolommen de daaraan gekoppeld de meetwaarden en opmerkingen van de verschilllende
    observatie typen (regulier_voorlopig, regulier_beoordeeld, controle en onbekend).
    Bedoeld voor bijvoorbeeld het grafisch weergeven van standen.

    Parameters
    ----------
    bro_id : string
        The BRO-ID of the Groundwater Level Dossier.
    filter_on_status_quality_control : string or list of strings, optional
        One or more quality control statusses that the measurements are categorized in.
        Possible values are onbeslist, goedgekeurd (and afgekeurd?). The default is None.
    asISO8601 : bool, optional
        If True, dan worden de tijdstippen in ISO8601 formaat weergegeven. Anders in de
        Unix Epoch. The default is False.
    to_file : string, optional
        DESCRIPTION. The default is None.

    Returns
    -------
    df : pd.DataFrame
        DESCRIPTION.

    """
    url = f"{GroundwaterLevelDossier._rest_url}/seriesAsCsv/{bro_id}"
    params = {}
    if filter_on_status_quality_control is not None:
        if not isinstance(filter_on_status_quality_control, str):
            filter_on_status_quality_control = ",".join(
                filter_on_status_quality_control
            )
        params["filterOnStatusQualityControl"] = filter_on_status_quality_control
    if asISO8601:
        params["asISO8601"] = ""
    req = requests.get(url, params=params)
    if req.status_code > 200:
        logger.error(req.json()["errors"][0]["message"])
        return
    if to_file is not None:
        with open(to_file, "w") as f:
            f.write(req.text)
    if req.text == "":
        return None
    else:
        df = pd.read_csv(StringIO(req.text))
        if "Tijdstip" in df.columns:
            if asISO8601:
                df["Tijdstip"] = pd.to_datetime(df["Tijdstip"])
            else:
                df["Tijdstip"] = pd.to_datetime(df["Tijdstip"], unit="ms")
            df = df.set_index("Tijdstip")
        return df


def get_observations(
    bro_id,
    as_csv=False,
    rapportagetype="compact_met_timestamps",
    observatietype="regulier_voorlopig",
    tmin=None,
    tmax=None,
    fname=None,
    **kwargs,
):
    if as_csv:
        # download the observations as csv
        if tmin is not None or tmax is not None:
            raise (Exception("tmin and tmax not supported when as_csv=True"))
        return get_objects_as_csv(
            bro_id, rapportagetype=rapportagetype, observatietype=observatietype
        )
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


def read_gld_csv(fname, bro_id, rapportagetype, **kwargs):
    names = [
        "time",
        "value",
        "qualifier",
        "censored_reason",
        "censoring_limitvalue",
        "interpolation_type",
    ]
    if rapportagetype == "compact":
        parse_dates = ["time"]
    else:
        parse_dates = None
    df = pd.read_csv(
        fname,
        names=names,
        index_col="time",
        parse_dates=parse_dates,
        usecols=[0, 1, 2],
    )
    if rapportagetype == "compact_met_timestamps":
        df.index = pd.to_datetime(df.index, unit="ms")
    df = process_observations(df, bro_id, **kwargs)
    return df


def get_observations_summary(bro_id):
    """
    Operatie die een samenvatting van een GLD ophaalt in JSON op basis van een BRO-ID

    Parameters
    ----------
    bro_id : string
        The BRO-ID of the Groundwater Level Dossier.

    Raises
    ------

        DESCRIPTION.

    Returns
    -------
    df : pd.DataFrame
        DESCRIPTION.

    """
    url = GroundwaterLevelDossier._rest_url
    url = "{}/objects/{}/observationsSummary".format(url, bro_id)
    req = requests.get(url)
    if req.status_code > 200:
        raise (Exception(req.json()["errors"][0]["message"]))
    df = pd.DataFrame(req.json())
    if "observationId" in df.columns:
        df = df.set_index("observationId")
    if "startDate" in df.columns:
        df["startDate"] = pd.to_datetime(df["startDate"], dayfirst=True)
    if "endDate" in df.columns:
        df["endDate"] = pd.to_datetime(df["endDate"], dayfirst=True)
    return df


class GroundwaterLevelDossier(bro.XmlFileOrUrl):
    _rest_url = "https://publiek.broservices.nl/gm/gld/v1"

    def _read_contents(self, tree, **kwargs):
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
                setattr(self, "groundwaterMonitoringWell", gmw_id)
                tube_nr = int(well.find("gldcommon:tubeNumber", ns).text)
                setattr(self, "tubeNumber", tube_nr)
            elif key in ["registrationHistory"]:
                self._read_children_of_children(child)
            elif key == "groundwaterMonitoringNet":
                for grandchild in child:
                    key2 = grandchild.tag.split("}", 1)[1]
                    if key2 == "GroundwaterMonitoringNet":
                        setattr(self, key, grandchild[0].text)
                    else:
                        logger.warning(f"Unknown key: {key2}")
            elif key == "observation":
                times = []
                values = []
                qualifiers = []
                for measurement in child.findall(".//waterml:MeasurementTVP", ns):
                    times.append(measurement.find("waterml:time", ns).text)
                    value = measurement.find("waterml:value", ns).text
                    if value is None:
                        values.append(np.nan)
                    else:
                        values.append(float(value))
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
            self.observation = process_observations(
                self.observation, self.broId, **kwargs
            )
        else:
            self.observation = _get_empty_observation_df()


def process_observations(
    df,
    bro_id,
    to_wintertime=True,
    drop_duplicates=True,
    sort=True,
    qualifier=None,
):
    if to_wintertime:
        # remove time zone information by transforming to dutch winter time
        one_hour = pd.Timedelta(1, unit="h")
        df.index = pd.to_datetime(df.index, utc=True).tz_localize(None) + one_hour

    if qualifier is not None:
        if isinstance(qualifier, str):
            df = df[df["qualifier"] == qualifier]
        else:
            df = df[df["qualifier"].isin(qualifier)]

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
