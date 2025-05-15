import logging
from functools import partial
from io import StringIO

import numpy as np
import pandas as pd
import requests

from . import bro

logger = logging.getLogger(__name__)


def get_objects_as_csv(
    bro_id,
    rapportagetype="compact_met_timestamps",
    observatietype="regulier_voorlopig",
    to_file=None,
    **kwargs,
):
    """
    Fetch a complete Groundwater Level Dossier (GLD) as a CSV (RFC 4180) file
    based on the provided BRO-ID. The data can be filtered by report type and
    observation type.

    Parameters
    ----------
    bro_id : str
        The BRO-ID of the Groundwater Level Dossier to fetch.
    rapportagetype : str, optional
        Type of report. The valid values are:
        - "volledig" : Full report
        - "compact" : Compact report with readable timestamps
        - "compact_met_timestamps" : Compact report with Unix epoch timestamps
        Default is "compact_met_timestamps". Only "compact" and "compact_met_timestamps"
        are supported.
    observatietype : str, optional
        Type of observations. The valid values are:
        - "regulier_beoordeeld" : Regular measurement with full evaluation
        (observatietype = reguliere meting en mate beoordeling = volledig beoordeeld)
        - "regulier_voorlopig" : Regular measurement with preliminary evaluation
        (observatietype = reguliere meting en mate beoordeling = voorlopig)
        - "controle" : Control measurement
        (observatietype = controle meting)
        - "onbekend" : Unknown evaluation
        (observatietype = reguliere meting en mate beoordeling = onbekend)
        If None, all observation types will be included, separated by empty lines and with an explanation.
        Default is "regulier_voorlopig".
    to_file : str, optional
        If provided, the CSV data will be written to the specified file.
        If None, the function returns the CSV data as a DataFrame. Default is None.
    **kwargs : additional keyword arguments
        Additional arguments passed to `read_gld_csv`.

    Raises
    ------
    Exception
        If the `rapportagetype` is not supported, or if `observatietype` is None.

    Returns
    -------
    pd.DataFrame or None
        If successful, returns a DataFrame containing the parsed CSV data.
        If `to_file` is provided, returns None after saving the CSV to the specified file.
        If the request fails or returns empty data, returns None.

    Notes
    -----
    The function sends a GET request to the Groundwater Level Dossier API
    and fetches the data in CSV format. The `rapportagetype` and `observatietype`
    parameters can be used to filter the data.
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
    Get groundwater level series as a CSV, with timestamps and corresponding measurements.

    This function retrieves a table with timestamps (Unix epoch or ISO8601 format)
    as the first column and corresponding measurements for different observation
    types (regulier_voorlopig, regulier_beoordeeld, controle en onbekend) as columns.

    Parameters
    ----------
    bro_id : str
        The BRO-ID of the Groundwater Level Dossier.
    filter_on_status_quality_control : str or list of str, optional
        One or more quality control statuses to filter the measurements by.
        Possible values are 'onbeslist', 'goedgekeurd', and 'afgekeurd'.
        The default is None.
    asISO8601 : bool, optional
        If True, timestamps are returned in ISO8601 format; otherwise, in Unix
        epoch format. The default is False.
    to_file : str, optional
        If provided, the CSV data will be written to this file path. The default
        is None.

    Returns
    -------
    pd.DataFrame or None
        A DataFrame containing the time series of measurements, with timestamps
        as the index. Returns None if no data is available.
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


def read_gld_csv(fname, bro_id, rapportagetype, **kwargs):
    """
    Read and process a Groundwater Level Dossier (GLD) CSV file.

    This function reads a CSV file containing groundwater level observations,
    processes the data according to the specified report type (`rapportagetype`),
    and returns a DataFrame of the observations. The file is assumed to contain
    at least three columns: time, value, and qualifier. The 'time' column is parsed
    as datetime, and additional processing is applied to the data.

    Parameters
    ----------
    fname : str
        The path to the CSV file containing the groundwater level observations.
    bro_id : str
        The BRO-ID of the Groundwater Level Dossier being processed.
    rapportagetype : str
        The report type. Can be one of:
        - 'volledig': as complete as possible (not supported yet)
        - 'compact': simple format with time and value.
        - 'compact_met_timestamps': format with timestamps for each observation.
    **kwargs : additional keyword arguments
        Additional arguments passed to the `process_observations` function.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the processed observations with the following columns:
        - time: The observation time.
        - value: The observed groundwater level.
        - qualifier: The quality code of the observation.
        - censored_reason: Reason for censoring, if applicable.
        - censoring_limitvalue: Limit value for censoring, if applicable.
        - interpolation_type: The interpolation method used, if applicable.

    Notes
    -----
    The time column is parsed as a datetime index. If the report type is
    'compact_met_timestamps', the time values are converted from Unix epoch time
    (milliseconds) to a datetime format.
    """
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
    # remove empty indices
    mask = df.index.isna() & df.isna().all(1)
    if mask.any():
        df = df[~mask]
    df = process_observations(df, bro_id, **kwargs)
    return df


def get_observations_summary(bro_id):
    """
    Fetch a summary of a Groundwater Level Dossier (GLD) in JSON format based on
    the provided BRO-ID. The summary includes details about the groundwater level
    observations, such as observation ID, start and end dates.

    Parameters
    ----------
    bro_id : str
        The BRO-ID of the Groundwater Level Dossier to fetch the summary for.

    Raises
    ------
    Exception
        If the request to the API fails or the status code is greater than 200,
        an exception will be raised with the error message returned by the API.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the summary of the groundwater level observations.
        The DataFrame will be indexed by the `observationId` and include
        `startDate` and `endDate` columns, converted to `datetime` format.

    Notes
    -----
    The function sends a GET request to the REST API and processes the returned
    JSON data into a DataFrame. If the response contains valid `startDate` or
    `endDate` fields, they will be converted to `datetime` format using the
    `pd.to_datetime` function.
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


class GroundwaterLevelDossier(bro.FileOrUrl):
    """
    Represents a Groundwater Level Dossier (GLD) containing groundwater monitoring
    information and observation data.

    Attributes
    ----------
    _rest_url : str
        The REST URL used to fetch data for this class.

    observation : pd.DataFrame
        DataFrame containing groundwater level observations with time and value
        columns. The data is processed and filtered based on the provided arguments.

    tubeNumber : int
        The tube number associated with the observation.

    groundwaterMonitoringWell : str
        The BRO-ID of the groundwater monitoring well.

    Methods
    -------
    _read_contents(tree, **kwargs)
        Reads the XML content from the given tree and populates the attributes of
        the instance with the parsed data.
    """

    _rest_url = "https://publiek.broservices.nl/gm/gld/v1"

    def _read_contents(self, tree, status=None, observation_type=None, **kwargs):
        """
        Parse and extract data from the XML tree to populate the Groundwater Level
        Dossier attributes.

        This method reads and processes the XML contents, extracting relevant
        groundwater monitoring information such as the groundwater monitoring well,
        tube number, and observations. It also processes the observations into a
        DataFrame, which is filtered and transformed based on the provided arguments.

        Parameters
        ----------
        tree : xml.etree.ElementTree
            The XML tree to parse and extract data from.

        **kwargs : keyword arguments
            Additional parameters passed to the `process_observations` function to
            filter and transform the observations.

        Raises
        ------
        Exception
            If more than one or no GLD element is found in the XML tree.

        Notes
        -----
        The method expects the XML structure to adhere to the specified namespaces
        and element tags. It processes observation values, timestamps, and qualifiers
        into a pandas DataFrame.

        The observation data is stored in the `observation` attribute and can be
        accessed as a DataFrame.
        """
        ns = {
            "ns11": "http://www.broservices.nl/xsd/dsgld/1.0",
            "gldcommon": "http://www.broservices.nl/xsd/gldcommon/1.0",
            "waterml": "http://www.opengis.net/waterml/2.0",
            "swe": "http://www.opengis.net/swe/2.0",
            "om": "http://www.opengis.net/om/2.0",
            "xlink": "http://www.w3.org/1999/xlink",
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
                # get observation_metadata
                om_observation = child.find("om:OM_Observation", ns)
                if om_observation is None:
                    continue
                metadata = om_observation.find("om:metadata", ns)
                observation_metadata = metadata.find("waterml:ObservationMetadata", ns)

                # get status
                water_ml_status = observation_metadata.find("waterml:status", ns)
                if water_ml_status is None:
                    status_value = None
                else:
                    status_value = water_ml_status.attrib[
                        f"{{{ns['xlink']}}}href"
                    ].rsplit(":", 1)[-1]
                if status is not None and status != status_value:
                    continue

                # get observation_type
                parameter = observation_metadata.find("waterml:parameter", ns)
                named_value = parameter.find("om:NamedValue", ns)
                name = named_value.find("om:name", ns)
                assert (
                    name.attrib[f"{{{ns['xlink']}}}href"]
                    == "urn:bro:gld:ObservationMetadata:observationType"
                )
                value = named_value.find("om:value", ns)
                observation_type_value = value.text
                if (
                    observation_type is not None
                    and observation_type != observation_type_value
                ):
                    continue

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
                        "status": status_value,
                        "observation_type": observation_type_value,
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
    """
    Process groundwater level observations.

    This function processes a DataFrame containing groundwater level observations,
    applying the following operations based on the provided parameters:
    - Conversion to Dutch winter time (optional).
    - Filtering observations based on the qualifier.
    - Dropping duplicate observations (optional).
    - Sorting the observations by time (optional).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the groundwater level observations, with a time
        index and columns such as "value", "qualifier", etc.
    bro_id : str
        The BRO-ID of the Groundwater Level Dossier being processed.
    to_wintertime : bool, optional
        If True, the observation times are converted to Dutch winter time by
        removing any time zone information and adding one hour. Default is True.
    drop_duplicates : bool, optional
        If True, any duplicate observation times will be dropped, keeping only
        the first occurrence. Default is True.
    sort : bool, optional
        If True, the DataFrame will be sorted by the time index. Default is True.
    qualifier : str or list of str, optional
        If provided, the observations are filtered based on their "qualifier"
        column. Only rows with the specified qualifier(s) will be kept.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the processed observations, with duplicate rows
        (if any) removed, the time index sorted, and filtered by qualifier if
        applicable.

    Notes
    -----
    The function assumes the 'time' index is in UTC if `to_wintertime=True` and
    will convert it to Dutch winter time (CET/CEST).
    """
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


cl = GroundwaterLevelDossier

get_bro_ids_of_bronhouder = partial(bro._get_bro_ids_of_bronhouder, cl)
get_bro_ids_of_bronhouder.__doc__ = bro._get_bro_ids_of_bronhouder.__doc__
