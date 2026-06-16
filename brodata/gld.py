import csv
import logging
import time
from functools import partial
from io import StringIO

import numpy as np
import pandas as pd

from . import bro, util

logger = logging.getLogger(__name__)


def get_objects_as_csv(
    bro_id,
    rapportagetype="volledig",
    observatietype=None,
    to_file=None,
    return_contents=True,
    **kwargs,
):
    """
    Fetch a complete Groundwater Level Dossier (GLD) as a CSV (RFC 4180) file
    based on the provided BRO-ID. The data can be filtered by report type and
    observation type.

    Parameters
    ----------
    bro_id : str
        The BRO-ID of the Groundwater Level Dossier to fetch. It can also be a full url,
        which is used by the gm-services. When using a full url, the parameter
        `rapportagetype` needs to reflect the choice in the url, and the parameter
        `observatietype` is ignored.
    rapportagetype : str, optional
        Type of report. The valid values are:
        - "volledig" : Full report
        - "compact" : Compact report with readable timestamps
        - "compact_met_timestamps" : Compact report with Unix epoch timestamps
        Default is "volledig".
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
        If None, all observation types will be returned. Default is None.
    to_file : str, optional
        If provided, the CSV data will be written to the specified file.
        If None, the function returns the CSV data as a DataFrame. Default is None.
    return_contents : bool, optional
        If True, the function returns the parsed CSV data as a DataFrame. If False,
        the function returns None after saving the CSV to the specified file (if
        `to_file` is provided). Default is True.
    **kwargs : additional keyword arguments
        Additional arguments passed to `read_gld_csv`.

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
    if bro_id.startswith("http"):
        req = util.get_with_rate_limit(bro_id)
    else:
        url = f"{GroundwaterLevelDossier._rest_url}/objectsAsCsv/{bro_id}"
        params = {
            "rapportagetype": rapportagetype,
        }
        if observatietype is not None:
            params["observatietype"] = observatietype
        req = util.get_with_rate_limit(url, params=params)
    req = _check_request_status(req)
    if to_file is not None:
        with open(to_file, "w") as f:
            f.write(req.text)
    if not return_contents:
        return
    if req.text == "":
        return None
    else:
        df = read_gld_csv(
            StringIO(req.text),
            bro_id,
            rapportagetype=rapportagetype,
            observatietype=observatietype,
            **kwargs,
        )
        return df


def _check_request_status(req):
    if req.status_code == 429:
        msg = "Too many requests. The BRO API has rate limits in place."
        logger.warning(msg)
        # try 3 times with increasing wait time
        wait_times = [1, 2, 4]
        for wait_time in wait_times:
            logger.warning(f"Waiting for {wait_time} seconds before retrying...")
            time.sleep(wait_time)
            req = util.get_with_rate_limit(req.url)
            if req.status_code <= 200:
                break
        if req.status_code == 429:
            raise Exception(msg + " Please try again later.")
    if req.status_code > 200:
        json_data = req.json()
        if "errors" in json_data:
            msg = json_data["errors"][0]["message"]
        else:
            msg = "{}: {}".format(json_data["title"], json_data["description"])
        raise Exception(msg)
    return req


def get_series_as_csv(
    bro_id, filter_on_status_quality_control=None, asISO8601=False, to_file=None
):
    """
    Get groundwater level series as a CSV, with timestamps and corresponding measurements.

    This function retrieves a table with measurements for different observation types
    (regulier_voorlopig, regulier_beoordeeld, controle en onbekend) as columns. It is
    intended for applications such as the graphical visualization of groundwater levels.

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
    req = util.get_with_rate_limit(url, params=params)
    req = _check_request_status(req)
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


def read_gld_csv(
    fname, bro_id, rapportagetype="volledig", observatietype=None, **kwargs
):
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
    rapportagetype : str, optional
        The report type. Can be one of:
        - 'volledig': as complete as possible
        - 'compact': simple format with time and value.
        - 'compact_met_timestamps': format with timestamps for each observation.
        Default is "volledig".
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
        If None, all observation types will be returned. Default is None.
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
    if observatietype is None or rapportagetype == "volledig":
        # the csv contains multiple observation types, seperated by a header with
        # observation-type and status.
        if isinstance(fname, StringIO):
            lines = fname.readlines()
        else:
            with open(fname, "r") as f:
                lines = f.readlines()

        # look for header lines
        headers = []
        if rapportagetype == "volledig":
            # the line with metdata is proceeded by a line starting with "observatie ID"
            for i, line in enumerate(lines):
                if line.startswith('"observatie ID",'):
                    headers.append(i + 1)
            header_length = 3
        else:
            # the line with metdata is proceeded by an empty line
            # but directly after the header, there can also be empty lines, that we skip
            data_lines = False
            for i, line in enumerate(lines):
                only_commas = all(c == "," for c in line.rstrip("\r\n"))
                last_line_was_header = len(headers) > 0 and headers[-1] == i - 1

                if only_commas:
                    if last_line_was_header:
                        data_lines = True
                    else:
                        data_lines = False
                else:
                    if not data_lines:
                        headers.append(i)
            header_length = 2

        dfs = []
        for i, header in enumerate(headers):
            line = lines[header]
            # split string by comma, but ignore commas between quotes
            reader = csv.reader(StringIO(line))
            parts = next(reader)
            observation_type = parts[3]
            status = parts[4]

            if i < len(headers) - 1:
                current_lines = lines[header + header_length : headers[i + 1] - 1]
            else:
                current_lines = lines[header + header_length :]
            df = pd.read_csv(
                StringIO("".join(current_lines)),
                names=names,
                index_col="time",
                parse_dates=parse_dates,
                usecols=[0, 1, 2],
            )
            # remove empty indices
            mask = df.index.isna() & df.isna().all(axis=1)
            if mask.any():
                df = df[~mask]
            df["status"] = status
            df["observation_type"] = observation_type
            dfs.append(df)
        if len(dfs) > 0:
            df = pd.concat(dfs)
        else:
            df = _get_empty_observation_df()
    else:
        df = pd.read_csv(
            fname,
            names=names,
            index_col="time",
            parse_dates=parse_dates,
            usecols=[0, 1, 2],
        )
        if observatietype == "regulier_beoordeeld":
            df["status"] = "volledigBeoordeeld"
            df["observation_type"] = "reguliereMeting"
        elif observatietype == "regulier_voorlopig":
            df["status"] = "voorlopig"
            df["observation_type"] = "reguliereMeting"
        elif observatietype == "controle":
            df["status"] = np.nan
            df["observation_type"] = "controleMeting"
        elif observatietype == "onbekend":
            df["status"] = "onbekend"
            df["observation_type"] = "reguliereMeting"
    if rapportagetype == "compact_met_timestamps":
        df.index = pd.to_datetime(df.index, unit="ms")
    # remove empty indices
    mask = df.index.isna() & df.isna().all(axis=1)
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
    req = util.get_with_rate_limit(url)
    req = _check_request_status(req)
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
    Class to represent a Groundwater Level Dossier (GLD) from the BRO.

    Attributes
    ----------
    observation : pd.DataFrame
        DataFrame containing groundwater level observations with time and value
        columns. The data is processed and filtered based on the provided arguments.

    tubeNumber : int
        The tube number associated with the observation.

    groundwaterMonitoringWell : str
        The BRO-ID of the groundwater monitoring well.
    """

    _rest_url = "https://publiek.broservices.nl/gm/gld/v1"

    def _read_contents(self, tree, status=None, observation_type=None, **kwargs):
        """
        Parse data to populate the Groundwater Level Dossier attributes.

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
            "xmlns": "http://www.broservices.nl/xsd/dsgld/1.0",
            "gldcommon": "http://www.broservices.nl/xsd/gldcommon/1.0",
            "waterml": "http://www.opengis.net/waterml/2.0",
            "swe": "http://www.opengis.net/swe/2.0",
            "om": "http://www.opengis.net/om/2.0",
            "xlink": "http://www.w3.org/1999/xlink",
        }
        gld = self._get_main_object(tree, "GLD_O", ns)
        for key in gld.attrib:
            setattr(self, key.split("}", 1)[1], gld.attrib[key])
        for child in gld:
            key = self._get_tag(child)
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
                        "time": times,
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
                self._warn_unknown_tag(key)
        if hasattr(self, "observation"):
            self.observation = pd.concat(self.observation)
            self.observation = process_observations(
                self.observation, self.broId, **kwargs
            )
        else:
            self.observation = _get_empty_observation_df()


def process_observations(
    df,
    bro_id="gld",
    to_wintertime=True,
    qualifier=None,
    tmin=None,
    tmax=None,
    sort=True,
    drop_duplicates=True,
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
        The BRO-ID of the Groundwater Level Dossier being processed. Only used for
        logging-purposes. The default is "gld".
    to_wintertime : bool, optional
        If True, the observation times are converted to Dutch winter time by
        removing any time zone information and adding one hour. If to_wintertime is
        False, observation times are kept in CET/CEST. Default is True.
    qualifier : str or list of str, optional
        If provided, the observations are filtered based on their "qualifier"
        column. Only rows with the specified qualifier(s) will be kept.
    tmin : str or datetime, optional
        The minimum time for filtering observations. Defaults to None.
    tmax : str or datetime, optional
        The maximum time for filtering observations. Defaults to None.
    sort : bool, optional
        If True, the DataFrame will be sorted, see `sort_observations`. Default is
        True.
    drop_duplicates : bool, optional
        If True, any duplicate observation times will be dropped, keeping only
        the first occurrence. Default is True.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the processed observations, with duplicate rows
        (if any) removed, the time index sorted, and filtered by qualifier if
        applicable.

    """
    df.index = pd.to_datetime(df.index, utc=True)
    if to_wintertime:
        # remove time zone information by transforming to dutch winter time
        df.index = df.index.tz_localize(None) + pd.Timedelta(1, unit="h")
    else:
        df.index = df.index.tz_convert("CET")

    if qualifier is not None:
        if isinstance(qualifier, str):
            df = df[df["qualifier"] == qualifier]
        else:
            df = df[df["qualifier"].isin(qualifier)]

    if tmin is not None:
        df = df.loc[pd.Timestamp(tmin) :]

    if tmax is not None:
        df = df.loc[: pd.Timestamp(tmax)]

    if sort:
        df = sort_observations(df)

    if drop_duplicates:
        df = drop_duplicate_observations(df, bro_id=bro_id, sort=sort)

    return df


def sort_observations(df):
    """
    Sort observations in a DataFrame by multiple criteria. Applies a multi-level sort
    to the input DataFrame, prioritizing the following criteria in order:
    1. By the DataFrame's DatetimeIndex in ascending order
    2. By status (if present): volledigBeoordeeld before voorlopig before onbekend
    3. By observation_type (if present): reguliereMeting before controleMeting

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame with optional 'observation_type' and 'status' columns,
        and a DatetimeIndex.

    Returns
    -------
    pandas.DataFrame
        Sorted DataFrame with the same structure as input.
    """
    if "observation_type" in df.columns:
        # make sure measurements with observation_type set to reguliereMeting are first
        sort_dict = {"reguliereMeting": 0, "controleMeting": 1}
        df = df.sort_values("observation_type", key=lambda x: x.map(sort_dict))

    if "status" in df.columns:
        # make sure measurements with status set to volledigBeoordeeld are first
        sort_dict = {"volledigBeoordeeld": 0, "voorlopig": 1, "onbekend": 2}
        df = df.sort_values("status", key=lambda x: x.map(sort_dict))

    # sort based on DatetimeIndex
    df = df.sort_index()

    return df


def drop_duplicate_observations(df, bro_id="gld", keep="first", sort=True):
    """
    Remove duplicate observations from a DataFrame based on its index.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to process.
    bro_id : str, optional
        Identifier for the dataset, used in warning messages. Default is "gld".
    keep : {'first', 'last', False}, optional
        Which duplicates to mark:
        - 'first' : Mark duplicates as True except for the first occurrence.
        - 'last' : Mark duplicates as True except for the last occurrence.
        - False : Mark all duplicates as True.
        Default is 'first'.

    Returns
    -------
    pd.DataFrame
        DataFrame with duplicate index values removed, keeping only the rows
        specified by the `keep` parameter.

    Warnings
    --------
    Logs a warning message if duplicates are found, indicating the number and
    total count of duplicates before removal.
    """
    if df.index.has_duplicates:
        duplicates = df.index.duplicated(keep=keep)
        message = "{} contains {} duplicates (of {}). Keeping only first values"
        message = message.format(bro_id, duplicates.sum(), len(df.index))
        if sort:
            message = f"{message} (sorted for importance)"
        message = f"{message}."
        logger.warning(message)
        df = df[~duplicates]
    return df


def _get_empty_observation_df():
    columns = ["time", "value", "qualifier", "status", "observation_type"]
    return pd.DataFrame(columns=columns).set_index("time")


cl = GroundwaterLevelDossier

get_bro_ids_of_bronhouder = partial(bro._get_bro_ids_of_bronhouder, cl)
get_bro_ids_of_bronhouder.__doc__ = bro._get_bro_ids_of_bronhouder.__doc__

get_data_for_bro_ids = partial(bro._get_data_for_bro_ids, cl)
get_data_for_bro_ids.__doc__ = bro._get_data_for_bro_ids.__doc__
