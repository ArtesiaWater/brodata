# ruff: noqa: F401
import logging
import tempfile

import pandas as pd
import requests

from . import bro

logger = logging.getLogger(__name__)


class _BoreholeResearch(bro.FileOrUrl):
    """Class to represent a Borehole Research (BHR) from the BRO."""

    def _read_contents(self, tree):
        ns = {
            "brocom": "http://www.broservices.nl/xsd/brocommon/3.0",
            "gml": "http://www.opengis.net/gml/3.2",
            "bhrgtcom": "http://www.broservices.nl/xsd/bhrgtcommon/2.1",
            "xmlns": self._xmlns,
        }
        bhrs = tree.findall(f".//xmlns:{self._object_name}", ns)
        if len(bhrs) != 1:
            raise (Exception(f"Only one {self._object_name} supported"))
        bhr = bhrs[0]
        for key in bhr.attrib:
            setattr(self, key.split("}", 1)[1], bhr.attrib[key])
        for child in bhr:
            key = self._get_tag(child)
            if len(child) == 0:
                setattr(self, key, child.text)
            elif key == "standardizedLocation":
                self._read_standardized_location(child)
            elif key == "deliveredLocation":
                self._read_delivered_location(child)
            elif key in ["researchReportDate"]:
                setattr(self, key, self._read_date(child))
            elif key in ["siteCharacteristic"]:
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key in ["landUse", "drained", "soilUse"]:
                        setattr(self, key, grandchild.text)
                    elif key in [
                        "meanHighestGroundwaterTable",
                        "meanLowestGroundwaterTable",
                    ]:
                        setattr(self, key, float(grandchild.text))
                    else:
                        self._warn_unknown_tag(key)
            elif key in [
                "registrationHistory",
                "reportHistory",
            ]:
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    setattr(self, key, grandchild.text)
            elif key == "deliveredVerticalPosition":
                self._read_delivered_vertical_position(child)
            elif key == "boring":
                if len(child) == 1 and self._get_tag(child[0]) == "Boring":
                    self._read_boring(child[0])
                else:
                    self._read_boring(child)
            elif key == "boreholeSampleDescription":
                self._read_borehole_sample_description(child)
            elif key == "boreholeSampleAnalysis":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "BoreholeSampleAnalysis":
                        self._read_borehole_sample_analysis(grandchild)
                    else:
                        self._warn_unknown_tag(key)
            else:
                self._warn_unknown_tag(key)
        for key in [
            "completedInterval",
            "boredInterval",
            "sampledInterval",
            "investigatedInterval",
        ]:
            if hasattr(self, key):
                setattr(self, key, pd.DataFrame(getattr(self, key)))

    def _read_boring(self, node):
        for child in node:
            key = self._get_tag(child)
            if key in ["boringStartDate", "boringEndDate"]:
                setattr(self, key, self._read_date(child))
            elif key in [
                "preparation",
                "trajectoryExcavated",
                "rockReached",
                "boringProcedure",
                "casingUsed",
                "flushingMedium",
                "stopCriterion",
                "trajectoryRemoved",
                "samplingProcedure",
                "subsurfaceContaminated",
                "boreholeCompleted",
            ]:
                setattr(self, key, child.text)
            elif key in ["finalDepthBoring", "groundwaterLevel", "finalDepthSampling"]:
                setattr(self, key, self._parse_float(child))
            elif key == "boredTrajectory":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key in ["beginDepth", "endDepth"]:
                        setattr(self, key, self._parse_float(grandchild))
            elif key in ["boredInterval", "sampledInterval", "completedInterval"]:
                self._read_interval(child, key)
            elif key == "boringTool":
                self._read_boring_tool(child)
            else:
                self._warn_unknown_tag(key)

    def _read_borehole_sample_analysis(self, node):
        for child in node:
            key = self._get_tag(child)
            if key == "analysisReportDate":
                setattr(self, key, self._read_date(child))
            elif key in ["analysisType", "locationSpecific"]:
                setattr(self, key, child.text)
            elif key == "investigatedInterval":
                if not hasattr(self, key):
                    setattr(self, key, [])
                for grandchild in child:
                    key2 = self._get_tag(grandchild)
                    if key2 == "InvestigatedInterval":
                        ii = self._read_investigated_interval(grandchild)
                        self.investigatedInterval.append(ii)
                    else:
                        self._warn_unknown_tag(key2)
            else:
                self._warn_unknown_tag(key)

    def _read_investigated_interval(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if key in ["beginDepth", "endDepth"]:
                d[key] = self._parse_float(child)
            elif key == "locationSpecific":
                d[key] = child.text
            elif key == "particleSizeDistributionDetermination":
                if key not in d:
                    d[key] = []
                for grandchild in child:
                    key2 = self._get_tag(grandchild)
                    if key2 == "ParticleSizeDistributionDetermination":
                        psdd = self._read_particle_size_distribution_determination(
                            grandchild
                        )
                        d[key].append(psdd)
                    else:
                        self._warn_unknown_tag(key2)
            else:
                self._warn_unknown_tag(key)
        if "particleSizeDistributionDetermination" in d:
            d["particleSizeDistributionDetermination"] = pd.DataFrame(
                d["particleSizeDistributionDetermination"]
            )
        return d

    def _read_particle_size_distribution_determination(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if key in [
                "determinationProcedure",
                "determinationMethod",
                "particleSizeDistributionStandardised",
                "dispersionMethod",
            ]:
                d[key] = child.text
            elif key == "nonStandardisedFraction":
                if key not in d:
                    d[key] = []
                d2 = {}
                for grandchild in child:
                    key2 = self._get_tag(grandchild)
                    if key2 in ["lowerBoundary", "upperBoundary"]:
                        d2[key2] = int(grandchild.text)
                    elif key2 in ["proportion"]:
                        d2[key2] = self._parse_float(grandchild)
                    else:
                        self._warn_unknown_tag(key2)
                d[key].append(d2)
            else:
                self._warn_unknown_tag(key)
        if "nonStandardisedFraction" in d:
            d["nonStandardisedFraction"] = pd.DataFrame(d["nonStandardisedFraction"])
        return d

    def _read_interval(self, node, key):
        if not hasattr(self, key):
            setattr(self, key, [])
        d = {}
        to_float = ["beginDepth", "endDepth"]
        self._read_children_of_children(node, d, to_float=to_float)
        getattr(self, key).append(d)

    def _read_boring_tool(self, node):
        d = {}
        to_float = ["boringToolDiameter", "beginDepth", "endDepth"]
        self._read_children_of_children(node, d, to_float=to_float)
        self.boringTool = d

    def _read_borehole_sample_description(self, node):
        for child in node:
            key = self._get_tag(child)
            if key == "BoreholeSampleDescription":
                self._read_borehole_sample_description(child)
            elif key == "descriptiveBoreholeLog":
                if not hasattr(self, "descriptiveBoreholeLog"):
                    self.descriptiveBoreholeLog = []
                if (
                    len(child) == 1
                    and self._get_tag(child[0]) == "DescriptiveBoreholeLog"
                ):
                    dbl = self._read_descriptive_borehole_log(child[0])
                else:
                    dbl = self._read_descriptive_borehole_log(child)
                self.descriptiveBoreholeLog.append(dbl)
            elif key == "descriptionReportDate":
                setattr(self, key, self._read_date(child))
            elif key == "result":
                self._read_borehole_sample_description_result(child)
            elif key in ["phenomenonTime", "resultTime"]:
                setattr(self, key, self._read_time_instant(child))
            elif key in [
                "descriptionProcedure",
                "procedure",
                "observedProperty",
                "featureOfInterest",
                "descriptionMethod",
                "descriptionLocation",
                "fractionDistributionDetermined",
            ]:
                setattr(self, key, child.text)
            elif key in ["lowerBoundarySandFraction"]:
                setattr(self, key, int(child.text))
            elif key == "soilClassification":
                if hasattr(self, key):
                    self._raise_assumed_single(key)
                setattr(self, key, self._read_soil_classification(child))
            else:
                self._warn_unknown_tag(key)

    def _read_soil_classification(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if key in [
                "codeGroup",
                "classificationCode",
                "featureTop",
                "soilClass",
                "subsoilPeat",
                "lowerBoundaryPeat",
                "textureClass",
                "textureProfile",
                "subsoilDuinVagueSoil",
                "carbonateProfile",
                "peatClass",
                "reworkingClass",
                "groundwaterTableClass",
                "featureSite",
            ]:
                d[key] = child.text
            elif key == "featureBottom":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "feature":
                        d[key] = grandchild.text
                    elif key == "beginDepth":
                        d[key] = float(grandchild.text)
                    else:
                        self._warn_unknown_tag(key)
            else:
                self._warn_unknown_tag(key)
        return d

    def _read_borehole_sample_description_result(self, node):
        boreholeSampleDescription = []
        for child in node:
            key = self._get_tag(child)
            if len(child) == 0:
                setattr(self, key, child.text)
            elif key == "soilLayer":
                d = {}
                to_float = [
                    "upperBoundary",
                    "lowerBoundary",
                    "organicMatterContent",
                    "clayContent",
                ]
                to_int = ["numberOfLayerComponents"]
                self._read_children_of_children(
                    child, d=d, to_float=to_float, to_int=to_int
                )
                boreholeSampleDescription.append(d)
            elif key == "litterLayer":
                if hasattr(self, key):
                    self._raise_assumed_single(key)
                setattr(self, key, {})
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key in [
                        "upperBoundary",
                        "lowerBoundary",
                        "organicMatterContent",
                    ]:
                        self.litterLayer[key] = float(grandchild.text)
                    elif key in ["horizonCode", "litterType"]:
                        self.litterLayer[key] = grandchild.text
                    else:
                        self._warn_unknown_tag(key)

            else:
                self._warn_unknown_tag(key)
        df = pd.DataFrame(boreholeSampleDescription)
        if hasattr(self, "boreholeSampleDescription"):
            self._raise_assumed_single("boreholeSampleDescription")
        setattr(self, "boreholeSampleDescription", df)


class GeotechnicalBoreholeResearch(_BoreholeResearch):
    """Class to represent a Geotechnical Borehole Research (BHR_GT) from the BRO."""

    _object_name = "BHR_GT_O"
    _xmlns = "http://www.broservices.nl/xsd/dsbhr-gt/2.1"
    _rest_url = "https://publiek.broservices.nl/sr/bhrgt/v2"
    _char = "BHR_GT_C"


def bhrgt_graph(
    xml_file,
    to_file=None,
    timeout=5,
    language="nl",
    asNAP=False,
    topMv=None,
    bottomMv=None,
    topNap=None,
    bottomNap=None,
    return_fname=False,
):
    """
    Generate a svg-graph of a bhrgt-file (GeotechnicalBoreholeResearch).

    Parameters
    ----------
    xml_file : str
        The filename of the xml-file to generate a graphical representation of.
    to_file : str, optional
        The filename to save the svg-file to. The default is None.
    timeout : int or float, optional
        A number indicating how many seconds to wait for the client to make a connection
        and/or send a response. The default is 5.
    language : str, optional
        DESCRIPTION. The default is "nl".
    asNAP : bool, optional
        If True, display the height of the drilling in m NAP. If False, display the
        height in meter below surface level. The default is False.
    topMv : float, optional
        Highest point in the graph, in m below surface level (mv). Needs to be specified
        together with bottomMv. The default is None.
    bottomMv : float, optional
        Lowest point in the graph, in m below surface level (mv). Needs to be specified
        together with topMv. The default is None.
    topNap : float, optional
        Highest point in the graph, in m NAP. Needs to be specified together with
        bottomNap. The default is None.
    bottomNap : float, optional
        Lowest point in the graph, in m NAP. Needs to be specified together with
        topNAP. The default is None.
    return_fname : bool, optional
        If True, Return the filename of the svg-file. The default is False.

    Returns
    -------
    IPython.display.SVG or str
        A graphical representation of the svg-file or the filename of the svg-file.

    """
    url = "https://publiek.broservices.nl/sr/bhrgt/v2/profile/graph/dispatch"
    params = {"language": language, "asNAP": asNAP}

    if ((topMv is None) + (bottomMv is None)) == 1:
        raise (ValueError("Both topMv and bottomMv need to be specified, or none"))
    if topMv is not None:
        params["topMv"] = topMv
        params["bottomMv"] = bottomMv

    if ((topNap is None) + (bottomNap is None)) == 1:
        raise (ValueError("Both topNap and bottomNap need to be specified, or none"))
    if topNap is not None:
        params["topNap"] = topNap
        params["bottomNap"] = bottomNap

    with open(xml_file, "rb") as data:
        r = requests.post(url, data=data, timeout=timeout, params=params)
    r.raise_for_status()
    if to_file is None:
        to_file = tempfile.NamedTemporaryFile(suffix=".svg").name
    with open(to_file, "w", encoding="utf-8") as f:
        f.write(r.text)
    if return_fname:
        return to_file
    else:
        from IPython.display import SVG

        return SVG(to_file)


class PedologicalBoreholeResearch(_BoreholeResearch):
    """Class to represent a Pedological Borehole Research (BHR_P) from the BRO."""

    _object_name = "BHR_O"
    _xmlns = "http://www.broservices.nl/xsd/dsbhr/2.0"
    _rest_url = "https://publiek.broservices.nl/sr/bhrp/v2"
    _char = "BHR_C"


class GeologicalBoreholeResearch(_BoreholeResearch):
    """Class to represent a Geological Borehole Research (BHR_G) from the BRO."""

    _object_name = "BHR_G_O"
    _xmlns = "http://www.broservices.nl/xsd/dsbhrg/3.1"
    _rest_url = "https://publiek.broservices.nl/sr/bhrg/v3"
    _char = "BHR_C"


def get_bro_ids_of_bronhouder(bronhouder, bhr_class=GeotechnicalBoreholeResearch):
    """
    Retrieve list of BRO (Basisregistratie Ondergrond) IDs for a given bronhouder.

    This function sends a GET request to the REST API to fetch the BRO IDs associated
    with the specified bronhouder. If the request is unsuccessful, it logs an error
    message.

    Parameters
    ----------
    bronhouder : str
        The identifier for the bronhouder to retrieve the associated BRO IDs.
    bhr_class : class
        The class of borehole objects. The default is GeotechnicalBoreholeResearch.
        Other options are PedologicalBoreholeResearch and GeologicalBoreholeResearch.

    Returns
    -------
    list or None
        A list of BRO IDs if the request is successful. Returns `None` if the request
        fails.
    """
    return bro._get_bro_ids_of_bronhouder(bhr_class, bronhouder)


def get_characteristics(bhr_class=GeotechnicalBoreholeResearch, **kwargs):
    """
    Get characteristics of a set of registered objects for a given object class.

    The maximum number of objects that can be retrieved is 2000 for a single request.

    Parameters
    ----------
    bhr_class : class
        The class of borehole objects. The default is GeotechnicalBoreholeResearch.
        Other options are PedologicalBoreholeResearch and GeologicalBoreholeResearch.
    tmin : str or pd.Timestamp, optional
        The minimum registrationPeriod of the requested characteristics. The default is
        None.
    tmax : str or pd.Timestamp, optional
        The maximum registrationPeriod of the requested characteristics. The default is
        None.
    extent : list or tuple of 4 floats, optional
        Download the characteristics within extent ([xmin, xmax, ymin, ymax]). The
        default is None.
    x : float, optional
        The x-coordinate of the center of the circle in which the characteristics are
        requested. The default is None.
    y : float, optional
        The y-coordinate of the center of the circle in which the characteristics are
        requested. The default is None.
    radius : float, optional
        The radius in meters of the center of the circle in which the characteristics
        are requested. The default is 1000.0.
    epsg : str, optional
        The coordinate reference system of the specified extent, x or y, and of the
        resulting GeoDataFrame. The default is 28992, which is the Dutch RD-system.
    to_file : str, optional
        When not None, save the characteristics to a file with a name as specified in
        to_file. The defaults None.
    use_all_corners_of_extent : bool, optional
        Because the extent by default is given in epsg 28992, some locations along the
        border of a requested extent will not be returned in the result. To solve this
        issue, when use_all_corners_of_extent is True, all four corners of the extent
        are used to calculate the minimum and maximum lan and lon values. The default is
        True.
    timeout : int or float, optional
        A number indicating how many seconds to wait for the client to make a connection
        and/or send a response. The default is 5.

    Returns
    -------
    gpd.GeoDataFrame
        A GeoDataFrame contraining the characteristics.

    Notes
    -----
    Haalt de karakteristieken op van een set van registratie objecten, gegeven een
    kenmerkenverzameling (kenset).

    De karakteristieken geven een samenvatting van een object zodat een verdere selectie
    gemaakt kan worden. Het past in een tweetrapsbenadering, waarbij de eerste stap
    bestaat uit het ophalen van de karakteristieken en de 2e stap uit het ophalen van de
    gewenste registratie objecten. Het resultaat van deze operatie is gemaximaliseerd op
    2000.
    """
    if not isinstance(bhr_class, type):
        raise TypeError(
            "First argument must be a class (a subclass of _BoreholeResearch), "
            "for example GeotechnicalBoreholeResearch. "
            "If you passed other arguments positionally, use keyword arguments "
            "instead, e.g. extent=[xmin, xmax, ymin, ymax]."
        )
    return bro._get_characteristics(bhr_class, **kwargs)


def get_data_in_extent(bhr_class=GeotechnicalBoreholeResearch, **kwargs):
    if not isinstance(bhr_class, type):
        raise TypeError(
            "First argument must be a class (a subclass of _BoreholeResearch), "
            "for example GeotechnicalBoreholeResearch. "
            "If you passed other arguments positionally, use keyword arguments "
            "instead, e.g. extent=[xmin, xmax, ymin, ymax]."
        )
    return bro._get_data_in_extent(bhr_class, **kwargs)
