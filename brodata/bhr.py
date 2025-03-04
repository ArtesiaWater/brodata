import logging
import requests
import tempfile
import pandas as pd
from . import bro
from .webservices import get_gdf

logger = logging.getLogger(__name__)


def get_bhrp_within_extent(extent, config=None, timeout=5, silent=False):
    kind = "Bodemkundig booronderzoek (BRO)"
    gdf = get_gdf(
        kind,
        config=config,
        extent=extent,
        timeout=timeout,
    )

    bhrp_data = {}
    logger.warning("Reading of {kind} not supported yet")

    return gdf, bhrp_data


class _BoreholeResearch(bro.FileOrUrl):
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
            key = child.tag.split("}", 1)[1]
            if len(child) == 0:
                setattr(self, key, child.text)
            elif key == "standardizedLocation":
                point = child.find("brocom:location", ns)
                lat, lon = self._read_pos(point)
                setattr(self, "lat", lat)
                setattr(self, "lon", lon)
            elif key == "deliveredLocation":
                self._read_delivered_location(child)
            elif key in ["researchReportDate"]:
                setattr(self, key, self._read_date(child))
            elif key in ["siteCharacteristic"]:
                setattr(self, key, child[0].text)
            elif key in [
                "registrationHistory",
                "reportHistory",
            ]:
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    setattr(self, key, grandchild.text)
            elif key == "deliveredVerticalPosition":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if key == "verticalPositioningDate":
                        setattr(self, key, self._read_date(grandchild))
                    elif key == "offset":
                        setattr(self, key, float(grandchild.text))
                    else:
                        setattr(self, key, grandchild.text)
            elif key == "boring":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if len(grandchild) == 0:
                        setattr(self, key, grandchild.text)
                    elif key in ["boringStartDate", "boringEndDate"]:
                        setattr(self, key, self._read_date(grandchild))
                    elif key in [
                        "boredInterval",
                        "completedInterval",
                        "boringProcedure",
                        "boredTrajectory",
                    ]:
                        to_float = None
                        if key == "boredTrajectory":
                            to_float = ["beginDepth", "endDepth"]
                        self._read_children_of_children(grandchild, to_float=to_float)
                    elif key == "sampledInterval":
                        self._read_sampled_interval(grandchild)
                    elif key == "boringTool":
                        self._read_boring_tool(grandchild)
                    else:
                        logger.warning(f"Unknown key: {key}")
            elif key == "boreholeSampleDescription":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if key == "descriptiveBoreholeLog":
                        self._read_descriptive_borehole_log(grandchild)
                    elif key == "descriptionReportDate":
                        setattr(self, key, self._read_date(grandchild))
                    elif key == "result":
                        self._read_borehole_sample_description_result(grandchild)
                    else:
                        setattr(self, key, grandchild.text)
            else:
                logger.warning(f"Unknown key: {key}")
        if hasattr(self, "sampledInterval"):
            self.sampledInterval = pd.DataFrame(self.sampledInterval)

    def _read_sampled_interval(self, node):
        if not hasattr(self, "sampledInterval"):
            self.sampledInterval = []
        d = {}
        self._read_children_of_children(node, d)
        self.sampledInterval.append(d)

    def _read_boring_tool(self, node):
        d = {}
        to_float = ["boringToolDiameter", "beginDepth", "endDepth"]
        self._read_children_of_children(node, d, to_float=to_float)
        self.boringTool = d

    def _read_descriptive_borehole_log(self, node):
        if not hasattr(self, "descriptiveBoreholeLog"):
            self.descriptiveBoreholeLog = []
        d = {}
        to_float = ["upperBoundary", "lowerBoundary"]
        for child in node:
            key = child.tag.split("}", 1)[1]
            if len(child) == 0:
                d[key] = child.text
            elif key == "layer":
                if key not in d:
                    d[key] = []
                layer = {}
                self._read_children_of_children(child, d=layer, to_float=to_float)
                d[key].append(layer)
            else:
                logger.warning(f"Unknown key: {key}")
        if "layer" in d:
            d["layer"] = pd.DataFrame(d["layer"])

        self.descriptiveBoreholeLog.append(d)

    def _read_borehole_sample_description_result(self, node):
        boreholeSampleDescription = []
        for child in node:
            key = child.tag.split("}", 1)[1]
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
            else:
                logger.warning(f"Unknown key: {key}")
        df = pd.DataFrame(boreholeSampleDescription)
        setattr(self, "boreholeSampleDescription", df)


class GeotechnicalBoreholeResearch(_BoreholeResearch):
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
    _object_name = "BHR_O"
    _xmlns = "http://www.broservices.nl/xsd/dsbhr/2.0"
    _rest_url = "https://publiek.broservices.nl/sr/bhrp/v2"
    _char = "BHR_C"


class GeologicalBoreholeResearch(_BoreholeResearch):
    _object_name = "BHR_O"
    _xmlns = "http://www.broservices.nl/xsd/dsbhrg/2.0"
    _rest_url = "https://publiek.broservices.nl/sr/bhrg/v3"
    _char = "BHR_C"
