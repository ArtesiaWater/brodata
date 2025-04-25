import logging
import tempfile
from functools import partial
from io import StringIO

import pandas as pd
import requests

from . import bro

logger = logging.getLogger(__name__)


class ConePenetrationTest(bro.FileOrUrl):
    _rest_url = "https://publiek.broservices.nl/sr/cpt/v1"
    _xmlns = "http://www.broservices.nl/xsd/dscpt/1.1"
    _char = "CPT_C"

    def _read_contents(self, tree):
        ns = {
            "brocom": "http://www.broservices.nl/xsd/brocommon/3.0",
            "gml": "http://www.opengis.net/gml/3.2",
            "cptcommon": "http://www.broservices.nl/xsd/cptcommon/1.1",
            "xmlns": self._xmlns,
        }
        cpts = tree.findall(".//xmlns:CPT_O", ns)
        if len(cpts) > 1:
            raise (Exception("Only one CPT_0 supported"))
        elif len(cpts) == 0:
            raise (Exception("No CPT_0 found"))
        cpt = cpts[0]
        for key in cpt.attrib:
            setattr(self, key.split("}", 1)[1], cpt.attrib[key])
        for child in cpt:
            key = child.tag.split("}", 1)[1]
            if len(child) == 0:
                setattr(self, key, child.text)
            elif key == "standardizedLocation":
                self._read_standardized_location(child)
            elif key == "deliveredLocation":
                self._read_delivered_location(child)
            elif key in ["researchReportDate"]:
                setattr(self, key, self._read_date(child))
            elif key in ["deliveredVerticalPosition", "registrationHistory"]:
                self._read_children_of_children(child)
            elif key in ["conePenetrometerSurvey"]:
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if len(grandchild) == 0:
                        setattr(self, key, grandchild.text)
                    elif key in [
                        "finalProcessingDate",
                        "trajectory",
                        "conePenetrometer",
                        "procedure",
                    ]:
                        self._read_children_of_children(grandchild)
                    elif key == "parameters":
                        self._read_parameters(grandchild)
                    elif key == "conePenetrationTest":
                        self._read_cone_penetration_test(grandchild, key)
                    elif key == "dissipationTest":
                        self._read_cone_penetration_test(grandchild, key)
                    else:
                        logger.warning(f"Unknown key: {key}")
            elif key == "additionalInvestigation":
                self._read_additional_investigation(child)
            else:
                logger.warning(f"Unknown key: {key}")
        if hasattr(self, "conePenetrationTest") and hasattr(self, "parameters"):
            self.conePenetrationTest.columns = self.parameters.index
            if "penetrationLength" in self.conePenetrationTest.columns:
                self.conePenetrationTest = self.conePenetrationTest.set_index(
                    "penetrationLength"
                )

    def _read_parameters(self, node):
        self.parameters = pd.Series()
        for child in node:
            key = child.tag.split("}", 1)[1]
            self.parameters[key] = child.text

    def _read_cone_penetration_test(self, node, name):
        for child in node:
            key = child.tag.split("}", 1)[1]
            if key in ["phenomenonTime", "resultTime"]:
                setattr(self, f"{name}_{key}", self._read_time_instant(child))
            elif key in [
                "procedure",
                "observedProperty",
                "featureOfInterest",
                "penetrationLength",
            ]:
                self._read_children_of_children(child)
            elif key in ["cptResult", "disResult"]:
                for grandchild in child:
                    key2 = grandchild.tag.split("}", 1)[1]
                    if key2 == "encoding":
                        ns = {"swe": "http://www.opengis.net/swe/2.0"}
                        text_encoding = grandchild.find("swe:TextEncoding", ns)
                        for key3 in text_encoding.attrib:
                            setattr(self, f"{name}_{key3}", text_encoding.attrib[key3])

                    elif key2 == "elementCount":
                        pass
                    elif key2 == "elementType":
                        pass
                    elif key2 == "values":
                        values = pd.read_csv(
                            StringIO(grandchild.text),
                            header=None,
                            decimal=getattr(self, f"{name}_decimalSeparator"),
                            sep=getattr(self, f"{name}_tokenSeparator"),
                            lineterminator=getattr(self, f"{name}_blockSeparator"),
                            na_values=-999999,
                        )
                        setattr(self, name, values)
                    else:
                        logger.warning(f"Unknown key: {key}")
            else:
                logger.warning(f"Unknown key: {key}")

    def _read_additional_investigation(self, node):
        for child in node:
            key = child.tag.split("}", 1)[1]
            if len(child) == 0:
                setattr(self, key, child.text)
            elif key == "removedLayer":
                if not hasattr(self, key):
                    self.removedLayer = []
                d = {}
                self._read_children_of_children(
                    child,
                    d=d,
                    to_float=["upperBoundary", "lowerBoundary"],
                    to_int="sequenceNumber",
                )
                self.removedLayer.append(d)
        if hasattr(self, "removedLayer"):
            self.removedLayer = pd.DataFrame(self.removedLayer)
            if "sequenceNumber" in self.removedLayer.columns:
                self.removedLayer = self.removedLayer.set_index("sequenceNumber")


def get_graph_types(timeout=5):
    """
    Get the graph types that can be generated for CPT by the REST API of the BRO.

    Parameters
    ----------
    timeout : int or float, optional
        A number indicating how many seconds to wait for the client to make a connection
        and/or send a response. The default is 5.

    Returns
    -------
    pd.DataFrame
        A Pandas DataFrame that contains the supported graph types, with the columns
        'name' and 'description'. The index of this DataFrame contains the strings that
        can be used for the graphType-argument in nlmod.cpt.graph().

    """
    url = "https://publiek.broservices.nl/sr/cpt/v1/result/graph/types"
    r = requests.get(url)
    supported_graphs = r.json()["supportedGraphs"]
    assert len(supported_graphs) == 1
    return pd.DataFrame(supported_graphs[0]["graphs"]).set_index("graphType")


def graph(
    xml_file, graphType="cptCombinedLength", to_file=None, timeout=5, return_fname=False
):
    """
    Generate a svg-graph of a cpt-file (ConePenetrationTest).

    Parameters
    ----------
    xml_file : str
        The filename of the xml-file to generate a graphical representation of.
    graphType : str, optional
        The type of graph. Run `brodata.cpt.get_graph_types()` to view available graph
        types. The default is "cptCombinedLength".
    to_file : str, optional
        The filename to save the svg-file to. The default is None.
    timeout : int or float, optional
        A number indicating how many seconds to wait for the client to make a connection
        and/or send a response. The default is 5.
    return_fname : bool, optional
        If True, Return the filename of the svg-file. The default is False.

    Returns
    -------
    IPython.display.SVG or str
        A graphical representation of the svg-file or the filename of the svg-file.

    """
    url = "https://publiek.broservices.nl/sr/cpt/v1/result/graph/dispatch"

    params = {"graphType": graphType}
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


cl = ConePenetrationTest

get_bro_ids_of_bronhouder = partial(bro._get_bro_ids_of_bronhouder, cl=cl)
get_bro_ids_of_bronhouder.__doc__ = bro._get_bro_ids_of_bronhouder.__doc__

get_characteristics = partial(bro._get_characteristics, cl)
get_characteristics.__doc__ = bro._get_characteristics.__doc__

get_data_in_extent = partial(bro._get_data_in_extent, cl)
get_data_in_extent.__doc__ = bro._get_data_in_extent.__doc__
