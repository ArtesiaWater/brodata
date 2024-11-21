import logging
import pandas as pd
from io import StringIO
from . import bro

logger = logging.getLogger(__name__)


def get_characteristics(**kwargs):
    """
    Get characteristics of Cone Penetration Tests (see bro.get_characteristics)
    """
    return bro.get_characteristics(ConePenetrationTest, **kwargs)


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
        if len(cpts) != 1:
            raise (Exception("Only one CPT_0 supported"))
        cpt = cpts[0]
        for key in cpt.attrib:
            setattr(self, key.split("}", 1)[1], cpt.attrib[key])
        for child in cpt:
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
