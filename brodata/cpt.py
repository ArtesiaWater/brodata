import logging
import pandas as pd
from io import StringIO
from . import bro

logger = logging.getLogger(__name__)


def get_characteristics(**kwargs):
    """
    Get characteristics of Cone Penetration Tests (see bro.get_characteristics)
    """
    return bro.get_characteristics("cpt", **kwargs)


class GeotechnischSondeeronderzoek(bro.XmlFileOrUrl):
    def _read_contents(self, tree):
        ns = {
            "brocom": "http://www.broservices.nl/xsd/brocommon/3.0",
            "gml": "http://www.opengis.net/gml/3.2",
            "cptcommon": "http://www.broservices.nl/xsd/cptcommon/1.1",
            "xmlns": "http://www.broservices.nl/xsd/dscpt/1.1",
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
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    setattr(self, key, grandchild.text)
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
                        "parameters",
                    ]:
                        self._read_children_of_children(grandchild)
                    elif key == "conePenetrationTest":
                        self.read_cone_penetration_test(grandchild)
                    else:
                        logger.warning(f"Unknown key: {key}")
            else:
                logger.warning(f"Unknown key: {key}")

    def read_cone_penetration_test(self, node):
        for child in node:
            key = child.tag.split("}", 1)[1]
            if key in ["phenomenonTime", "resultTime"]:
                setattr(self, key, self._read_time_instant(child))
            elif key in ["procedure", "observedProperty", "featureOfInterest"]:
                self._read_children_of_children(child)
            elif key == "cptResult":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if key == "encoding":
                        ns = {"swe": "http://www.opengis.net/swe/2.0"}
                        text_encoding = grandchild.find("swe:TextEncoding", ns)
                        for key in text_encoding.attrib:
                            setattr(self, key, text_encoding.attrib[key])

                    elif key == "values":
                        values = pd.read_csv(
                            StringIO(grandchild.text),
                            header=None,
                            decimal=self.decimalSeparator,
                            sep=self.tokenSeparator,
                            lineterminator=self.blockSeparator,
                        )
                        setattr(self, key, values)
                    else:
                        logger.warning(f"Unknown key: {key}")
            else:
                logger.warning(f"Unknown key: {key}")
