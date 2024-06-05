import logging
import pandas as pd
from . import bro

logger = logging.getLogger(__name__)


class GroundwaterAnalysisReport(bro.XmlFileOrUrl):
    _rest_url = "https://publiek.broservices.nl/gm/gar/v1"
    _xmlns = "http://www.broservices.nl/xsd/dsgar/1.0"

    def _read_contents(self, tree):
        ns = {
            "brocom": "http://www.broservices.nl/xsd/brocommon/3.0",
            "gml": "http://www.opengis.net/gml/3.2",
            "garcommon": "http://www.broservices.nl/xsd/garcommon/1.0",
            "xmlns": self._xmlns,
        }
        gars = tree.findall(".//xmlns:GAR_O", ns)
        if len(gars) != 1:
            raise (Exception("Only one GAR_O supported"))
        gar = gars[0]
        for key in gar.attrib:
            setattr(self, key.split("}", 1)[1], gar.attrib[key])
        for child in gar:
            key = child.tag.split("}", 1)[1]
            if len(child) == 0:
                setattr(self, key, child.text)
            elif key == "registrationHistory":
                self._read_children_of_children(child)
            elif key == "groundwaterMonitoringNet":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    setattr(self, key, grandchild[0].text)
            elif key == "monitoringPoint":
                well = child.find("garcommon:GroundwaterMonitoringTube", ns)
                gmw_id = well.find("garcommon:broId", ns).text
                setattr(self, "GroundwaterMonitoringWell", gmw_id)
                tube_nr = int(well.find("garcommon:tubeNumber", ns).text)
                setattr(self, "tubeNumber", tube_nr)
            elif key == "fieldResearch":
                self._read_children_of_children(child)
            elif key == "laboratoryAnalysis":
                setattr(self, key, self._read_laboratory_analysis(child))
            else:
                logger.warning(f"Unknown key: {key}")

    def _read_laboratory_analysis(self, node):
        laboratory_analysis = []
        for child in node:
            d = {}
            for grandchild in child:
                key = grandchild.tag.split("}", 1)[1]
                if key == "analysisDate":
                    d[key] = self._read_date(grandchild)
                elif key in ["analyticalTechnique", "valuationMethod"]:
                    d[key] = grandchild.text
                elif key == "analysis":
                    for greatgrandchild in grandchild:
                        key = greatgrandchild.tag.split("}", 1)[1]
                        if key in ["parameter", "qualityControlStatus"]:
                            d[key] = greatgrandchild.text
                        elif key == "analysisMeasurementValue":
                            d[key] = float(greatgrandchild.text)
                            d["uom"] = greatgrandchild.attrib["uom"]
                        else:
                            logger.warning(f"Unknown key: {key}")
                    self._read_children_of_children(grandchild, d)
            laboratory_analysis.append(d)
        return pd.DataFrame(laboratory_analysis)
