import logging
import pandas as pd
import requests
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
                    key2 = grandchild.tag.split("}", 1)[1]
                    if key2 == "GroundwaterMonitoringNet":
                        setattr(self, key, grandchild[0].text)
                    else:
                        logger.warning(f"Unknown key: {key2}")
            elif key == "monitoringPoint":
                well = child.find("garcommon:GroundwaterMonitoringTube", ns)
                gmw_id = well.find("garcommon:broId", ns).text
                setattr(self, "groundwaterMonitoringWell", gmw_id)
                tube_nr = int(well.find("garcommon:tubeNumber", ns).text)
                setattr(self, "tubeNumber", tube_nr)
            elif key == "fieldResearch":
                self._read_children_of_children(child)
            elif key == "laboratoryAnalysis":
                if not hasattr(self, key):
                    self.laboratoryAnalysis = []
                self.laboratoryAnalysis.append(self._read_laboratory_analysis(child))
            else:
                logger.warning(f"Unknown key: {key}")
        if hasattr(self, "laboratoryAnalysis"):
            self.laboratoryAnalysis = pd.concat(self.laboratoryAnalysis)

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
                        if key in ["parameter", "qualityControlStatus", "limitSymbol"]:
                            d[key] = greatgrandchild.text
                        elif key == "analysisMeasurementValue":
                            d[key] = float(greatgrandchild.text)
                            d["uom"] = greatgrandchild.attrib["uom"]
                        else:
                            logger.warning(f"Unknown key: {key}")
                    self._read_children_of_children(grandchild, d)
            laboratory_analysis.append(d)
        df = pd.DataFrame(laboratory_analysis)
        if "analysisDate" in df.columns:
            df = df.set_index("analysisDate")
        return df


def get_parameter_list(url=None, timeout=5, to_file=None, **kwargs):
    if url is None:
        url = "https://publiek.broservices.nl/bro/refcodes/v1/attribute_values?domain=urn:bro:gar:ParameterList&version=latest"
    r = requests.get(url, timeout=timeout, **kwargs)
    if not r.ok:
        raise (Exception((f"Retieving data from {url} failed")))
    if to_file is not None:
        with open(to_file, "w") as f:
            f.write(r.text)
    data = r.json()["refDomainVersions"][0]["refCodes"]
    for d in data:
        for prop in d["refAttributeValues"]:
            d[prop["name"]] = prop["value"]
        d.pop("refAttributeValues")

    df = pd.json_normalize(data).set_index("code")
    return df
