import logging
from functools import partial

import pandas as pd
import requests

from . import bro

logger = logging.getLogger(__name__)


class GroundwaterAnalysisReport(bro.FileOrUrl):
    _rest_url = "https://publiek.broservices.nl/gm/gar/v1"
    _xmlns = "http://www.broservices.nl/xsd/dsgar/1.0"

    def _read_csv(self, csvfile, **kwargs):
        df = pd.read_csv(csvfile, **kwargs)
        na_rows = df.index[df.isna().all(axis=1)]
        idata = df.iloc[: na_rows[0]].dropna(how="all", axis=1).squeeze().to_dict()
        for i in range(len(na_rows) - 1):
            idf = df.iloc[na_rows[i] + 2 : na_rows[i + 1]]
            idf.columns = df.iloc[na_rows[i] + 1]
            idf.columns.name = None
            idf = idf.dropna(how="all", axis=1)
            if "analysedatum" in idf.columns:
                key = "laboratoryAnalysis"
            else:
                key = "fieldResearch"
            idata[key] = idf
        for k, v in idata.items():
            setattr(self, k, v)

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
                if not hasattr(self, key):
                    self.fieldResearch = []
                self.fieldResearch.append(self._read_field_research(child))
            elif key == "laboratoryAnalysis":
                if not hasattr(self, key):
                    self.laboratoryAnalysis = []
                self.laboratoryAnalysis.append(self._read_laboratory_analysis(child))
            else:
                logger.warning(f"Unknown key: {key}")
        if hasattr(self, "fieldResearch"):
            self.fieldResearch = pd.concat(self.fieldResearch)
        if hasattr(self, "laboratoryAnalysis"):
            self.laboratoryAnalysis = pd.concat(self.laboratoryAnalysis)

    def _read_field_research(self, node):
        field_research = []

        d = {}
        for child in node:
            key = child.tag.split("}", 1)[1]
            if key == "samplingDateTime":
                d[key] = pd.to_datetime(child.text)
            elif key in ["samplingStandard", "valuationMethod"]:
                d[key] = child.text
            elif key in ["samplingDevice"]:
                d[key] = f"{child[0].tag.split('}', 1)[1]}: {child[0].text}"
            elif key in ["fieldObservation"]:
                d2 = {}
                self._read_children_of_children(child, d2)
                setattr(self, key, d2)
            elif key in ["fieldMeasurement"]:
                d2 = d.copy()
                for greatgrandchild in child:
                    key2 = greatgrandchild.tag.split("}", 1)[1]
                    if key2 in ["parameter", "qualityControlStatus"]:
                        d2[key2] = greatgrandchild.text
                    elif key2 in ["fieldMeasurementValue"]:
                        d2[key2] = float(greatgrandchild.text)
                        d2["uom"] = greatgrandchild.attrib["uom"]
                    else:
                        self._read_children_of_children(node, d2)
                field_research.append(d2)
            # field_research.append(d)
        df = pd.DataFrame(field_research)
        return df

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
                    d2 = d.copy()
                    for greatgrandchild in grandchild:
                        key2 = greatgrandchild.tag.split("}", 1)[1]
                        if key2 in ["parameter", "qualityControlStatus", "limitSymbol"]:
                            d2[key2] = greatgrandchild.text
                        elif key2 in ["analysisMeasurementValue", "reportingLimit"]:
                            d2[key2] = float(greatgrandchild.text)
                            d2["uom"] = greatgrandchild.attrib["uom"]
                        else:
                            logger.warning(f"Unknown key: {key2}")
                    laboratory_analysis.append(d2)
            # laboratory_analysis.append(d)
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


def _get_empty_observation_df():
    columns = [
        "analyticalTechnique",
        "valuationMethod",
        "parameter",
        "analysisMeasurementValue",
        "uom",
        "qualityControlStatus",
        "limitSymbol",
    ]
    return pd.DataFrame(columns=columns).set_index("analysisDate")


cl = GroundwaterAnalysisReport

get_bro_ids_of_bronhouder = partial(bro._get_bro_ids_of_bronhouder, cl=cl)
get_bro_ids_of_bronhouder.__doc__ = bro._get_bro_ids_of_bronhouder.__doc__
