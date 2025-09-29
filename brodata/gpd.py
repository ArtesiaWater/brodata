import logging
from functools import partial
import pandas as pd

from . import bro, util

logger = logging.getLogger(__name__)


class GroundwaterProductionDossier(bro.FileOrUrl):
    """Class to represent a Groundwater Production Dossier (GPD) from the BRO."""
    _rest_url = "https://publiek.broservices.nl/gu/gpd/v1"
    _xmlns = "http://www.broservices.nl/xsd/dsgpd/1.0"

    def _read_contents(self, tree):
        ns = {
            "brocom": "http://www.broservices.nl/xsd/brocommon/3.0",
            "xmlns": self._xmlns,
        }

        gpds = tree.findall(".//xmlns:GPD_O", ns)

        if len(gpds) == 0:
            raise (ValueError("No gpd found"))
        elif len(gpds) > 1:
            raise (Exception("Only one gpd supported"))
        gpd = gpds[0]

        for key in gpd.attrib:
            setattr(self, key.split("}", 1)[1], gpd.attrib[key])
        for child in gpd:
            key = self._get_tag(child)
            if len(child) == 0:
                setattr(self, key, child.text)
            elif key in ["registrationHistory", "lifespan"]:
                self._read_children_of_children(child)
            elif key == "report":
                if not hasattr(self, "report"):
                    self.report = []
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "Report":
                        self.report.append(self._read_report(grandchild))
                    else:
                        self._warn_unknown_tag(key)
            else:
                self._warn_unknown_tag(key)

        if hasattr(self, "report"):
            self.report = pd.DataFrame(self.report)
        if hasattr(self, "volumeSeries"):
            self.volumeSeries = pd.DataFrame(self.volumeSeries)
            for column in ["beginDate", "endDate"]:
                if column in self.volumeSeries.columns:
                    self.volumeSeries[column] = pd.to_datetime(
                        self.volumeSeries[column]
                    )
            if "volume" in self.volumeSeries.columns:
                self.volumeSeries["volume"] = pd.to_numeric(self.volumeSeries["volume"])

    def _read_report(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if len(child) == 0:
                d[key] = child.text
            elif key == "reportPeriod":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key in ["beginDate", "endDate"]:
                        d[key] = grandchild.text
                    else:
                        self._warn_unknown_tag(key)
            elif key == "volumeSeries":
                if not hasattr(self, "volumeSeries"):
                    self.volumeSeries = []
                vs = self._read_volume_series(child)
                vs["reportId"] = d["reportId"]
                self.volumeSeries.append(vs)

            elif key == "installationOrFacility":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "InstallationOrFacility":
                        self._read_installation_facility(grandchild)
                    else:
                        self._warn_unknown_tag(key)
            else:
                self._warn_unknown_tag(key)

        if "volumeSeries" in d:
            d["volumeSeries"] = pd.DataFrame(d["volumeSeries"])
        return d

    def _read_volume_series(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if len(child) == 0:
                d[key] = child.text
            elif key == "period":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key in ["beginDate", "endDate"]:
                        d[key] = grandchild.text
                    else:
                        self._warn_unknown_tag(key)
        return d

    def _read_installation_facility(self, node):
        for child in node:
            key = child.tag.split("}", 1)[1]
            if key == "relatedGroundwaterUsageFacility":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "GroundwaterUsageFacility":
                        for greatgrandchild in grandchild:
                            key2 = greatgrandchild.tag.split("}", 1)[1]
                            if key2 == "broId":
                                setattr(self, key, greatgrandchild.text)
                            else:
                                util._warn_unknown_key(key2, self)
                    else:
                        self._warn_unknown_tag(key)
            elif key == "relatedRealisedInstallation":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "RealisedInstallation":
                        for greatgrandchild in grandchild:
                            key2 = greatgrandchild.tag.split("}", 1)[1]
                            if key2 == "broId":
                                setattr(self, key, greatgrandchild.text)
                            elif key2 == "realisedInstallationId":
                                setattr(self, key2, greatgrandchild.text)
                            else:
                                util._warn_unknown_key(key2, self)
                    else:
                        self._warn_unknown_tag(key)
            else:
                self._warn_unknown_tag(key)


cl = GroundwaterProductionDossier

get_bro_ids_of_bronhouder = partial(bro._get_bro_ids_of_bronhouder, cl)
get_bro_ids_of_bronhouder.__doc__ = bro._get_bro_ids_of_bronhouder.__doc__
