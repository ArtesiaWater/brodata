import logging
from functools import partial
import pandas as pd

from . import bro, util

logger = logging.getLogger(__name__)


class GroundwaterProductionDossier(bro.FileOrUrl):
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
            key = child.tag.split("}", 1)[1]
            if len(child) == 0:
                setattr(self, key, child.text)
            elif key in ["registrationHistory", "lifespan"]:
                self._read_children_of_children(child)
            elif key == "report":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if key == "Report":
                        if hasattr(self, "report"):
                            util._raise_assumed_single("report", self)
                        setattr(
                            self,
                            "report",
                            self._read_report(grandchild),
                        )
                    else:
                        util._warn_unknown_key(key, self)
            else:
                util._warn_unknown_key(key, self)

    def _read_report(self, node):
        d = {}
        for child in node:
            key = child.tag.split("}", 1)[1]
            if len(child) == 0:
                d[key] = child.text
            elif key == "reportPeriod":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if key in ["beginDate", "endDate"]:
                        d[key] = grandchild.text
                    else:
                        util._warn_unknown_key(key, self)
            elif key == "volumeSeries":
                if key not in d:
                    d[key] = []
                d[key].append(self._read_volume_series(child))

            elif key == "installationOrFacility":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if key == "InstallationOrFacility":
                        self._read_installation_facility(grandchild)
                    else:
                        util._warn_unknown_key(key, self)
            else:
                util._warn_unknown_key(key, self)

        if "volumeSeries" in d:
            d["volumeSeries"] = pd.DataFrame(d["volumeSeries"])
        return d

    def _read_volume_series(self, node):
        d = {}
        for child in node:
            key = child.tag.split("}", 1)[1]
            if len(child) == 0:
                d[key] = child.text
            elif key == "period":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if key in ["beginDate", "endDate"]:
                        d[key] = grandchild.text
                    else:
                        util._warn_unknown_key(key, self)
        return d

    def _read_installation_facility(self, node):
        for child in node:
            key = child.tag.split("}", 1)[1]
            if key == "relatedGroundwaterUsageFacility":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if key == "GroundwaterUsageFacility":
                        for greatgrandchild in grandchild:
                            key2 = greatgrandchild.tag.split("}", 1)[1]
                            if key2 == "broId":
                                setattr(self, key, greatgrandchild.text)
                            else:
                                util._warn_unknown_key(key2, self)
                    else:
                        util._warn_unknown_key(key, self)
            elif key == "relatedRealisedInstallation":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
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
                        util._warn_unknown_key(key, self)
            else:
                util._warn_unknown_key(key, self)


cl = GroundwaterProductionDossier

get_bro_ids_of_bronhouder = partial(bro._get_bro_ids_of_bronhouder, cl)
get_bro_ids_of_bronhouder.__doc__ = bro._get_bro_ids_of_bronhouder.__doc__
