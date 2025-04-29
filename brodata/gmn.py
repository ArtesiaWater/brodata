import logging
from functools import partial

import pandas as pd

from . import bro

logger = logging.getLogger(__name__)


class GroundwaterMonitoringNetwork(bro.FileOrUrl):
    _rest_url = "https://publiek.broservices.nl/gm/gmn/v1"
    _xmlns = "http://www.broservices.nl/xsd/dsgmn/1.0"

    def _read_contents(self, tree):
        ns = {
            "brocom": "http://www.broservices.nl/xsd/brocommon/3.0",
            "gml": "http://www.opengis.net/gml/3.2",
            "gmncom": "http://www.broservices.nl/xsd/gmncommon/1.0",
            "xmlns": self._xmlns,
        }
        gmns = tree.findall(".//xmlns:GMN_PO", ns)
        if len(gmns) != 1:
            raise (Exception("Only one GMN_PO supported"))
        gmn = gmns[0]
        for key in gmn.attrib:
            setattr(self, key.split("}", 1)[1], gmn.attrib[key])
        for child in gmn:
            key = child.tag.split("}", 1)[1]
            if len(child) == 0:
                setattr(self, key, child.text)
            elif key == "monitoringNetHistory":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if key == "startDateMonitoring":
                        setattr(self, key, self._read_date(grandchild))
                    else:
                        logger.warning(f"Unknown key: {key}")
            elif key == "registrationHistory":
                self._read_children_of_children(child)
            elif key == "measuringPoint":
                if not hasattr(self, key):
                    self.measuringPoint = []
                point = {}
                self._read_children_of_children(child, point)
                self.measuringPoint.append(point)
            else:
                logger.warning(f"Unknown key: {key}")

        if hasattr(self, "measuringPoint"):
            self.measuringPoint = pd.DataFrame(self.measuringPoint)
            if (
                "broId" in self.measuringPoint.columns
                and "tubeNumber" in self.measuringPoint.columns
            ):
                self.measuringPoint = self.measuringPoint.set_index(
                    ["broId", "tubeNumber"]
                )
            if "date" in self.measuringPoint.columns:
                self.measuringPoint["date"] = pd.to_datetime(
                    self.measuringPoint["date"]
                )


cl = GroundwaterMonitoringNetwork

get_bro_ids_of_bronhouder = partial(bro._get_bro_ids_of_bronhouder, cl)
get_bro_ids_of_bronhouder.__doc__ = bro._get_bro_ids_of_bronhouder.__doc__
