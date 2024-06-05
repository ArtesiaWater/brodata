import logging
from . import bro

logger = logging.getLogger(__name__)


class GroundwaterMonitoringNetwork(bro.XmlFileOrUrl):
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
            else:
                logger.warning(f"Unknown key: {key}")
