import logging
import pandas as pd
from . import bro

logger = logging.getLogger(__name__)


class GroundwaterUtilisationFacility(bro.XmlFileOrUrl):
    _rest_url = "https://publiek.broservices.nl/gu/guf/v1"

    def _read_contents(self, tree):
        ns = {
            "brocom": "http://www.broservices.nl/xsd/brocommon/3.0",
            "gml": "http://www.opengis.net/gml/3.2",
            "gufcommon": "http://www.broservices.nl/xsd/gufcommon/1.0",
            "xmlns": "http://www.broservices.nl/xsd/dsguf/1.0",
        }
        gufs = tree.findall(".//xmlns:GUF_PO", ns)
        if len(gufs) == 0:
            gufs = tree.findall(".//xmlns:GUF_PPO", ns)
        if len(gufs) != 1:
            raise (Exception("Only one GUF_PO supported"))
        guf = gufs[0]
        for key in guf.attrib:
            setattr(self, key.split("}", 1)[1], guf.attrib[key])
        for child in guf:
            key = child.tag.split("}", 1)[1]
            if len(child) == 0:
                setattr(self, key, child.text)
            elif key == "standardizedLocation":
                location = child.find("brocom:location", ns)
                setattr(self, key, self._read_pos(location))
            elif key in ["registrationHistory"]:
                self._read_children_of_children(child)
            elif key == "validityPeriod":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if key == "startValidity":
                        setattr(self, key, self._read_date(grandchild))
                    else:
                        logger.warning(f"Unknown key: {key}")
            elif key == "lifespan":
                self._read_lifespan(child)
            elif key == "objectHistory":
                objectHistory = []
                for event in child:
                    d = {}
                    for grandchild in event:
                        key = grandchild.tag.split("}", 1)[1]
                        if key == "date":
                            d[key] = self._read_date(grandchild)
                        else:
                            d[key] = grandchild.text
                    objectHistory.append(d)
                setattr(self, "objectHistory", pd.DataFrame(objectHistory))
            elif key == "licence":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if key == "LicenceGroundwaterUsage":
                        setattr(
                            self, key, self._read_licence_groundwater_usage(grandchild)
                        )
                    else:
                        logger.warning(f"Unknown key: {key}")
            elif key == "realisedInstallation":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if key == "RealisedInstallation":
                        setattr(self, key, self._read_realised_installation(grandchild))
                    else:
                        logger.warning(f"Unknown key: {key}")
            else:
                logger.warning(f"Unknown key: {key}")

    def _read_lifespan(self, node, d=None):
        for child in node:
            key = child.tag.split("}", 1)[1]
            if key == "startTime":
                if d is None:
                    setattr(self, key, self._read_date(child))
                else:
                    d[key] = self._read_date(child)
            else:
                logger.warning(f"Unknown key: {key}")

    def _read_licence_groundwater_usage(self, node):
        d = {}
        for child in node:
            key = child.tag.split("}", 1)[1]
            if key in ["identificationLicence", "legalType", "", ""]:
                d[key] = child.text
            elif key == "usageTypeFacility":
                self._read_children_of_children(child, d)
            elif key == "lifespan":
                self._read_lifespan(child, d)
            else:
                logger.warning(f"Unknown key: {key}")
        return d

    def _read_realised_installation(self, node):
        d = {}
        for child in node:
            key = child.tag.split("}", 1)[1]
            if key in ["realisedInstallationId", "installationFunction"]:
                d[key] = child.text
            else:
                logger.warning(f"Unknown key: {key}")
        return d
