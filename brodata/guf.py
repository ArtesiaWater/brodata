import logging
from functools import partial

import pandas as pd
from shapely.geometry import Point

from . import bro

logger = logging.getLogger(__name__)


class GroundwaterUtilisationFacility(bro.FileOrUrl):
    _rest_url = "https://publiek.broservices.nl/gu/guf/v1"
    _xmlns = "http://www.broservices.nl/xsd/dsguf/1.0"
    _char = "GUF_C"
    _namespace = {
        "brocom": "http://www.broservices.nl/xsd/brocommon/3.0",
        "gml": "http://www.opengis.net/gml/3.2",
        "gufcommon": "http://www.broservices.nl/xsd/gufcommon/1.0",
        "xmlns": _xmlns,
    }

    def _read_contents(self, tree):
        ns = self._namespace
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
                self._read_standardized_location(child)
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
                        if hasattr(self, "licence"):
                            raise (ValueError("Assumed there is only one licence"))
                        setattr(
                            self,
                            "licence",
                            self._read_licence_groundwater_usage(grandchild),
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
            if key in ["identificationLicence", "legalType"]:
                d[key] = child.text
            elif key == "usageTypeFacility":
                self._read_children_of_children(child, d)
            elif key == "lifespan":
                self._read_lifespan(child, d)
            elif key == "designInstallation":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if key == "DesignInstallation":
                        if "designInstallation" in d:
                            raise (
                                ValueError(
                                    "Assumed there is only one designInstallation"
                                )
                            )
                        d["designInstallation"] = self._read_design_installation(
                            grandchild
                        )
                    else:
                        logger.warning(f"Unknown key: {key}")
            else:
                logger.warning(f"Unknown key: {key}")
        return d

    def _read_design_installation(self, node):
        d = {}
        for child in node:
            key = child.tag.split("}", 1)[1]
            if key in ["designInstallationId", "installationFunction"]:
                to_int = ["designInstallationId"]
                d[key] = self._parse_text(child, key, to_int=to_int)
            elif key == "geometry":
                d[key] = self._read_geometry(child)
            elif key in ["energyCharacteristics", "lifespan"]:
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    to_float = [
                        "energyCold",
                        "energyWarm",
                        "maximumInfiltrationTemperatureWarm",
                        "power",
                    ]
                    d[key] = self._parse_text(grandchild, key, to_float=to_float)
            elif key == "designLoop":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if key == "DesignLoop":
                        if "designLoop" in d:
                            raise (ValueError("Assumed there is only one designLoop"))
                        d["designLoop"] = self._read_design_loop(grandchild)
                    else:
                        logger.warning(f"Unknown key: {key}")
            else:
                logger.warning(f"Unknown key: {key}")
        return d

    def _read_design_loop(self, node):
        d = {}
        for child in node:
            key = child.tag.split("}", 1)[1]
            if key in ["designLoopId", "loopType"]:
                to_int = ["designLoopId"]
                d[key] = self._parse_text(child, key, to_int=to_int)
            elif key == "geometry":
                d[key] = self._read_geometry(child)
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
                to_int = ["realisedInstallationId"]
                d[key] = self._parse_text(child, key, to_int=to_int)
            elif key == "geometry":
                d[key] = self._read_geometry(child)
            elif key in "validityPeriod":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if key == "startValidity":
                        d[key] = self._read_date(grandchild)
                    else:
                        logger.warning(f"Unknown key: {key}")
            elif key in "lifespan":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if key == "startTime":
                        d[key] = self._read_date(grandchild)
                    else:
                        logger.warning(f"Unknown key: {key}")
            elif key == "realisedLoop":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if key == "RealisedLoop":
                        if "realisedLoop" in d:
                            raise (ValueError("Assumed there is only one realisedLoop"))
                        d["realisedLoop"] = self._read_realised_loop(grandchild)
                    else:
                        logger.warning(f"Unknown key: {key}")
            else:
                logger.warning(f"Unknown key: {key}")
        return d

    def _read_realised_loop(self, node):
        d = {}
        for child in node:
            key = child.tag.split("}", 1)[1]
            if key in ["realisedLoopId", "loopType", "loopDepth"]:
                to_float = ["loopDepth"]
                to_int = ["realisedLoopId"]
                d[key] = self._parse_text(child, key, to_float=to_float, to_int=to_int)
            elif key == "geometry":
                d[key] = self._read_geometry(child)
            elif key == "lifespan":
                self._read_lifespan(child, d)
            else:
                logger.warning(f"Unknown key: {key}")
        return d

    def _read_point(self, node):
        pos = node.find("gml:pos", self._namespace)
        x, y = [float(x) for x in pos.text.split()]
        return Point(x, y)

    def _read_geometry(self, node):
        ns = {
            "gml": "http://www.opengis.net/gml/3.2",
            "gufcommon": "http://www.broservices.nl/xsd/gufcommon/1.0",
        }
        point = node.find("gml:Point", self._namespace)
        if point is not None:
            return self._read_point(point)
        point_or_curve_or_surface = node.find("gufcommon:PointOrCurveOrSurface", ns)
        if point_or_curve_or_surface is not None:
            point = point_or_curve_or_surface.find("gml:Point", self._namespace)
            if point is not None:
                return self._read_point(point)
        logger.warning("Other types of geometries than point not supported yet")


cl = GroundwaterUtilisationFacility

get_bro_ids_of_bronhouder = partial(bro._get_bro_ids_of_bronhouder, cl)
get_bro_ids_of_bronhouder.__doc__ = bro._get_bro_ids_of_bronhouder.__doc__

get_characteristics = partial(bro._get_characteristics, cl)
get_characteristics.__doc__ = bro._get_characteristics.__doc__

get_data_in_extent = partial(bro._get_data_in_extent, cl)
get_data_in_extent.__doc__ = bro._get_data_in_extent.__doc__
