import logging
from functools import partial

import pandas as pd

from . import bro

logger = logging.getLogger(__name__)


class GroundwaterUtilisationFacility(bro.FileOrUrl):
    """Class to represent a Groundwater Utilisation Facility (GUF) from the BRO.

    Attributes
    ----------
    broId : str
        The BRO identifier of the GroundwaterUtilisationFacility object.
    objectHistory : pd.DataFrame
        DataFrame with the history of changes to the GUF object.
    licence : dict
        Dictionary with information about the groundwater usage licence.
    realisedInstallation : dict
        Dictionary with information about the realised installation.
    """

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
            key = self._get_tag(child)
            if len(child) == 0:
                setattr(self, key, child.text)
            elif key == "standardizedLocation":
                self._read_standardized_location(child)
            elif key in ["registrationHistory"]:
                self._read_children_of_children(child)
            elif key == "validityPeriod":
                self._read_validity_period(child)
            elif key == "lifespan":
                self._read_lifespan(child)
            elif key == "objectHistory":
                objectHistory = []
                for event in child:
                    d = {}
                    for grandchild in event:
                        key = self._get_tag(grandchild)
                        if key == "date":
                            d[key] = self._read_date(grandchild)
                        else:
                            d[key] = grandchild.text
                    objectHistory.append(d)
                setattr(self, "objectHistory", pd.DataFrame(objectHistory))
            elif key == "licence":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "LicenceGroundwaterUsage":
                        if hasattr(self, "licence"):
                            self._raise_assumed_single("licence")
                        setattr(
                            self,
                            "licence",
                            self._read_licence_groundwater_usage(grandchild),
                        )
                    else:
                        self._warn_unknown_tag(key)
            elif key == "realisedInstallation":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "RealisedInstallation":
                        setattr(
                            self,
                            "realisedInstallation",
                            self._read_realised_installation(grandchild),
                        )
                    else:
                        self._warn_unknown_tag(key)
            else:
                self._warn_unknown_tag(key)
        if hasattr(self, "designLoop"):
            self.designLoop = pd.DataFrame(self.designLoop)
        if hasattr(self, "designWell"):
            self.designWell = pd.DataFrame(self.designWell)
        if hasattr(self, "realisedLoop"):
            self.realisedLoop = pd.DataFrame(self.realisedLoop)
        if hasattr(self, "realisedWell"):
            self.realisedWell = pd.DataFrame(self.realisedWell)
        if hasattr(self, "licensedQuantity"):
            self.licensedQuantity = pd.DataFrame(self.licensedQuantity)
        if hasattr(self, "designInstallation"):
            self.designInstallation = pd.DataFrame(self.designInstallation)

    def _read_licence_groundwater_usage(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if key in ["identificationLicence", "legalType"]:
                d[key] = child.text
            elif key == "usageTypeFacility":
                self._read_children_of_children(child, d)
            elif key == "lifespan":
                self._read_lifespan(child, d)
            elif key == "designInstallation":
                if not hasattr(self, key):
                    self.designInstallation = []
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "DesignInstallation":
                        di = self._read_design_installation(grandchild)
                        self.designInstallation.append(di)
                    else:
                        self._warn_unknown_tag(key)
            elif key == "licensedQuantity":
                if not hasattr(self, key):
                    self.licensedQuantity = []
                lq = {}
                self._read_children_of_children(child, d=lq)
                self.licensedQuantity.append(lq)
            else:
                self._warn_unknown_tag(key)
        return d

    def _read_design_installation(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if key in ["designInstallationId", "installationFunction"]:
                to_int = ["designInstallationId"]
                d[key] = self._parse_text(child, key, to_int=to_int)
            elif key == "geometry":
                d[key] = self._read_geometry(child)
            elif key in ["energyCharacteristics", "lifespan"]:
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    to_float = [
                        "energyCold",
                        "energyWarm",
                        "maximumInfiltrationTemperatureWarm",
                        "power",
                    ]
                    d[key] = self._parse_text(grandchild, key, to_float=to_float)
            elif key == "designLoop":
                if not hasattr(self, key):
                    self.designLoop = []
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "DesignLoop":
                        self.designLoop.append(self._read_design_loop(grandchild))
                    else:
                        self._warn_unknown_tag(key)
            elif key == "designWell":
                if not hasattr(self, key):
                    self.designWell = []
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "DesignWell":
                        self.designWell.append(self._read_design_well(grandchild))
                    else:
                        self._warn_unknown_tag(key)
            else:
                self._warn_unknown_tag(key)
        return d

    def _read_design_loop(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if key in ["designLoopId", "loopType"]:
                to_int = ["designLoopId"]
                d[key] = self._parse_text(child, key, to_int=to_int)
            elif key == "geometry":
                d[key] = self._read_geometry(child)
            elif key == "lifespan":
                self._read_lifespan(child, d)
            else:
                self._warn_unknown_tag(key)
        return d

    def _read_design_well(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if key in [
                "designWellId",
                "wellFunction",
                "height",
                "maximumWellDepth",
                "maximumWellCapacity",
                "relativeTemperature",
            ]:
                to_int = ["designWellId"]
                to_float = ["height", "maximumWellDepth", "maximumWellCapacity"]
                d[key] = self._parse_text(child, key, to_int=to_int, to_float=to_float)
            elif key == "geometry":
                d[key] = self._read_geometry(child)
            elif key == "designScreen":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key in ["screenType", "designScreenTop", "designScreenBottom"]:
                        to_float = ["designScreenTop", "designScreenBottom"]
                        d[key] = self._parse_text(child, key, to_float=to_float)
            elif key == "lifespan":
                self._read_lifespan(child, d)
            else:
                self._warn_unknown_tag(key)
        return d

    def _read_realised_installation(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if key in ["realisedInstallationId", "installationFunction"]:
                to_int = ["realisedInstallationId"]
                d[key] = self._parse_text(child, key, to_int=to_int)
            elif key == "geometry":
                d[key] = self._read_geometry(child)
            elif key in "validityPeriod":
                self._read_validity_period(child, d=d)
            elif key in "lifespan":
                self._read_lifespan(child, d=d)
            elif key == "realisedLoop":
                if not hasattr(self, key):
                    self.realisedLoop = []
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "RealisedLoop":
                        self.realisedLoop.append(self._read_realised_loop(grandchild))
                    else:
                        self._warn_unknown_tag(key)
            elif key == "realisedWell":
                if not hasattr(self, key):
                    self.realisedWell = []
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "RealisedWell":
                        loop = self._read_realised_well(grandchild)
                        self.realisedWell.append(loop)
                    else:
                        self._warn_unknown_tag(key)
            else:
                self._warn_unknown_tag(key)
        return d

    def _read_realised_loop(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if key in ["realisedLoopId", "loopType", "loopDepth"]:
                to_float = ["loopDepth"]
                to_int = ["realisedLoopId"]
                d[key] = self._parse_text(child, key, to_float=to_float, to_int=to_int)
            elif key == "geometry":
                d[key] = self._read_geometry(child)
            elif key == "lifespan":
                self._read_lifespan(child, d)
            else:
                self._warn_unknown_tag(key)
        return d

    def _read_realised_well(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if key in [
                "realisedWellId",
                "wellFunction",
                "height",
                "wellDepth",
                "relativeTemperature",
            ]:
                to_float = ["height", "wellDepth"]
                to_int = ["realisedLoopId"]
                d[key] = self._parse_text(child, key, to_float=to_float, to_int=to_int)
            elif key == "geometry":
                d[key] = self._read_geometry(child)
            elif key == "validityPeriod":
                self._read_validity_period(child, d=d)
            elif key == "lifespan":
                self._read_lifespan(child, d)
            elif key == "realisedScreen":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key in [
                        "realisedScreenId",
                        "screenType",
                        "topScreenDepth",
                        "length",
                        # "relativeTemperature",
                    ]:
                        if key == "realisedScreenId" and key in d:
                            self._raise_assumed_single("realisedScreenId")
                        to_int = ["realisedScreenId"]
                        to_float = ["topScreenDepth", "length"]
                        d[key] = self._parse_text(child, key, to_float=to_float)
                    elif key == "validityPeriod":
                        self._read_validity_period(child, d=d)
                    elif key == "lifespan":
                        self._read_lifespan(grandchild, d)

            else:
                self._warn_unknown_tag(key)
        return d

    def _read_geometry(self, node):
        assert len(node) == 1
        ns = {
            "gml": "http://www.opengis.net/gml/3.2",
            "gufcommon": "http://www.broservices.nl/xsd/gufcommon/1.0",
        }
        point_or_curve_or_surface = node.find("gufcommon:PointOrCurveOrSurface", ns)
        if point_or_curve_or_surface is not None:
            node = point_or_curve_or_surface
        return super()._read_geometry(node)


cl = GroundwaterUtilisationFacility

get_bro_ids_of_bronhouder = partial(bro._get_bro_ids_of_bronhouder, cl)
get_bro_ids_of_bronhouder.__doc__ = bro._get_bro_ids_of_bronhouder.__doc__

get_characteristics = partial(bro._get_characteristics, cl)
get_characteristics.__doc__ = bro._get_characteristics.__doc__

get_data_in_extent = partial(bro._get_data_in_extent, cl)
get_data_in_extent.__doc__ = bro._get_data_in_extent.__doc__
