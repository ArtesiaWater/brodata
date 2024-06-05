import logging
import pandas as pd
from . import bro
from .webservices import get_gdf

logger = logging.getLogger(__name__)


def get_bhrp_within_extent(extent, config=None, timeout=5, silent=False):
    kind = "Bodemkundig booronderzoek (BRO)"
    gdf = get_gdf(
        kind,
        config=config,
        extent=extent,
        timeout=timeout,
    )

    bhrp_data = {}
    logger.warning("Reading of {kind} not supported yet")

    return gdf, bhrp_data


class _BoreholeResearch(bro.XmlFileOrUrl):
    def _read_contents(self, tree):
        ns = {
            "brocom": "http://www.broservices.nl/xsd/brocommon/3.0",
            "gml": "http://www.opengis.net/gml/3.2",
            "bhrgtcom": "http://www.broservices.nl/xsd/bhrgtcommon/2.1",
            "xmlns": self._xmlns,
        }
        bhrs = tree.findall(f".//xmlns:{self._object_name}", ns)
        if len(bhrs) != 1:
            raise (Exception(f"Only one {self._object_name} supported"))
        bhr = bhrs[0]
        for key in bhr.attrib:
            setattr(self, key.split("}", 1)[1], bhr.attrib[key])
        for child in bhr:
            key = child.tag.split("}", 1)[1]
            if len(child) == 0:
                setattr(self, key, child.text)
            elif key == "standardizedLocation":
                point = child.find("brocom:location", ns)
                lat, lon = self._read_pos(point)
                setattr(self, "lat", lat)
                setattr(self, "lon", lon)
            elif key == "deliveredLocation":
                self._read_delivered_location(child)
            elif key in ["researchReportDate"]:
                setattr(self, key, self._read_date(child))
            elif key in ["siteCharacteristic"]:
                setattr(self, key, child[0].text)
            elif key in [
                "deliveredVerticalPosition",
                "registrationHistory",
                "reportHistory",
            ]:
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    setattr(self, key, grandchild.text)
            elif key == "boring":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if len(grandchild) == 0:
                        setattr(self, key, grandchild.text)
                    elif key in ["boringStartDate", "boringEndDate"]:
                        setattr(self, key, self._read_date(grandchild))
                    elif key in [
                        "boredInterval",
                        "completedInterval",
                        "boringProcedure",
                        "boredTrajectory",
                    ]:
                        self._read_children_of_children(grandchild)
                    elif key == "sampledInterval":
                        self._read_sampled_interval(grandchild)
                    elif key == "boringTool":
                        self._read_boring_tool(grandchild)
                    else:
                        logger.warning(f"Unknown key: {key}")
            elif key == "boreholeSampleDescription":
                for grandchild in child:
                    key = grandchild.tag.split("}", 1)[1]
                    if key == "descriptiveBoreholeLog":
                        self._read_descriptive_borehole_log(grandchild)
                    elif key == "descriptionReportDate":
                        setattr(self, key, self._read_date(grandchild))
                    else:
                        setattr(self, key, grandchild.text)
            else:
                logger.warning(f"Unknown key: {key}")
        if hasattr(self, "sampledInterval"):
            self.sampledInterval = pd.DataFrame(self.sampledInterval)

    def _read_sampled_interval(self, node):
        if not hasattr(self, "sampledInterval"):
            self.sampledInterval = []
        d = {}
        self._read_children_of_children(node, d)
        self.sampledInterval.append(d)

    def _read_boring_tool(self, node):
        d = {}
        self._read_children_of_children(node, d)
        self.boringTool = d

    def _read_descriptive_borehole_log(self, node):
        if not hasattr(self, "descriptiveBoreholeLog"):
            self.descriptiveBoreholeLog = []
        d = {}
        for child in node:
            key = child.tag.split("}", 1)[1]
            if len(child) == 0:
                d[key] = child.text
            elif key == "layer":
                if key not in d:
                    d[key] = []
                layer = {}
                self._read_children_of_children(child, d=layer)
                d[key].append(layer)
            else:
                logger.warning(f"Unknown key: {key}")
        if "layer" in d:
            d["layer"] = pd.DataFrame(d["layer"])

        self.descriptiveBoreholeLog.append(d)


class GeotechnicalBoreholeResearch(_BoreholeResearch):
    _object_name = "BHR_GT_O"
    _xmlns = "http://www.broservices.nl/xsd/dsbhr-gt/2.1"
    _rest_url = "https://publiek.broservices.nl/sr/bhrgt/v2"
    _char = "BHR_GT_C"


class PedologicalBoreholeResearch(_BoreholeResearch):
    _object_name = "BHR_O"
    _xmlns = "http://www.broservices.nl/xsd/dsbhr/2.0"
    _rest_url = "https://publiek.broservices.nl/sr/bhrp/v2"
    _char = "BHR_C"


class GeologicalBoreholeResearch(_BoreholeResearch):
    _object_name = "BHR_O"
    _xmlns = "http://www.broservices.nl/xsd/dsbhrg/2.0"
    _rest_url = "https://publiek.broservices.nl/sr/bhrg/v3"
    _char = "BHR_C"
