import requests
import xml
import logging
import pandas as pd
from . import bro

logger = logging.getLogger(__name__)


def get_bhrgt(bro_id):
    url = f"https://publiek.broservices.nl/sr/bhrgt/v2/objects/{bro_id}"
    req = requests.get(url)
    if req.status_code > 200:
        logger.error(req.json()["errors"][0]["message"])
        return
    return read_bhrgt(req.text)


def read_bhrgt(fname):
    tree = xml.etree.ElementTree.fromstring(fname)
    namespaces = {
        "brocom": "http://www.broservices.nl/xsd/brocommon/3.0",
        "gml": "http://www.opengis.net/gml/3.2",
        "bhrgtcom": "http://www.broservices.nl/xsd/bhrgtcommon/2.1",
        "xmlns": "http://www.broservices.nl/xsd/dsbhr-gt/2.1",
    }
    bhr_gts = tree.findall(".//xmlns:BHR_GT_O", namespaces=namespaces)
    if len(bhr_gts) != 1:
        raise (Exception("Only one gmw supported"))
    bhr_gt = bhr_gts[0]
    d = {}
    for key in bhr_gt.attrib:
        d[key.split("}", 1)[1]] = bhr_gt.attrib[key]
    for child in bhr_gt:
        key = child.tag.split("}", 1)[1]
        if len(child) == 0:
            d[key] = child.text
        elif key == "standardizedLocation":
            point = child.find("brocom:location", namespaces=namespaces)
            d["lat"], d["lon"] = bro.read_point(point)
        elif key == "deliveredLocation":
            location = child.find("bhrgtcom:location", namespaces=namespaces)
            point = location.find("gml:Point", namespaces=namespaces)
            d["x"], d["y"] = bro.read_point(point)
        elif key in ["researchReportDate", "siteCharacteristic"]:
            d[key] = child[0].text
        elif key in [
            "deliveredVerticalPosition",
            "registrationHistory",
            "reportHistory",
        ]:
            for grandchild in child:
                key = grandchild.tag.split("}", 1)[1]
                d[key] = grandchild.text
        elif key == "boring":
            logger.error(f"{key} not supported yet")
        elif key == "boreholeSampleDescription":
            for grandchild in child:
                key = grandchild.tag.split("}", 1)[1]
                if key == "descriptiveBoreholeLog":
                    for layer in grandchild.findall(
                        "bhrgtcom:layer", namespaces=namespaces
                    ):
                        print(layer)
                else:
                    d[key] = grandchild.text
        elif key == "boreholeSampleAnalysis":
            logger.error(f"{key} not supported yet")
        else:
            logger.warning(f"Unknown key: {key}")

    s = pd.Series(d)
    return s
