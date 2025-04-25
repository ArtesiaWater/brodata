import logging
from functools import partial

from . import bro

logger = logging.getLogger(__name__)


class SoilFaceResearch(bro.FileOrUrl):
    _rest_url = "https://publiek.broservices.nl/sr/sfr/v2"
    _xmlns = "http://www.broservices.nl/xsd/dssfr/2.0"

    def _read_contents(self, tree):
        ns = {
            "brocom": "http://www.broservices.nl/xsd/brocommon/3.0",
            "gml": "http://www.opengis.net/gml/3.2",
            "sfrcom": "http://www.broservices.nl/xsd/sfrcommon/2.0",
            "xmlns": self._xmlns,
        }
        sfrs = tree.findall(".//xmlns:SFR_O", ns)
        if len(sfrs) != 1:
            raise (Exception("Only one SFR_O supported"))
        sfr = sfrs[0]
        for key in sfr.attrib:
            setattr(self, key.split("}", 1)[1], sfr.attrib[key])
        for child in sfr:
            key = child.tag.split("}", 1)[1]
            if len(child) == 0:
                setattr(self, key, child.text)
            else:
                logger.warning(f"Unknown key: {key}")


cl = SoilFaceResearch

get_bro_ids_of_bronhouder = partial(bro._get_bro_ids_of_bronhouder, cl)
get_bro_ids_of_bronhouder.__doc__ = bro._get_bro_ids_of_bronhouder.__doc__

get_characteristics = partial(bro._get_characteristics, cl)
get_characteristics.__doc__ = bro._get_characteristics.__doc__

get_data_in_extent = partial(bro._get_data_in_extent, cl)
get_data_in_extent.__doc__ = bro._get_data_in_extent.__doc__
