import logging

from . import bro

logger = logging.getLogger(__name__)


class FormationResistanceDossier(bro.FileOrUrl):
    """Class to represent a Formation Resistance Dossier (FRD) from the BRO."""

    _rest_url = "https://publiek.broservices.nl/gm/frd/v1"

    def _read_contents(self, tree):
        raise (NotImplementedError("FormationResistanceDossier not available yet"))
        ns = {
            "brocom": "http://www.broservices.nl/xsd/brocommon/3.0",
            "gml": "http://www.opengis.net/gml/3.2",
            "frdcom": "http://www.broservices.nl/xsd/frdcommon/1.0",
            "xmlns": "http://www.broservices.nl/xsd/dsfrd/1.0",
        }
        frd = self._get_main_object(tree, "FRD_O", ns)
        for key in frd.attrib:
            setattr(self, key.split("}", 1)[1], frd.attrib[key])
        for child in frd:
            key = self._get_tag(child)
            if len(child) == 0:
                setattr(self, key, child.text)
            else:
                self._warn_unknown_tag(key)
