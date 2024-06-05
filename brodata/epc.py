import logging
from . import bro

logger = logging.getLogger(__name__)


class ExplorationProductionConstruction(bro.XmlFileOrUrl):
    _rest_url = "https://publiek.broservices.nl/ep/epc/v1/"

    def _read_contents(self, tree):
        raise (
            NotImplementedError(
                f"The reading of the contents of a {self.__class__.__name__} is not supported yet"
            )
        )
