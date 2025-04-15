import logging

from . import bro
from functools import partial

logger = logging.getLogger(__name__)


class ExplorationProductionConstruction(bro.FileOrUrl):
    _rest_url = "https://publiek.broservices.nl/ep/epc/v1/"

    def _read_contents(self, tree):
        raise (
            NotImplementedError(
                f"The reading of the contents of a {self.__class__.__name__} is not supported yet"
            )
        )


get_bro_ids_of_bronhouder = partial(
    bro._get_bro_ids_of_bronhouder, cl=ExplorationProductionConstruction
)
get_bro_ids_of_bronhouder.__doc__ = bro._get_bro_ids_of_bronhouder.__doc__

get_characteristics = partial(
    bro._get_characteristics, cl=ExplorationProductionConstruction
)
get_characteristics.__doc__ = bro._get_characteristics.__doc__
