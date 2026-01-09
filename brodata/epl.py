import logging
from functools import partial

from . import bro

logger = logging.getLogger(__name__)


class ExplorationProductionLicence(bro.FileOrUrl):
    """Class to represent a ExplorationProductionLicence (EPL) from the BRO."""

    _rest_url = "https://publiek.broservices.nl/ep/epl/v1"

    def _read_contents(self, tree):
        raise (
            NotImplementedError(
                f"The reading of the contents of a {self.__class__.__name__} is not supported yet"
            )
        )


cl = ExplorationProductionLicence

get_bro_ids_of_bronhouder = partial(bro._get_bro_ids_of_bronhouder, cl)
get_bro_ids_of_bronhouder.__doc__ = bro._get_bro_ids_of_bronhouder.__doc__

get_data_for_bro_ids = partial(bro._get_data_for_bro_ids, cl)
get_data_for_bro_ids.__doc__ = bro._get_data_for_bro_ids.__doc__

get_characteristics = partial(bro._get_characteristics, cl)
get_characteristics.__doc__ = bro._get_characteristics.__doc__

get_data_in_extent = partial(bro._get_data_in_extent, cl)
get_data_in_extent.__doc__ = bro._get_data_in_extent.__doc__
