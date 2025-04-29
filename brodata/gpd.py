import logging
from functools import partial

from . import bro

logger = logging.getLogger(__name__)


class GroundwaterProductionDossier(bro.FileOrUrl):
    _rest_url = "https://publiek.broservices.nl/gu/gpd/v1"


cl = GroundwaterProductionDossier

get_bro_ids_of_bronhouder = partial(bro._get_bro_ids_of_bronhouder, cl)
get_bro_ids_of_bronhouder.__doc__ = bro._get_bro_ids_of_bronhouder.__doc__
