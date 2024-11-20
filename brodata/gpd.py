import logging
from . import bro

logger = logging.getLogger(__name__)


class GroundwaterProductionDossier(bro.FileOrUrl):
    _rest_url = "https://publiek.broservices.nl/gu/gpd/v1"
