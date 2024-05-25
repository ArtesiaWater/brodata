import logging
from ..webservices import get_gdf

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
