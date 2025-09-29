import logging

from . import bro

logger = logging.getLogger(__name__)


class FormationResistanceDossier(bro.FileOrUrl):
    """Class to represent a Formation Resistance Dossier (FRD) from the BRO."""

    def _read_contents(self, tree):
        raise (NotImplementedError("FormationResistanceDossier not available yet"))
