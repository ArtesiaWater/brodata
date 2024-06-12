import logging
from . import bro

logger = logging.getLogger(__name__)


class FormationResistanceDossier(bro.XmlFileOrUrl):
    def _read_contents(self, tree):
        raise (NotImplementedError("FormationResistanceDossier not available yet"))
