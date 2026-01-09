from . import bro


class SoilLegalDecision(bro.FileOrUrl):
    """Class to represent a SoilLegalDecision (EPL) from the BRO."""

    _rest_url = "https://publiek.broservices.nl/sq/sld/v1"

    def _read_contents(self, tree):
        raise (
            NotImplementedError(
                f"The reading of the contents of a {self.__class__.__name__} is not supported yet"
            )
        )
