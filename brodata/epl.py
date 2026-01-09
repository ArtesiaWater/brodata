from . import bro


class ExplorationProductionLicence(bro.FileOrUrl):
    """Class to represent a ExplorationProductionLicence (EPL) from the BRO."""

    _rest_url = "https://publiek.broservices.nl/ep/epl/v1"

    def _read_contents(self, tree):
        raise (
            NotImplementedError(
                f"The reading of the contents of a {self.__class__.__name__} is not supported yet"
            )
        )
