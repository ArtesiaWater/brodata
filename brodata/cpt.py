from . import bro


def get_characteristics(**kwargs):
    """
    Get characteristics of Cone Penetration Tests (see bro.get_characteristics)
    """
    return bro.get_characteristics("cpt", **kwargs)
