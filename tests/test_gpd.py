import os

import brodata


def test_groundwater_production_dossier():
    fname = os.path.join("tests", "data", "GPD000000017250.xml")
    brodata.gpd.GroundwaterProductionDossier(fname)
