import os

import brodata


def test_exploration_production_construction():
    fname = os.path.join("tests", "data", "EPC000000000140.xml")
    brodata.epc.ExplorationProductionConstruction(fname)
