import os

import brodata


def test_get_cpt_characteristics():
    extent = [117700, 118700, 439400, 440400]
    brodata.cpt.get_characteristics(extent=extent)


def test_get_cpt_graph_types():
    brodata.cpt.get_graph_types()


def test_get_cpt():
    fname = os.path.join("tests", "data", "CPT000000005925.xml")
    cpt = brodata.cpt.ConePenetrationTest(fname)
    brodata.plot.cone_penetration_test(cpt)
    brodata.cpt.graph(fname)


def test_get_cpt_test_with_dissipation_test():
    fname = os.path.join("tests", "data", "CPT000000115243.xml")
    brodata.cpt.ConePenetrationTest(fname)
