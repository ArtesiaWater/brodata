import os

import brodata


def test_soil_face_research():
    fname = os.path.join("tests", "data", "SFR000000000243.xml")
    brodata.sfr.SoilFaceResearch(fname)


def test_soil_face_research_with_non_standardised_fraction():
    brodata.sfr.SoilFaceResearch.from_bro_id("SFR000000001861")
