import os

import brodata


def test_site_assessment_data():
    fname = os.path.join("tests", "data", "SAD000000011742.xml")
    brodata.sad.SiteAssessmentData(fname)
