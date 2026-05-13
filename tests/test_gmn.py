import os

import brodata


def test_groundwater_monitoring_network():
    fname = os.path.join("tests", "data", "GMN000000000163.xml")
    brodata.gmn.GroundwaterMonitoringNetwork(fname)
