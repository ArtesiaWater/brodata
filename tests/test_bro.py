import os
import brodata


def test_get_gwm_of_bronhouder(bronhouder):
    brodata.gmw.get_bro_ids_of_bronhouder(30277172)


def test_get_gmw_characteristics():
    extent = [117700, 118700, 439400, 440400]
    brodata.gmw.get_characteristics(extent=extent)


def tets_get_well_code():
    brodata.gmw.get_well_code("GMW000000049567")


def test_get_gmw():
    brodata.gmw.get_gmw("GMW000000049567")


def test_groundwater_monitoring_well():
    fname = os.path.join("data", "GMW000000036287.xml")
    gmw = brodata.gmw.GroundwaterMonitoringWell(fname)


def test_geotechnisch_booronderzoek():
    fname = os.path.join("data", "BHR000000353924.xml")
    bhrgt = brodata.bhr.GeotechnischBooronderzoek(fname)


def test_bodemkundig_booronderzoek():
    fname = os.path.join("data", "BHR000000175723.xml")
    bhr = brodata.bhr.BodemkundigBooronderzoek(fname)


def test_get_cpt_characteristics():
    extent = [117700, 118700, 439400, 440400]
    brodata.cpt.get_characteristics(extent=extent)


def test_get_geotechnisch_sondeeronderzoek():
    fname = os.path.join("data", "CPT000000005925.xml")
    cpt = brodata.cpt.GeotechnischSondeeronderzoek(fname)
