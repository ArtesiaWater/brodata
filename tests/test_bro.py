import os
import brodata


def test_get_gwm_of_bronhouder():
    brodata.gmw.get_bro_ids_of_bronhouder(30277172)


def test_get_gmw_characteristics():
    extent = [117700, 118700, 439400, 440400]
    gmws = brodata.gmw.get_characteristics(extent=extent)


def test_gmw_get_gld_data_in_extent():
    extent = [118200, 118400, 439700, 440000]
    data = brodata.gmw.get_data_in_extent(extent=extent, combine=True, as_csv=False)


def test_gmw_get_gld_data_in_extent_as_csv():
    extent = [118200, 118400, 439700, 440000]
    data = brodata.gmw.get_data_in_extent(extent=extent, combine=True, as_csv=True)


def test_gmw_get_gar_data_in_extent():
    extent = [115000, 120000, 438000, 441000]
    data = brodata.gmw.get_data_in_extent(extent=extent, kind="gar", combine=True)


def test_gmw_get_frd_data_in_extent():
    extent = [115000, 120000, 438000, 441000]
    gdf, frd = brodata.gmw.get_data_in_extent(extent=extent, kind="frd")


def tets_get_well_code():
    brodata.gmw.get_well_code("GMW000000049567")


def test_get_gmw():
    gmw = brodata.gmw.GroundwaterMonitoringWell.from_bro_id("GMW000000049567")


def test_groundwater_monitoring_well():
    fname = os.path.join("data", "GMW000000036287.xml")
    gmw = brodata.gmw.GroundwaterMonitoringWell(fname)


def test_groundwater_level_dossier():
    fname = os.path.join("data", "GLD000000012893.xml")
    gld = brodata.gld.GroundwaterLevelDossier(fname)


def test_observations_summary():
    brodata.gld.get_observations_summary("GLD000000012893")


def test_geotechnical_borehole_research():
    fname = os.path.join("data", "BHR000000353924.xml")
    bhrgt = brodata.bhr.GeotechnicalBoreholeResearch(fname)
    brodata.plot.bro_lithology(bhrgt.descriptiveBoreholeLog[0]["layer"])


def test_pedological_borehole_research():
    fname = os.path.join("data", "BHR000000175723.xml")
    bhr = brodata.bhr.PedologicalBoreholeResearch(fname)


def test_groundwater_analysis_report():
    fname = os.path.join("data", "GAR000000019636.xml")
    gar = brodata.gar.GroundwaterAnalysisReport(fname)


def test_soil_face_research():
    fname = os.path.join("data", "SFR000000000243.xml")
    sfr = brodata.sfr.SoilFaceResearch(fname)


def test_groundwater_monitoring_network():
    fname = os.path.join("data", "GMN000000000163.xml")
    gmn = brodata.gmn.GroundwaterMonitoringNetwork(fname)


def test_get_cpt_characteristics():
    extent = [117700, 118700, 439400, 440400]
    cpts = brodata.cpt.get_characteristics(extent=extent)


def test_get_cone_penetration_test():
    fname = os.path.join("data", "CPT000000005925.xml")
    cpt = brodata.cpt.ConePenetrationTest(fname)
    # also test the plot
    brodata.plot.cone_penetration_test(cpt)


def test_get_cone_penetration_test_with_dissipation_test():
    fname = os.path.join("data", "CPT000000115243.xml")
    cpt = brodata.cpt.ConePenetrationTest(fname)


def test_groundwater_utilisation_facility():
    fname = os.path.join("data", "GUF000000016723.xml")
    guf = brodata.guf.GroundwaterUtilisationFacility(fname)
