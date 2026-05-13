import os
import tempfile

import brodata


def test_groundwater_utilisation_facility():
    fname = os.path.join("tests", "data", "GUF000000016723.xml")
    brodata.guf.GroundwaterUtilisationFacility(fname)


def test_get_guf_data_in_extent():
    extent = [117700, 118700, 439400, 440400]
    brodata.guf.get_characteristics(extent=extent)

    tempdir = tempfile.gettempdir()
    fname_zip = os.path.join(tempdir, "test_get_guf_data_in_extent.zip")
    gdf1 = brodata.guf.get_data_in_extent(extent=extent, to_zip=fname_zip)

    extent = [118300, 118700, 439400, 440400]
    gdf2 = brodata.guf.get_data_in_extent(extent=extent, to_zip=fname_zip)
    assert len(gdf2) < len(gdf1)
