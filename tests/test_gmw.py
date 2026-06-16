import os
import tempfile

import pandas as pd
import pytest

import brodata


def test_get_gmw_of_bronhouder():
    brodata.gmw.get_bro_ids_of_bronhouder(30277172)


def test_get_gmw_characteristics():
    extent = [117700, 118700, 439400, 440400]
    brodata.gmw.get_characteristics(extent=extent)


def test_gmw_get_gld_data_in_extent():
    extent = [118200, 118400, 439700, 440000]
    tempdir = tempfile.gettempdir()
    fname_zip = os.path.join(tempdir, "test_gmw_get_gld_data_in_extent.zip")
    gdf1 = brodata.gmw.get_data_in_extent(
        extent=extent,
        combine=True,
        as_csv=False,
        to_zip=fname_zip,
        redownload=True,
    )
    gdf2 = brodata.gmw.get_data_in_extent(
        extent=extent,
        combine=True,
        as_csv=False,
        to_zip=fname_zip,
        redownload=False,
    )
    gdf3 = brodata.gmw.get_data_in_extent(fname_zip, combine=True)

    pd.testing.assert_frame_equal(gdf1, gdf2)
    pd.testing.assert_frame_equal(gdf1, gdf3)


def test_gmw_get_gld_data_in_extent_as_csv():
    to_path = os.path.join(
        tempfile.gettempdir(), "test_gmw_get_gld_data_in_extent_as_csv"
    )
    extent = [118200, 118400, 439700, 440000]
    gdf1 = brodata.gmw.get_data_in_extent(
        extent=extent, combine=True, as_csv=True, to_path=to_path, redownload=True
    )

    gdf2 = brodata.gmw.get_data_in_extent(
        extent=extent, combine=True, as_csv=True, to_path=to_path, redownload=False
    )

    pd.testing.assert_frame_equal(gdf1, gdf2)


def test_gmw_get_gar_data_in_extent():
    extent = [115000, 120000, 438000, 441000]
    brodata.gmw.get_data_in_extent(extent=extent, kind="gar", combine=True)


# def test_gmw_get_frd_data_in_extent():
#    extent = [115000, 120000, 438000, 441000]
#    gdf, frd = brodata.gmw.get_data_in_extent(extent=extent, kind="frd")


def test_get_well_code():
    brodata.gmw.get_well_code("GMW000000049567")


def test_get_gmw():
    brodata.gmw.GroundwaterMonitoringWell.from_bro_id("GMW000000049567")


def test_groundwater_monitoring_well():
    fname = os.path.join("tests", "data", "GMW000000036287.xml")
    brodata.gmw.GroundwaterMonitoringWell(fname)


def test_unknwon_gmw_raises_value_error():
    with pytest.raises(ValueError):
        brodata.gmw.GroundwaterMonitoringWell.from_bro_id("GMW000000000000")
