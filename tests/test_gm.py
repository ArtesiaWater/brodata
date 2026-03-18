import os
import tempfile

import brodata
from shapely.geometry import Polygon


def test_gm_gar():
    extent = [112_000, 119_000, 430_000, 445_000]
    tempdir = tempfile.gettempdir()
    fname_zip = os.path.join(tempdir, "test_gm_gar.zip")
    gdf1 = brodata.gm.get_data_in_extent(
        extent, kind="gar", tmin="2000", tmax="2010", to_zip=fname_zip
    )
    gdf2 = brodata.gm.get_data_in_extent(
        extent, kind="gar", tmin="2008", tmax="2010", to_zip=fname_zip
    )
    assert len(gdf2) < len(gdf1)


def test_gm_gar2():
    extent = [112_000, 119_000, 430_000, 445_000]
    tubes = brodata.gm.gmw_monitoringtube_items(extent)
    gars_gdf = brodata.gm.gar_items(extent)
    # add information from tubes to gar-data
    gars_gdf = gars_gdf.join(
        tubes.set_index("gm_gmw_monitoringtube_pk").drop(columns="geometry"),
        on="gm_gmw_monitoringtube_fk",
    )
    assert "screen_top_position" in gars_gdf.columns


def test_gm_gld():
    extent = [112_000, 119_000, 430_000, 445_000]
    tempdir = tempfile.gettempdir()
    fname_zip = os.path.join(tempdir, "test_gm_gld.zip")
    brodata.gm.get_data_in_extent(
        extent, kind="gld", tmin="2000", tmax="2010", to_zip=fname_zip
    )


def test_gm_gld_polygon():
    polygon = Polygon(
        [
            (112_000, 430_000),
            (119_000, 430_000),
            (119_000, 445_000),
            (112_000, 445_000),
            (112_000, 430_000),
        ]
    )
    gdf = brodata.gm.gld_items(polygon)
    assert gdf is not None
    assert len(gdf) > 0
