import os

import pandas as pd

import brodata


def test_geotechnical_borehole_research():
    fname = os.path.join("tests", "data", "BHR000000353924.xml")
    bhrgt = brodata.bhr.GeotechnicalBoreholeResearch(fname)
    brodata.plot.bro_lithology(bhrgt.descriptiveBoreholeLog[0]["layer"])
    brodata.bhr.bhrgt_graph(fname)


def test_geotechnical_borehole_research_with_investigated_interval():
    bhr = brodata.bhr.GeotechnicalBoreholeResearch.from_bro_id("BHR000000365423")
    assert isinstance(bhr.boredInterval, pd.DataFrame)
    assert isinstance(bhr.investigatedInterval, pd.DataFrame)
    brodata.plot.descriptive_borehole_log(bhr)


def test_pedological_borehole_research():
    fname = os.path.join("tests", "data", "BHR000000175723.xml")
    brodata.bhr.PedologicalBoreholeResearch(fname)


def test_pedological_borehole_research_with_borehole_sample_analysis():
    fname = os.path.join("tests", "data", "BHR000000343841.xml")
    bhr = brodata.bhr.PedologicalBoreholeResearch(fname)
    assert hasattr(bhr, "investigatedInterval")


def test_geological_borehole_research():
    brodata.bhr.GeologicalBoreholeResearch.from_bro_id("BHR000000429481")


def test_get_bhr_in_extent():
    extent = [119000, 120000, 440500, 441000]
    gdf = brodata.bhr.get_data_in_extent(extent=extent)
    line = [(extent[0], extent[2]), (extent[1], extent[3])]
    colors = brodata.plot.lithology_colors.copy()
    colors["kleiigeHumus"] = colors["klei"]
    brodata.plot.lithology_along_line(gdf, line, "bro", colors=colors)
