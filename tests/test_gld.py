import os

import pandas as pd

import brodata


def test_groundwater_level_dossier():
    fname = os.path.join("tests", "data", "GLD000000012893.xml")
    brodata.gld.GroundwaterLevelDossier(fname)


def test_gld_observations_summary():
    brodata.gld.get_observations_summary("GLD000000012893")


def test_gld_get_objects_as_csv():
    csv_obs = brodata.gld.get_objects_as_csv("GLD000000012893", drop_duplicates=False)

    gld = brodata.gld.GroundwaterLevelDossier.from_bro_id(
        "GLD000000012893", drop_duplicates=False
    )

    xml_obs = gld.observation

    # the order of the measurements in csv and xml could be different,
    # therefore we downloaded with `drop_duplicates=False`
    # we now sort for the values as well
    columns = ["value", "qualifier", "status", "observation_type"]
    csv_obs = csv_obs.sort_values(by=columns)
    xml_obs = xml_obs.sort_values(by=columns)

    # make sure the shape of csv_obs and gld.observation are the same
    assert csv_obs.shape == xml_obs.shape
    assert (csv_obs.index == xml_obs.index).all()
    assert (csv_obs == xml_obs).all(axis=None)


def test_gld_get_series_as_csv():
    brodata.gld.get_series_as_csv("GLD000000012893")


def test_gld_sort_measurements():
    time = "2020-7-1"
    df = pd.DataFrame(
        [
            {
                "time": time,
                "value": 1.0,
                "qualifier": "goedgekeurd",
                "status": "voorlopig",
                "observation_type": "reguliereMeting",
            },
            {
                "time": time,
                "value": 2.0,
                "qualifier": "goedgekeurd",
                "status": "volledigBeoordeeld",
                "observation_type": "controleMeting",
            },
            {
                "time": time,
                "value": 3.0,
                "qualifier": "goedgekeurd",
                "status": "volledigBeoordeeld",
                "observation_type": "reguliereMeting",
            },
            {
                "time": time,
                "value": 1.0,
                "qualifier": "goedgekeurd",
                "status": "voorlopig",
                "observation_type": "reguliereMeting",
            },
        ]
    ).set_index("time")

    df_proc = brodata.gld.process_observations(df, "test")
    assert len(df_proc.index) == 1
    assert df_proc.iloc[0]["status"] == "volledigBeoordeeld"
    assert df_proc.iloc[0]["observation_type"] == "reguliereMeting"
