import os
import brodata


def test_verticaal_elektrisch_sondeeronderzoek_from_url():
    brodata.dino.VerticaalElektrischSondeeronderzoek.from_dino_nr("W38B0016")


def test_verticaal_elektrisch_sondeeronderzoek_from_file_no_models():
    fname = os.path.join("data", "W38B0016.csv")
    ves = brodata.dino.VerticaalElektrischSondeeronderzoek(fname)
    assert len(ves.interpretaties) == 0


def test_verticaal_elektrisch_sondeeronderzoek_from_file_one_model():
    fname = os.path.join("data", "W38B0022.csv")
    ves = brodata.dino.VerticaalElektrischSondeeronderzoek(fname)
    assert len(ves.interpretaties) == 1


def test_verticaal_elektrisch_sondeeronderzoek_from_file_multiple_models():
    fname = os.path.join("data", "W38D0010.csv")
    ves = brodata.dino.VerticaalElektrischSondeeronderzoek(fname)
    assert len(ves.interpretaties) == 2


def test_grondwaterstand():
    brodata.dino.Grondwaterstand.from_dino_nr("B38B0207", 1)


def test_grondwaterstand_from_file():
    fname = os.path.join("data", "B38B0207_001_full.csv")
    brodata.dino.Grondwaterstand(fname)


def test_oppervlaktewaterstand():
    brodata.dino.Oppervlaktewaterstand.from_dino_nr("P38G0010")


def test_oppervlaktewaterstand_from_file():
    fname = os.path.join("data", "P38G0010_full.csv")
    brodata.dino.Oppervlaktewaterstand(fname)


def test_grondwatersamenstelling_from_file():
    fname = os.path.join("data", "B38B0079_qua.csv")
    qua = brodata.dino.Grondwatersamenstelling(fname)


def test_geologisch_booronderzoek():
    brodata.dino.GeologischBooronderzoek.from_dino_nr("B42E0199")


def test_geologisch_booronderzoek_from_file():
    fname = os.path.join("data", "B38B2152.csv")
    gb = brodata.dino.GeologischBooronderzoek(fname)
    brodata.plot.dino_lithology(gb.lithologie_lagen)
    brodata.plot.dino_lithology(gb.lithologie_lagen, x=None)


def test_get_verticaal_elektrisch_sondeeronderzoek_within_extent():
    extent = [116000, 120000, 439400, 442000]
    brodata.dino.get_verticaal_elektrisch_sondeeronderzoek(extent)


def test_grondwaterstanden_within_extent():
    extent = [117700, 118700, 439400, 440400]
    brodata.dino.get_grondwaterstand(extent)


def test_grondwatersamenstelling_within_extent():
    extent = [117700, 118700, 439400, 440400]
    gdf = brodata.dino.get_grondwatersamenstelling(extent)


def test_get_geologisch_booronderzoek_within_extent():
    extent = [118000, 118400, 439560, 440100]
    gdf = brodata.dino.get_geologisch_booronderzoek(extent)

    # plot the lithology along a line from west to east
    y_mean = gdf.geometry.y.mean()
    line = [(gdf.geometry.x.min(), y_mean), (gdf.geometry.x.max(), y_mean)]
    ax = brodata.plot.lithology_along_line(gdf, line, "dino")


def test_get_oppervlaktewaterstanden_within_extent():
    extent = [116000, 121000, 434000, 442000]
    brodata.dino.get_oppervlaktewaterstand(extent)
