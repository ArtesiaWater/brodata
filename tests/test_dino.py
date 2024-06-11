import os
import brodata


def test_verticaal_elektrisch_sondeeronderzoek_from_url():
    brodata.dino.VerticaalElektrischSondeeronderzoek.from_dino_nr("W38B0016")


def test_verticaal_elektrisch_sondeeronderzoek_from_file():
    fname = os.path.join("data", "W38B0016.csv")
    brodata.dino.VerticaalElektrischSondeeronderzoek(fname)


def test_grondwaterstand():
    brodata.dino.Grondwaterstand.from_dino_nr("B38B0207", 1)


def test_grondwaterstand_from_file():
    fname = os.path.join("data", "B38B0207_001_full.csv")
    brodata.dino.Grondwaterstand(fname)


def test_grondwatersamenstelling_from_file():
    fname = os.path.join("data", "B38B0079_qua.csv")
    qua = brodata.dino.Grondwatersamenstelling(fname)


def test_geologisch_booronderzoek():
    brodata.dino.GeologischBooronderzoek.from_dino_nr("B42E0199")


def test_geologisch_booronderzoek_from_file():
    fname = os.path.join("data", "B38B2152.csv")
    brodata.dino.GeologischBooronderzoek(fname)


def test_get_verticaal_elektrisch_sondeeronderzoek_within_extent():
    extent = [116000, 120000, 439400, 442000]
    brodata.dino.get_verticaal_elektrisch_sondeeronderzoek(extent)


def test_grondwaterstanden_within_extent():
    extent = [117700, 118700, 439400, 440400]
    brodata.dino.get_grondwaterstand(extent)


def test_grondwatersamenstelling_within_extent():
    extent = [117700, 118700, 439400, 440400]
    gdf = brodata.dino.get_grondwatersamenstelling(extent)
