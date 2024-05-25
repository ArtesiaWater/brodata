import os
import brodata

config = brodata.webservices.get_configuration()


def test_verticaal_elektrisch_sondeeronderzoek_from_url():
    url = config["Verticaal elektrisch sondeeronderzoek"]["download"]
    url = f"{url}/W38B0016"
    brodata.dino.VerticaalElektrischSondeeronderzoek(url)


def test_verticaal_elektrisch_sondeeronderzoek_from_file():
    fname = os.path.join("data", "W38B0016.csv")
    brodata.dino.VerticaalElektrischSondeeronderzoek(fname)


def test_grondwaterstand():
    url = config["Grondwaterstand"]["download"]
    url = f"{url}/B38B0207/001"
    brodata.dino.Grondwaterstand(url)


def test_grondwaterstand_from_file():
    fname = os.path.join("data", "B38B0207_001_full.csv")
    brodata.dino.Grondwaterstand(fname)


def test_geologisch_booronderzoek():
    url = config["Geologisch booronderzoek"]["download"]
    url = f"{url}/B42E0199"
    brodata.dino.GeologischBooronderzoek(url)


def test_geologisch_booronderzoek_from_file():
    fname = os.path.join("data", "B38B2152.csv")
    brodata.dino.GeologischBooronderzoek(fname)


def test_ves_within_extent():
    extent = [200000, 220000, 605000, 615000]
    brodata.dino.get_ves_within_extent(extent)


def test_grondwaterstanden_within_extent():
    extent = [200000, 220000, 605000, 615000]
    brodata.dino.get_grondwaterstanden(extent)
