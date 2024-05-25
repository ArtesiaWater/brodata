import brodata


def test_get_gdf():
    extent = [200000, 220000, 605000, 615000]
    brodata.webservices.get_gdf("Verticaal elektrisch sondeeronderzoek", extent=extent)
