import brodata
from shapely.geometry import Polygon


def test_get_gdf():
    extent = [200000, 220000, 605000, 615000]
    brodata.webservices.get_gdf("Verticaal elektrisch sondeeronderzoek", extent=extent)


def test_get_gdf_polygon():
    polygon = Polygon(
        [
            (200000, 605000),
            (220000, 605000),
            (220000, 615000),
            (200000, 615000),
            (200000, 605000),
        ]
    )
    brodata.webservices.get_gdf(
        "Verticaal elektrisch sondeeronderzoek",
        extent=polygon,
    )
