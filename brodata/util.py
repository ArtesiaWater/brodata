import pandas as pd
import geopandas as gpd


def objects_to_gdf(objects, x="X-coordinaat", y="Y-coordinaat"):
    """

    Parameters
    ----------
    dino_collection: list of dinoloket objects
        ondersteund deze dinoloket objecten: Grondwaterstanden, Peilschaal
        of Boormonsterprofile
    x: str
        name of column of x-coordinate
    y: str
        name of column of y-coordinate

    Returns
    -------
    gdf: GeoDataFrame
    """
    # convert a list of dino-objects to a geodataframe
    df = pd.DataFrame([objects[key].to_dict() for key in objects])
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df[x], df[y]))
    return gdf
