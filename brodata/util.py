import os
import logging
import numpy as np
import pandas as pd
from zipfile import ZipFile

logger = logging.getLogger(__name__)


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
    import geopandas as gpd

    # convert a list of dino-objects to a geodataframe
    df = pd.DataFrame([objects[key].to_dict() for key in objects])
    if df.empty:
        logger.warning("no data found")
        geometry = None
    else:
        if x not in df:
            logger.warning(f"{x} not found in data")
            geometry = None
        elif y not in df:
            logger.warning(f"{y} not found in data")
            geometry = None
        else:
            geometry = gpd.points_from_xy(df[x], df[y])
    gdf = gpd.GeoDataFrame(df, geometry=geometry)
    return gdf


def read_zipfile(fname, pathnames=None):
    with ZipFile(fname) as zf:
        namelist = np.array(zf.namelist())
        extensions = np.array([os.path.splitext(x)[1] for x in namelist])
        dirnames = np.array([os.path.dirname(x) for x in namelist])
        if pathnames is None:
            pathnames = np.unique(dirnames)
            pathnames = pathnames[pathnames != ""]
        elif isinstance(pathnames, str):
            pathnames = [pathnames]

        data = {}
        for pathname in pathnames:
            data[pathname] = {}
            logger.info(f"Reading {pathname} from {fname}")
            if pathname == "BRO_Grondwatermonitoringput":
                from .gmw import GroundwaterMonitoringWell

                cl = GroundwaterMonitoringWell
                ext = ".xml"
            elif pathname == "BRO_Grondwatergebruiksysteem":
                from .guf import GroundwaterUtilisationFacility

                cl = GroundwaterUtilisationFacility
                ext = ".xml"
            elif pathname == "BRO_Grondwatermonitoringnet":
                from .gmn import GroundwaterMonitoringNetwork

                cl = GroundwaterMonitoringNetwork
                ext = ".xml"
            elif pathname == "BRO_Grondwaterstandonderzoek":
                from .gld import GroundwaterLevelDossier

                cl = GroundwaterLevelDossier
                ext = ".xml"
            elif pathname == "BRO_GeotechnischSondeeronderzoek":
                from .cpt import ConePenetrationTest

                cl = ConePenetrationTest
                ext = ".xml"
            elif pathname == "BRO_GeotechnischBooronderzoek":
                from .bhr import GeotechnicalBoreholeResearch

                cl = GeotechnicalBoreholeResearch
                ext = ".xml"
            elif pathname == "DINO_GeologischBooronderzoekBoormonsterprofiel":
                from .dino import GeologischBooronderzoek

                cl = GeologischBooronderzoek
                ext = ".csv"
            elif pathname == "DINO_GeotechnischSondeeronderzoek":
                cl = None
                ext = ".tif"
            elif pathname == "DINO_GeologischBooronderzoekKorrelgrootteAnalyse":
                logger.warning(f"Folder {pathname} not supported yet")
                cl = None
            elif pathname == "DINO_GeologischBooronderzoekChemischeAnalyse":
                logger.warning(f"Folder {pathname} not supported yet")
                cl = None
            elif pathname == "DINO_Grondwatersamenstelling":
                from .dino import Grondwatersamenstelling

                cl = Grondwatersamenstelling
                ext = ".csv"
            elif pathname == "DINO_Grondwaterstanden":
                from .dino import Grondwaterstand

                cl = Grondwaterstand
                ext = ".csv"
            elif pathname == "DINO_VerticaalElektrischSondeeronderzoek":
                from .dino import VerticaalElektrischSondeeronderzoek

                cl = VerticaalElektrischSondeeronderzoek
                ext = ".csv"
            else:
                logger.warning(f"Folder {pathname} not supported yet")
                cl = None

            if cl is not None or ext == ".tif":
                mask = (dirnames == pathname) & (extensions == ext)
                if not mask.any():
                    logger.warning(f"No {ext} files found in {pathname}.")
                for file in namelist[mask]:
                    name = os.path.splitext(os.path.basename(file))[0]
                    if ext == ".tif":
                        from PIL import Image

                        data[pathname][name] = Image.open(zf.open(file))
                    else:
                        data[pathname][name] = cl(file, zipfile=zf)
        return data
