import logging
import os
from zipfile import ZipFile, ZIP_DEFLATED, ZIP_STORED

import numpy as np

logger = logging.getLogger(__name__)


def read_zipfile(fname, pathnames=None, override_ext=None):
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
                ext = ".xml" if override_ext is None else override_ext
            elif pathname == "BRO_Grondwatergebruiksysteem":
                from .guf import GroundwaterUtilisationFacility

                cl = GroundwaterUtilisationFacility
                ext = ".xml" if override_ext is None else override_ext
            elif pathname == "BRO_Grondwatermonitoringnet":
                from .gmn import GroundwaterMonitoringNetwork

                cl = GroundwaterMonitoringNetwork
                ext = ".xml" if override_ext is None else override_ext
            elif pathname == "BRO_Grondwaterstandonderzoek":
                from .gld import GroundwaterLevelDossier

                cl = GroundwaterLevelDossier
                ext = ".xml" if override_ext is None else override_ext
            elif pathname == "BRO_GeotechnischSondeeronderzoek":
                from .cpt import ConePenetrationTest

                cl = ConePenetrationTest
                ext = ".xml" if override_ext is None else override_ext
            elif pathname == "BRO_GeotechnischBooronderzoek":
                from .bhr import GeotechnicalBoreholeResearch

                cl = GeotechnicalBoreholeResearch
                ext = ".xml" if override_ext is None else override_ext
            elif pathname == "DINO_GeologischBooronderzoekBoormonsterprofiel":
                from .dino import GeologischBooronderzoek

                cl = GeologischBooronderzoek
                ext = ".csv" if override_ext is None else override_ext
            elif pathname == "DINO_GeotechnischSondeeronderzoek":
                cl = None
                ext = ".tif" if override_ext is None else override_ext
            elif pathname == "DINO_GeologischBooronderzoekKorrelgrootteAnalyse":
                logger.warning(f"Folder {pathname} not supported yet")
                cl = None
                ext = None
            elif pathname == "DINO_GeologischBooronderzoekChemischeAnalyse":
                logger.warning(f"Folder {pathname} not supported yet")
                cl = None
                ext = None
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
                ext = None

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


def _save_data_to_zip(to_zip, files, remove_path_again, to_path):
    try:
        import zlib

        compression = ZIP_DEFLATED
    except:
        compression = ZIP_STORED
    with ZipFile(to_zip, "w", compression=compression) as zf:
        for file in files:
            zf.write(file, os.path.split(file)[1])
    if remove_path_again:
        # remove individual files again
        for file in files:
            os.remove(file)
        os.removedirs(to_path)


def _format_repr(self, props):
    # format these properties into a string
    props_str = ""
    for key in props:
        value = props[key]
        props_str = f"{props_str}{key}={value.__repr__()}, "
    if len(props_str) > 1:
        props_str = props_str[:-2]
    # generate name
    name = f"{self.__class__.__name__}({props_str})"
    return name
