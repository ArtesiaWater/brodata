import logging
import os
from zipfile import ZipFile, ZIP_DEFLATED, ZIP_STORED

import numpy as np

logger = logging.getLogger(__name__)

try:
    from tqdm import tqdm
except ImportError:
    # fallback: generate a dummy method with the same interface
    def tqdm(iterable=None, **kwargs):
        return iterable if iterable is not None else []


def read_zipfile(fname, pathnames=None, override_ext=None):
    """
    Read and parse files from a ZIP archive downloaded from BROloket.

    Parameters
    ----------
    fname : str
        Path to the ZIP file to read.
    pathnames : list of str or str, optional
        List of folder names within the ZIP archive to process. If None, all unique
        non-root directories are processed.
    override_ext : str, optional
        File extension to override the default extension for the selected folder(s).

    Returns
    -------
    dict
        Nested dictionary where the first-level keys are folder names, and the
        second-level keys are file base names.
        The values are either parsed objects (from corresponding classes) or file
        objects (e.g., PIL.Image for .tif files).

    Notes
    -----
    - The function supports specific folder names and file types, mapping them to
    corresponding classes for parsing.
    - Unsupported folders are logged with a warning and skipped.
    - If no files with the expected extension are found in a folder, a warning is
    logged.
    - For .tif files, PIL.Image objects are returned.
    - For other supported types, the corresponding class is instantiated with the file
    and the ZipFile object.
    """
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


def _get_to_file(fname, zipfile, to_path, _files):
    to_file = None
    if zipfile is not None or to_path is not None:
        to_file = fname
        if zipfile is None:
            to_file = os.path.join(to_path, to_file)
            if _files is not None:
                _files.append(to_file)
    return to_file


def _save_data_to_zip(to_zip, files, remove_path_again, to_path):
    try:
        import zlib

        compression = ZIP_DEFLATED
    except ImportError:
        logger.warning("Could not import zlib, saving zipfile without compression")
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


def _get_tag(node):
    return node.tag.split("}", 1)[1]
