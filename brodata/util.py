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


def read_zipfile(fname, pathnames=None, use_bro_abbreviation=False, override_ext=None):
    """
    Read and parse files from a ZIP archive downloaded from BROloket.

    Parameters
    ----------
    fname : str
        Path to the ZIP file to read.
    pathnames : list of str or str, optional
        List of folder names within the ZIP archive to process. If None, all unique
        non-root directories are processed.
    use_bro_abbreviation: bool, optional
        If True, use the abbreviation of bro-objects (e.g. GMW, GLD, BHR) to store the
        data in the root of the returned dictionary. If False, use the first level of
        the folder structure in the zip-file to store the returned objects (e.g.
        BRO_Grondwatermonitoring, BRO_GeologischBooronderzoek). The default is False.
    override_ext : str, optional
        Removed argument from `read_zipfile`

    Returns
    -------
    dict
        Nested dictionary where the first-level keys are data-categories, and the
        second-level keys are file base names (bro-id or nitg-nr).
        The values are either parsed objects (from corresponding classes) or file
        objects (e.g., PIL.Image for .tif files).

    Notes
    -----
    - For .tif files, PIL.Image objects are returned.
    - For other supported types, the corresponding class is instantiated with the file
    and the ZipFile object.
    """
    if override_ext is not None:
        raise (Exception("The parameter `override_ext` is removed from `read_zipfile`"))

    data = {}
    with ZipFile(fname) as zf:
        namelist = np.array(zf.namelist())
        for file in namelist:
            name, ext = os.path.splitext(os.path.basename(file))
            if name == "":
                # this is a directory
                continue
            pathname = os.path.dirname(file)
            if pathname == "":
                # skip file in the root path (usually the file 'locatie_levering.kml')
                continue
            if pathnames is not None:
                if pathname not in pathnames:
                    continue
            if pathname.startswith("BRO"):
                if ext != ".xml":
                    logger.info(f"Skipping file: {file}")
                    continue
                if use_bro_abbreviation:
                    key = name[:3]
                else:
                    key = os.path.normpath(pathname).split(os.sep)[0]
                if name.startswith("BHR"):
                    if pathname == "BRO_GeotechnischBooronderzoek":
                        from .bhr import GeotechnicalBoreholeResearch as cl
                    elif pathname == "BRO_GeologischBooronderzoek":
                        from .bhr import GeologicalBoreholeResearch as cl
                    elif pathname == "BodemkundigBooronderzoek":
                        from .bhr import PedologicalBoreholeResearch as cl
                    else:
                        logger.warning(f"Unknown BHR-type: {pathname}")
                elif name.startswith("CPT"):
                    from .cpt import ConePenetrationTest as cl
                elif name.startswith("EPC"):
                    from .epc import ExplorationProductionConstruction as cl
                elif name.startswith("FRD"):
                    from .frd import FormationResistanceDossier as cl
                elif name.startswith("GAR"):
                    from .gar import GroundwaterAnalysisReport as cl
                elif name.startswith("GLD"):
                    from .gld import GroundwaterLevelDossier as cl
                elif name.startswith("GMN"):
                    from .gmn import GroundwaterMonitoringNetwork as cl
                elif name.startswith("GMW"):
                    from .gmw import GroundwaterMonitoringWell as cl
                elif name.startswith("GPD"):
                    from .gpd import GroundwaterProductionDossier as cl
                elif name.startswith("GUF"):
                    from .guf import GroundwaterUtilisationFacility as cl
                elif name.startswith("SAD"):
                    from .sad import SiteAssessmentData as cl
                elif name.startswith("SFR"):
                    from .sfr import SoilFaceResearch as cl
                else:
                    logger.warning("Unknown file-type: {file}")
                    continue

            elif pathname.startswith("DINO"):
                key = pathname
                if pathname == "DINO_GeologischBooronderzoekBoormonsterprofiel":
                    from .dino import GeologischBooronderzoek as cl

                    if ext != ".csv":
                        logger.info(f"Skipping file: {file}")
                        continue
                elif pathname == "DINO_GeotechnischSondeeronderzoek":
                    cl = None
                    if ext != ".tif":
                        logger.info(f"Skipping file: {file}")
                        continue
                elif pathname == "DINO_GeologischBooronderzoekKorrelgrootteAnalyse":
                    logger.warning(f"Folder {pathname} not supported yet")
                    continue
                elif pathname == "DINO_GeologischBooronderzoekChemischeAnalyse":
                    logger.warning(f"Folder {pathname} not supported yet")
                    continue
                elif pathname == "DINO_Grondwatersamenstelling":
                    from .dino import Grondwatersamenstelling as cl

                    if ext != ".csv":
                        logger.info(f"Skipping file: {file}")
                        continue
                elif pathname == "DINO_Grondwaterstanden":
                    from .dino import Grondwaterstand as cl

                    if ext != ".csv":
                        logger.info(f"Skipping file: {file}")
                        continue
                elif pathname in [
                    "DINO_VerticaalElektrischSondeeronderzoek",
                    "DINO_GeoElectrischOnderzoek",
                ]:
                    from .dino import VerticaalElektrischSondeeronderzoek as cl

                    if ext != ".csv":
                        logger.info(f"Skipping file: {file}")
                        continue
                else:
                    logger.warning(f"Folder {pathname} not supported yet")
                    continue

            if key not in data:
                data[key] = {}
            logger.info(f"Reading {file} from {fname}")
            if ext == ".tif":
                from PIL import Image

                data[key][name] = Image.open(zf.open(file))
            else:
                data[key][name] = cl(file, zipfile=zf)
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
