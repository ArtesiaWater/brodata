import logging
import os
from io import BytesIO, StringIO, TextIOWrapper
from pathlib import Path
from zipfile import ZipFile

import numpy as np
import pandas as pd
import geopandas as gpd
import requests
from shapely.geometry import LineString
import matplotlib.pyplot as plt
from tqdm import tqdm

from .util import (
    _format_repr,
    _save_data_to_zip,
)
from .webservices import get_configuration, get_gdf

logger = logging.getLogger(__name__)


def objects_to_gdf(
    objects,
    x="X-coordinaat",
    y="Y-coordinaat",
    geometry=None,
    index=None,
    to_gdf=True,
):
    """
    Convert a dictionary of dino-objects to a geopandas GeoDataFrame.

    Parameters
    ----------
    objects: dictionary of bro or dinoloket objects
        dictionary of objects to convert to (geo)dataframe
    geometry: str
        name of column of geometry
    x: str
        name of column of x-coordinate
    y: str
        name of column of y-coordinate
    index: str or list of str
        name of column to use as index
    to_gdf: bool
        convert to geodataframe

    Returns
    -------
    gdf: GeoDataFrame or DataFrame
        Returns a GeoDataFrame if to_gdf is True, otherwise a DataFrame
    """

    if not to_gdf:
        return objects

    # convert a list of dino-objects to a geodataframe
    df = pd.DataFrame([objects[key].to_dict() for key in objects])
    if geometry is not None:
        if geometry in df.columns:
            geometry = df[geometry]
        else:
            geometry = None
    else:
        if df.empty:
            logger.warning("no data found")
        else:
            if x not in df:
                logger.warning(f"{x} not found in data. No geometry column created.")
            elif y not in df:
                logger.warning(f"{y} not found in data. No geometry column created.")
            else:
                geometry = gpd.points_from_xy(df[x], df[y])
    gdf = gpd.GeoDataFrame(df, geometry=geometry)
    if index is not None and not gdf.empty:
        if isinstance(index, str):
            if index in gdf.columns:
                gdf = gdf.set_index(index)
        elif np.all([x in gdf.columns for x in index]):
            # we assume index is an iterable (list), to form a MultiIndex
            gdf = gdf.set_index(index)
    return gdf


def _get_data_within_extent(
    dino_cl,
    kind,
    extent,
    config=None,
    timeout=5,
    silent=False,
    to_path=None,
    to_zip=None,
    redownload=False,
    x="X-coordinaat",
    y="Y-coordinaat",
    geometry=None,
    index="NITG-nr",
    to_gdf=True,
    max_retries=2,
):
    if isinstance(extent, (str, Path)):
        data = _get_data_from_path(extent, dino_cl, silent=silent)
        return objects_to_gdf(data, x, y, geometry, index, to_gdf)

    if to_zip is not None:
        if not redownload and os.path.isfile(to_zip):
            data = _get_data_from_zip(to_zip, dino_cl, silent=silent, extent=extent)
            return objects_to_gdf(data, x, y, geometry, index, to_gdf)
        if to_path is None:
            to_path = os.path.splitext(to_zip)[0]
        remove_path_again = not os.path.isdir(to_path)
        files = []

    if config is None:
        config = get_configuration()

    if to_path is not None and not os.path.isdir(to_path):
        os.makedirs(to_path)

    to_file = None
    gdf = None
    if to_path is not None:
        to_file = os.path.join(to_path, f"{dino_cl.__name__}.geojson")
        if to_zip is not None:
            files.append(to_file)
        if not redownload and os.path.isfile(to_file):
            gdf = gpd.read_file(to_file)
            if not gdf.empty and "DINO_NR" in gdf.columns:
                gdf = gdf.set_index("DINO_NR")
    if gdf is None:
        gdf = get_gdf(
            kind,
            config=config,
            extent=extent,
            timeout=timeout,
        )
        if to_file is not None:
            gdf.to_file(to_file)

    to_file = None

    data = {}
    for dino_nr in tqdm(gdf.index, disable=silent):
        if to_path is not None:
            to_file = os.path.join(to_path, f"{dino_nr}.csv")
            if to_zip is not None:
                files.append(to_file)
            if not redownload and os.path.isfile(to_file):
                data[dino_nr] = dino_cl(to_file)
                continue
        data[dino_nr] = dino_cl.from_dino_nr(
            dino_nr, timeout=timeout, to_file=to_file, max_retries=max_retries
        )
    if to_zip is not None:
        _save_data_to_zip(to_zip, files, remove_path_again, to_path)

    return objects_to_gdf(data, x, y, geometry, index, to_gdf)


def _get_data_from_path(from_path, dino_class, silent=False, ext=".csv"):
    if str(from_path).endswith(".zip"):
        return _get_data_from_zip(from_path, dino_class, silent=silent)
    files = os.listdir(from_path)
    files = [file for file in files if file.endswith(ext)]
    data = {}
    for file in tqdm(files, disable=silent):
        fname = os.path.join(from_path, file)
        data[os.path.splitext(file)[0]] = dino_class(fname)
    return data


def _get_data_from_zip(to_zip, dino_class, silent=False, extent=None):
    # read data from zipfile
    data = {}
    with ZipFile(to_zip) as zf:
        names = zf.namelist()
        name = f"{dino_class.__name__}.geojson"
        has_location_file = name in names
        if has_location_file:
            names.remove(name)
        if has_location_file and extent is not None:
            gdf = gpd.read_file(zf.open(name))
            gdf = gdf.set_index("DINO_NR")
            gdf = gdf.cx[extent[0] : extent[1], extent[2] : extent[3]]
            names = [f"{name}.csv" for name in gdf.index]
        for name in tqdm(names, disable=silent):
            data[name] = dino_class(name, zipfile=zf)
    return data


def get_verticaal_elektrisch_sondeeronderzoek(extent, **kwargs):
    dino_class = VerticaalElektrischSondeeronderzoek
    kind = "Verticaal elektrisch sondeeronderzoek"
    return _get_data_within_extent(
        dino_class, kind, extent, geometry="geometry", **kwargs
    )


def get_grondwaterstand(
    extent,
    config=None,
    timeout=5,
    silent=False,
    to_path=None,
    to_zip=None,
    redownload=False,
    to_gdf=True,
):
    dino_class = Grondwaterstand
    index = ["Locatie", "Filternummer"]

    if isinstance(extent, str):
        data = _get_data_from_path(extent, dino_class, silent=silent)
        return objects_to_gdf(data, index=index, to_gdf=to_gdf)

    if to_zip is not None:
        if not redownload and os.path.isfile(to_zip):
            data = _get_data_from_zip(to_zip, dino_class, silent=silent)
            return objects_to_gdf(data, index=index, to_gdf=to_gdf)
        if to_path is None:
            to_path = os.path.splitext(to_zip)[0]
        remove_path_again = not os.path.isdir(to_path)
        files = []

    kind = "Grondwaterstand"
    if config is None:
        config = get_configuration()
    gdf = get_gdf(
        kind,
        config=config,
        extent=extent,
        timeout=timeout,
    )
    download_url = config[kind]["download"]

    to_file = None
    if to_path is not None and not os.path.isdir(to_path):
        os.makedirs(to_path)
    data = {}
    for name in tqdm(gdf.index, disable=silent):
        for i_st in range(1, gdf.at[name, "ST_CNT"] + 1):
            piezometer_nr = f"{i_st:03d}"
            url = f"{download_url}/{name}/{piezometer_nr}"
            if to_path is not None:
                to_file = os.path.join(to_path, f"{name}_{piezometer_nr}.csv")
                if to_zip is not None:
                    files.append(to_file)
                if not redownload and os.path.isfile(to_file):
                    data[f"{name}_{piezometer_nr}"] = dino_class(to_file)
                    continue
            data[f"{name}_{piezometer_nr}"] = dino_class(
                url, timeout=timeout, to_file=to_file
            )
    if to_zip is not None:
        _save_data_to_zip(to_zip, files, remove_path_again, to_path)
    return objects_to_gdf(
        data, index=index, to_gdf=to_gdf, x="X-coordinaat", y="Y-coordinaat"
    )


def get_grondwatersamenstelling(extent, **kwargs):
    dino_class = Grondwatersamenstelling
    kind = "Grondwatersamenstelling"
    return _get_data_within_extent(dino_class, kind, extent, **kwargs)


def get_geologisch_booronderzoek(extent, **kwargs):
    logger.warning(
        "`get_geologisch_booronderzoek` is deprecated. Use `get_boormonsterprofiel` instead"
    )
    dino_class = GeologischBooronderzoek
    kind = "Geologisch booronderzoek"
    return _get_data_within_extent(dino_class, kind, extent, **kwargs)


def get_boormonsterprofiel(extent, **kwargs):
    dino_class = Boormonsterprofiel
    kind = "Boormonsterprofiel"
    return _get_data_within_extent(dino_class, kind, extent, **kwargs)


def get_boorgatmeting(extent, **kwargs):
    dino_class = Boorgatmeting
    kind = "Boorgatmeting"
    return _get_data_within_extent(dino_class, kind, extent, **kwargs)


def get_chemische_analyse(extent, **kwargs):
    dino_class = ChemischeAnalyse
    kind = "Chemische analyse"
    return _get_data_within_extent(dino_class, kind, extent, **kwargs)


def get_korrelgrootte_analyse(extent, **kwargs):
    dino_class = KorrelgrootteAnalyse
    kind = "Korrelgrootte analyse"
    return _get_data_within_extent(dino_class, kind, extent, **kwargs)


def get_oppervlaktewaterstand(extent, **kwargs):
    dino_class = Oppervlaktewaterstand
    kind = "Oppervlaktewateronderzoek"
    return _get_data_within_extent(dino_class, kind, extent, **kwargs)


class CsvFileOrUrl:
    def __init__(
        self,
        url_or_file,
        zipfile=None,
        timeout=5,
        to_file=None,
        redownload=True,
        max_retries=2,
    ):
        if zipfile is not None:
            with zipfile.open(url_or_file) as f:
                self._read_contents(TextIOWrapper(f))
        elif url_or_file.startswith("http"):
            if redownload or to_file is None or not os.path.isfile(to_file):
                if max_retries > 1:
                    adapter = requests.adapters.HTTPAdapter(max_retries=max_retries)
                    session = requests.Session()
                    session.mount("https://", adapter)
                    req = session.get(url_or_file, timeout=timeout)
                else:
                    req = requests.get(url_or_file, timeout=timeout)
                if not req.ok:
                    raise (Exception((f"Retieving data from {url_or_file} failed")))
                is_zipfile = False
                if "content-disposition" in req.headers:
                    if req.headers["content-disposition"].endswith(".zip"):
                        is_zipfile = True
                if is_zipfile:
                    # BoorgatMetingen are las files that are delivered in a zip-file
                    with ZipFile(BytesIO(req.content)) as myzip:
                        files = myzip.namelist()
                        assert len(files) == 1, "Only one file in the zipfile supported"
                        with myzip.open(files[0]) as myfile:
                            if to_file is not None:
                                with open(to_file, "wb") as f:
                                    f.write(myfile.read())
                            self._read_contents(TextIOWrapper(myfile))
                else:
                    if to_file is not None:
                        with open(to_file, "w") as f:
                            f.write(req.text)
                    self._read_contents(StringIO(req.text))
            else:
                with open(to_file, "r") as f:
                    self._read_contents(f)
        else:
            with open(url_or_file, "r") as f:
                self._read_contents(f)

    def __repr__(self):
        # retrieve properties if they exist
        propdict = {"NITG-nr": "NITG-nr", "X-coordinaat": "x", "Y-coordinaat": "y"}
        props = {}
        for key in propdict:
            if hasattr(self, key):
                props[propdict[key]] = getattr(self, key)
        name = _format_repr(self, props)
        return name

    @classmethod
    def from_dino_nr(cls, dino_nr, **kwargs):
        if not hasattr(cls, "_download_url"):
            raise (NotImplementedError(f"No download-url defined for {cls.__name__}"))
        return cls(f"{cls._download_url}/{dino_nr}", **kwargs)

    def _read_properties_csv_rows(self, f, merge_columns=False, **kwargs):
        # this is the new format of properties from dinoloket
        df, line = self._read_csv_part(f, header=None, index_col=0, **kwargs)
        # remove empty columns
        df = df.loc[:, ~df.isna().all(0)]
        if merge_columns:
            for index in df.index:
                df.at[index, 1] = " ".join(df.loc[index, ~df.loc[index].isna()].values)
            df = df.loc[:, :1]
        else:
            assert df.shape[1] == 1
        d = df.squeeze().to_dict()
        return d, line

    def _read_properties_csv_columns(self, f, **kwargs):
        df, line = self._read_csv_part(f, **kwargs)
        assert df.shape[0] == 1
        d = df.squeeze().to_dict()
        return d, line

    def _read_csv_part(self, f, sep=",", header=0, index_col=False, **kwargs):
        strt = f.tell()
        if header is None:
            nrows = 0
        else:
            nrows = -1  # the header does not count
        line = f.readline()
        while line.replace(",", "") not in ["\n", ""]:
            nrows += 1
            line = f.readline()
        eind = f.tell()
        # go back to where we were before
        f.seek(strt)
        df = pd.read_csv(
            f, sep=sep, index_col=index_col, nrows=nrows, header=header, **kwargs
        )
        if header is not None:
            df = df.loc[:, ~df.columns.str.startswith("Unnamed: ")]
        f.seek(eind)

        if line != "":
            # read empty lines gat
            while line.replace(",", "") == "\n":
                new_start = f.tell()
                line = f.readline()
            f.seek(new_start)

        return df, line


class Oppervlaktewaterstand(CsvFileOrUrl):
    _download_url = "https://www.dinoloket.nl/uitgifteloket/api/wo/owo/full"

    def __repr__(self):
        # retrieve properties if they exist

        props = {}
        if hasattr(self, "meta") and not self.meta.empty:
            s = self.meta.iloc[-1]
            propdict = {"Locatie": "Locatie", "X-coordinaat": "x", "Y-coordinaat": "y"}
            for key in propdict:
                if key in s:
                    props[propdict[key]] = s[key]
        name = _format_repr(self, props)
        return name

    def _read_contents(self, f):
        self.props, line = self._read_properties_csv_rows(f, merge_columns=True)
        if line.startswith(
            '"Van deze put zijn geen standen opgenomen in de DINO-database"'
        ):
            return
        self.meta, line = self._read_csv_part(f)
        self.data, line = self._read_csv_part(f)
        for column in ["Peildatum"]:
            if column in self.data.columns:
                self.data[column] = pd.to_datetime(self.data[column], dayfirst=True)

    def to_dict(self):
        d = {**self.props}
        if hasattr(self, "meta"):
            d["meta"] = self.meta
            for column in d["meta"]:
                d[column] = d["meta"][column].iloc[-1]
        if hasattr(self, "data"):
            d["data"] = self.data
        return d


class Grondwaterstand(CsvFileOrUrl):
    _download_url = "https://www.dinoloket.nl/uitgifteloket/api/wo/gwo/full"

    @classmethod
    def from_dino_nr(cls, dino_nr, filter_nr, **kwargs):
        return cls(f"{cls._download_url}/{dino_nr}/{filter_nr:03d}", **kwargs)

    def __repr__(self):
        # retrieve properties if they exist

        props = {}
        if hasattr(self, "meta") and not self.meta.empty:
            s = self.meta.iloc[-1]
            propdict = {
                "Locatie": "Locatie",
                "Filternummer": "filter",
                "X-coordinaat": "x",
                "Y-coordinaat": "y",
            }
            for key in propdict:
                if key in s:
                    props[propdict[key]] = s[key]
        name = _format_repr(self, props)
        return name

    def _read_contents(self, f):
        self.props, line = self._read_properties_csv_rows(f, merge_columns=True)
        self.props2, line = self._read_properties_csv_rows(f)
        if line.startswith(
            '"Van deze put zijn geen standen opgenomen in de DINO-database"'
        ):
            return
        self.meta, line = self._read_csv_part(f)
        self.data, line = self._read_csv_part(f)
        for column in ["Peildatum"]:
            if column in self.data.columns:
                self.data[column] = pd.to_datetime(self.data[column], dayfirst=True)

    def to_dict(self):
        d = {**self.props, **self.props2}
        if hasattr(self, "meta"):
            d["meta"] = self.meta
            for column in d["meta"]:
                d[column] = d["meta"][column].iloc[-1]
        if hasattr(self, "data"):
            d["data"] = self.data
        return d


class Grondwatersamenstelling(CsvFileOrUrl):
    _download_url = "https://www.dinoloket.nl/uitgifteloket/api/wo/gwo/qua/report"

    def _read_contents(self, f):
        # read first line and place cursor at start of document again
        start = f.tell()
        line = f.readline().rstrip("\n")
        f.seek(start)

        # LOCATIE gegevens
        if line.startswith('"LOCATIE gegevens"'):
            line = f.readline()
            self.locatie_gegevens, line = self._read_properties_csv_columns(f)
            for key in self.locatie_gegevens:
                setattr(self, key, self.locatie_gegevens[key])

        # KWALITEIT gegevens VLOEIBAAR
        if line.startswith('"KWALITEIT gegevens VLOEIBAAR"'):
            line = f.readline()
            self.kwaliteit_gegevens_vloeibaar, line = self._read_csv_part(f)
            for column in ["Monster datum", "Analyse datum"]:
                if column in self.kwaliteit_gegevens_vloeibaar.columns:
                    self.kwaliteit_gegevens_vloeibaar[column] = pd.to_datetime(
                        self.kwaliteit_gegevens_vloeibaar[column], dayfirst=True
                    )

    def to_dict(self):
        d = {**self.locatie_gegevens}
        if hasattr(self, "kwaliteit_gegevens_vloeibaar"):
            d["kwaliteit_gegevens_vloeibaar"] = self.kwaliteit_gegevens_vloeibaar
        return d


class Boormonsterprofiel(CsvFileOrUrl):
    _download_url = (
        "https://www.dinoloket.nl/uitgifteloket/api/brh/sampledescription/csv"
    )

    def _read_contents(self, f):
        # read first line and place cursor at start of document again
        start = f.tell()
        line = f.readline().rstrip("\n")
        f.seek(start)
        if line.startswith('"ALGEMENE GEGEVENS BORING"'):
            line = f.readline()
            self.algemene_gegevens_boring, line = self._read_properties_csv_columns(f)
            for key in self.algemene_gegevens_boring:
                setattr(self, key, self.algemene_gegevens_boring[key])
        if line.startswith('"ALGEMENE GEGEVENS LITHOLOGIE"'):
            line = f.readline()
            self.algemene_gegevens_lithologie, line = self._read_properties_csv_columns(
                f
            )
        if line.startswith('"LITHOLOGIE LAGEN"'):
            line = f.readline()
            self.lithologie_lagen, line = self._read_csv_part(f)
        if line.startswith('"LITHOLOGIE SUBLAGEN"'):
            line = f.readline()
            self.lithologie_sublagen, line = self._read_csv_part(f)

    def to_dict(self):
        d = {**self.algemene_gegevens_boring}
        if hasattr(self, "algemene_gegevens_lithologie"):
            for key in self.algemene_gegevens_boring:
                if key in self.algemene_gegevens_lithologie:
                    # 'Datum boring' can be specified in algemene_gegevens_boring and algemene_gegevens_lithologie
                    if pd.isna(self.algemene_gegevens_lithologie[key]):
                        self.algemene_gegevens_lithologie.pop(key)
            d = {**d, **self.algemene_gegevens_lithologie}
        if hasattr(self, "lithologie_lagen"):
            d["lithologie_lagen"] = self.lithologie_lagen
        if hasattr(self, "lithologie_sublagen"):
            d["lithologie_sublagen"] = self.lithologie_sublagen
        return d


class GeologischBooronderzoek(Boormonsterprofiel):
    # In brodata, Boormonsterprofiel used to be called GeologischBooronderzoek.
    # Therefore, this is a copy of GeologischBooronderzoek, for backwards compatibility
    pass


class Boorgatmeting(CsvFileOrUrl):
    _download_url = "https://www.dinoloket.nl/uitgifteloket/api/brh/log/las"

    def __repr__(self):
        # retrieve properties if they exist

        props = {}
        if hasattr(self, "las") and "Well" in self.las.header:
            items = self.las.header["Well"]
            for item in items:
                props[item.descr] = item.value
        name = _format_repr(self, props)
        return name

    def _read_contents(self, f):
        import lasio

        self.las = lasio.read(f)

    def to_dict(self):
        import lasio

        return lasio.las.JSONEncoder().default(self.las)

    def plot(self, ax=None, columns=None, z=0.0, **kwargs):
        if ax is None:
            import matplotlib.pyplot as plt

            ax = plt.gca()
        df = self.las.df()
        if columns is None:
            columns = df.columns
        elif isinstance(columns, str):
            columns = [columns]

        for column in df.columns:
            # df.reset_index().plot(y="DEPTH", x=column)
            ax.plot(df[column], z - df.index, label=column, **kwargs)
        return ax


class ChemischeAnalyse(CsvFileOrUrl):
    _download_url = (
        "https://www.dinoloket.nl/uitgifteloket/api/brh/chemicalanalysis/csv"
    )

    def _read_contents(self, f):
        # read first line and place cursor at start of document again
        start = f.tell()
        line = f.readline().rstrip("\n")
        f.seek(start)

        # LOCATIE gegevens
        if line.startswith('"LOCATIE gegevens"'):
            line = f.readline()
            self.locatie_gegevens, line = self._read_properties_csv_columns(f)
            for key in self.locatie_gegevens:
                setattr(self, key, self.locatie_gegevens[key])

        # KWALITEIT gegevens VLOEIBAAR
        if line.startswith('"KWALITEIT gegevens VAST"'):
            line = f.readline()
            self.kwaliteit_gegevens_vast, line = self._read_csv_part(f)
            for column in ["Monster datum", "Analyse datum"]:
                if column in self.kwaliteit_gegevens_vast.columns:
                    self.kwaliteit_gegevens_vast[column] = pd.to_datetime(
                        self.kwaliteit_gegevens_vast[column], dayfirst=True
                    )

    def to_dict(self):
        d = {**self.locatie_gegevens}
        if hasattr(self, "kwaliteit_gegevens_vast"):
            d["kwaliteit_gegevens_vast"] = self.kwaliteit_gegevens_vast
        return d


class KorrelgrootteAnalyse(ChemischeAnalyse):
    _download_url = (
        "https://www.dinoloket.nl/uitgifteloket/api/brh/grainsizeanalysis/csv"
    )


class VerticaalElektrischSondeeronderzoek(CsvFileOrUrl):
    _download_url = "https://www.dinoloket.nl/uitgifteloket/api/ves/csv"

    # Read a VES-file
    def _read_contents(self, f):
        # read first line and place cursor at start of document again
        start = f.tell()
        line = f.readline().rstrip("\n")
        f.seek(start)

        # VES Overzicht
        if line.startswith('"VES Overzicht"'):
            line = f.readline()
            self.ves_overzicht, line = self._read_properties_csv_columns(f)
            for key in self.ves_overzicht:
                setattr(self, key, self.ves_overzicht[key])

        # Kop
        if line.startswith('"Kop"'):
            line = f.readline()
            self.kop, line = self._read_properties_csv_columns(f)

        if line.startswith('"Data"'):
            line = f.readline()
            self.data, line = self._read_csv_part(f)

        self.interpretatie_door_tno_nitg = []
        self.interpretaties = []

        while line.startswith('"Interpretatie door: TNO-NITG"'):
            # Interpretatie door: TNO-NITG
            line = f.readline()
            df, line = self._read_properties_csv_columns(f)
            self.interpretatie_door_tno_nitg.append(df)

            # Interpretaties
            if line.startswith('"Interpretaties"'):
                line = f.readline()
                df, line = self._read_csv_part(f)
                self.interpretaties.append(df)

    def to_dict(self):
        d = {**self.ves_overzicht, **self.kop}
        if hasattr(self, "data"):
            d["data"] = self.data
        d["Aantal interpretaties"] = len(self.interpretaties)
        if len(self.interpretatie_door_tno_nitg) > 0:
            # only take the first interpretatie_door_tno_nitg, as the data will not fit in a DataFrame
            d["interpretatie_door_tno_nitg"] = self.interpretatie_door_tno_nitg[0]
        if len(self.interpretaties) > 0:
            # only take the first interpretation, as the data will not fit in a DataFrame
            d["interpretaties"] = self.interpretaties[0]
        if (
            "Richting" in d
            and "Maximale elektrode afstand L2" in d
            and "X-coordinaat" in d
            and "Y-coordinaat" in d
        ):
            angle = (d["Richting"] - 90) * np.pi / 180
            x = d["X-coordinaat"]
            y = d["Y-coordinaat"]
            dx = -np.cos(angle) * d["Maximale elektrode afstand L2"]
            dy = np.sin(angle) * d["Maximale elektrode afstand L2"]
            d["geometry"] = LineString([(x + dx, y + dy), (x - dx, y - dy)])
        return d

    def plot_interpretaties(
        self, nr=None, ax=None, top=0, bot=None, negative_depth=True, **kwargs
    ):
        """
        Plot interpreted resistance profiles from VES data.

        This method visualizes one or more interpretation profiles by plotting the
        'Werkelijke weerstand' (actual resistance) against depth as a line (stairs).

        Parameters
        ----------
        nr : int or None, optional
            Index of a specific interpretation to plot. If None (default), all
            interpretations in `self.interpretaties` are plotted.
        ax : matplotlib.axes.Axes, optional
            The matplotlib Axes object to draw the plot on. If None, the current Axes
            (`plt.gca()`) is used. The default is None.
        top : float, optional
            Top depth of the plot in meters. The default is 0.
        bot : float or None, optional
            Bottom depth of the plot in meters. If None (default), it is inferred from
            the data, by setting the length of the last section equal to the length of
            the next to last section.
        negative_depth : bool, optional
            If True (default), depth is plotted as negative (i.e., increasing downwards,
            following geotechnical convention).
        **kwargs : dict, optional
            Additional keyword arguments passed to `matplotlib.axes.Axes.plot` (e.g.,
            color, linestyle, label).

        Returns
        -------
        ax : matplotlib.axes.Axes
            The Axes object containing the plot.
        """
        if nr is None:
            dfs = self.interpretaties
            if len(dfs) == 0:
                nitg_nr = getattr(self, "NITG-nr")
                logger.warning(f"No interpretations in {nitg_nr}")
                return
        else:
            dfs = [self.interpretaties[nr]]

        if ax is None:
            ax = plt.gca()

        for df in dfs:
            values = df["Werkelijke weerstand"].values

            edges = df["Bovenkant laag (m)"].values[1:]
            edges = np.vstack((edges, edges)).transpose().ravel()
            edge_top = df["Bovenkant laag (m)"].iloc[0]
            if np.isnan(edge_top):
                edge_top = top
            edge_bot = df["Onderkant laag (m)"].iloc[-1]
            if np.isnan(edge_bot):
                if bot is None or np.isnan(bot):
                    edge_bot = df["Bovenkant laag (m)"].iloc[-1] + (
                        df["Bovenkant laag (m)"].iloc[-1]
                        - df["Bovenkant laag (m)"].iloc[-2]
                    )
                else:
                    edge_bot = bot
            edges = np.hstack((edge_top, edges, edge_bot))

            values = np.vstack((values, values)).transpose().ravel()

            if negative_depth:
                edges = -edges

            ax.plot(values, edges, **kwargs)

        return ax
