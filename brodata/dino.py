import os
import requests
from tqdm import tqdm
import numpy as np
import pandas as pd
from io import StringIO, TextIOWrapper
from shapely.geometry import LineString
from .webservices import get_configuration, get_gdf
from .util import (
    objects_to_gdf,
    _get_data_from_path,
    _get_data_from_zip,
    _save_data_to_zip,
)


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
):
    if isinstance(extent, str):
        data = _get_data_from_path(extent, dino_cl, silent=silent)
        return objects_to_gdf(data, x, y, geometry, index, to_gdf)

    if to_zip is not None:
        if not redownload and os.path.isfile(to_zip):
            data = _get_data_from_zip(to_zip, dino_cl, silent=silent)
            return objects_to_gdf(data, x, y, geometry, index, to_gdf)
        if to_path is None:
            to_path = os.path.splitext(to_zip)[0]
        remove_path_again = not os.path.isdir(to_path)
        files = []

    if config is None:
        config = get_configuration()
    gdf = get_gdf(
        kind,
        config=config,
        extent=extent,
        timeout=timeout,
    )

    to_file = None
    if to_path is not None and not os.path.isdir(to_path):
        os.makedirs(to_path)
    data = {}
    for dino_nr in tqdm(gdf.index, disable=silent):
        if to_path is not None:
            to_file = os.path.join(to_path, f"{dino_nr}.csv")
            if to_zip is not None:
                files.append(to_file)
            if not redownload and os.path.isfile(to_file):
                data[dino_nr] = dino_cl(to_file)
                continue
        data[dino_nr] = dino_cl.from_dino_nr(dino_nr, timeout=timeout, to_file=to_file)
    if to_zip is not None:
        _save_data_to_zip(to_zip, files, remove_path_again, to_path)

    return objects_to_gdf(data, x, y, geometry, index, to_gdf)


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
    return objects_to_gdf(data, index=index, to_gdf=to_gdf)


def get_grondwatersamenstelling(extent, **kwargs):
    dino_class = Grondwatersamenstelling
    kind = "Grondwatersamenstelling"
    return _get_data_within_extent(dino_class, kind, extent, **kwargs)


def get_geologisch_booronderzoek(extent, **kwargs):
    dino_class = GeologischBooronderzoek
    kind = "Geologisch booronderzoek"
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


class GeologischBooronderzoek(CsvFileOrUrl):
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
