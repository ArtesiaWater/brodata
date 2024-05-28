import requests
from tqdm import tqdm
import pandas as pd
from .webservices import get_configuration, get_gdf
from io import StringIO


def get_verticaal_elektrisch_sondeeronderzoek(
    extent, config=None, timeout=5, silent=False
):
    kind = "Verticaal elektrisch sondeeronderzoek"
    if config is None:
        config = get_configuration()
    gdf = get_gdf(
        kind,
        config=config,
        extent=extent,
        timeout=timeout,
    )

    download_url = config[kind]["download"]
    data = {}
    for name in tqdm(gdf.index, disable=silent):
        url = f"{download_url}/{name}"
        data[name] = VerticaalElektrischSondeeronderzoek(url, timeout=timeout)
    return gdf, data


def get_grondwaterstand(extent, config=None, timeout=5, silent=False):
    kind = "Grondwaterstand"
    if config is None:
        config = get_configuration()
    gdf = get_gdf(
        kind,
        config=config,
        extent=extent,
        timeout=timeout,
    )
    gdf = gdf[gdf["ST_CNT"] > 0]

    download_url = config[kind]["download"]
    data = {}
    for name in tqdm(gdf.index, disable=silent):
        if False:
            url = config[kind]["details"]
            r = requests.post(url, data='["{name}"]', timeout=timeout)
            if not r.ok:
                raise (ValueError(f"Retreiving data from {url} failed"))
            data = r.json()
        else:
            url = "{}/{}/query".format(config[kind]["mapserver"], config[kind]["table"])
            GDW_DBK = gdf.at[name, "GDW_DBK"]
            params = {"where": f"GDW_DBK = {GDW_DBK}", "f": "pjson"}
            r = requests.get(url, params=params)
            if not r.ok:
                raise (ValueError(f"Retreiving data from {url} failed"))
            for feature in r.json()["features"]:
                piezometer_nr = feature["attributes"]["PIEZOMETER_NR"]
                url = f"{download_url}/{name}/{piezometer_nr}"
                data[f"{name}_{piezometer_nr}"] = Grondwaterstand(url, timeout=timeout)

    return gdf, data


def get_grondwatersamenstelling(extent, config=None, timeout=5, silent=False):
    kind = "Grondwatersamenstelling"
    if config is None:
        config = get_configuration()
    gdf = get_gdf(
        kind,
        config=config,
        extent=extent,
        timeout=timeout,
    )
    gdf = gdf[gdf["SA_CNT"] > 0]
    download_url = config[kind]["download"]
    data = {}
    for name in tqdm(gdf.index, disable=silent):
        url = f"{download_url}/{name}"
        data[name] = Grondwatersamenstelling(url)

    return gdf, data


def get_geologisch_booronderzoek(extent, config=None, timeout=5, silent=False):
    kind = "Geologisch booronderzoek"
    if config is None:
        config = get_configuration()
    gdf = get_gdf(
        kind,
        config=config,
        extent=extent,
        timeout=timeout,
    )
    download_url = config[kind]["download"]
    data = {}
    for name in tqdm(gdf.index, disable=silent):
        url = f"{download_url}/{name}"
        data[name] = GeologischBooronderzoek(url, timeout=timeout)


class CsvFileOrUrl:
    def __init__(self, fname, timeout=5):
        if fname.startswith("http"):
            r = requests.get(fname, timeout=timeout)
            if not r.ok:
                raise (Exception((f"Retieving data from {fname} failed")))
            self._read_contents(StringIO(r.text))
        else:
            with open(fname, "r") as f:
                self._read_contents(f)

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


class Grondwaterstand(CsvFileOrUrl):
    def _read_contents(self, f):
        self.props, line = self._read_properties_csv_rows(f, merge_columns=True)
        self.props2, line = self._read_properties_csv_rows(f)
        self.meta, line = self._read_csv_part(f)
        self.data, line = self._read_csv_part(f)

    def to_dict(self):
        d = {**self.props, **self.props2}
        if hasattr(self, "data"):
            d["meta"] = self.meta
        if hasattr(self, "data"):
            d["data"] = self.data
        return d


class Grondwatersamenstelling(CsvFileOrUrl):
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

    def to_dict(self):
        d = {**self.locatie_gegevens}
        if hasattr(self, "kwaliteit_gegevens_vloeibaar"):
            d["kwaliteit_gegevens_vloeibaar"] = self.kwaliteit_gegevens_vloeibaar
        return d


class GeologischBooronderzoek(CsvFileOrUrl):
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
        for key in self.algemene_gegevens_boring:
            if key in self.algemene_gegevens_lithologie:
                # 'Datum boring' can be specified in algemene_gegevens_boring and algemene_gegevens_lithologie
                if pd.isna(self.algemene_gegevens_lithologie[key]):
                    self.algemene_gegevens_lithologie.pop(key)
        d = {**self.algemene_gegevens_boring, **self.algemene_gegevens_lithologie}
        d["lithologie_lagen"] = self.lithologie_lagen
        if hasattr(self, "lithologie_sublagen"):
            d["lithologie_sublagen"] = self.lithologie_sublagen
        return d


class VerticaalElektrischSondeeronderzoek(CsvFileOrUrl):
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

        # Interpretatie door: TNO-NITG
        if line.startswith('"Interpretatie door: TNO-NITG"'):
            line = f.readline()
            self.interpretatie_door_tno_nitg, line = self._read_properties_csv_columns(
                f
            )

        # Interpretaties
        if line.startswith('"Interpretaties"'):
            line = f.readline()
            self.interpretaties, line = self._read_csv_part(f)

    def to_dict(self):
        d = {**self.ves_overzicht, **self.kop}
        if hasattr(self, "interpretatie_door_tno_nitg"):
            d.update(self.interpretatie_door_tno_nitg)
        if hasattr(self, "data"):
            d["data"] = self.data
        if hasattr(self, "interpretaties"):
            d["interpretaties"] = self.interpretaties
        if hasattr(self, "lithologie_sublagen"):
            d["lithologie_sublagen"] = self.lithologie_sublagen
        return d
