import logging

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def cone_penetration_test(
    cpt, figsize=(10, 10), ax=None, linewidth=1.0, ylabel="Sondeertrajectlengte"
):
    if hasattr(cpt, "conePenetrationTest"):
        df = cpt.conePenetrationTest
    else:
        df = cpt
    if ax is None:
        f, ax1 = plt.subplots(figsize=figsize)
    else:
        ax1 = ax
    ax1.set_ylabel(ylabel)
    ax1.invert_yaxis()

    axes = []

    if not df["coneResistance"].isna().all():
        ax1.plot(df["coneResistance"], df.index, color="b", linewidth=linewidth)
        ax1.set_xlim(0, df["coneResistance"].max() * 2)
        ax1.tick_params(axis="x", labelcolor="b")
        lab = ax1.set_xlabel("Conusweerstand MPa", color="b")
        lab.set_position((0.0, lab.get_position()[1]))
        lab.set_horizontalalignment("left")
        axes.append(ax1)

    if not df["frictionRatio"].isna().all():
        ax2 = ax1.twiny()
        ax2.xaxis.set_ticks_position("bottom")
        ax2.xaxis.set_label_position("bottom")
        ax2.plot(df["frictionRatio"], df.index, color="g", linewidth=linewidth)
        ax2.set_xlim(0, df["frictionRatio"].max() * 2)
        ax2.tick_params(axis="x", labelcolor="g")
        ax2.invert_xaxis()
        lab = ax2.set_xlabel("Wrijvingsgetal", color="g")
        lab.set_position((1.0, lab.get_position()[1]))
        lab.set_horizontalalignment("right")
        axes.append(ax2)

    if not df["localFriction"].isna().all():
        ax3 = ax1.twiny()
        ax3.plot(
            df["localFriction"],
            df.index,
            color="r",
            linestyle="--",
            linewidth=linewidth,
        )
        ax3.set_xlim(0, df["localFriction"].max() * 2)
        ax3.tick_params(axis="x", labelcolor="r")
        lab = ax3.set_xlabel("Plaatselijke wrijving", color="r")
        lab.set_position((0.0, lab.get_position()[1]))
        lab.set_horizontalalignment("left")
        axes.append(ax3)

    if not df["inclinationResultant"].isna().all():
        ax4 = ax1.twiny()
        ax4.plot(
            df["inclinationResultant"],
            df.index,
            color="m",
            linestyle="--",
            linewidth=linewidth,
        )

        ax4.set_xlim(0, df["inclinationResultant"].max() * 2)
        ax4.tick_params(axis="x", labelcolor="m")
        ax4.invert_xaxis()
        lab = ax4.set_xlabel("Hellingsresultante", color="m")
        lab.set_position((1.0, lab.get_position()[1]))
        lab.set_horizontalalignment("right")
        axes.append(ax4)

    if ax is None:
        f.tight_layout(pad=0.0)

    return axes


def get_lithology_color(hoofdgrondsoort, zandmediaanklasse=None, drilling=None):
    colors = {
        "grind": (216, 163, 32),
        "hout": (157, 78, 64),
        "klei": (0, 146, 0),
        "leem": (194, 207, 92),
        "oer": (200, 200, 200),
        "puin": (200, 200, 200),
        "stenen": (216, 163, 32),
        "veen": (157, 78, 64),
        "zand": (255, 255, 0),
        "slib": (144, 144, 144),
        "schelpen": (95, 95, 255),
        "sterkGrindigZand": (231, 195, 22),  # same as zand grove categorie
        "wegverhardingsmateriaal": (200, 200, 200),  # same as puin, check at B25D3298
        "zwakZandigeKlei": (0, 146, 0),  # same as klei
        "gyttja": (157, 78, 64),  # same as hout, check at B02G0307
        "niet benoemd": (255, 255, 255),
        "geen monster": (255, 255, 255),
    }

    label = None
    if hoofdgrondsoort in colors:
        if hoofdgrondsoort == "zand":
            if zandmediaanklasse in [
                "fijne categorie (O)",
                "zeer fijn (O)",
                "uiterst fijn (O)",
                "zeer fijn",
                "uiterst fijn",
            ]:
                color = colors[hoofdgrondsoort]
                label = "Zand fijne categorie"
            elif zandmediaanklasse in [
                "matig fijn",
                "matig fijn (O)",
                "matig grof",
                "matig grof (O)",
                "midden categorie (O)",
            ]:
                label = "Zand midden categorie"
                color = (243, 225, 6)
            elif zandmediaanklasse in [
                "grove  categorie (O)",
                "zeer grof",
                "zeer grof (O)",
                "uiterst grof",
                "uiterst grof (O)",
            ]:
                color = (231, 195, 22)
                label = "Zand grove categorie"
            else:
                if not (
                    pd.isna(zandmediaanklasse)
                    or zandmediaanklasse in ["zandmediaan onduidelijk"]
                ):
                    msg = f"Unknown zandmediaanklasse: {zandmediaanklasse}"
                    if drilling is not None:
                        msg = f"{msg} in drilling {drilling}"
                    logger.warning(msg)
                # for zandmediaanklasse is None or something other than mentioned above
                color = colors[hoofdgrondsoort]
        else:
            color = colors[hoofdgrondsoort]
        color = tuple(x / 255 for x in color)
    else:
        msg = f"No color defined for hoofdgrondsoort {hoofdgrondsoort}"
        if drilling is not None:
            msg = f"{msg} in drilling {drilling}"
        logger.warning(msg)
        color = (1.0, 1.0, 1.0)

    if label is None:
        label = hoofdgrondsoort.capitalize()
    return color, label


def lithology(
    df,
    top,
    bot,
    kind,
    sand_class=None,
    ax=None,
    x=0.5,
    z=0.0,
    solid_capstyle="butt",
    linewidth=6,
    drilling=None,
    **kwargs,
):
    h = []
    if not isinstance(df, pd.DataFrame):
        return h
    if ax is None:
        ax = plt.gca()
    for index in df.index:
        z_top = z - df.at[index, top]
        z_bot = z - df.at[index, bot]
        zandmediaanklasse = None if sand_class is None else df.at[index, sand_class]
        color, label = get_lithology_color(
            df.at[index, kind], zandmediaanklasse, drilling=drilling
        )
        if x is not None and np.isfinite(x):
            h.append(
                ax.plot(
                    [x, x],
                    [z_bot, z_top],
                    color=color,
                    label=label,
                    linewidth=linewidth,
                    solid_capstyle=solid_capstyle,
                    **kwargs,
                )
            )
        else:
            h.append(
                ax.axhspan(
                    z_bot,
                    z_top,
                    facecolor=color,
                    label=label,
                    linewidth=linewidth,
                    **kwargs,
                )
            )
    return h


def lithology_along_line(
    gdf, line, kind, ax=None, legend=True, max_distance=None, **kwargs
):
    """
    Plot lithological drillings along a cross-sectional line.

    This function visualizes subsurface lithology data from borehole records
    in a 2D cross-section view, based on their proximity to a specified line.
    It supports both 'dino' and 'bro' formatted datasets.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame containing borehole data. This typically includes geometry and
        lithology-related columns. Can be retrieved using, for example,
        `brodata.dino.get_boormonsterprofiel`.
    line : shapely.geometry.LineString or list of tuple[float, float]
        The cross-sectional line along which to plot the lithologies. Determines the
        x-coordinates of the lithology logs. If `max_distance` is set, only boreholes
        within this distance from the line will be included.
    kind : str
        Specifies the data source format. Must be either 'dino' or 'bro'.
    ax : matplotlib.axes.Axes, optional
        The matplotlib axes object to plot on. If None, uses the current axes.
    legend : bool, optional
        Whether to include a legend for the lithology classes. Default is True.
    max_distance : float, optional
        Maximum distance (in the same units as the GeoDataFrame's CRS) from the line
        within which boreholes are included in the cross-section. If None, includes all.
    **kwargs :
        Additional keyword arguments passed to either `dino_lithology` or `bro_lithology`.

    Returns
    -------
    ax : matplotlib.axes.Axes
        The matplotlib axes object containing the lithology cross-section plot.

    Raises
    ------
    Exception
        If `kind` is not 'dino' or 'bro'.
    """
    from shapely.geometry import LineString

    ax = plt.gca() if ax is None else ax

    line = LineString(line) if not isinstance(line, LineString) else line

    if max_distance is not None:
        gdf = gdf[gdf.distance(line) < max_distance]

    # calculate length along line
    s = pd.Series([line.project(point) for point in gdf["geometry"]], gdf.index)

    for index in gdf.index:
        if kind == "dino":
            dino_lithology(
                gdf.at[index, "lithologie_lagen"],
                z=gdf.at[index, "Maaiveldhoogte (m tov NAP)"],
                x=s[index],
                drilling=index,
                ax=ax,
                **kwargs,
            )
        elif kind == "bro":
            if len(gdf.at[index, "descriptiveBoreholeLog"]) > 0:
                msg = (
                    f"More than 1 descriptiveBoreholeLog for {index}. "
                    "Only plotting the first one."
                )
                logger.warning(msg)
            df = gdf.at[index, "descriptiveBoreholeLog"][0]["layer"]
            bro_lithology(df, x=s[index], drilling=index, ax=ax, **kwargs)
        else:
            raise (Exception(f"Unknown kind: {kind}"))

    if legend:  # add a legend
        add_lithology_legend(ax=ax)

    return ax


def add_lithology_legend(ax, **kwargs):
    handles, labels = ax.get_legend_handles_labels()
    labels, index = np.unique(np.array(labels), return_index=True)
    boven = np.array(
        [
            "Veen",
            "Klei",
            "Leem",
            "Zand fijne categorie",
            "Zand midden categorie",
            "Zand grove categorie",
            "Zand",
            "Grind",
        ]
    )
    for lab in boven:
        if lab in labels:
            mask = labels == lab
            labels = np.hstack((labels[mask], labels[~mask]))
            index = np.hstack((index[mask], index[~mask]))
    onder = np.array(["Niet benoemd", "Geen monster"])
    for lab in onder:
        if lab in labels:
            mask = labels == lab
            labels = np.hstack((labels[~mask], labels[mask]))
            index = np.hstack((index[~mask], index[mask]))
    return ax.legend(np.array(handles)[index], labels, **kwargs)


def dino_lithology(df, **kwargs):
    return lithology(
        df,
        top="Bovenkant laag (m beneden maaiveld)",
        bot="Onderkant laag (m beneden maaiveld)",
        kind="Hoofdgrondsoort",
        sand_class="Zandmediaanklasse",
        **kwargs,
    )


def bro_lithology(df, **kwargs):
    return lithology(
        df,
        top="upperBoundary",
        bot="lowerBoundary",
        kind="geotechnicalSoilName",
        **kwargs,
    )
