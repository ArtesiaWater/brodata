import logging

import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def cone_penetration_test(
    cpt, figsize=(10, 10), ax=None, linewidth=1.0, ylabel="Sondeertrajectlengte"
):
    """
    Plot the results of a cone penetration test (CPT).

    This function visualizes multiple CPT parameters (cone resistance, friction ratio,
    local friction, and inclination resultant) against the test depth or trajectory
    length. Each parameter is plotted on a separate x-axis, sharing the same y-axis.

    Parameters
    ----------
    cpt : pandas.DataFrame or object
        The CPT data as a DataFrame or an object with a 'conePenetrationTest' attribute
        containing the DataFrame.
    figsize : tuple, optional
        Size of the figure to create if `ax` is not provided. Default is (10, 10).
    ax : matplotlib.axes.Axes, optional
        Existing matplotlib Axes to plot on. If None, a new figure and axes are created.
    linewidth : float, optional
        Width of the plot lines. Default is 1.0.
    ylabel : str, optional
        Label for the y-axis. Default is "Sondeertrajectlengte".

    Returns
    -------
    list of matplotlib.axes.Axes
        List of axes objects for each parameter plotted.

    Notes
    -----
    - The y-axis is inverted to represent increasing depth downward.
    - Each parameter is plotted only if its column in the DataFrame is not entirely NaN.
    - The function supports plotting up to four parameters: 'coneResistance',
    'frictionRatio', 'localFriction', and 'inclinationResultant'.
    """
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


lithology_colors = {
    "ballast": (200 / 255, 200 / 255, 200 / 255),  # checked at B38D4055
    "bruinkool": (140 / 255, 92 / 255, 54 / 255),  # checked at B51G2426
    "detritus": (157 / 255, 78 / 255, 64 / 255),  # checked at B44A0733
    "glauconietzand": (204 / 255, 1, 153 / 255),  # checked at B49E1446
    "grind": (216 / 255, 163 / 255, 32 / 255),
    "hout": (157 / 255, 78 / 255, 64 / 255),
    "ijzeroer": (242 / 255, 128 / 255, 13 / 255),  # checked at B49E1446
    "kalksteen": (140 / 255, 180 / 255, 1),  # checked at B44B0062
    "klei": (0, 146 / 255, 0),
    "leem": (194 / 255, 207 / 255, 92 / 255),
    "oer": (200 / 255, 200 / 255, 200 / 255),
    "puin": (200 / 255, 200 / 255, 200 / 255),
    "slurrie": (144/255, 144/255, 144/255),  # same as slib, checked at B25A3512
    "stenen": (216 / 255, 163 / 255, 32 / 255),
    "veen": (157 / 255, 78 / 255, 64 / 255),
    "zand": (1, 1, 0),
    "zand fijn": (1, 1, 0),  # same as zand
    "zand midden": (243 / 255, 225 / 255, 6 / 255),
    "zand grof": (231 / 255, 195 / 255, 22 / 255),
    "sideriet": (242 / 255, 128 / 255, 13 / 255),  # checked at B51D2864
    "slib": (144 / 255, 144 / 255, 144 / 255),
    "schelpen": (95 / 255, 95 / 255, 1),
    "sterkGrindigZand": (
        231 / 255,
        195 / 255,
        22 / 255,
    ),  # same as zand grove categorie
    "wegverhardingsmateriaal": (
        200 / 255,
        200 / 255,
        200 / 255,
    ),  # same as puin, checked at B25D3298
    "zwakZandigeKlei": (0, 146 / 255, 0),  # same as klei
    "gyttja": (157 / 255, 78 / 255, 64 / 255),  # same as hout, checked at B02G0307
    "zandsteen": (200 / 255, 171 / 255, 55 / 255),  # checked at B44B0119
    "niet benoemd": (1, 1, 1),
    "geen monster": (1, 1, 1),
}

sand_class_fine = [
    "fijne categorie (O)",
    "zeer fijn (O)",
    "uiterst fijn (O)",
    "zeer fijn",
    "uiterst fijn",
]

sand_class_medium = [
    "matig fijn",
    "matig fijn (O)",
    "matig grof",
    "matig grof (O)",
    "midden categorie (O)",
]

sand_class_course = [
    "grove  categorie (O)",
    "zeer grof",
    "zeer grof (O)",
    "uiterst grof",
    "uiterst grof (O)",
]


def get_lithology_color(
    hoofdgrondsoort,
    zandmediaanklasse=None,
    drilling=None,
    colors=None,
):
    """
    Return the RGB color and label for a given lithology (hoofdgrondsoort).

    Parameters
    ----------
    hoofdgrondsoort : str or any
        The main soil type (lithology) to get the color for. If not a string (e.g.,
        NaN), a default color is used.
    zandmediaanklasse : str, optional
        The sand median class, used for further classification if hoofdgrondsoort is
        "zand".
    drilling : any, optional
        Optional drilling identifier, used for logging warnings.
    colors : dict, optional
        Dictionary mapping lithology names to RGB color tuples (0-1). If None, uses
        the default `lithology_colors`.

    Returns
    -------
    color : tuple of float
        The RGB color as a tuple of floats in the range [0, 1].
    label : str
        The label for the lithology, possibly more specific for sand classes.

    Notes
    -----
    - If the hoofdgrondsoort is not recognized, a warning is logged and a default white
    color is returned.
    - For "zand", the zandmediaanklasse determines the specific sand color and label.
    - If colors is not provided, the function uses a default color mapping.
    """
    if colors is None:
        colors = lithology_colors
    label = None
    if not isinstance(hoofdgrondsoort, str):
        # hoofdgrondsoort is nan
        color = colors["niet benoemd"]
        label = str(hoofdgrondsoort)
    elif hoofdgrondsoort in colors:
        if hoofdgrondsoort == "zand":
            if zandmediaanklasse in sand_class_fine:
                color = colors["zand fijn"]
                label = "Zand fijne categorie"
            elif zandmediaanklasse in sand_class_medium:
                label = "Zand midden categorie"
                color = colors["zand midden"]
            elif zandmediaanklasse in sand_class_course:
                color = colors["zand grof"]
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
    else:
        msg = f"No color defined for hoofdgrondsoort {hoofdgrondsoort}"
        if drilling is not None:
            msg = f"{msg} in drilling {drilling}"
        logger.warning(msg)
        color = (1.0, 1.0, 1.0)

    if isinstance(color, (tuple, list, np.ndarray)) and np.any([x > 1 for x in color]):
        logger.warning(
            f"Color {color} specified as as integers between 0 and 255. "
            "Please specify rgb-values as floats between 0 and 1."
        )
        color = tuple(x / 255 for x in color)

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
    colors=None,
    **kwargs,
):
    """
    Plot lithology intervals from a DataFrame as vertical lines or filled spans.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing lithology data.
    top : str
        Column name in `df` representing the top depth of each interval.
    bot : str
        Column name in `df` representing the bottom depth of each interval.
    kind : str
        Column name in `df` specifying the lithology type for color mapping.
    sand_class : str, optional
        Column name in `df` specifying sand class for color mapping (default: None).
    ax : matplotlib.axes.Axes, optional
        Matplotlib axis to plot on. If None, uses current axis (default: None).
    x : float, optional
        X-coordinate for vertical lines (default: 0.5). If None or not finite, uses
        filled spans.
    z : float, optional
        Reference depth for vertical positioning (default: 0.0).
    solid_capstyle : str, optional
        Cap style for vertical lines (default: "butt").
    linewidth : float, optional
        Line width for plotting (default: 6).
    drilling : any, optional
        Additional drilling information for color mapping (default: None).
    colors : dict, optional
        Custom color mapping for lithologies (default: None).
    **kwargs
        Additional keyword arguments passed to matplotlib plotting functions.

    Returns
    -------
    list
        List of matplotlib artist objects corresponding to the plotted lithology
        intervals.

    Notes
    -----
    - If `x` is provided and finite, plots vertical lines at `x`.
    - If `x` is None or not finite, plots filled horizontal spans between `z_top` and
    `z_bot`.
    - Uses `get_lithology_color` to determine color and label for each interval.
    """
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
            df.at[index, kind], zandmediaanklasse, drilling=drilling, colors=colors
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
        Additional keyword arguments passed to either `dino_lithology` or
        `bro_lithology`.

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
    s = pd.Series([line.project(point) for point in gdf.geometry], gdf.index)

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
    """
    Add a custom legend to a matplotlib Axes for lithology categories.

    ax : matplotlib.axes.Axes
        The matplotlib Axes object to which the legend will be added.
    **kwargs : dict, optional
        Additional keyword arguments passed to `ax.legend()` (e.g., loc, fontsize).

    Returns
    -------
    matplotlib.legend.Legend
        The legend object added to the axes.

    Notes
    -----
    The function reorders legend entries so that common lithology categories appear in a
    preferred order:
    - "Veen", "Klei", "Leem", "Zand fijne categorie", "Zand midden categorie",
    "Zand grove categorie", "Zand", "Grind"
    These are placed at the top of the legend, while "Niet benoemd" and "Geen monster"
    are placed at the bottom.
    Duplicate labels are removed, keeping only the first occurrence.

    """
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
    """
    Plot lithology information from a DataFrame containing lithology data from DINO.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame containing lithology data.
    **kwargs
        Additional keyword arguments passed to the underlying `lithology` function.

    Returns
    -------
    list
        List of matplotlib artist objects corresponding to the plotted lithology
        intervals.

    Notes
    -----
    This function is a wrapper around the `lithology` function, mapping the DataFrame
    columns:
    - 'Bovenkant laag (m beneden maaiveld)' as top
    - 'Onderkant laag (m beneden maaiveld)' as bot
    - 'Hoofdgrondsoort' as kind
    - 'Zandmediaanklasse' as sand_class
    """
    return lithology(
        df,
        top="Bovenkant laag (m beneden maaiveld)",
        bot="Onderkant laag (m beneden maaiveld)",
        kind="Hoofdgrondsoort",
        sand_class="Zandmediaanklasse",
        **kwargs,
    )


def bro_lithology(df, **kwargs):
    """
    Plot lithology information from a DataFrame containing lithology data from BRO.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame containing lithology data.
    **kwargs
        Additional keyword arguments passed to the underlying `lithology` function.

    Returns
    -------
    list
        List of matplotlib artist objects corresponding to the plotted lithology
        intervals.

    Notes
    -----
    This function is a wrapper around the `lithology` function, mapping the DataFrame
    columns:
    - 'upperBoundary' as the top,
    - 'lowerBoundary' as the bot,
    - 'geotechnicalSoilName' as kind.
    """
    return lithology(
        df,
        top="upperBoundary",
        bot="lowerBoundary",
        kind="geotechnicalSoilName",
        **kwargs,
    )


def get_dino_lithology_colors():
    return lithology_colors


def get_bro_lithology_properties():
    legend = {
        "veen": {"color": (153 / 255, 76 / 255, 58 / 255), "hatch": "-"},
        "klei": {"color": (0, 150 / 255, 8 / 255), "hatch": "/"},
        "leem": {"color": (219 / 255, 219 / 255, 219 / 255), "hatch": "\\"},
        "zand": {"color": (254 / 255, 254 / 255, 8 / 255), "hatch": "."},
        "grind": {"color": (243 / 255, 192 / 255, 39 / 255), "hatch": "o"},
        "silt": {"color": (219 / 255, 219 / 255, 219 / 255), "hatch": "|"},
        "nietBepaald": {"color": (112 / 255, 48 / 255, 160 / 255)},
        "grondNietGespecificeerd": {"color": (1, 1, 1)},
    }

    legend = legend | {
        "mineraalarmVeen": legend["veen"],
        "zwakZandigVeen": [
            {"width": 50 / 60} | legend["veen"],
            {"width": 10 / 60} | legend["zand"],
        ],
        "sterkZandigVeen": [
            {"width": 41 / 60} | legend["veen"],
            {"width": 19 / 60} | legend["zand"],
        ],
        "zwakKleiigVeen": [  # not checked at broloket
            {"width": 50 / 60} | legend["veen"],
            {"width": 10 / 60} | legend["klei"],
        ],
        "sterkKleiigVeen": [
            {"width": 41 / 60} | legend["veen"],
            {"width": 19 / 60} | legend["klei"],
        ],
        "kleiigVeen": [
            {"width": 42 / 60} | legend["veen"],
            {"width": 18 / 60} | legend["klei"],
        ],
        "zwakZandigeKlei": [
            {"width": 48 / 60} | legend["klei"],
            {"width": 12 / 60} | legend["zand"],
        ],
        "matigZandigeKlei": [
            {"width": 41 / 60} | legend["klei"],
            {"width": 19 / 60} | legend["zand"],
        ],
        "sterkZandigeKlei": [
            {"width": 30 / 60} | legend["klei"],
            {"width": 30 / 60} | legend["zand"],
        ],
        "zwakSiltigeKlei": [
            {"width": 50 / 60} | legend["klei"],
            {"width": 10 / 60} | legend["leem"],  # with a hatch
        ],
        "matigSiltigeKlei": [
            {"width": 41 / 60} | legend["klei"],
            {"width": 19 / 60} | legend["leem"],  # with a hatch
        ],
        "sterkSiltigeKlei": [
            {"width": 30 / 60} | legend["klei"],
            {"width": 30 / 60} | legend["leem"],  # with a hatch
        ],
        "uiterstSiltigeKlei": [
            {"width": 26 / 60} | legend["klei"],
            {"width": 34 / 60} | {"color": legend["silt"]["color"]},  # without a hatch
        ],
        "zwakZandigeLeem": [
            {"width": 50 / 60} | legend["leem"],
            {"width": 10 / 60} | legend["zand"],
        ],
        "sterkZandigeLeem": [
            {"width": 30 / 60} | legend["leem"],
            {"width": 30 / 60} | legend["zand"],
        ],
        "sterkGrindigZand": [
            {"width": 36 / 60} | legend["zand"],
            {"width": 24 / 60} | legend["grind"],
        ],
        "zwakSiltigZand": [
            {"width": 50 / 60} | legend["zand"],
            {"width": 10 / 60} | legend["leem"],
        ],
        "matigSiltigZand": [
            {"width": 41 / 60} | legend["zand"],
            {"width": 19 / 60} | legend["leem"],
        ],
        "sterkSiltigZand": [
            {"width": 30 / 60} | legend["zand"],
            {"width": 30 / 60} | legend["leem"],
        ],
        "kleiigZand": [
            {"width": 50 / 60} | legend["zand"],
            {"width": 10 / 60} | legend["klei"],
        ],
        "siltigZand": [
            {"width": 42 / 60} | legend["zand"],
            {"width": 18 / 60} | legend["silt"],
        ],
    }
    return legend


def bro_lithology_advanced(
    df,
    soil_name_column="geotechnicalSoilName",
    z=0.0,
    x=0.5,
    width=0.1,
    lithology_properties=None,
    ax=None,
    hatch_factor=2,
    hatch_color=(0.0, 0.0, 0.0, 0.2),
    hatch_linewidth=2,
    bro_id=None,
):
    # TODO: create a legend. See https://stackoverflow.com/questions/55501860/how-to-put-multiple-colormap-patches-in-a-matplotlib-legend
    ax = plt.gca() if ax is None else ax

    if lithology_properties is None:
        lithology_properties = get_bro_lithology_properties()

    if soil_name_column not in df.columns:
        raise (ValueError(f"Column {soil_name_column} not present in df"))

    handles = []
    for index in df.index:
        # soil_name_column = "geotechnicalSoilName" for GeotechnicalBoreholeResearch
        # soil_name_column = "standardSoilName" for PedologicalBoreholeResearch
        sn = df.at[index, soil_name_column]
        left = x - width / 2
        if sn not in lithology_properties:
            msg = f"SoilName {sn} not supported"
            if bro_id is not None:
                msg = f"{msg} (found at broId {bro_id})"
            logger.warning(f"{msg}. Please add {sn} to lithology_properties")
            continue
        ps = lithology_properties[sn]
        if isinstance(ps, dict):
            ps = [ps]
        for p in ps:
            xy = (left, z - df.at[index, "upperBoundary"])
            w = p["width"] * width if "width" in p else width
            h = df.at[index, "upperBoundary"] - df.at[index, "lowerBoundary"]
            hatch = p["hatch"] * hatch_factor if "hatch" in p else None
            h = ax.add_patch(
                Rectangle(xy, w, h, facecolor=p["color"], hatch=hatch, edgecolor="k")
            )
            h._hatch_color = hatch_color
            h._hatch_linewidth = hatch_linewidth
            left = left + w
            handles.append(h)
    return handles
