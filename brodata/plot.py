import matplotlib.pyplot as plt


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
