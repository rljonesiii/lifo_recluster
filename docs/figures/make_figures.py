"""Regenerate the figures in docs/figures from the benchmark results in results/.

    uv run --group docs python docs/figures/make_figures.py

speedup.svg: for each of the 14 benchmark operations, how many times faster the
compiled engine's incremental update is than a compiled fresh trace, at 10k, 100k and
1M transactions (results/nb_n10000.json, nb_n100000.json, nb_n1000000.json).
"""
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.ticker import NullLocator  # noqa: E402

HERE = Path(__file__).resolve().parent
RESULTS = HERE.parent.parent / "results"

SIZES = [(10_000, "10k"), (100_000, "100k"), (1_000_000, "1M")]
# one hue, light to dark for small to large networks; shapes repeat the encoding
STYLE = {"10k": ("#86b6ef", "o"), "100k": ("#2a78d6", "s"), "1M": ("#104281", "D")}
INK, INK2, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#fcfcfb"

# bench.py's operation names, in the order shown, with the labels used in the docs
LOCAL = [
    ("nested-excursion merge (no-op)", "Nested excursion (B+E type)"),
    ("late local merge", "Late local merge"),
    ("entity discovery merge", "Entity discovery"),
    ("root merge (large early hack-1 holders)", "Root merge near hack 1"),
    ("distant-branch merge (hack 1 x hack 2)", "Distant branches (hack 1 × 2)"),
    ("overlapping merge (4 + 5a)", "Overlapping merge"),
    ("split v4 out of (4+5a)", "Split one member back out"),
    ("undo late local merge", "Undo late local merge"),
]
EXCHANGE = [
    ("absorb into exchange cluster", "Wallet absorbed into exchange"),
    ("split exchange 1 in half", "Split an exchange in half"),
    ("merge exchanges 2 + 3", "Merge two exchanges"),
    ("re-merge exchange 1", "Re-merge exchange halves"),
    ("undo exchange 2 + 3 merge", "Undo exchange merge"),
    ("merge all four exchanges", "Merge all four exchanges"),
]


def speedups():
    """{size label: {operation: fresh-trace time / incremental time}}."""
    out = {}
    for n, label in SIZES:
        rows = json.loads((RESULTS / f"nb_n{n}.json").read_text())["main"]["rows"]
        out[label] = {r["op"]: r["fresh_s"] / r["inc_s"] for r in rows}
    return out


def speedup_figure(path):
    s = speedups()
    plt.rcParams.update({
        "font.family": "sans-serif",
        "font.sans-serif": ["DejaVu Sans", "Helvetica", "Arial"],
        "svg.fonttype": "none",
        "svg.hashsalt": "lifo_recluster",
    })
    fig, ax = plt.subplots(figsize=(8.6, 6.6), facecolor=SURFACE)
    ax.set_facecolor(SURFACE)

    # rows top to bottom: a heading, the local operations, a gap, a heading, the rest
    rows, y, headings = [], 0, []
    for title, group in (("Local changes", LOCAL), ("Exchange-scale changes", EXCHANGE)):
        headings.append((y, title))
        y += 1
        for op, label in group:
            rows.append((y, op, label))
            y += 1
        y += 0.6

    for yy, op, _ in rows:
        vals = [s[lab][op] for _, lab in SIZES]
        ax.plot([min(vals), max(vals)], [yy, yy], color=GRID, lw=2, zorder=1,
                solid_capstyle="round")
        for (_, lab), v in zip(SIZES, vals):
            color, marker = STYLE[lab]
            ax.scatter([v], [yy], s=64, color=color, marker=marker, zorder=3,
                       edgecolors=SURFACE, linewidths=1.5)

    # direct labels on the first row, so the sizes read without the legend
    y0, op0, _ = rows[0]
    for _, lab in SIZES:
        ax.annotate(lab, (s[lab][op0], y0), xytext=(0, 9), textcoords="offset points",
                    ha="center", va="bottom", fontsize=8.5, color=INK2)

    ax.set_xscale("log")
    ax.set_xlim(0.5, 2500)
    ax.set_xticks([1, 10, 100, 1000])
    ax.set_xticklabels(["1×", "10×", "100×", "1,000×"])
    ax.xaxis.set_minor_locator(NullLocator())
    ax.tick_params(axis="x", colors=MUTED, labelcolor=INK2, length=0, labelsize=9)
    ax.grid(axis="x", which="major", color=GRID, lw=0.8, zorder=0)
    ax.axvline(1, color=MUTED, lw=1, ls=(0, (3, 3)), zorder=2)
    ax.text(1.08, headings[0][0], "same as a fresh trace", fontsize=8.5, color=MUTED,
            va="center")

    ax.set_yticks([yy for yy, _, _ in rows])
    ax.set_yticklabels([label for _, _, label in rows], fontsize=9.5, color=INK)
    ax.tick_params(axis="y", length=0)
    for yy, title in headings:
        ax.text(-0.02, yy, title, transform=ax.get_yaxis_transform(), ha="right",
                va="center", fontsize=9.5, fontweight="bold", color=INK2)
    ax.set_ylim(y - 0.2, -0.8)
    for side in ("top", "right", "left", "bottom"):
        ax.spines[side].set_visible(False)
    ax.set_xlabel("Times faster than a fresh trace (log scale)", fontsize=9.5,
                  color=INK2, labelpad=8)

    handles = [Line2D([], [], ls="", marker=STYLE[lab][1], color=STYLE[lab][0],
                      markersize=8, markeredgecolor=SURFACE, label=f"{lab} transactions")
               for _, lab in SIZES]
    fig.legend(handles=handles, loc="upper left", bbox_to_anchor=(0.012, 0.915),
               ncol=3, frameon=False, fontsize=9, labelcolor=INK2, handletextpad=0.3,
               columnspacing=1.4)
    fig.text(0.012, 0.975, "Local updates get faster as the network grows",
             fontsize=12, fontweight="bold", color=INK, va="top")
    fig.text(0.012, 0.938, "Compiled engine: fresh-trace time divided by incremental-update "
             "time, for 14 operations at 3 network sizes",
             fontsize=9, color=INK2, va="top")
    fig.text(0.012, 0.012, "Source: results/nb_n10000.json, nb_n100000.json, "
             "nb_n1000000.json (bench.py --nb). Every update matched a fresh trace exactly.",
             fontsize=8, color=MUTED, va="bottom")
    fig.subplots_adjust(left=0.30, right=0.97, top=0.86, bottom=0.12)
    fig.savefig(path, facecolor=SURFACE, metadata={"Date": None})
    plt.close(fig)


if __name__ == "__main__":
    speedup_figure(HERE / "speedup.svg")
    print("wrote", HERE / "speedup.svg")
