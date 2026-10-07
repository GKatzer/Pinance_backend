"""Draw the figures used in README.md from the committed live samples.

Inputs  : docs/examples/live/*.json (read-only GETs against the public deployment, see
          docs/examples/live/CAPTURED_AT.txt)
Outputs : docs/media/*.png

    uv pip install matplotlib
    python docs/figures/make_figures.py
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter

ROOT = Path(__file__).resolve().parents[1]
LIVE = ROOT / "examples" / "live"
OUT = ROOT / "media"
OUT.mkdir(exist_ok=True)

BLUE, VIOLET, GREY, RED = "#2b6cb0", "#7c3aed", "#6b7280", "#c2410c"
plt.rcParams.update({
    "font.size": 10, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.alpha": 0.25, "figure.dpi": 100,
})


def load(name: str):
    return json.loads((LIVE / name).read_text())


def captured_at() -> str:
    return (LIVE / "CAPTURED_AT.txt").read_text().strip()


def coverage_by_window() -> None:
    cov = load("coverage.json")["windows"]
    labels = list(cov)
    q10 = [cov[w]["q10_coverage"] * 100 for w in labels]
    q90 = [cov[w]["q90_coverage"] * 100 for w in labels]
    ns = [cov[w]["n"] for w in labels]

    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8), sharex=True)
    for ax, vals, target, title, colour in (
        (axes[0], q10, 10, "Share of outcomes below the q10 bound (target 10 %)", BLUE),
        (axes[1], q90, 90, "Share of outcomes below the q90 bound (target 90 %)", VIOLET),
    ):
        bars = ax.bar(range(len(labels)), vals, color=colour, width=0.6)
        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels([f"{w}\nn={n:,}" for w, n in zip(labels, ns)], fontsize=8.5)
        ax.axhline(target, color=RED, lw=1.5, ls="--")
        ax.set_title(title, fontsize=10)
        ax.set_ylabel("%")
        for b, v in zip(bars, vals):
            ax.text(b.get_x() + b.get_width() / 2, v + 1.2, f"{v:.1f}", ha="center", fontsize=9)
        ax.set_ylim(0, 105)
    fig.suptitle(f"BTC/USDT corridor calibration, GET /metrics/BTC%2FUSDT/coverage ({captured_at()})", fontsize=11)
    fig.text(0.5, -0.02, "n counts forecast rows (12 horizons per candle), so it overstates the number of independent samples.",
             ha="center", fontsize=8, color=GREY)
    fig.tight_layout()
    fig.savefig(OUT / "coverage-by-window.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def accuracy_hourly() -> None:
    from datetime import datetime

    pts = load("history-accuracy.json")["points"]
    xs = [datetime.fromisoformat(p["ts"].replace("Z", "+00:00")) for p in pts]
    ys = [p["value"] for p in pts]

    fig, ax = plt.subplots(figsize=(11, 3.6))
    ax.plot(xs, ys, color=BLUE, marker="o", ms=4, lw=1.6)
    ax.axhline(50, color=RED, lw=1.2, ls="--", label="coin flip (50 %)")
    summary = load("summary.json")["windows"]["24h"]
    ax.axhline(summary["directional_accuracy"], color=GREY, lw=1.2, ls=":",
               label=f"24 h mean {summary['directional_accuracy']} % (n={summary['n']:,} rows)")
    ax.set_ylim(0, 100)
    ax.set_ylabel("directional accuracy, %")
    ax.xaxis.set_major_formatter(DateFormatter("%d %H:%M"))
    ax.set_title(f"BTC/USDT hourly directional accuracy, GET /metrics/BTC%2FUSDT/history ({captured_at()})", fontsize=11)
    ax.legend(loc="upper right", fontsize=9)
    fig.text(0.5, -0.03,
             "Each point averages about 144 rows (12 candles x 12 horizons) whose targets overlap in time, "
             "so the effective sample behind a point is far smaller than 144.",
             ha="center", fontsize=8, color=GREY)
    fig.tight_layout()
    fig.savefig(OUT / "accuracy-hourly.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def error_histogram() -> None:
    d = load("errors.json")
    edges, counts = d["bin_edges"], d["counts"]
    centers = [(a + b) / 2 for a, b in zip(edges[:-1], edges[1:])]
    width = edges[1] - edges[0]

    fig, ax = plt.subplots(figsize=(11, 3.4))
    ax.bar(centers, counts, width=width * 0.92, color=VIOLET)
    ax.axvline(0, color=RED, lw=1.2, ls="--")
    ax.set_xlabel("forecast price minus realised price, USDT")
    ax.set_ylabel("rows")
    ax.set_title(
        f"BTC/USDT forecast error, last 24 h, GET /metrics/BTC%2FUSDT/errors "
        f"(n={sum(counts):,}, mean {d['mean_error']}, std {d['std_error']}; {captured_at()})",
        fontsize=10.5,
    )
    fig.tight_layout()
    fig.savefig(OUT / "error-histogram.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    coverage_by_window()
    accuracy_hourly()
    error_histogram()
    print("written:", ", ".join(sorted(p.name for p in OUT.glob("*.png"))))
