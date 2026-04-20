#!/usr/bin/env python3
"""Generate combined PR-curve figure for all routes from real hold-out data."""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import auc


def main() -> None:
    plt.rcParams.update(
        {
            "figure.dpi": 300,
            "savefig.dpi": 300,
            "font.size": 10,
            "axes.titlesize": 11,
            "axes.labelsize": 10,
            "figure.figsize": (6, 5),
            "savefig.bbox": "tight",
        }
    )

    output_dir = Path("data/outputs/longitudinal")
    routes = [
        {"id": "14", "label": "Route 14", "color": "steelblue"},
        {"id": "38", "label": "Route 38", "color": "darkorange"},
        {"id": "49", "label": "Route 49", "color": "forestgreen"},
    ]

    fig, ax = plt.subplots()

    for route in routes:
        csv_path = output_dir / f"pr_curve_route{route['id']}.csv"
        if not csv_path.exists():
            print(f"Warning: Missing {csv_path}. Skipping.")
            continue

        df = pd.read_csv(csv_path)
        if df.empty or "recall" not in df.columns or "precision" not in df.columns:
            print(f"Warning: Invalid format in {csv_path}. Skipping.")
            continue

        # Compute area under curve using trapezoidal rule for display in legend
        pr_auc = auc(df["recall"], df["precision"])
        
        ax.plot(
            df["recall"], 
            df["precision"], 
            color=route["color"], 
            linewidth=2, 
            label=f"{route['label']} (AUC = {pr_auc:.3f})"
        )

    ax.set_xlabel("Recall (True Positive Rate)")
    ax.set_ylabel("Precision (Positive Predictive Value)")
    ax.set_title("Early Warning Classifier: Precision-Recall Curves")
    ax.set_xlim(0, 1.05)
    ax.set_ylim(0, 1.05)
    ax.grid(True, linestyle="--", alpha=0.35)
    ax.legend(loc="lower left")

    # Save to both data/outputs and manuscript folders
    pdf_name = "Figure 1.pdf"
    ax_pdf_path = output_dir / pdf_name
    fig.savefig(ax_pdf_path)
    print(f"Generated combined PR curve {ax_pdf_path}")

    for lang in ["english", "spanish"]:
        manuscript_path = Path("manuscript") / lang / pdf_name
        manuscript_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(manuscript_path)
        print(f"Updated {manuscript_path}")


if __name__ == "__main__":
    main()
