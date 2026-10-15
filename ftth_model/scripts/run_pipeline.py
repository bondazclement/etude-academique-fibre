"""CLI headless du pipeline FTTH (alternative au notebook).

Exemples
--------
python scripts/run_pipeline.py --demo --n-zapm 40
python scripts/run_pipeline.py --base-dir data --force --mode AUTO_MODEL
python scripts/run_pipeline.py --only brique1 --base-dir data
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ftth.brique0_etl import run_brique0
from ftth.brique1_prequadrant import compute_national_taux, run_brique1
from ftth.brique2_postquadrant import run_brique2
from ftth.brique3_econometrie import run_brique3
from ftth.config import FTTHConfig
from ftth.synthetic_generator import generate_demo_dataset


def main() -> int:
    ap = argparse.ArgumentParser(description="Pipeline FTTH B0->B3")
    ap.add_argument("--base-dir", default="data")
    ap.add_argument("--demo", action="store_true", help="genere un jeu synthetique avant B0")
    ap.add_argument("--n-zapm", type=int, default=40)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--mode", default="AUTO_MODEL", choices=["AUTO_MODEL", "MANUAL_EXCEL_EXPORT"])
    ap.add_argument("--only", default="all", choices=["all", "brique0", "brique1", "brique2", "brique3"])
    ap.add_argument("--test1-sample-max", type=int, default=None)
    args = ap.parse_args()

    config = FTTHConfig(BASE_DIR=Path(args.base_dir), POST_QUADRANT_MODE=args.mode)
    if args.demo:
        print(f"[demo] generation synthetique ({args.n_zapm} ZAPM) ...")
        generate_demo_dataset(config=config, base_dir=args.base_dir, n_zapm=args.n_zapm, seed=args.seed)

    only = args.only
    if only in ("all", "brique0"):
        print(run_brique0(config=config, force=args.force))
    if only in ("all", "brique1"):
        outs = run_brique1(config=config, force=args.force)
        print(compute_national_taux(outs["pre_classified"]))
    if only in ("all", "brique2"):
        print(run_brique2(config=config, force=args.force))
    if only in ("all", "brique3"):
        res = run_brique3(config=config, test1_sample_max=args.test1_sample_max)
        print(res["test1"].summary)
        print(res["test2a"].summary)
        print(res["sfa"].summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
