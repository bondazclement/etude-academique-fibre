"""Tests du pipeline complet sur donnees synthetiques (B0 -> B3)."""

from pathlib import Path

import pandas as pd


def test_pipeline_demo_b0_b2(tmp_path: Path):
    from ftth.brique0_etl import run_brique0, validate_brique0_output
    from ftth.brique1_prequadrant import compute_national_taux, run_brique1
    from ftth.brique2_postquadrant import run_brique2
    from ftth.config import FTTHConfig
    from ftth.synthetic_generator import generate_demo_dataset

    base = tmp_path / "data"
    config = FTTHConfig(BASE_DIR=base)
    generate_demo_dataset(config=config, base_dir=base, n_zapm=12, seed=11)

    b0 = run_brique0(config=config)
    assert b0.exists()
    checks = validate_brique0_output(b0)
    assert checks["n_lignes"] > 500
    assert checks["n_zapm"] >= 8

    outs1 = run_brique1(config=config)
    assert outs1["pre_classified"].exists()
    taux = compute_national_taux(outs1["pre_classified"])
    assert 0.0 < taux["tau_IMB"] < 0.6

    outs2 = run_brique2(config=config)
    assert outs2["quadrants"].exists()
    df = pd.read_parquet(outs2["quadrants"], columns=["quadrant_final"])
    quads = set(df["quadrant_final"].unique().tolist())
    assert "Q2_Inefficience_OI_Non_Justifiee" in quads
    assert "Q4_Deploiement_Nominal" in quads


def test_brique3_small_sample(tmp_path: Path):
    from ftth.brique0_etl import run_brique0
    from ftth.brique1_prequadrant import run_brique1
    from ftth.brique2_postquadrant import run_brique2
    from ftth.brique3_econometrie import run_brique3
    from ftth.config import FTTHConfig
    from ftth.synthetic_generator import generate_demo_dataset

    base = tmp_path / "data"
    config = FTTHConfig(BASE_DIR=base)
    generate_demo_dataset(config=config, base_dir=base, n_zapm=30, seed=5)
    run_brique0(config=config)
    run_brique1(config=config)
    run_brique2(config=config)
    res = run_brique3(config=config)
    assert len(res["zapm_features"]) >= 20
    assert "quintile_difficulte" in res["zapm_features"].columns
    assert res["test1"].n_obs > 100
