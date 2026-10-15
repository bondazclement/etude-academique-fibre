"""Tests unitaires des mathematiques cinematiques (Z robuste, quadrants)."""

import numpy as np
import pandas as pd


def robust_z(durations: np.ndarray, m: float, mad: float) -> np.ndarray:
    return (durations - m) / max(1.4826 * mad, 1.0)


def test_robust_z_no_masking():
    # ZAPM tres bloquee : 70 % a D=2, 30 % a D=14. Le Z classique masque tout,
    # le Z robuste doit detecter les 30 %.
    d = np.array([2] * 70 + [14] * 30, dtype=float)
    mu, sigma = d.mean(), d.std(ddof=0)
    classique = (d - mu) / sigma
    assert (classique >= 2.0).sum() == 0  # masquage confirme
    m = float(np.median(d))
    mad = float(np.median(np.abs(d - m)))
    zr = robust_z(d, m, mad)
    assert m == 2.0
    assert ((zr >= 2.0) | ((d - m) >= 4)).sum() == 30


def test_mad_zero_floor():
    # > 50 % de la ZAPM ouvre au meme trimestre -> MAD = 0 -> plancher a 1.0.
    d = np.array([3] * 80 + [4] * 10 + [12] * 10, dtype=float)
    m = float(np.median(d))
    mad = float(np.median(np.abs(d - m)))
    assert mad == 0.0
    zr = robust_z(d, m, mad)
    assert np.allclose(zr[d == 12], 9.0)  # (12-3)/1.0


def test_quadrant_matrix():
    from ftth.config import Q1_LABEL, Q2_LABEL, Q3_LABEL, Q4_LABEL

    def quadrant(statut: str, blocage: int) -> str:
        if statut == "ANOMALIE_VITESSE" and blocage == 1:
            return Q1_LABEL
        if statut == "ANOMALIE_VITESSE":
            return Q2_LABEL
        if blocage == 1:
            return Q3_LABEL
        return Q4_LABEL

    assert quadrant("ANOMALIE_VITESSE", 1) == Q1_LABEL
    assert quadrant("ANOMALIE_VITESSE", 0) == Q2_LABEL
    assert quadrant("NOMINAL", 1) == Q3_LABEL
    assert quadrant("NOMINAL", 0) == Q4_LABEL


def test_duree_censure():
    # t_raccordable = 99 -> min(99, T_MAX+1) = 33.
    T_MAX = 32
    t0, t_racc = 5, 99
    D = min(t_racc if t_racc != 99 else T_MAX + 1, T_MAX + 1) - t0
    assert D == 28
