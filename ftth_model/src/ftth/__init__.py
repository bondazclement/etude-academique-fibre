"""FTTH — Modelisation economique du deploiement fibre (Zones B et C, hors ZTD).

Pipeline : Brique 0 (ETL) -> Brique 1 (Pre-Quadrant cinematique)
-> Brique 2 (Post-Quadrant Q1-Q4) -> Brique 3 (Econometrie Quadrant 2).
"""

__version__ = "1.0.0"

from .config import FTTHConfig, DEFAULT_CONFIG  # noqa: F401
