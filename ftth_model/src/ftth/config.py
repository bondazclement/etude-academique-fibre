"""Section 0 — Configuration globale du pipeline FTTH.

Centralise tous les parametres modifiables en tete de notebook
(bornes temporelles, seuils de detection, modes, chemins, ressources DuckDB).

Conventions trimestrielles
---------------------------
t = 1  -> 2018_T2
t = 32 -> 2026_T1
t = 33 -> 2026_T2  (calendrier inclusif : 33 trimestres, pas 32)
t = 99 -> code sentinelle "jamais raccordable observe" (censure a droite)
Le notebook aligne T_MAX sur le dernier millesime reellement telecharge.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


def trimestre_label(t: int) -> str:
    """Libelle Arcep (YYYY_TX) pour un indice trimestriel t in [1, 32]."""
    # t=1 -> 2018_T2. Chaque annee = 4 trimestres.
    # Decalage : annee = 2018 + (t + 1) // 4 ... verification :
    # t=1 -> (1+1)//4 = 0 -> 2018, trimestre = ((1+1) % 4) + 1 = 3? Non.
    # Methode explicite : trimestre civil = ((1 + t) % 4) + 1 avec t=1 -> T2.
    # t=1 -> idx global = 2018*4 + (2-1) + (t-1) = mois-trimestre absolu.
    base_year, base_q = 2018, 2
    total_q = (base_year * 4 + (base_q - 1)) + (t - 1)
    year = total_q // 4
    q = (total_q % 4) + 1
    return f"{year}_T{q}"


def trimestre_index(year: int, quarter: int) -> int:
    """Indice t in [1, 32] depuis (annee, trimestre civil)."""
    base_year, base_q = 2018, 2
    return (year * 4 + (quarter - 1)) - (base_year * 4 + (base_q - 1)) + 1


@dataclass
class FTTHConfig:
    """Parametres globaux du notebook (Section 0)."""

    # --- Bornes temporelles ---
    T_MIN: int = 1
    T_MAX: int = 32
    T_CENSORE_CODE: int = 99  # code sentinelle "jamais raccordable"

    # --- Perimetre reglementaire ---
    # Seules les ZAPM de Zone B (AMII/AMEL) et Zone C (RIP) sont retenues.
    RETAINED_ZONES: list = field(
        default_factory=lambda: ["B_AMII", "B_AMEL", "C_RIP"]
    )
    EXCLUDED_ZONES: list = field(
        default_factory=lambda: ["A_ZTD_HD", "A_ZTD_BD", "A_ZTD", "ZTD", "ZTD_HD", "ZTD_BD"]
    )

    # --- Seuils de detection (Brique 1) ---
    K_ROBUST_Z: float = 2.0  # seuil Z_robuste >= 2.0 -> anomalie
    MIN_EXCESS_QUARTERS: int = 4  # retard pur >= 4 trimestres -> anomalie
    MAD_FLOOR: float = 1.0  # plancher du denominateur max(1.4826*MAD, 1.0)
    MAD_NORMALIZER: float = 1.4826  # constante de normalite du MAD

    # --- Mode post-quadrant (Brique 2) ---
    # "AUTO_MODEL" : affectation algorithmique Q1-Q4 + econometrie.
    # "MANUAL_EXCEL_EXPORT" : export Excel seul pour audit manuel.
    POST_QUADRANT_MODE: str = "AUTO_MODEL"

    # --- Ressources DuckDB ---
    DUCKDB_MEMORY: str = "14GB"
    DUCKDB_THREADS: int = 8
    DUCKDB_TMP_DIR: str | None = None

    # --- Chemins ---
    BASE_DIR: Path = field(default_factory=lambda: Path("data"))
    RAW_IMB_GLOB: str = "raw_data/01_arcep_imb_trimestriels/imb_t*.parquet"
    RAW_IMB_CSV_GLOB: str = "raw_data/01_arcep_imb_trimestriels/*.csv*"
    RAW_ZAPM_GPKG: str = "raw_data/02_arcep_sig_zapm_pm/contours_zapm_ftth.gpkg"
    RAW_ZAPM_PARQUET_FALLBACK: str = "raw_data/02_arcep_sig_zapm_pm/zapm_pm.parquet"
    RAW_ZONAGE_PARQUET: str = "raw_data/03_arcep_zonage_ref/reference_communes.parquet"
    RAW_BAN_PARQUET: str = "raw_data/04_ign_bdtopo_ban/ban_france.parquet"
    RAW_BDTOPO_GPKG: str = "raw_data/04_ign_bdtopo_ban/bdtopo_batiments.gpkg"
    RAW_ENEDIS_PARQUET: str = "raw_data/05_enedis_odre_aerien/enedis_bt_communes.parquet"

    PROC_STEP0_DIR: str = "processed_parquet/step0_etl"
    PROC_STEP1_DIR: str = "processed_parquet/step1_pre_quadrant"
    PROC_STEP2_DIR: str = "processed_parquet/step2_post_quadrant"
    PROC_STEP3_DIR: str = "processed_parquet/step3_econometrie"
    OUTPUTS_EXCEL_DIR: str = "outputs_excel"

    OUT_BATIMENTS_CYCLES: str = "processed_parquet/step0_etl/batiments_cycles.parquet"
    OUT_ZAPM_STATS: str = "processed_parquet/step1_pre_quadrant/zapm_stats.parquet"
    OUT_PRE_CLASSIFIED: str = "processed_parquet/step1_pre_quadrant/batiments_pre_quadrant.parquet"
    OUT_ANOMALIES_ONLY: str = "processed_parquet/step1_pre_quadrant/anomalies_vitesse_seules.parquet"
    OUT_QUADRANTS: str = "processed_parquet/step2_post_quadrant/batiments_4_quadrants.parquet"
    OUT_ZAPM_FEATURES: str = "processed_parquet/step3_econometrie/zapm_features.parquet"

    # --- Econometrie (Brique 3) ---
    OI_REFERENCE: str = "OI_Reference"  # modalite de reference auto si absente -> OI le plus frequent
    ZONE_REFERENCE: str = "C_RIP"
    N_DIFFICULTY_QUINTILES: int = 5
    SFA_DUREE_QUANTILE: float = 0.95  # Duree95 pour la frontiere stochastique

    # --- Divers ---
    RANDOM_STATE: int = 42
    EXCEL_MAX_ROWS: int = 1_048_576

    def resolve(self, rel: str) -> Path:
        return Path(self.BASE_DIR) / rel

    @property
    def t_max_plus_one(self) -> int:
        return self.T_MAX + 1

    def trimestre_labels(self) -> dict[int, str]:
        return {t: trimestre_label(t) for t in range(self.T_MIN, self.T_MAX + 1)}


DEFAULT_CONFIG = FTTHConfig()

# --- Normalisation des etats IPE Arcep ---
# Etats reglementaires hors ZTD : etudes(1) -> programme(2) -> en_cours(3)
# -> raccordable_demande/RAD(3.5) -> raccordable(4). "neant"(0) = absent de l'IPE.
ETAT_ORDER = {
    "neant": 0,
    "etudes": 1,
    "etude": 1,
    "programmee": 2,
    "programme": 2,
    "en_cours": 3,
    "encours": 3,
    "raccordable_demande": 3.5,
    "rad": 3.5,
    "raccordable": 4,
}

# Motifs consideres comme "AUCUN blocage" (insensibles a la casse/espaces).
MOTIFS_ABSENCE_BLOCAGE = {"", "AUCUN", "AUCUNE", "NULL", "NONE", "AUCUN_BLOCAGE", "NON", "NA", "N/A"}

# Noms canoniques des quadrants (cahier des charges).
Q1_LABEL = "Q1_Blocage_Exogene_Concordant"
Q2_LABEL = "Q2_Inefficience_OI_Non_Justifiee"
Q3_LABEL = "Q3_Declaration_Preventive_Faux_RAD"
Q4_LABEL = "Q4_Deploiement_Nominal"

STATUT_ANOMALIE = "ANOMALIE_VITESSE"
STATUT_NOMINAL = "NOMINAL"
