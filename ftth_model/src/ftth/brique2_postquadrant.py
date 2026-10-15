"""Brique 2 — Moteur Post-Quadrant (recoupement blocages Arcep -> Q1-Q4).

Definitions
-----------
BlocageDeclareArcep_j,i = I(ever_rad = 1 OU motif_blocage_arcep not in {'AUCUN', NULL})

Matrice d'affectation :
  ANOMALIE_VITESSE + blocage=1 -> Q1_Blocage_Exogene_Concordant
  ANOMALIE_VITESSE + blocage=0 -> Q2_Inefficience_OI_Non_Justifiee (cible d'etude)
  NOMINAL          + blocage=1 -> Q3_Declaration_Preventive_Faux_RAD
  NOMINAL          + blocage=0 -> Q4_Deploiement_Nominal

Deux voies d'exploitation :
  - Voie 1 (MANUAL_EXCEL_EXPORT) : export Excel des anomalies pour audit humain.
  - Voie 2 (AUTO_MODEL) : quadrants automatiques + syntheses pour la Brique 3.
"""

from __future__ import annotations

from pathlib import Path

from .config import FTTHConfig, MOTIFS_ABSENCE_BLOCAGE, Q1_LABEL, Q2_LABEL, Q3_LABEL, Q4_LABEL
from .io_utils import checkpoint_ok, count_parquet, get_duckdb_connection, get_logger, write_copy_parquet


def blocage_sql(ever_rad_col: str = "ever_rad", motif_col: str = "motif_blocage_arcep") -> str:
    motifs = ", ".join(f"'{m}'" for m in sorted(MOTIFS_ABSENCE_BLOCAGE))
    return (
        f"(COALESCE({ever_rad_col}, 0) = 1 "
        f"OR UPPER(TRIM(COALESCE(CAST({motif_col} AS VARCHAR), 'AUCUN'))) NOT IN ({motifs}))"
    )


def run_brique2(
    config: FTTHConfig | None = None,
    base_dir: str | Path | None = None,
    force: bool = False,
    mode: str | None = None,
) -> dict[str, Path]:
    """Execute la Brique 2. Retourne les chemins des sorties."""
    from .config import FTTHConfig as _C

    config = config or _C(BASE_DIR=Path(base_dir) if base_dir else Path("data"))
    if base_dir is not None:
        config.BASE_DIR = Path(base_dir)
    log = get_logger()
    base = Path(config.BASE_DIR)
    mode = (mode or config.POST_QUADRANT_MODE).upper()

    pre_path = base / config.OUT_PRE_CLASSIFIED
    if not pre_path.exists():
        raise FileNotFoundError(f"[B2] Entree Brique 1 introuvable : {pre_path}")
    out_path = base / config.OUT_QUADRANTS

    con = get_duckdb_connection(memory=config.DUCKDB_MEMORY, threads=config.DUCKDB_THREADS, load_spatial=False)

    if mode == "MANUAL_EXCEL_EXPORT":
        log.info("[B2] Mode MANUAL_EXCEL_EXPORT : pas d'affectation auto (voir exports_excel).")
        con.close()
        return {"quadrants": out_path, "mode": mode}  # type: ignore[dict-item]

    if not force and checkpoint_ok(out_path, con):
        log.info("[B2] Checkpoint existant : %s (%d lignes) — SKIP.", out_path, count_parquet(con, str(out_path)))
        con.close()
        return {"quadrants": out_path}

    q = f"""
    SELECT pre.*,
      CASE WHEN {blocage_sql('pre.ever_rad', 'pre.motif_blocage_arcep')} THEN 1 ELSE 0 END AS blocage_declare_arcep,
      CASE
        WHEN pre.statut_pre_quadrant = 'ANOMALIE_VITESSE'
             AND {blocage_sql('pre.ever_rad', 'pre.motif_blocage_arcep')} THEN '{Q1_LABEL}'
        WHEN pre.statut_pre_quadrant = 'ANOMALIE_VITESSE' THEN '{Q2_LABEL}'
        WHEN {blocage_sql('pre.ever_rad', 'pre.motif_blocage_arcep')} THEN '{Q3_LABEL}'
        ELSE '{Q4_LABEL}'
      END AS quadrant_final
    FROM read_parquet('{pre_path}') pre
    """
    write_copy_parquet(con, q, out_path)
    n = count_parquet(con, str(out_path))
    log.info("[B2] Termine : %d batiments quadrants -> %s", n, out_path)

    # Ventilation de controle.
    try:
        rows = con.execute(
            f"SELECT quadrant_final, COUNT(*) n FROM read_parquet('{out_path}') GROUP BY 1 ORDER BY 1"
        ).fetchall()
        for quad, cnt in rows:
            log.info("[B2]   %-40s %d (%.2f %%)", quad, cnt, 100 * cnt / max(n, 1))
    except Exception:
        pass
    con.close()
    return {"quadrants": out_path}


def quadrant_summary_duckdb(quadrants_path: str | Path):
    """Tableau de bord agrege OI x zone x quadrant (leger, < 50k lignes)."""
    import duckdb

    con = duckdb.connect(":memory:")
    q = f"""
    SELECT operateur_infra, zone_reglementaire, quadrant_final,
      COUNT(*) AS nb_batiments, SUM(nb_locaux) AS nb_locaux_total,
      AVG(retard_pur_q) AS retard_moyen_trimestres,
      AVG(CASE WHEN quadrant_final = '{Q2_LABEL}' THEN 1.0 ELSE 0.0 END) AS part_q2
    FROM read_parquet('{quadrants_path}')
    GROUP BY operateur_infra, zone_reglementaire, quadrant_final
    ORDER BY operateur_infra, zone_reglementaire, quadrant_final
    """
    df = con.execute(q).pl()
    con.close()
    return df
