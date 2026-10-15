"""Brique 1 — Moteur Pre-Quadrant (cinematique ZAPM, Z-score robuste, anomalies).

Mathematiques (cahier des charges)
---------------------------------
Pour chaque ZAPM i :
  t0_i^ZAPM = min_{j in i}(t_etudes,j)   (lancement des etudes de la ZAPM)

Pour chaque batiment j de i (censure a droite en T_MAX) :
  D_j,i        = min(t_raccordable,j, T_MAX+1) - t0_i^ZAPM
  D_j,i^(0->1) = min(t_etudes,j, T_MAX+1) - t0_i^ZAPM
  D_j,i^(1->3) = min(t_encours,j, T_MAX+1) - min(t_etudes,j, T_MAX+1)
  D_j,i^(3->4) = min(t_raccordable,j, T_MAX+1) - min(t_encours,j, T_MAX+1)

Statistiques robustes intra-ZAPM :
  M_i   = Mediane_j(D_j,i)
  MAD_i = Mediane_j(|D_j,i - M_i|)
  RetardPur_j,i = D_j,i - M_i
  Z_j,i^robuste = (D_j,i - M_i) / max(1.4826 * MAD_i, 1.0)

Regle de classification :
  ANOMALIE_VITESSE si Z >= K (2.0) OU RetardPur >= 4 trimestres, sinon NOMINAL.

Pourquoi mediane/MAD et non moyenne/ecart-type : dans une ZAPM ou > 20 %
des batiments sont bloques longtemps, sigma explose (effet de masquage)
et le seuil mu + 2*sigma devient inatteignable. La mediane reste ancree
sur la cohorte modale d'ouverture du PM.
"""

from __future__ import annotations

from pathlib import Path

from .config import FTTHConfig
from .io_utils import (
    checkpoint_ok,
    count_parquet,
    get_duckdb_connection,
    get_logger,
    write_copy_parquet,
)


def run_brique1(
    config: FTTHConfig | None = None,
    base_dir: str | Path | None = None,
    force: bool = False,
    k_threshold: float | None = None,
    delay_quarters_threshold: int | None = None,
) -> dict[str, Path]:
    """Execute la Brique 1. Retourne les chemins des checkpoints parquet."""
    from .config import FTTHConfig as _C

    config = config or _C(BASE_DIR=Path(base_dir) if base_dir else Path("data"))
    if base_dir is not None:
        config.BASE_DIR = Path(base_dir)
    log = get_logger()
    base = Path(config.BASE_DIR)
    k = config.K_ROBUST_Z if k_threshold is None else k_threshold
    dq = config.MIN_EXCESS_QUARTERS if delay_quarters_threshold is None else delay_quarters_threshold
    t_max, t_cens = config.T_MAX, config.T_CENSORE_CODE
    t1 = t_max + 1

    cycles_path = base / config.OUT_BATIMENTS_CYCLES
    if not cycles_path.exists():
        raise FileNotFoundError(f"[B1] Entree Brique 0 introuvable : {cycles_path}")
    out_zapm = base / config.OUT_ZAPM_STATS
    out_classed = base / config.OUT_PRE_CLASSIFIED
    out_anom = base / config.OUT_ANOMALIES_ONLY

    con = get_duckdb_connection(memory=config.DUCKDB_MEMORY, threads=config.DUCKDB_THREADS, load_spatial=False)

    # --- 1. t0 par ZAPM + durees D_j,i ---
    # Note : t_etudes NULL (Neant permanent) -> t0 ignore NULL ; D utilise T_MAX+1.
    if force or not checkpoint_ok(out_zapm, con):
        q_zapm = f"""
        WITH base AS (
          SELECT ref_pm, nb_locaux,
            COALESCE(NULLIF(t_raccordable, {t_cens}), {t1}) AS t_end,
            t_etudes AS t_start_raw
          FROM read_parquet('{cycles_path}') WHERE ref_pm IS NOT NULL
        ),
        t0 AS (
          SELECT ref_pm, COALESCE(MIN(t_start_raw), {t1}) AS t0_zapm
          FROM base GROUP BY ref_pm
        ),
        dur AS (
          SELECT b.ref_pm AS ref_pm, b.nb_locaux AS nb_locaux,
                 GREATEST(b.t_end - t.t0_zapm, 0) AS duree_q, t.t0_zapm AS t0_zapm
          FROM base b JOIN t0 t ON b.ref_pm = t.ref_pm
        ),
        med AS (
          SELECT ref_pm, COUNT(*) AS nb_batiments_zapm, SUM(nb_locaux) AS nb_locaux_zapm,
                 MEDIAN(duree_q) AS med_duree_zapm, AVG(duree_q) AS mean_duree_zapm,
                 MIN(t0_zapm) AS t0_zapm
          FROM dur GROUP BY ref_pm
        )
        SELECT m.ref_pm, m.nb_batiments_zapm, m.nb_locaux_zapm,
               m.med_duree_zapm, m.mean_duree_zapm, m.t0_zapm,
               MEDIAN(ABS(d.duree_q - m.med_duree_zapm)) AS mad_duree_zapm
        FROM dur d JOIN med m ON d.ref_pm = m.ref_pm
        GROUP BY m.ref_pm, m.nb_batiments_zapm, m.nb_locaux_zapm,
                 m.med_duree_zapm, m.mean_duree_zapm, m.t0_zapm
        """
        write_copy_parquet(con, q_zapm, out_zapm)
        log.info("[B1] zapm_stats : %d ZAPM -> %s", count_parquet(con, str(out_zapm)), out_zapm)
    else:
        log.info("[B1] Checkpoint zapm_stats existant — SKIP.")

    # --- 2. Classification NOMINAL / ANOMALIE_VITESSE (+ sous-durees) ---
    if force or not checkpoint_ok(out_classed, con):
        q_bat = f"""
        WITH base AS (
          SELECT c.*,
            COALESCE(MIN(c.t_etudes) OVER (PARTITION BY c.ref_pm), {t1}) AS t0_zapm,
            COALESCE(NULLIF(c.t_raccordable, {t_cens}), {t1}) AS t_end,
            COALESCE(c.t_etudes, {t1}) AS t_s,
            COALESCE(c.t_encours, c.t_etudes, {t1}) AS t_e
          FROM read_parquet('{cycles_path}') c WHERE c.ref_pm IS NOT NULL
        ),
        dur AS (
          SELECT base.*,
            GREATEST(t_end - t0_zapm, 0) AS duree_totale_q,
            GREATEST(LEAST(COALESCE(t_etudes, {t1}), {t1}) - t0_zapm, 0) AS dur_0_1_q,
            GREATEST(LEAST(COALESCE(t_encours, COALESCE(t_etudes, {t1})), {t1}) - LEAST(COALESCE(t_etudes, {t1}), {t1}), 0) AS dur_1_3_q,
            GREATEST(t_end - LEAST(COALESCE(t_encours, COALESCE(t_etudes, {t1})), {t1}), 0) AS dur_3_4_q
          FROM base
        )
        SELECT d.* EXCLUDE (t0_zapm, t_end, t_s, t_e),
          z.med_duree_zapm, z.mad_duree_zapm, z.t0_zapm,
          (d.duree_totale_q - z.med_duree_zapm) AS retard_pur_q,
          (d.duree_totale_q - z.med_duree_zapm)
            / GREATEST({config.MAD_NORMALIZER} * COALESCE(z.mad_duree_zapm, 0), {config.MAD_FLOOR}) AS z_score_robuste,
          CASE WHEN ((d.duree_totale_q - z.med_duree_zapm)
                       / GREATEST({config.MAD_NORMALIZER} * COALESCE(z.mad_duree_zapm, 0), {config.MAD_FLOOR})) >= {k}
                     OR (d.duree_totale_q - z.med_duree_zapm) >= {dq}
                THEN 'ANOMALIE_VITESSE' ELSE 'NOMINAL' END AS statut_pre_quadrant
        FROM dur d JOIN read_parquet('{out_zapm}') z ON d.ref_pm = z.ref_pm
        """
        write_copy_parquet(con, q_bat, out_classed)
        log.info("[B1] pre_quadrant : %d batiments -> %s", count_parquet(con, str(out_classed)), out_classed)
    else:
        log.info("[B1] Checkpoint pre_quadrant existant — SKIP.")

    # --- 3. Extraction anomalies seules (sas vers Brique 2 / audit manuel) ---
    if force or not checkpoint_ok(out_anom):
        con.execute(
            f"""COPY (SELECT * FROM read_parquet('{out_classed}')
            WHERE statut_pre_quadrant = 'ANOMALIE_VITESSE')
            TO '{out_anom}' (FORMAT PARQUET, COMPRESSION ZSTD);"""
        )
        log.info("[B1] anomalies : %d -> %s", count_parquet(con, str(out_anom)), out_anom)
    else:
        log.info("[B1] Checkpoint anomalies existant — SKIP.")

    con.close()
    return {"zapm_stats": out_zapm, "pre_classified": out_classed, "anomalies": out_anom}


def compute_national_taux(pre_classified_path: str | Path) -> dict:
    """Taux nationaux tau_IMB et tau_Locaux (+ ventilation individuel/collectif).

      tau_IMB    = P(batiment en anomalie)
      tau_Locaux = sum(L_j * I_anom) / sum(L_j)
    """
    import duckdb

    con = duckdb.connect(":memory:")
    p = str(pre_classified_path)
    q = f"""
    SELECT COUNT(*) AS n,
      SUM(CASE WHEN statut_pre_quadrant = 'ANOMALIE_VITESSE' THEN 1 ELSE 0 END) AS n_anom,
      SUM(nb_locaux) AS L,
      SUM(CASE WHEN statut_pre_quadrant = 'ANOMALIE_VITESSE' THEN nb_locaux ELSE 0 END) AS L_anom,
      SUM(CASE WHEN nb_locaux <= 2 THEN 1 ELSE 0 END) AS n_ind,
      SUM(CASE WHEN nb_locaux <= 2 AND statut_pre_quadrant = 'ANOMALIE_VITESSE' THEN 1 ELSE 0 END) AS n_ind_anom,
      SUM(CASE WHEN nb_locaux >= 3 THEN 1 ELSE 0 END) AS n_coll,
      SUM(CASE WHEN nb_locaux >= 3 AND statut_pre_quadrant = 'ANOMALIE_VITESSE' THEN 1 ELSE 0 END) AS n_coll_anom,
      SUM(CASE WHEN nb_locaux <= 2 THEN nb_locaux ELSE 0 END) AS L_ind,
      SUM(CASE WHEN nb_locaux <= 2 AND statut_pre_quadrant = 'ANOMALIE_VITESSE' THEN nb_locaux ELSE 0 END) AS L_ind_anom,
      SUM(CASE WHEN nb_locaux >= 3 THEN nb_locaux ELSE 0 END) AS L_coll,
      SUM(CASE WHEN nb_locaux >= 3 AND statut_pre_quadrant = 'ANOMALIE_VITESSE' THEN nb_locaux ELSE 0 END) AS L_coll_anom
    FROM read_parquet('{p}')
    """
    row = con.execute(q).fetchone()
    (n, n_anom, L, L_anom, n_ind, n_ind_anom, n_coll, n_coll_anom,
     L_ind, L_ind_anom, L_coll, L_coll_anom) = row
    con.close()
    safe = lambda a, b: (a / b) if b else 0.0
    return {
        "n_batiments": n, "n_anomalies": n_anom, "tau_IMB": safe(n_anom, n),
        "tau_Locaux": safe(L_anom, L),
        "tau_IMB_individuel": safe(n_ind_anom, n_ind),
        "tau_IMB_collectif": safe(n_coll_anom, n_coll),
        "tau_Locaux_individuel": safe(L_ind_anom, L_ind),
        "tau_Locaux_collectif": safe(L_coll_anom, L_coll),
    }
