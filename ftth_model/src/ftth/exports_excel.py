"""Exports Excel : audit manuel (Voie 1) et tableaux de bord (Voie 2).

Contrainte Excel : 1 048 576 lignes max par feuille. Tous les exports sont
donc soit agreges, soit filtres (departement / OI / echantillon), soit
decoupes par departement.
"""

from __future__ import annotations

from pathlib import Path

from .config import FTTHConfig
from .io_utils import get_duckdb_connection, get_logger


def _write_excel_polars(df, path: Path, sheet: str = "data") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        # Polars >= 0.20 : write_excel via xlsxwriter.
        df.write_excel(str(path), worksheet=sheet)
        return
    except Exception:
        pass
    # Repli pandas / openpyxl.
    import pandas as pd

    if hasattr(df, "to_pandas"):
        pdf = df.to_pandas()
    elif hasattr(df, "toPandas"):
        pdf = df.toPandas()
    else:
        pdf = pd.DataFrame(df)
    with pd.ExcelWriter(str(path), engine="openpyxl") as w:
        pdf.to_excel(w, sheet_name=sheet, index=False)


def export_anomalies_for_manual_audit(
    pre_quadrant_path: str | Path,
    out_xlsx: str | Path,
    code_dept: str | list[str] | None = None,
    operateur: str | list[str] | None = None,
    limit: int = 200_000,
    order_by: str = "z_score_robuste DESC",
) -> Path:
    """Voie 1 : classeur d'audit manuel des ANOMALIE_VITESSE.

    Colonnes : id_imb, ref_pm, code_insee, nb_locaux, jalons, sous-durees
    D(0->1)/D(1->3)/D(3->4), M_i, MAD_i, retard pur, Z robuste.
    """
    log = get_logger()
    con = get_duckdb_connection(load_spatial=False)
    filters = ["statut_pre_quadrant = 'ANOMALIE_VITESSE'"]
    if code_dept:
        depts = [code_dept] if isinstance(code_dept, str) else code_dept
        in_list = ", ".join(f"'{d}'" for d in depts)
        filters.append(f"code_dept IN ({in_list})")
    if operateur:
        ois = [operateur] if isinstance(operateur, str) else operateur
        in_list = ", ".join(f"'{o}'" for o in ois)
        filters.append(f"operateur_infra IN ({in_list})")
    where = " AND ".join(filters)
    q = f"""
    SELECT id_imb, ref_pm, code_insee, code_dept, operateur_infra, zone_reglementaire,
      nb_locaux, t_etudes, t_programme, t_encours, t_raccordable,
      duree_totale_q, dur_0_1_q, dur_1_3_q, dur_3_4_q,
      med_duree_zapm, mad_duree_zapm, retard_pur_q, z_score_robuste,
      ever_rad, motif_blocage_arcep, dist_pm_m
    FROM read_parquet('{pre_quadrant_path}')
    WHERE {where}
    ORDER BY {order_by}
    LIMIT {int(limit)}
    """
    df = con.execute(q).pl()
    con.close()
    out = Path(out_xlsx)
    _write_excel_polars(df, out, sheet="anomalies_audit")
    log.info("Export audit manuel : %d lignes -> %s", len(df), out)
    return out


def export_zapm_summary_excel(
    zapm_stats_path: str | Path,
    quadrants_path: str | Path | None,
    out_xlsx: str | Path,
) -> Path:
    """Synthese ZAPM (~110k lignes France entiere : tient dans une feuille)."""
    log = get_logger()
    con = get_duckdb_connection(load_spatial=False)
    if quadrants_path is not None and Path(quadrants_path).exists():
        q = f"""
        SELECT z.*,
          k.taux_Q2, k.taux_Q1, k.nb_bat_total
        FROM read_parquet('{zapm_stats_path}') z
        LEFT JOIN (
          SELECT ref_pm, COUNT(*) AS nb_bat_total,
            AVG(CASE WHEN quadrant_final LIKE 'Q2%' THEN 1.0 ELSE 0.0 END) AS taux_Q2,
            AVG(CASE WHEN quadrant_final LIKE 'Q1%' THEN 1.0 ELSE 0.0 END) AS taux_Q1
          FROM read_parquet('{quadrants_path}') GROUP BY ref_pm
        ) k ON z.ref_pm = k.ref_pm
        ORDER BY k.taux_Q2 DESC NULLS LAST
        """
    else:
        q = f"SELECT * FROM read_parquet('{zapm_stats_path}') ORDER BY ref_pm"
    df = con.execute(q).pl()
    con.close()
    out = Path(out_xlsx)
    _write_excel_polars(df, out, sheet="zapm_stats")
    log.info("Export ZAPM : %d lignes -> %s", len(df), out)
    return out


def export_dashboard_final_excel(
    quadrants_path: str | Path,
    zapm_features_path: str | Path | None,
    out_xlsx: str | Path,
) -> Path:
    """Voie 2 : tableau de bord final OI x zone x quintile x quadrant."""
    from .brique2_postquadrant import quadrant_summary_duckdb

    log = get_logger()
    df_synth = quadrant_summary_duckdb(str(quadrants_path))
    out = Path(out_xlsx)
    out.parent.mkdir(parents=True, exist_ok=True)
    try:
        import pandas as pd

        synth_pd = df_synth.to_pandas() if hasattr(df_synth, "to_pandas") else pd.DataFrame(df_synth)
        with pd.ExcelWriter(str(out), engine="openpyxl") as w:
            synth_pd.to_excel(w, sheet_name="synthese_OI_zone", index=False)
            if zapm_features_path is not None and Path(zapm_features_path).exists():
                zf = pd.read_parquet(zapm_features_path)
                agg = (
                    zf.groupby(["operateur_infra", "quintile_difficulte"], as_index=False)
                    .agg(taux_Q2_moyen=("taux_Q2", "mean"), nb_zapm=("ref_pm", "count"))
                    .sort_values(["operateur_infra", "quintile_difficulte"])
                )
                agg.to_excel(w, sheet_name="resilience_quintiles", index=False)
    except Exception:
        _write_excel_polars(df_synth, out, sheet="synthese_OI_zone")
    log.info("Dashboard final -> %s", out)
    return out


def export_with_fallback(df_pandas, out_xlsx: str | Path, sheet: str = "data") -> Path:
    out = Path(out_xlsx)
    out.parent.mkdir(parents=True, exist_ok=True)
    import pandas as pd

    pdf = df_pandas if isinstance(df_pandas, pd.DataFrame) else pd.DataFrame(df_pandas)
    with pd.ExcelWriter(str(out), engine="openpyxl") as w:
        pdf.to_excel(w, sheet_name=sheet, index=False)
    return out
