"""Brique 0 — ETL geomatique & temporel (fusion des 3 sources -> table condensee).

Entree : 3 sources brutes
  A. batiments physiques (BAN/BD TOPO) : existence a t=0 (etat Neant = 0)
  B. ZAPM/PM Arcep (contours + PM) : polygones ref_pm + armoires PM
  C. 32 millesimes IMB trimestriels Arcep : etat_imb(t) + ref_pm declare + motif

Sortie : ``batiments_cycles.parquet`` — 1 ligne = 1 batiment avec ses
trimestres de premier passage a chaque jalon reglementaire :

  t_etudes     = min{t | etat in {etudes, programme, en_cours, rad, raccordable}}
  t_programme  = min{t | etat in {programme, en_cours, rad, raccordable}}
  t_encours    = min{t | etat in {en_cours, rad, raccordable}}
  t_raccordable= min{t | etat = raccordable} (99 si jamais, censure a droite)
  ever_rad     = max_t I(etat = raccordable_demande)
  duree_en_rad_q = sum_t I(etat = raccordable_demande)

Regles :
  - filtrage hors ZTD (zones B_AMII/B_AMEL/C_RIP uniquement) ;
  - ancrage ref_pm = LAST(ref_pm ORDER BY trimestre), flag has_pm_reconfigured ;
  - ref_pm final = COALESCE(ref_pm_arcep_final, ref_pm_geographique) ;
  - id_imb final = COALESCE(id_imb_arcep, 'NEANT*' || id_ban).
"""

from __future__ import annotations

from pathlib import Path

from .config import FTTHConfig
from .io_utils import (
    checkpoint_ok,
    count_parquet,
    get_duckdb_connection,
    get_logger,
    parquet_columns,
    write_copy_parquet,
)

# Aliases toleres pour la normalisation des colonnes IMB reelles.
IMB_COLUMN_ALIASES = {
    "id_imb": ["id_imb", "id_immeuble", "imb_id", "idimmeuble", "cle_imb"],
    "id_ban": ["id_ban", "cle_interop_ban", "idban", "cle_ban", "ban_id"],
    "code_insee": ["code_insee", "insee", "codeinsee", "code_commune"],
    "ref_pm": ["ref_pm", "refpm", "id_pm", "code_pm", "pm_ref", "ref_zapm"],
    "operateur_infra": ["operateur_infra", "nom_oi", "operateur", "oi", "nom_operateur"],
    "etat_imb": ["etat_imb", "etat", "statut", "statut_imb", "etat_/deployment", "etat_deploiement"],
    "nb_locaux": ["nb_locaux", "nblocaux", "nb_logements", "locaux", "nb_lignes"],
    "motif_blocage": ["motif_blocage", "motif", "motif_refus", "cause_blocage", "motif_arcep"],
    "trimestre": ["trimestre", "t", "periode", "trimestre_idx"],
    "x_l93": ["x_l93", "x", "coord_x", "lambert_x"],
    "y_l93": ["y_l93", "y", "coord_y", "lambert_y"],
}

ETAT_ALIASES = {
    "etudes": ["etudes", "etude", "etudes_realisees", "1", "e"],
    "programme": ["programme", "programmee", "programmes", "2", "p"],
    "en_cours": ["en_cours", "encours", "en-cours", "en_cours_deploiement", "3", "ec"],
    "raccordable_demande": ["raccordable_demande", "rad", "raccordable-sur-demande", "3.5", "rd"],
    "raccordable": ["raccordable", "racco", "4", "r"],
    "neant": ["neant", "neant_0", "0", "absent", "non_renseigne"],
}


def normalize_etat_sql(col: str) -> str:
    """Expression SQL normalisant un etat brut vers le vocabulaire canonique."""
    lowers = f"LOWER(TRIM(CAST({col} AS VARCHAR)))"
    cases = []
    for canon, aliases in ETAT_ALIASES.items():
        in_list = ", ".join(f"'{a}'" for a in aliases)
        cases.append(f"WHEN {lowers} IN ({in_list}) THEN '{canon}'")
    return "CASE " + " ".join(cases) + f" ELSE {lowers} END"


def resolve_column(available: list[str], candidates: list[str]) -> str | None:
    low = {c.lower(): c for c in available}
    for c in candidates:
        if c.lower() in low:
            return low[c.lower()]
    return None


def build_imb_union_view(con, imb_glob: str, trimestre_fallback: int | None = None) -> str:
    """Cree une vue normalisee sur l'empilement des millesimes (glob parquet/csv).

    Retourne le nom de la vue. Gere l'absence de colonne ``trimestre``
    (cas d'un fichier unique) via ``trimestre_fallback``.
    """
    log = get_logger()
    cols = parquet_columns(con, imb_glob)
    if not cols:
        # Peut-etre un glob CSV.
        try:
            cols = [r[0] for r in con.execute(f"SELECT * FROM read_csv_auto('{imb_glob}') LIMIT 0").description]
        except Exception:
            cols = []
    resolved = {}
    for canon, aliases in IMB_COLUMN_ALIASES.items():
        resolved[canon] = resolve_column(cols, aliases) if cols else None
    if resolved["trimestre"] is None and trimestre_fallback is not None:
        log.warning("Colonne trimestre absente, fallback t=%s.", trimestre_fallback)

    def col(canon: str, default_sql: str = "NULL") -> str:
        c = resolved.get(canon)
        return f'"{c}"' if c else default_sql

    etat_expr = (
        normalize_etat_sql(col("etat_imb", "'neant'"))
        if resolved.get("etat_imb")
        else "'neant'"
    )
    tri_expr = col("trimestre", str(trimestre_fallback) if trimestre_fallback else "NULL")
    query = f"""
    SELECT
      CAST({col('id_imb')} AS VARCHAR) AS id_imb,
      CAST({col('id_ban')} AS VARCHAR) AS id_ban,
      CAST({col('code_insee')} AS VARCHAR) AS code_insee,
      CAST({col('ref_pm')} AS VARCHAR) AS ref_pm,
      CAST({col('operateur_infra')} AS VARCHAR) AS operateur_infra,
      {etat_expr} AS etat_imb,
      CAST({col('nb_locaux', '1')} AS INTEGER) AS nb_locaux,
      CAST({col('motif_blocage', "'AUCUN'")} AS VARCHAR) AS motif_blocage,
      CAST({tri_expr} AS INTEGER) AS trimestre,
      CAST({col('x_l93')} AS DOUBLE) AS x_l93,
      CAST({col('y_l93')} AS DOUBLE) AS y_l93
    FROM read_parquet('{imb_glob}')
    """
    # Fallback CSV si read_parquet echoue (detecte a l'execution).
    try:
        con.execute(f"CREATE OR REPLACE TEMP VIEW imb_union AS {query}")
        con.execute("SELECT COUNT(*) FROM imb_union LIMIT 1").fetchone()
    except Exception:
        query_csv = query.replace("read_parquet(", "read_csv_auto(")
        con.execute(f"CREATE OR REPLACE TEMP VIEW imb_union AS {query_csv}")
    return "imb_union"


def run_brique0(
    config: FTTHConfig | None = None,
    base_dir: str | Path | None = None,
    force: bool = False,
) -> Path:
    """Execute la Brique 0 et retourne le chemin de ``batiments_cycles.parquet``.

    Deux chemins d'affectation spatiale :
      1. Reel (GPKG) : ST_Intersects(centroid BAT, polygone ZAPM) via DuckDB spatial.
      2. Degrade (parquet) : jointure sur ref_pm_geo pre-calcule (demo/tests,
         ou BAN deja projetee), ou jointure sur ref_pm declare Arcep.
    """
    from .config import FTTHConfig as _C

    config = config or _C(BASE_DIR=Path(base_dir) if base_dir else Path("data"))
    if base_dir is not None:
        config.BASE_DIR = Path(base_dir)
    log = get_logger()
    base = Path(config.BASE_DIR)

    out_path = base / config.OUT_BATIMENTS_CYCLES
    con = get_duckdb_connection(
        memory=config.DUCKDB_MEMORY, threads=config.DUCKDB_THREADS,
        load_spatial=True, tmp_dir=config.DUCKDB_TMP_DIR,
    )
    if not force and checkpoint_ok(out_path, con):
        log.info("[B0] Checkpoint existant : %s (%d lignes) — SKIP.", out_path, count_parquet(con, str(out_path)))
        return out_path

    imb_glob = str(base / config.RAW_IMB_GLOB)
    zonage_path = base / config.RAW_ZONAGE_PARQUET
    zapm_gpkg = base / config.RAW_ZAPM_GPKG
    zapm_parquet = base / config.RAW_ZAPM_PARQUET_FALLBACK
    ban_parquet = base / config.RAW_BAN_PARQUET
    bdtopo_gpkg = base / config.RAW_BDTOPO_GPKG

    if not zonage_path.exists():
        raise FileNotFoundError(f"[B0] Referentiel zonage introuvable : {zonage_path}")
    retained = ", ".join(f"'{z}'" for z in config.RETAINED_ZONES)
    t_max, t_cens = config.T_MAX, config.T_CENSORE_CODE

    # --- Vue IMB normalisee ---
    build_imb_union_view(con, imb_glob)

    # --- ZAPM hors ZTD : GPKG reel si present, sinon fallback parquet ---
    use_spatial = False
    if zapm_gpkg.exists():
        try:
            con.execute(f"SELECT COUNT(*) FROM ST_Read('{zapm_gpkg}') LIMIT 1").fetchone()
            use_spatial = True
            log.info("[B0] Contours ZAPM GPKG detectes : %s", zapm_gpkg)
        except Exception as e:
            log.warning("[B0] ST_Read GPKG impossible (%s) -> fallback parquet.", e)

    if use_spatial:
        zapm_cte = f"""
        zapm_hors_ztd AS (
          SELECT z.ref_pm, z.operateur_infra, z.code_insee_pm,
                 ref.zone_reglementaire,
                 z.geom_polygon AS geom_zapm, z.geom_point_pm AS geom_pm
          FROM ST_Read('{zapm_gpkg}') z
          INNER JOIN read_parquet('{zonage_path}') ref
            ON z.code_insee_pm = ref.code_insee
          WHERE ref.zone_reglementaire IN ({retained})
        )"""
        if bdtopo_gpkg.exists():
            bat_src = f"ST_Read('{bdtopo_gpkg}')"
            bat_select = f"""
            batiments_affectes AS (
              SELECT b.id_ban, b.code_insee,
                     CAST(COALESCE(b.nb_locaux_ban, b.nb_logements, 1) AS INTEGER) AS nb_locaux_init,
                     z.ref_pm AS ref_pm_geo, z.operateur_infra, z.zone_reglementaire,
                     ST_Distance(ST_Centroid(b.geom), z.geom_pm) AS dist_pm_m
              FROM {bat_src} b
              INNER JOIN zapm_hors_ztd z
                ON ST_Intersects(ST_Centroid(b.geom), z.geom_zapm)
            )"""
        elif ban_parquet.exists():
            # BAN avec x/y mais sans geom : on reconstruit le point.
            bat_select = f"""
            batiments_affectes AS (
              SELECT b.id_ban, b.code_insee,
                     CAST(COALESCE(b.nb_locaux, 1) AS INTEGER) AS nb_locaux_init,
                     z.ref_pm AS ref_pm_geo, z.operateur_infra, z.zone_reglementaire,
                     ST_Distance(ST_Point(b.x_l93, b.y_l93), z.geom_pm) AS dist_pm_m
              FROM read_parquet('{ban_parquet}') b
              INNER JOIN zapm_hors_ztd z
                ON ST_Intersects(ST_Point(b.x_l93, b.y_l93), z.geom_zapm)
              WHERE b.x_l93 IS NOT NULL AND b.y_l93 IS NOT NULL
            )"""
        else:
            raise FileNotFoundError("[B0] Ni BD TOPO GPKG ni BAN parquet trouves.")
    else:
        # --- Mode degrade (demo / hors-ligne) : pas de jointure spatiale ---
        if zapm_parquet.exists() and ban_parquet.exists():
            zapm_cte = f"""
            zapm_hors_ztd AS (
              SELECT z.ref_pm, z.operateur_infra, z.code_insee_pm,
                     ref.zone_reglementaire, z.x_pm_l93, z.y_pm_l93
              FROM read_parquet('{zapm_parquet}') z
              INNER JOIN read_parquet('{zonage_path}') ref
                ON z.code_insee_pm = ref.code_insee
              WHERE ref.zone_reglementaire IN ({retained})
            )"""
            ban_cols = [c.lower() for c in parquet_columns(con, str(ban_parquet))]
            if "ref_pm_geo" in ban_cols:
                bat_select = f"""
                batiments_affectes AS (
                  SELECT b.id_ban, b.code_insee,
                         CAST(COALESCE(b.nb_locaux, 1) AS INTEGER) AS nb_locaux_init,
                         b.ref_pm_geo AS ref_pm_geo,
                         COALESCE(z.operateur_infra, 'NON_RENSEIGNE') AS operateur_infra,
                         COALESCE(z.zone_reglementaire, 'C_RIP') AS zone_reglementaire,
                         CAST(COALESCE(b.dist_pm_m,
                           SQRT(POWER(COALESCE(b.x_l93, 0) - COALESCE(z.x_pm_l93, 0), 2)
                                + POWER(COALESCE(b.y_l93, 0) - COALESCE(z.y_pm_l93, 0), 2)), 0) AS DOUBLE) AS dist_pm_m,
                         CAST(COALESCE(b.part_aerien_troncon, 0.0) AS DOUBLE) AS part_aerien_troncon,
                         CAST(COALESCE(b.dist_voisin_m, 25.0) AS DOUBLE) AS dist_voisin_m
                  FROM read_parquet('{ban_parquet}') b
                  LEFT JOIN zapm_hors_ztd z ON b.ref_pm_geo = z.ref_pm
                )"""
            else:
                # BAN sans affectation : rattachement via le ref_pm declare Arcep (LAST).
                bat_select = f"""
                dernier_pm_arcep AS (
                  SELECT id_ban, LAST(ref_pm ORDER BY trimestre) AS ref_pm_last
                  FROM imb_union WHERE ref_pm IS NOT NULL GROUP BY id_ban
                ),
                batiments_affectes AS (
                  SELECT b.id_ban, b.code_insee,
                         CAST(COALESCE(b.nb_locaux, 1) AS INTEGER) AS nb_locaux_init,
                         d.ref_pm_last AS ref_pm_geo,
                         COALESCE(z.operateur_infra, 'NON_RENSEIGNE') AS operateur_infra,
                         COALESCE(z.zone_reglementaire, 'C_RIP') AS zone_reglementaire,
                         0.0 AS dist_pm_m, 0.0 AS part_aerien_troncon, 25.0 AS dist_voisin_m
                  FROM read_parquet('{ban_parquet}') b
                  LEFT JOIN dernier_pm_arcep d ON b.id_ban = d.id_ban
                  LEFT JOIN zapm_hors_ztd z ON d.ref_pm_last = z.ref_pm
                  WHERE d.ref_pm_last IS NOT NULL
                )"""
        else:
            # Chemin donnees reelles : les millesimes IPE portent deja pm_ref.
            # La zone vient du referentiel communal (commune du PM, sinon du batiment).
            # La distance est euclidienne en Lambert-93 vers le centroide ZAPM.
            log.info("[B0] Chemin IPE Arcep (pm_ref + zonage communal, hors ZTD).")
            if zapm_parquet.exists():
                zapm_cte = f"""
                zapm_hors_ztd AS (
                  SELECT z.ref_pm, z.operateur_infra, z.code_insee_pm,
                         ref.zone_reglementaire AS zone_pm,
                         z.x_pm_l93, z.y_pm_l93
                  FROM read_parquet('{zapm_parquet}') z
                  LEFT JOIN read_parquet('{zonage_path}') ref
                    ON z.code_insee_pm = ref.code_insee
                )"""
                dist_sql = """CASE WHEN b.x_l93 IS NOT NULL AND z.x_pm_l93 IS NOT NULL
                    THEN SQRT(POWER(b.x_l93 - z.x_pm_l93, 2) + POWER(b.y_l93 - z.y_pm_l93, 2))
                    ELSE 0.0 END"""
                zapm_join = "LEFT JOIN zapm_hors_ztd z ON b.ref_pm_geo = z.ref_pm"
                zone_sql = "COALESCE(z.zone_pm, ref.zone_reglementaire, 'INCONNU')"
                oi_sql = "COALESCE(NULLIF(b.operateur_imb, ''), z.operateur_infra, 'NON_RENSEIGNE')"
            else:
                zapm_cte = "zapm_hors_ztd AS (SELECT NULL::VARCHAR AS ref_pm WHERE FALSE)"
                dist_sql = "0.0"
                zapm_join = ""
                zone_sql = "COALESCE(ref.zone_reglementaire, 'INCONNU')"
                oi_sql = "COALESCE(NULLIF(b.operateur_imb, ''), 'NON_RENSEIGNE')"
            bat_select = f"""
            bat_last AS (
              SELECT id_ban,
                LAST(code_insee ORDER BY trimestre) AS code_insee,
                CAST(COALESCE(MAX(nb_locaux), 1) AS INTEGER) AS nb_locaux_init,
                LAST(ref_pm ORDER BY trimestre) AS ref_pm_geo,
                LAST(operateur_infra ORDER BY trimestre) AS operateur_imb,
                LAST(x_l93 ORDER BY trimestre) AS x_l93,
                LAST(y_l93 ORDER BY trimestre) AS y_l93
              FROM imb_union
              WHERE id_ban IS NOT NULL AND ref_pm IS NOT NULL
              GROUP BY id_ban
            ),
            batiments_affectes AS (
              SELECT b.id_ban, b.code_insee, b.nb_locaux_init, b.ref_pm_geo,
                {oi_sql} AS operateur_infra,
                {zone_sql} AS zone_reglementaire,
                CAST({dist_sql} AS DOUBLE) AS dist_pm_m,
                0.0 AS part_aerien_troncon,
                25.0 AS dist_voisin_m
              FROM bat_last b
              {zapm_join}
              LEFT JOIN read_parquet('{zonage_path}') ref ON b.code_insee = ref.code_insee
              WHERE {zone_sql} IN ({retained})
            )"""

    has_extra_cols = "part_aerien_troncon" in bat_select

    query = f"""
    WITH {zapm_cte},
    {bat_select},
    historique AS (
      SELECT id_ban,
        LAST(id_imb ORDER BY trimestre) AS id_imb_arcep,
        LAST(ref_pm ORDER BY trimestre) AS ref_pm_arcep_final,
        COUNT(DISTINCT ref_pm) AS n_pm_distincts,
        MAX(nb_locaux) AS nb_locaux_arcep,
        MIN(CASE WHEN etat_imb IN ('etudes','programme','en_cours','raccordable_demande','raccordable') THEN trimestre END) AS t_etudes,
        MIN(CASE WHEN etat_imb IN ('programme','en_cours','raccordable_demande','raccordable') THEN trimestre END) AS t_programme,
        MIN(CASE WHEN etat_imb IN ('en_cours','raccordable_demande','raccordable') THEN trimestre END) AS t_encours,
        MIN(CASE WHEN etat_imb = 'raccordable' THEN trimestre END) AS t_racc_obs,
        MAX(CASE WHEN etat_imb = 'raccordable_demande' THEN 1 ELSE 0 END) AS ever_rad,
        SUM(CASE WHEN etat_imb = 'raccordable_demande' THEN 1 ELSE 0 END) AS duree_en_rad_q,
        LAST(motif_blocage ORDER BY trimestre) AS motif_blocage_arcep
      FROM imb_union
      WHERE id_ban IS NOT NULL
      GROUP BY id_ban
    )
    SELECT
      g.id_ban AS id_ban,
      COALESCE(h.id_imb_arcep, 'NEANT*' || g.id_ban) AS id_imb,
      COALESCE(h.ref_pm_arcep_final, g.ref_pm_geo) AS ref_pm,
      g.code_insee AS code_insee,
      CASE WHEN SUBSTR(g.code_insee, 1, 2) IN ('97','98') THEN SUBSTR(g.code_insee, 1, 3)
           ELSE SUBSTR(g.code_insee, 1, 2) END AS code_dept,
      g.zone_reglementaire AS zone_reglementaire,
      COALESCE(NULLIF(h.nb_locaux_arcep, 0), g.nb_locaux_init, 1) AS nb_locaux,
      h.t_etudes AS t_etudes, h.t_programme AS t_programme, h.t_encours AS t_encours,
      COALESCE(h.t_racc_obs, {t_cens}) AS t_raccordable,
      CASE WHEN COALESCE(h.t_racc_obs, {t_cens}) = {t_cens} THEN 1 ELSE 0 END AS is_right_censored,
      COALESCE(h.ever_rad, 0) AS ever_rad,
      COALESCE(h.duree_en_rad_q, 0) AS duree_en_rad_q,
      COALESCE(h.motif_blocage_arcep, 'AUCUN') AS motif_blocage_arcep,
      g.operateur_infra AS operateur_infra,
      CAST(COALESCE(g.dist_pm_m, 0) AS DOUBLE) AS dist_pm_m,
      CASE WHEN COALESCE(h.n_pm_distincts, 1) > 1 THEN 1 ELSE 0 END AS has_pm_reconfigured
      {", CAST(COALESCE(g.part_aerien_troncon, 0.0) AS DOUBLE) AS part_aerien_troncon, CAST(COALESCE(g.dist_voisin_m, 25.0) AS DOUBLE) AS dist_voisin_m" if has_extra_cols else ", 0.0 AS part_aerien_troncon, 25.0 AS dist_voisin_m"}
    FROM batiments_affectes g
    LEFT JOIN historique h ON g.id_ban = h.id_ban
    WHERE COALESCE(h.ref_pm_arcep_final, g.ref_pm_geo) IS NOT NULL
    """
    write_copy_parquet(con, query, out_path)
    n = count_parquet(con, str(out_path))
    log.info("[B0] Termine : %d batiments -> %s", n, out_path)
    con.close()
    return out_path


def validate_brique0_output(path: str | Path) -> dict:
    """Controles d'integrite de la sortie Brique 0 (cles, jalons monotones, ZTD)."""
    import duckdb

    con = duckdb.connect(":memory:")
    p = str(path)
    checks: dict = {}
    checks["n_lignes"] = con.execute(f"SELECT COUNT(*) FROM read_parquet('{p}')").fetchone()[0]
    checks["n_batiments_uniques"] = con.execute(f"SELECT COUNT(DISTINCT id_ban) FROM read_parquet('{p}')").fetchone()[0]
    checks["n_zapm"] = con.execute(f"SELECT COUNT(DISTINCT ref_pm) FROM read_parquet('{p}')").fetchone()[0]
    checks["part_censures"] = con.execute(f"SELECT AVG(is_right_censored) FROM read_parquet('{p}')").fetchone()[0]
    checks["jalons_non_monotones"] = con.execute(
        f"""SELECT COUNT(*) FROM read_parquet('{p}')
        WHERE (t_programme IS NOT NULL AND t_etudes IS NOT NULL AND t_programme < t_etudes)
           OR (t_encours IS NOT NULL AND t_programme IS NOT NULL AND t_encours < t_programme)"""
    ).fetchone()[0]
    checks["zones"] = con.execute(f"SELECT DISTINCT zone_reglementaire FROM read_parquet('{p}')").fetchall()
    con.close()
    return checks
