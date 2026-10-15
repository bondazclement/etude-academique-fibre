"""Generateur de donnees synthetiques pour demo, tests et developpement hors-ligne.

Reproduit la structure exacte des 3 sources brutes de la Brique 0 :
  A. batiments physiques (BAN/BD TOPO) : id_ban, code_insee, x/y, nb_locaux
  B. ZAPM/PM (Arcep SIG) : ref_pm, operateur, polygone (approx. centroide + rayon),
     zone reglementaire
  C. 32 millesimes IMB trimestriels (Arcep) : etat_imb par trimestre + motif blocage

puis la cinematique realiste d'une ZAPM de rue :
  - cohorte modale d'ouverture du PM (75-90 % basculent ensemble a t0 + M),
  - longue traine de completude (Q1 blocages exogenes, Q2 inefficiences OI),
  - declarations preventives (Q3 faux RAD).

Le generateur ecrit directement les fichiers attendus par la Brique 0 dans
``data/raw_data/...`` ainsi que les features terrain pour la Brique 3.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from .config import FTTHConfig, trimestre_label
from .io_utils import ensure_dirs, get_logger

OPERATEURS_DEMO = ["Orange", "XpFibre", "Altitude", "Axione", "TDF", "Covage"]
ZONES_DEMO = ["B_AMII", "B_AMEL", "C_RIP"]


def _rng(seed: int) -> np.random.Generator:
    return np.random.default_rng(seed)


def generate_reference_communes(
    out_path: str | Path,
    n_communes: int = 60,
    seed: int = 42,
    dept_prefix: str = "01",
) -> pd.DataFrame:
    """Referentiel communal synthetique (zonage B/C uniquement, hors ZTD)."""
    rng = _rng(seed)
    codes = [f"{dept_prefix}{str(i).zfill(3)}" for i in range(1, n_communes + 1)]
    zones = rng.choice(ZONES_DEMO, size=n_communes, p=[0.45, 0.20, 0.35])
    df = pd.DataFrame(
        {
            "code_insee": codes,
            "nom_commune": [f"Commune-demo-{c}" for c in codes],
            "zone_reglementaire": zones,
            "operateur_amii_amel": np.where(
                zones == "C_RIP", "RIP", rng.choice(OPERATEURS_DEMO, size=n_communes)
            ),
            "statut_zone_fibree": rng.choice(["fibree", "en_cours"], size=n_communes, p=[0.7, 0.3]),
        }
    )
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, compression="zstd", index=False)
    return df


def generate_zapm_pm(
    out_path: str | Path,
    communes: pd.DataFrame,
    n_zapm: int = 40,
    seed: int = 42,
) -> pd.DataFrame:
    """Table ZAPM/PM synthetique (fallback parquet de contours_zapm.gpkg)."""
    rng = _rng(seed + 1)
    rows = []
    # Centroide Lambert-93 fictif autour de Bourg-en-Bresse (dept 01).
    x0, y0 = 860_000.0, 6_570_000.0
    for i in range(n_zapm):
        com = communes.iloc[rng.integers(0, len(communes))]
        zone = com["zone_reglementaire"]
        if zone == "C_RIP":
            oi = str(rng.choice(["Altitude", "Axione", "TDF", "Covage"]))
        else:
            oi = str(rng.choice(["Orange", "XpFibre", "Altitude"]))
        cx = x0 + rng.normal(0, 25_000)
        cy = y0 + rng.normal(0, 25_000)
        rows.append(
            {
                "ref_pm": f"PM-{str(i + 1).zfill(5)}",
                "operateur_infra": oi,
                "code_insee_pm": com["code_insee"],
                "zone_reglementaire": zone,
                "x_pm_l93": cx,
                "y_pm_l93": cy,
                # Rayon de desserte ~ 300-1200 m (ZAPM de rue).
                "rayon_zapm_m": float(rng.uniform(300, 1200)),
                # Features terrain ZAPM (Brique 3 / Test 2).
                "lineaire_voirie_m": float(rng.uniform(2_000, 25_000)),
                "ratio_aerien_zapm": float(rng.beta(2, 5)),
                "pente_moyenne_deg": float(np.abs(rng.normal(2.5, 2.0))),
            }
        )
    df = pd.DataFrame(rows)
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, compression="zstd", index=False)
    return df


def _etat_at(t: int, t_etudes, t_programme, t_encours, t_rad: int | None, t_racc) -> str:
    """Etat IPE d'un batiment au trimestre t (regle des jalons monotones)."""
    if t_etudes is None or t < t_etudes:
        return "neant"
    if t_racc is not None and t >= t_racc:
        return "raccordable"
    if t_rad is not None and t >= t_rad:
        return "raccordable_demande"
    if t_encours is not None and t >= t_encours:
        return "en_cours"
    if t_programme is not None and t >= t_programme:
        return "programme"
    return "etudes"


def generate_demo_dataset(
    config: FTTHConfig | None = None,
    base_dir: str | Path = "data",
    n_zapm: int = 40,
    batiments_par_zapm: tuple[int, int] = (60, 220),
    seed: int = 42,
    taux_q1: float = 0.06,
    taux_q2: float = 0.08,
    taux_q3: float = 0.04,
) -> dict[str, Path]:
    """Genere un jeu demo complet et l'ecrit dans ``base_dir/raw_data/...``.

    Retourne les chemins des artefacts generes. Idempotent (reecrit).
    """
    from .config import FTTHConfig as _C

    config = config or _C(BASE_DIR=Path(base_dir))
    log = get_logger()
    rng = _rng(seed)
    base = Path(base_dir)

    ensure_dirs(
        base / "raw_data/01_arcep_imb_trimestriels",
        base / "raw_data/02_arcep_sig_zapm_pm",
        base / "raw_data/03_arcep_zonage_ref",
        base / "raw_data/04_ign_bdtopo_ban",
        base / "raw_data/05_enedis_odre_aerien",
        base / config.PROC_STEP0_DIR,
        base / config.PROC_STEP1_DIR,
        base / config.PROC_STEP2_DIR,
        base / config.PROC_STEP3_DIR,
        base / config.OUTPUTS_EXCEL_DIR,
    )

    # --- Communes + ZAPM ---
    communes_path = base / config.RAW_ZONAGE_PARQUET
    communes = generate_reference_communes(communes_path, n_communes=max(30, n_zapm), seed=seed)
    zapm_path = base / config.RAW_ZAPM_PARQUET_FALLBACK
    zapm = generate_zapm_pm(zapm_path, communes, n_zapm=n_zapm, seed=seed)

    # --- Batiments physiques ---
    bat_rows: list[dict] = []
    # Plan cinematique par batiment (pour generer les millesimes) : indexe par id_ban.
    plan: dict[str, dict] = {}
    motif_pool_q1 = ["SYNDIC_AG", "REFUS_TIERS", "VOIRIE_ENEDIS", "SERVITUDE", "FOURREAU_SATURE"]

    for _, z in zapm.iterrows():
        n_bat = int(rng.integers(batiments_par_zapm[0], batiments_par_zapm[1] + 1))
        # Lancement ZAPM : t0 uniforme 1..18 (pour laisser de la traine jusqu'a 32).
        t0 = int(rng.integers(1, 19))
        # Duree modale d'ouverture du PM : 1..4 trimestres (gel 3 mois + PBO).
        m_modal = int(rng.integers(1, 5))
        rayon = float(z["rayon_zapm_m"])
        for b in range(n_bat):
            id_ban = f"{z['ref_pm']}-BAN-{b:04d}"
            # Position : disque autour du PM (densite decroissante).
            r = rayon * float(np.sqrt(rng.uniform(0, 1)))
            theta = float(rng.uniform(0, 2 * np.pi))
            x = float(z["x_pm_l93"] + r * np.cos(theta))
            y = float(z["y_pm_l93"] + r * np.sin(theta))
            dist_pm = float(np.hypot(x - z["x_pm_l93"], y - z["y_pm_l93"]))
            # Habitat : 55 % individuel (<=2 locaux), sinon collectif.
            if rng.uniform() < 0.55:
                nb_locaux = int(rng.choice([1, 1, 1, 2]))
            else:
                nb_locaux = int(rng.choice([3, 4, 5, 6, 8, 12, 20, 30]))
            # Tirage du quadrant cible (verite terrain du generateur).
            u = rng.uniform()
            if u < taux_q1:
                profil = "Q1"
            elif u < taux_q1 + taux_q2:
                profil = "Q2"
            elif u < taux_q1 + taux_q2 + taux_q3:
                profil = "Q3"
            else:
                profil = "Q4"
            # Jalons.
            t_etudes = t0 + int(rng.integers(0, 2))  # 0-1 trim. de decalage de prise en compte
            t_programme = t_etudes + int(rng.integers(0, 2))
            t_encours = t_programme + int(rng.integers(0, 2))
            if profil in ("Q4", "Q3"):
                # Cohorte modale : raccordable a t0 + M (+/- 1).
                t_racc = t0 + m_modal + int(rng.integers(-1, 2))
                t_racc = max(t_racc, t_encours)
                t_rad = None
                motif = "AUCUN"
                if profil == "Q3":
                    # Faux RAD preventif : passage bref en RAD juste avant raccordable.
                    t_rad = max(t_encours, t_racc - 1)
                    motif = "RAD_PREVENTIF"
            elif profil == "Q1":
                # Blocage exogene : longue traine + motif declare + RAD durable.
                extra = int(rng.integers(5, 14))
                t_racc = t0 + m_modal + extra
                t_rad = t0 + m_modal + int(rng.integers(1, 3))
                motif = str(rng.choice(motif_pool_q1))
                # 25 % des Q1 restent censes a droite (jamais raccordables a T32).
                if rng.uniform() < 0.25:
                    t_racc = 99
            else:  # Q2
                extra = int(rng.integers(4, 13))
                t_racc = t0 + m_modal + extra
                t_rad = None
                motif = "AUCUN"
                if rng.uniform() < 0.20:
                    t_racc = 99
            # Censure : au-dela de T_MAX -> 99.
            if t_racc != 99 and t_racc > config.T_MAX:
                t_racc = 99
            # Monotonie des jalons (un jalon ne peut depasser le suivant observe).
            t_lim = config.T_MAX if t_racc == 99 else t_racc
            t_encours = min(t_encours, t_lim)
            t_programme = min(t_programme, t_encours)
            t_etudes = min(t_etudes, t_programme)
            # Features batiment (Brique 3 / Test 1).
            part_aerien = float(min(1.0, max(0.0, rng.beta(2, 6) + (0.25 if r > rayon * 0.7 else 0.0))))
            dist_voisin = float(rng.uniform(5, 120) if nb_locaux >= 3 else rng.uniform(10, 250))

            # Commune du batiment : celle du PM dans 85 % des cas (ZAPM a cheval sinon).
            if rng.uniform() < 0.85:
                code_insee = z["code_insee_pm"]
            else:
                code_insee = str(communes.iloc[rng.integers(0, len(communes))]["code_insee"])

            bat_rows.append(
                {
                    "id_ban": id_ban,
                    "id_imb": f"IMB-{id_ban}",
                    "code_insee": code_insee,
                    "x_l93": x,
                    "y_l93": y,
                    "nb_locaux": nb_locaux,
                    "ref_pm_geo": z["ref_pm"],
                    "dist_pm_m": dist_pm,
                    "part_aerien_troncon": part_aerien,
                    "dist_voisin_m": dist_voisin,
                    "profil_verite": profil,  # colonne DEBUG (verite generateur, ignoree par B0-B2)
                }
            )
            plan[id_ban] = {
                "t_etudes": t_etudes,
                "t_programme": t_programme,
                "t_encours": t_encours,
                "t_rad": t_rad,
                "t_racc": t_racc,
                "motif": motif,
                "ref_pm": z["ref_pm"],
                "code_insee": code_insee,
                "nb_locaux": nb_locaux,
                "x": x,
                "y": y,
                "operateur_infra": z["operateur_infra"],
            }

    ban_df = pd.DataFrame(bat_rows)
    ban_path = base / config.RAW_BAN_PARQUET
    ban_df.to_parquet(ban_path, compression="zstd", index=False)
    log.info("BAN synthetique : %d batiments -> %s", len(ban_df), ban_path)

    # --- Millesimes trimestriels IMB (format long : 1 ligne = 1 batiment present a t) ---
    # Un batiment absent a t < t_etudes (troncature a gauche : etat Neant).
    imb_dir = base / "raw_data/01_arcep_imb_trimestriels"
    for t in range(config.T_MIN, config.T_MAX + 1):
        rows = []
        for id_ban, p in plan.items():
            etat = _etat_at(t, p["t_etudes"], p["t_programme"], p["t_encours"], p["t_rad"], p["t_racc"] if p["t_racc"] != 99 else None)
            if etat == "neant":
                continue  # troncature a gauche : absent du fichier Arcep
            # Reconfiguration PM simulée : 1 % des batiments changent de ref_pm a mi-parcours.
            ref_pm = p["ref_pm"]
            rows.append(
                {
                    "id_imb": f"IMB-{id_ban}",
                    "id_ban": id_ban,
                    "code_insee": p["code_insee"],
                    "ref_pm": ref_pm,
                    "operateur_infra": p["operateur_infra"],
                    "etat_imb": etat,
                    "nb_locaux": p["nb_locaux"],
                    "x_l93": p["x"],
                    "y_l93": p["y"],
                    "motif_blocage": p["motif"] if (etat == "raccordable_demande" or p["motif"] not in ("AUCUN",)) else "AUCUN",
                    "trimestre": t,
                    "trimestre_label": trimestre_label(t),
                }
            )
        df_t = pd.DataFrame(rows)
        out_t = imb_dir / f"imb_t{t:02d}.parquet"
        df_t.to_parquet(out_t, compression="zstd", index=False)
    log.info("Millesimes synthetiques T01..T%02d -> %s", config.T_MAX, imb_dir)

    # --- Enedis BT par commune (ratio aerien) ---
    enedis_rows = []
    for _, c in communes.iterrows():
        aer = float(np.clip(_rng(seed + 7).beta(2, 5), 0, 1))
        tot = float(_rng(seed + 8).uniform(5_000, 80_000))
        enedis_rows.append(
            {
                "code_insee": c["code_insee"],
                "longueur_bt_aerien_m": aer * tot,
                "longueur_bt_souterrain_m": (1 - aer) * tot,
                "longueur_bt_total_m": tot,
                "ratio_aerien_commune": aer,
            }
        )
    enedis_df = pd.DataFrame(enedis_rows)
    enedis_path = base / config.RAW_ENEDIS_PARQUET
    enedis_df.to_parquet(enedis_path, compression="zstd", index=False)

    return {
        "communes": communes_path,
        "zapm": zapm_path,
        "ban": ban_path,
        "imb_dir": imb_dir,
        "enedis": enedis_path,
    }


def generate_batiments_cycles_direct(
    out_path: str | Path,
    n_zapm: int = 25,
    seed: int = 123,
    config: FTTHConfig | None = None,
) -> pd.DataFrame:
    """Raccourci pour tests rapides : genere directement la sortie Brique 0.

    Evite l'ETL spatial complet en simulant les trimestres de jalons
    (t_etudes, t_programme, t_encours, t_raccordable) par batiment.
    """
    from .config import FTTHConfig as _C

    config = config or _C()
    rng = _rng(seed)
    rows = []
    for i in range(n_zapm):
        ref_pm = f"PM-{str(i + 1).zfill(5)}"
        oi = str(rng.choice(OPERATEURS_DEMO))
        zone = str(rng.choice(ZONES_DEMO))
        dept = f"{rng.integers(1, 96):02d}"
        n_bat = int(rng.integers(40, 150))
        t0 = int(rng.integers(1, 15))
        m_modal = int(rng.integers(1, 4))
        for b in range(n_bat):
            u = rng.uniform()
            is_anom = u < 0.14
            has_bloc = (u < 0.06)
            t_etudes = t0 + int(rng.integers(0, 2))
            t_prog = t_etudes + int(rng.integers(0, 2))
            t_enc = t_prog + int(rng.integers(0, 2))
            if is_anom:
                t_racc = t0 + m_modal + int(rng.integers(4, 12))
            else:
                t_racc = t0 + m_modal + int(rng.integers(-1, 2))
            if rng.uniform() < 0.03:
                t_racc = 99
            elif t_racc > config.T_MAX:
                t_racc = 99
            t_racc = max(t_racc, t_enc) if t_racc != 99 else 99
            nb_loc = int(rng.choice([1, 1, 2, 3, 5, 10]))
            rows.append(
                {
                    "id_ban": f"{ref_pm}-BAN-{b:04d}",
                    "id_imb": f"IMB-{ref_pm}-BAN-{b:04d}",
                    "ref_pm": ref_pm,
                    "code_insee": f"{dept}{rng.integers(1, 400):03d}",
                    "code_dept": dept,
                    "zone_reglementaire": zone,
                    "operateur_infra": oi,
                    "nb_locaux": nb_loc,
                    "t_etudes": t_etudes,
                    "t_programme": t_prog,
                    "t_encours": t_enc,
                    "t_raccordable": t_racc,
                    "is_right_censored": 1 if t_racc == 99 else 0,
                    "ever_rad": 1 if (has_bloc or (u > 0.90 and u < 0.94)) else 0,
                    "duree_en_rad_q": int(rng.integers(1, 5)) if has_bloc else 0,
                    "dist_pm_m": float(rng.uniform(15, 1500)),
                    "part_aerien_troncon": float(rng.beta(2, 6)),
                    "dist_voisin_m": float(rng.uniform(5, 150)),
                    "has_pm_reconfigured": 0,
                    "motif_blocage_arcep": "SYNDIC_AG" if has_bloc else "AUCUN",
                }
            )
    df = pd.DataFrame(rows)
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, compression="zstd", index=False)
    return df
