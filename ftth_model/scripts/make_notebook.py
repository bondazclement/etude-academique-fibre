"""Generateur du notebook unique FTTH_Model_Pipeline_Complet.ipynb."""
import json
from pathlib import Path

def md(source: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": source}

def code(source: str) -> dict:
    return {"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": source}

cells = []

# ---------------------------------------------------------------- Titre
cells.append(md("""# FTTH — Pipeline complet (Zones B & C, hors ZTD)\n**Notebook unique de préparation, lancement et pilotage** : Brique 0 (ETL 3 sources) → Brique 1 (Pré-Quadrant cinématique) → Brique 2 (Post-Quadrant Q1–Q4) → Brique 3 (Économétrie Quadrant 2).\n\n> Périmètre : hors Zones Très Denses (Zone A exclue). Zones B (AMII/AMEL, privée L. 33-13) et C (RIP, publique). Trimestres `t = 1..32` (2018_T2 → 2026_T2). Maille **ZAPM subdivisée au bâtiment** (`ref_pm` / `id_ban`), indépendante des frontières communales.\n\n**Lecture conseillée** : exécuter de haut en bas. Chaque brique écrit un **checkpoint `.parquet` ZSTD** reprise-sur-panne ; les cellules lourdes sont idempotentes (`SKIP` si le checkpoint existe, `FORCE = True` pour forcer).\n\n**Sommaire**\n- §0 — Configuration, paramètres globaux, DuckDB/Polars, données (démo vs réelles)\n- **Brique 0** — ETL géomatique & temporel (fusion 3 sources → `batiments_cycles.parquet`)\n- **Brique 1** — Moteur Pré-Quadrant (cinématique ZAPM, Z-score robuste, anomalies) + Sorties 1A (Excel audit) / 1B (flux Parquet)\n- **Brique 2** — Moteur Post-Quadrant (recoupement Arcep → Q1/Q2/Q3/Q4)\n- **Brique 3** — Économétrie Quadrant 2 (Test 1 intra-ZAPM, Test 2A résilience, Test 2B SFA)\n- Conclusion — limites, reproductibilité, checkpoints\n"""))

cells.append(md("""## Pré-requis & environnement\n\n- **Matériel visé** : poste local ou VPS Linux, 16–20 Go RAM, SSD. DuckDB plafonné via `PRAGMA memory_limit='14GB'`, traitement **out-of-core** (jamais 750 M lignes en RAM).\n- **Stack** : DuckDB ≥ 1.0 (+ extension `spatial`), Polars + PyArrow, `xlsxwriter`/`openpyxl`, `scikit-learn`/`scipy`, `pyfixest` (+ repli `statsmodels`), `matplotlib`.\n- **Données réelles** (optionnel, §0.4) : 32 millésimes IMB Arcep, contours ZAPM/PM, zonage communes, BAN/BD TOPO, Enedis BT, (optionnel) BD ALTI + INSEE. Voir tableau §0.4 et `python -m ftth.data_acquisition`.\n- **Mode démo** (défaut) : jeu synthétique réaliste généré localement, **aucun téléchargement**, cinématique ZAPM de rue (cohorte modale PM + longue traîne Q1/Q2/Q3) avec vérité terrain pour valider la détection.\n\n```bash\npip install -r requirements.txt\n```"""))

# ---------------------------------------------------------------- §0
cells.append(md("""## §0 — Configuration, paramètres globaux & initialisation\n\n| Paramètre | Défaut | Rôle |\n|---|---|---|\n| `T_MIN / T_MAX` | 1 / 32 | 2018_T2 → 2026_T2 (trimestrialité des arrêtés Arcep) |\n| `RETAINED_ZONES` | B_AMII, B_AMEL, C_RIP | hors ZTD : 100 % de ZAPM de rue, jalonnement obligatoire (Déc. 2010-1312, 2020-1432) |\n| `K_ROBUST_Z` | 2.0 | seuil $Z^{robuste} \\ge 2.0$ → anomalie |\n| `MIN_EXCESS_QUARTERS` | 4 | retard pur $\\ge 4$ trim. (≥ 1 an après la cohorte PM) → anomalie |\n| `POST_QUADRANT_MODE` | AUTO_MODEL | `AUTO_MODEL` (quadrants + économétrie) ou `MANUAL_EXCEL_EXPORT` (audit Excel seul) |\n| `DUCKDB_MEMORY / THREADS` | 14GB / 8 | plafond RAM, parallélisme |\n\nCode sentinelle `t_raccordable = 99` = jamais raccordable observé (censure à droite) ; valeur de calcul $T_{max}+1 = 33$."""))

cells.append(code('''# §0.1 — Imports, chemins, config modifiable ici
import sys
from pathlib import Path
ROOT = Path.cwd()
if (ROOT / "src").exists():
    sys.path.insert(0, str(ROOT / "src"))
elif (Path("..") / "src").exists():
    sys.path.insert(0, str((Path("..") / "src").resolve()))

from ftth.config import FTTHConfig, trimestre_label
from ftth.io_utils import ensure_dirs, get_duckdb_connection, get_logger

# ===== PARAMETRES A MODIFIER AU BESOIN =====
DEMO_MODE = True            # True = jeu synthetique local (aucun telechargement) ; False = donnees reelles
DEMO_N_ZAPM = 40            # taille du territoire demo (12 = smoke test, 40 = demo, 200+ = stress)
FORCE = False               # True = re-execute tout en ignorant les checkpoints
BASE_DIR = "data"           # racine des donnees (raw_data, processed_parquet, outputs_excel)
POST_QUADRANT_MODE = "AUTO_MODEL"  # ou "MANUAL_EXCEL_EXPORT"
K_ROBUST_Z = 2.0
MIN_EXCESS_QUARTERS = 4
# ===========================================

config = FTTHConfig(BASE_DIR=Path(BASE_DIR), POST_QUADRANT_MODE=POST_QUADRANT_MODE,
                    K_ROBUST_Z=K_ROBUST_Z, MIN_EXCESS_QUARTERS=MIN_EXCESS_QUARTERS)
log = get_logger()
print(f"T range : {config.T_MIN} ({trimestre_label(1)}) -> {config.T_MAX} ({trimestre_label(32)})")
print(f"Zones retenues : {config.RETAINED_ZONES} | exclues : {config.EXCLUDED_ZONES}")
print(f"Seuils : K={config.K_ROBUST_Z}, retard>={config.MIN_EXCESS_QUARTERS} trim. | Mode : {config.POST_QUADRANT_MODE}")
print(f"BASE_DIR = {config.BASE_DIR.resolve()}")
ensure_dirs(config.BASE_DIR / "raw_data", config.BASE_DIR / "processed_parquet", config.BASE_DIR / "outputs_excel")
'''))

cells.append(code('''# §0.2 — Verification DuckDB (+ spatial), versions
import duckdb
print("duckdb", duckdb.__version__)
con = get_duckdb_connection(memory=config.DUCKDB_MEMORY, threads=config.DUCKDB_THREADS, load_spatial=True)
try:
    con.execute("SELECT ST_Point(1,2);").fetchone()
    print("spatial : OK (jointure point-dans-polygone disponible)")
except Exception as e:
    print("spatial : INDISPONIBLE -> repli non-spatial (demo OK, reel GPKG necessite l'extension).", e)
try:
    import polars as pl; print("polars", pl.__version__)
except Exception as e: print("polars absent :", e)
try:
    import pyfixest; print("pyfixest OK")
except Exception: print("pyfixest absent -> repli statsmodels (OK en demo)")
con.close()
'''))

cells.append(md("""### §0.3 — Données démo (synthétiques) ou réelles ?\n\n- **Démo (`DEMO_MODE = True`)** : génère `reference_communes.parquet`, `zapm_pm.parquet` (fallback des contours GPKG), `ban_france.parquet`, `imb_t01..t32.parquet`, `enedis_bt_communes.parquet`. Cinématique : lancement ZAPM `t0 ∈ [1,18]`, cohorte modale d'ouverture PM `M ∈ [1,4]` (gel réglementaire 3 mois), ~6 % Q1 (traîne + motif + RAD durable, 25 % censurés), ~8 % Q2 (traîne sans motif, 20 % censurés), ~4 % Q3 (faux RAD préventif), reste Q4. Troncature à gauche reproduite (absent de l'IPE avant `t_etudes` = état Néant).\n- **Réel (`DEMO_MODE = False`)** : placez vos fichiers selon §0.4 puis exécutez B0 directement (le notebook vérifie les chemins)."""))

cells.append(code('''# §0.3 — Generation demo (idempotente) ou controle des donnees reelles
from pathlib import Path
if DEMO_MODE:
    from ftth.synthetic_generator import generate_demo_dataset
    paths = generate_demo_dataset(config=config, base_dir=BASE_DIR, n_zapm=DEMO_N_ZAPM, seed=config.RANDOM_STATE)
    for k, v in paths.items(): print(f"{k:10s} -> {v}")
else:
    base = Path(BASE_DIR)
    attendus = [config.RAW_IMB_GLOB, config.RAW_ZAPM_GPKG, config.RAW_ZAPM_PARQUET_FALLBACK,
                config.RAW_ZONAGE_PARQUET, config.RAW_BAN_PARQUET, config.RAW_ENEDIS_PARQUET]
    for rel in attendus:
        p = base / rel
        print(("OK   " if (p.exists() or ("*" in rel and list(base.glob(rel)))) else "MANQUANT "), rel)
'''))

cells.append(md("""### §0.4 — Données réelles : où les récupérer, où les poser\n\n| Base | URL | Destination locale |\n|---|---|---|\n| 32 millésimes IMB Arcep | https://data.arcep.fr/fixe/maconnexioninternet/ (`YYYY_TX/ftth/`) | `raw_data/01_arcep_imb_trimestriels/imb_tXX.parquet` + col. `trimestre` 1..32 |\n| Contours ZAPM + PM | dernier millésime, `cartographie/`/`contours_zapm/` | `raw_data/02_arcep_sig_zapm_pm/contours_zapm_ftth.gpkg` |\n| Zonage communes | https://www.data.gouv.fr/fr/datasets/le-marche-du-haut-et-tres-haut-debit-fixe-deploiements/ | `raw_data/03_arcep_zonage_ref/reference_communes.parquet` |\n| BAN | https://adresse.data.gouv.fr/data/ban/adresses/latest/csv/ | `raw_data/04_ign_bdtopo_ban/ban_france.parquet` |\n| BD TOPO (voirie) | https://geoservices.ign.fr/bdtopo | `raw_data/04_ign_bdtopo_ban/` |\n| Enedis BT | https://data.enedis.fr/ | `raw_data/05_enedis_odre_aerien/enedis_bt_communes.parquet` |\n| BD ALTI, INSEE RP/SIRENE (opt.) | voir README | `raw_data/06_insee_sirene_controle/` |\n\nColonnes IMB minimales : `id_imb, id_ban, code_insee, ref_pm, operateur_infra, etat_imb, nb_locaux, x_l93, y_l93` (+ `motif_blocage`, `trimestre`). La cellule suivante automatise BAN + Enedis + scan de l'index Arcep (idempotent ; tolère les 404)."""))

cells.append(code('''# §0.4 (optionnel) — Acquisition semi-automatique des bases publiques
# A executer uniquement en mode reel avec acces internet. Sans reseau : passez manuellement.
if not DEMO_MODE:
    from ftth.data_acquisition import ingest_public_databases
    print(ingest_public_databases(base_dir=BASE_DIR))
else:
    print("DEMO_MODE : acquisition reelle ignoree (donnees synthetiques deja generees).")
    print("Astuce : convert_arcep_csv_to_parquet(csv, imb_tXX.parquet, trimestre=t) pour vos CSV Arcep.")
'''))

# ---------------------------------------------------------------- Brique 0
cells.append(md("""## Brique 0 — ETL géomatique & temporel (fusion des 3 sources)\n\n**Pourquoi 3 sources ?** L'Arcep seul tronque à gauche (un bâtiment absent = encore à l'état **Néant = 0**, pas inexistant) ; la BAN/BD TOPO seule ignore la fibre (`ref_pm`, PM, états). La Brique 0 projette **tous** les bâtiments physiques dans leur ZAPM dès $t=0$ (état 0), puis pivote les 32 millésimes :\n\n$$t_{études}=\\min\\{t\\mid état\\in\\{études,programmé,en\\_cours,rad,raccordable\\}\\}$$\n$$t_{programme}=\\min\\{t\\mid état\\in\\{programmé,en\\_cours,rad,raccordable\\}\\}$$\n$$t_{encours}=\\min\\{t\\mid état\\in\\{en\\_cours,rad,raccordable\\}\\}$$\n$$t_{raccordable}=\\min\\{t\\mid état=raccordable\\}\\;(99\\text{ si jamais, }is\\_right\\_censored=1)$$\n$$ever\\_rad=\\max_t\\mathbb{I}(état=rad),\\;duree\\_en\\_rad=\\sum_t\\mathbb{I}(état=rad)$$\n\n**Règles** : filtrage hors ZTD (B/C uniquement) ; ancrage $ref\\_pm=LAST(ref\\_pm\\;ORDER\\;BY\\;t)$ + flag `has_pm_reconfigured` ; $ref\\_pm=COALESCE(arcep, géo)$ ; `code_dept` (97/98 → 3 car.) ; `dist_pm_m`.\n\nSortie : `processed_parquet/step0_etl/batiments_cycles.parquet` (1 ligne = 1 bâtiment)."""))

cells.append(code('''# B0.1 — Execution ETL (checkpoint ZSTD, SKIP si existant sauf FORCE=True)
from ftth.brique0_etl import run_brique0, validate_brique0_output
b0_path = run_brique0(config=config, force=FORCE)
print("Sortie B0 :", b0_path)
print(validate_brique0_output(b0_path))
'''))

cells.append(code('''# B0.2 — Inspection : schema, jalons, censure, zones, operateurs
import duckdb
con = duckdb.connect(":memory:")
print(con.execute(f"SELECT * FROM read_parquet('{b0_path}') LIMIT 3").df().T)
print(con.execute(f"""SELECT zone_reglementaire, COUNT(*) n, AVG(is_right_censored) part_censuree,
  AVG(ever_rad) part_rad, COUNT(DISTINCT ref_pm) zapm, COUNT(DISTINCT operateur_infra) oi
  FROM read_parquet('{b0_path}') GROUP BY 1 ORDER BY 1""").df())
print(con.execute(f"""SELECT operateur_infra, COUNT(*) n FROM read_parquet('{b0_path}')
  GROUP BY 1 ORDER BY 2 DESC LIMIT 10""").df())
con.close()
'''))

cells.append(code('''# B0.3 (visuel) — Distribution des jalons + part censures
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
df0 = pd.read_parquet(b0_path, columns=["t_etudes","t_encours","t_raccordable","is_right_censored","nb_locaux"])
fig, ax = plt.subplots(1, 2, figsize=(11, 3.5))
df0[df0.t_etudes.notna()].t_etudes.hist(bins=32, ax=ax[0]); ax[0].set_title("t_etudes (entree IPE)"); ax[0].set_xlabel("trimestre t")
df0.t_raccordable.replace(99, 33).hist(bins=33, ax=ax[1]); ax[1].set_title("t_raccordable (33 = censure 99)"); ax[1].set_xlabel("trimestre t")
plt.tight_layout(); plt.savefig("b0_jalons.png", dpi=110); print("b0_jalons.png | part censures =", round(float(df0.is_right_censored.mean()), 4))
'''))

# ---------------------------------------------------------------- Brique 1
cells.append(md("""## Brique 1 — Moteur Pré-Quadrant (cinématique ZAPM, Z-score robuste)\n\nPour chaque ZAPM $i$ : $t_{0,i}^{ZAPM}=\\min_{j\\in i}(t_{études,j})$. Pour chaque bâtiment $j$ :\n\n$$D_{j,i}=\\min(t_{racc,j},33)-t_{0,i},\\;D^{(0\\to1)},D^{(1\\to3)},D^{(3\\to4)}\\text{ (localisation du ralentissement)}$$\n$$M_i=Médiane(D),\\;MAD_i=Médiane(|D-M_i|),\\;RetardPur=D-M_i,\\;Z^{robuste}=\\frac{D-M_i}{\\max(1.4826\\cdot MAD_i,\\;1.0)}$$\n\n$$\\text{ANOMALIE\\_VITESSE si }Z\\ge 2.0\\;OU\\;RetardPur\\ge 4\\text{ trim., sinon NOMINAL}$$\n\n**Plancher à 1.0** : si > 50 % de la ZAPM ouvre au même trimestre ($MAD_i=0$), le dénominateur vaut 1 trimestre. **Anti-masquage** : avec 30 % de bâtiments à 14 trim. contre 70 % à 2 trim., $\\mu+2\\sigma=16.6$ ne détecte **rien**, alors que $M/MAD$ détecte les 30 % (voir test `test_robust_z_no_masking`).\n\nTaux nationaux : $\\tau_{IMB}=\\frac{\\sum\\mathbb{I}(anom)}{N}$, $\\tau_{Locaux}=\\frac{\\sum L_j\\mathbb{I}(anom)}{\\sum L_j}$ (× individuel $L\\le2$ / collectif $L\\ge3$)."""))

cells.append(code('''# B1.1 — Execution Pre-Quadrant (ZAPM stats + classification + anomalies seules)
from ftth.brique1_prequadrant import run_brique1, compute_national_taux
outs1 = run_brique1(config=config, force=FORCE)
print(outs1)
taux = compute_national_taux(outs1["pre_classified"])
for k, v in taux.items(): print(f"{k:24s} = {v:.4f}" if isinstance(v, float) else f"{k:24s} = {v}")
'''))

cells.append(code('''# B1.2 — Inspection : une ZAPM type (M, MAD, Z) + cas MAD=0 (plancher)
import duckdb
con = duckdb.connect(":memory:")
print("--- 5 ZAPM les plus volumineuses ---")
print(con.execute(f"SELECT * FROM read_parquet('{outs1['zapm_stats']}') ORDER BY nb_batiments_zapm DESC LIMIT 5").df())
print("--- Part des ZAPM avec MAD=0 (plancher actif) ---")
print(con.execute(f"SELECT AVG(CASE WHEN mad_duree_zapm = 0 THEN 1.0 ELSE 0.0 END) part_mad0, COUNT(*) n FROM read_parquet('{outs1['zapm_stats']}')").df())
print("--- Exemple : distribution des D et Z sur la plus grosse ZAPM ---")
z0 = con.execute(f"SELECT ref_pm FROM read_parquet('{outs1['zapm_stats']}') ORDER BY nb_batiments_zapm DESC LIMIT 1").fetchone()[0]
print(con.execute(f"""SELECT statut_pre_quadrant, COUNT(*) n, ROUND(AVG(duree_totale_q),2) D_moy,
  ROUND(AVG(z_score_robuste),2) Z_moy, ROUND(AVG(retard_pur_q),2) retard_moy
  FROM read_parquet('{outs1['pre_classified']}') WHERE ref_pm = '{z0}' GROUP BY 1""").df())
con.close()
'''))

cells.append(code('''# B1.3 (visuel) — Histogrammes des durees D et Z robuste + verification anti-masquage
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
df1 = pd.read_parquet(outs1["pre_classified"], columns=["duree_totale_q","z_score_robuste","retard_pur_q","statut_pre_quadrant"])
fig, ax = plt.subplots(1, 3, figsize=(13, 3.5))
df1.duree_totale_q.clip(upper=25).hist(bins=26, ax=ax[0]); ax[0].set_title("D_j,i (trimestres depuis t0 ZAPM)"); ax[0].axvline(df1.duree_totale_q.median(), color="r", ls="--", label="mediane"); ax[0].legend()
df1.z_score_robuste.clip(-2, 10).hist(bins=48, ax=ax[1]); ax[1].set_title("Z robuste (seuil 2.0)"); ax[1].axvline(2.0, color="r", ls="--")
df1.retard_pur_q.clip(-5, 15).hist(bins=40, ax=ax[1+1]); ax[1+1].set_title("Retard pur (seuil 4 trim.)"); ax[1+1].axvline(4, color="r", ls="--")
plt.tight_layout(); plt.savefig("b1_distributions.png", dpi=110)
print("b1_distributions.png | verifiez : pic modal (cohorte PM) + longue traine, seuil Z=2.0 dans la traine (pas de masquage).")
'''))

cells.append(md("""### Sortie 1A — Export Excel pour audit manuel (Voie 1) & Sortie 1B — Flux Parquet (Voie 2)\n\n- **Voie 1** : `export_anomalies_for_manual_audit(code_dept, operateur)` → classeur `.xlsx` des seuls `ANOMALIE_VITESSE` avec sous-durées $D^{(0\\to1)},D^{(1\\to3)},D^{(3\\to4)}$ (localise le ralentissement : prise en compte, programmation, travaux). Filtrer par département/OI pour rester < 1 048 576 lignes.\n- **Voie 2** : `anomalies_vitesse_seules.parquet` (+ `batiments_pre_quadrant.parquet` complet) → entrée Brique 2."""))

cells.append(code('''# B1.4 — Sortie 1A : export audit manuel (exemple : 1er departement, top anomalies)
from ftth.exports_excel import export_anomalies_for_manual_audit, export_zapm_summary_excel
import duckdb
con = duckdb.connect(":memory:")
dept0 = con.execute(f"SELECT code_dept FROM read_parquet('{outs1['pre_classified']}') GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 1").fetchone()[0]
con.close()
audit_xlsx = Path(BASE_DIR) / "outputs_excel" / f"Audit_Manuel_Anomalies_dept{dept0}.xlsx"
export_anomalies_for_manual_audit(outs1["pre_classified"], audit_xlsx, code_dept=dept0, limit=200_000)
zapm_xlsx = Path(BASE_DIR) / "outputs_excel" / "Synthese_ZAPM_PreQuadrant.xlsx"
export_zapm_summary_excel(outs1["zapm_stats"], None, zapm_xlsx)
print("1A OK :", audit_xlsx, "|", zapm_xlsx)
print("1B OK :", outs1["anomalies"], "(sas parquet vers Brique 2)")
'''))

# ---------------------------------------------------------------- Brique 2
cells.append(md("""## Brique 2 — Moteur Post-Quadrant (Q1/Q2/Q3/Q4)\n\nCroisement avec les blocages déclarés à l'Arcep : $BlocageDéclaré=\\mathbb{I}(ever\\_rad=1\\;\\lor\\;motif\\notin\\{AUCUN,NULL\\})$.\n\n| Brique 1 | Blocage Arcep | Quadrant | Signification |\n|---|---|---|---|\n| ANOMALIE | oui | **Q1_Blocage_Exogene_Concordant** | retard réel et justifié (syndic, tiers, voirie/Enedis, RAD légitime) |\n| ANOMALIE | non | **Q2_Inefficience_OI_Non_Justifiee** | **cible d'étude** : retard anormal sans justification |\n| NOMINAL | oui | Q3_Declaration_Preventive_Faux_RAD | RAD préventif sans retard (validation artificielle de jalons) |\n| NOMINAL | non | Q4_Deploiement_Nominal | cohorte standard |\n\nMode `MANUAL_EXCEL_EXPORT` : pas d'affectation auto (audit humain seul). Mode `AUTO_MODEL` : quadrants + dashboard pour la Brique 3."""))

cells.append(code('''# B2.1 — Execution Post-Quadrant
from ftth.brique2_postquadrant import run_brique2
outs2 = run_brique2(config=config, force=FORCE)
print(outs2)
'''))

cells.append(code('''# B2.2 — Inspection : ventilation Q1-Q4 (globale, par zone, par OI)
import duckdb
con = duckdb.connect(":memory:")
qp = outs2["quadrants"]
print(con.execute(f"SELECT quadrant_final, COUNT(*) n, ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (),2) pct FROM read_parquet('{qp}') GROUP BY 1 ORDER BY 1").df())
print(con.execute(f"SELECT zone_reglementaire, quadrant_final, COUNT(*) n FROM read_parquet('{qp}') GROUP BY 1,2 ORDER BY 1,2").df().pivot_table(index="zone_reglementaire", columns="quadrant_final", values="n", aggfunc="sum", fill_value=0))
print(con.execute(f"SELECT operateur_infra, COUNT(*) n, ROUND(100*AVG(CASE WHEN quadrant_final LIKE 'Q2%' THEN 1.0 ELSE 0.0 END),2) pct_Q2 FROM read_parquet('{qp}') GROUP BY 1 ORDER BY 3 DESC").df())
con.close()
'''))

cells.append(code('''# B2.3 — Dashboard final Excel (synthese OI x zone x quadrant)
from ftth.exports_excel import export_dashboard_final_excel
dash = Path(BASE_DIR) / "outputs_excel" / "Synthese_Finale_Quadrants_OI_Zones.xlsx"
export_dashboard_final_excel(outs2["quadrants"], None, dash)
print("Dashboard :", dash)
'''))

# ---------------------------------------------------------------- Brique 3
cells.append(md("""## Brique 3 — Économétrie du Quadrant 2 (Tests 1 & 2, SFA)\n\n**Variables.** Bâtiment : `dist_pm_m`, `part_aerien_troncon` (Enedis), `is_individuel = I(L ≤ 2)`, `dist_voisin_m` (isolement/bout de ligne). ZAPM : `m_voirie_par_local` (BD TOPO), `ratio_aerien_zapm`, `pente_moyenne_deg` (BD ALTI), `part_habitat_disperse` (> 50 m du voisin) → `score_difficulte_zapm` (moyenne des Z-scores ou ACP) → `quintile_difficulte` 1–5 ; `rang_portefeuille_oi ∈ [0,1]` (essoufflement budgétaire/industriel).\n\n**Test 1 (intra-ZAPM, $j ∈ Q2 ∪ Q4$, FE `ref_pm`)** : $\\mathbb{P}(Q2)=\\Lambda(\\alpha_i+\\beta_1\\ln DistPM+\\beta_2 Aérien+\\beta_3 Individuel+\\gamma[\\ln DistPM×OI]+\\delta[\\ln DistPM×Zone])$. $H_0$ : bruit aléatoire ($\\beta=0$) ; $H_1$ : écrémage de coût marginal ($\\beta_1>0$, plus fort en Zone B). Estimateurs : Logit FE (`feglm`), LPM (`feols`), PPML sur `retard_pur_q` (`fepois`).\n\n**Test 2A (inter-ZAPM)** : $\\tau_i^{Q2}=\\frac{N_i^{Q2}}{N_i^{Q2}+N_i^{Q3}+N_i^{Q4}}$, régression quintiles × OI + score × zone + rang + $\\tau^{Q1}$ | dept + $t_0$ (poids $N_{horsQ1}$, cluster dept). $\\phi_{k,5}\\approx0$ = résilient ; $\\psi<0$ = RIP (C) protège mieux qu'AMII (B) à difficulté égale ; $\\lambda>0$ = essoufflement.\n\n**Test 2B (SFA)** : $\\ln(Durée95_i)=f(inputs)+v_i+u_i$, $u_i\\ge0$, $TE_i=e^{-u_i}$. MLE normal/half-normal (Python/`scipy`) + 2e étape OLS de $-\\ln TE$ sur OI × difficulté + zone + rang (approximation de $\\mu_u(Z)$ ; alternative R/`frontier` documentée)."""))

cells.append(code('''# B3.0 — Features ZAPM : score de difficulte, quintiles, rang portefeuille
from ftth.brique3_econometrie import build_zapm_features
zapm_sig = Path(BASE_DIR) / config.RAW_ZAPM_PARQUET_FALLBACK
enedis = Path(BASE_DIR) / config.RAW_ENEDIS_PARQUET
zapm_df = build_zapm_features(outs2["quadrants"], Path(BASE_DIR) / config.OUT_ZAPM_FEATURES,
    zapm_sig if zapm_sig.exists() else None, enedis if enedis.exists() else None,
    method="zscore_mean", n_quintiles=config.N_DIFFICULTY_QUINTILES, duree_quantile=config.SFA_DUREE_QUANTILE)
print(zapm_df[["ref_pm","operateur_infra","zone_reglementaire","nb_bat_hors_Q1","taux_Q2","score_difficulte_zapm","quintile_difficulte","rang_portefeuille_oi"]].head(10))
print(zapm_df.groupby("quintile_difficulte").agg(n=("ref_pm","count"), taux_Q2_moy=("taux_Q2","mean"),
    voirie_loc_med=("m_voirie_par_local","median"), aerien_moy=("ratio_aerien_zapm","mean")))
'''))

cells.append(code('''# B3.1 — Test 1 intra-ZAPM (Logit FE / LPM / PPML). Echantillon Q2+Q4.
from ftth.brique3_econometrie import run_test1
t1 = run_test1(outs2["quadrants"], config=config)
print("backend :", t1.backend, "| n =", t1.n_obs, "|", t1.summary)
for name, mod in [("LOGIT", t1.logit), ("LPM", t1.lpm), ("PPML", t1.ppml)]:
    print(f"\\n===== Test 1 — {name} =====")
    try:
        print(mod.summary() if hasattr(mod, "summary") else mod)
    except Exception as e:
        print(f"(resume indisponible : {e})")
'''))

cells.append(code('''# B3.2 — Test 2A resilience inter-ZAPM (quintiles x OI)
from ftth.brique3_econometrie import run_test2a
t2a = run_test2a(zapm_df, config=config)
print("backend :", t2a.backend, "| n_zapm =", t2a.n_zapm, "|", t2a.summary)
try:
    print(t2a.resilience.summary() if hasattr(t2a.resilience, "summary") else t2a.resilience)
except Exception as e:
    print("(resume indisponible :", e, ")")
'''))

cells.append(code('''# B3.3 (visuel) — Taux Q2 par quintile de difficulte x operateur (resilience)
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
piv = zapm_df.pivot_table(index="quintile_difficulte", columns="operateur_infra", values="taux_Q2", aggfunc="mean")
ax = piv.plot(marker="o", figsize=(10, 4)); ax.set_title("Taux Q2 moyen par quintile de difficulte et OI (plat = resilient)")
ax.set_xlabel("quintile_difficulte (1 facile -> 5 difficile)"); ax.set_ylabel("taux_Q2"); plt.tight_layout()
plt.savefig("b3_resilience.png", dpi=110); print("b3_resilience.png"); print(piv.round(3))
'''))

cells.append(code('''# B3.4 — Test 2B SFA (frontiere stochastique cout/temps -> TE)
from ftth.brique3_econometrie import run_sfa
sfa = run_sfa(zapm_df, config=config)
print("backend :", sfa.backend, "|", sfa.summary)
print("params :", sfa.params)
if sfa.te is not None:
    import pandas as pd
    te_df = zapm_df[["ref_pm","operateur_infra","zone_reglementaire","quintile_difficulte"]].copy()
    te_df["TE"] = sfa.te.to_numpy()
    print(te_df.groupby("operateur_infra").TE.agg(["mean","median","count"]).round(3).sort_values("mean"))
    print(te_df.groupby(["quintile_difficulte","zone_reglementaire"]).TE.mean().round(3))
    te_df.to_parquet(Path(BASE_DIR) / "processed_parquet/step3_econometrie/te_scores.parquet", compression="zstd", index=False)
    # MAJ dashboard avec resilience par quintile
    from ftth.exports_excel import export_dashboard_final_excel
    export_dashboard_final_excel(outs2["quadrants"], Path(BASE_DIR) / config.OUT_ZAPM_FEATURES,
        Path(BASE_DIR) / "outputs_excel" / "Synthese_Finale_Quadrants_OI_Zones.xlsx")
'''))

# ---------------------------------------------------------------- Conclusion
cells.append(md("""## Conclusion — limites, reproductibilité, checkpoints\n\n**Ce que le pipeline prouve.** La Brique 1 isole un retard **relatif à la cohorte PM** (effet fixe ZAPM implicite, robuste au masquage) ; la Brique 2 sépare le retard **justifié** (Q1) du retard **inexpliqué** (Q2, cible) ; la Brique 3 teste si Q2 est un bruit aléatoire ou un **écrémage de coût marginal** (Test 1), et quels OI / régimes (B vs C) **décrochent** dans les zones difficiles (Test 2 + SFA).\n\n**Limites.** ZAPM de rue uniquement (ZTD exclue : PM d'immeuble, saut d'états) ; `dist_voisin_m`/`voirie`/`pente` = proxys en démo (brancher BD TOPO/BD ALTI/Enedis en production) ; SFA Python half-normal homogène + 2e étape naïve (alternative R/`frontier` à hétéroscédasticité paramétrique si exigée par le jury) ; RAD vs vrai blocage tributaire de la qualité des motifs OI.\n\n**Reproductibilité.** `RANDOM_STATE=42`, seuils `K=2.0` / `4 trim.`, sentinelle `99` / calcul `33`, checkpoints ZSTD ci-dessous. Tests : `pytest -q`, smoke CLI : `python scripts/run_pipeline.py --demo --n-zapm 12`."""))

cells.append(code('''# Checkpoints finaux (reprise-sur-panne) + rappel des sorties Excel
from pathlib import Path
for p in [config.OUT_BATIMENTS_CYCLES, config.OUT_ZAPM_STATS, config.OUT_PRE_CLASSIFIED,
          config.OUT_ANOMALIES_ONLY, config.OUT_QUADRANTS, config.OUT_ZAPM_FEATURES]:
    fp = Path(BASE_DIR) / p
    print(("OK  " if fp.exists() else "ABSENT"), f"{fp} ({fp.stat().st_size/1e6:.1f} Mo)" if fp.exists() else "")
print("--- Excel ---")
for x in sorted((Path(BASE_DIR) / "outputs_excel").glob("*.xlsx")):
    print("OK  ", x.name, f"({x.stat().st_size/1e3:.0f} Ko)")
'''))

nb = {
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.10.0"},
    },
    "nbformat": 4, "nbformat_minor": 5, "cells": cells,
}
out = Path("/workspace/notebooks/FTTH_Model_Pipeline_Complet.ipynb")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")
print(f"Notebook : {out} ({len(cells)} cellules)")
