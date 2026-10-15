# FTTH — Modélisation économique du déploiement fibre (Zones B & C, hors ZTD)

Programme Python complet + **notebook unique de lancement, préparation et pilotage** pour :
1. fusionner les 3 sources brutes (BAN/BD TOPO + ZAPM/PM + 32 millésimes IMB Arcep),
2. détecter les **anomalies cinématiques** intra-ZAPM par **Z-score robuste** (médiane/MAD),
3. recouper avec les **blocages déclarés Arcep** pour affecter les **4 quadrants** (Q1–Q4),
4. estimer l'**efficience des opérateurs** (Tests 1 & 2 du Quadrant 2 + frontière stochastique SFA).

Périmètre : **hors Zones Très Denses** (Zone A exclue), Zones B (AMII/AMEL, privée L. 33-13) et C (RIP, publique), trimestres `t = 1..32` (2018_T2 → 2026_T2), maille **ZAPM subdivisée au bâtiment** (`ref_pm` / `id_ban`).

## Démarrage (données Arcep réelles)

Décompressez l'archive, ouvrez `notebooks/FTTH_Model_Pipeline_Complet.ipynb` et exécutez-le de haut en bas. `requirements.txt` reste à la racine.

1. La première cellule de code installe les dépendances (`pip install -r requirements.txt`) dans l'interpréteur du notebook et affiche le journal pip.
2. La cellule suivante télécharge les fichiers réels de l'Arcep (jeu « Le marché du haut et très haut débit fixe », immeubles IPE + contours ZAPM + zonage communal), les convertit en parquet et **s'arrête si le contrôle échoue**.
3. Les briques 0 à 3 enchaînent sur ces fichiers.

`DEMO_MODE` vaut `False`. Le remettre à `True` régénère un territoire synthétique à la place du téléchargement.

Les erreurs vues au téléchargement réel (archives 7z, Windows, CSV opérateur de 2020) et leurs corrections sont dans `NOTES_ERREURS_ET_CORRECTIONS.md`. `QUARTERS = None` et `DEPARTEMENTS = None` prennent tous les millésimes publiés (2018_T2 → 2026_T2, environ 15 à 25 Go compressés). Pour un premier passage plus court : `QUARTERS = ["2024_T4", "2026_T2"]` et `DEPARTEMENTS = ["05"]`.

```bash
jupyter notebook notebooks/FTTH_Model_Pipeline_Complet.ipynb
# alternative sans notebook :
python scripts/run_pipeline.py --demo --n-zapm 40
```

## Architecture

```
notebooks/FTTH_Model_Pipeline_Complet.ipynb   # UNIQUE notebook : config + B0 + B1 + B2 + B3
src/ftth/
  config.py                 # Section 0 : T_MIN/T_MAX, seuils K=2.0/4 trim., modes, chemins
  data_acquisition.py       # téléchargement BAN/Enedis/scan Arcep + conversion Parquet
  synthetic_generator.py    # jeu démo + vérité terrain Q1-Q4 (tests, dev hors-ligne)
  brique0_etl.py            # fusion 3 sources -> batiments_cycles.parquet (checkpoints ZSTD)
  brique1_prequadrant.py    # t0, D, M/MAD, Z robuste, ANOMALIE vs NOMINAL, tau nationaux
  brique2_postquadrant.py   # blocage déclaré -> Q1/Q2/Q3/Q4 (AUTO_MODEL ou MANUAL_EXCEL_EXPORT)
  brique3_econometrie.py    # features, Test 1 (FE ZAPM), Test 2A (quintiles), Test 2B (SFA)
  exports_excel.py          # audit manuel .xlsx (Voie 1) + dashboards (Voie 2)
  io_utils.py               # DuckDB out-of-core, checkpoints
scripts/run_pipeline.py     # CLI headless
tests/                      # pytest : maths (masquage, MAD=0, quadrants) + pipeline synthétique
data/
  raw_data/01_arcep_imb_trimestriels/imb_tXX.parquet   # 32 millésimes (t=1..32)
  raw_data/02_arcep_sig_zapm_pm/                       # contours_zapm.gpkg ou zapm_pm.parquet
  raw_data/03_arcep_zonage_ref/reference_communes.parquet
  raw_data/04_ign_bdtopo_ban/ban_france.parquet
  raw_data/05_enedis_odre_aerien/enedis_bt_communes.parquet
  processed_parquet/step0_etl|step1_pre_quadrant|step2_post_quadrant|step3_econometrie/
  outputs_excel/            # audits + dashboards .xlsx
```

## Mathématiques implémentées (strictement cahier des charges)

**Brique 0** — `t_etudes/t_programme/t_encours/t_raccordable` = premiers trimestres de passage aux jalons ; `t_raccordable = 99` + `is_right_censored = 1` si jamais raccordable à T32 ; `ever_rad`, `duree_en_rad_q` ; ancrage `ref_pm = LAST(ref_pm ORDER BY t)` + flag `has_pm_reconfigured` ; `ref_pm = COALESCE(arcep, geo)`.

**Brique 1** — `t0,i = min_j(t_etudes,j)` ; `D_j,i = min(t_racc,33) − t0,i` (+ sous-durées `D(0→1), D(1→3), D(3→4)`) ; `M_i = médiane(D)`, `MAD_i = médiane(|D − M_i|)` ; `RetardPur = D − M_i` ; `Z^robuste = (D − M_i)/max(1.4826·MAD_i, 1.0)` ; `ANOMALIE_VITESSE` si `Z ≥ 2.0` **ou** `RetardPur ≥ 4` ; `τ_IMB`, `τ_Locaux` (× individuel/collectif).

**Brique 2** — `BlocageDéclaré = I(ever_rad=1 ∨ motif ∉ {AUCUN,NULL})` ; Q1 = anomalie + blocage (exogène concordant), **Q2 = anomalie sans blocage (inefficience OI, cible d'étude)**, Q3 = nominal + blocage (faux RAD), Q4 = nominal.

**Brique 3** — Test 1 intra-ZAPM `is_Q2 ~ ln(dist_pm) + aérien + individuel + ln(dist)×OI + ln(dist)×zone | ref_pm` (Logit FE / LPM / PPML) ; Test 2A `τ_i^Q2 ~ quintiles×OI + score×zone + rang_portefeuille + Q1 | dept + t0` ; Test 2B SFA `ln(Durée95) = f(inputs) + v + u`, `TE = exp(−u)`. Indice de difficulté : `ln(m_voirie/local)`, `ratio_aérien`, `pente`, `dispersion` (moyenne des Z-scores ou ACP) → quintiles 1–5.

Détails, équations et justifications (effet de masquage, troncature à gauche / état Néant = 0, censure à droite, gel PM 3 mois) : voir les cellules Markdown du notebook.

## Données réelles (quand vous êtes prêt)

| Base | URL | Destination |
|---|---|---|
| Immeubles IPE, ZAPM, communes (tous millésimes) | jeu data.gouv « Le marché du haut et très haut débit fixe (déploiements) » — téléchargé par le notebook | `raw_data/01_arcep_imb_trimestriels/`, `02_arcep_sig_zapm_pm/`, `03_arcep_zonage_ref/` |
| Contours ZAPM + PM | dernier millésime, sous-dossier `cartographie/`/`contours_zapm/` | `raw_data/02_arcep_sig_zapm_pm/contours_zapm_ftth.gpkg` |
| Zonage communes | https://www.data.gouv.fr/fr/datasets/le-marche-du-haut-et-tres-haut-debit-fixe-deploiements/ | `raw_data/03_arcep_zonage_ref/reference_communes.parquet` (`code_insee`, `zone_reglementaire`) |
| BAN | https://adresse.data.gouv.fr/data/ban/adresses/latest/csv/ | `raw_data/04_ign_bdtopo_ban/ban_france.parquet` |
| BD TOPO (voirie) | https://geoservices.ign.fr/bdtopo | `raw_data/04_ign_bdtopo_ban/` |
| Enedis BT aérien/souterrain | https://data.enedis.fr/ | `raw_data/05_enedis_odre_aerien/enedis_bt_communes.parquet` |
| BD ALTI, INSEE RP/SIRENE (optionnel) | voir notebook §0 | `raw_data/06_insee_sirene_controle/` |

Acquisition semi-automatique : `python -m ftth.data_acquisition` (BAN + Enedis + scan index Arcep), ou cellules §0 du notebook. Convertir chaque CSV Arcep : `convert_arcep_csv_to_parquet(csv, imb_tXX.parquet, trimestre=t)`. Colonnes IMB minimales : `id_imb, id_ban, code_insee, ref_pm, operateur_infra, etat_imb, nb_locaux, x_l93, y_l93` (+ `motif_blocage`, `trimestre`).

## Choix techniques (pourquoi)

- **DuckDB ≥ 1.0 (+ spatial)** : ~750 M lignes trimestrielles traitées **out-of-core** (Parquet/ZSTD sur SSD, `memory_limit='14GB'`), jointure point-dans-polygone sans PostGIS.
- **Polars + PyArrow + xlsxwriter/openpyxl** : zero-copy Arrow, exports `.xlsx` rapides (< 1 048 576 lignes).
- **pyfixest** (repli `statsmodels`) : ~110 k effets fixes ZAPM absorbés par projections alternées ; **SFA** MLE normal/half-normal en pur Python (`scipy`), repli R/`frontier` documenté.
- **Notebook unique modulaire** : exécution par briques avec **checkpoints parquet** reprise-sur-panne, inspection visuelle (histogrammes, anti-masquage) avant le choix audit manuel (Voie 1) / quadrants auto (Voie 2), traçabilité académique (équation avant chaque calcul).

## Tests & reproductibilité

```bash
pytest -q            # maths + pipeline synthétique B0->B3
python scripts/run_pipeline.py --demo --n-zapm 12   # smoke test rapide
```

`RANDOM_STATE = 42` partout ; seuils `K_ROBUST_Z = 2.0`, `MIN_EXCESS_QUARTERS = 4` ; sentinelle `99` ; `T_MAX+1 = 33`.

## Limites connues

- En ZTD (exclue), les états intermédiaires sautent (PM d'immeuble) : le modèle exige des ZAPM de rue (B/C).
- `dist_voisin_m`, `m_voirie_par_local`, `pente` : proxys synthétiques en démo ; brancher BD TOPO/BD ALTI/Enedis en production (colonnes documentées dans le notebook §B3).
- SFA Python : half-normal homogène + 2e étape OLS sur `−ln(TE)` (vs `frontier` R à hétéroscédasticité paramétrique : résultats comparables, écarts-types de la 2e étape naïfs).
