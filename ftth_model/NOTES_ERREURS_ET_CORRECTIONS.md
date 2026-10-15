# Erreurs rencontrées et corrections

Note de reprise, 15 octobre 2026. Elle décrit les erreurs vues en exécutant le notebook sur une machine Windows avec les fichiers réels de l'Arcep, et les corrections déjà présentes dans cette archive.

Fichiers concernés : `src/ftth/data_acquisition.py` et la cellule 3 de `notebooks/FTTH_Model_Pipeline_Complet.ipynb`.

## 1. Le lien de téléchargement de l'archive renvoyait une 404

Le premier lien pointait vers un chemin interne de la machine de développement (`/opt/cursor/artifacts/...`). Ce chemin n'est pas une adresse publique.

**Correction.** L'archive complète est le fichier `ftth_model_complet.zip`, à la racine du projet. Après décompression, le dossier `ftth_model` est la racine : `requirements.txt`, `src/`, `notebooks/` et `data/` sont directement dedans.

## 2. `BadZipFile: File is not a zip file` au premier millésime Immeuble

La cellule 3 télécharge chaque millésime et l'enregistre sous un nom se terminant par `.zip`. Les millésimes **2018_T2, 2018_T3, 2018_T4, 2019_T1 à 2019_T4** (et, selon le jeu data.gouv, parfois **2021_T2**) sont en réalité des archives **7z**. Leur signature est `7z\xbc\xaf'`, pas `PK`. Python ouvrait ce contenu avec `zipfile` et s'arrêtait sur 2018_T2, le premier de la liste.

**Correction.** Le format est lu sur les 8 premiers octets, pas sur l'extension. Un 7z est extrait avec `py7zr`. Un zip est extrait avec la bibliothèque standard.

Vérification faite sur le vrai fichier 2018_T2 ouvert comme un `.zip` : la même `BadZipFile` apparaît, puis la lecture 7z aboutit (22 184 immeubles sur le département 07).

## 3. Extraction zip impossible sous Windows (tube nommé)

Le chemin zip d'origine décompressait le CSV vers un tube nommé (`os.mkfifo`) et appelait la commande `unzip`. Windows n'a pas ce tube, et `unzip` n'est pas forcément installé. L'erreur serait apparue juste après le correctif 7z, sur le premier vrai zip.

**Correction.** Le CSV est extrait dans un fichier temporaire, puis lu par DuckDB. Le fichier temporaire est supprimé à la fin du millésime. La cellule 3 pose en plus un filet : elle remplace `mkfifo` et le flux `unzip` par la même extraction, au cas où une ancienne copie de `data_acquisition.py` serait encore importée.

## 4. `2020_T1 : colonnes indispensables absentes. Entete : ['code_l33', 'nom_operateur']`

Ce n'est pas un changement de structure du fichier immeubles. L'archive `2020t1-immeuble.zip` contient deux CSV :

| Fichier | Taille | Rôle |
|---|---|---|
| `2020T1_Immeuble/codes_operateur.csv` | 3 Ko | Dictionnaire d'opérateurs (`code_l33`, `nom_operateur`) |
| `2020T1_Immeuble/mv_immeubles_2020t1.csv` | 2,5 Go | Immeubles (`x`, `y`, `imb_id`, `imb_etat`, `pm_ref`, …) |

Le chargeur prenait le premier CSV de l'archive, donc le dictionnaire. `2020_T2` a le même second fichier. Les millésimes suivants n'en ont qu'un.

L'en-tête réel des immeubles 2020_T1 est :

`x, y, imb_id, num_voie, cp_no_voie, type_voie, nom_voie, batiment, code_poste, nom_com, catg_loc_imb, imb_etat, pm_ref, pm_etat, code_l331, geom_mod, type_imb`

**Correction.** Quand une archive contient plusieurs CSV, celui dont le nom évoque les immeubles (`immeuble`, `immeule`, `carte_fibre`, `mv_imm`) et dont la taille est la plus grande est retenu. `codes_operateur.csv` est ignoré. Le journal doit afficher :

`CSV retenu « 2020T1_Immeuble/mv_immeubles_2020t1.csv »`

## Schémas réellement observés (2018 à 2026)

Les colonnes utiles au modèle restent `x`, `y`, `imb_id`, `imb_etat`, `pm_ref`. Les variations sont absorbées :

| Période | Particularité | Traitement |
|---|---|---|
| 2018_T2 à 2020_T4 | Pas de `code_insee` (parfois seulement `code_poste`) | Commune retrouvée par les coordonnées dans le shapefile communal |
| 2020_T1 et 2020_T2 | CSV annexe `codes_operateur.csv` | Ignoré |
| À partir de 2021_T1 | `code_insee` présent | Utilisé directement |
| Selon les millésimes | `type_imb` vaut `pavillon`, `PA` ou `IM` | `pavillon` et `PA` comptent comme de l'habitat individuel |
| 2022_T2 | Nom de fichier `mv_immeules_...` (coquille Arcep) | Reconnu par le préfixe `mv_imm` |
| À partir de 2023_T2 | Fichier renommé `carte_fibre_immeubles_...` | Reconnu par `carte_fibre` |

Les états IPE sont traduits ainsi : `cible` → études, `signe` → programmé, `en cours de deploiement` → en cours, `raccordable demande` et `rad en cours de deploiement` → RAD, `deploye` → raccordable. Les lignes `abandonne` sont écartées.

## Ce qu'il faut faire pour reprendre

Remplacer la cellule 3 par celle du notebook de cette archive, ou garder la cellule déjà collée si elle contient à la fois `_format_archive`, `_extraire_zip` et `_csv_immeubles`. Relancer uniquement cette cellule. Les parquets déjà écrits sont sautés. Le fichier `2020_T1` déjà téléchargé dans `data/raw_data/_downloads` est réutilisé.
