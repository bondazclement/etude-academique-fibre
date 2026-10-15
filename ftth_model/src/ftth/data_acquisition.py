"""Telechargement des donnees reelles Arcep et controle de chargement.

Source officielle : jeu data.gouv « Le marche du haut et tres haut debit fixe
(deploiements) », alimente par les fichiers IPE / ZAPM transmis a l'Arcep.

  - Immeuble (CSV, un millesime par trimestre) : imb_etat, pm_ref, code_l331, x, y
  - ZAPM (shapefile) : polygone, RefPM, CodeOI, commune du PM
  - Commune (shapefile) : zone ZTD / ZIPRI / ZIPU, engagement L. 33-13, AMEL

Le fichier public « Ma connexion internet / base_imb » ne contient PAS l'etat
de deploiement ni le PM. Il n'est donc pas la source du modele.

Etats IPE publies -> chaine du modele :
  cible                          -> etudes
  signe                          -> programme
  en cours de deploiement        -> en_cours
  raccordable demande
  rad en cours de deploiement    -> raccordable_demande
  deploye                        -> raccordable
  abandonne                      -> ecarte (commercialisation annulee)
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import zipfile
from pathlib import Path

from .config import FTTHConfig, trimestre_index, trimestre_label
from .io_utils import get_duckdb_connection

DATASET_API = (
    "https://www.data.gouv.fr/api/1/datasets/"
    "le-marche-du-haut-et-tres-haut-debit-fixe-deploiements/"
)
USER_AGENT = "ftth-model/1.0 (notebook de recherche ; donnees ouvertes Arcep)"

# Vocabulaire reel observe sur 2026T2 (carte_fibre_immeubles).
_ETAT_TO_CANON = {
    "cible": "etudes",
    "signe": "programme",
    "signé": "programme",
    "en cours de deploiement": "en_cours",
    "en cours de déploiement": "en_cours",
    "raccordable demande": "raccordable_demande",
    "rad en cours de deploiement": "raccordable_demande",
    "rad en cours de déploiement": "raccordable_demande",
    "deploye": "raccordable",
    "déployé": "raccordable",
    "deployé": "raccordable",
    "abandonne": "abandonne",
    "abandonné": "abandonne",
}


def step(message: str) -> None:
    """Journal de pilotage, immediatement visible dans le notebook."""
    now = time.strftime("%H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def normalize_imb_etat(raw: str | None) -> str:
    key = re.sub(r"\s+", " ", str(raw or "").strip().lower())
    return _ETAT_TO_CANON.get(key, key or "inconnu")


def classify_zone(zone, l33, oi_amel, ztd) -> str:
    """Zone reglementaire du modele a partir des champs du shapefile communal."""
    z = str(zone or "").strip().upper()
    l = str(l33 or "").strip()
    try:
        amel = float(oi_amel or 0)
    except (TypeError, ValueError):
        amel = 0.0
    try:
        ztd_flag = float(ztd or 0)
    except (TypeError, ValueError):
        ztd_flag = 1.0 if str(ztd).strip().lower() in {"oui", "true", "ztd"} else 0.0
    if z == "ZTD" or ztd_flag == 1.0:
        return "A_ZTD"
    if l in {"Orange", "SFR", "mixte"}:
        return "B_AMII"
    if amel not in (0.0,):
        return "B_AMEL"
    if z == "ZIPU":
        return "C_RIP"
    if z == "ZIPRI":
        return "B_AMII"
    return "INCONNU"


def install_requirements(requirements_path: str | Path) -> None:
    """Installe les paquets du requirements.txt dans l'interpreteur en cours."""
    req = Path(requirements_path).resolve()
    if not req.is_file():
        raise FileNotFoundError(f"requirements.txt introuvable : {req}")
    step(f"Interpreteur Python : {sys.executable} ({sys.version.split()[0]})")
    step(f"Installation des dependances depuis {req}")
    cmd = [sys.executable, "-m", "pip", "install", "-r", str(req)]
    step("Commande : " + " ".join(cmd))
    t0 = time.time()
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        print(line, end="", flush=True)
    code = proc.wait()
    step(f"pip termine en {time.time() - t0:.1f} s, code de sortie {code}")
    if code != 0:
        raise RuntimeError(
            "L'installation des dependances a echoue. Le detail est au-dessus. "
            "Relancez cette cellule apres avoir corrige l'acces reseau ou les droits d'ecriture."
        )


def find_repo_root(start: Path | None = None) -> Path:
    ici = (start or Path.cwd()).resolve()
    for candidat in [ici, *ici.parents]:
        if (candidat / "requirements.txt").is_file() and (candidat / "src" / "ftth").is_dir():
            return candidat
    raise FileNotFoundError(
        "Racine du projet introuvable (requirements.txt + src/ftth). "
        "Decompressez l'archive et lancez Jupyter dans ce dossier, ou dans notebooks/."
    )


def _urlopen(url: str, timeout: int = 120):
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    return urllib.request.urlopen(request, timeout=timeout)


def fetch_catalog() -> list[dict]:
    step(f"Lecture du catalogue data.gouv : {DATASET_API}")
    with _urlopen(DATASET_API, timeout=60) as response:
        payload = json.loads(response.read().decode("utf-8"))
    resources = payload.get("resources") or []
    step(f"Catalogue : {len(resources)} ressources, jeu « {payload.get('title', '')} »")
    return resources


def _parse_resource(resource: dict) -> tuple[str, str] | None:
    """Reconnaît 2026T2-Immeuble / 2025T4-Zapm / 2024T3-Commune."""
    title = resource.get("title") or ""
    match = re.search(r"(20\d{2})\s*T\s*([1-4]).{0,12}(immeuble|zapm|commune)", title, re.I)
    if not match:
        return None
    label = f"{match.group(1)}_T{match.group(2)}"
    kind = match.group(3).lower()
    return label, kind


def select_resources(resources: list[dict]) -> dict[str, dict[str, dict]]:
    """{kind: {YYYY_TX: resource}} en gardant la ressource la plus recente par cle."""
    chosen: dict[str, dict[str, dict]] = {"immeuble": {}, "zapm": {}, "commune": {}}
    for resource in resources:
        parsed = _parse_resource(resource)
        if not parsed or not resource.get("url"):
            continue
        label, kind = parsed
        previous = chosen[kind].get(label)
        if previous is None or (resource.get("last_modified") or "") >= (previous.get("last_modified") or ""):
            chosen[kind][label] = resource
    return chosen


def download_file(url: str, dest: str | Path, force: bool = False) -> Path:
    """Telecharge url vers dest avec journal de progression. Idempotent."""
    out = Path(dest)
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists() and out.stat().st_size > 0 and not force:
        step(f"DEJA LA  {out.name} ({out.stat().st_size / 1e6:.1f} Mo) — telechargement saute")
        return out
    part = out.with_suffix(out.suffix + ".part")
    step(f"TELECHARGEMENT  {url}")
    step(f"            -> {out}")
    t0 = time.time()
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=180) as response:
        total = int(response.headers.get("Content-Length") or 0)
        if total:
            step(f"            taille annoncee : {total / 1e6:.1f} Mo")
        done = 0
        last_log = 0
        with part.open("wb") as handle:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                handle.write(chunk)
                done += len(chunk)
                if done - last_log >= 40 * 1024 * 1024 or (total and done >= total):
                    elapsed = max(time.time() - t0, 0.1)
                    pct = f"{100 * done / total:.0f}%" if total else "?"
                    step(
                        f"            {out.name}: {done / 1e6:.0f}"
                        f"/{(total / 1e6) if total else 0:.0f} Mo ({pct}) "
                        f"— {done / elapsed / 1e6:.1f} Mo/s"
                    )
                    last_log = done
    part.replace(out)
    step(f"OK          {out.name} ({out.stat().st_size / 1e6:.1f} Mo) en {time.time() - t0:.0f} s")
    return out


def _archive_format(path: Path) -> str:
    """Zip (PK) ou 7z. Plusieurs millesimes Arcep de 2018-2021 sont des 7z."""
    with Path(path).open("rb") as handle:
        signature = handle.read(8)
    if signature.startswith(b"PK"):
        return "zip"
    if signature.startswith(b"7z\xbc\xaf'\x1c"):
        return "7z"
    raise RuntimeError(
        f"{path.name} n'est ni un zip ni un 7z (telechargement incomplet ou page d'erreur). "
        "Supprimez ce fichier dans data/raw_data/_downloads puis relancez la cellule."
    )


def _rank_immeuble_csv(name: str, size: int = 0) -> tuple[int, int]:
    """Choisit le CSV des immeubles quand l'archive en contient plusieurs.

    2020_T1 et 2020_T2 embarquent aussi codes_operateur.csv
    (colonnes code_l33, nom_operateur). Ce n'est pas un changement de schema
    du fichier immeubles : c'est un second fichier, beaucoup plus petit.
    """
    low = name.lower().replace("\\", "/")
    base = low.rsplit("/", 1)[-1]
    if not base.endswith(".csv") or low.endswith("/"):
        return (-1, 0)
    if any(token in base for token in ("operateur", "opérateur", "commune", "zapm", "readme")):
        return (0, size)
    priority = 1
    if any(token in base for token in ("immeuble", "immeule", "carte_fibre", "mv_imm")):
        priority = 3
    return (priority, size)


def _zip_csv_member(zip_path: Path) -> str:
    with zipfile.ZipFile(zip_path) as archive:
        infos = [
            info for info in archive.infolist()
            if info.filename.lower().endswith(".csv") and not info.filename.endswith("/")
        ]
    if not infos:
        raise FileNotFoundError(f"Aucun CSV dans {zip_path}")
    infos.sort(key=lambda info: _rank_immeuble_csv(info.filename, info.file_size), reverse=True)
    best = infos[0]
    if _rank_immeuble_csv(best.filename, best.file_size)[0] <= 0:
        raise FileNotFoundError(
            f"Aucun CSV d'immeubles dans {zip_path}. Fichiers : {[i.filename for i in infos]}"
        )
    if len(infos) > 1:
        step(
            f"Archive multi-fichiers : CSV retenu « {best.filename} » "
            f"({best.file_size / 1e6:.1f} Mo). Ignores : "
            + ", ".join(i.filename for i in infos if i is not best)
        )
    return best.filename


def _csv_header_and_delim(zip_path: Path, member: str) -> tuple[list[str], str]:
    with zipfile.ZipFile(zip_path) as archive, archive.open(member) as raw:
        line = raw.readline().decode("utf-8", "replace")
    delim = ";" if line.count(";") > line.count(",") else ","
    header = next(csv.reader(io.StringIO(line), delimiter=delim))
    return [h.strip() for h in header], delim


def _pick(header: list[str], *candidates: str) -> str | None:
    low = {h.lower(): h for h in header}
    for candidate in candidates:
        if candidate.lower() in low:
            return low[candidate.lower()]
    return None


def _stream_zip_csv(zip_path: Path, member: str, fifo: Path) -> dict:
    """Decompresse le CSV du zip vers un tube nomme, sans ecrire les ~4,5 Go.

    Le thread bloque sur l'ouverture du tube jusqu'a ce que DuckDB le lise,
    puis unzip ecrit directement dedans.
    """
    holder: dict = {}

    def _pump() -> None:
        with fifo.open("wb") as out:
            holder["proc"] = subprocess.Popen(
                ["unzip", "-p", str(zip_path), member],
                stdout=out,
                stderr=subprocess.PIPE,
            )
            holder["proc"].wait()

    thread = threading.Thread(target=_pump, daemon=True)
    thread.start()
    holder["thread"] = thread
    return holder


def _load_oi_names(xlsx_path: Path) -> dict[str, str]:
    import openpyxl

    book = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    sheet = book.active
    rows = sheet.iter_rows(values_only=True)
    header = [str(c or "").replace("\n", " ") for c in next(rows)]
    try:
        i_code = next(i for i, h in enumerate(header) if "33-1" in h or "33‑1" in h)
    except StopIteration:
        i_code = 2
    i_name = next((i for i, h in enumerate(header) if "infrastructure" in h.lower() or "exploitant" in h.lower()), 3)
    mapping: dict[str, str] = {}
    for row in rows:
        if not row or row[i_code] is None:
            continue
        code = str(row[i_code]).strip()
        name = str(row[i_name]).strip() if row[i_name] else code
        if code:
            mapping[code] = name
    book.close()
    return mapping


def _csv_header_from_file(csv_path: Path) -> tuple[list[str], str]:
    with csv_path.open("rb") as handle:
        line = handle.readline().decode("utf-8", "replace")
    delim = ";" if line.count(";") > line.count(",") else ","
    header = next(csv.reader(io.StringIO(line), delimiter=delim))
    return [h.strip() for h in header], delim


def _convert_immeuble_zip(
    zip_path: Path,
    out_parquet: Path,
    trimestre: int,
    label: str,
    oi_names: dict[str, str],
    departements: list[str] | None,
    memory: str,
    threads: int,
    zonage_parquet: Path | None = None,
) -> dict:
    work = Path(tempfile.mkdtemp(prefix="ftth_imb_"))
    # Le nom enregistre est toujours .zip ; 2018_T2 et d'autres millesimes sont des 7z.
    extracted = _archive_format(zip_path) == "7z"
    if extracted:
        import py7zr

        step(f"{label}  archive 7z : extraction du CSV (millesime ancien, supprime ensuite)")
        with py7zr.SevenZipFile(zip_path, "r") as archive:
            archive.extractall(work)
        csvs = [p for p in work.rglob("*.csv") if p.is_file()]
        if not csvs:
            raise FileNotFoundError(f"Aucun CSV dans {zip_path}")
        csvs.sort(key=lambda p: _rank_immeuble_csv(p.name, p.stat().st_size), reverse=True)
        csv_path = csvs[0]
        member = csv_path.name
        header, delim = _csv_header_from_file(csv_path)
        source = csv_path
        holder = {"thread": None, "proc": None}
        parallel = "true"
    else:
        member = _zip_csv_member(zip_path)
        header, delim = _csv_header_and_delim(zip_path, member)
        source = work / "immeuble.csv"
        step(f"{label}  extraction du CSV depuis le zip (fichier temporaire, pas de tube nomme)")
        with zipfile.ZipFile(zip_path) as archive, archive.open(member) as src, source.open("wb") as dst:
            shutil.copyfileobj(src, dst, length=1 << 20)
        holder = {"thread": None, "proc": None}
        parallel = "true"
    step(f"{label}  CSV « {member} », separateur '{delim}', {len(header)} colonnes")
    step(f"{label}  colonnes : {', '.join(header)}")
    needed = {
        "x": _pick(header, "x", "lon", "longitude"),
        "y": _pick(header, "y", "lat", "latitude"),
        "imb_id": _pick(header, "imb_id", "id_imb"),
        "code_insee": _pick(header, "code_insee", "imb_code_insee"),
        "etat": _pick(header, "imb_etat", "etat_imb"),
        "pm": _pick(header, "pm_ref", "ref_pm"),
        "oi": _pick(header, "code_l331", "code_oi", "operateur_infra"),
        "cat": _pick(header, "catg_loc_imb", "catg_log_imb"),
        "type_imb": _pick(header, "type_imb"),
    }
    missing = [k for k, v in needed.items() if v is None and k in {"imb_id", "etat", "pm", "x", "y"}]
    if missing:
        raise RuntimeError(f"{label} : colonnes indispensables absentes : {missing}. Entete : {header}")
    if needed["code_insee"] is None:
        if zonage_parquet is None or not zonage_parquet.exists():
            raise RuntimeError(f"{label} : pas de code_insee et pas de contour communal pour le retrouver.")
        step(f"{label}  pas de code_insee dans ce millesime : commune retrouvee par les coordonnees")

    def col(key: str, default: str = "NULL") -> str:
        name = needed.get(key)
        return f'"{name}"' if name else default

    etat_sql = f"lower(trim(CAST({col('etat')} AS VARCHAR)))"
    whens = " ".join(
        f"WHEN {etat_sql} = '{src}' THEN '{dst}'" for src, dst in _ETAT_TO_CANON.items()
    )
    dept_filter = ""
    if departements:
        likes = " OR ".join(f"code_insee_final LIKE '{d}%'" for d in departements)
        dept_filter = f"WHERE {likes}"
        step(f"{label}  filtre departements : {', '.join(departements)}")

    oi_values = ", ".join(
        "('" + code.replace("'", "''") + "', '" + name.replace("'", "''") + "')"
        for code, name in sorted(oi_names.items())
    ) or "('__aucun__', '__aucun__')"

    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    step(f"{label}  lecture CSV ({'fichier 7z extrait' if extracted else 'zip extrait en fichier temporaire'})")
    insee_expr = (
        f"lpad(CAST({col('code_insee')} AS VARCHAR), 5, '0')"
        if needed["code_insee"]
        else "CAST(NULL AS VARCHAR)"
    )
    commune_join = ""
    if needed["code_insee"] is None:
        commune_join = f"""
        LEFT JOIN (
          SELECT code_insee, ST_GeomFromWKB(geom_wkb) AS geom
          FROM read_parquet('{zonage_parquet}')
          WHERE geom_wkb IS NOT NULL
        ) com
          ON ST_Intersects(
               ST_Transform(ST_Point(n.lon, n.lat), 'EPSG:4326', 'EPSG:3857', true),
               com.geom)
        """
    insee_final = "n.code_insee" if needed["code_insee"] else "com.code_insee"
    con = get_duckdb_connection(memory=memory, threads=threads, load_spatial=True, tmp_dir=str(work))
    con.execute(f"CREATE TEMP TABLE oi_map(code VARCHAR, nom VARCHAR); INSERT INTO oi_map VALUES {oi_values};")
    query = f"""
    COPY (
      WITH raw AS (
        SELECT * FROM read_csv('{source}', delim='{delim}', header=true,
                               ignore_errors=true, strict_mode=false, null_padding=true,
                               parallel={parallel})
      ),
      norm AS (
        SELECT
          CAST({col('imb_id')} AS VARCHAR) AS imb_id,
          {insee_expr} AS code_insee,
          CAST({col('pm')} AS VARCHAR) AS pm_ref,
          CAST({col('oi')} AS VARCHAR) AS code_oi,
          CASE {whens} ELSE {etat_sql} END AS etat_imb,
          TRY_CAST({col('x')} AS DOUBLE) AS lon,
          TRY_CAST({col('y')} AS DOUBLE) AS lat,
          lower(trim(CAST({col('cat')} AS VARCHAR))) AS catg,
          upper(trim(CAST({col('type_imb')} AS VARCHAR))) AS type_imb
        FROM raw
      ),
      situe AS (
        SELECT n.*, {insee_final} AS code_insee_final
        FROM norm n
        {commune_join}
        WHERE n.imb_id IS NOT NULL
          AND n.etat_imb <> 'abandonne'
          AND n.lon IS NOT NULL AND n.lat IS NOT NULL
          AND lower(n.imb_id) <> 'imb_id'
      )
      SELECT
        s.imb_id AS id_imb,
        s.imb_id AS id_ban,
        s.code_insee_final AS code_insee,
        NULLIF(s.pm_ref, '') AS ref_pm,
        COALESCE(oi.nom, NULLIF(s.code_oi, ''), 'NON_RENSEIGNE') AS operateur_infra,
        s.code_oi AS code_oi_l331,
        s.etat_imb,
        CASE
          WHEN s.type_imb IN ('PA', 'PAVILLON', 'INDIVIDUEL') OR s.catg = 'individuel' THEN 1
          WHEN s.catg LIKE '12%' THEN 12
          WHEN s.catg LIKE 'entre 2%' THEN 6
          ELSE 1
        END AS nb_locaux,
        CASE WHEN s.etat_imb = 'raccordable_demande' THEN 'RAD' ELSE 'AUCUN' END AS motif_blocage,
        {int(trimestre)} AS trimestre,
        '{label}' AS trimestre_label,
        ST_X(ST_Transform(ST_Point(s.lon, s.lat), 'EPSG:4326', 'EPSG:2154', true)) AS x_l93,
        ST_Y(ST_Transform(ST_Point(s.lon, s.lat), 'EPSG:4326', 'EPSG:2154', true)) AS y_l93
      FROM situe s
      LEFT JOIN oi_map oi ON s.code_oi = oi.code
      {dept_filter}
    ) TO '{out_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """
    t0 = time.time()
    try:
        con.execute(query)
    finally:
        if holder.get("thread") is not None:
            holder["thread"].join(timeout=120)
        proc = holder.get("proc")
        err = ""
        if proc is not None and proc.stderr is not None:
            err = proc.stderr.read().decode("utf-8", "replace")
        con.close()
        shutil.rmtree(work, ignore_errors=True)
    if proc is not None and proc.returncode not in (0, None) and not out_parquet.exists():
        raise RuntimeError(f"{label} : unzip a echoue ({proc.returncode}) {err[:400]}")
    n = _count(out_parquet)
    step(f"{label}  parquet : {n} immeubles retenus -> {out_parquet.name} en {time.time() - t0:.0f} s")
    return {"trimestre": trimestre, "label": label, "lignes": n, "path": str(out_parquet)}


def _count(path: Path) -> int:
    con = get_duckdb_connection(load_spatial=False, memory="4GB", threads=2)
    try:
        return int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0])
    finally:
        con.close()


def _convert_communes(shp: Path, out: Path) -> int:
    step(f"Zonage communal : {shp.name}")
    con = get_duckdb_connection(load_spatial=True, memory="8GB", threads=4)
    # La classification est refaite en SQL, alignee sur classify_zone.
    con.execute(
        f"""
        COPY (
          SELECT
            lpad(CAST(INSEE_COM AS VARCHAR), 5, '0') AS code_insee,
            NOM_COM AS nom_commune,
            INSEE_DEP AS code_dept,
            zone AS zone_cartefibre,
            "L_33-13" AS engagement_l3313,
            OI_AMEL AS oi_amel,
            CASE
              WHEN upper(trim(zone)) = 'ZTD' OR ZTD_2013 = 1 THEN 'A_ZTD'
              WHEN trim(CAST("L_33-13" AS VARCHAR)) IN ('Orange', 'SFR', 'mixte') THEN 'B_AMII'
              WHEN COALESCE(OI_AMEL, 0) <> 0 THEN 'B_AMEL'
              WHEN upper(trim(zone)) = 'ZIPU' THEN 'C_RIP'
              WHEN upper(trim(zone)) = 'ZIPRI' THEN 'B_AMII'
              ELSE 'INCONNU'
            END AS zone_reglementaire,
            oi AS operateurs_presents,
            Locaux AS locaux,
            ftth AS locaux_ftth,
            ST_AsWKB(geom) AS geom_wkb
          FROM ST_Read('{shp}', open_options=['ENCODING=ISO-8859-1'])
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )
    n = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{out}')").fetchone()[0])
    con.close()
    step(f"Zonage : {n} communes -> {out}")
    return n


def _convert_zapm(shp: Path, out: Path, oi_names: dict[str, str]) -> int:
    step(f"Contours ZAPM : {shp.name}")
    con = get_duckdb_connection(load_spatial=True, memory="8GB", threads=4)
    oi_values = ", ".join(
        "('" + c.replace("'", "''") + "', '" + n.replace("'", "''") + "')"
        for c, n in sorted(oi_names.items())
    ) or "('__aucun__', '__aucun__')"
    con.execute(f"CREATE TEMP TABLE oi_map(code VARCHAR, nom VARCHAR); INSERT INTO oi_map VALUES {oi_values};")
    con.execute(
        f"""
        COPY (
          SELECT
            CAST(z.RefPM AS VARCHAR) AS ref_pm,
            CAST(z.CodeOI AS VARCHAR) AS code_oi,
            COALESCE(oi.nom, CAST(z.CodeOI AS VARCHAR), 'NON_RENSEIGNE') AS operateur_infra,
            lpad(CAST(z.INSEE_COM AS VARCHAR), 5, '0') AS code_insee_pm,
            CAST(z.EtatPM AS VARCHAR) AS etat_pm,
            ST_X(ST_Centroid(ST_Transform(z.geom, 'EPSG:3857', 'EPSG:2154', true))) AS x_pm_l93,
            ST_Y(ST_Centroid(ST_Transform(z.geom, 'EPSG:3857', 'EPSG:2154', true))) AS y_pm_l93
          FROM ST_Read('{shp}', open_options=['ENCODING=ISO-8859-1']) z
          LEFT JOIN oi_map oi ON CAST(z.CodeOI AS VARCHAR) = oi.code
          WHERE z.RefPM IS NOT NULL
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )
    n = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{out}')").fetchone()[0])
    con.close()
    step(f"ZAPM : {n} polygones (centroide Lambert-93) -> {out}")
    return n


def _extract_shp(zip_path: Path, dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(dest_dir)
    shapes = list(dest_dir.rglob("*.shp"))
    if not shapes:
        raise FileNotFoundError(f"Pas de shapefile dans {zip_path}")
    return shapes[0]


def _oi_resource(resources: list[dict]) -> dict | None:
    for resource in resources:
        title = (resource.get("title") or "").lower()
        if "opérateur" in title or "operateur" in title:
            if resource.get("url"):
                return resource
    return None


def prepare_real_arcep(
    base_dir: str | Path = "data",
    quarters: list[str] | None = None,
    departements: list[str] | None = None,
    force: bool = False,
    memory: str = "14GB",
    threads: int = 8,
) -> dict:
    """Telecharge, normalise et ecrit les entrees de la Brique 0.

    quarters=None : tous les millesimes Immeuble publies.
    departements=None : France entiere. Exemple : ["05", "2A"] pour un essai.
    """
    base = Path(base_dir)
    downloads = base / "raw_data" / "_downloads"
    imb_dir = base / "raw_data" / "01_arcep_imb_trimestriels"
    zapm_dir = base / "raw_data" / "02_arcep_sig_zapm_pm"
    zone_dir = base / "raw_data" / "03_arcep_zonage_ref"
    for folder in (downloads, imb_dir, zapm_dir, zone_dir):
        folder.mkdir(parents=True, exist_ok=True)

    resources = fetch_catalog()
    selected = select_resources(resources)
    for kind, items in selected.items():
        labels = ", ".join(sorted(items))
        step(f"Ressources {kind} : {len(items)} millesimes ({labels})")

    oi_res = _oi_resource(resources)
    oi_names: dict[str, str] = {}
    if oi_res:
        xlsx = download_file(oi_res["url"], downloads / "referentiel_oi.xlsx", force=force)
        oi_names = _load_oi_names(xlsx)
        step(f"Referentiel OI : {len(oi_names)} codes L. 33-1 rattaches a un nom d'operateur")
    else:
        step("Referentiel OI absent du catalogue : les codes L. 33-1 seront conserves tels quels")

    # Zonage et contours : dernier millesime (ancrage du cahier des charges).
    for kind, folder, converter in (
        ("commune", zone_dir, "commune"),
        ("zapm", zapm_dir, "zapm"),
    ):
        if not selected[kind]:
            raise RuntimeError(f"Aucune ressource {kind} dans le catalogue Arcep")
        label = sorted(selected[kind])[-1]
        resource = selected[kind][label]
        size = resource.get("filesize") or 0
        step(f"{kind} retenu : {label} ({resource.get('title')}, {size / 1e6:.1f} Mo)")
        archive = download_file(resource["url"], downloads / f"{label}-{kind}.zip", force=force)
        if kind == "commune":
            out = zone_dir / "reference_communes.parquet"
            if force or not out.exists():
                shp = _extract_shp(archive, downloads / f"{label}-commune-shp")
                _convert_communes(shp, out)
                shutil.rmtree(downloads / f"{label}-commune-shp", ignore_errors=True)
            else:
                step(f"DEJA LA  {out.name} — conversion sautee")
        else:
            out = zapm_dir / "zapm_pm.parquet"
            if force or not out.exists():
                shp = _extract_shp(archive, downloads / f"{label}-zapm-shp")
                _convert_zapm(shp, out, oi_names)
                shutil.rmtree(downloads / f"{label}-zapm-shp", ignore_errors=True)
            else:
                step(f"DEJA LA  {out.name} — conversion sautee")

    available = sorted(selected["immeuble"])
    if quarters is None:
        chosen = available
        step(f"Millesimes immeuble : TOUS les {len(chosen)} fichiers publies")
    else:
        unknown = [q for q in quarters if q not in selected["immeuble"]]
        if unknown:
            raise RuntimeError(f"Millesimes demandes absents du catalogue : {unknown}. Publies : {available}")
        chosen = list(quarters)
        step(f"Millesimes immeuble demandes : {', '.join(chosen)}")
    total_bytes = sum((selected["immeuble"][label].get("filesize") or 0) for label in chosen)
    step(
        f"Volume des archives immeuble selectionnees : {total_bytes / 1e9:.2f} Go "
        f"({len(chosen)} fichiers). Les fichiers deja converts sont sautes."
    )
    step("Le CSV decompresse (plusieurs Go par millesime) n'est pas conserve : seul le parquet reste.")

    published_idx = []
    for label in available:
        year, quarter = label.split("_T")
        published_idx.append(trimestre_index(int(year), int(quarter)))
    if published_idx:
        hole = [
            trimestre_label(t)
            for t in range(1, max(published_idx) + 1)
            if t not in set(published_idx)
        ]
        if hole:
            step(
                "Millesimes absents du serveur Arcep (la duree ne peut pas les observer) : "
                + ", ".join(hole)
            )

    produced = []
    for label in chosen:
        year, quarter = label.split("_T")
        t = trimestre_index(int(year), int(quarter))
        out = imb_dir / f"imb_t{t:02d}.parquet"
        resource = selected["immeuble"][label]
        size = (resource.get("filesize") or 0) / 1e6
        step(f"----- Immeuble {label} = t{t:02d} ({trimestre_label(t)}) ~{size:.0f} Mo compresses -----")
        if out.exists() and not force and _count(out) > 0:
            step(f"DEJA LA  {out.name} ({_count(out)} lignes) — telechargement et conversion sautes")
            produced.append({"trimestre": t, "label": label, "lignes": _count(out), "path": str(out)})
            continue
        archive = download_file(resource["url"], downloads / f"{label}-immeuble.zip", force=force)
        produced.append(
            _convert_immeuble_zip(
                archive, out, t, label, oi_names, departements, memory, threads,
                zonage_parquet=zone_dir / "reference_communes.parquet",
            )
        )

    t_max = max(item["trimestre"] for item in produced)
    summary = {
        "imb": produced,
        "t_min_observe": min(item["trimestre"] for item in produced),
        "t_max_observe": t_max,
        "t_max_label": trimestre_label(t_max),
        "zonage": str(zone_dir / "reference_communes.parquet"),
        "zapm": str(zapm_dir / "zapm_pm.parquet"),
        "departements": departements,
    }
    step(
        f"Acquisition terminee : {len(produced)} millesimes, "
        f"t={summary['t_min_observe']}..{t_max} ({trimestre_label(summary['t_min_observe'])} → {trimestre_label(t_max)})"
    )
    return summary


def verify_real_inputs(base_dir: str | Path = "data") -> dict:
    """Controle que les fichiers charges sont exploitables par les briques 0 a 3."""
    base = Path(base_dir)
    imb_dir = base / "raw_data" / "01_arcep_imb_trimestriels"
    zonage = base / "raw_data" / "03_arcep_zonage_ref" / "reference_communes.parquet"
    zapm = base / "raw_data" / "02_arcep_sig_zapm_pm" / "zapm_pm.parquet"
    files = sorted(imb_dir.glob("imb_t*.parquet"))
    step("========== VERIFICATION DU CHARGEMENT ==========")
    errors: list[str] = []
    if not files:
        errors.append(f"Aucun imb_tXX.parquet dans {imb_dir}")
    if not zonage.exists():
        errors.append(f"Zonage absent : {zonage}")
    if not zapm.exists():
        errors.append(f"ZAPM absent : {zapm}")
    if errors:
        for err in errors:
            step(f"ECHEC  {err}")
        raise RuntimeError("Chargement incomplet. " + " | ".join(errors))

    con = get_duckdb_connection(load_spatial=False, memory="8GB", threads=4)
    step(f"Millesimes imb : {len(files)} fichiers")
    total = 0
    etats: dict[str, int] = {}
    for path in files:
        n, t, label, n_pm, n_sans_pm = con.execute(
            f"""SELECT COUNT(*), MIN(trimestre), MIN(trimestre_label),
                       COUNT(DISTINCT ref_pm),
                       SUM(CASE WHEN ref_pm IS NULL OR ref_pm = '' THEN 1 ELSE 0 END)
                FROM read_parquet('{path}')"""
        ).fetchone()
        total += n
        step(f"  {path.name:16s}  t={t:<3} {label:8s}  {n:>10} lignes  {n_pm} PM  sans PM={n_sans_pm}")
        if n == 0:
            errors.append(f"{path.name} est vide")
        rows = con.execute(
            f"SELECT etat_imb, COUNT(*) n FROM read_parquet('{path}') GROUP BY 1"
        ).fetchall()
        for etat, k in rows:
            etats[etat] = etats.get(etat, 0) + k
            if etat not in {
                "etudes", "programme", "en_cours", "raccordable_demande", "raccordable", "inconnu"
            }:
                errors.append(f"{path.name} : etat non reconnu « {etat} »")
    step("Etats cumules (apres traduction IPE → modele) :")
    for etat, k in sorted(etats.items(), key=lambda kv: -kv[1]):
        step(f"  {etat:24s} {k:>12}  ({100 * k / max(total, 1):.2f} %)")

    step("Zonage communal :")
    for zone, n in con.execute(
        f"SELECT zone_reglementaire, COUNT(*) FROM read_parquet('{zonage}') GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall():
        step(f"  {zone:12s} {n}")
    retained = con.execute(
        f"""SELECT COUNT(*) FROM read_parquet('{zonage}')
            WHERE zone_reglementaire IN ('B_AMII','B_AMEL','C_RIP')"""
    ).fetchone()[0]
    if retained == 0:
        errors.append("Aucune commune en zone B ou C")

    n_zapm = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT ref_pm) FROM read_parquet('{zapm}')").fetchone()
    step(f"ZAPM : {n_zapm[0]} lignes, {n_zapm[1]} ref_pm distincts")
    glob = str(imb_dir / "imb_t*.parquet")
    matched, imb_pm = con.execute(
        f"""
        SELECT COUNT(DISTINCT i.ref_pm),
               (SELECT COUNT(DISTINCT ref_pm) FROM read_parquet('{glob}'))
        FROM read_parquet('{glob}') i
        INNER JOIN read_parquet('{zapm}') z ON i.ref_pm = z.ref_pm
        """
    ).fetchone()
    rate = matched / imb_pm if imb_pm else 0
    step(f"PM des immeubles retrouves dans les contours ZAPM : {matched}/{imb_pm} ({100 * rate:.1f} %)")
    if imb_pm and rate < 0.5:
        errors.append(f"Moins de 50 % des PM imb sont dans le shapefile ZAPM ({100 * rate:.1f} %)")

    # Coordonnees Lambert-93 plausibles (metropole + DROM approximatif).
    bad_xy = con.execute(
        f"""SELECT COUNT(*) FROM read_parquet('{glob}')
            WHERE x_l93 IS NULL OR y_l93 IS NULL OR x_l93 < 100000 OR y_l93 < 1000000"""
    ).fetchone()[0]
    step(f"Coordonnees Lambert-93 hors emprise ou nulles : {bad_xy} / {total}")
    if total and bad_xy / total > 0.05:
        errors.append(f"{bad_xy} coordonnees Lambert-93 invalides ({100 * bad_xy / total:.1f} %)")

    con.close()
    if errors:
        step("========== ECHECS ==========")
        for err in errors:
            step(f"ECHEC  {err}")
        raise RuntimeError("Verification du chargement en echec. " + " | ".join(errors))
    step("========== CHARGEMENT VALIDE : les briques peuvent demarrer ==========")
    return {
        "fichiers_imb": len(files),
        "lignes_imb": total,
        "etats": etats,
        "zapm": n_zapm[1],
        "pm_apparies": matched,
        "communes_bc": retained,
    }


# Alias historiques gardes pour les scripts deja ecrits.
def scan_arcep_index(url: str = DATASET_API) -> list[str]:
    try:
        return sorted(select_resources(fetch_catalog())["immeuble"])
    except Exception as exc:
        step(f"Catalogue indisponible : {exc}")
        return []


def ingest_public_databases(base_dir: str | Path = "data", **kwargs) -> dict:
    return prepare_real_arcep(base_dir=base_dir, **kwargs)
