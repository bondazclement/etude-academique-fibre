"""Utilitaires IO : connexions DuckDB, dossiers, checkpoints parquet."""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger("ftth")


def get_logger() -> logging.Logger:
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def ensure_dirs(*paths: str | Path) -> None:
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)


def get_duckdb_connection(
    memory: str = "14GB",
    threads: int = 8,
    load_spatial: bool = True,
    load_httpfs: bool = False,
    db_path: str = ":memory:",
    tmp_dir: str | None = None,
):
    """Connexion DuckDB out-of-core avec extensions optionnelles.

    Les extensions sont installees au mieux : en environnement sans acces
    reseau, l'-installation peut echouer et on degrade gracieusement
    (la Brique 0 bascule alors sur jointure non-spatiale).
    """
    import duckdb

    log = get_logger()
    con = duckdb.connect(database=db_path)
    try:
        con.execute(f"PRAGMA threads={int(threads)};")
    except Exception:
        pass
    try:
        con.execute(f"PRAGMA memory_limit='{memory}';")
    except Exception:
        pass
    if tmp_dir:
        try:
            Path(tmp_dir).mkdir(parents=True, exist_ok=True)
            con.execute(f"PRAGMA temp_directory='{tmp_dir}';")
        except Exception as e:
            log.warning("PRAGMA temp_directory ignore : %s", e)
    if load_spatial:
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
        except Exception as e:
            log.warning("Extension DuckDB 'spatial' indisponible (%s).", e)
    if load_httpfs:
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception as e:
            log.warning("Extension DuckDB 'httpfs' indisponible (%s).", e)
    return con


def duckdb_has_spatial(con) -> bool:
    try:
        con.execute("SELECT ST_Point(0, 0);").fetchone()
        return True
    except Exception:
        return False


def count_parquet(con, path: str) -> int:
    try:
        return int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0])
    except Exception:
        return 0


def parquet_columns(con, path: str) -> list[str]:
    try:
        rows = con.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 0").description
        return [r[0] for r in rows]
    except Exception:
        return []


def checkpoint_ok(path: str | Path, con=None) -> bool:
    p = Path(path)
    if not p.exists():
        return False
    # Accepte fichier unique ou dossier partitionne (glob interne).
    if p.is_dir():
        return any(p.glob("*.parquet"))
    if p.suffix != ".parquet":
        return p.stat().st_size > 0
    if p.stat().st_size == 0:
        return False
    if con is not None:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{p}')").fetchone()[0]
            return int(n) > 0
        except Exception:
            return False
    return True


def write_copy_parquet(con, query: str, out_path: str | Path) -> None:
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({query}) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD);")
