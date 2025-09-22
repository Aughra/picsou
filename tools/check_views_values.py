############################################################
# CHANGELOG:
# - [2025-09-22] agt018 {author=agent} {reason: vérification des vues par monnaie — détection de NULL}
# - Impact: permet d'auditer v_excel_saisir_<coin> et de repérer les valeurs NULL restantes
# - Tests: exécution locale; compte les NULL pour chaque coin, affiche un échantillon de dates
# - Notes: utiliser après run.sh pour s'assurer que les séries sont complètes
############################################################

from __future__ import annotations

from typing import Dict, List, Optional, Tuple
import os
import sys

# Assurer l'import des modules du dépôt picsou
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from logger_config import logger
from src.db import get_conn

SCHEMA = "dbo"
VIEW_PREFIX = "v_excel_saisir"
COINS = ["btc", "eth", "avax", "dot", "ada", "sol", "xrp"]


def _get_columns(view: str) -> List[str]:
    with get_conn() as con:
        with con.cursor() as cur:
            cur.execute(f"SELECT TOP 1 * FROM [{SCHEMA}].[{view}]")
            cur.fetchone()
            cols = [d[0] for d in (cur.description or [])]
    return cols


def _find_value_column(cols: List[str], coin: str) -> Optional[str]:
    low_map: Dict[str, str] = {c.lower().strip(): c for c in cols}
    # Priorité: "<coin> valeur"
    key = f"{coin} valeur"
    if key in low_map:
        return low_map[key]
    # Sinon: toute colonne qui commence par coin et contient "valeur"
    for k, orig in low_map.items():
        if k.startswith(coin) and "valeur" in k:
            return orig
    # Dernier recours: "valeur <COIN>" (majuscule)
    alt = f"valeur {coin.upper()}"
    if alt in low_map:
        return low_map[alt]
    return None


def check_view(
    coin: str, limit_sample: int = 5
) -> Tuple[int, List[Tuple[object, object]]]:
    view = f"{VIEW_PREFIX}_{coin}"
    cols = _get_columns(view)
    col_val = _find_value_column(cols, coin)
    if not col_val:
        raise RuntimeError(
            f"Colonne 'valeur' introuvable dans [{SCHEMA}].[{view}] (coin={coin})."
        )
    sql_count = f"SELECT COUNT(*) FROM [{SCHEMA}].[{view}] WHERE [date] <= CONVERT(date, GETDATE()) AND [{col_val}] IS NULL"
    sql_sample = (
        f"SELECT TOP {limit_sample} [date], [{col_val}] FROM [{SCHEMA}].[{view}] "
        f"WHERE [date] <= CONVERT(date, GETDATE()) AND [{col_val}] IS NULL ORDER BY [date] ASC"
    )
    with get_conn() as con:
        with con.cursor() as cur:
            cur.execute(sql_count)
            row = cur.fetchone()
            n_null = int(row[0]) if row and row[0] is not None else 0
            samples: List[Tuple[object, object]] = []
            if n_null:
                cur.execute(sql_sample)
                samples = cur.fetchall() or []
    return n_null, samples


def main() -> None:
    has_issue = False
    for coin in COINS:
        try:
            n, samples = check_view(coin)
            if n:
                has_issue = True
                logger.warning(
                    "%s: %d valeur(s) NULL (exemples: %s)",
                    coin,
                    n,
                    ", ".join(str(s[0]) for s in samples),
                )
            else:
                logger.info("%s: OK — aucune valeur NULL", coin)
        except Exception as e:
            has_issue = True
            logger.error("%s: échec de vérification: %s", coin, e)
    if has_issue:
        logger.warning(
            "Des valeurs NULL subsistent dans certaines vues. Lancez fix_nulls_excel_saisir.py puis re-vérifiez."
        )
    else:
        logger.info("Toutes les vues sont complètes (0 au lieu de NULL).")


if __name__ == "__main__":
    main()
