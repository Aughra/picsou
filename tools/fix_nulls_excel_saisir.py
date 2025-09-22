############################################################
# CHANGELOG:
# - [2025-09-22] agt019 {author=agent} {reason: correctif global — remplacer les NULL numériques par 0 dans dbo.excel_saisir}
# - Impact: élimine les NULL dans toutes les colonnes DECIMAL(20,2), corrige toutes les vues d’un coup
# - Tests: exécution après run.sh; re-check avec tools/check_views_values.py
############################################################

from __future__ import annotations

from typing import Dict

import pandas as pd

import os
import sys

# Assurer l'import des modules du dépôt picsou
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from logger_config import logger
from src.db import get_conn
from tools.sync_excel_saisir import infer_types, normalize_col, SCHEMA, TABLE_NAME


def _load_one_row_df() -> pd.DataFrame:
    """Récupère l’en-tête de la vue principale pour inférer les types via la même logique que la sync."""
    # On utilise la vue pour obtenir les noms d'origine (ordre identique)
    with get_conn() as con:
        with con.cursor() as cur:
            cur.execute(f"SELECT TOP 1 * FROM [{SCHEMA}].[v_excel_saisir]")
            cur.fetchone()
            cols = [d[0] for d in (cur.description or [])]
    # Crée un DF vide avec ces colonnes
    return pd.DataFrame(columns=cols)


def fix_nulls() -> None:
    df = _load_one_row_df()
    types = infer_types(df)
    # Colonnes numériques d’après la logique sync
    num_cols = [
        c for c, t in types.items() if c.lower() != "date" and t == "DECIMAL(20,2)"
    ]
    if not num_cols:
        logger.info("Aucune colonne numérique détectée via la vue — rien à corriger.")
        return
    norm_map: Dict[str, str] = {orig: normalize_col(orig) for orig in num_cols}

    with get_conn() as con:
        cur = con.cursor()
        fixed_total = 0
        for orig, norm in norm_map.items():
            try:
                cur.execute(
                    f"UPDATE [{SCHEMA}].[{TABLE_NAME}] SET [{norm}] = 0 WHERE [{norm}] IS NULL"
                )
                # Si possible, compter modifs
                cur.execute(
                    f"SELECT COUNT(*) FROM [{SCHEMA}].[{TABLE_NAME}] WHERE [{norm}] IS NULL"
                )
                row = cur.fetchone()
                remaining = int(row[0]) if row and row[0] is not None else 0
                logger.info("Colonne %s: NULL restants après fix: %d", orig, remaining)
                fixed_total += remaining
            except Exception as e:
                logger.error("Fix NULL→0 échoué pour %s: %s", orig, e)
        con.commit()
    logger.info(
        "Correction terminée. Vérifiez les vues avec tools/check_views_values.py."
    )


def main() -> None:
    fix_nulls()


if __name__ == "__main__":
    main()
