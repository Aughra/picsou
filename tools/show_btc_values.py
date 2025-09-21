############################################################
# CHANGELOG:
# - [2025-09-21] agt015 {author=agent} {reason: script d’inspection de la vue v_excel_saisir_btc}
# - Impact: permet d’afficher la colonne « btc valeur » depuis MSSQL
# - Tests: exécution locale; affichage des 10 dernières lignes
# - Notes: utilise pymssql via get_conn(); tri par date croissante
# - [2025-09-21] agt016 {author=agent} {reason: filtrer aux dates <= aujourd’hui pour éviter lignes futures parasites}
# - Impact: l’affichage montre les valeurs pertinentes pour les graphes
############################################################

from __future__ import annotations

from typing import List, Tuple, Optional

from logger_config import logger

try:
    from src.db import get_conn
except Exception:  # pragma: no cover
    from db import get_conn  # type: ignore


def _detect_btc_value_column() -> Optional[str]:
    with get_conn() as con:
        with con.cursor() as cur:
            cur.execute("SELECT TOP 1 * FROM [picsou].[dbo].[v_excel_saisir_btc]")
            cur.fetchone()
            cols = [d[0] for d in (cur.description or [])]
    lowered = {c.lower().strip(): c for c in cols}
    if "btc valeur" in lowered:
        return lowered["btc valeur"]
    for k, orig in lowered.items():
        if k.startswith("btc") and "valeur" in k:
            return orig
    return None


def fetch_btc_values(limit: int = 0) -> List[Tuple[object, object]]:
    # Essai direct
    sql = (
        "SELECT [date], [btc valeur] FROM [picsou].[dbo].[v_excel_saisir_btc] "
        "WHERE [date] <= CONVERT(date, GETDATE()) ORDER BY [date] ASC"
    )
    rows: List[Tuple[object, object]] = []
    try:
        with get_conn() as con:
            with con.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall() or []
    except Exception:
        # Fallback avec détection
        col = _detect_btc_value_column()
        if not col:
            raise RuntimeError("Impossible d’identifier la colonne 'btc valeur'.")
        quoted = col.replace("]", "]]")
        dyn_sql = (
            f"SELECT [date], [{quoted}] FROM [picsou].[dbo].[v_excel_saisir_btc] "
            "WHERE [date] <= CONVERT(date, GETDATE()) ORDER BY [date] ASC"
        )
        with get_conn() as con:
            with con.cursor() as cur:
                cur.execute(dyn_sql)
                rows = cur.fetchall() or []
    if limit and limit > 0:
        rows = rows[-limit:]
    return rows


def main() -> None:
    vals = fetch_btc_values()
    if not vals:
        logger.info("Aucune ligne dans la vue v_excel_saisir_btc")
        return
    logger.info("Affichage des %d lignes (date, btc valeur):", len(vals))
    for d, v in vals:
        print(f"{d} ; {v}")


if __name__ == "__main__":
    main()
