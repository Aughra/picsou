############################################################
# CHANGELOG:
# - [2025-09-21] agt017 {author=agent} {reason: éviter les NULLs → forcer 0 pour colonnes numériques + commits}
# - Impact: les champs numériques (ex: "btc valeur") ne restent plus NULL; nettoyage des NULL existants après sync
# - Tests: relance de la sync puis SELECT sur v_excel_saisir_btc → absence de NULL
# - [2025-09-21] agt005 {author=agent} {reason: ajout de vues par monnaie (btc/eth/avax/dot/ada/sol/xrp)}
# - Impact: crée v_excel_saisir_<coin> avec uniquement les colonnes associées à chaque coin (+ date)
# - Tests: sync exécutée; SELECT de validation sur chaque vue
# - [2025-09-21] agt006 {author=agent} {reason: ajout vues totaux et positions (quantités par coin)}
# - Impact: v_excel_saisir_totaux (date + totaux) et v_excel_positions (date + qty_<coin>)
# - Tests: création via sync; SELECT TOP 1 sur chacune
# - [2025-09-21] agt004 {author=agent} {reason: nettoyage colonne parasite 'index'}
# - Impact: DROP COLUMN [index] si présente et non attendue; la vue n'expose plus 'index'
# - Tests: relance de la sync et vérification des colonnes de la vue
# - [2025-09-21] agt003 {author=agent} {reason: source DB via builder (plus de CSV)}
# - Impact: build_saisir() génère le DataFrame; la table/vue sont mises à jour en conséquence
# - Tests: exécution post-pipeline; SELECT sur dbo.v_excel_saisir
# - Notes: entêtes identiques à la feuille attendue (btc, eth, avax, dot, ada, sol, xrp)
# - [2025-09-21] agt002 {author=agent} {reason: corriger création de vue MSSQL (batch CREATE VIEW en premier)}
# - Impact: exécute DROP VIEW et CREATE VIEW dans deux batches séparés ; évite l'erreur 111
# - Tests: exécution du script de sync sans erreur ; vue recréée correctement
# - Notes: contrainte SQL Server: CREATE VIEW doit être la première instruction du batch
# - [2025-09-21] agt001 {author=agent} {reason: sync feuille Excel 'Saisir' → table MSSQL + vue}
# - Impact: crée/altère dbo.excel_saisir (noms normalisés) et dbo.v_excel_saisir (noms identiques au CSV), charge via MERGE par date
# - Tests: testé localement avec un CSV exporté; MERGE idempotent; ajout automatique de nouvelles colonnes
# - Notes: types: date→DATE, numériques→DECIMAL(20,2); autres→NVARCHAR(255)
############################################################

from __future__ import annotations

import re
from typing import Any, Dict, List

import pandas as pd

import sys
import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from logger_config import logger
from src.db import get_conn
from src.build_saisir import build_saisir


TABLE_NAME = "excel_saisir"
VIEW_NAME = "v_excel_saisir"
SCHEMA = "dbo"
COINS = ["btc", "eth", "avax", "dot", "ada", "sol", "xrp"]


def normalize_col(name: str) -> str:
    """Normalise un nom de colonne pour MSSQL (snake_case, ascii, sans espaces).

    - Remplace accents et caractères spéciaux par des équivalents sûrs (approximation simple).
    - Remplace les espaces et séparateurs par "_"; supprime ce qui n'est pas alphanumérique/_
    - Si commence par un chiffre, préfixe par "c_".
    - Corrige quelques cas connus (avx→avax, dot variants).
    """

    original = name
    s = name.strip().lower()
    replacements = {
        "é": "e",
        "è": "e",
        "ê": "e",
        "à": "a",
        "ù": "u",
        "ç": "c",
        "œ": "oe",
    }
    for k, v in replacements.items():
        s = s.replace(k, v)
    # Corrections sémantiques
    s = s.replace("avx", "avax").replace("dot", "dot")
    s = re.sub(r"[\s\-/]+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = "col"
    if s[0].isdigit():
        s = f"c_{s}"
    logger.debug(f"normalize_col: '{original}' -> '{s}'")
    return s


def infer_types(df: pd.DataFrame) -> Dict[str, str]:
    """Infère des types MSSQL pour chaque colonne.

    Règles:
    - Si la colonne s'appelle exactement "date" (insensible à la casse) → DATE
    - Si convertible en float pour toutes les lignes non vides → DECIMAL(20,2)
    - Sinon → NVARCHAR(255)
    """

    types: Dict[str, str] = {}
    for col in df.columns:
        if col.strip().lower() == "date":
            types[col] = "DATE"
            continue
        # Test num
        series = df[col].dropna().astype(str)
        is_numeric = True
        for v in series:
            try:
                float(v)
            except ValueError:
                is_numeric = False
                break
        types[col] = "DECIMAL(20,2)" if is_numeric else "NVARCHAR(255)"
    return types


def get_existing_columns(cursor: Any, schema: str, table: str) -> List[str]:
    cursor.execute(
        """
        SELECT c.name
        FROM sys.columns c
        JOIN sys.objects o ON c.object_id = o.object_id
        JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE s.name = %s AND o.name = %s
        ORDER BY c.column_id
        """,
        (schema, table),
    )
    rows = cursor.fetchall() or []
    return [r[0] for r in rows]


def ensure_table_and_view(df: pd.DataFrame, original_cols: List[str]) -> None:
    """Crée ou adapte la table normalisée et la vue exposant les noms finaux.

    Table: dbo.excel_saisir (noms normalisés, types inférés)
    Vue: dbo.v_excel_saisir (noms originaux et ordre identique à la feuille finale)
    """

    with get_conn() as conn:
        cur = conn.cursor()

        # Mapping colonnes originales -> normalisées
        norm_map: Dict[str, str] = {orig: normalize_col(orig) for orig in original_cols}

        # Types inférés
        types = infer_types(df)

        # Créer la table si absente (clé primaire sur [date])
        cur.execute(
            f"""
            IF OBJECT_ID('{SCHEMA}.{TABLE_NAME}','U') IS NULL
            BEGIN
                CREATE TABLE [{SCHEMA}].[{TABLE_NAME}] (
                    [date] DATE NOT NULL PRIMARY KEY
                );
            END
            """
        )

        # S'assurer que la table existe et ajouter automatiquement les colonnes manquantes
        existing = set(get_existing_columns(cur, SCHEMA, TABLE_NAME))
        for orig in original_cols:
            if orig.lower() == "date":
                continue
            col = norm_map[orig]
            if col not in existing:
                col_type = types[orig]
                logger.info(f"Ajout colonne manquante: {col} {col_type}")
                cur.execute(
                    f"ALTER TABLE [{SCHEMA}].[{TABLE_NAME}] ADD [{col}] {col_type} NULL"
                )

        # Nettoyage: supprimer une éventuelle colonne parasite 'index' si elle n'est pas attendue
        expected_norm_cols = {"date"} | {
            norm_map[o] for o in original_cols if o.lower() != "date"
        }
        existing_after = set(get_existing_columns(cur, SCHEMA, TABLE_NAME))
        if "index" in existing_after and "index" not in expected_norm_cols:
            logger.info("Suppression de la colonne parasite: index")
            cur.execute(f"ALTER TABLE [{SCHEMA}].[{TABLE_NAME}] DROP COLUMN [index]")

        # Créer/Mettre à jour la vue exposant les noms originaux (ordre CSV)
        select_list: List[str] = []
        for orig in original_cols:
            if orig.lower() == "date":
                select_list.append("[date] AS [date]")
            else:
                select_list.append(f"[{norm_map[orig]}] AS [{orig}]")
        select_sql = ", ".join(select_list)
        # DROP VIEW et CREATE VIEW doivent être dans des batches séparés (CREATE VIEW en premier statement)
        cur.execute(
            f"""
            IF OBJECT_ID('{SCHEMA}.{VIEW_NAME}', 'V') IS NOT NULL
              DROP VIEW [{SCHEMA}].[{VIEW_NAME}];
            """
        )
        cur.execute(
            f"""
            CREATE VIEW [{SCHEMA}].[{VIEW_NAME}] AS
            SELECT {select_sql} FROM [{SCHEMA}].[{TABLE_NAME}];
            """
        )

        # Vues par monnaie: v_excel_saisir_<coin>
        for coin in COINS:
            # Colonnes attendues pour ce coin dans l'ordre
            wanted = [
                f"{coin} acheté",
                f"{coin} valeur",
                f"{coin} gain perte",
                f"{coin} acheté cumul",
                f"{coin} valeur cumul",
                f"acheté {coin.upper()}",
                f"Valeur {coin.upper()}",
            ]
            # Filtrer aux colonnes présentes dans df
            coin_cols = [c for c in wanted if c in original_cols]
            if not coin_cols:
                continue
            # Construire le SELECT (date + colonnes du coin)
            coin_select_parts: List[str] = ["[date] AS [date]"]
            for orig in coin_cols:
                norm = norm_map.get(orig) or normalize_col(orig)
                coin_select_parts.append(f"[{norm}] AS [{orig}]")
            coin_select_sql = ", ".join(coin_select_parts)

            view_coin = f"{VIEW_NAME}_{coin}"
            # DROP en batch séparé
            cur.execute(
                f"""
                IF OBJECT_ID('{SCHEMA}.{view_coin}', 'V') IS NOT NULL
                  DROP VIEW [{SCHEMA}].[{view_coin}];
                """
            )
            # CREATE en batch dédié
            cur.execute(
                f"""
                CREATE VIEW [{SCHEMA}].[{view_coin}] AS
                SELECT {coin_select_sql} FROM [{SCHEMA}].[{TABLE_NAME}];
                """
            )

        # Vue TOTEAUX: date + total acheté/valeur/gain perte
        tot_labels = ["total acheté", "total valeur", "total gain perte"]
        tot_present = [lbl for lbl in tot_labels if lbl in original_cols]
        if tot_present:
            tot_select_parts: List[str] = ["[date] AS [date]"]
            for orig in tot_present:
                norm = norm_map[orig]
                tot_select_parts.append(f"[{norm}] AS [{orig}]")
            tot_select_sql = ", ".join(tot_select_parts)
            view_tot = f"{VIEW_NAME}_totaux"
            cur.execute(
                f"""
                IF OBJECT_ID('{SCHEMA}.{view_tot}', 'V') IS NOT NULL
                  DROP VIEW [{SCHEMA}].[{view_tot}];
                """
            )
            cur.execute(
                f"""
                CREATE VIEW [{SCHEMA}].[{view_tot}] AS
                SELECT {tot_select_sql} FROM [{SCHEMA}].[{TABLE_NAME}];
                """
            )

        # Vue POSITIONS: date + qty_<coin> calculée depuis transactions (cumul jusqu'à la date)
        # Utilise la table des dates de excel_saisir et un CTE cumulé sur transactions en Europe/Paris
        def _positions_view_sql() -> str:
            cols_sql = ",\n  ".join(
                [
                    f"(SELECT TOP 1 qty_cum FROM cum WHERE symbol = '{coin}' AND date_local <= es.[date] ORDER BY date_local DESC) AS qty_{coin}"
                    for coin in COINS
                ]
            )
            return f"""
WITH tx AS (
  SELECT LOWER(symbol) AS symbol,
         CONVERT(date, (date_utc AT TIME ZONE 'UTC') AT TIME ZONE 'Romance Standard Time') AS date_local,
         qty
    FROM transactions
),
agg AS (
  SELECT symbol, date_local, SUM(qty) AS qty_day
    FROM tx
   GROUP BY symbol, date_local
),
cum AS (
  SELECT symbol, date_local,
         SUM(qty_day) OVER (PARTITION BY symbol ORDER BY date_local ROWS UNBOUNDED PRECEDING) AS qty_cum
    FROM agg
)
SELECT es.[date],
  {cols_sql}
  FROM [{SCHEMA}].[{TABLE_NAME}] es
"""

        view_pos = "v_excel_positions"
        cur.execute(
            f"""
            IF OBJECT_ID('{SCHEMA}.{view_pos}', 'V') IS NOT NULL
              DROP VIEW [{SCHEMA}].[{view_pos}];
            """
        )
        cur.execute(
            f"""
            CREATE VIEW [{SCHEMA}].[{view_pos}] AS
            {_positions_view_sql()}
            """
        )

        # Commit explicite des DDL afin d'assurer la visibilité immédiate des vues/colonnes
        conn.commit()


def load_db_dataframe() -> pd.DataFrame:
    df = build_saisir()
    exact_col = next((c for c in df.columns if c.lower() == "date"), None)
    if exact_col:
        dt_series = pd.to_datetime(df[exact_col], errors="coerce")
        df[exact_col] = dt_series.dt.date  # type: ignore[attr-defined]
    return df


def upsert_rows(df: pd.DataFrame, original_cols: List[str]) -> None:
    norm_map: Dict[str, str] = {orig: normalize_col(orig) for orig in original_cols}
    types = infer_types(df)

    with get_conn() as conn:
        cur = conn.cursor()
        for _, row in df.iterrows():
            # Build MERGE par date
            # ON: date
            # UPDATE/INSERT: toutes les autres colonnes
            assignments = []
            for orig in original_cols:
                if orig.lower() == "date":
                    continue
                col = norm_map[orig]
                assignments.append(f"TARGET.[{col}] = %s")

            update_set = ", ".join(assignments)
            insert_cols = ["date"] + [
                norm_map[o] for o in original_cols if o.lower() != "date"
            ]
            insert_placeholders = ["%s"] * len(insert_cols)

            merge_sql = f"""
            MERGE [{SCHEMA}].[{TABLE_NAME}] AS TARGET
            USING (SELECT %s AS [date]) AS SOURCE
            ON (TARGET.[date] = SOURCE.[date])
            WHEN MATCHED THEN
              UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
              INSERT ({', '.join('['+c+']' for c in insert_cols)})
              VALUES ({', '.join(insert_placeholders)});
            """

            # Valeurs paramétrées
            date_val = None
            for c in original_cols:
                if c.lower() == "date":
                    date_val = row[c]
                    break

            # Convertir NaN/None → 0 pour numériques; sinon None
            def _to_db(col_name: str, v: Any) -> Any:
                try:
                    import math

                    if v is None:
                        return 0 if types.get(col_name) == "DECIMAL(20,2)" else None
                    if isinstance(v, float) and math.isnan(v):
                        return 0 if types.get(col_name) == "DECIMAL(20,2)" else None
                except Exception:
                    pass
                return v

            update_vals = [
                _to_db(o, row[o]) for o in original_cols if o.lower() != "date"
            ]
            insert_vals = [date_val] + update_vals

            try:
                cur.execute(merge_sql, tuple([date_val] + update_vals + insert_vals))
            except Exception as e:
                logger.error(f"MERGE échoué pour date={date_val}: {e}")
                raise

        # Commit des MERGE
        conn.commit()


def cleanup_nulls_numeric(df: pd.DataFrame, original_cols: List[str]) -> None:
    """Remplace en base les NULL des colonnes numériques par 0 pour garantir des séries complètes."""
    types = infer_types(df)
    num_cols = [c for c in original_cols if types.get(c) == "DECIMAL(20,2)"]
    if not num_cols:
        return
    norm_map: Dict[str, str] = {orig: normalize_col(orig) for orig in num_cols}
    with get_conn() as conn:
        cur = conn.cursor()
        for orig, norm in norm_map.items():
            try:
                cur.execute(
                    f"UPDATE [{SCHEMA}].[{TABLE_NAME}] SET [{norm}] = 0 WHERE [{norm}] IS NULL"
                )
            except Exception as e:
                logger.error("Nettoyage NULL→0 échoué pour %s: %s", orig, e)
        conn.commit()


def main() -> None:
    logger.info("Construction de la feuille 'Saisir' depuis DB…")
    df = load_db_dataframe()
    original_cols: List[str] = list(df.columns)
    ensure_table_and_view(df.head(2000), original_cols)
    upsert_rows(df, original_cols)
    # Nettoyage final pour éviter les trous de données
    cleanup_nulls_numeric(df, original_cols)
    logger.info(
        f"Synchronisation terminée → table [{SCHEMA}].[{TABLE_NAME}] et vue [{SCHEMA}].[{VIEW_NAME}]"
    )


if __name__ == "__main__":
    main()
