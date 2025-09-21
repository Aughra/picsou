"""
Exporte des données depuis MariaDB vers une base MySQL pour permettre la visualisation dans Excel.

Tables mises à jour :
  - portfolio_latest : vue v_portfolio_latest (photo courante)
  - portfolio_timeseries : total du portefeuille dans le temps
  - portfolio_history : historique détaillé (optionnel, peut être lourd)

Dépendances Python : pandas, SQLAlchemy, PyMySQL, python-dotenv (facultatif)
Variables d'environnement DB_* lues par src.db.get_engine() :
  DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
---
Changelog:
- 2025-09-20 19:40 (Europe/Paris) — [Agent] Remplacement de l'export Excel par l'insertion dans une base MySQL.
"""

import os
import pandas as pd

# On réutilise l'engine existant du projet
try:
    from src.db import get_engine
except ImportError:
    from db import get_engine  # fallback si l'import relatif n'est pas utilisé


def export_to_mysql(include_history: bool = True):
    eng = get_engine()

    # Requêtes principales
    sql_latest = (
        "SELECT ts, symbol, qty, investi, price_eur, valeur_actuelle, pnl_eur, pnl_pct "
        "FROM v_portfolio_latest ORDER BY symbol"
    )

    sql_timeseries = (
        "SELECT ts, SUM(valeur_actuelle) AS total_eur, SUM(investi) AS investi_eur "
        "FROM portfolio_snapshot GROUP BY ts ORDER BY ts"
    )

    sql_history = (
        "SELECT ts, symbol, qty, investi, price_eur, valeur_actuelle, pnl_eur, pnl_pct "
        "FROM portfolio_snapshot ORDER BY ts, symbol"
    )

    print("[DB] Lecture des données…")
    df_latest = pd.read_sql(sql_latest, eng)
    df_timeseries = pd.read_sql(sql_timeseries, eng)
    df_history = pd.read_sql(sql_history, eng) if include_history else None

    print("[DB] Insertion des données dans MySQL…")
    df_latest.to_sql("portfolio_latest", eng, if_exists="replace", index=False)
    df_timeseries.to_sql("portfolio_timeseries", eng, if_exists="replace", index=False)
    if df_history is not None:
        df_history.to_sql("portfolio_history", eng, if_exists="replace", index=False)

    print("[OK] Données insérées dans la base MySQL.")


def main():
    # Permettre de désactiver l'onglet history via variable d'env (pour gros jeux de données)
    include_history = os.getenv("PICSOU_INCLUDE_HISTORY", "1") not in {
        "0",
        "false",
        "False",
    }

    export_to_mysql(include_history=include_history)
    print("Les données ont été exportées vers la base MySQL.")


if __name__ == "__main__":
    main()
