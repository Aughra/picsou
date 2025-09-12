"""
Ce script calcule un "snapshot" (photo instantanée) d'un portefeuille d'actifs.

Fonctions principales :
1. Lire les transactions (achats/ventes) et le dernier prix connu de chaque actif.
2. Calculer pour chaque symbole :
   - la quantité totale détenue
   - le montant investi
   - la valeur actuelle
   - le PnL (Profit and Loss)
3. Exporter les résultats :
   - CSV : reports/snapshot.csv

Usage :
- Lancer le script directement : python compute_report.py
- Vérifier le fichier généré dans le dossier "reports/"
"""

from pathlib import Path  # stdlib
from typing import cast

import pandas as pd
from pandas import DataFrame, Series

from datetime import datetime, timezone
from sqlalchemy import text

from src.db import get_engine  # local


def load_data() -> tuple[DataFrame, DataFrame]:
    """
    Charge les données nécessaires depuis la base:
    - transactions: liste des opérations avec date, symbole, quantité, prix et frais
    - last: dernier prix connu pour chaque symbole (snapshot)
    Retourne deux DataFrames pandas.
    """
    eng = get_engine()  # Connexion à la base de données
    with eng.begin() as con:
        # Chargement des transactions
        tx = pd.read_sql(
            "SELECT date_utc, symbol, qty, price_eur, fee_eur FROM transactions",
            con,
        )
        # Chargement du dernier prix par symbole
        last = pd.read_sql(
            """
            SELECT p.symbol, p.price_eur
            FROM price_snapshot p
            JOIN (
              SELECT symbol, MAX(ts) AS mts FROM price_snapshot GROUP BY symbol
            ) b ON p.symbol=b.symbol AND p.ts=b.mts
            """,
            con,
        )
    # Normaliser les symboles en minuscules pour éviter les problèmes de jointure (ex: ETH vs eth)
    tx["symbol"] = tx["symbol"].str.lower()
    last["symbol"] = last["symbol"].str.lower()
    return tx, last


def build_report(tx: DataFrame, last: DataFrame) -> DataFrame:
    """
    Construit un rapport synthétique à partir des transactions et des derniers prix.
    Calcule pour chaque symbole:
    - la quantité totale détenue
    - le montant investi (coût total)
    - la valeur actuelle (quantité * dernier prix)
    - le PnL (profit ou perte)
    Retourne un DataFrame avec ces informations.
    """
    tx = tx.copy()
    tx["symbol"] = tx["symbol"].str.lower()
    last = last.copy()
    last["symbol"] = last["symbol"].str.lower()

    # Nettoyer et convertir en nombres les colonnes numériques
    for col in ["qty", "price_eur", "fee_eur"]:
        if col in tx.columns:
            # Enlever les espaces dans les chaînes avant conversion
            tx[col] = tx[col].apply(lambda v: v.strip() if isinstance(v, str) else v)
            # Convertir en numérique, mettre NaN si erreur
            tx[col] = pd.to_numeric(tx[col], errors="coerce")

    # Remplacer les NaN par 0.0 et forcer le type float64
    tx[["qty", "price_eur", "fee_eur"]] = (
        tx[["qty", "price_eur", "fee_eur"]].fillna(0.0).astype("float64")
    )

    # Calculer le coût total de chaque transaction (quantité absolue * prix + frais)
    tx["cost_eur"] = tx["qty"].abs() * tx["price_eur"] + tx["fee_eur"]

    # Grouper par symbole pour sommer les quantités et les coûts investis
    agg_df: DataFrame = tx.groupby("symbol", as_index=False).agg(
        qty=("qty", "sum"), investi=("cost_eur", "sum")
    )

    # Fusionner avec le dernier prix connu par symbole
    holdings: DataFrame = agg_df.merge(
        last, on="symbol", how="left", copy=False, validate="1:1"
    )

    # Nettoyer la colonne price_eur issue de la fusion (enlever espaces, convertir en float)
    price_col: Series = cast(Series, holdings["price_eur"])
    price_clean: Series = (
        price_col.map(lambda v: v.strip() if isinstance(v, str) else v)
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0.0)
        .astype("float64")
    )
    holdings["price_eur"] = price_clean

    # Calculer la valeur actuelle (quantité * prix)
    holdings["valeur_actuelle"] = holdings["qty"] * holdings["price_eur"]
    # Calculer le PnL (valeur actuelle - investi)
    holdings["pnl"] = holdings["valeur_actuelle"] - holdings["investi"]
    return holdings


def push_snapshot_to_db(df: DataFrame) -> None:
    """
    Insère le snapshot calculé dans la table `portfolio_snapshot` (historisée, clé primaire (ts, symbol)).
    Attend un DataFrame `df` avec colonnes: symbol, qty, investi, price_eur, valeur_actuelle, pnl.
    Crée aussi les colonnes pnl_eur et pnl_pct à partir de `pnl` et `investi` si nécessaire.
    """
    if df is None or df.empty:
        print("[WARN] Aucun résultat à insérer.")
        return

    # Normalisation symboles en minuscule
    dat = df.copy()
    dat["symbol"] = dat["symbol"].str.lower()

    # Colonnes attendues
    if "pnl_eur" not in dat.columns and "pnl" in dat.columns:
        dat["pnl_eur"] = dat["pnl"].astype(float)
    elif "pnl_eur" not in dat.columns:
        dat["pnl_eur"] = dat["valeur_actuelle"].astype(float) - dat["investi"].astype(
            float
        )

    # pnl_pct en %; éviter division par zéro
    inv = dat["investi"].astype(float)
    dat["pnl_pct"] = dat["pnl_eur"] / inv.replace({0.0: float("nan")}) * 100.0
    dat["pnl_pct"] = dat["pnl_pct"].fillna(0.0)

    # Timestamp UTC identique pour toutes les lignes
    ts = datetime.now(timezone.utc)
    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

    rows = [
        {
            "ts": ts_str,
            "symbol": str(r["symbol"]),
            "qty": float(r["qty"]),
            "investi": float(r["investi"]),
            "price_eur": float(r["price_eur"]),
            "valeur_actuelle": float(r["valeur_actuelle"]),
            "pnl_eur": float(r["pnl_eur"]),
            "pnl_pct": float(r["pnl_pct"]),
        }
        for _, r in dat.iterrows()
    ]

    sql = text(
        """
        INSERT INTO portfolio_snapshot
        (ts, symbol, qty, investi, price_eur, valeur_actuelle, pnl_eur, pnl_pct)
        VALUES (:ts, :symbol, :qty, :investi, :price_eur, :valeur_actuelle, :pnl_eur, :pnl_pct)
        ON DUPLICATE KEY UPDATE
          qty=VALUES(qty),
          investi=VALUES(investi),
          price_eur=VALUES(price_eur),
          valeur_actuelle=VALUES(valeur_actuelle),
          pnl_eur=VALUES(pnl_eur),
          pnl_pct=VALUES(pnl_pct)
        """
    )

    eng = get_engine()
    with eng.begin() as con:
        con.execute(sql, rows)

    print(f"[OK] {len(rows)} lignes insérées dans portfolio_snapshot @ {ts_str} UTC")


def main():
    """
    Fonction principale qui orchestre le calcul du snapshot,
    affiche un résumé, et exporte les résultats.
    """
    tx, last = load_data()  # Charger les données

    if tx.empty:
        print("Aucune transaction.")  # Pas de données, on arrête
        return

    rep = build_report(tx, last)  # Construire le rapport

    # Calculer les totaux pour affichage
    total_investi = float(rep["investi"].sum())
    total_valeur = float(rep["valeur_actuelle"].sum())
    total_pnl = float(rep["pnl"].sum())

    # Affichage debug des totaux
    print(
        f"[DEBUG] Investi total: {total_investi:.2f} € | Valeur actuelle: {total_valeur:.2f} € | PnL: {total_pnl:.2f} €"
    )

    # Afficher un aperçu des premières lignes du rapport
    print(rep[["symbol", "qty", "investi", "price_eur", "valeur_actuelle"]].head())

    # 1) Pousser en base (historisé)
    push_snapshot_to_db(rep)

    # 2) Export CSV simple (sans graphique)
    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    snap_csv = reports_dir / "snapshot.csv"
    try:
        rep.to_csv(snap_csv, index=False)
        print(f"[OK] CSV écrit: {snap_csv}")
    except Exception as e:
        print(f"[WARN] CSV non écrit: {e}")

    print("Snapshot poussé en base (et CSV écrit).")


if __name__ == "__main__":
    main()  # Lancer le script principal si exécuté directement
