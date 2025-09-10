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
   - Graphique : reports/graphs/valeur_vs_investi.png

Usage :
- Lancer le script directement : python compute_report.py
- Vérifier les fichiers générés dans le dossier "reports/"
"""

from pathlib import Path  # stdlib
from typing import cast

import matplotlib.pyplot as plt  # third-party
import pandas as pd
from pandas import DataFrame, Series

from src.db import get_engine  # local

# Dossier de sortie pour les graphiques, création si nécessaire
OUT_DIR = Path("reports/graphs")
OUT_DIR.mkdir(parents=True, exist_ok=True)


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


def export_outputs(df: DataFrame) -> None:
    """
    Exporte le rapport en CSV et génère un graphique simple comparant
    la somme investie et la valeur actuelle totale du portefeuille.
    """
    # Chemin du fichier CSV à générer (un niveau au-dessus du dossier graphique)
    snap_csv = OUT_DIR.parent / "snapshot.csv"
    df.to_csv(snap_csv, index=False)

    # Préparer une série avec les totaux à afficher sur le graphique
    serie = pd.DataFrame(
        {
            "Investi cumulé (€)": [df["investi"].sum()],
            "Valeur actuelle (€)": [df["valeur_actuelle"].sum()],
        }
    ).T

    # Tracer un graphique en barres
    ax = serie.plot(kind="bar", legend=False)
    ax.set_ylabel("€")  # Label de l'axe Y
    ax.set_title("Portefeuille – Investi vs Valeur")  # Titre du graphique

    # Ajuster la mise en page pour éviter les coupures
    plt.tight_layout()
    # Sauvegarder le graphique en PNG avec une bonne résolution
    plt.savefig(OUT_DIR / "valeur_vs_investi.png", dpi=160)
    plt.close()  # Fermer la figure pour libérer la mémoire


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

    # Exporter CSV et graphique
    export_outputs(rep)
    print("Rapport généré dans reports/ (CSV + PNG).")


if __name__ == "__main__":
    main()  # Lancer le script principal si exécuté directement
