"""
Calcule un snapshot portefeuille et exporte un CSV + un graphique simple.
- Lecture `transactions` + dernier `price_snapshot` par symbol
- Export: reports/snapshot.csv
- Graphique: reports/graphs/valeur_vs_investi.png
"""

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from src.db import get_engine

OUT_DIR = Path("reports/graphs")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def load_data():
    eng = get_engine()
    with eng.begin() as con:
        tx = pd.read_sql(
            "SELECT date_utc, symbol, qty, price_eur, fee_eur FROM transactions",
            con,
        )
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
    # Normaliser les symboles pour la jointure (ex: ETH vs eth)
    tx["symbol"] = tx["symbol"].str.lower()
    last["symbol"] = last["symbol"].str.lower()
    return tx, last


def build_report(tx: pd.DataFrame, last: pd.DataFrame) -> pd.DataFrame:
    tx = tx.copy()
    tx["symbol"] = tx["symbol"].str.lower()
    last = last.copy()
    last["symbol"] = last["symbol"].str.lower()
    # Normaliser les types numériques (certains drivers renvoient des str)
    for col in ["qty", "price_eur", "fee_eur"]:
        if col in tx.columns:
            # strip des chaînes avant conversion
            tx[col] = tx[col].apply(lambda v: v.strip() if isinstance(v, str) else v)
            tx[col] = pd.to_numeric(tx[col], errors="coerce")
    tx[["qty", "price_eur", "fee_eur"]] = (
        tx[["qty", "price_eur", "fee_eur"]].fillna(0.0).astype("float64")
    )

    tx["cost_eur"] = tx["qty"].abs() * tx["price_eur"] + tx["fee_eur"]
    holdings = (
        tx.groupby("symbol", as_index=False)
        .agg(qty=("qty", "sum"), investi=("cost_eur", "sum"))
        .merge(last, on="symbol", how="left")
    )
    # S'assurer que le prix issu du merge est numérique
    holdings["price_eur"] = holdings["price_eur"].apply(
        lambda v: v.strip() if isinstance(v, str) else v
    )
    holdings["price_eur"] = (
        pd.to_numeric(holdings["price_eur"], errors="coerce")
        .fillna(0.0)
        .astype("float64")
    )
    holdings["valeur_actuelle"] = holdings["qty"] * holdings["price_eur"]
    holdings["pnl"] = holdings["valeur_actuelle"] - holdings["investi"]
    return holdings


def export_outputs(df: pd.DataFrame) -> None:
    snap_csv = OUT_DIR.parent / "snapshot.csv"
    df.to_csv(snap_csv, index=False)
    serie = pd.DataFrame(
        {
            "Investi cumulé (€)": [df["investi"].sum()],
            "Valeur actuelle (€)": [df["valeur_actuelle"].sum()],
        }
    ).T
    ax = serie.plot(kind="bar", legend=False)
    ax.set_ylabel("€")
    ax.set_title("Portefeuille – Investi vs Valeur")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "valeur_vs_investi.png", dpi=160)
    plt.close()


def main():
    tx, last = load_data()
    if tx.empty:
        print("Aucune transaction.")
        return
    rep = build_report(tx, last)
    total_investi = float(rep["investi"].sum())
    total_valeur = float(rep["valeur_actuelle"].sum())
    total_pnl = float(rep["pnl"].sum())
    print(
        f"[DEBUG] Investi total: {total_investi:.2f} € | Valeur actuelle: {total_valeur:.2f} € | PnL: {total_pnl:.2f} €"
    )
    # Aperçu des lignes
    print(rep[["symbol", "qty", "investi", "price_eur", "valeur_actuelle"]].head())
    export_outputs(rep)
    print("Rapport généré dans reports/ (CSV + PNG).")


if __name__ == "__main__":
    main()
