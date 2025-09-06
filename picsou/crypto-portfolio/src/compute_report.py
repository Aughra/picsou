# src/compute_report.py
import sqlite3, pandas as pd, matplotlib.pyplot as plt
from pathlib import Path

DB = "data/portfolio.db"
OUT_DIR = Path("reports")
OUT_DIR.mkdir(parents=True, exist_ok=True)

con = sqlite3.connect(DB)

tx = pd.read_sql_query("SELECT * FROM transactions", con)
last = pd.read_sql_query(
    """
  SELECT symbol, price_eur
  FROM price_snapshot
  WHERE ts = (SELECT MAX(ts) FROM price_snapshot)
""",
    con,
)

con.close()

tx["cost_eur"] = tx["qty"].abs() * tx["price"] + tx["fee"]
holdings = tx.groupby("symbol", as_index=False).agg(
    qty=("qty", "sum"), investi=("cost_eur", "sum")
)
holdings = holdings.merge(last, on="symbol", how="left")
holdings["valeur_actuelle"] = holdings["qty"] * holdings["price_eur"]
holdings["pnl"] = holdings["valeur_actuelle"] - holdings["investi"]

# Export CSV
holdings.to_csv(OUT_DIR / "snapshot.csv", index=False)

# Graph global Investi vs Valeur (style simple)
serie = pd.DataFrame(
    {
        "Investi cumulé (€)": [holdings["investi"].sum()],
        "Valeur actuelle (€)": [holdings["valeur_actuelle"].sum()],
    }
).T
ax = serie.plot(kind="bar", legend=False)
ax.set_ylabel("€")
ax.set_title("Portefeuille – Investi vs Valeur")
plt.tight_layout()
plt.savefig(OUT_DIR / "graphs/valeur_vs_investi.png", dpi=160)
plt.close()
print("Rapport généré dans", OUT_DIR)
