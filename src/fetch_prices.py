"""
Récupère les prix EUR via l'API CoinGecko pour tous les symbols présents en base,
et enregistre dans `price_snapshot` (UPSERT sur (ts,symbol)).
"""

import os
import requests
from datetime import datetime, timezone
from src.db import get_conn

# Map symbol -> coingecko id (depuis .env ou valeurs par défaut)
_ENV_MAP = os.getenv("COINS_MAP", "btc:bitcoin,eth:ethereum,sol:solana,ada:cardano")
SYMBOL_TO_ID = {
    k.strip(): v.strip()
    for k, v in (pair.split(":") for pair in _ENV_MAP.split(",") if ":" in pair)
}


def fetch_prices_eur(symbols: list[str]) -> dict[str, float]:
    ids = ",".join(SYMBOL_TO_ID[s] for s in symbols if s in SYMBOL_TO_ID)
    if not ids:
        return {}
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=eur"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    return {
        s: float(data[SYMBOL_TO_ID[s]]["eur"])
        for s in symbols
        if s in SYMBOL_TO_ID and SYMBOL_TO_ID[s] in data
    }


def upsert_prices(prices: dict[str, float], ts: str) -> None:
    if not prices:
        return
    rows = [{"ts": ts, "symbol": s, "price_eur": p} for s, p in prices.items()]
    sql = """
        INSERT INTO price_snapshot (ts, symbol, price_eur)
        VALUES (%(ts)s, %(symbol)s, %(price_eur)s)
        ON DUPLICATE KEY UPDATE price_eur = VALUES(price_eur)
        """
    con = get_conn()
    try:
        with con.cursor() as cur:
            cur.executemany(sql, rows)
        con.commit()
    finally:
        con.close()


def main():
    con = get_conn()
    try:
        with con.cursor() as cur:
            cur.execute("SELECT DISTINCT LOWER(symbol) AS s FROM transactions")
            syms = [r["s"] for r in cur.fetchall()]
    finally:
        con.close()
    if not syms:
        print("Aucun symbole en base.")
        return
    now = datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    prices = fetch_prices_eur(syms)
    upsert_prices(prices, now)
    print(f"Prix mis à jour @ {now} pour {len(prices)} symbole(s).")


if __name__ == "__main__":
    main()
