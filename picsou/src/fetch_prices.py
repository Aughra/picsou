# src/fetch_prices.py
import requests, sqlite3, datetime as dt

SYMBOL_TO_ID = {"btc": "bitcoin", "eth": "ethereum", "sol": "solana", "ada": "cardano"}


def fetch_prices_eur(symbols):
    ids = ",".join(SYMBOL_TO_ID[s] for s in symbols)
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=eur"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    data = r.json()
    out = {}
    for s in symbols:
        out[s] = float(data[SYMBOL_TO_ID[s]]["eur"])
    return out


def main():
    con = sqlite3.connect("data/portfolio.db")
    cur = con.cursor()
    # lister les coins présents en base
    syms = [
        r[0] for r in cur.execute("SELECT DISTINCT LOWER(symbol) FROM transactions")
    ]
    if not syms:
        print("Pas de symboles en base.")
        return
    prices = fetch_prices_eur(syms)
    ts = dt.datetime.utcnow().isoformat(timespec="seconds")
    for s, p in prices.items():
        cur.execute(
            "INSERT OR REPLACE INTO price_snapshot(ts, symbol, price_eur) VALUES (?,?,?)",
            (ts, s, p),
        )
    con.commit()
    con.close()
    print("Prix enregistrés @", ts)


if __name__ == "__main__":
    main()
