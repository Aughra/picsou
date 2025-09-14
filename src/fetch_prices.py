"""
Ce script récupère les prix en euros (EUR) des cryptomonnaies via l'API CoinGecko pour tous les symboles présents dans la base de données.
Il met ensuite à jour ou insère ces prix dans la table `price_snapshot` de la base de données.

Fonctionnement :
- Le script récupère la liste des symboles de cryptomonnaies depuis la table `transactions`.
- Il utilise une correspondance entre symboles et identifiants CoinGecko pour interroger l'API.
- Les prix récupérés sont ensuite sauvegardés en base avec un horodatage.

Utilisation :
- Exécuter ce script directement (par exemple avec `python fetch_prices.py`).
- Assurez-vous que la variable d'environnement `COINS_MAP` est configurée si vous souhaitez personnaliser la correspondance symboles -> CoinGecko.
- La base de données doit être accessible via la fonction `get_conn` du module `src.db`.


---
Changelog:
- 2025-09-12 19:00 (Europe/Paris) — [Aya] Ajout du mapping CoinGecko par défaut pour AVAX/DOT/XRP — ces tokens manquaient dans les prix, ce qui provoquait des valeurs nulles et des -100% dans compute_report.
- 2025-09-12 19:10 (Europe/Paris) — [Eliorë] Modif import: `from src.db import get_conn` → `from db import get_conn` — nécessaire pour simplifier l’exécution directe sur l’environnement Synology.
- 2025-09-12 19:20 (Europe/Paris) — [Aya] Ajout d'un User-Agent explicite dans la requête HTTP CoinGecko — permet d’éviter les rejets de l’API pour requêtes anonymes (erreurs 429 ou blocages).
- 2025-09-12 19:30 (Europe/Paris) — [Aya] Ajout d’un affichage du mapping CoinGecko utilisé à chaque exécution pour plus de transparence.
"""

import os
from datetime import datetime, timezone

import requests

from db import get_conn

# Map symbol -> coingecko id (depuis .env ou valeurs par défaut)
# Cette variable récupère une chaîne de caractères de type "btc:bitcoin,eth:ethereum,..."
# et crée un dictionnaire pour faire correspondre chaque symbole à l'id CoinGecko correspondant.
_ENV_MAP = os.getenv(
    "COINS_MAP",
    "btc:bitcoin,eth:ethereum,sol:solana,ada:cardano,avax:avalanche-2,dot:polkadot,xrp:ripple",
)
SYMBOL_TO_ID = {
    k.strip(): v.strip()
    for k, v in (pair.split(":") for pair in _ENV_MAP.split(",") if ":" in pair)
}


def _warn_unmapped(symbols: list[str]) -> None:
    missing = [s for s in symbols if s not in SYMBOL_TO_ID]
    if missing:
        print(
            "[WARN] Aucun mapping CoinGecko pour: "
            + ", ".join(sorted(set(missing)))
            + ". Ajoute-les via COINS_MAP (ex: matic:polygon-ecosystem-token)."
        )


def fetch_prices_eur(symbols: list[str]) -> dict[str, float]:
    """
    Récupère les prix en euros pour une liste de symboles donnés via l'API CoinGecko.

    Arguments:
    - symbols : liste de symboles (ex: ['btc', 'eth'])

    Retourne:
    - dictionnaire {symbole: prix_en_eur}
    """
    # On construit une chaîne d'identifiants CoinGecko séparés par des virgules pour la requête API
    ids = ",".join(SYMBOL_TO_ID[s] for s in symbols if s in SYMBOL_TO_ID)
    if not ids:
        # Si aucun symbole valide, on retourne un dictionnaire vide
        return {}
    # URL de l'API CoinGecko pour récupérer les prix en EUR
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=eur"
    # Envoi de la requête HTTP GET avec un timeout de 20 secondes et User-Agent explicite
    headers = {"User-Agent": "picsou/1.0 (+https://local)"}
    r = requests.get(url, timeout=20, headers=headers)
    r.raise_for_status()  # Vérifie que la requête a réussi
    data = r.json()  # Récupère la réponse JSON
    # Construire le résultat et détecter les manquants
    out: dict[str, float] = {}
    for s in symbols:
        cid = SYMBOL_TO_ID.get(s)
        if cid and cid in data and "eur" in data[cid]:
            out[s] = float(data[cid]["eur"])
    # Avertir si l'API n'a pas renvoyé certains IDs attendus
    expected = {SYMBOL_TO_ID[s] for s in symbols if s in SYMBOL_TO_ID}
    missing_ids = sorted(expected.difference(data.keys()))
    if missing_ids:
        print("[WARN] API CoinGecko sans prix pour IDs: " + ", ".join(missing_ids))
    return out


def upsert_prices(prices: dict[str, float], ts: str) -> None:
    """
    Insère ou met à jour les prix dans la table `price_snapshot` pour un timestamp donné.

    Arguments:
    - prices : dictionnaire {symbole: prix_en_eur}
    - ts : timestamp au format string (ex: '2023-01-01 12:00:00')
    """
    if not prices:
        # Rien à faire si le dictionnaire est vide
        return
    # Préparation des données à insérer dans la base
    rows = [{"ts": ts, "symbol": s, "price_eur": p} for s, p in prices.items()]
    sql = """
        INSERT INTO price_snapshot (ts, symbol, price_eur)
        VALUES (%(ts)s, %(symbol)s, %(price_eur)s)
        ON DUPLICATE KEY UPDATE price_eur = VALUES(price_eur)
        """
    con = get_conn()  # Connexion à la base de données
    try:
        with con.cursor() as cur:
            # Exécution d'une insertion multiple avec gestion des doublons (upsert)
            cur.executemany(sql, rows)
        con.commit()  # Validation de la transaction
    finally:
        con.close()  # Fermeture de la connexion


def main():
    """
    Fonction principale qui orchestre la récupération des symboles,
    la récupération des prix, puis la mise à jour en base.
    """
    con = get_conn()  # Connexion à la base de données
    try:
        with con.cursor() as cur:
            # Récupère la liste distincte des symboles en minuscules depuis la table transactions
            cur.execute("SELECT DISTINCT LOWER(symbol) AS s FROM transactions")
            syms = [r["s"] for r in cur.fetchall()]
    finally:
        con.close()  # Fermeture de la connexion

    if not syms:
        print("Aucun symbole en base.")
        return

    # Normalisation et avertissement sur les mappings manquants
    syms = sorted(set(s.lower() for s in syms))
    _warn_unmapped(syms)

    # Afficher le mapping final utilisé pour transparence
    print("[INFO] Mapping CoinGecko utilisé :")
    for s in syms:
        cid = SYMBOL_TO_ID.get(s)
        if cid:
            print(f"  - {s} → {cid}")
        else:
            print(f"  - {s} → (non mappé)")

    # Timestamp UTC (sans tzinfo) pour l'insert
    now = datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")

    # Récupération des prix
    prices = fetch_prices_eur(syms)
    # Mise à jour ou insertion des prix en base avec le timestamp
    upsert_prices(prices, now)
    print(f"Prix mis à jour @ {now} pour {len(prices)} symbole(s).")


if __name__ == "__main__":
    main()
