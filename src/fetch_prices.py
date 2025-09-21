############################################################
# CHANGELOG:
# - [2025-09-21] agt014 {author=agent} {reason: ajout fetch prix courant (CoinGecko simple/price) → price_snapshot}
# - Impact: met à jour le prix du jour (UTC) pour chaque symbole; Excel voit la valeur du jour
# - Tests: exécution via run.sh; vérif logs et lignes upsertées
# - Notes: un seul appel pour tous les ids (vs_currency=eur); upsert MERGE (ts,symbol)
############################################################

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Dict, List

import requests

from logger_config import logger

try:
    from src.db import get_conn
except Exception:  # pragma: no cover
    from db import get_conn  # type: ignore


_ENV_MAP = os.getenv(
    "COINS_MAP",
    "btc:bitcoin,eth:ethereum,sol:solana,ada:cardano,avax:avalanche-2,dot:polkadot,xrp:ripple",
)
SYMBOL_TO_ID: Dict[str, str] = {
    k.strip(): v.strip()
    for k, v in (pair.split(":") for pair in _ENV_MAP.split(",") if ":" in pair)
}


def _get_symbols_from_db() -> List[str]:
    with get_conn() as con:
        with con.cursor() as cur:
            cur.execute("SELECT DISTINCT LOWER(symbol) FROM transactions")
            rows = cur.fetchall() or []
    return sorted(set(r[0] for r in rows if r and r[0]))


def _ensure_price_table() -> None:
    ddl = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'price_snapshot')
    BEGIN
        CREATE TABLE price_snapshot (
            id INT IDENTITY(1,1) PRIMARY KEY,
            ts DATETIME2 NOT NULL,
            symbol NVARCHAR(20) NOT NULL,
            price_eur DECIMAL(18,8) NOT NULL,
            created_at DATETIME2 DEFAULT GETDATE(),
            CONSTRAINT UK_price_snapshot_ts_symbol UNIQUE (ts, symbol)
        );
        CREATE INDEX IX_price_snapshot_symbol ON price_snapshot(symbol);
        CREATE INDEX IX_price_snapshot_ts ON price_snapshot(ts);
    END
    """
    with get_conn() as con:
        with con.cursor() as cur:
            cur.execute(ddl)
        con.commit()


def _upsert(symbol: str, ts: datetime, price_eur: float) -> None:
    sql = """
        MERGE price_snapshot AS target
        USING (VALUES (%s, %s, %s)) AS source (ts, symbol, price_eur)
        ON target.ts = source.ts AND target.symbol = source.symbol
        WHEN MATCHED THEN UPDATE SET price_eur = source.price_eur
        WHEN NOT MATCHED THEN INSERT (ts, symbol, price_eur) VALUES (source.ts, source.symbol, source.price_eur);
    """
    with get_conn() as con:
        with con.cursor() as cur:
            ts_naive = ts.astimezone(timezone.utc).replace(tzinfo=None)
            cur.execute(sql, (ts_naive, symbol, float(price_eur)))
        con.commit()


def main() -> None:
    _ensure_price_table()
    syms = _get_symbols_from_db()
    syms = [s for s in syms if s in SYMBOL_TO_ID]
    if not syms:
        logger.info("Aucun symbole mappé; fetch courant annulé.")
        return

    ids = ",".join(sorted({SYMBOL_TO_ID[s] for s in syms}))
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": ids, "vs_currencies": "eur"}
    headers = {"User-Agent": "picsou/1.0 (+https://local)"}
    logger.info("Fetch prix courant EUR: %s", ids)
    r = requests.get(url, params=params, headers=headers, timeout=20)
    r.raise_for_status()
    data = r.json() or {}

    ts = datetime.now(timezone.utc)
    done = 0
    for s in syms:
        cid = SYMBOL_TO_ID[s]
        entry = data.get(cid)
        if not entry:
            logger.warning("Prix absent pour %s (%s)", s, cid)
            continue
        eur = entry.get("eur")
        if eur is None:
            logger.warning("EUR absent pour %s (%s)", s, cid)
            continue
        try:
            _upsert(s, ts, float(eur))
            done += 1
        except Exception as e:
            logger.error("Upsert prix courant échoué %s: %s", s, e)
            continue
    logger.info("Prix courants upsertés: %d", done)


if __name__ == "__main__":
    main()
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

from src.db import get_conn

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

    # Utilisation de MERGE pour SQL Server (gestion des doublons)
    sql = """
        MERGE price_snapshot AS target
        USING (VALUES (%s, %s, %s)) AS source (ts, symbol, price_eur)
        ON target.ts = source.ts AND target.symbol = source.symbol
        WHEN MATCHED THEN
            UPDATE SET price_eur = source.price_eur
        WHEN NOT MATCHED THEN
            INSERT (ts, symbol, price_eur)
            VALUES (source.ts, source.symbol, source.price_eur);
    """

    con = get_conn()  # Connexion à la base de données
    try:
        with con.cursor() as cur:
            # Traiter chaque prix individuellement car MERGE ne fonctionne pas bien avec executemany
            for symbol, price in prices.items():
                try:
                    cur.execute(sql, (ts, symbol, float(price)))
                except Exception as e:
                    print(f"Erreur lors de l'insertion du prix {symbol}: {e}")
                    continue
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
            results = cur.fetchall()
            syms = [r[0] for r in results] if results else []
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
