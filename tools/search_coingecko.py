############################################################
# CHANGELOG:
# - [2025-09-21] agt007 {author=agent} {reason: ajout CLI de recherche CoinGecko (id/symbol/name)}
# - Impact: nouveau script `python picsou/tools/search_coingecko.py --q <query>` pour aider à remplir COINS_MAP
# - Tests: requêtes simples (btc, ripple, sol) → résultats pertinents
# - Notes: utilise l’endpoint public /api/v3/search
############################################################

from __future__ import annotations

import argparse
import sys
from typing import Any

import requests

from logger_config import logger


API_URL = "https://api.coingecko.com/api/v3/search"


def search_coins(query: str) -> list[dict[str, Any]]:
    try:
        r = requests.get(API_URL, params={"query": query}, timeout=15)
        r.raise_for_status()
        data = r.json()
        return data.get("coins", [])
    except Exception as e:
        logger.error(f"Erreur API CoinGecko: {e}")
        return []


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Recherche d'ids CoinGecko (id/symbol/name)"
    )
    parser.add_argument(
        "--q",
        "--query",
        dest="query",
        required=True,
        help="Terme à rechercher (ex: 'xrp' ou 'ripple')",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Nombre maximum de résultats à afficher (défaut: 10)",
    )
    args = parser.parse_args()

    coins = search_coins(args.query)
    if not coins:
        print("Aucun résultat.")
        sys.exit(0)

    print(f"Résultats pour '{args.query}' (top {min(args.limit, len(coins))}):")
    for c in coins[: args.limit]:
        # c: { id, name, api_symbol, symbol, market_cap_rank, thumb, large }
        print(
            f"- id={c.get('id')}  symbol={c.get('symbol')}  name={c.get('name')}  rank={c.get('market_cap_rank')}"
        )


if __name__ == "__main__":
    main()
