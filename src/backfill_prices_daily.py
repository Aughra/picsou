############################################################
# CHANGELOG:
# - [2025-09-21] agt007 {author=agent} {reason: backfill quotidien des prix (CoinGecko) → price_snapshot}
# - Impact: remplit/actualise la table price_snapshot avec un point par jour pour chaque coin (EUR)
# - Tests: smoke via run.sh; vérif manuelle avec SELECT COUNT(*) sur price_snapshot
# - Notes: utilise market_chart de CoinGecko; gère COINS_MAP; bornes: min(date_utc)→aujourd’hui
############################################################
"""
Backfill quotidien des prix en EUR pour chaque coin présent dans `transactions`.

Sources:
- CoinGecko `/coins/{id}/market_chart` (vs_currency=eur, days=max)

Règles:
- Périmètre: de la première transaction (UTC) jusqu’à aujourd’hui (UTC)
- Un point par jour (timestamp fourni par CoinGecko, en millisecondes, converti UTC)
- Insertion via MERGE sur (ts, symbol) → met à jour si existe

Env:
- COINS_MAP: "btc:bitcoin,eth:ethereum,..." (symbol→coingecko id)

Notes:
- Le builder `build_saisir` utilise le dernier snapshot par jour en Europe/Paris et ffill. Avoir au moins un point par jour suffit.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Tuple

import requests

from logger_config import logger

try:  # compat: certains environnements exposent get_conn sous db au lieu de src.db
    from src.db import get_conn
except Exception:  # noqa: S110
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


def _get_date_range_from_db() -> Tuple[datetime, datetime]:
    # UTC
    with get_conn() as con:
        with con.cursor() as cur:
            cur.execute("SELECT MIN(date_utc), MAX(date_utc) FROM transactions")
            row = cur.fetchone()
    if not row or not row[0]:
        raise RuntimeError("Aucune transaction pour déterminer la période de backfill.")
    start = row[0]
    if isinstance(start, str):
        start = datetime.fromisoformat(start)
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    # Fin = aujourd’hui UTC fin de journée
    end = datetime.now(timezone.utc)
    return start, end


def _fetch_market_chart_daily(
    coin_id: str, start: datetime, end: datetime
) -> List[Tuple[datetime, float]]:
    """Retourne une liste [(ts_utc, price_eur)] quotidienne pour un id CoinGecko donné.

    Utilise /coins/{id}/market_chart/range?vs_currency=eur&from=...&to=...
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
    params = {
        "vs_currency": "eur",
        "from": str(int(start.timestamp())),
        "to": str(int(end.timestamp())),
    }
    headers = {"User-Agent": "picsou/1.0 (+https://local)"}
    r = requests.get(url, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    data = r.json() or {}
    out: List[Tuple[datetime, float]] = []
    for ts_ms, price in data.get("prices", []):
        try:
            ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
            out.append((ts, float(price)))
        except Exception:
            continue
    # Dédupliquer par jour UTC (on garde le dernier point si doublons)
    by_day: Dict[str, Tuple[datetime, float]] = {}
    for ts, p in out:
        key = ts.strftime("%Y-%m-%d")
        by_day[key] = (ts, p)
    return [by_day[k] for k in sorted(by_day.keys())]


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


def _upsert_many(symbol: str, rows: Iterable[Tuple[datetime, float]]) -> int:
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
    count = 0
    with get_conn() as con:
        with con.cursor() as cur:
            for ts, price in rows:
                try:
                    # enlever tzinfo pour SQL Server si nécessaire
                    ts_naive = ts.astimezone(timezone.utc).replace(tzinfo=None)
                    cur.execute(sql, (ts_naive, symbol, float(price)))
                    count += 1
                except Exception as e:
                    logger.error(f"Upsert prix échoué {symbol}@{ts}: {e}")
                    continue
        con.commit()
    return count


def main() -> None:
    syms = _get_symbols_from_db()
    if not syms:
        logger.info("Aucun symbole; backfill annulé.")
        return
    _ = SYMBOL_TO_ID  # declenche l’évaluation du mapping
    missing = [s for s in syms if s not in SYMBOL_TO_ID]
    if missing:
        logger.warning(
            "COINS_MAP incomplet, ces symboles seront ignorés: " + ", ".join(missing)
        )

    start, end = _get_date_range_from_db()
    logger.info(
        "Backfill prix quotidiens EUR — période: %s → %s (UTC)",
        start.strftime("%Y-%m-%d"),
        end.strftime("%Y-%m-%d"),
    )

    # S’assurer que la table existe
    _ensure_price_table()

    for s in syms:
        cid = SYMBOL_TO_ID.get(s)
        if not cid:
            continue
        logger.info("Fetch daily market_chart: %s → %s", s, cid)
        try:
            pairs = _fetch_market_chart_daily(cid, start, end)
            n = _upsert_many(s, pairs)
            logger.info("%s: %d point(s) upserté(s)", s, n)
            time.sleep(1.2)  # rate-limit soft
        except requests.HTTPError as he:
            status = getattr(he.response, "status_code", None)
            logger.error("HTTP %s pour %s (%s): %s", status, s, cid, he)
            if status == 429:
                logger.info("Backoff 10s suite à 429…")
                time.sleep(10)
            continue
        except Exception as e:
            logger.error("Erreur backfill %s: %s", s, e)
            continue

    logger.info("Backfill quotidien terminé.")


if __name__ == "__main__":
    main()
