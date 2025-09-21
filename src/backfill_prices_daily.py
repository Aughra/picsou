############################################################
# CHANGELOG:
# - [2025-09-21] agt013 {author=agent} {reason: backfill incrémental (skip API si jours présents)}
# - Impact: par symbole, on ne requête que les jours manquants; sinon on saute l’API
# - Tests: run.sh → logs "Skip <sym>" si complet; sinon fetch + upsert borné
# - Notes: réduit 429, accélère les runs; comportement idempotent
# - [2025-09-21] agt007 {author=agent} {reason: backfill quotidien des prix (CoinGecko) → price_snapshot}
# - Impact: remplit/actualise la table price_snapshot (EUR)
# - Tests: smoke via run.sh
# - Notes: utilise market_chart (range)
############################################################

from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Set, Tuple

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
    end = datetime.now(timezone.utc)
    return start, end


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


def _fetch_market_chart_daily(
    coin_id: str, start: datetime, end: datetime
) -> List[Tuple[datetime, float]]:
    """Liste [(ts_utc, price_eur)] pour chaque jour entre start (inclus) et end (exclu)."""
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
    # Dédupliquer par jour UTC (garder le dernier point du jour)
    by_day: Dict[str, Tuple[datetime, float]] = {}
    for ts, p in out:
        key = ts.strftime("%Y-%m-%d")
        by_day[key] = (ts, p)
    return [by_day[k] for k in sorted(by_day.keys())]


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
                    ts_naive = ts.astimezone(timezone.utc).replace(tzinfo=None)
                    cur.execute(sql, (ts_naive, symbol, float(price)))
                    count += 1
                except Exception as e:
                    logger.error(f"Upsert prix échoué {symbol}@{ts}: {e}")
                    continue
        con.commit()
    return count


def _get_last_day_in_db(symbol: str) -> Optional[date]:
    sql = "SELECT MAX(CONVERT(date, ts)) FROM price_snapshot WHERE symbol = %s"
    with get_conn() as con:
        with con.cursor() as cur:
            try:
                cur.execute(sql, (symbol,))
                row = cur.fetchone()
                if row and row[0]:
                    return row[0]
            except Exception as e:
                logger.error("Lecture max jour échouée %s: %s", symbol, e)
    return None


def _all_days_utc(start: datetime, end: datetime) -> List[date]:
    """Retourne la liste des dates (UTC) de start→end inclus."""
    d0 = start.date()
    d1 = end.date()
    out: List[date] = []
    cur = d0
    while cur <= d1:
        out.append(cur)
        cur = cur + timedelta(days=1)
    return out


def _get_existing_days(symbol: str, start: datetime, end: datetime) -> Set[date]:
    """Jours (UTC) déjà présents dans price_snapshot pour un symbole sur [start, end]."""
    end_excl = datetime(end.year, end.month, end.day, tzinfo=timezone.utc) + timedelta(
        days=1
    )
    sql = (
        "SELECT DISTINCT CONVERT(date, ts) AS d FROM price_snapshot "
        "WHERE symbol = %s AND ts >= %s AND ts < %s"
    )
    out: Set[date] = set()
    with get_conn() as con:
        with con.cursor() as cur:
            try:
                cur.execute(
                    sql,
                    (
                        symbol,
                        datetime(
                            start.year, start.month, start.day, tzinfo=timezone.utc
                        ).replace(tzinfo=None),
                        end_excl.replace(tzinfo=None),
                    ),
                )
                rows = cur.fetchall() or []
                for (d,) in rows:
                    out.add(d)
            except Exception as e:
                logger.error("Lecture jours existants échouée %s: %s", symbol, e)
    return out


def main() -> None:
    syms = _get_symbols_from_db()
    if not syms:
        logger.info("Aucun symbole; backfill annulé.")
        return
    # Mapping
    missing_map = [s for s in syms if s not in SYMBOL_TO_ID]
    if missing_map:
        logger.warning("COINS_MAP incomplet, ignoré: %s", ", ".join(missing_map))

    start, end = _get_date_range_from_db()
    logger.info(
        "Backfill prix quotidiens EUR — période: %s → %s (UTC)",
        start.strftime("%Y-%m-%d"),
        end.strftime("%Y-%m-%d"),
    )

    _ensure_price_table()

    for s in syms:
        cid = SYMBOL_TO_ID.get(s)
        if not cid:
            continue
        # Jours manquants sur toute la période (robuste aux trous)
        all_days = set(_all_days_utc(start, end))
        existing = _get_existing_days(s, start, end)
        missing = sorted(d for d in all_days.difference(existing))
        if not missing:
            logger.info("Skip %s: déjà complet (%d jour(s))", s, len(all_days))
            continue

        min_m = missing[0]
        max_m = missing[-1]
        from_dt = datetime(min_m.year, min_m.month, min_m.day, tzinfo=timezone.utc)
        to_dt = datetime(
            max_m.year, max_m.month, max_m.day, tzinfo=timezone.utc
        ) + timedelta(days=1)

        logger.info(
            "Fetch manquant: %s → %s (jours=%d, plage=%s→%s)",
            s,
            cid,
            len(missing),
            from_dt.date(),
            (to_dt - timedelta(seconds=1)).date(),
        )
        try:
            pairs = _fetch_market_chart_daily(cid, from_dt, to_dt)
            # Filtrer strictement aux jours manquants
            misset = set(missing)
            pairs = [(ts, p) for (ts, p) in pairs if ts.date() in misset]
            n = _upsert_many(s, pairs)
            logger.info("%s: %d point(s) upserté(s)", s, n)
            time.sleep(1.2)
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
