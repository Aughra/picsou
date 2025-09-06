"""
Importe un export CSV Ledger Live dans la table `transactions` (MariaDB).
• Déduplication via `dedup_hash` (SHA1 de champs stables).
• Tolérant au ré-import (ON DUPLICATE KEY UPDATE no-op).

Usage:
  LEDGER_CSV=path/to/export.csv python -m src.import_ledger_csv
"""

import csv
import hashlib
import os
import re
from datetime import datetime, timezone
from src.db import get_conn

LEDGER_CSV = os.getenv("LEDGER_CSV", "data/ledger_exports/ledger_latest.csv")


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _norm_key(s: str) -> str:
    # normalise: lower, trim, collapse spaces, remove surrounding quotes
    return re.sub(r"\s+", " ", (s or "").strip().strip('"').strip("'").lower())


def _get_first(d: dict, keys: list[str], default=None):
    for k in keys:
        if k in d:
            return d[k]
    return default


def _get_first_like(
    d: dict, candidates: list[str], regexes: list[str] | None = None, default=None
):
    """
    Retourne la première valeur dont la *clé normalisée* est soit égale à l’un des `candidates`,
    soit le contient en sous-chaîne. Optionnellement, essaie des regex.
    """
    keys = list(d.keys())
    # exact
    for c in candidates:
        c_norm = _norm_key(c)
        for k in keys:
            if k == c_norm:
                return d[k]
    # contient (substring dans un sens comme dans l'autre)
    for c in candidates:
        c_norm = _norm_key(c)
        for k in keys:
            if c_norm in k or k in c_norm:
                return d[k]
    # regex optionnelles
    if regexes:
        for pat in regexes:
            r = re.compile(pat)
            for k in keys:
                if r.search(k):
                    return d[k]
    return default


def _to_float(x, default=0.0) -> float:
    if x is None:
        return float(default)
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return float(default)
    # support comma decimals and thousands separators
    s = s.replace("\u202f", "").replace("\u00a0", "").replace(" ", "").replace(",", ".")
    # remove currency symbols if any
    s = re.sub(r"[^\d\.\-eE]", "", s)
    try:
        return float(s)
    except ValueError:
        return float(default)


def _parse_dt(value: str) -> datetime:
    """
    Try multiple common Ledger Live export formats, fall back to naive UTC now if all fail.
    """
    if not value:
        return datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(timezone.utc)
    s = value.strip().replace("Z", "")
    # Common patterns: ISO, "YYYY-MM-DD HH:MM:SS", "DD/MM/YYYY HH:MM", etc.
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    # As a last resort, try fromisoformat (which handles microseconds)
    try:
        dt = datetime.fromisoformat(s)
        # If timezone-aware, convert to UTC; else assume UTC
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc)
        else:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        # Fallback: now (UTC)
        return datetime.utcnow().replace(tzinfo=timezone.utc)


def row_to_tx(row: dict) -> dict:
    """
    Mapping robuste pour Ledger Live (FR/EN).
    Gère Operation Date, Currency Ticker, Operation Amount/Fees, Operation Hash,
    Countervalue at Operation Date (+ ticker), etc.
    """
    # Vue normalisée (clés en minuscules, espaces condensés)
    nrow = {_norm_key(k): v for k, v in row.items()}

    # Alias d'en-têtes
    date_keys = ["date", "operation date", "datetime", "time", "timestamp"]
    currency_keys = ["currency", "currency ticker", "asset", "symbol", "ticker"]
    amount_keys = [
        "operation amount",
        "amount",
        "amount (asset)",
        "quantity",
        "qty",
        "value",
    ]
    fee_asset_keys = ["operation fees", "network fee", "fee", "fees"]
    countervalue_keys = [
        "countervalue at operation date",
        "countervalue",
        "countervalue at csv export",
    ]
    countervalue_ticker_keys = ["countervalue ticker", "fiat", "fiat ticker"]
    op_type_keys = ["operation type", "type"]
    note_keys = [
        "operation hash",
        "hash",
        "tx hash",
        "txid",
        "operation id",
        "id",
        "memo",
        "note",
        "type",
    ]
    price_eur_fallback_keys = [
        "price eur",
        "price (eur)",
        "spot price eur",
        "spot price (eur)",
        "eur price",
        "eur/price",
        "price",
    ]

    # Date / symbole
    date_val = _get_first_like(nrow, date_keys)
    dt = _parse_dt(date_val)
    symbol = (_get_first_like(nrow, currency_keys) or "").strip().lower()

    # Quantité + signe selon Operation Type
    qty = _to_float(_get_first_like(nrow, amount_keys), 0.0)
    op_type = (_get_first_like(nrow, op_type_keys) or "").strip().lower()
    if op_type in ("out", "send", "withdrawal", "withdraw", "sell"):
        qty = -abs(qty)
    elif op_type in ("in", "receive", "deposit", "buy"):
        qty = abs(qty)

    # Prix unitaire EUR : préférer Countervalue(EUR)/|qty|
    price_eur = 0.0
    counter_ticker = (
        (_get_first_like(nrow, countervalue_ticker_keys) or "").strip().upper()
    )
    counter_at_date = _to_float(_get_first_like(nrow, countervalue_keys), 0.0)
    if counter_ticker == "EUR" and qty:
        price_eur = abs(counter_at_date) / abs(qty) if qty != 0 else 0.0
    if price_eur == 0.0:
        # fallback: s'il existe un prix direct en EUR
        price_eur = _to_float(_get_first_like(nrow, price_eur_fallback_keys), 0.0)

    # Frais en asset -> EUR via prix unitaire si dispo
    fee_asset = _to_float(_get_first_like(nrow, fee_asset_keys), 0.0)
    fee_eur = fee_asset * price_eur if price_eur else 0.0

    note = _get_first_like(nrow, note_keys) or ""

    basis = f"{dt.isoformat()}|{symbol}|{qty}|{price_eur}|{fee_eur}|{note}"
    dedup = sha1_hex(basis)

    return {
        "date_utc": dt.strftime("%Y-%m-%d %H:%M:%S"),
        "symbol": symbol,
        "qty": qty,
        "price_eur": price_eur,
        "fee_eur": fee_eur,
        "exchange": "ledger",
        "note": note,
        "dedup_hash": dedup,
    }


def bulk_insert(rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO transactions
          (date_utc, symbol, qty, price_eur, fee_eur, exchange, note, dedup_hash)
        VALUES
          (%(date_utc)s, %(symbol)s, %(qty)s, %(price_eur)s, %(fee_eur)s, %(exchange)s, %(note)s, %(dedup_hash)s)
        ON DUPLICATE KEY UPDATE id = id
        """
    con = get_conn()
    try:
        with con.cursor() as cur:
            cur.executemany(sql, rows)
        con.commit()
    finally:
        con.close()
    return len(rows)


def main():
    if not os.path.exists(LEDGER_CSV):
        raise SystemExit(f"Fichier CSV introuvable: {LEDGER_CSV}")
    print(f"Import CSV (Ledger): {LEDGER_CSV}")
    rows = []
    with open(LEDGER_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                rows.append(row_to_tx(r))
            except KeyError as e:
                keys_seen = list(r.keys())
                raise KeyError(
                    f"Colonne manquante {e}. En-têtes disponibles: {keys_seen}"
                ) from e
    n = bulk_insert(rows)
    print(f"{n} lignes traitées (doublons ignorés).")


if __name__ == "__main__":
    main()
