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
from datetime import datetime, timezone
from src.db import get_conn

LEDGER_CSV = os.getenv("LEDGER_CSV", "data/ledger_exports/ledger_latest.csv")


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def row_to_tx(row: dict) -> dict:
    """
    Adapter ici les noms de colonnes à ton CSV Ledger Live exact.
    Exemples de colonnes possibles: Date, Currency, Amount, Price EUR, Fee EUR, Hash, Type
    """
    # Date -> UTC naive (YYYY-MM-DD HH:MM:SS)
    dt = (
        datetime.fromisoformat(row["Date"].replace("Z", ""))
        .astimezone(timezone.utc)
        .replace(tzinfo=None)
    )
    symbol = row["Currency"].lower()
    qty = float(row["Amount"])  # signe inclus dans le CSV
    price_eur = float(row.get("Price EUR", 0) or 0)
    fee_eur = float(row.get("Fee EUR", 0) or 0)
    note = row.get("Hash") or row.get("Operation ID") or row.get("Type")

    basis = f"{dt.isoformat()}|{symbol}|{qty}|{price_eur}|{fee_eur}|{note or ''}"
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
    rows = []
    with open(LEDGER_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(row_to_tx(r))
    n = bulk_insert(rows)
    print(f"{n} lignes traitées (doublons ignorés).")


if __name__ == "__main__":
    main()
