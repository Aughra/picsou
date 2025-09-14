"""
Script d'importation d'un fichier CSV exporté depuis Ledger Live vers une base de données MariaDB.
Ce script lit un fichier CSV contenant les transactions exportées de Ledger Live, les transforme en un format adapté,
puis les insère dans une table `transactions` en base de données.

Fonctionnement :
- Lecture du fichier CSV spécifié par la variable d'environnement LEDGER_CSV (par défaut un chemin donné).
- Pour chaque ligne, extraction et normalisation des données importantes (date, symbole, quantité, prix, frais, note).
- Calcul d'un hash de déduplication pour éviter l'insertion de doublons.
- Insertion en base avec gestion des doublons via ON DUPLICATE KEY UPDATE no-op.

Utilisation :
  LEDGER_CSV=chemin/vers/mon_export.csv python -m src.import_ledger_csv

Le script est tolérant aux ré-imports du même fichier grâce à la déduplication.


---
Changelog:
- 2025-09-12 19:45 (Europe/Paris) — [Aya] Création du script d’import Ledger CSV (`import_ledger_csv.py`), avec déduplication par hash SHA1 et gestion robuste des formats (dates, nombres, colonnes multilingues).
"""

import csv
import hashlib
import os
import re
from datetime import datetime, timezone
from src.db import get_conn

# Chemin vers le fichier CSV Ledger Live à importer, défini via variable d'environnement ou valeur par défaut
LEDGER_CSV = os.getenv("LEDGER_CSV", "data/ledger_exports/ledger_latest.csv")


def sha1_hex(s: str) -> str:
    # Calcule le hash SHA1 d'une chaîne de caractères et retourne son hexadécimal
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _norm_key(s: str) -> str:
    # Normalise une clé de dictionnaire : minuscules, suppression d'espaces superflus et de guillemets
    return re.sub(r"\s+", " ", (s or "").strip().strip('"').strip("'").lower())


def _get_first(d: dict, keys: list[str], default=None):
    # Cherche la première clé présente dans le dictionnaire parmi une liste de clés candidates
    for k in keys:
        if k in d:
            return d[k]
    return default


def _get_first_like(
    d: dict, candidates: list[str], regexes: list[str] | None = None, default=None
):
    """
    Cherche dans un dictionnaire la première valeur dont la clé normalisée correspond :
    - soit exactement à une des clés candidates,
    - soit contient ou est contenue dans une des clés candidates,
    - soit correspond à un des motifs regex optionnels.
    """
    keys = list(d.keys())
    # Recherche exacte
    for c in candidates:
        c_norm = _norm_key(c)
        for k in keys:
            if k == c_norm:
                return d[k]
    # Recherche par inclusion de sous-chaîne
    for c in candidates:
        c_norm = _norm_key(c)
        for k in keys:
            if c_norm in k or k in c_norm:
                return d[k]
    # Recherche par regex optionnelle
    if regexes:
        for pat in regexes:
            r = re.compile(pat)
            for k in keys:
                if r.search(k):
                    return d[k]
    return default


def _to_float(x, default=0.0) -> float:
    # Convertit une valeur en float, en gérant les formats avec virgule, espaces, symboles monétaires, etc.
    if x is None:
        return float(default)
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return float(default)
    # Supprime espaces insécables et remplace virgules par points
    s = s.replace("\u202f", "").replace("\u00a0", "").replace(" ", "").replace(",", ".")
    # Supprime les caractères non numériques (sauf . - e E)
    s = re.sub(r"[^\d\.\-eE]", "", s)
    try:
        return float(s)
    except ValueError:
        return float(default)


def _parse_dt(value: str) -> datetime:
    """
    Analyse une chaîne de date/heure en essayant plusieurs formats courants de Ledger Live.
    Retourne un objet datetime en UTC.
    Si la chaîne est vide ou invalide, retourne la date/heure actuelle UTC.
    """
    if not value:
        return datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(timezone.utc)
    s = value.strip().replace("Z", "")
    # Essai de plusieurs formats courants
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
    # Dernier recours : fromisoformat
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc)
        else:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        # Si tout échoue, retourne maintenant UTC
        return datetime.utcnow().replace(tzinfo=timezone.utc)


def row_to_tx(row: dict) -> dict:
    """
    Transforme une ligne CSV brute en dictionnaire prêt à l'insertion en base.
    Gère les noms de colonnes en français ou anglais, normalise les valeurs,
    calcule la quantité signée selon le type d'opération, et calcule le hash de déduplication.
    """
    # Normalisation des clés de la ligne (minuscules, espaces condensés)
    nrow = {_norm_key(k): v for k, v in row.items()}

    # Listes d'alias pour les différents champs importants
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

    # Extraction et normalisation des valeurs clés
    date_val = _get_first_like(nrow, date_keys)
    dt = _parse_dt(date_val)
    symbol = (_get_first_like(nrow, currency_keys) or "").strip().lower()

    # Quantité avec signe selon type d'opération (ex: retrait négatif, dépôt positif)
    qty = _to_float(_get_first_like(nrow, amount_keys), 0.0)
    op_type = (_get_first_like(nrow, op_type_keys) or "").strip().lower()
    if op_type in ("out", "send", "withdrawal", "withdraw", "sell"):
        qty = -abs(qty)
    elif op_type in ("in", "receive", "deposit", "buy"):
        qty = abs(qty)

    # Calcul du prix unitaire en EUR à partir de la contre-valeur si disponible
    price_eur = 0.0
    counter_ticker = (
        (_get_first_like(nrow, countervalue_ticker_keys) or "").strip().upper()
    )
    counter_at_date = _to_float(_get_first_like(nrow, countervalue_keys), 0.0)
    if counter_ticker == "EUR" and qty:
        price_eur = abs(counter_at_date) / abs(qty) if qty != 0 else 0.0
    if price_eur == 0.0:
        # Sinon, fallback sur un prix direct en EUR dans une autre colonne
        price_eur = _to_float(_get_first_like(nrow, price_eur_fallback_keys), 0.0)

    # Frais en asset convertis en EUR via le prix unitaire si possible
    fee_asset = _to_float(_get_first_like(nrow, fee_asset_keys), 0.0)
    fee_eur = fee_asset * price_eur if price_eur else 0.0

    # Note ou hash de la transaction pour identification
    note = _get_first_like(nrow, note_keys) or ""

    # Calcul du hash de déduplication à partir des champs stables
    basis = f"{dt.isoformat()}|{symbol}|{qty}|{price_eur}|{fee_eur}|{note}"
    dedup = sha1_hex(basis)

    # Retourne un dictionnaire prêt pour insertion en base
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
    # Insère en base une liste de transactions, ignore les doublons grâce au hash unique
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
    # Point d'entrée principal du script
    # Vérifie que le fichier CSV existe
    if not os.path.exists(LEDGER_CSV):
        raise SystemExit(f"Fichier CSV introuvable: {LEDGER_CSV}")
    print(f"Import CSV (Ledger): {LEDGER_CSV}")

    rows = []
    # Lecture du fichier CSV avec encodage UTF-8
    with open(LEDGER_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                # Conversion de chaque ligne en transaction normalisée
                rows.append(row_to_tx(r))
            except KeyError as e:
                # En cas d'erreur, affiche les colonnes disponibles pour faciliter le debug
                keys_seen = list(r.keys())
                raise KeyError(
                    f"Colonne manquante {e}. En-têtes disponibles: {keys_seen}"
                ) from e
    # Insertion en base des transactions extraites
    n = bulk_insert(rows)
    print(f"{n} lignes traitées (doublons ignorés).")


if __name__ == "__main__":
    main()
