############################################################
# CHANGELOG:
# - [2025-09-21] agt012 {author=agent} {reason: builder feuille "Saisir" depuis DB}
# - Impact: expose build_saisir(coins) → DataFrame (colonnes/ordre attendus), Europe/Paris, ffill prix
# - Tests: utilisé par tools/sync_excel_saisir.py via run.sh
# - Notes: valeur/gain NULL si aucun prix connu avant la date
############################################################
"""
Construction de la feuille "Saisir" à partir des tables SQL:
- transactions(date_utc, symbol, qty, price_eur, fee_eur)
- price_snapshot(ts, symbol, price_eur)

Règles:
- Coins: ["btc", "eth", "avax", "dot", "ada", "sol", "xrp"]
- Europe/Paris pour l'agrégation par jour
- Prix ffill (si aucun prix historique → valeurs NULL ce jour)
- "<coin> acheté" (jour) = sum((qty>0) * qty*price_eur + fee_eur)
- "<coin> acheté cumul" = cumul des apports (ne diminue pas aux ventes)
- Quantité détenue = cumul (achats - ventes) → valeur = qty_cum * prix_jour
- "<coin> valeur cumul" = valeur (niveau du jour)
- "<coin> gain perte" = valeur - acheté cumul
- Blocs fin: "acheté COIN" = acheté cumul; "Valeur COIN" = valeur
- Totaux = sommes de ces blocs
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable
from zoneinfo import ZoneInfo

import pandas as pd

from logger_config import logger
from src.db import get_engine


PARIS = ZoneInfo("Europe/Paris")
COINS_DEFAULT = ["btc", "eth", "avax", "dot", "ada", "sol", "xrp"]


def _load_transactions() -> pd.DataFrame:
    eng = get_engine()
    with eng.begin() as con:
        tx = pd.read_sql(
            "SELECT date_utc, LOWER(symbol) AS symbol, qty, price_eur, fee_eur FROM transactions",
            con,
        )
    if tx.empty:
        return tx
    tx["date_utc"] = pd.to_datetime(tx["date_utc"], utc=True)
    tx["date_local"] = tx["date_utc"].dt.tz_convert(PARIS).dt.normalize()
    for col in ("qty", "price_eur", "fee_eur"):
        tx[col] = pd.to_numeric(tx[col], errors="coerce").fillna(0.0)
    return tx


def _load_prices() -> pd.DataFrame:
    eng = get_engine()
    with eng.begin() as con:
        pr = pd.read_sql(
            "SELECT LOWER(symbol) AS symbol, ts, price_eur FROM price_snapshot",
            con,
        )
    if pr.empty:
        return pr
    pr["ts"] = pd.to_datetime(pr["ts"], utc=True)
    pr["date_local"] = pr["ts"].dt.tz_convert(PARIS).dt.normalize()
    pr["price_eur"] = pd.to_numeric(pr["price_eur"], errors="coerce")
    return pr


def _date_range(start, end) -> pd.DatetimeIndex:
    return pd.date_range(start=start, end=end, freq="D", tz=PARIS).normalize()


def _build_frames(
    coins: Iterable[str],
) -> tuple[pd.DatetimeIndex, dict, dict, dict, dict]:
    tx = _load_transactions()
    if tx.empty:
        raise RuntimeError(
            "Aucune transaction en base — impossible de déterminer le début."
        )
    pr = _load_prices()

    start_day = tx["date_local"].min()
    today = pd.Timestamp(datetime.now(PARIS)).normalize()
    days = _date_range(start_day, today)

    # Prix du jour (dernier snapshot du jour), puis reindex + ffill + bfill
    price_series: dict[str, pd.Series] = {
        c: pd.Series(index=days, dtype="float64") for c in coins
    }
    if not pr.empty:
        pr = pr.sort_values(["symbol", "date_local", "ts"]).dropna(subset=["price_eur"])
        last_per_day = (
            pr.groupby(["symbol", "date_local"])["price_eur"].last().reset_index()
        )
        for c in coins:
            s = (
                last_per_day[last_per_day["symbol"] == c]
                .set_index("date_local")["price_eur"]
                .sort_index()
            )
            s = s.reindex(days)
            # Forward-fill pour couvrir les jours sans snapshot
            s = s.ffill()
            # Backward-fill pour les jours avant le premier prix connu (utile pour valeur=0 quand qty=0)
            s = s.bfill()
            price_series[c] = s

    # Agrégats journaliers
    buy_eur: dict[str, pd.Series] = {}
    buy_eur_cum: dict[str, pd.Series] = {}
    qty_cum: dict[str, pd.Series] = {}

    for c in coins:
        tx_c = tx[tx["symbol"] == c].copy()
        tx_c["buy_eur"] = (tx_c["qty"].clip(lower=0) * tx_c["price_eur"]) + tx_c[
            "fee_eur"
        ]
        s_buy = (
            tx_c.groupby("date_local")["buy_eur"].sum()
            if not tx_c.empty
            else pd.Series(dtype="float64")
        )
        s_buy = s_buy.reindex(days).fillna(0.0)
        buy_eur[c] = s_buy
        buy_eur_cum[c] = s_buy.cumsum()

        s_qty = (
            tx_c.groupby("date_local")["qty"].sum()
            if not tx_c.empty
            else pd.Series(dtype="float64")
        )
        s_qty = s_qty.reindex(days).fillna(0.0).cumsum()
        qty_cum[c] = s_qty

    return days, buy_eur, buy_eur_cum, qty_cum, price_series


def build_saisir(coins: Iterable[str] = COINS_DEFAULT) -> pd.DataFrame:
    days, buy_eur, buy_eur_cum, qty_cum, price = _build_frames(coins)
    out = pd.DataFrame(index=days)
    out.index.name = "date"

    # Colonnes par coin
    for c in coins:
        # Valeur = quantité cumulée × prix du jour
        # - Si quantité = 0, la valeur doit être 0 même si le prix est NaN
        # - Les séries de prix ont été ffill+bfill pour éviter des NaN persistants
        #   (reste un filet de sécurité: remplacer NaN par 0 après coup)
        val = qty_cum[c].fillna(0.0) * price[c].fillna(0.0)
        val = val.fillna(0.0)
        gain = val - buy_eur_cum[c]
        out[f"{c} acheté"] = buy_eur[c]
        out[f"{c} valeur"] = val
        out[f"{c} gain perte"] = gain
        out[f"{c} acheté cumul"] = buy_eur_cum[c]
        out[f"{c} valeur cumul"] = val

    # Blocs fin et totaux
    for c in coins:
        u = c.upper()
        out[f"acheté {u}"] = out[f"{c} acheté cumul"]
        out[f"Valeur {u}"] = out[f"{c} valeur cumul"]

    out["total acheté"] = out[[f"acheté {c.upper()}" for c in coins]].sum(axis=1)
    out["total valeur"] = out[[f"Valeur {c.upper()}" for c in coins]].sum(axis=1)
    out["total gain perte"] = out["total valeur"] - out["total acheté"]

    # Ordre choisi: par coin puis tail puis totaux
    head_cols: list[str] = []
    for c in coins:
        head_cols += [
            f"{c} acheté",
            f"{c} valeur",
            f"{c} gain perte",
            f"{c} acheté cumul",
            f"{c} valeur cumul",
        ]
    tail_cols = []
    for c in coins:
        tail_cols += [f"acheté {c.upper()}", f"Valeur {c.upper()}"]
    final_cols = (
        head_cols + tail_cols + ["total acheté", "total valeur", "total gain perte"]
    )
    out = out[final_cols]

    # Index → colonne 'date' sans TZ
    out = out.reset_index()  # conserve le nom d'index 'date' en colonne 'date'
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    return out


if __name__ == "__main__":
    df = build_saisir()
    logger.info(
        f"DataFrame 'Saisir' construit: {df.shape[0]} lignes, {df.shape[1]} colonnes"
    )
    # Aperçu
    logger.info(
        "Colonnes: "
        + ", ".join(df.columns[:12])
        + (" ..." if len(df.columns) > 12 else "")
    )
