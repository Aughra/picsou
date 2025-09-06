#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

DEFAULT_DROP_PATTERNS = [
    r"\bxpub\b",
    r"\baccount\s*name\b",
    r"\baccount\b.*\bname\b",
    r"\bhd[_\-\s]*path\b",
    r"\bseed\b",
]

DEFAULT_HASH_COL_PATTERNS = [
    r"\btxid\b",
    r"\btx[_\-]?hash\b",
    r"\bhash\b",
]

HASH_TRUNC_HEAD = 8
HASH_TRUNC_TAIL = 4


def compile_patterns(patterns):
    return [re.compile(p, re.IGNORECASE) for p in patterns]


def match_any(name, regexes):
    return any(r.search(name) for r in regexes)


def looks_like_hash(val: str) -> bool:
    if not isinstance(val, str):
        return False
    s = val.strip()
    # heuristique : chaîne hex/base58/64-ish assez longue
    return bool(re.fullmatch(r"[A-Za-z0-9+/=_:-]{16,}", s))


def truncate_hash(val: str, head=HASH_TRUNC_HEAD, tail=HASH_TRUNC_TAIL):
    s = val.strip()
    if len(s) <= head + tail + 1:
        return s
    return f"{s[:head]}…{s[-tail:]}"


def sanitize_df(
    df: pd.DataFrame,
    drop_regexes,
    hash_col_regexes,
    head=HASH_TRUNC_HEAD,
    tail=HASH_TRUNC_TAIL,
):
    original_cols = list(df.columns)
    # 1) Drop colonnes sensibles
    cols_to_drop = [c for c in df.columns if match_any(c, drop_regexes)]
    df = df.drop(columns=cols_to_drop, errors="ignore")

    # 2) Strip whitespace global
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # 3) Tronquer les hashes dans colonnes ciblées
    redactions = 0
    for col in df.columns:
        if match_any(col, hash_col_regexes):

            def redact(x):
                nonlocal redactions
                if looks_like_hash(x):
                    redactions += 1
                    return truncate_hash(x, head, tail)
                return x

            df[col] = df[col].map(redact)

    # 4) Déduplication
    before = len(df)
    df = df.drop_duplicates()
    dedup_count = before - len(df)

    return df, {
        "dropped_columns": cols_to_drop,
        "redactions": redactions,
        "deduplicated_rows": dedup_count,
        "original_columns": original_cols,
        "final_columns": list(df.columns),
    }


def main():
    ap = argparse.ArgumentParser(
        description="Import CSV 'safe' (drop xpub/account name, tronque hashes)."
    )
    ap.add_argument("input", help="CSV source")
    ap.add_argument("-o", "--output", help="CSV de sortie (sécurisé)")
    ap.add_argument("--sep", default=",", help="Séparateur CSV (défaut: ,)")
    ap.add_argument("--encoding", default="utf-8", help="Encodage (défaut: utf-8)")
    ap.add_argument(
        "--drop",
        nargs="*",
        default=[],
        help="Motifs regex ADDITIONNELS à supprimer (colonnes).",
    )
    ap.add_argument(
        "--hash-cols",
        nargs="*",
        default=[],
        help="Motifs regex ADDITIONNELS pour colonnes à tronquer comme hash.",
    )
    ap.add_argument(
        "--head", type=int, default=HASH_TRUNC_HEAD, help="Tête conservée pour hash."
    )
    ap.add_argument(
        "--tail", type=int, default=HASH_TRUNC_TAIL, help="Queue conservée pour hash."
    )
    ap.add_argument(
        "--dry-run", action="store_true", help="N'écrit rien, affiche le rapport."
    )
    args = ap.parse_args()

    inp = Path(args.input)
    if not inp.exists():
        print(f"[ERREUR] Fichier introuvable: {inp}", file=sys.stderr)
        sys.exit(2)

    outp = Path(args.output) if args.output else inp.with_name(inp.stem + "_SAFE.csv")
    if outp.resolve() == inp.resolve():
        print(
            "[ERREUR] Output identique à l'input — choisis un autre chemin.",
            file=sys.stderr,
        )
        sys.exit(2)

    # Charge
    try:
        df = pd.read_csv(inp, sep=args.sep, encoding=args.encoding)
    except Exception as e:
        print(f"[ERREUR] Lecture CSV: {e}", file=sys.stderr)
        sys.exit(2)

    drop_regexes = compile_patterns(DEFAULT_DROP_PATTERNS + args.drop)
    hash_col_regexes = compile_patterns(DEFAULT_HASH_COL_PATTERNS + args.hash_cols)

    safe_df, report = sanitize_df(
        df, drop_regexes, hash_col_regexes, args.head, args.tail
    )

    # Rapport
    print("=== SAFE IMPORT REPORT ===")
    print(
        f"- Colonnes originales ({len(report['original_columns'])}): {report['original_columns']}"
    )
    print(
        f"- Colonnes supprimées ({len(report['dropped_columns'])}): {report['dropped_columns']}"
    )
    print(
        f"- Colonnes finales ({len(report['final_columns'])}): {report['final_columns']}"
    )
    print(f"- Hash redactions: {report['redactions']}")
    print(f"- Lignes dédupliquées: {report['deduplicated_rows']}")
    print(f"- Sortie prévue: {outp}")

    if args.dry_run:
        print("\n[dry-run] Aucun fichier écrit.")
        return

    try:
        safe_df.to_csv(outp, sep=args.sep, index=False, encoding=args.encoding)
        print(f"[OK] Fichier écrit: {outp}")
    except Exception as e:
        print(f"[ERREUR] Écriture CSV: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
