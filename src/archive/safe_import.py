#!/usr/bin/env python3

"""
Script de "safe import" CSV pour nettoyer et sécuriser les données sensibles.

Objectif:
Ce script importe un fichier CSV et effectue un nettoyage automatique pour supprimer
des colonnes sensibles (ex: clés xpub, noms de compte, seeds) et tronquer les valeurs
qui ressemblent à des hashes (ex: txid, hash de transaction).

Comment ça marche:
- Suppression des colonnes dont le nom correspond à des motifs sensibles prédéfinis
  ou fournis par l'utilisateur via l'option --drop.
- Tronquage des colonnes identifiées comme contenant des hashes, selon des motifs
  prédéfinis ou fournis via --hash-cols. Les hashes sont tronqués en conservant
  les premiers et derniers caractères (par défaut 8 en tête et 4 en queue).
- Nettoyage des espaces superflus dans les cellules.
- Déduplication des lignes identiques.
- Génération d'un rapport de nettoyage.

Heuristique des hashes:
Une valeur est considérée comme un hash si elle est une chaîne alphanumérique (avec
quelques caractères spéciaux) suffisamment longue (16 caractères ou plus).

Options importantes:
--drop : motifs regex supplémentaires pour supprimer des colonnes sensibles.
--hash-cols : motifs regex supplémentaires pour identifier les colonnes de hash à tronquer.
--head : nombre de caractères conservés en début de hash tronqué (défaut 8).
--tail : nombre de caractères conservés en fin de hash tronqué (défaut 4).
--dry-run : n'écrit pas de fichier, affiche seulement le rapport.

Exemples d'utilisation:
1) Nettoyer un CSV standard:
   python safe_import.py fichier.csv

2) Nettoyer en ajoutant un motif de suppression:
   python safe_import.py fichier.csv --drop "clé\s*privée"

3) Nettoyer en ajoutant une colonne de hash personnalisée à tronquer:
   python safe_import.py fichier.csv --hash-cols "custom_hash"

4) Nettoyer sans écrire de fichier (dry-run):
   python safe_import.py fichier.csv --dry-run
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

# Motifs regex pour identifier les colonnes à supprimer (ex: xpub, noms de compte, seed)
DEFAULT_DROP_PATTERNS = [
    r"\bxpub\b",
    r"\baccount\s*name\b",
    r"\baccount\b.*\bname\b",
    r"\bhd[_\-\s]*path\b",
    r"\bseed\b",
]

# Motifs regex pour identifier les colonnes contenant des hashes à tronquer
DEFAULT_HASH_COL_PATTERNS = [
    r"\btxid\b",
    r"\btx[_\-]?hash\b",
    r"\bhash\b",
]

# Nombre de caractères conservés en début et fin de hash tronqué
# Exemple: abcdefghijklmnopqrstuvwxyz -> abcdefgh…wxyz
HASH_TRUNC_HEAD = 8
HASH_TRUNC_TAIL = 4


def compile_patterns(patterns):
    """
    Compile une liste de motifs regex en objets regex avec insensibilité à la casse.

    Args:
        patterns (list[str]): Liste de motifs regex sous forme de chaînes.

    Returns:
        list[re.Pattern]: Liste d'objets regex compilés.
    """
    return [re.compile(p, re.IGNORECASE) for p in patterns]


def match_any(name, regexes):
    """
    Vérifie si une chaîne correspond à au moins un des motifs regex fournis.

    Args:
        name (str): Chaîne à tester.
        regexes (list[re.Pattern]): Liste d'objets regex.

    Returns:
        bool: True si au moins un motif matche, sinon False.
    """
    return any(r.search(name) for r in regexes)


def looks_like_hash(val: str) -> bool:
    """
    Détermine si une valeur ressemble à un hash selon une heuristique simple.

    Args:
        val (str): Valeur à tester.

    Returns:
        bool: True si la valeur ressemble à un hash, sinon False.
    """
    if not isinstance(val, str):
        return False
    s = val.strip()
    # heuristique : chaîne hex/base58/64-ish assez longue
    return bool(re.fullmatch(r"[A-Za-z0-9+/=_:-]{16,}", s))


def truncate_hash(val: str, head=HASH_TRUNC_HEAD, tail=HASH_TRUNC_TAIL):
    """
    Tronque un hash en conservant la tête et la queue, séparées par une ellipse.

    Args:
        val (str): Hash à tronquer.
        head (int): Nombre de caractères conservés en début.
        tail (int): Nombre de caractères conservés en fin.

    Returns:
        str: Hash tronqué ou original si trop court.
    """
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
    """
    Nettoie un DataFrame en supprimant des colonnes sensibles, en tronquant les hashes,
    en supprimant les espaces superflus, et en dédupliquant les lignes.

    Args:
        df (pd.DataFrame): DataFrame original.
        drop_regexes (list[re.Pattern]): Regex pour colonnes à supprimer.
        hash_col_regexes (list[re.Pattern]): Regex pour colonnes à tronquer.
        head (int): Nombre de caractères conservés en tête de hash.
        tail (int): Nombre de caractères conservés en queue de hash.

    Returns:
        tuple: (DataFrame nettoyé, dict rapport)
            rapport contient:
                - dropped_columns: colonnes supprimées
                - redactions: nombre de hashes tronqués
                - deduplicated_rows: nombre de lignes supprimées par déduplication
                - original_columns: colonnes initiales
                - final_columns: colonnes après nettoyage
    """
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
    # Étapes principales:
    # 1) Parser les arguments
    # 2) Vérifier existence fichier input et éviter écrasement output
    # 3) Charger CSV
    # 4) Compiler regex
    # 5) Nettoyer DataFrame
    # 6) Afficher rapport
    # 7) Écrire fichier output (sauf dry-run)

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
        sys.exit(2)  # Sortie erreur si fichier input absent

    outp = Path(args.output) if args.output else inp.with_name(inp.stem + "_SAFE.csv")
    if outp.resolve() == inp.resolve():
        print(
            "[ERREUR] Output identique à l'input — choisis un autre chemin.",
            file=sys.stderr,
        )
        sys.exit(2)  # Sortie erreur si output écraserait input

    # Lecture CSV
    try:
        df = pd.read_csv(inp, sep=args.sep, encoding=args.encoding)
    except Exception as e:
        print(f"[ERREUR] Lecture CSV: {e}", file=sys.stderr)
        sys.exit(2)  # Sortie erreur si lecture échoue

    # Compilation des regex
    drop_regexes = compile_patterns(DEFAULT_DROP_PATTERNS + args.drop)
    hash_col_regexes = compile_patterns(DEFAULT_HASH_COL_PATTERNS + args.hash_cols)

    # Nettoyage DataFrame
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
        return  # Fin sans écriture en dry-run

    # Écriture CSV nettoyé
    try:
        safe_df.to_csv(outp, sep=args.sep, index=False, encoding=args.encoding)
        print(f"[OK] Fichier écrit: {outp}")
    except Exception as e:
        print(f"[ERREUR] Écriture CSV: {e}", file=sys.stderr)
        sys.exit(2)  # Sortie erreur si écriture échoue


if __name__ == "__main__":
    main()
