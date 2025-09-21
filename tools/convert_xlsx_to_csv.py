############################################################
# CHANGELOG:
# - [2025-09-21] agt001 {author=agent} {reason: ajout outil conversion XLSX→CSV/JSON}
# - Impact: nouvelle CLI utilitaire dans picsou/tools pour exporter chaque feuille d'un classeur Excel en CSV et/ou JSON
# - Tests: exécution locale recommandée sur un fichier exemple; validation par lecture pandas et création des fichiers de sortie
# - Notes: nécessite pandas et openpyxl; journalisation via logger_config
############################################################

"""
Outil de conversion Excel → CSV/JSON

Description
- Lit un fichier .xlsx et exporte chaque feuille dans un répertoire de sortie.
- Formats supportés: CSV (par défaut), JSON (optionnel).
- Conserve les noms de feuilles en les normalisant pour des noms de fichiers sûrs.

Utilisation rapide
- Depuis la racine du dépôt: `python picsou/tools/convert_xlsx_to_csv.py --input chemin/vers/fichier.xlsx --outdir sorties/`
- Ajouter `--json` pour produire également des .json (orient=records).

Exceptions
- En cas d'erreur de lecture/écriture, une exception est journalisée et relancée.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
import sys
from typing import Iterable, Optional

import pandas as pd

# Assurer l'import du logger commun quel que soit le cwd
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
from logger_config import logger


def _safe_filename(name: str) -> str:
    """Normalise un nom de feuille en nom de fichier sûr.

    Remplacements:
    - espaces et séparateurs → "_"
    - caractères non alphanumériques → supprimés
    - trim des underscores multiples

    Args:
        name: Nom d'entrée (feuille Excel).

    Returns:
        Nom de fichier sans extension, prêt à être suffixé (.csv/.json).
    """

    base = name.strip().lower()
    base = re.sub(r"[\s\-/]+", "_", base)
    base = re.sub(r"[^a-z0-9_]+", "", base)
    base = re.sub(r"_+", "_", base).strip("_")
    return base or "sheet"


def _ensure_unique(basename: str, used: set[str]) -> str:
    """Assure l'unicité du nom de fichier (sans extension) dans un ensemble donné.

    Si `basename` existe déjà, ajoute un suffixe numérique.

    Args:
        basename: Nom proposé, sans extension.
        used: Ensemble des noms déjà utilisés.

    Returns:
        Nom unique sans extension.
    """

    if basename not in used:
        used.add(basename)
        return basename
    i = 2
    while f"{basename}_{i}" in used:
        i += 1
    unique = f"{basename}_{i}"
    used.add(unique)
    return unique


def convert_xlsx(
    input_path: Path,
    outdir: Path,
    sheets: Optional[Iterable[str]] = None,
    export_json: bool = False,
) -> list[Path]:
    """Convertit les feuilles d'un classeur Excel en CSV et éventuellement en JSON.

    Args:
        input_path: Chemin du fichier .xlsx à lire.
        outdir: Répertoire de sortie (créé si absent).
        sheets: Sous-ensemble de feuilles à exporter (noms exacts). Si None, toutes les feuilles.
        export_json: Si True, créer aussi un .json pour chaque feuille (orient=records).

    Returns:
        Liste des chemins de fichiers créés.

    Raises:
        FileNotFoundError: si le fichier d'entrée est introuvable.
        ValueError: si aucune feuille demandée n'existe.
        Exception: pour les autres erreurs de lecture/écriture.
    """

    input_path = Path(input_path)
    outdir = Path(outdir)
    if not input_path.exists():
        raise FileNotFoundError(f"Fichier introuvable: {input_path}")

    outdir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Lecture Excel: {input_path}")

    # engine=openpyxl pour .xlsx
    xls = pd.ExcelFile(input_path, engine="openpyxl")
    # sheet_names peut contenir des types non-str selon certains moteurs; on force str
    all_sheets: list[str] = [str(s) for s in xls.sheet_names]

    target_sheets: list[str]
    if sheets:
        target_sheets = [s for s in all_sheets if s in set(sheets)]
        if not target_sheets:
            raise ValueError(
                f"Aucune des feuilles demandées n'existe. Demandées={list(sheets)}, disponibles={all_sheets}"
            )
    else:
        target_sheets = list(all_sheets)

    created: list[Path] = []
    used_names: set[str] = set()

    for sheet in target_sheets:
        logger.info(f"Export de la feuille: {sheet}")
        df = xls.parse(sheet_name=sheet)

        base = _safe_filename(sheet)
        base = _ensure_unique(base, used_names)

        csv_path = outdir / f"{base}.csv"
        df.to_csv(csv_path, index=False)
        created.append(csv_path)
        logger.info(f"→ CSV: {csv_path}")

        if export_json:
            json_path = outdir / f"{base}.json"
            # orient=records pour un format simple par ligne
            df.to_json(
                json_path, orient="records", date_format="iso", force_ascii=False
            )
            created.append(json_path)
            logger.info(f"→ JSON: {json_path}")

    logger.info(f"Fichiers créés: {len(created)}")
    return created


def _parse_args() -> argparse.Namespace:
    """Construit l'interface CLI et parse les arguments utilisateur."""

    p = argparse.ArgumentParser(
        description=(
            "Convertit un classeur Excel (.xlsx) en CSV (et JSON en option), une sortie par feuille."
        )
    )
    p.add_argument(
        "--input",
        required=True,
        help="Chemin du fichier .xlsx d'entrée",
    )
    p.add_argument(
        "--outdir",
        required=True,
        help="Répertoire de sortie pour les fichiers générés",
    )
    p.add_argument(
        "--sheets",
        nargs="*",
        default=None,
        help="Noms de feuilles à exporter (par défaut: toutes)",
    )
    p.add_argument(
        "--json",
        action="store_true",
        dest="export_json",
        help="Exporter également en JSON (orient=records)",
    )
    return p.parse_args()


def main() -> None:
    """Point d'entrée CLI."""

    args = _parse_args()
    input_path = Path(args.input)
    outdir = Path(args.outdir)
    try:
        convert_xlsx(
            input_path=input_path,
            outdir=outdir,
            sheets=args.sheets,
            export_json=args.export_json,
        )
    except Exception as e:
        logger.error(f"Erreur de conversion: {e}")
        raise


if __name__ == "__main__":
    main()
