#!/usr/bin/env bash
############################################################
# CHANGELOG:
# - [2025-09-21] agt007 {author=agent} {reason: ajout étape backfill quotidien des prix}
# - Impact: la pipeline récupère toutes les valeurs par jour dans price_snapshot avant le snapshot courant
# - Tests: exécution locale de run.sh (environnement avec MSSQL et COINS_MAP)
# - Notes: respecte les limites CoinGecko; ignore les symboles non mappés
############################################################
# Strict mode: arrêter dès la première erreur, variables non définies interdites, pipe propage les erreurs
set -euo pipefail

# ---
# Description:
#   Lance la pipeline Picsou : import ledger CSV → fetch des prix → compute & push snapshot DB.
#   Charge automatiquement les variables depuis .env et exporte COINS_MAP.
#
# Changelog:
# - 2025-09-12 19:35 (Europe/Paris) — [Aya] Sourcing .env sécurisé (set -a), logs lisibles, sélection souple du Python.
#
# Astuce:
#   Pour forcer un autre Python: `PYTHON_BIN=/usr/bin/python3 ./run.sh`
# ---

# Journaliser la commande qui plante si erreur
trap 'echo "[ERR] ${BASH_SOURCE[0]}:${LINENO}: commande \"${BASH_COMMAND}\" a échoué" >&2' ERR

# Détecter le dossier du script et s'y placer
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"


# Charger .env s'il existe (export auto de toutes les variables)
if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

# Choix du binaire Python (priorité à $PYTHON_BIN si fourni)
PYTHON_BIN="${PYTHON_BIN:-/opt/homebrew/bin/python3}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  PYTHON_BIN="python3"
fi

# Log de contexte
printf '[INFO] %s — Python: %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$PYTHON_BIN"
echo   "[INFO] COINS_MAP=$COINS_MAP"

# Exécution des étapes
"$PYTHON_BIN" -m src.import_ledger_csv

# Backfill quotidien des prix (CoinGecko market_chart → price_snapshot)
"$PYTHON_BIN" -m src.backfill_prices_daily || echo "[WARN] Backfill quotidien a échoué"

# Snapshot de prix courant (complément temps réel)
"$PYTHON_BIN" -m src.fetch_prices || echo "[WARN] fetch_prices a échoué"

"$PYTHON_BIN" -m src.compute_report


# Synchroniser la feuille Excel "Saisir" → calcul depuis DB (pas besoin du CSV)
"$PYTHON_BIN" tools/sync_excel_saisir.py || echo "[WARN] Sync Excel 'Saisir' a échoué"


echo "[OK] Pipeline terminée."
