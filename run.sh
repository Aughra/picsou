#!/usr/bin/env bash
set -euo pipefail

# Se placer dans le dossier du script (la racine du projet)
cd "$(dirname "$0")"

# Charger .env s'il existe
if [ -f .env ]; then
  # shellcheck disable=SC2046
  export $(grep -v '^#' .env | xargs -I {} echo {})
fi

/opt/homebrew/bin/python3 -m src.import_ledger_csv
/opt/homebrew/bin/python3 -m src.fetch_prices
/opt/homebrew/bin/python3 -m src.compute_report