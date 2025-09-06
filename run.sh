#!/usr/bin/env bash
set -euo pipefail

# Charger .env s'il existe
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs -I {} echo {})
fi

python -m src.import_ledger_csv
python -m src.fetch_prices
python -m src.compute_report