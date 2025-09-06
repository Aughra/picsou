#!/usr/bin/env bash
set -euo pipefail

# Se placer à la racine du projet
cd "$(dirname "$0")"

SRC_DIR="$HOME/Downloads"
DST_DIR="data/ledger_exports"
DST_FILE="$DST_DIR/ledger_latest.csv"

mkdir -p "$DST_DIR"

# Chercher le CSV le plus récent dans Downloads
latest=$(ls -t "$SRC_DIR"/*.csv 2>/dev/null | head -n 1 || true)

if [ -z "$latest" ]; then
  echo "❌ Aucun CSV trouvé dans $SRC_DIR"
  exit 1
fi

cp "$latest" "$DST_FILE"
echo "✅ Copié : $latest → $DST_FILE"

# Nettoyer automatiquement avec safe_import.py
if [ -f src/safe_import.py ]; then
  echo "🧹 Nettoyage avec safe_import.py..."
  TMP_FILE="${DST_FILE%.csv}_SAFE.csv"
  python3 src/safe_import.py "$DST_FILE" -o "$TMP_FILE" --head 8 --tail 4
  mv "$TMP_FILE" "$DST_FILE"
fi