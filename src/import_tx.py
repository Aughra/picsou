# src/import_tx.py
# -----------------------------------------------------------------------------
# import_tx.py
# Script d'importation de transactions depuis un fichier CSV vers une base de
# données MariaDB/MySQL. Ce script lit un fichier CSV contenant des transactions
# crypto et les insère dans la table "transactions".
# Utilisation : python import_tx.py chemin/vers/transactions.csv
# -----------------------------------------------------------------------------

# --- Imports des modules nécessaires ---
import os
import csv
import pymysql
import pathlib
import sys

# --- Configuration des paramètres de connexion à la base de données ---
db_host = os.getenv("DB_HOST", "localhost")
db_port = int(os.getenv("DB_PORT", "3306"))
db_user = os.getenv("DB_USER", "root")
db_password = os.getenv("DB_PASSWORD", "")
db_name = os.getenv("DB_NAME", "portfolio")

# --- Lecture du chemin du fichier CSV depuis les arguments ---
csv_path = pathlib.Path(sys.argv[1])  # ex: seeds/transactions.csv

# --- Connexion à la base de données ---
con = pymysql.connect(
    host=db_host,
    port=db_port,
    user=db_user,
    password=db_password,
    database=db_name,
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor,
)
cur = con.cursor()

# --- Lecture du CSV et insertion des transactions ---
with open(csv_path, newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        cur.execute(
            """INSERT INTO transactions
            (date, symbol, qty, price, fee, exchange, note)
            VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (
                row["date"],
                row["symbol"].lower(),
                float(row["qty"]),
                float(row["price"]),
                float(row.get("fee", 0) or 0),
                row.get("exchange"),
                row.get("note"),
            ),
        )

# --- Validation des modifications et fermeture de la connexion ---
con.commit()
con.close()
print("Transactions importées.")
