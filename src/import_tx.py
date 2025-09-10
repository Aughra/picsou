# src/import_tx.py
# -----------------------------------------------------------------------------
"""
Script d'importation de transactions crypto depuis un fichier CSV vers une base de données MariaDB/MySQL.

Ce script lit un fichier CSV contenant des transactions (avec colonnes : date, symbol, qty, price, fee, exchange, note),
et insère ces données dans la table "transactions" de la base de données configurée via des variables d'environnement.

Modules utilisés :
- os : pour récupérer les variables d'environnement
- csv : pour lire le fichier CSV
- pymysql : pour se connecter et interagir avec la base de données MySQL/MariaDB
- pathlib : pour manipuler le chemin du fichier CSV
- sys : pour récupérer les arguments passés au script

Utilisation :
    python import_tx.py chemin/vers/transactions.csv

Hypothèses :
- Le fichier CSV doit contenir au minimum les colonnes suivantes : date, symbol, qty, price
- Les colonnes optionnelles sont : fee, exchange, note
- Les variables d'environnement DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME doivent être définies pour la connexion à la base
"""

# --- Imports des modules nécessaires ---
import os  # pour accéder aux variables d'environnement
import csv  # pour lire les fichiers CSV
import pymysql  # pour la connexion à la base de données MySQL/MariaDB
import pathlib  # pour manipuler les chemins de fichiers de façon portable
import sys  # pour récupérer les arguments passés au script

# --- Configuration des paramètres de connexion à la base de données ---
# On récupère les paramètres depuis les variables d'environnement, avec des valeurs par défaut si elles ne sont pas définies
db_host = os.getenv("DB_HOST", "localhost")
db_port = int(os.getenv("DB_PORT", "3306"))
db_user = os.getenv("DB_USER", "root")
db_password = os.getenv("DB_PASSWORD", "")
db_name = os.getenv("DB_NAME", "portfolio")

# --- Lecture du chemin du fichier CSV depuis les arguments ---
# Le premier argument passé au script doit être le chemin vers le fichier CSV contenant les transactions
csv_path = pathlib.Path(sys.argv[1])  # ex: seeds/transactions.csv

# --- Connexion à la base de données ---
# On établit une connexion à la base de données avec les paramètres configurés
con = pymysql.connect(
    host=db_host,
    port=db_port,
    user=db_user,
    password=db_password,
    database=db_name,
    charset="utf8mb4",  # encodage utf8mb4 pour supporter tous les caractères Unicode
    cursorclass=pymysql.cursors.DictCursor,  # pour récupérer les résultats sous forme de dictionnaires
)
cur = con.cursor()  # création d'un curseur pour exécuter les requêtes SQL

# --- Lecture du CSV et insertion des transactions ---
# On ouvre le fichier CSV en lecture, avec encodage UTF-8 et sans saut de ligne supplémentaire
with open(csv_path, newline="", encoding="utf-8") as f:
    # On parcourt chaque ligne du fichier CSV sous forme de dictionnaire (clé = nom de colonne)
    for row in csv.DictReader(f):
        # On exécute une requête d'insertion SQL pour chaque transaction
        cur.execute(
            """INSERT INTO transactions
            (date, symbol, qty, price, fee, exchange, note)
            VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (
                row["date"],  # date de la transaction
                row["symbol"].lower(),  # symbole en minuscules pour homogénéité
                float(row["qty"]),  # quantité convertie en float
                float(row["price"]),  # prix converti en float
                float(row.get("fee", 0) or 0),  # frais, par défaut 0 si absent ou vide
                row.get("exchange"),  # plateforme d'échange, peut être None
                row.get("note"),  # note éventuelle, peut être None
            ),
        )

# --- Validation des modifications et fermeture de la connexion ---
con.commit()  # on valide toutes les insertions dans la base de données
con.close()  # on ferme la connexion proprement
print("Transactions importées.")  # message de confirmation à l'utilisateur
