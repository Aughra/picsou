# src/db.py
"""
Module de gestion de la connexion à la base de données MariaDB.

Ce module fournit deux fonctions principales pour se connecter à la base de données :
- get_conn() : pour obtenir une connexion directe via pymysql.
- get_engine() : pour obtenir un engine SQLAlchemy, utile notamment avec pandas.

Les paramètres de connexion (hôte, port, utilisateur, mot de passe, nom de la base)
sont lus depuis un fichier .env situé à la racine du projet, grâce à python-dotenv.

Ces fonctions sont utilisées par d'autres scripts du projet, par exemple :
- import_ledger_csv.py
- fetch_prices.py
- compute_report.py

Cela permet de centraliser la configuration et la gestion des connexions à la base de données.

---
Changelog:
- 2025-09-12 19:00 (Europe/Paris) — [Aya] Passage de get_conn() en autocommit=True pour pousser immédiatement chaque requête.
- 2025-09-12 19:35 (Europe/Paris) — [Aya] Ajout de pool_pre_ping=True dans get_engine() pour éviter les connexions zombies.
"""
import os
import pymysql
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine
from urllib.parse import quote_plus


# Charge le fichier .env depuis la racine du projet (deux niveaux au-dessus de ce fichier)
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(
    dotenv_path=ENV_PATH
)  # Charge les variables d'environnement dans os.environ


def get_conn():
    """
    Crée et retourne une connexion pymysql vers la base de données MariaDB.

    Utilise les variables d'environnement pour configurer la connexion.
    La connexion est en autocommit, ce qui pousse immédiatement chaque requête.

    Retourne un objet pymysql.connections.Connection.
    """
    return pymysql.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),  # Adresse du serveur MariaDB
        port=int(os.getenv("DB_PORT", "3306")),  # Port de connexion (par défaut 3306)
        user=os.getenv("DB_USER", "picsou"),  # Nom d'utilisateur pour la base
        password=os.getenv("DB_PASSWORD", ""),  # Mot de passe associé
        database=os.getenv("DB_NAME", "picsou"),  # Nom de la base de données à utiliser
        autocommit=True,  # Désactive l'autocommit pour gérer les transactions
        charset="utf8mb4",  # Jeu de caractères utilisé
        cursorclass=pymysql.cursors.DictCursor,  # Curseur qui retourne des dictionnaires (clé=nom colonne)
    )


def get_engine():
    """
    Crée et retourne un engine SQLAlchemy pour se connecter à la base MariaDB.

    Cet engine est utile pour utiliser pandas.read_sql ou d'autres outils SQLAlchemy.

    L'URL de connexion est construite à partir des variables d'environnement,
    avec encodage des identifiants pour gérer les caractères spéciaux.

    Retourne un objet sqlalchemy.engine.Engine.
    L'engine utilise pool_pre_ping pour maintenir la validité des connexions dans le pool.
    """
    user = os.getenv("DB_USER", "picsou")  # Récupère le nom d'utilisateur
    password = os.getenv("DB_PASSWORD", "")  # Récupère le mot de passe
    host = os.getenv("DB_HOST", "127.0.0.1")  # Adresse du serveur
    port = os.getenv("DB_PORT", "3306")  # Port de connexion
    dbname = os.getenv("DB_NAME", "picsou")  # Nom de la base de données
    u = quote_plus(user)  # Encode le nom d'utilisateur (pour URL)
    pw = quote_plus(password)  # Encode le mot de passe (pour URL)
    url = f"mysql+pymysql://{u}:{pw}@{host}:{port}/{dbname}"  # Construction de l'URL de connexion
    return create_engine(
        url, future=True, pool_pre_ping=True
    )  # Création et retour de l'engine SQLAlchemy
