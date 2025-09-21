# src/db.py
"""
Module de gestion de la connexion à la base de données MSSQL.

Ce module fournit deux fonctions principales pour se connecter à la base de données :
- get_conn() : pour obtenir une connexion directe via pymssql.
- get_engine() : pour obtenir un engine SQLAlchemy, utile notamment avec pandas.

Les paramètres de connexion (hôte, port, utilisateur, mot de passe, nom de la base)
sont lus depuis un fichier .env situé à la racine du projet, grâce à python-dotenv.

Cela permet de centraliser la configuration et la gestion des connexions à la base de données.

---
Changelog:
- 2025-09-21 10:00 (Europe/Paris) — [Agent] Migration de MariaDB vers MSSQL.
"""
import os
import pymssql
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
    Crée et retourne une connexion pymssql vers la base de données MSSQL.

    Utilise les variables d'environnement pour configurer la connexion.
    La connexion est en autocommit, ce qui pousse immédiatement chaque requête.

    Retourne un objet pymssql.Connection.
    """
    db_name = os.getenv("DB_NAME")
    db_host = os.getenv("DB_HOST", "192.168.1.43")
    db_port = os.getenv("DB_PORT", "1433")

    if db_name is None:
        raise ValueError("La variable d'environnement DB_NAME est obligatoire.")

    return pymssql.connect(
        server=db_host,  # Adresse du serveur MSSQL
        port=db_port,  # Port de connexion (par défaut 1433, en str)
        user=os.getenv("DB_USER"),  # Nom d'utilisateur pour la base (obligatoire)
        password=os.getenv("DB_PASSWORD"),  # Mot de passe associé (obligatoire)
        database=db_name,  # Nom de la base de données à utiliser (obligatoire)
        autocommit=True,  # Active l'autocommit pour pousser immédiatement chaque requête
    )


def get_engine():
    """
    Crée et retourne un engine SQLAlchemy pour se connecter à la base MSSQL.

    Cet engine est utile pour utiliser pandas.read_sql ou d'autres outils SQLAlchemy.

    L'URL de connexion est construite à partir des variables d'environnement,
    avec encodage des identifiants pour gérer les caractères spéciaux.

    Retourne un objet sqlalchemy.engine.Engine.
    """
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")
    db_host = os.getenv("DB_HOST", "127.0.0.1")
    db_port = os.getenv("DB_PORT", "1433")

    if not all([user, password, db_name]):
        raise ValueError(
            "Les variables d'environnement DB_USER, DB_PASSWORD et DB_NAME sont obligatoires."
        )

    u = quote_plus(user) if user else ""  # Encode le nom d'utilisateur (pour URL)
    pw = quote_plus(password) if password else ""  # Encode le mot de passe (pour URL)
    url = f"mssql+pymssql://{u}:{pw}@{db_host}:{db_port}/{db_name}"  # Construction de l'URL de connexion
    return create_engine(
        url, future=True, pool_pre_ping=True
    )  # Création et retour de l'engine SQLAlchemy
