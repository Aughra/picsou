# src/db.py
"""
Connexion MariaDB centralisée (pymysql).
Lit la config depuis les variables d'environnement (.env via python-dotenv).
"""
import os
import pymysql
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine


# Charge .env depuis la racine du projet (../.env par rapport à ce fichier)
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH)


def get_conn():
    return pymysql.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "picsou"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "picsou"),
        autocommit=False,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def get_engine():
    """
    Retourne un engine SQLAlchemy pour usage avec pandas.read_sql.
    L'URL est construite à partir des variables d'environnement.
    Exemple : mysql+mysqlclient://user:password@host:port/dbname
    """
    user = os.getenv("DB_USER", "picsou")
    password = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "3306")
    dbname = os.getenv("DB_NAME", "picsou")
    url = f"mysql+mysqlclient://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url, future=True)
