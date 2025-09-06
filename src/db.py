"""
Connexion MariaDB centralisée (pymysql).
Lit la config depuis les variables d'environnement (.env via python-dotenv).
"""

import os
import pymysql
from dotenv import load_dotenv

# Charger .env si présent
load_dotenv()


def get_conn():
    return pymysql.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "picsou"),
        autocommit=False,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
