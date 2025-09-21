############################################################
# CHANGELOG:
# - [2025-09-21] agt008 {author=agent} {reason: ajout module DB (get_conn/get_engine) pour MSSQL}
# - Impact: unifie l’accès à la base pour tous les scripts (pymssql + SQLAlchemy)
# - Tests: connexion simple via diagnose_db.py; utilisé par backfill_prices_daily et sync_excel_saisir
# - Notes: lit la configuration depuis variables d’environnement (.env chargé par run.sh)
############################################################
"""
Accès base MSSQL pour Picsou.

Variables d’environnement attendues (chargées par run.sh depuis .env):
- DB_HOST (ex: 127.0.0.1)
- DB_PORT (ex: 1433)
- DB_USER (ex: sa)
- DB_PASSWORD
- DB_NAME (ex: picsou)

Fournit:
- get_conn() → connexion pymssql
- get_engine() → SQLAlchemy Engine (mssql+pymssql)
"""

from __future__ import annotations

import os
from typing import Optional

import pymssql
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from logger_config import logger


def _get_env(name: str, default: Optional[str] = None) -> str:
    val = os.getenv(name, default or "")
    if not val and default is None:
        logger.warning("Variable d’environnement manquante: %s", name)
    return val


def get_conn() -> pymssql.Connection:
    """Retourne une connexion pymssql configurée depuis l’environnement."""
    host = _get_env("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "1433")
    user = _get_env("DB_USER", "sa")
    password = _get_env("DB_PASSWORD", "")
    database = _get_env("DB_NAME", "picsou")
    try:
        conn = pymssql.connect(
            server=host, port=port, user=user, password=password, database=database
        )
        return conn
    except Exception as e:
        logger.error("Connexion MSSQL échouée (%s:%s/%s): %s", host, port, database, e)
        raise


_engine_singleton: Optional[Engine] = None


def get_engine() -> Engine:
    """Retourne un Engine SQLAlchemy réutilisable (mssql+pymssql)."""
    global _engine_singleton
    if _engine_singleton is not None:
        return _engine_singleton
    host = _get_env("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "1433")
    user = _get_env("DB_USER", "sa")
    password = _get_env("DB_PASSWORD", "")
    database = _get_env("DB_NAME", "picsou")
    # mssql+pymssql DSN
    dsn = f"mssql+pymssql://{user}:{password}@{host}:{port}/{database}?charset=utf8"
    try:
        _engine_singleton = create_engine(dsn, pool_pre_ping=True, future=True)
        return _engine_singleton
    except Exception as e:
        logger.error("Création Engine SQLAlchemy échouée: %s", e)
        raise
