#!/usr/bin/env python3
"""
Script de création de la table portfolio_snapshot pour MSSQL.
"""

import sys
import os

# Ajouter le répertoire parent au chemin Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.db import get_conn


def create_portfolio_snapshot_table():
    """Crée la table portfolio_snapshot si elle n'existe pas."""
    print("=== Création de la table portfolio_snapshot ===")

    create_table_sql = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'portfolio_snapshot')
    BEGIN
        CREATE TABLE portfolio_snapshot (
            id INT IDENTITY(1,1) PRIMARY KEY,
            ts DATETIME2 NOT NULL,
            symbol NVARCHAR(20) NOT NULL,
            qty DECIMAL(18,8) NOT NULL,
            investi DECIMAL(18,8) NOT NULL,
            price_eur DECIMAL(18,8) NOT NULL,
            valeur_actuelle DECIMAL(18,8) NOT NULL,
            pnl_eur DECIMAL(18,8) NOT NULL,
            pnl_pct DECIMAL(10,4) NOT NULL,
            created_at DATETIME2 DEFAULT GETDATE(),
            CONSTRAINT UK_portfolio_snapshot_ts_symbol UNIQUE (ts, symbol)
        );
        
        CREATE INDEX IX_portfolio_snapshot_symbol ON portfolio_snapshot(symbol);
        CREATE INDEX IX_portfolio_snapshot_ts ON portfolio_snapshot(ts);
        
        PRINT 'Table portfolio_snapshot créée avec succès.';
    END
    ELSE
    BEGIN
        PRINT 'Table portfolio_snapshot existe déjà.';
    END
    """

    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            conn.commit()
            print("✅ Script de création exécuté avec succès.")
        conn.close()

    except Exception as e:
        print(f"❌ Erreur lors de la création de la table: {e}")
        return False

    return True


if __name__ == "__main__":
    success = create_portfolio_snapshot_table()
    if not success:
        sys.exit(1)
    print("\n🎉 Table portfolio_snapshot prête à l'utilisation !")
