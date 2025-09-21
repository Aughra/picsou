#!/usr/bin/env python3
"""
Script de création de la table transactions pour MSSQL.
"""

import sys
import os

# Ajouter le répertoire parent au chemin Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.db import get_conn


def create_transactions_table():
    """Crée la table transactions si elle n'existe pas."""
    print("=== Création de la table transactions ===")

    create_table_sql = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'transactions')
    BEGIN
        CREATE TABLE transactions (
            id INT IDENTITY(1,1) PRIMARY KEY,
            date_utc DATETIME2 NOT NULL,
            symbol NVARCHAR(20) NOT NULL,
            qty DECIMAL(18,8) NOT NULL,
            price_eur DECIMAL(18,8) NOT NULL DEFAULT 0,
            fee_eur DECIMAL(18,8) NOT NULL DEFAULT 0,
            exchange NVARCHAR(50) NULL,
            note NVARCHAR(500) NULL,
            dedup_hash NVARCHAR(40) NOT NULL,
            created_at DATETIME2 DEFAULT GETDATE(),
            CONSTRAINT UK_transactions_dedup UNIQUE (dedup_hash)
        );
        
        CREATE INDEX IX_transactions_symbol ON transactions(symbol);
        CREATE INDEX IX_transactions_date ON transactions(date_utc);
        
        PRINT 'Table transactions créée avec succès.';
    END
    ELSE
    BEGIN
        PRINT 'Table transactions existe déjà.';
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


def verify_table_creation():
    """Vérifie que la table a été créée correctement."""
    print("\n=== Vérification de la table créée ===")

    try:
        conn = get_conn()
        with conn.cursor() as cur:
            # Vérifier la structure de la table
            cur.execute(
                """
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'transactions'
                ORDER BY ORDINAL_POSITION
            """
            )
            columns = cur.fetchall()

            if columns:
                print("Structure de la table 'transactions' :")
                for col in columns:
                    col_name, data_type, is_nullable, char_len, num_prec, num_scale = (
                        col
                    )
                    nullable = "NULL" if is_nullable == "YES" else "NOT NULL"

                    if data_type in ("decimal", "numeric") and num_prec and num_scale:
                        type_info = f"{data_type}({num_prec},{num_scale})"
                    elif data_type in ("nvarchar", "varchar") and char_len:
                        type_info = f"{data_type}({char_len})"
                    else:
                        type_info = data_type

                    print(f"  - {col_name}: {type_info} {nullable}")
                print("✅ Table vérifiée avec succès.")
            else:
                print("❌ La table n'a pas été créée correctement.")
                return False

        conn.close()

    except Exception as e:
        print(f"❌ Erreur lors de la vérification: {e}")
        return False

    return True


if __name__ == "__main__":
    success = create_transactions_table()
    if success:
        success = verify_table_creation()

    if not success:
        sys.exit(1)
    print("\n🎉 Table transactions prête à l'utilisation !")
