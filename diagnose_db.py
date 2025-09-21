#!/usr/bin/env python3
"""
Script de diagnostic pour vérifier la structure de la base de données MSSQL.
"""

import sys
import os

# Ajouter le répertoire parent au chemin Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.db import get_conn


def check_database_structure():
    """Vérifie la structure de la base de données."""
    print("=== Diagnostic de la base de données ===")

    try:
        conn = get_conn()
        with conn.cursor() as cur:
            # Lister toutes les tables
            print("\n1. Tables disponibles :")
            cur.execute(
                """
                SELECT TABLE_SCHEMA, TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """
            )
            tables = cur.fetchall()

            if tables:
                for table in tables:
                    print(f"  - {table[0]}.{table[1]}")
            else:
                print("  Aucune table trouvée !")

            # Vérifier spécifiquement la table transactions
            print("\n2. Vérification de la table 'transactions' :")
            try:
                cur.execute(
                    """
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'transactions'
                    ORDER BY ORDINAL_POSITION
                """
                )
                columns = cur.fetchall()

                if columns:
                    print("  Structure de la table 'transactions' :")
                    for col in columns:
                        nullable = "NULL" if col[2] == "YES" else "NOT NULL"
                        max_len = f"({col[3]})" if col[3] else ""
                        print(f"    - {col[0]}: {col[1]}{max_len} {nullable}")
                else:
                    print("  ❌ Table 'transactions' introuvable !")

            except Exception as e:
                print(f"  ❌ Erreur lors de la vérification de 'transactions': {e}")

        conn.close()

    except Exception as e:
        print(f"❌ Erreur de connexion à la base de données: {e}")
        return False

    return True


if __name__ == "__main__":
    success = check_database_structure()
    if not success:
        sys.exit(1)
    print("\n✅ Diagnostic terminé.")
