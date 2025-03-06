import sqlite3
import pandas as pd
from datetime import datetime

# Configuration de la base de données SQLite
DB_PATH = "/data/database.db"

# Requêtes SQL pour analyser les ventes
QUERIES = {
    "chiffre_affaires_total": """
        SELECT SUM(p.prix * v.quantite) AS chiffre_affaires_total
        FROM ventes v
        JOIN produits p ON v.id_reference_produit = p.id_reference_produit;
    """,
    "ventes_par_produit": """
        SELECT p.nom, SUM(v.quantite) AS total_vendu, SUM(p.prix * v.quantite) AS chiffre_affaires
        FROM ventes v
        JOIN produits p ON v.id_reference_produit = p.id_reference_produit
        GROUP BY p.nom
        ORDER BY chiffre_affaires DESC;
    """,
    "ventes_par_region": """
        SELECT m.ville, SUM(v.quantite) AS total_vendu, SUM(p.prix * v.quantite) AS chiffre_affaires
        FROM ventes v
        JOIN magasins m ON v.id_magasin = m.id
        JOIN produits p ON v.id_reference_produit = p.id_reference_produit
        GROUP BY m.ville
        ORDER BY chiffre_affaires DESC;
    """
}

# Création de la table analyse si elle n'existe pas
CREATE_TABLE_ANALYSE = """
CREATE TABLE IF NOT EXISTS analyse (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_analyse TEXT,
    type_analyse TEXT,
    resultat TEXT
);
"""

def execute_query(query):
    """Exécute une requête SQL et retourne un DataFrame Pandas."""
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Erreur lors de l'exécution de la requête : {e}")
        return None

def insert_into_analyse(type_analyse, resultat):
    """Insère le résultat d'une analyse dans la table `analyse`."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Création de la table si elle n'existe pas
        cursor.execute(CREATE_TABLE_ANALYSE)

        # Insérer les résultats
        cursor.execute(
            "INSERT INTO analyse (date_analyse, type_analyse, resultat) VALUES (?, ?, ?)",
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), type_analyse, resultat)
        )

        conn.commit()
        conn.close()
        print(f"✅ Résultat de '{type_analyse}' inséré dans la table `analyse`.")
    except Exception as e:
        print(f"❌ Erreur lors de l'insertion dans `analyse` : {e}")

def check_data():
    """Vérifie les données dans les tables."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    tables = ["ventes", "produits", "magasins"]
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"Table {table} contient {count} enregistrements.")

    conn.close()

def main():
    """Exécute et stocke les résultats des requêtes SQL."""
    for key, query in QUERIES.items():
        print(f"\n🔍 Exécution de l'analyse : {key.replace('_', ' ').capitalize()}")

        df = execute_query(query)
        if df is not None and not df.empty:
            print(df)

            # Convertir le DataFrame en JSON pour l'insérer dans la table
            resultat_json = df.to_json(orient="records")

            # Insérer le résultat dans la table `analyse`
            insert_into_analyse(key, resultat_json)
        else:
            print("Aucune donnée trouvée.")

if __name__ == "__main__":
    check_data()
    main()
