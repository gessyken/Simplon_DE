import pandas as pd
import sqlite3
import os

# Configuration
DB_PATH = "/data/database.db"
DATA_PATH = "/data"

# Mapping des fichiers CSV vers les tables
CSV_FILES = {
    "magasins": "magasins.csv",
    "produits": "produits.csv",
    "ventes": "ventes.csv"
}

def read_csv(file_path):
    """Lit un fichier CSV et retourne un DataFrame."""
    try:
        filename = os.path.basename(file_path)
        
        # Paramètres spécifiques pour la lecture des ventes
        if filename == 'ventes.csv':
            df = pd.read_csv(file_path, parse_dates=['Date'])
            # Convertir la date en format ISO
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
        else:
            df = pd.read_csv(file_path)
        
        # Mapping des noms de colonnes
        column_mappings = {
            'magasins.csv': {
                'ID Magasin': 'id',
                'Ville': 'ville',
                'Nombre de salariés': 'nombre_de_salaries'
            },
            'produits.csv': {
                'Nom': 'nom',
                'ID Référence produit': 'id_reference_produit',
                'Prix': 'prix',
                'Stock': 'stock'
            },
            'ventes.csv': {
                'Date': 'date',
                'ID Référence produit': 'id_reference_produit',
                'Quantité': 'quantite',
                'ID Magasin': 'id_magasin'
            }
        }
        
        # Renommer les colonnes si un mapping existe
        if filename in column_mappings:
            df = df.rename(columns=column_mappings[filename])
        
        print(f"Données lues depuis {filename}:")
        print(df.head())
        return df
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier {file_path}: {e}")
        return None

def insert_data(df, table_name, primary_keys=None):
    """Insère les données dans la table."""
    if df is None or df.empty:
        print(f"Aucune donnée à insérer pour {table_name}.")
        return

    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Pour les ventes, on fait un remplacement complet
        if table_name == 'ventes':
            df.to_sql(table_name, conn, if_exists="replace", index=False)
        else:
            # Pour les autres tables, on garde la logique existante
            if primary_keys:
                cursor = conn.cursor()
                query = f"SELECT {', '.join(primary_keys)} FROM {table_name}"
                existing_ids = pd.read_sql(query, conn)
                df = df.merge(existing_ids, on=primary_keys, how="left", indicator=True)
                df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])
            
            if not df.empty:
                df.to_sql(table_name, conn, if_exists="append", index=False)
        
        print(f"✅ Données insérées dans {table_name}")
        
        # Vérification du nombre d'enregistrements
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"📊 Nombre d'enregistrements dans {table_name}: {count}")
        
        conn.close()
    except Exception as e:
        print(f"❌ Erreur lors de l'insertion des données dans {table_name}: {e}")

def main():
    """Lit les fichiers CSV et insère les données dans la base SQLite."""
    for table, filename in CSV_FILES.items():
        file_path = os.path.join(DATA_PATH, filename)
        print(f"📖 Lecture des données pour {table} depuis {file_path}...")
        
        if not os.path.exists(file_path):
            print(f"❌ Fichier non trouvé: {file_path}")
            continue

        df = read_csv(file_path)
        if df is not None:
            # Définition des clés primaires pour éviter les doublons
            primary_keys = {
                "ventes": None,  # Pas de clé primaire pour les ventes car on veut toutes les entrées
                "produits": ["id_reference_produit"],
                "magasins": ["id"]
            }.get(table)

            insert_data(df, table, primary_keys)

if __name__ == "__main__":
    main()

