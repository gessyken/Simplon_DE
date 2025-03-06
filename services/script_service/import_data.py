import requests
import pandas as pd
import sqlite3
from io import StringIO

# Configuration de la base de données SQLite
DB_PATH = "/data/database.db"  # Nom du fichier SQLite

# URLs des fichiers de données partagées par le client
DATA_URLS = {
    "clients": "https://exemple.com/clients.csv",
    "produits": "https://exemple.com/produits.csv",
    "ventes": "https://exemple.com/ventes.csv"
}

def download_csv(url):
    """Télécharge un fichier CSV depuis une URL et retourne un DataFrame."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text))
        return df
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors du téléchargement : {url} - {e}")
        return None

def insert_data(df, table_name, primary_keys=None):
    """
    Insère les données dans la table en évitant les doublons si des clés primaires sont définies.
    """
    if df is None or df.empty:
        print(f"Aucune donnée à insérer pour {table_name}.")
        return

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Vérification des doublons pour les tables avec des clés primaires
        if primary_keys:
            # Construction de la requête pour récupérer les IDs existants
            query = f"SELECT {', '.join(primary_keys)} FROM {table_name}"
            existing_ids = pd.read_sql(query, conn)

            # Fusion des données pour ne garder que les nouvelles entrées
            df = df.merge(existing_ids, on=primary_keys, how="left", indicator=True)
            df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])

        # Insertion des nouvelles données dans la table
        if not df.empty:
            df.to_sql(table_name, conn, if_exists="append", index=False)
            print(f"Données insérées dans {table_name}.")
        else:
            print(f"Aucune nouvelle donnée à insérer dans {table_name}.")

        conn.close()
    except Exception as e:
        print(f"Erreur lors de l'insertion des données dans {table_name} : {e}")

def main():
    """Télécharge et insère les données dans la base de données SQLite."""
    for table, url in DATA_URLS.items():
        print(f"Téléchargement des données pour {table}...")
        df = download_csv(url)
        if df is not None:
            # Définition des clés primaires pour éviter les doublons
            primary_keys = None
            if table == "ventes":
                primary_keys = ["id_vente"]  # Remplace avec la vraie clé primaire
            elif table == "produits":
                primary_keys = ["id_reference_produit"]
            elif table == "clients":
                primary_keys = ["id_client"]

            insert_data(df, table, primary_keys)

if __name__ == "__main__":
    main()

