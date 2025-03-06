from flask import Flask, jsonify
import sqlite3
import pandas as pd
import os
from execute_queries import main as execute_main

app = Flask(__name__)  
DATABASE = "/data/database.db"  # SQLite database path inside the container

# Ensure the /data directory exists (for Docker volume)
os.makedirs(os.path.dirname(DATABASE), exist_ok=True)

def get_db_connection():
    """Connect to SQLite database."""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row  # Allows dict-like row access
    return conn

def create_tables():
    """Create necessary tables in the database if they do not exist."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS produits (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nom TEXT NOT NULL,
        id_reference_produit TEXT NOT NULL,
        prix REAL NOT NULL,
        stock INTEGER NOT NULL
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS magasins (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ville TEXT NOT NULL,
        nombre_de_salaries INTEGER NOT NULL
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ventes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        id_reference_produit TEXT NOT NULL,
        id_magasin INTEGER NOT NULL,
        quantite INTEGER NOT NULL,
        FOREIGN KEY (id_reference_produit) REFERENCES produits(id_reference_produit),
        FOREIGN KEY (id_magasin) REFERENCES magasins(id)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS analyse (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date_analyse TEXT,
        type_analyse TEXT,
        resultat TEXT
    );
    """)

    conn.commit()
    conn.close()
    print("✅ Database and tables created successfully!")

@app.route('/ventes', methods=['GET'])
def get_ventes():
    """Fetch all sales records from the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM ventes")
    ventes = cursor.fetchall()
    conn.close()

    ventes_list = [dict(vente) for vente in ventes]
    return jsonify(ventes_list)

@app.route('/produits', methods=['GET'])
def get_produits():
    """Fetch all product records from the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM produits")
    produits = cursor.fetchall()
    conn.close()

    produits_list = [dict(produit) for produit in produits]
    return jsonify(produits_list)

@app.route('/magasins', methods=['GET'])
def get_magasins():
    """Fetch all store records from the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM magasins")
    magasins = cursor.fetchall()
    conn.close()

    magasins_list = [dict(magasin) for magasin in magasins]
    return jsonify(magasins_list)

@app.route('/analyses', methods=['GET'])
def get_analyses():

    execute_main()
    """Fetch all analysis records from the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM analyse")
    analyses = cursor.fetchall()
    conn.close()

    analyses_list = [dict(analyse) for analyse in analyses]
    return jsonify(analyses_list)

@app.route('/execute_analyses', methods=['POST'])
def execute_analyses():
    execute_main()
    return jsonify({"message": "Analyses executed and results stored in the database."}), 201

@app.route('/tables', methods=['GET'])
def get_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    conn.close()
    return jsonify([table['name'] for table in tables])


if __name__ == '__main__':
    create_tables()  # Create database tables
    app.run(host="0.0.0.0", port=5000, debug=True)  # Start Flask server
