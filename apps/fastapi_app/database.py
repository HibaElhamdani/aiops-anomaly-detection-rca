"""
Gestion de la connexion SQLite (Mode WAL pour supporter Locust).
"""
import sqlite3
import os

DB_PATH = "banque.db"

def get_db():
    """Ouvre une connexion SQLite et la ferme automatiquement en fin de requête."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")  # Lectures simultanées OK
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    """Crée les 3 tables et insère un client de test si la base est vide."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE,
            password TEXT,
            nom_complet TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comptes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER,
            numero_compte TEXT UNIQUE,
            solde REAL,
            devise TEXT,
            FOREIGN KEY(client_id) REFERENCES clients(id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            compte_id INTEGER,
            type TEXT,
            montant REAL,
            destinataire TEXT,
            motif TEXT,
            date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(compte_id) REFERENCES comptes(id)
        )
    """)

    # Données de démo
    cursor.execute("SELECT COUNT(*) FROM clients")
    if cursor.fetchone()[0] == 0:
        cursor.execute(
            "INSERT INTO clients (username, password, nom_complet) VALUES (?, ?, ?)",
            ("hiba_el", "pass123", "Hiba El Hamdani")
        )
        cursor.execute(
            "INSERT INTO comptes (client_id, numero_compte, solde, devise) VALUES (?, ?, ?, ?)",
            (1, "MA123456789", 15420.50, "MAD")
        )
        conn.commit()
        print("✅ Base initialisée : client 'hiba_el' / pass123 | solde 15420.50 MAD")

    conn.close()